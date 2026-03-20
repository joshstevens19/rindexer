use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{self, format_address, generate_test_address};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct GraphqlQueriesTests;

impl TestModule for GraphqlQueriesTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_graphql_data_accuracy",
            "GraphQL: deploy+transfers, query via GraphQL, validate field values match chain",
            graphql_data_accuracy_test,
        )
        .with_timeout(300)]
    }
}

/// Validates that GraphQL returns accurate event data by:
/// 1. Deploying contract + 3 transfers with known amounts
/// 2. Starting indexer + GraphQL server
/// 3. Querying transfers via GraphQL
/// 4. Validating returned field values match what was sent on-chain
fn graphql_data_accuracy_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running GraphQL Data Accuracy Test");

        // Start Postgres
        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(container_name.clone());
        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        // Deploy contract + 3 transfers with varying amounts
        let contract_address = context.deploy_test_contract().await?;

        let amounts: Vec<u64> = vec![1000, 2000, 3000];
        let recipients: Vec<ethers::types::Address> = (0..3).map(generate_test_address).collect();

        for (recipient, amount) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, recipient, U256::from(*amount)).await?;
            context.anvil.mine_block().await?;
        }

        let end_block = context.anvil.get_block_number().await?;

        // Configure: Postgres enabled for GraphQL, CSV disabled, end_block set
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        // Allocate dynamic GraphQL port
        let gql_port = crate::docker::allocate_free_port()?;

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }
        r = r
            .with_env("GRAPHQL_PORT", &gql_port.to_string())
            .with_env("PORT", &gql_port.to_string());

        r.start_all().await?;
        context.rindexer = Some(r.clone());

        // Wait for GraphQL URL
        let fallback_url = format!("http://localhost:{}/graphql", gql_port);
        let gql_url = r.wait_for_graphql_url(20).await.unwrap_or(fallback_url);
        info!("GraphQL URL: {}", gql_url);

        // Wait for sync to complete
        context.wait_for_sync_completion(30).await?;

        // Give a moment for data to be queryable
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let client = reqwest::Client::new();

        // Discover the transfer query field via introspection
        let transfer_field = discover_transfer_field(&client, &gql_url).await?;
        info!("Discovered transfer field: {}", transfer_field);

        // PostGraphile with PgSimplifyInflectorPlugin uses relay connections
        // with `nodes` (not `items`). Column names are camelCased.
        // SQL reserved words `from`/`to` become `from`/`to` in GraphQL.
        let query = format!(
            "{{ {}(orderBy: BLOCK_NUMBER_ASC) {{ nodes {{ contractAddress from to value txHash blockNumber blockHash logIndex network }} }} }}",
            transfer_field
        );

        // Retry query until we get results (indexing may still be writing)
        let mut result_items: Vec<serde_json::Value> = Vec::new();
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);

        while start.elapsed() < timeout {
            match client.post(&gql_url).json(&serde_json::json!({"query": query})).send().await {
                Ok(rsp) if rsp.status().is_success() => {
                    if let Ok(json) = rsp.json::<serde_json::Value>().await {
                        // PostGraphile relay connection: nodes
                        if let Some(nodes) = json["data"][&transfer_field]["nodes"].as_array() {
                            if nodes.len() >= 4 {
                                result_items = nodes.clone();
                                break;
                            }
                        }
                        // Also try edges/node pattern
                        if let Some(edges) = json["data"][&transfer_field]["edges"].as_array() {
                            let nodes: Vec<_> = edges
                                .iter()
                                .filter_map(|e| e["node"].as_object())
                                .map(|n| serde_json::Value::Object(n.clone()))
                                .collect();
                            if nodes.len() >= 4 {
                                result_items = nodes;
                                break;
                            }
                        }
                    }
                }
                _ => {}
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }

        if result_items.len() < 4 {
            return Err(anyhow::anyhow!(
                "Expected at least 4 GraphQL results (1 mint + 3 transfers), got {}",
                result_items.len()
            ));
        }
        info!("GraphQL returned {} items", result_items.len());

        // Validate transfer rows (skip mint — first row with from=0x0)
        let transfer_items: Vec<_> = result_items
            .iter()
            .filter(|item| {
                let from =
                    item["from"].as_str().or_else(|| item["fromAddress"].as_str()).unwrap_or("");
                from != "0x0000000000000000000000000000000000000000"
            })
            .collect();

        if transfer_items.len() != 3 {
            return Err(anyhow::anyhow!(
                "Expected 3 non-mint transfers in GraphQL, got {}",
                transfer_items.len()
            ));
        }

        for (i, item) in transfer_items.iter().enumerate() {
            let to = item["to"].as_str().or_else(|| item["toAddress"].as_str()).unwrap_or("");
            let value = item["value"].as_str().unwrap_or("");

            let expected_to = format_address(&recipients[i]);
            let expected_value = amounts[i].to_string();

            if to.to_lowercase() != expected_to.to_lowercase() {
                return Err(anyhow::anyhow!(
                    "GraphQL transfer {}: to should be {}, got {}",
                    i,
                    expected_to,
                    to
                ));
            }
            if value != expected_value {
                return Err(anyhow::anyhow!(
                    "GraphQL transfer {}: value should be {}, got {}",
                    i,
                    expected_value,
                    value
                ));
            }

            // Validate tx_hash format
            let tx_hash =
                item["txHash"].as_str().or_else(|| item["tx_hash"].as_str()).unwrap_or("");
            if !tx_hash.starts_with("0x") || tx_hash.len() != 66 {
                return Err(anyhow::anyhow!(
                    "GraphQL transfer {}: invalid tx_hash format: {}",
                    i,
                    tx_hash
                ));
            }
        }

        // Verify all results have the correct network
        for (i, item) in result_items.iter().enumerate() {
            if let Some(network) = item["network"].as_str() {
                if network != "anvil" {
                    return Err(anyhow::anyhow!(
                        "GraphQL row {}: network should be 'anvil', got '{}'",
                        i,
                        network
                    ));
                }
            }
        }

        info!(
            "GraphQL Data Accuracy Test PASSED: {} items returned, \
             field values match chain state, tx_hash formats valid",
            result_items.len()
        );
        Ok(())
    })
}

/// Discover the GraphQL transfer query field name via introspection
async fn discover_transfer_field(client: &reqwest::Client, gql_url: &str) -> Result<String> {
    // Get query type name
    let introspect = r#"{ __schema { queryType { name } } }"#;
    let mut query_type = "Query".to_string();

    if let Ok(resp) =
        client.post(gql_url).json(&serde_json::json!({"query": introspect})).send().await
    {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(name) = json["data"]["__schema"]["queryType"]["name"].as_str() {
                query_type = name.to_string();
            }
        }
    }

    // Get fields on query type
    let fields_query = format!("{{ __type(name: \"{}\") {{ fields {{ name }} }} }}", query_type);
    if let Ok(resp) =
        client.post(gql_url).json(&serde_json::json!({"query": fields_query})).send().await
    {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(fields) = json["data"]["__type"]["fields"].as_array() {
                if let Some(name) = fields
                    .iter()
                    .filter_map(|f| f["name"].as_str())
                    .find(|n| n.to_lowercase().contains("transfer"))
                {
                    return Ok(name.to_string());
                }
            }
        }
    }

    Ok("transfers".to_string())
}
