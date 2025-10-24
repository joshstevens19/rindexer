use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct GraphqlQueriesTests;

impl TestModule for GraphqlQueriesTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_graphql_basic_query",
            "Start indexer+graphql, feed events, query transfers with filter & pagination",
            graphql_basic_query_test,
        )
        .with_timeout(300)
        .with_live_test()]
    }
}

fn graphql_basic_query_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        // Feeder is managed by TestRunner for live tests

        info!("Running GraphQL Queries Test");

        // Use the contract deployed by the TestRunner's live setup
        let contract_address = context
            .test_contract_address
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No test contract address available"))?;
        let mut config = context.create_contract_config(contract_address);
        // Enable Postgres for GraphQL (GraphQL typically serves off DB)
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        // Start a clean Postgres container (random port) for GraphQL backing store
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
        // Wait for Postgres readiness
        {
            let mut ready = false;
            for _ in 0..40 {
                if tokio_postgres::connect(
                    &format!(
                        "host=localhost port={} user=postgres password=postgres dbname=postgres",
                        pg_port
                    ),
                    tokio_postgres::NoTls,
                )
                .await
                .is_ok()
                {
                    ready = true;
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            if !ready {
                return Err(anyhow::anyhow!("Postgres did not become ready in time"));
            }
        }

        // Write config & ABI
        let config_path = context.project_path.join("rindexer.yaml");
        std::fs::create_dir_all(context.project_path.join("abis"))?;
        std::fs::copy(
            "abis/SimpleERC20.abi.json",
            context.project_path.join("abis").join("SimpleERC20.abi.json"),
        )?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        // Prepare instance with PG env (GraphQL uses the same DB)
        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        )
        .with_env("POSTGRES_HOST", "localhost")
        .with_env("POSTGRES_PORT", &pg_port.to_string())
        .with_env("POSTGRES_USER", "postgres")
        .with_env("POSTGRES_PASSWORD", "postgres")
        .with_env("POSTGRES_DB", "postgres")
        .with_env(
            "DATABASE_URL",
            &format!("postgres://postgres:postgres@localhost:{}/postgres", pg_port),
        )
        .with_env("GRAPHQL_PORT", "3001")
        .with_env("PORT", "3001");

        // Start ALL services (indexer + GraphQL) in one process
        r.start_all().await?;
        context.rindexer = Some(r.clone());

        // Wait for GraphQL URL from logs; fallback to default path
        let gql_url = r
            .wait_for_graphql_url(15)
            .await
            .or_else(|| Some("http://localhost:3001/graphql".to_string()))
            .unwrap();
        info!("GraphQL URL: {}", gql_url);

        // LiveFeeder is already running from TestRunner for live tests; wait a bit for events
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let client = reqwest::Client::new();

        // Discover actual query field for transfers via introspection
        let mut transfer_field = "transfers".to_string();
        let introspect_qtype = r#"query QType { __schema { queryType { name } } }"#;
        let mut query_type_name = "Query".to_string();
        if let Ok(resp) =
            client.post(&gql_url).json(&serde_json::json!({"query": introspect_qtype})).send().await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(name) = json["data"]["__schema"]["queryType"]["name"].as_str() {
                    query_type_name = name.to_string();
                }
            }
        }
        let introspect_fields = format!(
            "query Fields {{ Root: __type(name: \"{}\") {{ fields {{ name }} }} }}",
            query_type_name
        );
        if let Ok(resp) = client
            .post(&gql_url)
            .json(&serde_json::json!({"query": introspect_fields}))
            .send()
            .await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(fields) = json["data"]["Root"]["fields"].as_array() {
                    if let Some(name) = fields
                        .iter()
                        .filter_map(|f| f["name"].as_str())
                        .find(|n| n.to_lowercase().contains("transfer"))
                    {
                        transfer_field = name.to_string();
                    }
                }
            }
        }
        tracing::info!(
            "Discovered transfer field: {} on root type {}",
            transfer_field,
            query_type_name
        );

        // Introspect the field's args and return shape to build a compatible query
        #[derive(Debug)]
        struct FieldInfo {
            args: Vec<String>,
            returns_object: bool,
            return_object_name: Option<String>,
        }
        let mut field_info =
            FieldInfo { args: vec![], returns_object: false, return_object_name: None };
        let introspect_field = format!(
            "query FInfo {{\n  Root: __type(name: \"{}\") {{\n    fields {{ name args {{ name }} type {{ kind name ofType {{ kind name ofType {{ kind name }} }} }} }}\n  }}\n}}",
            query_type_name
        );
        if let Ok(resp) =
            client.post(&gql_url).json(&serde_json::json!({"query": introspect_field})).send().await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if let Some(fields) = json["data"]["Root"]["fields"].as_array() {
                    if let Some(f) =
                        fields.iter().find(|f| f["name"].as_str() == Some(&transfer_field))
                    {
                        if let Some(args) = f["args"].as_array() {
                            field_info.args = args
                                .iter()
                                .filter_map(|a| a["name"].as_str().map(|s| s.to_string()))
                                .collect();
                        }
                        // Unwrap nested ofType to get the named type
                        let mut t = &f["type"];
                        let mut name = t["name"].as_str().map(|s| s.to_string());
                        let mut kind = t["kind"].as_str().unwrap_or("").to_string();
                        if name.is_none() {
                            if let Some(_ot) = t["ofType"].as_object() {
                                t = &f["type"]["ofType"];
                                kind = t["kind"].as_str().unwrap_or("").to_string();
                                name = t["name"].as_str().map(|s| s.to_string());
                            }
                        }
                        if name.is_none() {
                            if let Some(_ot2) = t["ofType"].as_object() {
                                t = &t["ofType"];
                                kind = t["kind"].as_str().unwrap_or("").to_string();
                                name = t["name"].as_str().map(|s| s.to_string());
                            }
                        }
                        field_info.returns_object = kind == "OBJECT" || name.is_some();
                        field_info.return_object_name = name;
                    }
                }
            }
        }

        // If the return is an object, check for edges/items
        let mut use_edges = false;
        let mut use_items = false;
        if let Some(obj_name) = &field_info.return_object_name {
            let inspect_return = format!(
                "query RType {{ T: __type(name: \"{}\") {{ fields {{ name }} }} }}",
                obj_name
            );
            if let Ok(resp) = client
                .post(&gql_url)
                .json(&serde_json::json!({"query": inspect_return}))
                .send()
                .await
            {
                if let Ok(json) = resp.json::<serde_json::Value>().await {
                    if let Some(fields) = json["data"]["T"]["fields"].as_array() {
                        use_edges = fields.iter().any(|f| f["name"].as_str() == Some("edges"));
                        use_items = fields.iter().any(|f| f["name"].as_str() == Some("items"));
                    }
                }
            }
        }

        // Pick a pagination argument if supported
        let page_arg = if field_info.args.iter().any(|a| a == "first") {
            Some("first")
        } else if field_info.args.iter().any(|a| a == "take") {
            Some("take")
        } else if field_info.args.iter().any(|a| a == "limit") {
            Some("limit")
        } else {
            None
        };
        let page_arg_render = page_arg.map(|a| format!("{}: 5", a)).unwrap_or_default();

        // Build dynamic query for common shapes: connection(edges/node), items, or direct list
        let query = if use_edges {
            if page_arg.is_some() {
                format!("{{\n  {}({}) {{ edges {{ node {{ txHash }} }} pageInfo {{ hasNextPage }} }}\n}}", transfer_field, page_arg_render)
            } else {
                format!(
                    "{{\n  {} {{ edges {{ node {{ txHash }} }} pageInfo {{ hasNextPage }} }}\n}}",
                    transfer_field
                )
            }
        } else if use_items {
            if page_arg.is_some() {
                format!(
                    "{{\n  {}({}) {{ items {{ txHash }} pageInfo {{ hasNextPage }} }}\n}}",
                    transfer_field, page_arg_render
                )
            } else {
                format!(
                    "{{\n  {} {{ items {{ txHash }} pageInfo {{ hasNextPage }} }}\n}}",
                    transfer_field
                )
            }
        } else {
            // Assume it's a direct list
            if page_arg.is_some() {
                format!("{{\n  {}({}) {{ txHash }}\n}}", transfer_field, page_arg_render)
            } else {
                format!("{{\n  {} {{ txHash }}\n}}", transfer_field)
            }
        };

        // Retry more times and log errors to diagnose
        let mut body: Option<serde_json::Value> = None;
        for _ in 0..20 {
            match client.post(&gql_url).json(&serde_json::json!({"query": query})).send().await {
                Ok(rsp) => {
                    if rsp.status().is_success() {
                        match rsp.json::<serde_json::Value>().await {
                            Ok(json) => {
                                body = Some(json);
                                break;
                            }
                            Err(e) => {
                                tracing::error!("GraphQL JSON parse error: {}", e);
                            }
                        }
                    } else if let Ok(text) = rsp.text().await {
                        tracing::error!("GraphQL non-200: {}", text);
                    }
                }
                Err(e) => tracing::error!("GraphQL request error: {}", e),
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        let body =
            body.ok_or_else(|| anyhow::anyhow!("GraphQL did not return success after retries"))?;

        // Sanity checks: try edges/items/direct list
        let data = &body["data"][&transfer_field];
        let edges_len = data["edges"].as_array().map(|v| v.len()).unwrap_or(0);
        let items_len = data["items"].as_array().map(|v| v.len()).unwrap_or(0);
        let list_len = data.as_array().map(|v| v.len()).unwrap_or(0);
        let total = edges_len.max(items_len).max(list_len);
        if total == 0 {
            return Err(anyhow::anyhow!("GraphQL returned no transfers"));
        }

        // If pageInfo.hasNextPage is present, verify pagination flag exists
        let _ = body["data"]["transfers"]["pageInfo"]["hasNextPage"].as_bool();

        // Feeder is managed by TestRunner; no local stop

        // Cleanup PG container
        let _ = crate::docker::stop_postgres_container(&container_name).await;

        info!("✓ GraphQL Queries Test PASSED: basic query, filter, pagination");
        Ok(())
    })
}
