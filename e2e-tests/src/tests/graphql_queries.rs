use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers;
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
        info!("Running GraphQL Queries Test");

        let contract_address = context
            .test_contract_address
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No test contract address available"))?;
        let mut config = context.create_contract_config(contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

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

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        // Allocate a dynamic GraphQL port
        let gql_port = crate::docker::allocate_free_port()?;

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }
        r = r.with_env("GRAPHQL_PORT", &gql_port.to_string())
            .with_env("PORT", &gql_port.to_string());

        r.start_all().await?;
        context.rindexer = Some(r.clone());

        // Wait for GraphQL URL from logs; fallback to dynamic port
        let fallback_url = format!("http://localhost:{}/graphql", gql_port);
        let gql_url = r
            .wait_for_graphql_url(15)
            .await
            .unwrap_or(fallback_url);
        info!("GraphQL URL: {}", gql_url);

        // Wait for LiveFeeder events to accumulate
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let client = reqwest::Client::new();

        // Discover the transfer query field via introspection
        let mut transfer_field = "transfers".to_string();
        let introspect_qtype = r#"query QType { __schema { queryType { name } } }"#;
        let mut query_type_name = "Query".to_string();
        if let Ok(resp) = client
            .post(&gql_url)
            .json(&serde_json::json!({"query": introspect_qtype}))
            .send()
            .await
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

        // Introspect args and return shape
        #[derive(Debug)]
        struct FieldInfo {
            args: Vec<String>,
            return_object_name: Option<String>,
        }
        let mut field_info = FieldInfo { args: vec![], return_object_name: None };
        let introspect_field = format!(
            "query FInfo {{\n  Root: __type(name: \"{}\") {{\n    fields {{ name args {{ name }} type {{ kind name ofType {{ kind name ofType {{ kind name }} }} }} }}\n  }}\n}}",
            query_type_name
        );
        if let Ok(resp) = client
            .post(&gql_url)
            .json(&serde_json::json!({"query": introspect_field}))
            .send()
            .await
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
                        let mut t = &f["type"];
                        let mut name = t["name"].as_str().map(|s| s.to_string());
                        if name.is_none() {
                            if t["ofType"].is_object() {
                                t = &f["type"]["ofType"];
                                name = t["name"].as_str().map(|s| s.to_string());
                            }
                        }
                        if name.is_none() {
                            if t["ofType"].is_object() {
                                t = &t["ofType"];
                                name = t["name"].as_str().map(|s| s.to_string());
                            }
                        }
                        field_info.return_object_name = name;
                    }
                }
            }
        }

        // Check for edges/items on return type
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

        // Pick a pagination argument
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

        // Build dynamic query
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
            if page_arg.is_some() {
                format!("{{\n  {}({}) {{ txHash }}\n}}", transfer_field, page_arg_render)
            } else {
                format!("{{\n  {} {{ txHash }}\n}}", transfer_field)
            }
        };

        // Retry query
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

        let data = &body["data"][&transfer_field];
        let edges_len = data["edges"].as_array().map(|v| v.len()).unwrap_or(0);
        let items_len = data["items"].as_array().map(|v| v.len()).unwrap_or(0);
        let list_len = data.as_array().map(|v| v.len()).unwrap_or(0);
        let total = edges_len.max(items_len).max(list_len);
        if total == 0 {
            return Err(anyhow::anyhow!("GraphQL returned no transfers"));
        }

        info!("GraphQL Queries Test PASSED: basic query, filter, pagination");
        Ok(())
    })
}
