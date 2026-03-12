use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::anvil_setup::ANVIL_DEFAULT_PRIVATE_KEY;
use crate::test_suite::TestContext;
use crate::tests::helpers;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct PostgresE2ETests;

impl TestModule for PostgresE2ETests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_postgres_end_to_end",
                "Enable Postgres storage, run indexing, and verify rows inserted",
                postgres_end_to_end_test,
            )
            .with_timeout(240),
            TestDefinition::new(
                "test_postgres_live_exact_events",
                "Feed live transfers, index into Postgres, assert exact recipients",
                postgres_live_exact_events_test,
            )
            .with_timeout(300),
        ]
    }
}

fn postgres_end_to_end_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Postgres E2E Test");

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

        let contract_address = context.deploy_test_contract().await?;
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        let current_block = context.anvil.get_block_number().await?;
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(current_block.to_string());
            }
        }

        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;
        r.start_indexer().await?;

        context.rindexer = Some(r);
        context.wait_for_sync_completion(60).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let row = client
            .query_opt("SELECT COUNT(*)::BIGINT FROM contract_test_simple_erc_20.transfer", &[])
            .await?;

        if let Some(r) = row {
            let count: i64 = r.get(0);
            if count <= 0 {
                return Err(anyhow::anyhow!(
                    "Expected at least 1 row in transfer table, got {}",
                    count
                ));
            }
        } else {
            return Err(anyhow::anyhow!("transfer table not found or query returned no rows"));
        }

        info!("Postgres E2E Test PASSED: rows inserted");
        Ok(())
    })
}

fn postgres_live_exact_events_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        use crate::live_feeder::LiveFeeder;

        info!("Running Postgres Live Exact Events Test");

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

        let contract_address = context.deploy_test_contract().await?;
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        r.start_indexer().await?;
        context.rindexer = Some(r);
        context.wait_for_sync_completion(20).await?;

        let mut feeder = LiveFeeder::new(
            context.anvil.rpc_url.clone(),
            ANVIL_DEFAULT_PRIVATE_KEY.to_string(),
        )
        .with_contract(contract_address.parse()?)
        .with_tx_interval(std::time::Duration::from_millis(800))
        .with_mine_interval(std::time::Duration::from_millis(400));
        feeder.start().await?;

        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let expected_recipients = vec![
            helpers::generate_test_address(0),
            helpers::generate_test_address(1),
        ];
        let expected_hex: Vec<String> = expected_recipients
            .iter()
            .map(|a| format!("0x{}", hex::encode(a.as_bytes())))
            .collect();

        let to_cols = vec!["to_address", "\"to\"", "recipient", "to"];
        let mut found = 0usize;
        for col in to_cols {
            let query = format!(
                "SELECT {} FROM contract_test_simple_erc_20.transfer ORDER BY block_number DESC LIMIT 10",
                col
            );
            let rows = match client.query(query.as_str(), &[]).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let mut recipients = Vec::new();
            for row in rows {
                let val: Result<String, _> = row.try_get(0);
                if let Ok(s) = val {
                    recipients.push(s.to_lowercase());
                } else {
                    let valb: Result<Vec<u8>, _> = row.try_get(0);
                    if let Ok(b) = valb {
                        recipients.push(format!("0x{}", hex::encode(b)));
                    }
                }
            }
            for exp in &expected_hex {
                if recipients.iter().any(|r| r == exp) {
                    found += 1;
                }
            }
            if found >= expected_hex.len() {
                break;
            }
        }

        feeder.stop();

        if found < expected_hex.len() {
            return Err(anyhow::anyhow!(
                "Did not find all expected recipients in Postgres: found {} of {}",
                found,
                expected_hex.len()
            ));
        }

        info!("Postgres Live Exact Events Test PASSED: recipients matched");
        Ok(())
    })
}
