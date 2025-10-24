use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct PostgresE2ETests;

impl TestModule for PostgresE2ETests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_postgres_end_to_end",
                "Enable Postgres storage, run indexing, and verify rows inserted",
                postgres_end_to_end_test,
            ).with_timeout(240),
            TestDefinition::new(
                "test_postgres_live_exact_events",
                "Feed live transfers, index into Postgres, assert exact recipients",
                postgres_live_exact_events_test,
            ).with_timeout(300),
        ]
    }
}

fn postgres_end_to_end_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Postgres E2E Test");

        // Start a clean Postgres container (random local port)
        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => { return Err(crate::tests::test_runner::SkipTest(format!("Docker not available: {}", e)).into()); }
        };

        // Deploy contract and build config with Postgres enabled
        let contract_address = context.deploy_test_contract().await?;
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        // Set end_block to current so we get a finite set of rows
        let current_block = context.anvil.get_block_number().await?;
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(current_block.to_string());
            }
        }

        // Wait for Postgres to accept connections before starting rindexer
        {
            let mut ready = false;
            for _ in 0..40 {
                if tokio_postgres::connect(
                    &format!("host=localhost port={} user=postgres password=postgres dbname=postgres", pg_port),
                    tokio_postgres::NoTls,
                ).await.is_ok() {
                    ready = true; break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            if !ready { return Err(anyhow::anyhow!("Postgres did not become ready in time")); }
        }

        // Start rindexer with PG env vars (also provide DATABASE_URL)
        let mut r = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone())
            .with_env("POSTGRES_HOST", "localhost")
            .with_env("POSTGRES_PORT", &pg_port.to_string())
            .with_env("POSTGRES_USER", "postgres")
            .with_env("POSTGRES_PASSWORD", "postgres")
            .with_env("POSTGRES_DB", "postgres")
            .with_env("DATABASE_URL", &format!("postgres://postgres:postgres@localhost:{}/postgres", pg_port));

        // Write config and start
        let config_path = context.project_path.join("rindexer.yaml");
        std::fs::create_dir_all(context.project_path.join("abis"))?;
        // Copy ABI
        std::fs::copy("abis/SimpleERC20.abi.json", context.project_path.join("abis").join("SimpleERC20.abi.json"))?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;
        r.start_indexer().await?;

        // Wait for completion (logs)
        // Reuse context to track the process
        context.rindexer = Some(r);
        context.wait_for_sync_completion(60).await?;

        // Connect to Postgres and assert rows exist for SimpleERC20.Transfer
        let (client, connection) = tokio_postgres::connect(
            &format!("host=localhost port={} user=postgres password=postgres dbname=postgres", pg_port),
            tokio_postgres::NoTls,
        ).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Table naming follows rindexer logs: schema `contract_test_simple_erc_20`, table `transfer`
        let row = client.query_opt(
            "SELECT COUNT(*)::BIGINT FROM contract_test_simple_erc_20.transfer",
            &[],
        ).await?;

        if let Some(r) = row {
            let count: i64 = r.get(0);
            if count <= 0 {
                return Err(anyhow::anyhow!("Expected at least 1 row in simpleerc20_transfer, got {}", count));
            }
        } else {
            return Err(anyhow::anyhow!("simpleerc20_transfer table not found or query returned no rows"));
        }

        info!("✓ Postgres E2E Test PASSED: rows inserted");

        // Cleanup container
        let _ = crate::docker::stop_postgres_container(&container_name).await;
        Ok(())
    })
}

fn postgres_live_exact_events_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        use ethers::types::Address;
        use crate::live_feeder::LiveFeeder;

        info!("Running Postgres Live Exact Events Test");

        // Start clean Postgres container
        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => { return Err(crate::tests::test_runner::SkipTest(format!("Docker not available: {}", e)).into()); }
        };

        // Deploy contract and enable Postgres
        let contract_address = context.deploy_test_contract().await?;
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres.enabled = true;
        config.storage.csv.enabled = false;

        // Ensure Postgres is ready before starting rindexer
        {
            let mut ready = false;
            for _ in 0..40 {
                if tokio_postgres::connect(
                    &format!("host=localhost port={} user=postgres password=postgres dbname=postgres", pg_port),
                    tokio_postgres::NoTls,
                ).await.is_ok() {
                    ready = true; break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            if !ready { return Err(anyhow::anyhow!("Postgres did not become ready in time")); }
        }

        // Start rindexer with PG env vars
        let mut r = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone())
            .with_env("POSTGRES_HOST", "localhost")
            .with_env("POSTGRES_PORT", &pg_port.to_string())
            .with_env("POSTGRES_USER", "postgres")
            .with_env("POSTGRES_PASSWORD", "postgres")
            .with_env("POSTGRES_DB", "postgres")
            .with_env("DATABASE_URL", &format!("postgres://postgres:postgres@localhost:{}/postgres", pg_port));

        // Write config
        let config_path = context.project_path.join("rindexer.yaml");
        std::fs::create_dir_all(context.project_path.join("abis"))?;
        std::fs::copy("abis/SimpleERC20.abi.json", context.project_path.join("abis").join("SimpleERC20.abi.json"))?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;

        // Start indexer and wait initial historic sync
        r.start_indexer().await?;
        context.rindexer = Some(r);
        context.wait_for_sync_completion(20).await?;

        // Start live feeder to emit transfers
        let mut feeder = LiveFeeder::new(
            context.anvil.rpc_url.clone(),
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
        ).with_contract(contract_address.parse()?)
         .with_tx_interval(std::time::Duration::from_millis(800))
         .with_mine_interval(std::time::Duration::from_millis(400));
        feeder.start().await?;

        // Wait to accumulate a few events
        tokio::time::sleep(std::time::Duration::from_secs(4)).await;

        // Connect to Postgres
        let (client, connection) = tokio_postgres::connect(
            &format!("host=localhost port={} user=postgres password=postgres dbname=postgres", pg_port),
            tokio_postgres::NoTls,
        ).await?;
        tokio::spawn(async move { let _ = connection.await; });

        // Helper to compute expected recipient addresses for counters 0..2
        fn expected_address_for_counter(counter: u64) -> String {
            let mut bytes = [0u8; 20];
            bytes[0] = 0x42;
            bytes[1..8].copy_from_slice(&counter.to_be_bytes()[..7]);
            let addr = Address::from(bytes);
            format!("0x{}", hex::encode(addr.as_bytes()))
        }

        let expected_recipients = vec![
            expected_address_for_counter(0),
            expected_address_for_counter(1),
        ];

        // Fetch recent rows and try different possible recipient column names
        let to_cols = vec!["to_address", "\"to\"", "recipient", "to"]; // try quoted "to" as well
        let mut found = 0usize;
        for col in to_cols {
            let query = format!("SELECT {} FROM contract_test_simple_erc_20.transfer ORDER BY block_number DESC LIMIT 10", col);
            let rows = match client.query(query.as_str(), &[]).await {
                Ok(r) => r,
                Err(_) => continue,
            };
            let mut recipients = Vec::new();
            for row in rows {
                // Try both text and bytea
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
            for exp in &expected_recipients {
                if recipients.iter().any(|r| r == exp) {
                    found += 1;
                }
            }
            if found >= expected_recipients.len() { break; }
        }

        // Stop feeder
        feeder.stop();

        if found < expected_recipients.len() {
            return Err(anyhow::anyhow!("Did not find all expected recipients in Postgres: found {} of {}", found, expected_recipients.len()));
        }

        info!("✓ Postgres Live Exact Events Test PASSED: recipients matched");
        let _ = crate::docker::stop_postgres_container(&container_name).await;
        Ok(())
    })
}


