use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use tracing::info;

use crate::rindexer_client::RindexerInstance;
use crate::test_suite::{
    ClickHouseConfig, ContractConfig, ContractDetail, EventConfig, PostgresConfig, TestContext,
};
use crate::tests::helpers;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct DualWriteTests;

impl TestModule for DualWriteTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_dual_write_events",
                "DualWrite: 101 events (20 batches x 5 txs) indexed identically in PG and CH",
                dual_write_events,
            )
            .with_timeout(180),
            TestDefinition::new(
                "test_dual_write_reorg",
                "DualWrite: reorg at scale cleans up both PG and CH, no duplicates",
                dual_write_reorg,
            )
            .with_timeout(240)
            .with_chain_id(137),
        ]
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Start both PG and CH containers, return (pg_port, ch_port).
async fn start_dual_containers(
    context: &mut TestContext,
) -> Result<(u16, u16)> {
    let (pg_container, pg_port) = crate::docker::start_postgres_container().await?;
    context.register_container(pg_container);
    crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

    let (ch_container, ch_port) = crate::docker::start_clickhouse_container().await?;
    context.register_container(ch_container);
    crate::docker::wait_for_clickhouse_ready(ch_port, 15).await?;

    Ok((pg_port, ch_port))
}

/// Create a RindexerInstance with both PG and CH env vars.
fn create_dual_rindexer(
    context: &TestContext,
    pg_port: u16,
    ch_port: u16,
) -> RindexerInstance {
    let mut r = RindexerInstance::new(&context.rindexer_binary, context.project_path.clone());
    for (k, v) in crate::docker::postgres_env_vars(pg_port) {
        r = r.with_env(&k, &v);
    }
    for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
        r = r.with_env(&k, &v);
    }
    r
}

/// Query PG row count for a table, polling until expected or timeout.
async fn wait_for_pg_count(
    conn_str: &str,
    table: &str,
    expected: i64,
    timeout_secs: u64,
) -> Result<i64> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let query = format!("SELECT COUNT(*) FROM {}", table);
    let mut last_count: i64 = 0;

    while start.elapsed() < timeout {
        if let Ok((client, connection)) =
            tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await
        {
            tokio::spawn(async move { let _ = connection.await; });
            if let Ok(rows) = client.query(&query, &[]).await {
                last_count = rows[0].get(0);
                if last_count >= expected {
                    return Ok(last_count);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Ok(last_count)
}

/// Query CH row count for a table via HTTP API, polling until expected or timeout.
async fn wait_for_ch_count(
    http_client: &reqwest::Client,
    ch_port: u16,
    table: &str,
    expected: u64,
    timeout_secs: u64,
) -> Result<u64> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let mut last_count: u64 = 0;

    while start.elapsed() < timeout {
        let query = format!(
            "SELECT count() FROM {} FINAL FORMAT TabSeparated",
            table
        );
        if let Ok(resp) = http_client
            .get(format!("http://localhost:{}", ch_port))
            .query(&[("query", &query)])
            .send()
            .await
        {
            if let Ok(text) = resp.text().await {
                last_count = text.trim().parse().unwrap_or(0);
                if last_count >= expected {
                    return Ok(last_count);
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Ok(last_count)
}

/// Number of transfer batches. Each batch = 1 block with N_TXS_PER_BLOCK transfers.
/// Total events = 1 (mint) + BATCHES * TXS_PER_BLOCK.
const BATCHES: usize = 20;
const TXS_PER_BLOCK: usize = 5;

/// Generate N transfer blocks on Anvil. Disables automine, sends TXS_PER_BLOCK
/// transfers, mines a block, repeats BATCHES times. Returns total expected event count.
async fn generate_transfer_load(
    context: &TestContext,
    contract_address: &str,
) -> Result<(u64, u64)> {
    // 1 mint event from deployment is already on-chain
    let mut total_transfers: u64 = 0;

    for batch in 0..BATCHES {
        // Build transfers for this block
        let transfers: Vec<(ethers::types::Address, ethers::types::U256)> = (0..TXS_PER_BLOCK)
            .map(|i| {
                let idx = (batch * TXS_PER_BLOCK + i) as u64;
                let recipient = helpers::generate_test_address(idx);
                let amount = ethers::types::U256::from((idx + 1) * 1000);
                (recipient, amount)
            })
            .collect();

        // Disable automine, send batch, mine single block
        context.anvil.set_automine(false).await?;
        context
            .anvil
            .send_transfers_no_mine(contract_address, &transfers)
            .await?;
        context.anvil.mine_block().await?;
        context.anvil.set_automine(true).await?;

        total_transfers += TXS_PER_BLOCK as u64;
    }

    let end_block = context.anvil.get_block_number().await?;
    let total_events = 1 + total_transfers; // 1 mint + N transfers
    info!(
        "Generated {} transfers across {} blocks ({} events total, end_block={})",
        total_transfers, BATCHES, total_events, end_block
    );
    Ok((total_events, end_block))
}

// ============================================================================
// Test 1: Dual-write events — identical rows in PG and CH at scale
// ============================================================================

fn dual_write_events(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        // 1. Start both containers
        let (pg_port, ch_port) = start_dual_containers(context).await?;

        // 2. Deploy contract and generate load (20 batches x 5 txs = 100 transfers + 1 mint = 101 events)
        let contract_address = context.deploy_test_contract().await?;
        let (expected_events, end_block) =
            generate_transfer_load(context, &contract_address).await?;

        // 3. Configure with BOTH PG and CH enabled
        let mut config = RindexerInstance::create_contract_config(
            &context.anvil.rpc_url,
            &contract_address,
            context.health_port,
        );
        config.name = "dual_write_test".to_string();
        config.storage.postgres = Some(PostgresConfig { enabled: true });
        config.storage.clickhouse = Some(ClickHouseConfig {
            enabled: true,
            drop_each_run: Some(true),
        });
        config.storage.csv.enabled = false;
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        // 4. Write config and start indexer with dual env vars
        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let mut rindexer = create_dual_rindexer(context, pg_port, ch_port);
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);
        context.wait_for_sync_completion(90).await?;

        // 5. Query both backends
        let pg_conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let pg_table = "dual_write_test_simple_erc_20.transfer";
        let ch_table = "dual_write_test_simple_erc_20.transfer";
        let http_client = reqwest::Client::new();

        let pg_count =
            wait_for_pg_count(&pg_conn_str, pg_table, expected_events as i64, 30).await?;
        let ch_count =
            wait_for_ch_count(&http_client, ch_port, ch_table, expected_events, 30).await?;
        info!("PG row count: {}, CH row count: {} (expected: {})", pg_count, ch_count, expected_events);

        // 6. Assert identical counts
        assert!(
            pg_count >= expected_events as i64,
            "PG should have >= {} rows, got {}",
            expected_events, pg_count
        );
        assert!(
            ch_count >= expected_events,
            "CH should have >= {} rows, got {}",
            expected_events, ch_count
        );
        assert_eq!(
            pg_count, ch_count as i64,
            "PG ({}) and CH ({}) row counts should match",
            pg_count, ch_count
        );

        // 7. Verify no duplicates in either backend
        let (pg_client, pg_conn) =
            tokio_postgres::connect(&pg_conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move { let _ = pg_conn.await; });
        let pg_dups = pg_client
            .query(
                &format!(
                    "SELECT tx_hash, log_index, COUNT(*) as cnt FROM {} GROUP BY tx_hash, log_index HAVING COUNT(*) > 1",
                    pg_table
                ),
                &[],
            )
            .await?;
        assert!(
            pg_dups.is_empty(),
            "PG should have no duplicates, found {}",
            pg_dups.len()
        );

        let ch_dup_query = format!(
            "SELECT tx_hash, log_index, count() as cnt FROM {} FINAL GROUP BY tx_hash, log_index HAVING cnt > 1 FORMAT TabSeparated",
            ch_table
        );
        let ch_dup_resp = http_client
            .get(format!("http://localhost:{}", ch_port))
            .query(&[("query", &ch_dup_query)])
            .send()
            .await?
            .text()
            .await?;
        assert!(
            ch_dup_resp.trim().is_empty(),
            "CH should have no duplicates, found: {}",
            ch_dup_resp.trim()
        );

        // 8. Verify checkpoints in both backends match end_block
        let pg_checkpoint_rows = pg_client
            .query(
                "SELECT last_synced_block::TEXT as block FROM rindexer_internal.dual_write_test_simple_erc_20_transfer WHERE network = 'anvil'",
                &[],
            )
            .await?;
        assert!(!pg_checkpoint_rows.is_empty(), "PG checkpoint should exist");
        let pg_checkpoint: u64 = pg_checkpoint_rows[0]
            .get::<_, String>("block")
            .parse()
            .unwrap_or(0);

        let ch_checkpoint_query =
            "SELECT last_synced_block FROM rindexer_internal.dual_write_test_simple_erc_20_transfer FINAL WHERE network = 'anvil' FORMAT TabSeparated";
        let ch_checkpoint: u64 = http_client
            .get(format!("http://localhost:{}", ch_port))
            .query(&[("query", ch_checkpoint_query)])
            .send()
            .await?
            .text()
            .await?
            .trim()
            .parse()
            .unwrap_or(0);

        info!(
            "Checkpoints — PG: {}, CH: {}, end_block: {}",
            pg_checkpoint, ch_checkpoint, end_block
        );
        assert!(pg_checkpoint > 0, "PG checkpoint should be > 0");
        assert!(ch_checkpoint > 0, "CH checkpoint should be > 0");
        assert_eq!(
            pg_checkpoint, ch_checkpoint,
            "PG and CH checkpoints should match: PG={}, CH={}",
            pg_checkpoint, ch_checkpoint
        );

        info!(
            "dual_write_events PASSED: {} events, PG={} CH={}, checkpoints PG={} CH={}, 0 duplicates",
            expected_events, pg_count, ch_count, pg_checkpoint, ch_checkpoint
        );

        Ok(())
    })
}

// ============================================================================
// Test 2: Dual-write reorg — both backends cleaned up
// ============================================================================

fn dual_write_reorg(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        // 1. Start both containers
        let (pg_port, ch_port) = start_dual_containers(context).await?;

        // 2. Deploy contract and generate load
        let contract_address = context.deploy_test_contract().await?;
        let (expected_events, _end_block) =
            generate_transfer_load(context, &contract_address).await?;

        // 3. Create reorg-aware config with both backends (chain_id=137)
        let mut config = RindexerInstance::create_minimal_config(
            &context.anvil.rpc_url,
            context.health_port,
        );
        config.name = "dual_reorg_test".to_string();
        config.networks[0].name = "polygon".to_string();
        config.networks[0].chain_id = 137;
        config.contracts = vec![ContractConfig {
            name: "SimpleERC20".to_string(),
            details: vec![ContractDetail {
                network: "polygon".to_string(),
                address: contract_address.clone(),
                start_block: "0".to_string(),
                end_block: None,
            }],
            abi: Some("./abis/SimpleERC20.abi.json".to_string()),
            reorg_safe_distance: Some(serde_json::json!(false)),
            include_events: Some(vec![EventConfig {
                name: "Transfer".to_string(),
            }]),
            tables: None,
        }];
        config.storage.postgres = Some(PostgresConfig { enabled: true });
        config.storage.clickhouse = Some(ClickHouseConfig {
            enabled: true,
            drop_each_run: Some(true),
        });
        config.storage.csv.enabled = false;

        // 4. Write config, start indexer, wait for sync
        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let mut rindexer = create_dual_rindexer(context, pg_port, ch_port);
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);
        context.wait_for_sync_completion(30).await?;

        // 5. Verify pre-reorg rows in both backends
        let pg_conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let pg_table = "dual_reorg_test_simple_erc_20.transfer";
        let ch_table = "dual_reorg_test_simple_erc_20.transfer";
        let http_client = reqwest::Client::new();

        let pg_pre = wait_for_pg_count(&pg_conn_str, pg_table, expected_events as i64, 30).await?;
        let ch_pre = wait_for_ch_count(&http_client, ch_port, ch_table, expected_events, 30).await?;
        info!("Pre-reorg: PG={}, CH={} (expected: {})", pg_pre, ch_pre, expected_events);
        assert!(pg_pre >= expected_events as i64, "PG pre-reorg should have >= {} rows, got {}", expected_events, pg_pre);
        assert!(ch_pre >= expected_events, "CH pre-reorg should have >= {} rows, got {}", expected_events, ch_pre);

        // 6. Stop indexer, trigger reorg, restart
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(4).await?;
        context.anvil.mine_block().await?;
        info!("Reorg triggered (depth=4), restarting indexer...");

        let mut rindexer2 = create_dual_rindexer(context, pg_port, ch_port);
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        // 7. Verify no duplicate tx_hashes in PG
        let (pg_client, pg_conn) =
            tokio_postgres::connect(&pg_conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move { let _ = pg_conn.await; });
        let pg_dups = pg_client
            .query(
                &format!(
                    "SELECT tx_hash, COUNT(*) as cnt FROM {} GROUP BY tx_hash HAVING COUNT(*) > 1",
                    pg_table
                ),
                &[],
            )
            .await?;
        assert!(
            pg_dups.is_empty(),
            "PG should have no duplicate tx_hashes after reorg, found {}",
            pg_dups.len()
        );

        // 8. Verify no duplicate tx_hashes in CH
        let ch_dup_query = format!(
            "SELECT tx_hash, count() as cnt FROM {} FINAL GROUP BY tx_hash HAVING cnt > 1 FORMAT TabSeparated",
            ch_table
        );
        let ch_dup_resp = http_client
            .get(format!("http://localhost:{}", ch_port))
            .query(&[("query", &ch_dup_query)])
            .send()
            .await?
            .text()
            .await?;
        let ch_dup_trimmed = ch_dup_resp.trim();
        assert!(
            ch_dup_trimmed.is_empty(),
            "CH should have no duplicate tx_hashes after reorg, found: {}",
            ch_dup_trimmed
        );

        // 9. Verify both backends have consistent post-reorg counts
        let pg_post = wait_for_pg_count(&pg_conn_str, pg_table, 1, 5).await?;
        let ch_post = wait_for_ch_count(&http_client, ch_port, ch_table, 1, 5).await?;
        info!("Post-reorg: PG={}, CH={}", pg_post, ch_post);

        info!(
            "dual_write_reorg PASSED: pre-reorg PG={}/CH={}, post-reorg PG={}/CH={}, no duplicates",
            pg_pre, ch_pre, pg_post, ch_post
        );

        Ok(())
    })
}
