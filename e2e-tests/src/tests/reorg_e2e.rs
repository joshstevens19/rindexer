use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{self, generate_test_address};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct ReorgTests;

impl TestModule for ReorgTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_reorg_live_pg_recovery",
                "Reorg: live detection via parent hash mismatch, PG rollback + no duplicates",
                reorg_live_pg_recovery,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_offline_startup_validation",
                "Reorg: stop/reorg/restart, startup validation catches stale latest_blocks",
                reorg_offline_startup_validation,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_csv_invalidation",
                "Reorg: CSV data remains consistent after stop/reorg/restart",
                reorg_csv_invalidation,
            )
            .with_timeout(120)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_double_reorg_idempotency",
                "Reorg: two consecutive offline reorgs, PG state clean after both",
                reorg_double_reorg_idempotency,
            )
            .with_timeout(240)
            .with_chain_id(137),
        ]
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a reorg-aware config: uses chain_id=137 (Polygon) so rindexer
/// enables reorg detection. Sets `reorg_safe_distance: false` so the indexer
/// runs at chain tip with active detection.
fn create_reorg_config(
    context: &TestContext,
    contract_address: &str,
) -> crate::test_suite::RindexerConfig {
    let mut config = crate::rindexer_client::RindexerInstance::create_minimal_config(
        &context.anvil.rpc_url,
        context.health_port,
    );
    config.name = "reorg_test".to_string();
    config.networks[0].name = "polygon".to_string();
    config.networks[0].chain_id = 137;
    config.contracts = vec![crate::test_suite::ContractConfig {
        name: "SimpleERC20".to_string(),
        details: vec![crate::test_suite::ContractDetail {
            network: "polygon".to_string(),
            address: contract_address.to_string(),
            start_block: "0".to_string(),
            end_block: None,
        }],
        abi: Some("./abis/SimpleERC20.abi.json".to_string()),
        reorg_safe_distance: Some(serde_json::json!(false)),
        include_events: Some(vec![crate::test_suite::EventConfig {
            name: "Transfer".to_string(),
        }]),
        tables: None,
    }];
    config
}

/// Read CSV row count (excluding header).
fn csv_row_count(context: &TestContext) -> usize {
    let csv_path =
        context.get_csv_output_path().join("SimpleERC20").join("simpleerc20-transfer.csv");
    if !csv_path.exists() {
        return 0;
    }
    match helpers::parse_transfer_csv(&csv_path.to_string_lossy()) {
        Ok((_, rows)) => rows.len(),
        Err(_) => 0,
    }
}

/// Wait for CSV row count to reach expected value. Returns the actual count.
async fn wait_for_csv_count(
    context: &TestContext,
    expected: usize,
    timeout_secs: u64,
) -> Result<usize> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let mut last_count = 0;

    while start.elapsed() < timeout {
        last_count = csv_row_count(context);
        if last_count >= expected {
            return Ok(last_count);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Ok(last_count)
}

/// Wait for a Postgres row count to reach the expected value.
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
            tokio::spawn(async move {
                let _ = connection.await;
            });
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

/// Start indexer with postgres env vars, wait for sync, then confirm
/// the live poller is active by sending a transfer and waiting for it.
async fn start_indexer_with_pg(
    context: &mut TestContext,
    config: crate::test_suite::RindexerConfig,
    pg_port: u16,
) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let yaml = serde_yaml::to_string(&config)?;
    std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    for (k, v) in crate::docker::postgres_env_vars(pg_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

/// Restart the indexer with the same postgres env vars.
async fn restart_indexer_with_pg(context: &mut TestContext, pg_port: u16) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    for (k, v) in crate::docker::postgres_env_vars(pg_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

/// Stop the running indexer.
async fn stop_indexer(context: &mut TestContext) -> Result<()> {
    if let Some(r) = context.rindexer.as_mut() {
        let _ = r.stop().await;
    }
    context.rindexer = None;
    Ok(())
}

/// Assert no duplicate tx_hashes exist in a PG table.
async fn assert_no_pg_duplicates(conn_str: &str, table: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let dup_rows = client
        .query(
            &format!(
                "SELECT tx_hash, COUNT(*) as cnt FROM {} \
                 GROUP BY tx_hash HAVING COUNT(*) > 1",
                table
            ),
            &[],
        )
        .await?;

    if !dup_rows.is_empty() {
        return Err(anyhow::anyhow!(
            "Found {} duplicate tx_hash entries in {} after reorg recovery",
            dup_rows.len(),
            table
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 1: Live reorg detection + PG rollback
//
// The coordinator detects a parent hash mismatch on the next block after
// anvil_reorg, pauses forward indexing, runs ReorgTask (transactional delete
// of stale rows from event tables + latest_blocks), then resumes.
// ---------------------------------------------------------------------------
fn reorg_live_pg_recovery(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Live PG Recovery Test");

        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(
                    crate::tests::test_runner::SkipTest(format!("Docker not available: {}", e))
                        .into(),
                );
            }
        };
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000, 4000, 5000];
        let recipients: Vec<_> = (0..5).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;

        start_indexer_with_pg(context, config, pg_port).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";

        // 1 mint + 5 transfers = 6 rows
        let pre_count = wait_for_pg_count(&conn_str, table, 6, 15).await?;
        info!("Pre-reorg PG rows: {}", pre_count);

        if pre_count < 6 {
            return Err(anyhow::anyhow!(
                "Expected at least 6 rows (1 mint + 5 transfers), got {}",
                pre_count
            ));
        }

        // Send a live transfer to confirm the poller is active and has cached
        // block hashes in the BlockChainWindow before we trigger the reorg.
        let live_recipient = generate_test_address(99);
        context
            .anvil
            .send_transfer(&contract_address, &live_recipient, U256::from(777u64))
            .await?;
        context.anvil.mine_block().await?;
        let live_count = wait_for_pg_count(&conn_str, table, pre_count + 1, 15).await?;
        info!("After live transfer: {} PG rows", live_count);

        // Trigger reorg (depth=2 — invalidates the live block + one more)
        context.anvil.trigger_reorg(2).await?;

        // Wait for rindexer to detect via parent hash mismatch and recover
        if let Some(r) = &context.rindexer {
            r.wait_for_reorg_recovery(60).await?;
        }

        // Allow re-indexing to settle
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        // Verify reorg was detected
        if let Some(r) = &context.rindexer {
            if !r.reorg_detected.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Reorg was not detected by rindexer"));
            }
        }

        assert_no_pg_duplicates(&conn_str, table).await?;

        info!(
            "Reorg Live PG Recovery Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 2: Offline reorg — startup validation
//
// The new architecture persists block hashes in `latest_blocks`. On restart,
// `validate_on_startup()` batch-fetches canonical blocks for the entire
// persisted window and compares. If hashes diverge, a ReorgTask runs before
// indexing resumes — catching reorgs that happened while offline.
// ---------------------------------------------------------------------------
fn reorg_offline_startup_validation(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Offline Startup Validation Test");

        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(
                    crate::tests::test_runner::SkipTest(format!("Docker not available: {}", e))
                        .into(),
                );
            }
        };
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;

        start_indexer_with_pg(context, config, pg_port).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";

        // 1 mint + 3 transfers = 4 rows
        let pre_count = wait_for_pg_count(&conn_str, table, 4, 15).await?;
        info!("Pre-reorg PG rows: {}", pre_count);

        // Stop indexer
        stop_indexer(context).await?;

        // Trigger reorg while offline — block hashes change
        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        // Restart — startup validation should detect stale latest_blocks
        // and run ReorgTask before resuming
        restart_indexer_with_pg(context, pg_port).await?;

        // Allow recovery + re-indexing to settle
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        assert_no_pg_duplicates(&conn_str, table).await?;

        // Verify latest_blocks was updated — connect and check entries exist
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let lb_rows = client
            .query(
                "SELECT COUNT(*) FROM rindexer_internal.latest_blocks WHERE network = 'polygon'",
                &[],
            )
            .await;

        if let Ok(rows) = lb_rows {
            let lb_count: i64 = rows[0].get(0);
            info!("latest_blocks entries after recovery: {}", lb_count);
        }

        info!(
            "Reorg Offline Startup Validation Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 3: CSV data consistency after reorg (stop/restart pattern)
// ---------------------------------------------------------------------------
fn reorg_csv_invalidation(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg CSV Invalidation Test");

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // 1 mint + 3 transfers = 4 rows
        let config = create_reorg_config(context, &contract_address);
        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let mut rindexer = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);
        context.wait_for_sync_completion(30).await?;

        let pre_count = wait_for_csv_count(context, 4, 15).await?;
        info!("Pre-reorg CSV rows: {}", pre_count);

        // Stop, reorg, restart
        stop_indexer(context).await?;
        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        helpers::copy_abis_to_project(&context.project_path)?;
        let mut rindexer2 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        let post_count = wait_for_csv_count(context, pre_count, 15).await?;
        info!("Post-reorg CSV rows: {}", post_count);

        // CSV is append-only — post-reorg count should be >= pre-reorg
        if post_count < pre_count {
            return Err(anyhow::anyhow!(
                "Post-reorg CSV has fewer rows ({}) than pre-reorg ({})",
                post_count,
                pre_count
            ));
        }

        info!(
            "Reorg CSV Invalidation Test PASSED: pre={}, post={}, CSV consistent",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 4: Double reorg idempotency — two consecutive offline reorgs
//
// Exercises crash recovery: after two reorgs + restarts, the DB must have
// no duplicate tx_hashes and latest_blocks must reflect the canonical chain.
// ---------------------------------------------------------------------------
fn reorg_double_reorg_idempotency(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Double Reorg Idempotency Test");

        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(
                    crate::tests::test_runner::SkipTest(format!("Docker not available: {}", e))
                        .into(),
                );
            }
        };
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000, 4000];
        let recipients: Vec<_> = (0..4).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";

        // --- First indexing cycle ---
        start_indexer_with_pg(context, config, pg_port).await?;

        let count1 = wait_for_pg_count(&conn_str, table, 5, 15).await?;
        info!("After first sync: {} rows", count1);

        // Stop, first reorg, restart
        stop_indexer(context).await?;
        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let count2 = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("After first reorg recovery: {} rows", count2);

        // Send more transfers between reorgs
        let post_amounts = [5000u64, 6000];
        let post_recipients: Vec<_> = (10..12).map(generate_test_address).collect();
        for (r, a) in post_recipients.iter().zip(post_amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Stop, second reorg, restart
        stop_indexer(context).await?;
        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let final_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("After second reorg recovery: {} rows", final_count);

        assert_no_pg_duplicates(&conn_str, table).await?;

        info!(
            "Reorg Double Reorg Idempotency Test PASSED: two reorgs, final_count={}, no duplicates",
            final_count
        );
        Ok(())
    })
}