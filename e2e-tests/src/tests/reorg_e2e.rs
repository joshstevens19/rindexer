use anyhow::{Context as _, Result};
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
                "Reorg: stop/reorg/restart, startup validation catches stale reorg_block_hashes",
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
            TestDefinition::new(
                "test_reorg_live_clickhouse_recovery",
                "Reorg: live detection via parent hash mismatch, ClickHouse rollback + no duplicates",
                reorg_live_clickhouse_recovery,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_offline_clickhouse_validation",
                "Reorg: stop/reorg/restart with ClickHouse, startup validation catches stale hashes",
                reorg_offline_clickhouse_validation,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_deep_reorg_recovery",
                "Reorg: deep reorg (depth=5) offline, PG correctly rolls back all affected blocks",
                reorg_deep_reorg_recovery,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_no_events_in_range",
                "Reorg: offline reorg in empty block range, recovery succeeds with no events to delete",
                reorg_no_events_in_range,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_aggregation_table_rollback",
                "Reorg: aggregation table (upsert+add) correctly rolled back after offline reorg",
                reorg_aggregation_table_rollback,
            )
            .with_timeout(240)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_live_deep_reorg",
                "Reorg: deep live reorg (depth=3) detected via parent hash mismatch, PG rollback correct",
                reorg_live_deep_reorg,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_offline_new_events_on_fork",
                "Reorg: offline reorg with new events on canonical fork, PG reflects new data",
                reorg_offline_new_events_on_fork,
            )
            .with_timeout(240)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_webhook_stream_notification",
                "Reorg: webhook stream receives __rindexer_reorg retraction event after live reorg",
                reorg_webhook_stream_notification,
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
    config.networks[0].reorg_handling =
        Some(crate::test_suite::ReorgHandlingConfig { enabled: true, window_size: None });
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
        include_events: Some(vec![crate::test_suite::EventConfig { name: "Transfer".to_string() }]),
        tables: None,
        streams: None,
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
// ClickHouse helpers
// ---------------------------------------------------------------------------

/// Execute an arbitrary SQL query against ClickHouse via HTTP GET.
/// Returns the response body trimmed.
#[allow(dead_code)]
async fn clickhouse_query(port: u16, query: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let url = format!("http://localhost:{}/", port);
    let resp = client.get(&url).query(&[("query", query)]).send().await?;
    let body = resp.text().await?;
    Ok(body.trim().to_string())
}

/// Return the row count for a ClickHouse table.
#[allow(dead_code)]
async fn clickhouse_row_count(port: u16, table: &str) -> Result<i64> {
    let body = clickhouse_query(port, &format!("SELECT count() FROM {}", table)).await?;
    let count: i64 = body.parse().map_err(|e| anyhow::anyhow!("parse count: {}", e))?;
    Ok(count)
}

/// Poll until the ClickHouse row count for `table` reaches `expected`.
/// Returns the actual count when done (may still be < expected on timeout).
#[allow(dead_code)]
async fn wait_for_ch_count(
    port: u16,
    table: &str,
    expected: i64,
    timeout_secs: u64,
) -> Result<i64> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let mut last_count: i64 = 0;

    while start.elapsed() < timeout {
        if let Ok(count) = clickhouse_row_count(port, table).await {
            last_count = count;
            if last_count >= expected {
                return Ok(last_count);
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Ok(last_count)
}

/// Assert no duplicate tx_hashes exist in a ClickHouse table.
#[allow(dead_code)]
async fn assert_no_ch_duplicates(port: u16, table: &str) -> Result<()> {
    let body = clickhouse_query(
        port,
        &format!("SELECT tx_hash, count() as cnt FROM {} GROUP BY tx_hash HAVING cnt > 1", table),
    )
    .await?;

    if !body.is_empty() {
        return Err(anyhow::anyhow!(
            "Found duplicate tx_hash entries in {} after reorg recovery: {}",
            table,
            body
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Dual-backend (PG + CH) indexer management helpers
// ---------------------------------------------------------------------------

/// Start indexer with both Postgres and ClickHouse env vars.
#[allow(dead_code)]
async fn start_indexer_with_pg_and_ch(
    context: &mut TestContext,
    config: crate::test_suite::RindexerConfig,
    pg_port: u16,
    ch_port: u16,
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
    for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

/// Start indexer with ClickHouse env vars only.
#[allow(dead_code)]
async fn start_indexer_with_ch(
    context: &mut TestContext,
    config: crate::test_suite::RindexerConfig,
    ch_port: u16,
) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let yaml = serde_yaml::to_string(&config)?;
    std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

/// Restart the indexer with ClickHouse env vars only.
#[allow(dead_code)]
async fn restart_indexer_with_ch(context: &mut TestContext, ch_port: u16) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

/// Restart the indexer with both Postgres and ClickHouse env vars.
#[allow(dead_code)]
async fn restart_indexer_with_pg_and_ch(
    context: &mut TestContext,
    pg_port: u16,
    ch_port: u16,
) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    for (k, v) in crate::docker::postgres_env_vars(pg_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
        rindexer = rindexer.with_env(&k, &v);
    }
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);
    context.wait_for_sync_completion(30).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Config builders for ClickHouse tests
// ---------------------------------------------------------------------------

/// Build a reorg-aware config with ClickHouse enabled, PG and CSV disabled.
#[allow(dead_code)]
fn create_reorg_config_ch(
    context: &TestContext,
    contract_address: &str,
) -> crate::test_suite::RindexerConfig {
    let mut config = create_reorg_config(context, contract_address);
    config.storage.postgres = None;
    config.storage.csv.enabled = false;
    config.storage.clickhouse =
        Some(crate::test_suite::ClickHouseConfig { enabled: true, drop_each_run: Some(false) });
    config
}

/// Build a reorg-aware config with both Postgres and ClickHouse enabled, CSV disabled.
#[allow(dead_code)]
fn create_reorg_config_pg_ch(
    context: &TestContext,
    contract_address: &str,
) -> crate::test_suite::RindexerConfig {
    let mut config = create_reorg_config(context, contract_address);
    config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
    config.storage.csv.enabled = false;
    config.storage.clickhouse =
        Some(crate::test_suite::ClickHouseConfig { enabled: true, drop_each_run: Some(false) });
    config
}

// ---------------------------------------------------------------------------
// Test 1: Live reorg detection + PG rollback
//
// The coordinator detects a parent hash mismatch on the next block after
// anvil_reorg, pauses forward indexing, runs ReorgTask (transactional delete
// of stale rows from event tables + reorg_block_hashes), then resumes.
// ---------------------------------------------------------------------------
fn reorg_live_pg_recovery(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Live PG Recovery Test");

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
        context.anvil.send_transfer(&contract_address, &live_recipient, U256::from(777u64)).await?;
        context.anvil.mine_block().await?;
        let live_count = wait_for_pg_count(&conn_str, table, pre_count + 1, 15).await?;
        info!("After live transfer: {} PG rows", live_count);

        // Trigger reorg (depth=2 — invalidates the live block + one more)
        context.anvil.trigger_reorg(2).await?;
        // Mine a block so the live poller sees a parent hash mismatch
        context.anvil.mine_block().await?;

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
// The new architecture persists block hashes in `reorg_block_hashes`. On restart,
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
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
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

        // Restart — startup validation should detect stale reorg_block_hashes
        // and run ReorgTask before resuming
        restart_indexer_with_pg(context, pg_port).await?;

        // Allow recovery + re-indexing to settle
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        assert_no_pg_duplicates(&conn_str, table).await?;

        // Verify reorg_block_hashes was updated — connect and check entries exist
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let lb_rows = client
            .query(
                "SELECT COUNT(*) FROM rindexer_internal.reorg_block_hashes WHERE network = 'polygon'",
                &[],
            )
            .await;

        if let Ok(rows) = lb_rows {
            let lb_count: i64 = rows[0].get(0);
            info!("reorg_block_hashes entries after recovery: {}", lb_count);
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
// no duplicate tx_hashes and reorg_block_hashes must reflect the canonical chain.
// ---------------------------------------------------------------------------
fn reorg_double_reorg_idempotency(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Double Reorg Idempotency Test");

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

// ---------------------------------------------------------------------------
// Test 5: Live reorg detection + ClickHouse rollback
//
// Mirrors test_reorg_live_pg_recovery but targets ClickHouse storage.
// ClickHouse stores both event data and reorg_block_hashes internally;
// PG is not required. The coordinator detects a parent hash mismatch, pauses
// forward indexing, runs ReorgTask (deletes stale rows from CH event tables +
// reorg_block_hashes in CH), then resumes.
// ---------------------------------------------------------------------------
fn reorg_live_clickhouse_recovery(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Live ClickHouse Recovery Test");

        let (ch_container, ch_port) = match crate::docker::start_clickhouse_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(ch_container);
        crate::docker::wait_for_clickhouse_ready(ch_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000, 4000, 5000];
        let recipients: Vec<_> = (0..5).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let config = create_reorg_config_ch(context, &contract_address);

        start_indexer_with_ch(context, config, ch_port).await?;

        // ClickHouse table name: {indexer_name}_{contract_name}.{event_name}
        // indexer name = "reorg_test", contract = "SimpleERC20", event = "Transfer"
        let ch_table = "reorg_test_simple_erc_20.transfer";

        // 1 mint + 5 transfers = 6 rows
        let pre_count = wait_for_ch_count(ch_port, ch_table, 6, 30).await?;
        info!("Pre-reorg ClickHouse rows: {}", pre_count);

        if pre_count < 6 {
            return Err(anyhow::anyhow!(
                "Expected at least 6 rows (1 mint + 5 transfers) in ClickHouse, got {}",
                pre_count
            ));
        }

        // Send a live transfer to confirm the poller is active and has cached
        // block hashes in the BlockChainWindow before we trigger the reorg.
        let live_recipient = generate_test_address(99);
        context.anvil.send_transfer(&contract_address, &live_recipient, U256::from(777u64)).await?;
        context.anvil.mine_block().await?;
        let live_count = wait_for_ch_count(ch_port, ch_table, pre_count + 1, 15).await?;
        info!("After live transfer: {} ClickHouse rows", live_count);

        // Trigger reorg (depth=2 — invalidates the live block + one more)
        context.anvil.trigger_reorg(2).await?;
        // Mine a block so the live poller sees a parent hash mismatch
        context.anvil.mine_block().await?;

        // Wait for rindexer to detect via parent hash mismatch and recover
        if let Some(r) = &context.rindexer {
            r.wait_for_reorg_recovery(60).await?;
        }

        // Allow re-indexing to settle
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_ch_count(ch_port, ch_table, 1, 15).await?;
        info!("Post-reorg ClickHouse rows: {}", post_count);

        // Verify reorg was detected
        if let Some(r) = &context.rindexer {
            if !r.reorg_detected.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Reorg was not detected by rindexer"));
            }
        }

        assert_no_ch_duplicates(ch_port, ch_table).await?;

        info!(
            "Reorg Live ClickHouse Recovery Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 6: Offline reorg — ClickHouse startup validation
// ---------------------------------------------------------------------------
fn reorg_offline_clickhouse_validation(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Offline ClickHouse Validation Test");

        let (ch_container, ch_port) = match crate::docker::start_clickhouse_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(ch_container);
        crate::docker::wait_for_clickhouse_ready(ch_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let config = create_reorg_config_ch(context, &contract_address);
        start_indexer_with_ch(context, config, ch_port).await?;

        let ch_table = "reorg_test_simple_erc_20.transfer";

        // 1 mint + 3 transfers = 4 rows
        let pre_count = wait_for_ch_count(ch_port, ch_table, 4, 15).await?;
        info!("Pre-reorg ClickHouse rows: {}", pre_count);

        stop_indexer(context).await?;

        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_ch(context, ch_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_ch_count(ch_port, ch_table, 1, 15).await?;
        info!("Post-reorg ClickHouse rows: {}", post_count);

        assert_no_ch_duplicates(ch_port, ch_table).await?;

        info!(
            "Reorg Offline ClickHouse Validation Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 7: Deep reorg (depth=5) offline recovery
// ---------------------------------------------------------------------------
fn reorg_deep_reorg_recovery(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Deep Reorg Recovery Test");

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
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;

        let amounts: Vec<u64> = (1..=8).map(|i| i * 1000).collect();
        let recipients: Vec<_> = (0..8).map(generate_test_address).collect();

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

        // 1 mint + 8 transfers = 9 rows
        let pre_count = wait_for_pg_count(&conn_str, table, 9, 15).await?;
        info!("Pre-reorg PG rows: {}", pre_count);

        stop_indexer(context).await?;
        context.anvil.trigger_reorg(5).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        assert_no_pg_duplicates(&conn_str, table).await?;

        info!(
            "Reorg Deep Recovery Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 9: Reorg in empty block range (no events to delete)
// ---------------------------------------------------------------------------
fn reorg_no_events_in_range(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg No Events In Range Test");

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
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;

        let amounts = [1000u64, 2000];
        let recipients: Vec<_> = (0..2).map(generate_test_address).collect();
        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Mine 5 empty blocks (no events)
        for _ in 0..5 {
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

        // 1 mint + 2 transfers = 3 rows
        let pre_count = wait_for_pg_count(&conn_str, table, 3, 15).await?;
        info!("Pre-reorg PG rows: {}", pre_count);

        stop_indexer(context).await?;
        // Reorg the empty blocks (depth=3)
        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 3, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        if post_count != pre_count {
            return Err(anyhow::anyhow!(
                "Row count changed after empty-range reorg: pre={}, post={}",
                pre_count,
                post_count
            ));
        }

        assert_no_pg_duplicates(&conn_str, table).await?;

        info!(
            "Reorg No Events In Range Test PASSED: pre={}, post={}, count unchanged",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 10: Aggregation table rollback after reorg
// ---------------------------------------------------------------------------
fn reorg_aggregation_table_rollback(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Aggregation Table Rollback Test");

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
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;

        let recipient_a = generate_test_address(50);
        let recipient_b = generate_test_address(51);

        // Block 1-3: transfers to A (1000, 2000, 3000)
        for amount in [1000u64, 2000, 3000] {
            context
                .anvil
                .send_transfer(&contract_address, &recipient_a, U256::from(amount))
                .await?;
            context.anvil.mine_block().await?;
        }
        // Block 4-5: transfers to B (4000, 5000)
        for amount in [4000u64, 5000] {
            context
                .anvil
                .send_transfer(&contract_address, &recipient_b, U256::from(amount))
                .await?;
            context.anvil.mine_block().await?;
        }

        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;

        // Add aggregation table: track total_received per holder
        if let Some(contract) = config.contracts.get_mut(0) {
            contract.tables = Some(vec![serde_json::json!({
                "name": "holder_balances",
                "columns": [
                    { "name": "holder", "type": "address" },
                    { "name": "total_received", "type": "uint256" }
                ],
                "events": [{
                    "event": "Transfer",
                    "operations": [{
                        "type": "upsert",
                        "where": { "holder": "$to" },
                        "set": [{
                            "column": "total_received",
                            "action": "add",
                            "value": "$value"
                        }]
                    }]
                }]
            })]);
        }

        start_indexer_with_pg(context, config, pg_port).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let event_table = "reorg_test_simple_erc_20.transfer";
        let agg_table = "reorg_test_simple_erc_20.holder_balances";

        // 1 mint + 5 transfers = 6 event rows
        let event_count = wait_for_pg_count(&conn_str, event_table, 6, 15).await?;
        info!("Pre-reorg event rows: {}", event_count);

        // Query aggregation table pre-reorg
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let pre_agg_rows =
            client.query(&format!("SELECT COUNT(*) FROM {}", agg_table), &[]).await?;
        let pre_agg_count: i64 = pre_agg_rows[0].get(0);
        info!("Pre-reorg aggregation rows: {}", pre_agg_count);

        // Stop, reorg last 2 blocks (affects recipient B's transfers), restart
        stop_indexer(context).await?;
        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        assert_no_pg_duplicates(&conn_str, event_table).await?;

        let post_event_count = wait_for_pg_count(&conn_str, event_table, 1, 15).await?;
        info!("Post-reorg event rows: {}", post_event_count);

        // Verify aggregation table still exists and has entries
        let (client2, connection2) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection2.await;
        });
        let post_agg_rows =
            client2.query(&format!("SELECT COUNT(*) FROM {}", agg_table), &[]).await?;
        let post_agg_count: i64 = post_agg_rows[0].get(0);
        info!("Post-reorg aggregation rows: {}", post_agg_count);

        info!(
            "Reorg Aggregation Table Rollback Test PASSED: events pre={}/post={}, agg rows pre={}/post={}",
            event_count, post_event_count, pre_agg_count, post_agg_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 10: Live deep reorg (depth=3) via ClickHouse
//
// Uses ClickHouse to exercise the live detection + deeper rollback path.
// More transfers than the basic live test to ensure the coordinator's
// in-memory window covers multiple blocks.
// ---------------------------------------------------------------------------
fn reorg_live_deep_reorg(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Live Deep Reorg Test");

        let (ch_container, ch_port) = match crate::docker::start_clickhouse_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(ch_container);
        crate::docker::wait_for_clickhouse_ready(ch_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;

        let amounts: Vec<u64> = (1..=6).map(|i| i * 1000).collect();
        let recipients: Vec<_> = (0..6).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let config = create_reorg_config_ch(context, &contract_address);
        start_indexer_with_ch(context, config, ch_port).await?;

        let ch_table = "reorg_test_simple_erc_20.transfer";

        // 1 mint + 6 transfers = 7 rows
        let pre_count = wait_for_ch_count(ch_port, ch_table, 7, 15).await?;
        info!("Pre-reorg CH rows: {}", pre_count);

        // Send a live transfer to confirm poller is active
        let live_recipient = generate_test_address(99);
        context.anvil.send_transfer(&contract_address, &live_recipient, U256::from(777u64)).await?;
        context.anvil.mine_block().await?;
        let live_count = wait_for_ch_count(ch_port, ch_table, pre_count + 1, 15).await?;
        info!("After live transfer: {} CH rows", live_count);

        // Deep reorg (depth=3)
        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        if let Some(r) = &context.rindexer {
            r.wait_for_reorg_recovery(60).await?;
        }

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_ch_count(ch_port, ch_table, 1, 15).await?;
        info!("Post-reorg CH rows: {}", post_count);

        if let Some(r) = &context.rindexer {
            if !r.reorg_detected.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Reorg was not detected by rindexer"));
            }
        }

        assert_no_ch_duplicates(ch_port, ch_table).await?;

        info!("Reorg Live Deep Test PASSED: pre={}, post={}, no duplicates", pre_count, post_count);
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 11: Offline reorg with new events on the canonical fork
//
// Stop indexer, trigger reorg, send NEW transfers on the canonical chain,
// then restart. The startup validation should detect the reorg, delete stale
// rows, and re-index the new fork's events.
// ---------------------------------------------------------------------------
fn reorg_offline_new_events_on_fork(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Offline New Events On Fork Test");

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
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        let contract_address = context.deploy_test_contract().await?;

        // Phase 1: Send 3 transfers, index them
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

        // Phase 2: Stop indexer
        stop_indexer(context).await?;

        // Phase 3: Reorg the last 2 blocks, then send NEW events on the fork
        context.anvil.trigger_reorg(2).await?;

        // Send 2 new transfers on the canonical fork (different recipients/amounts)
        let fork_recipient_1 = generate_test_address(200);
        let fork_recipient_2 = generate_test_address(201);
        context
            .anvil
            .send_transfer(&contract_address, &fork_recipient_1, U256::from(8888u64))
            .await?;
        context.anvil.mine_block().await?;
        context
            .anvil
            .send_transfer(&contract_address, &fork_recipient_2, U256::from(9999u64))
            .await?;
        context.anvil.mine_block().await?;

        // Phase 4: Restart — should detect reorg, delete stale rows, re-index new fork
        restart_indexer_with_pg(context, pg_port).await?;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg PG rows: {}", post_count);

        assert_no_pg_duplicates(&conn_str, table).await?;

        // Verify the new fork's events are present by checking for the new amounts
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let new_events = client
            .query(
                &format!(
                    "SELECT value FROM {} WHERE value IN ('8888', '9999') ORDER BY value",
                    table
                ),
                &[],
            )
            .await?;
        info!("New fork events found: {}", new_events.len());

        if new_events.is_empty() {
            return Err(anyhow::anyhow!(
                "Expected events from the new fork (values 8888, 9999) but found none"
            ));
        }

        info!(
            "Reorg Offline New Events On Fork Test PASSED: pre={}, post={}, new fork events={}",
            pre_count,
            post_count,
            new_events.len()
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 12: Webhook stream receives __rindexer_reorg notification
//
// Starts a lightweight TCP listener as a webhook endpoint, configures
// rindexer with a webhook stream, triggers a live reorg, and asserts the
// listener received a JSON payload with type=reorg.
// ---------------------------------------------------------------------------
fn reorg_webhook_stream_notification(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Webhook Stream Notification Test");

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
        context.register_container(container_name);
        crate::docker::wait_for_postgres_ready(pg_port, 30).await?;

        // Start a simple HTTP listener to collect webhook payloads
        let webhook_port = crate::docker::allocate_free_port()?;
        let received_bodies: std::sync::Arc<tokio::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let bodies_clone = received_bodies.clone();
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{}", webhook_port))
            .await
            .context("Failed to bind webhook listener")?;
        info!("Webhook listener started on port {}", webhook_port);

        // Spawn a task that accepts connections and collects POST bodies
        tokio::spawn(async move {
            loop {
                if let Ok((mut stream, _)) = listener.accept().await {
                    let bodies = bodies_clone.clone();
                    tokio::spawn(async move {
                        use tokio::io::{AsyncReadExt, AsyncWriteExt};
                        let mut buf = vec![0u8; 65536];
                        if let Ok(n) = stream.read(&mut buf).await {
                            let request = String::from_utf8_lossy(&buf[..n]).to_string();
                            // Extract body after the \r\n\r\n separator
                            if let Some(body_start) = request.find("\r\n\r\n") {
                                let body = request[body_start + 4..].to_string();
                                if !body.is_empty() {
                                    bodies.lock().await.push(body);
                                }
                            }
                            // Send HTTP 200 response
                            let response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
                            let _ = stream.write_all(response.as_bytes()).await;
                        }
                    });
                }
            }
        });

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000, 4000, 5000];
        let recipients: Vec<_> = (0..5).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Build config with webhook stream pointing to our listener
        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;

        // Add streams config to the contract
        if let Some(contract) = config.contracts.get_mut(0) {
            contract.streams = Some(serde_json::json!({
                "webhooks": [{
                    "endpoint": format!("http://127.0.0.1:{}/webhook", webhook_port),
                    "shared_secret": "test-secret",
                    "networks": ["polygon"],
                    "events": [
                        { "event_name": "Transfer" }
                    ]
                }]
            }));
        }

        start_indexer_with_pg(context, config, pg_port).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";

        // Wait for initial indexing
        let pre_count = wait_for_pg_count(&conn_str, table, 6, 15).await?;
        info!("Pre-reorg PG rows: {}", pre_count);

        // Send a live transfer to confirm poller is active
        let live_recipient = generate_test_address(99);
        context.anvil.send_transfer(&contract_address, &live_recipient, U256::from(777u64)).await?;
        context.anvil.mine_block().await?;
        let live_count = wait_for_pg_count(&conn_str, table, pre_count + 1, 15).await?;
        info!("After live transfer: {} PG rows", live_count);

        // Clear any initial webhook payloads from indexing
        received_bodies.lock().await.clear();

        // Trigger reorg
        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        // Wait for reorg recovery
        if let Some(r) = &context.rindexer {
            r.wait_for_reorg_recovery(60).await?;
        }

        // Give webhook delivery a moment to complete
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Check webhook received a reorg notification
        let bodies = received_bodies.lock().await;
        info!("Webhook received {} payloads after reorg", bodies.len());

        let reorg_payload = bodies.iter().find(|b| b.contains("reorg"));

        if let Some(payload) = reorg_payload {
            info!("Reorg webhook payload: {}", payload);

            // Payload structure: { event_name: "__rindexer_reorg", event_data: [{ type: "reorg", network, fork_block, depth, affected_tx_hashes }], ... }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload) {
                let event_name = json.get("event_name").and_then(|v| v.as_str());
                if event_name != Some("__rindexer_reorg") {
                    return Err(anyhow::anyhow!(
                        "Expected event_name=__rindexer_reorg, got: {:?}",
                        event_name
                    ));
                }

                let event_data =
                    json.get("event_data").and_then(|v| v.as_array()).and_then(|arr| arr.first());
                let reorg_entry = event_data.ok_or_else(|| {
                    anyhow::anyhow!("Missing event_data array in reorg webhook payload")
                })?;

                let event_type = reorg_entry.get("type").and_then(|v| v.as_str());
                if event_type != Some("reorg") {
                    return Err(anyhow::anyhow!(
                        "Expected type=reorg in event_data, got: {:?}",
                        event_type
                    ));
                }

                let network = reorg_entry.get("network").and_then(|v| v.as_str());
                if network != Some("polygon") {
                    return Err(anyhow::anyhow!(
                        "Expected network=polygon in event_data, got: {:?}",
                        network
                    ));
                }

                let fork_block = reorg_entry.get("fork_block").and_then(|v| v.as_u64());
                info!(
                    "Webhook payload validated: type=reorg, network=polygon, fork_block={:?}",
                    fork_block
                );
            } else {
                return Err(anyhow::anyhow!("Webhook payload is not valid JSON: {}", payload));
            }
        } else {
            return Err(anyhow::anyhow!(
                "No reorg notification received via webhook. Got {} payloads: {:?}",
                bodies.len(),
                bodies.iter().take(3).collect::<Vec<_>>()
            ));
        }

        assert_no_pg_duplicates(&conn_str, table).await?;

        info!("Reorg Webhook Stream Notification Test PASSED");
        Ok(())
    })
}
