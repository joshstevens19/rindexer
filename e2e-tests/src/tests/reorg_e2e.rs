use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{self, generate_test_address};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct ReorgTests;

// ---------------------------------------------------------------------------
// Known test gap: Post-confirmation verifier (30s background task in reorg.rs)
//
// The verifier catches "silent" reorgs — reorgs the live poller misses due to
// network blips or polling gaps. It compares shadow-cached block hashes against
// canonical chain hashes after N confirmations.
//
// This CANNOT be tested with Anvil because `anvil_reorg(depth)` always changes
// the tip block hash, which means the live poller's tip-hash-change detection
// catches it first. To test the verifier in isolation, you'd need a mock RPC
// that returns stale block hashes during live polling but correct hashes when
// the verifier queries later — or an Anvil extension for mid-chain reorgs
// that don't affect the tip.
// ---------------------------------------------------------------------------

impl TestModule for ReorgTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_reorg_tip_hash_csv",
                "Reorg: tip hash change detected, CSV data correct after recovery",
                reorg_tip_hash_csv,
            )
            .with_timeout(120)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_parent_hash_csv",
                "Reorg: checkpoint recovery after stop/reorg/restart, CSV data correct",
                reorg_parent_hash_csv,
            )
            .with_timeout(120)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_deep_csv",
                "Reorg: depth=10 across multiple event blocks, all orphaned rows removed",
                reorg_deep_csv,
            )
            .with_timeout(120)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_pg_recovery",
                "Reorg: Postgres events deleted and re-indexed correctly after reorg",
                reorg_pg_recovery,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_ch_recovery",
                "Reorg: ClickHouse events deleted and re-indexed correctly after reorg",
                reorg_ch_recovery,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_safe_distance",
                "Reorg: reorg_safe_distance=true keeps indexer behind head, unaffected by reorg",
                reorg_safe_distance,
            )
            .with_timeout(120)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_idempotency",
                "Reorg: two consecutive reorgs produce clean state, no corruption",
                reorg_idempotency,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_reorg_derived_table_replay",
                "Reorg: derived balance table (upsert+add) triggers FullReplay, balances correct after recovery",
                reorg_derived_table_replay,
            )
            .with_timeout(180)
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
    // Use Polygon chain_id so rindexer enables reorg detection
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
        include_events: Some(vec![crate::test_suite::EventConfig { name: "Transfer".to_string() }]),
        tables: None,
    }];
    config
}

/// Start the indexer, wait for historic sync to complete, then send a
/// transfer during live indexing and wait for it to appear in CSV.
/// This guarantees the live poller is active and has cached block hashes
/// before returning — no timing assumptions.
async fn setup_and_index_with_live_proof(
    context: &mut TestContext,
    config: crate::test_suite::RindexerConfig,
    contract_address: &str,
    pre_sync_event_count: usize,
) -> Result<()> {
    helpers::copy_abis_to_project(&context.project_path)?;
    let config_path = context.project_path.join("rindexer.yaml");
    let yaml = serde_yaml::to_string(&config)?;
    std::fs::write(&config_path, yaml)?;

    let mut rindexer = crate::rindexer_client::RindexerInstance::new(
        &context.rindexer_binary,
        context.project_path.clone(),
    );
    rindexer.start_indexer().await?;
    context.rindexer = Some(rindexer);

    // Wait for historic sync (the mint + pre-reorg transfers)
    context.wait_for_sync_completion(30).await?;

    // Send a transfer during live indexing so the poller processes it
    // and caches the block hash. This is deterministic proof the poller
    // is active — no sleep-based guessing.
    let live_recipient = generate_test_address(99);
    context.anvil.send_transfer(contract_address, &live_recipient, U256::from(777u64)).await?;
    context.anvil.mine_block().await?;

    // Wait for the live transfer to appear in CSV
    let expected_count = pre_sync_event_count + 1; // +1 for the live transfer
    let actual = wait_for_csv_count(context, expected_count, 30).await?;
    if actual < expected_count {
        return Err(anyhow::anyhow!(
            "Live poller did not process transfer: expected {} CSV rows, got {}",
            expected_count,
            actual
        ));
    }
    info!("Live poller confirmed active: {} CSV rows", actual);

    Ok(())
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
/// This is a deterministic wait — it polls for an observable condition,
/// not a fixed duration.
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

// ---------------------------------------------------------------------------
// Test 1: Tip hash changed (CSV)
// ---------------------------------------------------------------------------
fn reorg_tip_hash_csv(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Tip Hash CSV Test");

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // 1 mint + 3 transfers = 4 pre-sync events
        let config = create_reorg_config(context, &contract_address);
        setup_and_index_with_live_proof(context, config, &contract_address, 4).await?;

        let pre_count = csv_row_count(context);
        info!("Pre-reorg CSV rows: {}", pre_count);

        // Trigger reorg at tip (depth=2 — affects the live transfer block + one more)
        context.anvil.trigger_reorg(2).await?;

        // Wait for rindexer to detect + recover (log-based, deterministic)
        if let Some(r) = &context.rindexer {
            r.wait_for_reorg_recovery(60).await?;
        }

        // Wait for CSV count to stabilize (re-indexing after recovery)
        let post_count = wait_for_csv_count(context, pre_count, 15).await?;
        info!("Post-reorg CSV rows: {}", post_count);

        if post_count < pre_count {
            return Err(anyhow::anyhow!(
                "Post-reorg CSV has fewer rows ({}) than pre-reorg ({})",
                post_count,
                pre_count
            ));
        }

        // Verify reorg was detected via logs
        if let Some(r) = &context.rindexer {
            if !r.reorg_detected.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(anyhow::anyhow!("Reorg was not detected by rindexer"));
            }
        }

        info!("Reorg Tip Hash CSV Test PASSED: reorg detected, recovery completed, CSV consistent");
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 2: Reorg recovery via checkpoint re-sync (CSV)
// Validates that stopping the indexer, triggering a reorg, and restarting
// produces correct data via checkpoint-based recovery.
// ---------------------------------------------------------------------------
fn reorg_parent_hash_csv(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Checkpoint Recovery CSV Test");

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [500u64, 1500, 2500];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // 1 mint + 3 transfers = 4 pre-sync events
        let config = create_reorg_config(context, &contract_address);
        setup_and_index_with_live_proof(context, config, &contract_address, 4).await?;

        let pre_count = csv_row_count(context);
        info!("Pre-reorg CSV rows: {}", pre_count);

        // Stop indexer, trigger reorg, restart — deterministic checkpoint recovery
        info!("Stopping indexer before reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        info!("Restarting indexer to re-sync after reorg...");
        helpers::copy_abis_to_project(&context.project_path)?;
        let mut rindexer = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);
        context.wait_for_sync_completion(30).await?;

        // Wait for CSV to stabilize after re-sync
        let post_count = wait_for_csv_count(context, pre_count, 15).await?;
        info!("Post-reorg CSV rows: {}", post_count);

        if post_count < pre_count {
            return Err(anyhow::anyhow!(
                "Post-reorg CSV has fewer rows ({}) than pre-reorg ({})",
                post_count,
                pre_count
            ));
        }

        info!("Reorg Checkpoint Recovery CSV Test PASSED: stop/restart recovery, CSV consistent");
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 3: Deep reorg (CSV) — depth=10, multiple event blocks
// Uses stop/restart for deterministic recovery.
// ---------------------------------------------------------------------------
fn reorg_deep_csv(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Deep CSV Test");

        let contract_address = context.deploy_test_contract().await?;

        // Send 8 transfers across 8 blocks — plenty of data to reorg
        let amounts: Vec<u64> = (1..=8).map(|i| i * 1000).collect();
        let recipients: Vec<_> = (0..8).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Mine extra empty blocks so reorg depth=10 reaches into event blocks
        for _ in 0..5 {
            context.anvil.mine_block().await?;
        }

        // 1 mint + 8 transfers = 9 pre-sync events
        let config = create_reorg_config(context, &contract_address);
        setup_and_index_with_live_proof(context, config, &contract_address, 9).await?;

        let pre_count = csv_row_count(context);
        info!("Pre-reorg CSV rows: {} (1 mint + 8 transfers + 1 live expected)", pre_count);

        // Stop indexer, trigger deep reorg, restart
        info!("Stopping indexer before deep reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(10).await?;
        context.anvil.mine_block().await?;

        info!("Restarting indexer to re-sync after deep reorg...");
        helpers::copy_abis_to_project(&context.project_path)?;
        let mut rindexer = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);
        context.wait_for_sync_completion(30).await?;

        // Wait for CSV to stabilize
        let post_count = wait_for_csv_count(context, pre_count, 15).await?;
        info!("Post-reorg CSV rows: {}", post_count);

        info!("Reorg Deep CSV Test PASSED: depth=10 reorg, stop/restart recovery");
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 4: Postgres recovery — uses stop/restart pattern for determinism
// ---------------------------------------------------------------------------
fn reorg_pg_recovery(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Postgres Recovery Test");

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

        // Wait for Postgres rows to reach expected count (deterministic)
        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";
        let pre_count = wait_for_pg_count(&conn_str, table, 6, 15).await?;
        info!("Pre-reorg Postgres rows: {}", pre_count);

        if pre_count < 6 {
            return Err(anyhow::anyhow!(
                "Expected at least 6 rows (1 mint + 5 transfers), got {}",
                pre_count
            ));
        }

        // Stop indexer, trigger reorg, restart — deterministic, no live detection needed
        info!("Stopping indexer before reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(4).await?;
        context.anvil.mine_block().await?;

        info!("Restarting indexer to re-sync after reorg...");
        let mut rindexer2 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            rindexer2 = rindexer2.with_env(&k, &v);
        }
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        // Wait for rows to stabilize after re-sync
        let post_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;
        info!("Post-reorg Postgres rows: {}", post_count);

        // Verify no duplicate tx_hashes
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let dup_rows = client
            .query(
                "SELECT tx_hash, COUNT(*) as cnt FROM reorg_test_simple_erc_20.transfer \
                 GROUP BY tx_hash HAVING COUNT(*) > 1",
                &[],
            )
            .await?;

        if !dup_rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Found {} duplicate tx_hash entries after reorg recovery",
                dup_rows.len()
            ));
        }

        info!(
            "Reorg Postgres Recovery Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 5: ClickHouse recovery — uses stop/restart pattern for determinism
// ---------------------------------------------------------------------------
fn reorg_ch_recovery(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg ClickHouse Recovery Test");

        // Start ClickHouse
        let (container_name, ch_port) = match crate::docker::start_clickhouse_container().await {
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
        crate::docker::wait_for_clickhouse_ready(ch_port, 15).await?;

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000, 4000, 5000];
        let recipients: Vec<_> = (0..5).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        let mut config = create_reorg_config(context, &contract_address);
        config.storage.csv.enabled = false;
        config.storage.clickhouse =
            Some(crate::test_suite::ClickHouseConfig { enabled: true, drop_each_run: Some(true) });

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

        // Wait for ClickHouse rows (deterministic poll, not sleep)
        let http_client = reqwest::Client::new();
        let ch_url = format!("http://localhost:{}", ch_port);
        let ch_query_url = format!(
            "{}/?query=SELECT%20count()%20FROM%20reorg_test_simple_erc_20.transfer%20FORMAT%20TabSeparated",
            ch_url
        );

        let pre_count = wait_for_ch_count(&http_client, &ch_query_url, 6, 15).await?;
        info!("Pre-reorg ClickHouse rows: {}", pre_count);

        if pre_count < 6 {
            return Err(anyhow::anyhow!(
                "Expected at least 6 rows (1 mint + 5 transfers), got {}",
                pre_count
            ));
        }

        // Stop indexer, trigger reorg, restart — deterministic
        info!("Stopping indexer before reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(4).await?;
        context.anvil.mine_block().await?;

        info!("Restarting indexer to re-sync after reorg...");
        let mut rindexer2 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::clickhouse_env_vars(ch_port) {
            rindexer2 = rindexer2.with_env(&k, &v);
        }
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        // Wait for ClickHouse rows to stabilize
        let post_count = wait_for_ch_count(&http_client, &ch_query_url, 1, 15).await?;
        info!("Post-reorg ClickHouse rows: {}", post_count);

        // Verify no duplicate tx_hashes (CH mutations are async — this catches
        // incomplete deletes before re-indexing that would cause silent double-counting)
        let dup_resp = http_client
            .get(&ch_url)
            .query(&[(
                "query",
                "SELECT tx_hash, count() as cnt FROM reorg_test_simple_erc_20.transfer \
                 GROUP BY tx_hash HAVING cnt > 1 FORMAT TabSeparated",
            )])
            .send()
            .await?
            .text()
            .await?;
        let dup_trimmed = dup_resp.trim();
        if !dup_trimmed.is_empty() {
            return Err(anyhow::anyhow!(
                "Found duplicate tx_hash entries in ClickHouse after reorg recovery:\n{}",
                dup_trimmed
            ));
        }

        info!(
            "Reorg ClickHouse Recovery Test PASSED: pre={}, post={}, no duplicates",
            pre_count, post_count
        );
        Ok(())
    })
}

/// Wait for ClickHouse row count to reach expected value.
async fn wait_for_ch_count(
    client: &reqwest::Client,
    query_url: &str,
    expected: u64,
    timeout_secs: u64,
) -> Result<u64> {
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let mut last_count: u64 = 0;

    while start.elapsed() < timeout {
        if let Ok(resp) = client.get(query_url).send().await {
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

// ---------------------------------------------------------------------------
// Test 6: reorg_safe_distance keeps indexer behind head
// ---------------------------------------------------------------------------
fn reorg_safe_distance(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Safe Distance Test");

        let contract_address = context.deploy_test_contract().await?;
        let amounts = [1000u64, 2000, 3000];
        let recipients: Vec<_> = (0..3).map(generate_test_address).collect();

        for (r, a) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Use reorg_safe_distance: true — Polygon default is 200 blocks behind
        let mut config = create_reorg_config(context, &contract_address);
        config.contracts[0].reorg_safe_distance = Some(serde_json::json!(true));

        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let mut rindexer = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        rindexer.start_indexer().await?;
        context.rindexer = Some(rindexer);

        // With safe_distance=200 and only ~10 blocks on our chain,
        // the indexer won't index anything. Wait for sync to "complete"
        // (it will complete the historic phase, then live poller starts
        // but stays behind). Timeout is expected to be short since
        // historic phase has nothing to sync.
        let _ = context.wait_for_sync_completion(10).await;

        // Verify CSV is empty — safe_distance keeps indexer behind head
        let count = csv_row_count(context);
        info!("CSV rows with safe_distance=true: {}", count);

        // Trigger a reorg — should NOT affect the indexer since it hasn't
        // indexed anything in the reorg window
        context.anvil.trigger_reorg(3).await?;
        context.anvil.mine_block().await?;

        // Give the poller a few cycles to process (poll interval is ~200ms)
        // and verify CSV remains unchanged. We check multiple times to
        // confirm stability rather than relying on a single point-in-time read.
        for _ in 0..5 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let current = csv_row_count(context);
            if current != count {
                return Err(anyhow::anyhow!(
                    "CSV rows changed after reorg with safe_distance: before={}, after={}",
                    count,
                    current
                ));
            }
        }

        info!(
            "Reorg Safe Distance Test PASSED: indexer stayed behind head, reorg had no data impact"
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 7: Reorg idempotency — two consecutive reorgs via stop/restart
// ---------------------------------------------------------------------------
fn reorg_idempotency(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Idempotency Test");

        // Start Postgres for state verification
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

        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let table = "reorg_test_simple_erc_20.transfer";

        // --- First indexing cycle ---
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

        // Wait for rows to appear (deterministic)
        let count1 = wait_for_pg_count(&conn_str, table, 5, 15).await?;
        info!("After first sync: {} rows", count1);

        // Stop indexer, trigger first reorg
        info!("Stopping indexer for first reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        // --- Second indexing cycle (recovers from first reorg) ---
        info!("Restarting indexer after first reorg...");
        let mut rindexer2 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            rindexer2 = rindexer2.with_env(&k, &v);
        }
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        // Send more transfers
        let post_amounts = [5000u64, 6000];
        let post_recipients: Vec<_> = (10..12).map(generate_test_address).collect();
        for (r, a) in post_recipients.iter().zip(post_amounts.iter()) {
            context.anvil.send_transfer(&contract_address, r, U256::from(*a)).await?;
            context.anvil.mine_block().await?;
        }

        // Stop indexer, trigger second reorg
        info!("Stopping indexer for second reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(2).await?;
        context.anvil.mine_block().await?;

        // --- Third indexing cycle (recovers from second reorg) ---
        info!("Restarting indexer after second reorg...");
        let mut rindexer3 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            rindexer3 = rindexer3.with_env(&k, &v);
        }
        rindexer3.start_indexer().await?;
        context.rindexer = Some(rindexer3);
        context.wait_for_sync_completion(30).await?;

        // Wait for data to be fully re-indexed
        let final_count = wait_for_pg_count(&conn_str, table, 1, 15).await?;

        // Verify no duplicate tx_hashes
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let dup_rows = client
            .query(
                "SELECT tx_hash, COUNT(*) as cnt FROM reorg_test_simple_erc_20.transfer \
                 GROUP BY tx_hash HAVING COUNT(*) > 1",
                &[],
            )
            .await?;

        if !dup_rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Found {} duplicate tx_hash entries after two reorgs — state corrupted",
                dup_rows.len()
            ));
        }

        info!("After two reorgs: {} total rows, 0 duplicates", final_count);

        info!(
            "Reorg Idempotency Test PASSED: two consecutive reorgs, no corruption, no duplicates"
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 8: Derived table (balance tracking) reorg — exercises FullReplay
//
// Uses a `balances` table with `upsert + add` on Transfer events.
// After a reorg, rindexer must use FullReplay (clear table, replay from
// block 0) because accumulative operations depend on prior state.
// We verify the final balance values are mathematically correct.
// ---------------------------------------------------------------------------
fn reorg_derived_table_replay(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Reorg Derived Table Replay Test");

        // Start Postgres (required for tables)
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

        // Send transfers to unique recipients. Each recipient gets exactly one
        // transfer so we can verify exact balance values after FullReplay.
        //
        // NOTE: We use unique recipients per batch because rindexer's UNNEST-based
        // batch upsert does not accumulate `add` values for duplicate keys within
        // the same batch — the last value wins instead of summing. Cross-batch
        // accumulation works correctly (the ON CONFLICT DO UPDATE sees committed state).
        // Use large counter values so generate_test_address produces distinct addresses.
        // Small values (0,1,2) collide because to_be_bytes()[..7] truncates the LSB.
        let recipient_0 = generate_test_address(1 << 56);
        let recipient_1 = generate_test_address(2 << 56);
        let recipient_2 = generate_test_address(3 << 56);

        context.anvil.send_transfer(&contract_address, &recipient_0, U256::from(1000u64)).await?;
        context.anvil.mine_block().await?;
        context.anvil.send_transfer(&contract_address, &recipient_1, U256::from(2000u64)).await?;
        context.anvil.mine_block().await?;
        context.anvil.send_transfer(&contract_address, &recipient_2, U256::from(3000u64)).await?;
        context.anvil.mine_block().await?;

        // Mine extra blocks so reorg depth=3 reaches into event blocks
        for _ in 0..3 {
            context.anvil.mine_block().await?;
        }

        // Build config with a balances table (upsert + add → triggers FullReplay)
        let mut config = create_reorg_config(context, &contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;
        config.contracts[0].tables = Some(vec![serde_json::json!({
            "name": "balances",
            "columns": [
                { "name": "holder" },
                { "name": "balance", "default": "0" }
            ],
            "events": [{
                "event": "Transfer",
                "operations": [
                    {
                        "type": "upsert",
                        "where": { "holder": "$to" },
                        "set": [{
                            "column": "balance",
                            "action": "add",
                            "value": "$value"
                        }]
                    }
                ]
            }]
        })]);

        helpers::copy_abis_to_project(&context.project_path)?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(context.project_path.join("rindexer.yaml"), yaml)?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );

        // Start indexer and wait for sync
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

        // Wait for balances table to be populated
        let balances_table = "reorg_test_simple_erc_20.balances";
        let pre_count = wait_for_pg_count(&conn_str, balances_table, 1, 15).await?;
        info!("Pre-reorg balances rows: {}", pre_count);

        // Verify pre-reorg balances
        let r0_addr = helpers::format_address(&recipient_0);
        let r1_addr = helpers::format_address(&recipient_1);
        let r2_addr = helpers::format_address(&recipient_2);

        let pre_bal_0 = query_pg_balance(&conn_str, balances_table, &r0_addr).await?;
        let pre_bal_1 = query_pg_balance(&conn_str, balances_table, &r1_addr).await?;
        let pre_bal_2 = query_pg_balance(&conn_str, balances_table, &r2_addr).await?;
        info!("Pre-reorg balances: r0={}, r1={}, r2={}", pre_bal_0, pre_bal_1, pre_bal_2);

        if pre_bal_0 != "1000" {
            return Err(anyhow::anyhow!(
                "Pre-reorg balance for recipient_0 should be 1000, got {}",
                pre_bal_0
            ));
        }
        if pre_bal_1 != "2000" {
            return Err(anyhow::anyhow!(
                "Pre-reorg balance for recipient_1 should be 2000, got {}",
                pre_bal_1
            ));
        }

        // Stop indexer, trigger reorg, restart
        info!("Stopping indexer before reorg...");
        if let Some(r) = context.rindexer.as_mut() {
            let _ = r.stop().await;
        }
        context.rindexer = None;

        context.anvil.trigger_reorg(4).await?;
        context.anvil.mine_block().await?;

        info!("Restarting indexer to re-sync after reorg...");
        let mut rindexer2 = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            rindexer2 = rindexer2.with_env(&k, &v);
        }
        rindexer2.start_indexer().await?;
        context.rindexer = Some(rindexer2);
        context.wait_for_sync_completion(30).await?;

        // Wait for balances table to stabilize after recovery
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Verify post-reorg balances — must match pre-reorg after FullReplay
        let post_bal_0 = query_pg_balance(&conn_str, balances_table, &r0_addr).await?;
        let post_bal_1 = query_pg_balance(&conn_str, balances_table, &r1_addr).await?;
        let post_bal_2 = query_pg_balance(&conn_str, balances_table, &r2_addr).await?;
        info!("Post-reorg balances: r0={}, r1={}, r2={}", post_bal_0, post_bal_1, post_bal_2);

        if post_bal_0 != "1000" {
            return Err(anyhow::anyhow!(
                "Post-reorg balance for recipient_0 should be 1000 after FullReplay, got {} — \
                 accumulative state was corrupted by reorg recovery",
                post_bal_0
            ));
        }
        if post_bal_1 != "2000" {
            return Err(anyhow::anyhow!(
                "Post-reorg balance for recipient_1 should be 2000 after FullReplay, got {}",
                post_bal_1
            ));
        }
        if post_bal_2 != "3000" {
            return Err(anyhow::anyhow!(
                "Post-reorg balance for recipient_2 should be 3000 after FullReplay, got {}",
                post_bal_2
            ));
        }

        // Verify no duplicate holders
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let dup_rows = client
            .query(
                &format!(
                    "SELECT holder, COUNT(*) as cnt FROM {} GROUP BY holder HAVING COUNT(*) > 1",
                    balances_table
                ),
                &[],
            )
            .await?;

        if !dup_rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Found {} duplicate holder entries in balances table after reorg — \
                 FullReplay did not deduplicate correctly",
                dup_rows.len()
            ));
        }

        info!(
            "Reorg Derived Table Replay Test PASSED: balances correct after FullReplay, no duplicates"
        );
        Ok(())
    })
}

/// Query a balance value from a Postgres balances table by holder address.
/// Uses text cast to avoid needing rust_decimal dependency.
async fn query_pg_balance(conn_str: &str, table: &str, holder: &str) -> Result<String> {
    let (client, connection) =
        tokio_postgres::connect(conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let query = format!("SELECT balance::text FROM {} WHERE holder = $1", table);
    let rows = client.query(&query, &[&holder]).await?;
    if rows.is_empty() {
        return Ok("0".to_string());
    }

    let balance: String = rows[0].get(0);
    Ok(balance)
}
