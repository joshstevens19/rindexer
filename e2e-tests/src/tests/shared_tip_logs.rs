//! The shared tip-block log fetch against a real rindexer binary and a real anvil chain.
//!
//! rindexer reaches anvil through [`RpcProxy`], which injects the empty `eth_getLogs` answers
//! of an upstream that has not indexed a block yet. The tests read the outcome from the
//! Transfer CSV, the proxy's counters, `/metrics` on the health port and the rindexer log.
//! Anvil's 1 s interval miner is stopped after the contract deployment so every block is
//! produced on purpose and the live loop (200 ms polls) sees each one as head.

use anyhow::{anyhow, ensure, Context, Result};
use ethers::types::U256;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tracing::info;

use crate::anvil_setup::AnvilInstance;
use crate::rpc_proxy::RpcProxy;
use crate::test_suite::{ReorgHandlingConfig, RindexerConfig, TestContext};
use crate::tests::helpers::{
    self, generate_test_address, parse_transfer_csv, validate_csv_structure, TransferRow,
};
use crate::tests::registry::{TestDefinition, TestModule};

/// `keccak256("Transfer(address,address,uint256)")`, the topic0 of the stream under test.
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const METRIC_PREFIX: &str = "rindexer_shared_tip_logs_";
const REQUESTS_TOTAL: &str = "rindexer_shared_tip_logs_requests_total";
const BLOCKS_TOTAL: &str = "rindexer_shared_tip_logs_blocks_total";
const RECOVERED_TOTAL: &str = "rindexer_shared_tip_logs_recovered_total";
const SERVED_TOTAL: &str = "rindexer_shared_tip_logs_served_total";
const FALLBACKS_TOTAL: &str = "rindexer_shared_tip_logs_fallbacks_total";
const SYNC_TIMEOUT_SECS: u64 = 30;
const CSV_TIMEOUT: Duration = Duration::from_secs(30);
/// Enough live-loop polls (200 ms pacing) to be sure the first live iteration happened.
const LIVE_SETTLE: Duration = Duration::from_millis(1500);

pub struct SharedTipLogsTests;

impl TestModule for SharedTipLogsTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_shared_tip_logs_recovers_empty_answer",
                "Shared tip logs: a live block whose first unfiltered eth_getLogs answers empty is retried and recovered",
                recovers_empty_answer,
            )
            .with_timeout(180)
            .with_live_test(),
            TestDefinition::new(
                "test_shared_tip_logs_zero_bloom_block_costs_no_rpc",
                "Shared tip logs: a zero-bloom head block is served empty without an eth_getLogs",
                zero_bloom_block_costs_no_rpc,
            )
            .with_timeout(120),
            TestDefinition::new(
                "test_shared_tip_logs_gave_up_falls_back",
                "Shared tip logs: a block empty past the retry budget is fetched by the stream itself",
                gave_up_falls_back,
            )
            .with_timeout(120),
            TestDefinition::new(
                "test_shared_tip_logs_reorg_invalidates",
                "Shared tip logs: a reorg drops the pending tip and the replacement heads are indexed",
                reorg_invalidates,
            )
            .with_timeout(180)
            .with_chain_id(137),
            TestDefinition::new(
                "test_shared_tip_logs_disabled_knob",
                "Shared tip logs: shared_tip_logs.enabled=false keeps the per-stream eth_getLogs path",
                disabled_knob,
            )
            .with_timeout(120),
        ]
    }
}

// ---------------------------------------------------------------------------
// Test 1: an injected empty answer is retried and recovered for every live block
// ---------------------------------------------------------------------------
fn recovers_empty_answer(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Shared Tip Logs Recovery Test");

        // The runner deployed the contract and started the feeder: a transfer every 2 s and
        // an evm_mine every 1 s. Anvil's own miner is stopped so blocks are 1 s apart.
        let contract = context
            .test_contract_address
            .clone()
            .ok_or_else(|| anyhow!("the live feeder did not deploy the contract"))?;
        context.anvil.set_interval_mining(0).await?;

        let proxy = start_behind_proxy(context, &contract, NetworkShape::Anvil, None).await?;
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        ensure!(
            !rindexer_log_lines(context, "shared tip logs enabled for").is_empty(),
            "rindexer did not log the shared tip logs boot line"
        );

        tokio::time::sleep(LIVE_SETTLE).await;
        let live_from = context.anvil.get_block_number().await?;
        info!("Live loop running; feeding for 20 s above block {}", live_from);
        tokio::time::sleep(Duration::from_secs(20)).await;
        let head = context.anvil.get_block_number().await?;

        // Identity: the CSV rows up to `head` are exactly the chain's transfers, once each.
        let chain = chain_transfers(&context.anvil, &contract, head).await?;
        let expected: BTreeSet<String> = chain.values().flatten().cloned().collect();
        let rows = wait_for_csv_transfers(&csv_path, &expected, CSV_TIMEOUT).await?;
        let (headers, _) = parse_transfer_csv(&csv_path)?;
        validate_csv_structure(&headers, &rows)?;
        let indexed: Vec<&TransferRow> =
            rows.iter().filter(|row| row.block_number <= head).collect();
        let indexed_hashes: BTreeSet<String> =
            indexed.iter().map(|row| row.tx_hash.clone()).collect();
        ensure!(
            indexed_hashes == expected,
            "CSV rows up to block {head} differ from the chain: extra {:?}, missing {:?}",
            indexed_hashes.difference(&expected).collect::<Vec<_>>(),
            expected.difference(&indexed_hashes).collect::<Vec<_>>()
        );
        ensure!(
            indexed.len() == indexed_hashes.len(),
            "duplicate rows: {} rows for {} transfers",
            indexed.len(),
            indexed_hashes.len()
        );

        // Every live block with a transfer cost two unfiltered calls: the injected empty
        // answer and the retry that recovered it.
        let live_transfer_blocks: Vec<u64> =
            chain.keys().copied().filter(|block| *block > live_from).collect();
        ensure!(
            live_transfer_blocks.len() >= 5,
            "only {} live block(s) carried a transfer in 20 s: {:?}",
            live_transfer_blocks.len(),
            live_transfer_blocks
        );
        let calls = proxy.unfiltered_calls_by_block();
        for block in &live_transfer_blocks {
            let seen = calls.get(block).copied().unwrap_or(0);
            ensure!(
                seen >= 2,
                "block {block} carried a transfer but the proxy saw {seen} unfiltered call(s) for it (calls by block: {calls:?})"
            );
        }

        let metrics = fetch_metrics(context).await?;
        let recovered = metric_value(&metrics, RECOVERED_TOTAL, &[]);
        let served_tip = metric_value(&metrics, SERVED_TOTAL, &[("mode", "tip")]);
        let fallbacks = metric_value(&metrics, FALLBACKS_TOTAL, &[]);
        let gave_up = metric_value(&metrics, BLOCKS_TOTAL, &[("outcome", "gave_up")]);
        info!(
            "recovered={recovered} served_tip={served_tip} fallbacks={fallbacks} gave_up={gave_up} \
             unfiltered_calls_by_block={calls:?} filtered_calls={}",
            proxy.filtered_calls_total()
        );
        ensure!(
            recovered >= live_transfer_blocks.len() as f64,
            "recovered_total {recovered} is below the {} live transfer blocks",
            live_transfer_blocks.len()
        );
        ensure!(served_tip > 0.0, "served_total{{mode=\"tip\"}} is {served_tip}");
        ensure!(fallbacks == 0.0, "fallbacks_total is {fallbacks}, expected 0");
        ensure!(gave_up == 0.0, "blocks_total{{outcome=\"gave_up\"}} is {gave_up}, expected 0");

        tear_down(context, proxy).await?;
        info!(
            "Shared Tip Logs Recovery Test PASSED: {} transfers indexed, {} live transfer \
             blocks recovered after an injected empty answer",
            expected.len(),
            live_transfer_blocks.len()
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 2: a zero-bloom head block is served empty with no RPC call
// ---------------------------------------------------------------------------
fn zero_bloom_block_costs_no_rpc(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Shared Tip Logs Zero Bloom Test");

        let contract = context.deploy_test_contract().await?;
        context.anvil.set_interval_mining(0).await?;
        let proxy = start_behind_proxy(context, &contract, NetworkShape::Anvil, None).await?;
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let before = wait_for_csv_rows(&csv_path, 1, CSV_TIMEOUT).await?;
        tokio::time::sleep(LIVE_SETTLE).await;
        let calls_before = proxy.unfiltered_calls_by_block();

        // Five empty blocks, each head long enough to be observed by the live loop.
        let empties = mine_spaced(&context.anvil, 5, Duration::from_millis(500)).await?;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let calls = proxy.unfiltered_calls_by_block();
        let fetched: Vec<u64> =
            empties.iter().copied().filter(|block| calls.contains_key(block)).collect();
        ensure!(
            fetched.is_empty(),
            "zero-bloom blocks {fetched:?} were fetched by the shared path (calls by block: {calls:?})"
        );
        ensure!(
            calls == calls_before,
            "unfiltered calls changed while only empty blocks were mined: before {calls_before:?}, after {calls:?}"
        );
        let after = csv_rows(&csv_path);
        ensure!(
            after.len() == before.len(),
            "CSV grew from {} to {} rows on empty blocks",
            before.len(),
            after.len()
        );
        let metrics = fetch_metrics(context).await?;
        let bloom_zero = metric_value(&metrics, BLOCKS_TOTAL, &[("outcome", "empty_bloom_zero")]);
        ensure!(
            bloom_zero >= 5.0,
            "blocks_total{{outcome=\"empty_bloom_zero\"}} is {bloom_zero}, expected at least 5 for {empties:?}"
        );

        // The cursor moved past the empty blocks: the next transfer lands above them and is
        // served from the shared path (one injected empty answer, then the recovered retry).
        let last_empty = empties.last().copied().unwrap_or(0);
        let (tx_hash, block) = transfer_in_next_block(&context.anvil, &contract, 7, 700).await?;
        ensure!(
            block > last_empty,
            "the transfer landed in block {block}, at or below {last_empty}"
        );
        wait_for_csv_transfers(&csv_path, &BTreeSet::from([tx_hash.clone()]), CSV_TIMEOUT).await?;
        let calls_for_block =
            wait_for_unfiltered_calls(&proxy, block, 2, Duration::from_secs(10)).await?;

        // The fetcher's request counter and the proxy's unfiltered counter are the same
        // number: the proxy's shape rule matches the fetcher's call exactly.
        let metrics = fetch_metrics(context).await?;
        let requests = metric_value(&metrics, REQUESTS_TOTAL, &[]);
        let proxied = proxy.unfiltered_calls_total();
        ensure!(
            requests == proxied as f64,
            "rindexer counted {requests} unfiltered requests, the proxy saw {proxied}"
        );

        tear_down(context, proxy).await?;
        info!(
            "Shared Tip Logs Zero Bloom Test PASSED: empty blocks {empties:?} cost no RPC \
             (empty_bloom_zero={bloom_zero}), transfer block {block} cost {calls_for_block} \
             unfiltered calls, {proxied} unfiltered calls in total"
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 3: a block empty past the retry budget is fetched by the stream itself
// ---------------------------------------------------------------------------
fn gave_up_falls_back(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Shared Tip Logs Gave Up Test");

        let contract = context.deploy_test_contract().await?;
        context.anvil.set_interval_mining(0).await?;
        let knob = json!({ "empty_retry_deadline_ms": 1500 });
        let proxy = start_behind_proxy(context, &contract, NetworkShape::Anvil, Some(knob)).await?;
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        wait_for_csv_rows(&csv_path, 1, CSV_TIMEOUT).await?;
        tokio::time::sleep(LIVE_SETTLE).await;
        let filtered_before = proxy.filtered_calls_total();

        // The next block carries a transfer the proxy answers empty on every unfiltered call.
        let doomed = context.anvil.get_block_number().await? + 1;
        proxy.behaviour().always_empty_blocks.insert(doomed);
        let (tx_hash, block) = transfer_in_next_block(&context.anvil, &contract, 8, 800).await?;
        ensure!(block == doomed, "the transfer was mined in block {block}, expected {doomed}");

        let started = Instant::now();
        let rows =
            wait_for_csv_transfers(&csv_path, &BTreeSet::from([tx_hash.clone()]), CSV_TIMEOUT)
                .await?;
        let indexed_after = started.elapsed();
        let row = rows
            .iter()
            .find(|row| row.tx_hash == tx_hash)
            .ok_or_else(|| anyhow!("transfer {tx_hash} vanished from the CSV"))?;
        ensure!(
            row.block_number == doomed,
            "transfer {tx_hash} indexed at block {}, expected {doomed}",
            row.block_number
        );

        let filtered_after = proxy.filtered_calls_total();
        ensure!(
            filtered_after > filtered_before,
            "filtered_calls_total stayed at {filtered_before}: the stream did not fall back to its own eth_getLogs"
        );
        let attempts = proxy.unfiltered_calls_by_block().get(&doomed).copied().unwrap_or(0);
        ensure!(
            attempts >= 2,
            "the fetcher issued {attempts} unfiltered call(s) for block {doomed}, expected the retries of a 1500 ms budget"
        );

        let metrics = fetch_metrics(context).await?;
        let gave_up = metric_value(&metrics, BLOCKS_TOTAL, &[("outcome", "gave_up")]);
        let fallback_gave_up = metric_value(&metrics, FALLBACKS_TOTAL, &[("reason", "gave_up")]);
        ensure!(gave_up == 1.0, "blocks_total{{outcome=\"gave_up\"}} is {gave_up}, expected 1");
        ensure!(
            fallback_gave_up >= 1.0,
            "fallbacks_total{{reason=\"gave_up\"}} is {fallback_gave_up}, expected at least 1"
        );

        let warn_lines = rindexer_log_lines(context, "still answered empty");
        let names_block = warn_lines.iter().any(|line| line.contains(&format!("block {doomed} (")));
        ensure!(names_block, "no give-up WARN names block {doomed}; lines: {warn_lines:?}");

        tear_down(context, proxy).await?;
        info!(
            "Shared Tip Logs Gave Up Test PASSED: block {doomed} answered empty {attempts} times, \
             the stream fell back ({} filtered call(s)) and indexed the transfer {} ms after mining",
            filtered_after - filtered_before,
            indexed_after.as_millis()
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 4: a reorg drops the pending tip and the replacement heads are indexed
// ---------------------------------------------------------------------------
fn reorg_invalidates(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Shared Tip Logs Reorg Test");

        let contract = context.deploy_test_contract().await?;
        context.anvil.set_interval_mining(0).await?;
        let mut expected = BTreeSet::new();
        for (recipient, amount) in [1000u64, 2000, 3000].into_iter().enumerate() {
            let (tx_hash, _) =
                transfer_in_next_block(&context.anvil, &contract, recipient as u64, amount).await?;
            expected.insert(tx_hash);
        }
        let proxy =
            start_behind_proxy(context, &contract, NetworkShape::PolygonWithReorgHandling, None)
                .await?;
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        wait_for_csv_transfers(&csv_path, &expected, CSV_TIMEOUT).await?;
        tokio::time::sleep(LIVE_SETTLE).await;

        // A live transfer warms the coordinator window and the shared cache.
        let (warm_tx, warm_block) =
            transfer_in_next_block(&context.anvil, &contract, 10, 777).await?;
        expected.insert(warm_tx);
        wait_for_csv_transfers(&csv_path, &expected, CSV_TIMEOUT).await?;
        wait_for_unfiltered_calls(&proxy, warm_block, 2, Duration::from_secs(10)).await?;

        // An empty block, then a block with a transfer the proxy keeps answering empty: it
        // stays pending in the cache, which is what the reorg must invalidate. The depth-2
        // reorg orphans both; neither has a CSV row.
        context.anvil.mine_block().await?;
        let empty_block = context.anvil.get_block_number().await?;
        let pending_block = empty_block + 1;
        proxy.behaviour().always_empty_blocks.insert(pending_block);
        let (pending_tx, mined_in) =
            transfer_in_next_block(&context.anvil, &contract, 11, 888).await?;
        ensure!(
            mined_in == pending_block,
            "the transfer landed in {mined_in}, expected {pending_block}"
        );
        let orphaned = [
            context.anvil.get_block(empty_block).await?.hash,
            context.anvil.get_block(pending_block).await?.hash,
        ];
        wait_for_unfiltered_calls(&proxy, pending_block, 1, Duration::from_secs(10)).await?;
        tokio::time::sleep(Duration::from_millis(300)).await;

        context.anvil.trigger_reorg(2).await?;
        proxy.behaviour().always_empty_blocks.clear();
        context.anvil.mine_block().await?;
        if let Some(rindexer) = &context.rindexer {
            rindexer.wait_for_reorg_recovery(60).await?;
        }
        // anvil_reorg replaces the orphaned blocks; whether it re-mines their transactions is
        // read from the chain, and the CSV must agree with it either way.
        let pending_survived = context.anvil.get_receipt(&pending_tx).await.is_ok();
        if pending_survived {
            expected.insert(pending_tx.clone());
        }

        // Replacement heads: a transfer after the reorg is indexed under its canonical hash.
        let (post_tx, post_block) =
            transfer_in_next_block(&context.anvil, &contract, 12, 999).await?;
        expected.insert(post_tx.clone());
        let rows = wait_for_csv_transfers(&csv_path, &expected, CSV_TIMEOUT).await?;

        // The CSV is append-only, so every row carrying a canonical hash proves that nothing
        // from the orphaned blocks was ever served.
        helpers::validate_no_duplicates(&rows)?;
        assert_rows_canonical(&context.anvil, &rows).await?;
        for row in &rows {
            ensure!(
                !orphaned.contains(&row.block_hash),
                "CSV row for tx {} carries the orphaned hash {} of block {}",
                row.tx_hash,
                row.block_hash,
                row.block_number
            );
        }
        let pending_indexed = rows.iter().any(|row| row.tx_hash == pending_tx);
        ensure!(
            pending_indexed == pending_survived,
            "tx {pending_tx} of the orphaned block {pending_block}: on the chain after the reorg = {pending_survived}, in the CSV = {pending_indexed}"
        );
        let post_row = rows
            .iter()
            .find(|row| row.tx_hash == post_tx)
            .ok_or_else(|| anyhow!("post-reorg transfer {post_tx} vanished from the CSV"))?;
        ensure!(
            post_row.block_number == post_block,
            "post-reorg transfer indexed at block {}, expected {post_block}",
            post_row.block_number
        );

        let metrics = fetch_metrics(context).await?;
        let invalidated = metric_value(&metrics, BLOCKS_TOTAL, &[("outcome", "invalidated")]);
        ensure!(
            invalidated >= 1.0,
            "blocks_total{{outcome=\"invalidated\"}} is {invalidated}, expected at least 1 for the pending block {pending_block}"
        );
        let detected = context
            .rindexer
            .as_ref()
            .is_some_and(|rindexer| rindexer.reorg_detected.load(Ordering::Relaxed));
        ensure!(detected, "rindexer did not log a reorg detection");

        tear_down(context, proxy).await?;
        info!(
            "Shared Tip Logs Reorg Test PASSED: blocks {empty_block} and {pending_block} \
             orphaned (pending transfer re-mined: {pending_survived}), invalidated={invalidated}, \
             {} canonical rows, post-reorg transfer at block {post_block}",
            rows.len()
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Test 5: the manifest kill switch keeps the per-stream path
// ---------------------------------------------------------------------------
fn disabled_knob(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Shared Tip Logs Disabled Knob Test");

        let contract = context.deploy_test_contract().await?;
        context.anvil.set_interval_mining(0).await?;
        let knob = json!({ "enabled": false });
        let proxy = start_behind_proxy(context, &contract, NetworkShape::Anvil, Some(knob)).await?;
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        wait_for_csv_rows(&csv_path, 1, CSV_TIMEOUT).await?;
        ensure!(
            !rindexer_log_lines(context, "shared tip logs disabled for").is_empty(),
            "rindexer did not log the shared tip logs disabled boot line"
        );
        tokio::time::sleep(LIVE_SETTLE).await;
        let filtered_before = proxy.filtered_calls_total();

        let mut expected = BTreeSet::new();
        for recipient in 20..23u64 {
            let (tx_hash, _) =
                transfer_in_next_block(&context.anvil, &contract, recipient, recipient).await?;
            expected.insert(tx_hash);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        wait_for_csv_transfers(&csv_path, &expected, CSV_TIMEOUT).await?;

        let unfiltered = proxy.unfiltered_calls_by_block();
        ensure!(
            unfiltered.is_empty(),
            "the disabled knob still issued unfiltered calls: {unfiltered:?}"
        );
        let filtered_after = proxy.filtered_calls_total();
        ensure!(
            filtered_after > filtered_before,
            "filtered_calls_total stayed at {filtered_before} while three transfer blocks were indexed"
        );
        let metrics = fetch_metrics(context).await?;
        let shared_total = shared_metrics_total(&metrics);
        ensure!(
            shared_total == 0.0,
            "rindexer_shared_tip_logs_* samples sum to {shared_total} with the knob disabled"
        );

        tear_down(context, proxy).await?;
        info!(
            "Shared Tip Logs Disabled Knob Test PASSED: 0 unfiltered calls, {} filtered calls \
             for 3 live transfer blocks, no shared metrics",
            filtered_after - filtered_before
        );
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

/// The network stanza of the test manifest.
#[derive(Debug, Clone, Copy)]
enum NetworkShape {
    /// Chain 31337 without the reorg coordinator (the harness default).
    Anvil,
    /// Chain 137 with the coordinator enabled, the shape of `reorg_e2e.rs`.
    PolygonWithReorgHandling,
}

/// Starts rindexer behind a fresh proxy on the SimpleERC20 Transfer stream and waits for the
/// historic phase. `shared_tip_logs` is the manifest stanza under the network.
async fn start_behind_proxy(
    context: &mut TestContext,
    contract: &str,
    shape: NetworkShape,
    shared_tip_logs: Option<Value>,
) -> Result<RpcProxy> {
    let proxy = RpcProxy::start(&context.anvil.rpc_url).await?;
    let mut config = context.create_contract_config(contract);
    config.name = "shared_tip_logs_test".to_string();
    config.networks[0].rpc = proxy.url().to_string();
    config.networks[0].shared_tip_logs = shared_tip_logs;
    if let NetworkShape::PolygonWithReorgHandling = shape {
        apply_reorg_network(&mut config);
    }
    context.start_rindexer(config).await?;
    context.wait_for_sync_completion(SYNC_TIMEOUT_SECS).await?;
    Ok(proxy)
}

fn apply_reorg_network(config: &mut RindexerConfig) {
    let network = &mut config.networks[0];
    network.name = "polygon".to_string();
    network.chain_id = 137;
    network.reorg_handling = Some(ReorgHandlingConfig { enabled: true, window_size: None });
    for contract in &mut config.contracts {
        contract.reorg_safe_distance = Some(json!(false));
        for detail in &mut contract.details {
            detail.network = "polygon".to_string();
        }
    }
}

/// Stops rindexer before the proxy so the shutdown produces no RPC errors; the instance stays
/// on the context for its captured log.
async fn tear_down(context: &mut TestContext, mut proxy: RpcProxy) -> Result<()> {
    if let Some(rindexer) = context.rindexer.as_mut() {
        rindexer.stop().await?;
    }
    proxy.stop().await;
    Ok(())
}

fn rindexer_log_lines(context: &TestContext, needle: &str) -> Vec<String> {
    context
        .rindexer
        .as_ref()
        .map(|rindexer| rindexer.log_lines_containing(needle))
        .unwrap_or_default()
}

/// Sends one Transfer, mines the next block and returns `(tx_hash, block_number)` from the
/// receipt. Interval mining is off in these tests, so the block is `mine_block`'s.
async fn transfer_in_next_block(
    anvil: &AnvilInstance,
    contract: &str,
    recipient: u64,
    amount: u64,
) -> Result<(String, u64)> {
    let tx_hash = anvil
        .send_transfer(contract, &generate_test_address(recipient), U256::from(amount))
        .await?;
    anvil.mine_block().await?;
    let receipt = anvil.get_receipt(&tx_hash).await?;
    ensure!(receipt.status, "transfer {tx_hash} reverted");
    Ok((tx_hash, receipt.block_number))
}

/// Mines `count` empty blocks `gap` apart so the live loop sees each one as head.
async fn mine_spaced(anvil: &AnvilInstance, count: u64, gap: Duration) -> Result<Vec<u64>> {
    let mut heights = Vec::new();
    for _ in 0..count {
        anvil.mine_block().await?;
        heights.push(anvil.get_block_number().await?);
        tokio::time::sleep(gap).await;
    }
    Ok(heights)
}

/// Transfer logs of `contract` up to `to_block`, straight from anvil: tx hashes by block.
async fn chain_transfers(
    anvil: &AnvilInstance,
    contract: &str,
    to_block: u64,
) -> Result<BTreeMap<u64, BTreeSet<String>>> {
    let filter = json!({
        "fromBlock": "0x0",
        "toBlock": format!("0x{to_block:x}"),
        "address": contract,
        "topics": [TRANSFER_TOPIC],
    });
    let response = anvil.rpc_call("eth_getLogs", json!([filter])).await?;
    let logs = response["result"]
        .as_array()
        .ok_or_else(|| anyhow!("eth_getLogs answered without a log array: {response}"))?;
    let mut by_block: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for log in logs {
        let block = hex_to_u64(&log["blockNumber"]).context("log blockNumber")?;
        let tx_hash = log["transactionHash"]
            .as_str()
            .ok_or_else(|| anyhow!("log without transactionHash: {log}"))?
            .to_lowercase();
        by_block.entry(block).or_default().insert(tx_hash);
    }
    Ok(by_block)
}

fn hex_to_u64(value: &Value) -> Result<u64> {
    let text = value.as_str().ok_or_else(|| anyhow!("expected a hex quantity, got {value}"))?;
    u64::from_str_radix(text.trim_start_matches("0x"), 16)
        .with_context(|| format!("parse hex quantity {text}"))
}

/// Every row's block hash is the hash anvil reports for that height now.
async fn assert_rows_canonical(anvil: &AnvilInstance, rows: &[TransferRow]) -> Result<()> {
    for row in rows {
        let block = anvil.get_block(row.block_number).await?;
        ensure!(
            block.hash == row.block_hash,
            "CSV row for tx {} carries block {} hash {} but the chain has {}",
            row.tx_hash,
            row.block_number,
            row.block_hash,
            block.hash
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CSV, proxy and metrics readers
// ---------------------------------------------------------------------------

fn csv_rows(csv_path: &str) -> Vec<TransferRow> {
    parse_transfer_csv(csv_path).map(|(_, rows)| rows).unwrap_or_default()
}

/// Polls the Transfer CSV until it holds at least `min_rows` rows.
async fn wait_for_csv_rows(
    csv_path: &str,
    min_rows: usize,
    timeout: Duration,
) -> Result<Vec<TransferRow>> {
    let start = Instant::now();
    loop {
        let rows = csv_rows(csv_path);
        if rows.len() >= min_rows {
            return Ok(rows);
        }
        if start.elapsed() > timeout {
            return Err(anyhow!(
                "timed out after {timeout:?} waiting for {min_rows} CSV row(s), found {}",
                rows.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Polls the Transfer CSV until it holds every tx hash of `expected`; returns all rows.
async fn wait_for_csv_transfers(
    csv_path: &str,
    expected: &BTreeSet<String>,
    timeout: Duration,
) -> Result<Vec<TransferRow>> {
    let start = Instant::now();
    loop {
        let rows = csv_rows(csv_path);
        let present: BTreeSet<String> = rows.iter().map(|row| row.tx_hash.clone()).collect();
        if expected.is_subset(&present) {
            return Ok(rows);
        }
        if start.elapsed() > timeout {
            let missing: Vec<&String> = expected.difference(&present).collect();
            return Err(anyhow!(
                "timed out after {timeout:?} waiting for {} transfer(s) in the CSV: {missing:?}",
                missing.len()
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Waits until the proxy has seen at least `min_calls` unfiltered calls for `block`.
async fn wait_for_unfiltered_calls(
    proxy: &RpcProxy,
    block: u64,
    min_calls: u32,
    timeout: Duration,
) -> Result<u32> {
    let start = Instant::now();
    loop {
        let calls = proxy.unfiltered_calls_by_block().get(&block).copied().unwrap_or(0);
        if calls >= min_calls {
            return Ok(calls);
        }
        if start.elapsed() > timeout {
            return Err(anyhow!(
                "the proxy saw {calls} unfiltered call(s) for block {block} after {timeout:?}, expected at least {min_calls}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn fetch_metrics(context: &TestContext) -> Result<String> {
    let url = format!("http://127.0.0.1:{}/metrics", context.health_port);
    let response = reqwest::get(&url).await.with_context(|| format!("GET {url}"))?;
    ensure!(response.status().is_success(), "GET {url} returned {}", response.status());
    response.text().await.context("read the /metrics body")
}

/// One sample line of the Prometheus text format.
struct Sample<'a> {
    name: &'a str,
    labels: Vec<(&'a str, &'a str)>,
    value: f64,
}

fn parse_sample(line: &str) -> Option<Sample<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (head, value) = line.rsplit_once(' ')?;
    let value = value.parse::<f64>().ok()?;
    let (name, labels) = match head.split_once('{') {
        Some((name, rest)) => (name, rest.strip_suffix('}')?),
        None => (head, ""),
    };
    let labels = labels
        .split(',')
        .filter(|pair| !pair.is_empty())
        .filter_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            Some((key, value.trim_matches('"')))
        })
        .collect();
    Some(Sample { name, labels, value })
}

/// Sum of the samples of `name` whose labels include every pair of `labels`.
fn metric_value(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    text.lines()
        .filter_map(parse_sample)
        .filter(|sample| {
            sample.name == name && labels.iter().all(|wanted| sample.labels.contains(wanted))
        })
        .fold(0.0, |total, sample| total + sample.value)
}

/// Sum of every `rindexer_shared_tip_logs_*` sample, whatever the series.
fn shared_metrics_total(text: &str) -> f64 {
    text.lines()
        .filter_map(parse_sample)
        .filter(|sample| sample.name.starts_with(METRIC_PREFIX))
        .fold(0.0, |total, sample| total + sample.value)
}
