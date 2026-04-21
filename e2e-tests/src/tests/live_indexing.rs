use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{
    self, event_identities, format_address, generate_test_address, parse_transfer_csv,
    validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct LiveIndexingTests;

impl TestModule for LiveIndexingTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_live_indexing_boundary",
                "Historic->live transition: no gaps, no duplicates at boundary",
                live_indexing_boundary_test,
            )
            .with_timeout(120),
            TestDefinition::new(
                "test_live_indexing_sustained_load",
                "Sustained live indexing: drains in-flight JoinHandles across many iterations",
                live_indexing_sustained_load_test,
            )
            .with_timeout(180),
        ]
    }
}

fn live_indexing_boundary_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Live Indexing Boundary Test");

        // Deploy contract (mint event)
        let contract_address = context.deploy_test_contract().await?;

        // Create 3 historic transfers with known values
        let historic_amounts: Vec<u64> = vec![100, 200, 300];
        for (i, amount) in historic_amounts.iter().enumerate() {
            let recipient = generate_test_address(i as u64);
            context.anvil.send_transfer(&contract_address, &recipient, U256::from(*amount)).await?;
            context.anvil.mine_block().await?;
        }

        let historic_end_block = context.anvil.get_block_number().await?;
        info!("Historic phase complete: {} blocks", historic_end_block);

        // Start indexer WITHOUT end_block (live mode)
        let config = context.create_contract_config(&contract_address);
        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(20).await?;

        // Snapshot historic CSV state
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");

        // Wait for CSV to appear and have the historic rows
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(15);
        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for historic CSV"));
            }
            match parse_transfer_csv(&csv_path) {
                Ok((_, rows)) if rows.len() >= 4 => break, // 1 mint + 3 transfers
                _ => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
            }
        }

        let (_, historic_rows) = parse_transfer_csv(&csv_path)?;
        let historic_identities = event_identities(&historic_rows);
        info!("Historic CSV has {} rows", historic_rows.len());

        // Verify all historic rows are at or before historic_end_block
        for row in &historic_rows {
            if row.block_number > historic_end_block {
                return Err(anyhow::anyhow!(
                    "Historic row at block {} exceeds historic_end_block {}",
                    row.block_number,
                    historic_end_block
                ));
            }
        }

        // Feed 3 live transfers with different values
        let live_amounts: Vec<u64> = vec![400, 500, 600];
        for (i, amount) in live_amounts.iter().enumerate() {
            let recipient = generate_test_address((i + 3) as u64);
            context.anvil.send_transfer(&contract_address, &recipient, U256::from(*amount)).await?;
            context.anvil.mine_block().await?;
            info!("Live transfer {}: {} tokens", i, amount);
        }

        // Wait for CSV to reach 7 rows (1 mint + 3 historic + 3 live)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for live events in CSV"));
            }
            match parse_transfer_csv(&csv_path) {
                Ok((_, rows)) if rows.len() >= 7 => break,
                _ => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
            }
        }

        let (headers, all_rows) = parse_transfer_csv(&csv_path)?;

        // Full structural validation
        validate_csv_structure(&headers, &all_rows)?;

        // Row count
        if all_rows.len() != 7 {
            return Err(anyhow::anyhow!(
                "Expected 7 rows (1 mint + 3 historic + 3 live), got {}",
                all_rows.len()
            ));
        }

        // Identify live rows (block_number > historic_end_block)
        let live_rows: Vec<_> =
            all_rows.iter().filter(|r| r.block_number > historic_end_block).collect();

        if live_rows.len() != 3 {
            return Err(anyhow::anyhow!(
                "Expected 3 live rows (block > {}), got {}",
                historic_end_block,
                live_rows.len()
            ));
        }

        // Validate live row values
        for (i, row) in live_rows.iter().enumerate() {
            let expected_to = format_address(&generate_test_address((i + 3) as u64));
            let expected_value = live_amounts[i].to_string();
            if row.to != expected_to {
                return Err(anyhow::anyhow!(
                    "Live transfer {}: to should be {}, got: {}",
                    i,
                    expected_to,
                    row.to
                ));
            }
            if row.value != expected_value {
                return Err(anyhow::anyhow!(
                    "Live transfer {}: value should be {}, got: {}",
                    i,
                    expected_value,
                    row.value
                ));
            }
        }

        // CRITICAL: No duplicates at boundary
        let all_identities = event_identities(&all_rows);
        let live_identities = event_identities(
            &all_rows
                .iter()
                .filter(|r| r.block_number > historic_end_block)
                .cloned()
                .collect::<Vec<_>>(),
        );
        let overlap: Vec<_> = historic_identities.intersection(&live_identities).collect();
        if !overlap.is_empty() {
            return Err(anyhow::anyhow!(
                "Duplicate events at historic/live boundary: {:?}",
                overlap
            ));
        }

        // Uniqueness across all rows
        if all_identities.len() != all_rows.len() {
            return Err(anyhow::anyhow!(
                "Duplicate events detected: {} unique vs {} total",
                all_identities.len(),
                all_rows.len()
            ));
        }

        info!(
            "Live Indexing Boundary Test PASSED: 7 events, boundary clean, \
             no duplicates, field values correct"
        );
        Ok(())
    })
}

/// Regression guard for the `FuturesUnordered` drain loop in
/// `process_event_logs`. Runs two event streams (Transfer + Approval)
/// concurrently against the same contract — each event registers its own
/// `process_non_blocking_event` task with its own `in_flight` queue, so
/// running them together exercises the push/drain/final-drain paths for two
/// independent live-indexing loops. Verifies all events land in the correct
/// CSV, in order, with no duplicates.
fn live_indexing_sustained_load_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Live Indexing Sustained Load Test");

        // Number of blocks to drive through the live-indexing loop. Each block
        // produces one FetchLogsResult per event (Transfer and Approval), so the
        // in-flight drain runs 2 * LIVE_BLOCKS times.
        const LIVE_BLOCKS: usize = 100;
        // Alternate transfer / approve calls so each event stream sees ~half
        // the blocks produce a log and the other half produce an empty batch.
        const TRANSFER_COUNT: usize = LIVE_BLOCKS / 2;
        const APPROVAL_COUNT: usize = LIVE_BLOCKS - TRANSFER_COUNT;

        let contract_address = context.deploy_test_contract().await?;

        // Extend the default contract config to index both Transfer AND Approval
        // events so two `process_event_logs` loops run concurrently.
        let mut config = context.create_contract_config(&contract_address);
        for contract in &mut config.contracts {
            contract.include_events = Some(vec![
                crate::test_suite::EventConfig { name: "Transfer".to_string() },
                crate::test_suite::EventConfig { name: "Approval".to_string() },
            ]);
        }
        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(20).await?;

        let transfer_csv = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let approval_csv = helpers::produced_csv_path_for(context, "SimpleERC20", "approval");

        wait_for_live_streams_ready(&transfer_csv, &approval_csv, Duration::from_secs(15)).await?;

        // Alternate Transfer and Approval over LIVE_BLOCKS blocks.
        for i in 0..LIVE_BLOCKS {
            let counterparty = generate_test_address((i + 1) as u64);
            if i % 2 == 0 {
                context
                    .anvil
                    .send_transfer(&contract_address, &counterparty, U256::from(1))
                    .await?;
            } else {
                context
                    .anvil
                    .send_approve(&contract_address, &counterparty, U256::from(i as u64 + 1))
                    .await?;
            }
            context.anvil.mine_block().await?;
        }

        // Expected Transfer rows: 1 mint + TRANSFER_COUNT transfers.
        let expected_transfers = TRANSFER_COUNT + 1;
        // Approval CSV: exactly APPROVAL_COUNT rows (no initial state).
        let expected_approvals = APPROVAL_COUNT;

        // Wait for both CSVs to fill to their expected counts.
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(120);
        loop {
            if start.elapsed() > timeout {
                let transfers_got =
                    parse_transfer_csv(&transfer_csv).map(|(_, r)| r.len()).unwrap_or(0);
                let approvals_got = count_csv_rows(&approval_csv).unwrap_or(0);
                return Err(anyhow::anyhow!(
                    "Timeout: expected {} transfers (got {}) and {} approvals (got {})",
                    expected_transfers,
                    transfers_got,
                    expected_approvals,
                    approvals_got
                ));
            }
            let transfers_got =
                parse_transfer_csv(&transfer_csv).map(|(_, r)| r.len()).unwrap_or(0);
            let approvals_got = count_csv_rows(&approval_csv).unwrap_or(0);
            if transfers_got >= expected_transfers && approvals_got >= expected_approvals {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // Validate Transfer CSV structure, counts, and no duplicates.
        let (transfer_headers, transfer_rows) = parse_transfer_csv(&transfer_csv)?;
        validate_csv_structure(&transfer_headers, &transfer_rows)?;
        if transfer_rows.len() != expected_transfers {
            return Err(anyhow::anyhow!(
                "Expected {} Transfer rows (1 mint + {} transfers), got {}",
                expected_transfers,
                TRANSFER_COUNT,
                transfer_rows.len()
            ));
        }
        let transfer_identities = event_identities(&transfer_rows);
        if transfer_identities.len() != transfer_rows.len() {
            return Err(anyhow::anyhow!(
                "Duplicate Transfer events: {} unique for {} rows",
                transfer_identities.len(),
                transfer_rows.len()
            ));
        }

        // Spot-check a sample of Transfer recipients to catch silently-dropped events.
        // Transfers happen on even block indices (i*2) for i in 0..TRANSFER_COUNT.
        let sample_indices = [0, TRANSFER_COUNT / 4, TRANSFER_COUNT / 2, TRANSFER_COUNT - 1];
        for sample in sample_indices {
            let block_index = sample * 2;
            let expected_to = format_address(&generate_test_address((block_index + 1) as u64));
            let matching = transfer_rows.iter().any(|r| r.to == expected_to && r.value == "1");
            if !matching {
                return Err(anyhow::anyhow!(
                    "Transfer to {} (block index {}) not found in CSV",
                    expected_to,
                    block_index
                ));
            }
        }

        // Validate Approval CSV count and uniqueness.
        let approval_rows = read_approval_rows(&approval_csv)?;
        if approval_rows.len() != expected_approvals {
            return Err(anyhow::anyhow!(
                "Expected {} Approval rows, got {}",
                expected_approvals,
                approval_rows.len()
            ));
        }
        let approval_tx_hashes: std::collections::BTreeSet<_> =
            approval_rows.iter().map(|r| r.tx_hash.clone()).collect();
        if approval_tx_hashes.len() != approval_rows.len() {
            return Err(anyhow::anyhow!(
                "Duplicate Approval events: {} unique tx_hashes for {} rows",
                approval_tx_hashes.len(),
                approval_rows.len()
            ));
        }

        // Cross-stream sanity: Transfer and Approval events share the same tx
        // pool but are distinct events, so their tx_hash sets must not overlap.
        let transfer_tx_hashes: std::collections::BTreeSet<_> =
            transfer_rows.iter().map(|r| r.tx_hash.clone()).collect();
        let overlap: Vec<_> = transfer_tx_hashes.intersection(&approval_tx_hashes).collect();
        if !overlap.is_empty() {
            return Err(anyhow::anyhow!(
                "Transfer and Approval CSVs share tx_hashes: {:?}",
                overlap
            ));
        }

        info!(
            "Live Indexing Sustained Load Test PASSED: {} Transfer + {} Approval events \
             indexed across {} blocks, no duplicates, no cross-stream leakage",
            transfer_rows.len(),
            approval_rows.len(),
            LIVE_BLOCKS
        );
        Ok(())
    })
}

/// Minimal Approval-row accessor — we only need tx_hash and row count
/// for the drain-loop regression guard.
#[derive(Debug, Clone)]
struct ApprovalRow {
    tx_hash: String,
}

fn read_approval_rows(path: &str) -> Result<Vec<ApprovalRow>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| anyhow::anyhow!("Cannot open Approval CSV at {}: {}", path, e))?;

    let headers: Vec<String> = rdr.headers()?.iter().map(|h| h.to_string()).collect();
    let tx_hash_idx = headers
        .iter()
        .position(|h| h == "tx_hash")
        .ok_or_else(|| anyhow::anyhow!("Approval CSV missing tx_hash column: {:?}", headers))?;

    let mut rows = Vec::new();
    for result in rdr.records() {
        let record = result?;
        if let Some(tx_hash) = record.get(tx_hash_idx) {
            rows.push(ApprovalRow { tx_hash: tx_hash.to_lowercase() });
        }
    }
    Ok(rows)
}

fn count_csv_rows(path: &str) -> Result<usize> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .map_err(|e| anyhow::anyhow!("Cannot open CSV at {}: {}", path, e))?;
    Ok(rdr.records().filter_map(|r| r.ok()).count())
}

async fn wait_for_live_streams_ready(
    transfer_csv: &str,
    approval_csv: &str,
    timeout: Duration,
) -> Result<()> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let transfer_rows =
                parse_transfer_csv(transfer_csv).map(|(_, rows)| rows.len()).unwrap_or(0);
            let approval_ready = approval_csv_ready(approval_csv);
            return Err(anyhow::anyhow!(
                "Timeout waiting for live streams to be ready: transfer_rows={}, approval_ready={}",
                transfer_rows,
                approval_ready
            ));
        }

        let transfer_ready =
            matches!(parse_transfer_csv(transfer_csv), Ok((_, rows)) if !rows.is_empty());
        let approval_ready = approval_csv_ready(approval_csv);

        if transfer_ready && approval_ready {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn approval_csv_ready(path: &str) -> bool {
    csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .ok()
        .and_then(|mut rdr| rdr.headers().ok().cloned())
        .map(|headers| headers.iter().any(|header| header == "tx_hash"))
        .unwrap_or(false)
}
