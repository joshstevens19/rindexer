use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
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
        vec![TestDefinition::new(
            "test_live_indexing_boundary",
            "Historic->live transition: no gaps, no duplicates at boundary",
            live_indexing_boundary_test,
        )
        .with_timeout(120)]
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
