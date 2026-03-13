use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{
    self, format_address, generate_test_address, parse_transfer_csv, validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct BlockEdgeCaseTests;

impl TestModule for BlockEdgeCaseTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_multi_tx_block",
                "5 transfers in 1 block: validate all captured with correct values",
                multi_tx_block_test,
            )
            .with_timeout(120),
            TestDefinition::new(
                "test_empty_block_gaps",
                "50 empty blocks between event clusters: no phantom events",
                empty_block_gaps_test,
            )
            .with_timeout(120),
        ]
    }
}

/// Test that all events from a single block are captured with correct values.
/// Uses evm_setAutomine(false) to batch 5 transfers into one block.
fn multi_tx_block_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Multi-Tx Block Test");

        // Deploy contract (auto-mining is on by default)
        let contract_address = context.deploy_test_contract().await?;

        // Disable auto-mining to batch transfers into one block
        context.anvil.set_automine(false).await?;

        // Submit 5 transfers WITHOUT mining between them
        let transfers: Vec<(ethers::types::Address, U256)> = vec![
            (generate_test_address(0), U256::from(100u64)),
            (generate_test_address(1), U256::from(200u64)),
            (generate_test_address(2), U256::from(300u64)),
            (generate_test_address(3), U256::from(400u64)),
            (generate_test_address(4), U256::from(500u64)),
        ];

        let tx_hashes = context
            .anvil
            .send_transfers_no_mine(&contract_address, &transfers)
            .await?;
        info!("Submitted {} transfers to mempool", tx_hashes.len());

        // Mine ONE block containing all 5 transactions
        context.anvil.mine_block().await?;
        let multi_tx_block = context.anvil.get_block_number().await?;
        info!("All 5 transfers mined in block {}", multi_tx_block);

        // Re-enable auto-mining for the single-tx block
        context.anvil.set_automine(true).await?;

        // Add a single-tx block for contrast
        let single_recipient = generate_test_address(5);
        context
            .anvil
            .send_transfer(&contract_address, &single_recipient, U256::from(600u64))
            .await?;
        // Auto-mining is on, so this gets mined automatically.
        // Mine an explicit block to be sure.
        context.anvil.mine_block().await?;

        let end_block = context.anvil.get_block_number().await?;

        // Configure and run indexer
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(30).await?;

        // Parse CSV
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let (headers, rows) = parse_transfer_csv(&csv_path)?;
        validate_csv_structure(&headers, &rows)?;

        // Expected: 1 mint + 5 multi-tx + 1 single-tx = 7
        if rows.len() != 7 {
            return Err(anyhow::anyhow!("Expected 7 rows, got {}", rows.len()));
        }

        // Find rows in the multi-tx block
        let multi_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.block_number == multi_tx_block)
            .collect();
        if multi_rows.len() != 5 {
            // Debug: print all block numbers to diagnose
            let block_nums: Vec<u64> = rows.iter().map(|r| r.block_number).collect();
            return Err(anyhow::anyhow!(
                "Expected 5 rows in block {}, got {}. All block_numbers: {:?}",
                multi_tx_block,
                multi_rows.len(),
                block_nums
            ));
        }

        // All rows in the same block should share the same block_hash
        let block_hash = &multi_rows[0].block_hash;
        for row in &multi_rows {
            if &row.block_hash != block_hash {
                return Err(anyhow::anyhow!(
                    "Block hash inconsistency within block {}: '{}' vs '{}'",
                    multi_tx_block,
                    block_hash,
                    row.block_hash
                ));
            }
        }

        // But each row should have a different tx_hash
        let tx_hashes_set: std::collections::BTreeSet<_> =
            multi_rows.iter().map(|r| &r.tx_hash).collect();
        if tx_hashes_set.len() != 5 {
            return Err(anyhow::anyhow!(
                "Expected 5 unique tx_hashes in multi-tx block, got {}",
                tx_hashes_set.len()
            ));
        }

        // Verify amounts match submission order (nonce ordering)
        let multi_values: Vec<&str> = multi_rows.iter().map(|r| r.value.as_str()).collect();
        let expected_values = vec!["100", "200", "300", "400", "500"];
        if multi_values != expected_values {
            return Err(anyhow::anyhow!(
                "Multi-tx block values should be {:?}, got {:?}",
                expected_values,
                multi_values
            ));
        }

        // Verify the single-tx block value
        let single_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.value == "600")
            .collect();
        if single_rows.len() != 1 {
            return Err(anyhow::anyhow!(
                "Expected 1 row with value=600, got {}",
                single_rows.len()
            ));
        }

        info!(
            "Multi-Tx Block Test PASSED: 5 events in 1 block, \
             values match nonce order, block_hash consistent, unique tx_hashes"
        );
        Ok(())
    })
}

/// Test that the indexer correctly handles large stretches of empty blocks.
fn empty_block_gaps_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Empty Block Gaps Test");

        // Deploy contract
        let contract_address = context.deploy_test_contract().await?;

        // First event cluster (2 transfers)
        for i in 0..2 {
            let recipient = generate_test_address(i);
            context
                .anvil
                .send_transfer(&contract_address, &recipient, U256::from((i + 1) * 1000))
                .await?;
            context.anvil.mine_block().await?;
        }

        let pre_gap_block = context.anvil.get_block_number().await?;
        info!("Pre-gap block: {}", pre_gap_block);

        // Mine 50 empty blocks
        context.anvil.mine_blocks(50).await?;
        let post_gap_block = context.anvil.get_block_number().await?;
        info!("Post-gap block: {} (50 empty blocks)", post_gap_block);

        // Second event cluster (2 transfers)
        for i in 0..2 {
            let recipient = generate_test_address(i + 10);
            context
                .anvil
                .send_transfer(
                    &contract_address,
                    &recipient,
                    U256::from((i + 1) * 5000),
                )
                .await?;
            context.anvil.mine_block().await?;
        }

        let end_block = context.anvil.get_block_number().await?;
        info!("End block: {}", end_block);

        // Configure and run indexer
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(60).await?;

        // Parse CSV
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let (headers, rows) = parse_transfer_csv(&csv_path)?;
        validate_csv_structure(&headers, &rows)?;

        // Expected: 1 mint + 2 pre-gap + 2 post-gap = 5
        if rows.len() != 5 {
            return Err(anyhow::anyhow!("Expected 5 rows, got {}", rows.len()));
        }

        // No rows should have block_number in the gap range
        let gap_start = pre_gap_block + 1;
        let gap_end = post_gap_block;
        let gap_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.block_number >= gap_start && r.block_number <= gap_end)
            .collect();
        if !gap_rows.is_empty() {
            return Err(anyhow::anyhow!(
                "Found {} phantom events in empty block gap [{}, {}]: {:?}",
                gap_rows.len(),
                gap_start,
                gap_end,
                gap_rows.iter().map(|r| r.block_number).collect::<Vec<_>>()
            ));
        }

        // Verify post-gap events have correct values
        let post_gap_rows: Vec<_> = rows
            .iter()
            .filter(|r| r.block_number > post_gap_block)
            .collect();
        if post_gap_rows.len() != 2 {
            return Err(anyhow::anyhow!(
                "Expected 2 post-gap rows, got {}",
                post_gap_rows.len()
            ));
        }

        // Verify post-gap recipients
        for (i, row) in post_gap_rows.iter().enumerate() {
            let expected_to = format_address(&generate_test_address(i as u64 + 10));
            if row.to != expected_to {
                return Err(anyhow::anyhow!(
                    "Post-gap row {}: expected to={}, got {}",
                    i,
                    expected_to,
                    row.to
                ));
            }
        }

        info!(
            "Empty Block Gaps Test PASSED: 5 events, no phantoms in 50-block gap, \
             post-gap events correct"
        );
        Ok(())
    })
}
