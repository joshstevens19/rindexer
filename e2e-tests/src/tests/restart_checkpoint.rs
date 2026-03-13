use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{
    self, event_identities, generate_test_address, parse_transfer_csv, validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct RestartCheckpointTests;

impl TestModule for RestartCheckpointTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_checkpoint_idempotency",
            "Restart indexer, compare event identities (tx_hash), full row equality",
            checkpoint_idempotency_test,
        )
        .with_timeout(180)]
    }
}

fn checkpoint_idempotency_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Checkpoint Idempotency Test");

        // Deploy contract + 5 transfers
        let contract_address = context.deploy_test_contract().await?;

        for i in 0..5 {
            let recipient = generate_test_address(i);
            context
                .anvil
                .send_transfer(&contract_address, &recipient, U256::from((i + 1) * 1000))
                .await?;
            context.anvil.mine_block().await?;
        }

        let end_block = context.anvil.get_block_number().await?;

        // Run 1: index with end_block set
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(30).await?;

        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let (headers1, rows1) = parse_transfer_csv(&csv_path)?;

        // Validate run 1
        validate_csv_structure(&headers1, &rows1)?;
        let run1_identities = event_identities(&rows1);
        let run1_count = rows1.len();

        info!("Run 1: {} events, {} unique identities", run1_count, run1_identities.len());

        if run1_count != 6 {
            return Err(anyhow::anyhow!(
                "Run 1: expected 6 events (1 mint + 5 transfers), got {}",
                run1_count
            ));
        }

        // Stop indexer
        if let Some(mut rindexer) = context.rindexer.take() {
            rindexer.stop().await?;
        }

        // Run 2: restart with same config
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(30).await?;

        let (headers2, rows2) = parse_transfer_csv(&csv_path)?;
        validate_csv_structure(&headers2, &rows2)?;
        let run2_identities = event_identities(&rows2);
        let run2_count = rows2.len();

        info!("Run 2: {} events, {} unique identities", run2_count, run2_identities.len());

        // 1. Identity-level comparison (NOT just count)
        if run1_identities != run2_identities {
            let only_in_1: Vec<_> = run1_identities.difference(&run2_identities).collect();
            let only_in_2: Vec<_> = run2_identities.difference(&run1_identities).collect();
            return Err(anyhow::anyhow!(
                "Event identities differ across restart!\n  Only in run 1: {:?}\n  Only in run 2: {:?}",
                only_in_1,
                only_in_2
            ));
        }

        // 2. Count stability (catches append-mode duplication)
        if run2_count != run1_count {
            return Err(anyhow::anyhow!(
                "Row count changed across restart: {} -> {} (possible append-mode duplication)",
                run1_count,
                run2_count
            ));
        }

        // 3. Full row comparison — every field must be identical
        for id in &run1_identities {
            let row1 = rows1.iter().find(|r| r.identity() == *id).unwrap();
            let row2 = rows2.iter().find(|r| r.identity() == *id).unwrap();
            if row1 != row2 {
                return Err(anyhow::anyhow!(
                    "Row data differs for event {:?}:\n  Run 1: {:?}\n  Run 2: {:?}",
                    id,
                    row1,
                    row2
                ));
            }
        }

        info!(
            "Checkpoint Idempotency Test PASSED: {} events identical across restart, \
             all fields match, no duplicates or losses",
            run1_count
        );
        Ok(())
    })
}
