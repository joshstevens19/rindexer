use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct RestartCheckpointTests;

impl TestModule for RestartCheckpointTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_restart_checkpoint_no_duplicates",
            "Restart indexer and ensure no duplicate events are written",
            restart_checkpoint_no_duplicates_test,
        )
        .with_timeout(180)]
    }
}

fn restart_checkpoint_no_duplicates_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Restart/Checkpoint Test: No Duplicates");

        // Deploy contract and start indexer for historic indexing
        let contract_address = context.deploy_test_contract().await?;
        let config = context.create_contract_config(&contract_address);
        context.start_rindexer(config).await?;

        // Wait for initial historic sync to complete
        context.wait_for_sync_completion(20).await?;

        // Count events after first run
        let first_count = context.get_event_count()?;
        info!("Event count after initial run: {}", first_count);

        // Stop the indexer process
        if let Some(mut rindexer) = context.rindexer.take() {
            rindexer.stop().await?;
        }

        // Restart the indexer with the same project path and config
        let config = context.create_contract_config(&contract_address);
        context.start_rindexer(config).await?;

        // Wait for sync (should be quick if checkpointing works)
        context.wait_for_sync_completion(20).await?;

        // Count events again and ensure no duplicates were written
        let second_count = context.get_event_count()?;
        info!("Event count after restart: {}", second_count);

        if second_count != first_count {
            return Err(anyhow::anyhow!(
                "Duplicate or missing events across restart (before={}, after={})",
                first_count,
                second_count
            ));
        }

        info!("✓ Restart/Checkpoint Test PASSED: No duplicates after restart");
        Ok(())
    })
}
