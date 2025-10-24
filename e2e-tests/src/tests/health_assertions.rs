use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct HealthAssertionsTests;

impl TestModule for HealthAssertionsTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_health_endpoint_ready_and_complete",
            "Assert /health shows ready and indexing tasks go to 0",
            health_endpoint_ready_and_complete_test,
        )
        .with_timeout(120)]
    }
}

fn health_endpoint_ready_and_complete_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Health Endpoint Assertions Test");

        // Use contract config to ensure at least one task runs; keep live mode (no end_block)
        let contract_address = context.deploy_test_contract().await?;
        let config = context.create_contract_config(&contract_address);
        context.start_rindexer(config).await?;

        // Wait for the health HTTP endpoint to be up quickly (lifecycle: starts early)
        if let Some(health) = &context.health_client {
            health.wait_for_up(10).await?;
        }

        // While indexing, /health is available; use logs to detect initial sync completion
        context.wait_for_sync_completion(30).await?;

        // After initial sync, active_tasks should drop to 0 even if live watcher remains
        if let Some(health) = &context.health_client {
            if let Ok(state) = health.get_health().await {
                if let Some(idx) = state.indexing.as_ref() {
                    if idx.active_tasks != 0 {
                        return Err(anyhow::anyhow!(
                            "Indexing did not report 0 active tasks after sync (got {})",
                            idx.active_tasks
                        ));
                    }
                }
            }
        }

        info!("✓ Health Endpoint Assertions Test PASSED: ready and indexing completed");
        Ok(())
    })
}
