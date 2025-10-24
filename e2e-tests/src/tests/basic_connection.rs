use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct BasicConnectionTests;

impl TestModule for BasicConnectionTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_1_basic_connection",
                "Test basic Rindexer connection to Anvil with minimal configuration",
                basic_connection_test,
            ).with_timeout(60),
        ]
    }
}

fn basic_connection_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Test 1: Basic Connection Test");
        
        // Create minimal configuration (just network, no contracts)
        let config = context.create_minimal_config();
        
        // Start Rindexer with minimal config
        context.start_rindexer(config).await?;
        
        // Wait for Rindexer to start up
        context.wait_for_sync_completion(5).await?;
        
        // Verify Rindexer is still running (basic health check)
        if !context.is_rindexer_running() {
            return Err(anyhow::anyhow!("Rindexer process is not running"));
        }
        
        info!("✓ Test 1 PASSED: Rindexer connected successfully with minimal config");
        Ok(())
    })
}
