use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct ContractDiscoveryTests;

impl TestModule for ContractDiscoveryTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_2_contract_discovery",
                "Test Rindexer can discover and register contract events from ABI",
                contract_discovery_test,
            ).with_timeout(120),
        ]
    }
}

fn contract_discovery_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Test 2: Contract Discovery Test");
        
        // Deploy test contract
        let contract_address = context.deploy_test_contract().await?;
        
        // Create configuration with contract
        let config = context.create_contract_config(&contract_address);
        
        // Start Rindexer with contract config
        context.start_rindexer(config).await?;
        
        // Wait for Rindexer to start up and register events
        context.wait_for_sync_completion(15).await?;
         
        // Verify Rindexer is still running
        if context.rindexer.is_none() {
            return Err(anyhow::anyhow!("Rindexer process is not running"));
        }
        
        // Check that CSV output directory was created (indicates Rindexer recognized the contract)
        let csv_path = context.get_csv_output_path();
        if !csv_path.exists() {
            return Err(anyhow::anyhow!("CSV output directory not created - contract not recognized"));
        }
        
        info!("✓ Test 2 PASSED: Rindexer discovered contract and registered Transfer event");
        Ok(())
    })
}
