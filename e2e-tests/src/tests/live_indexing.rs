use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct LiveIndexingTests;

impl TestModule for LiveIndexingTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_live_indexing_basic",
                "Test live indexing with background transaction feeder",
                live_indexing_basic_test,
            ).with_timeout(120).as_live_test(),
            
            TestDefinition::new(
                "test_live_indexing_high_frequency",
                "Test live indexing with high-frequency transactions",
                live_indexing_high_frequency_test,
            ).with_timeout(180).as_live_test(),
        ]
    }
}

fn live_indexing_basic_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Live Indexing Test: Basic");
    
        // Use the contract address that was already deployed by the test runner
        let contract_address = context.test_contract_address.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No test contract address available"))?;
        
        // Create configuration with contract
        let config = context.create_contract_config(contract_address);
        
        // Start Rindexer with contract config
        context.start_rindexer(config).await?;
        
        // Wait for Rindexer to be ready for live indexing
        context.wait_for_sync_completion(10).await?;
        
        // Get initial event count
        let initial_events = context.get_event_count()?;
        info!("Initial event count: {}", initial_events);
        
        // The LiveFeeder is already started by the TestRunner for live tests
        // Give the LiveFeeder some time to submit transactions
        info!("Waiting for LiveFeeder to submit transactions...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        
        // Wait for new events to be indexed
        let expected_new_events = 1; // Expect at least 1 new event from the feeder
        let final_events = context.wait_for_new_events(expected_new_events, 30).await?;
        
        info!("Final event count: {} (started with {})", final_events, initial_events);
        
        if final_events <= initial_events {
            return Err(anyhow::anyhow!("No new events were indexed during live test"));
        }
        
        let new_events = final_events - initial_events;
        if new_events < expected_new_events {
            return Err(anyhow::anyhow!("Expected at least {} new events, got {}", expected_new_events, new_events));
        }
        
        info!("✓ Live Indexing Test PASSED: {} new events indexed", new_events);
        Ok(())
    })
}

fn live_indexing_high_frequency_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Live Indexing Test: High Frequency");
    
        // Use the contract address that was already deployed by the test runner
        let contract_address = context.test_contract_address.as_ref()
            .ok_or_else(|| anyhow::anyhow!("No test contract address available"))?;
        
        // Create configuration with contract
        let config = context.create_contract_config(contract_address);
        
        // Start Rindexer with contract config
        context.start_rindexer(config).await?;
        
        // Wait for Rindexer to be ready for live indexing
        context.wait_for_sync_completion(10).await?;
        
        // Get initial event count
        let initial_events = context.get_event_count()?;
        info!("Initial event count: {}", initial_events);
        
        // The LiveFeeder is already started by the TestRunner for live tests
        // Give the LiveFeeder some time to submit transactions (need more time for 2 events)
        info!("Waiting for LiveFeeder to submit transactions...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        
        // For high frequency test, we expect more events
        let expected_new_events = 2; // Expect at least 2 new events
        let final_events = context.wait_for_new_events(expected_new_events, 60).await?;
        
        info!("Final event count: {} (started with {})", final_events, initial_events);
        
        if final_events <= initial_events {
            return Err(anyhow::anyhow!("No new events were indexed during high frequency test"));
        }
        
        let new_events = final_events - initial_events;
        if new_events < expected_new_events {
            return Err(anyhow::anyhow!("Expected at least {} new events, got {}", expected_new_events, new_events));
        }
        
        info!("✓ High Frequency Live Indexing Test PASSED: {} new events indexed", new_events);
        Ok(())
    })
}
