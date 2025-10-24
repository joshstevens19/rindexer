use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::test_suite::TestSuite;
use crate::tests::registry::{TestDefinition, TestRegistry};
use crate::tests::test_suite::{TestInfo, TestResult};
use thiserror::Error;
use crate::live_feeder::LiveFeeder;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct SkipTest(pub String);

pub struct TestRunner {
    config: TestRunnerConfig,
}

#[derive(Debug, Clone)]
pub struct TestRunnerConfig {
    pub rindexer_binary: String,
    pub anvil_port: u16,
    pub health_port: u16,
}

impl Default for TestRunnerConfig {
    fn default() -> Self {
        Self {
            rindexer_binary: "../rindexer/target/release/rindexer_cli".to_string(),
            anvil_port: 8545,
            health_port: 8080,
        }
    }
}

impl TestRunner {
    pub fn new(config: TestRunnerConfig) -> Self {
        Self { config }
    }

    pub async fn run_all_tests(&self) -> Result<TestSuite> {
        info!("[START] Rindexer E2E Test Suite");
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let mut suite = TestSuite::new("Rindexer E2E Tests".to_string());
        let overall_start = Instant::now();

        let registry_tests = TestRegistry::get_all_tests();

        for test_def in registry_tests {
            let test_result = self.run_registry_test(&test_def).await;
            suite.add_test(test_result);
        }

        suite.duration = overall_start.elapsed();
        suite.print_summary();
        Ok(suite)
    }

    pub async fn run_filtered_tests(&self, test_names: &[String]) -> Result<TestSuite> {
        info!("[START] Rindexer E2E Test Suite - Filtered Tests: {:?}", test_names);
        info!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let mut suite = TestSuite::new(format!("Filtered Tests: {:?}", test_names));
        let overall_start = Instant::now();

        let registry_tests = TestRegistry::get_tests_by_filter(test_names);

        if registry_tests.is_empty() {
            info!("No tests found matching filter: {:?}", test_names);
            return Ok(suite);
        }

        for test_def in registry_tests {
            let test_result = self.run_registry_test(&test_def).await;
            suite.add_test(test_result);
        }

        suite.duration = overall_start.elapsed();
        suite.print_summary();
        Ok(suite)
    }

    async fn run_registry_test(&self, test_def: &TestDefinition) -> TestInfo {
        println!("[TEST] {} ... ", test_def.description);
        let start = Instant::now();

        let result = timeout(
            Duration::from_secs(test_def.timeout_seconds),
            self.run_single_test(test_def)
        ).await;

        let test_result = match result {
            Ok(Ok(())) => {
                println!("[SUCCESS] PASS");
                TestResult::Passed
            }
            Ok(Err(e)) => {
                if let Some(skip) = e.downcast_ref::<SkipTest>() {
                    println!("[SKIP] SKIPPED");
                    TestResult::Skipped(skip.0.clone())
                } else {
                    println!("[ERROR] FAIL");
                    TestResult::Failed(e.to_string())
                }
            }
            Err(_) => {
                println!("[TIMEOUT] TIMEOUT");
                TestResult::Timeout
            }
        };

        let duration = start.elapsed();
        TestInfo::new(test_def.name.to_string(), test_result, duration)
    }

    async fn run_single_test(&self, test_def: &TestDefinition) -> Result<()> {
        // Create fresh test context for each test
        let mut context = TestContext::new(
            self.config.rindexer_binary.clone(),
            self.config.anvil_port,
            self.config.health_port,
        ).await?;

        // Start live feeder if this is a live test
        let mut live_feeder = if test_def.is_live_test {
            info!("Starting live feeder for live indexing test: {}", test_def.name);
            let contract_address = context.deploy_test_contract().await?;
            
            // Store the contract address in the context for the test to use
            context.test_contract_address = Some(contract_address.clone());
            
            let mut feeder = LiveFeeder::new(
                context.anvil.rpc_url.clone(),
                "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
            ).with_contract(contract_address.parse()?);
            
            // Start feeder in background
            feeder.start().await?;
            Some(feeder)
        } else {
            None
        };

        // Run the actual test
        let test_result = (test_def.function)(&mut context).await;

        // Stop live feeder if it was running
        if let Some(feeder) = live_feeder.take() {
            info!("Stopping live feeder for test: {}", test_def.name);
            feeder.stop();
        }

        // Cleanup context
        let _ = context.cleanup().await;

        test_result
    }
}
