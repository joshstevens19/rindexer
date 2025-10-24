// New registry-based test modules
pub mod basic_connection;
pub mod contract_discovery;
pub mod historic_indexing;
pub mod demo_yaml;
pub mod live_indexing;
pub mod restart_checkpoint;
pub mod graphql_start;
pub mod config_validation;
pub mod health_assertions;
pub mod postgres_e2e;
pub mod graphql_queries;
pub mod direct_rpc;
pub mod multi_network;

// Registry and runner
pub mod registry;
pub mod test_runner;
pub mod test_suite;

// Legacy test modules removed - now using registry-based system

use anyhow::Result;
use crate::tests::test_runner::{TestRunner, TestRunnerConfig};

// Legacy test system removed - now using registry-based system

/// Registry-based test runner
pub async fn run_tests(rindexer_binary: String, test_names: Option<Vec<String>>) -> Result<()> {
    let config = TestRunnerConfig {
        rindexer_binary,
        anvil_port: 8545,
        health_port: 8080,
    };

    let runner = TestRunner::new(config);

    let suite = if let Some(names) = test_names {
        runner.run_filtered_tests(&names).await?
    } else {
        runner.run_all_tests().await?
    };

    let failed_count = suite.failed_count() + suite.timeout_count();
    if failed_count > 0 {
        std::process::exit(1);
    }

    Ok(())
}
