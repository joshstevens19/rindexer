// Shared helpers
pub mod helpers;

// Test modules
pub mod block_edge_cases;
pub mod config_validation;
pub mod direct_rpc;
pub mod graphql_queries;
pub mod health_assertions;
pub mod historic_indexing;
pub mod live_indexing;
pub mod multi_network;
pub mod postgres_e2e;
pub mod restart_checkpoint;

// Registry and runner
pub mod registry;
pub mod test_runner;
pub mod test_suite;

use crate::tests::test_runner::{TestRunner, TestRunnerConfig};
use anyhow::Result;

pub async fn run_tests(rindexer_binary: String, test_names: Option<Vec<String>>) -> Result<()> {
    let config = TestRunnerConfig { rindexer_binary };

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
