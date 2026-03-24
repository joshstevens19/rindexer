use anyhow::Result;
use std::future::Future;
use std::pin::Pin;

use crate::test_suite::TestContext;

pub type TestFunction = fn(&mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>;

#[derive(Clone)]
pub struct TestDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub function: TestFunction,
    pub timeout_seconds: u64,
    pub is_live_test: bool,
    /// Custom Anvil chain_id (default: 31337). Reorg tests use 137 (Polygon).
    pub chain_id: u64,
}

impl TestDefinition {
    pub fn new(name: &'static str, description: &'static str, function: TestFunction) -> Self {
        Self {
            name,
            description,
            function,
            timeout_seconds: 180, // Default 3 minutes
            is_live_test: false,
            chain_id: 31337,
        }
    }

    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    #[allow(dead_code)]
    pub fn with_live_test(mut self) -> Self {
        self.is_live_test = true;
        self
    }

    pub fn with_chain_id(mut self, chain_id: u64) -> Self {
        self.chain_id = chain_id;
        self
    }
}

pub trait TestModule {
    fn get_tests() -> Vec<TestDefinition>;
}

pub struct TestRegistry;

impl TestRegistry {
    pub fn get_all_tests() -> Vec<TestDefinition> {
        let mut tests = Vec::new();

        // Core indexing tests
        tests.extend(crate::tests::historic_indexing::HistoricIndexingTests::get_tests());
        tests.extend(crate::tests::live_indexing::LiveIndexingTests::get_tests());
        tests.extend(crate::tests::block_edge_cases::BlockEdgeCaseTests::get_tests());

        // Resilience/restart
        tests.extend(crate::tests::restart_checkpoint::RestartCheckpointTests::get_tests());

        // Configuration
        tests.extend(crate::tests::config_validation::ConfigValidationTests::get_tests());

        // Storage backends
        tests.extend(crate::tests::postgres_e2e::PostgresE2ETests::get_tests());
        tests.extend(crate::tests::graphql_queries::GraphqlQueriesTests::get_tests());

        // Multi-network
        tests.extend(crate::tests::multi_network::MultiNetworkTests::get_tests());

        // Direct RPC
        tests.extend(crate::tests::direct_rpc::DirectRpcTests::get_tests());

        // Reorg detection & recovery
        tests.extend(crate::tests::reorg_e2e::ReorgTests::get_tests());

        // Health assertions
        tests.extend(crate::tests::health_assertions::HealthAssertionsTests::get_tests());

        // Dual-write (PG + CH simultaneously)
        tests.extend(crate::tests::dual_write_e2e::DualWriteTests::get_tests());

        tests
    }

    pub fn get_tests_by_filter(filter: &[String]) -> Vec<TestDefinition> {
        let all_tests = Self::get_all_tests();

        if filter.is_empty() {
            return all_tests;
        }

        all_tests.into_iter().filter(|test| filter.contains(&test.name.to_string())).collect()
    }
}
