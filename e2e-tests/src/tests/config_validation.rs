use anyhow::Result;
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct ConfigValidationTests;

impl TestModule for ConfigValidationTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_invalid_yaml_fails",
                "Invalid YAML causes rindexer to fail fast with clear error",
                invalid_yaml_fails_test,
            ).with_timeout(60),
            TestDefinition::new(
                "test_missing_abi_path_fails",
                "Missing ABI path for contract yields actionable error",
                missing_abi_path_fails_test,
            ).with_timeout(90),
        ]
    }
}

fn invalid_yaml_fails_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Invalid YAML Failure Test");

        // Write a deliberately invalid YAML file to the project
        let target_yaml_path = context.project_path.join("rindexer.yaml");
        let bad_yaml = "name: test\nnetworks: [\n  - name: anvil\n    chain_id: 31337\n    rpc: http://127.0.0.1:8545\nstorage: { csv: { enabled: true }"; // missing braces/lines
        std::fs::create_dir_all(context.project_path.join("abis"))?;
        std::fs::write(&target_yaml_path, bad_yaml)?;

        // Attempt to start the indexer; it should fail
        let mut r = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone());
        let res = r.start_indexer().await;
        if res.is_ok() {
            return Err(anyhow::anyhow!("Indexer unexpectedly started with invalid YAML"));
        }

        info!("✓ Invalid YAML Failure Test PASSED: start failed as expected");
        Ok(())
    })
}

fn missing_abi_path_fails_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        // Create a valid contract config but point ABI to a non-existent file
        let contract_address = context.deploy_test_contract().await?;
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            contract.abi = Some("./abis/DOES_NOT_EXIST.abi.json".to_string());
        }
        // Write config explicitly and try to start
        let target_yaml_path = context.project_path.join("rindexer.yaml");
        std::fs::create_dir_all(context.project_path.join("abis"))?;
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&target_yaml_path, yaml)?;

        let mut r = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone());
        let res = r.start_indexer().await;
        if res.is_ok() {
            return Err(anyhow::anyhow!("Indexer unexpectedly started with missing ABI path"));
        }
        Ok(())
    })
}


