use anyhow::{Result, Context};
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct DemoYamlTests;

impl TestModule for DemoYamlTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_6_demo_yaml",
                "Test Rindexer with the demo YAML configuration adapted for Anvil",
                demo_yaml_test,
            ).with_timeout(180),
        ]
    }
}

fn demo_yaml_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Test 6: Demo YAML Test");
    
        // Build a demo-like YAML programmatically (no external file dependency)
        let target_yaml_path = context.project_path.join("rindexer.yaml");

        let mut config = context.create_minimal_config();
        // Enable CSV, disable Postgres
        config.storage.postgres.enabled = false;
        config.storage.csv.enabled = true;

        // Add SimpleERC20 contract with known Hardhat/Anvil default address
        config.contracts = vec![
            crate::test_suite::ContractConfig {
                name: "SimpleERC20".to_string(),
                details: vec![crate::test_suite::ContractDetail {
                    network: "anvil".to_string(),
                    address: "0x5FbDB2315678afecb367f032d93F642f64180aa3".to_string(),
                    start_block: "0".to_string(),
                    end_block: Some("0".to_string()),
                }],
                abi: Some("./abis/SimpleERC20.abi.json".to_string()),
                include_events: Some(vec![crate::test_suite::EventConfig { name: "Transfer".to_string() }]),
            }
        ];

        let yaml_text = serde_yaml::to_string(&config)
            .context("Failed to serialize demo config to YAML")?;
        std::fs::write(&target_yaml_path, yaml_text)
            .context("Failed to write updated YAML file")?;
        
        // Copy the SimpleERC20 ABI file
        let demo_abi_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("abis/SimpleERC20.abi.json");
        let abis_dir = context.project_path.join("abis");
        std::fs::create_dir_all(&abis_dir)
            .context("Failed to create abis directory")?;
        
        let target_abi_path = abis_dir.join("SimpleERC20.abi.json");
        info!("Copying ABI file from: {:?}", demo_abi_path);
        std::fs::copy(&demo_abi_path, &target_abi_path)
            .context("Failed to copy ABI file")?;
        
        info!("Created Rindexer project with demo YAML at: {:?}", context.project_path);
        
        // Start Rindexer with the demo configuration
        info!("Starting Rindexer with demo configuration...");
        let mut rindexer = crate::rindexer_client::RindexerInstance::new(&context.rindexer_binary, context.project_path.clone());
        rindexer.start_indexer().await
            .context("Failed to start Rindexer instance")?;
        
        context.rindexer = Some(rindexer);
        info!("Rindexer started successfully");
        
        // Wait for Rindexer to complete initial sync (it may exit after sync)
        info!("Waiting for Rindexer to complete initial sync...");
        context.wait_for_sync_completion(10).await?;
        info!("Rindexer sync completed");
        
        // For demo YAML test, Rindexer may exit after initial sync since there are no live events
        // This is expected behavior, so we don't check if it's still running
        
        info!("✓ Test 6 PASSED: Rindexer started successfully with demo YAML");
        Ok(())
    })
}
