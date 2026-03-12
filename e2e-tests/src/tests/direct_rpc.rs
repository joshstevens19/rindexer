use anyhow::{Context, Result};
use std::future::Future;
use std::pin::Pin;
use tracing::{info, warn};

use crate::test_suite::TestContext;
use crate::tests::helpers::{derive_block_range_from_csv, load_tx_hashes_from_csv, produced_csv_path_for};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct DirectRpcTests;

impl TestModule for DirectRpcTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_direct_rpc",
            "Direct RPC realism: Rocket Pool rETH Transfer vs expected CSV",
            direct_rpc_test,
        )
        .with_timeout(900)]
    }
}

fn direct_rpc_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Direct RPC Realism Test (ERC20 Transfer)");

        let mainnet_rpc = match std::env::var("MAINNET_RPC_URL") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                return Err(crate::tests::test_runner::SkipTest(
                    "MAINNET_RPC_URL not set; skipping direct RPC test".to_string(),
                )
                .into());
            }
        };

        let expected_csv = std::env::var("DIRECT_RPC_EXPECTED_CSV")
            .unwrap_or_else(|_| "data/rocketpooleth-transfer-small.csv".to_string());

        let contract_address = std::env::var("DIRECT_RPC_CONTRACT_ADDRESS")
            .unwrap_or_else(|_| "0xae78736cd615f374d3085123a210448e74fc6393".to_string());

        let (start_block, end_block_inclusive) = derive_block_range_from_csv(&expected_csv)
            .context("Failed to derive block range from expected CSV")?;

        let config = build_direct_rpc_config(
            &mainnet_rpc,
            &contract_address,
            start_block,
            Some(end_block_inclusive),
            context.health_port,
        );
        context.start_rindexer(config).await?;

        let sync_timeout = std::env::var("DIRECT_RPC_SYNC_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(600);
        context.wait_for_sync_completion(sync_timeout).await?;

        let produced_csv_path = produced_csv_path_for(context, "ERC20", "transfer");
        let expected_hashes = load_tx_hashes_from_csv(&expected_csv)
            .context("Failed to load expected CSV tx hashes")?;
        let produced_hashes = load_tx_hashes_from_csv(&produced_csv_path).with_context(|| {
            format!("Failed to load produced CSV tx hashes from {:?}", produced_csv_path)
        })?;

        info!("Expected rows={}, Produced rows={}", expected_hashes.len(), produced_hashes.len());
        if expected_hashes != produced_hashes {
            warn!(
                "CSV mismatch: expected {} rows, produced {} rows",
                expected_hashes.len(),
                produced_hashes.len()
            );
            return Err(anyhow::anyhow!("Produced CSV does not match expected CSV"));
        }

        info!("Direct RPC Transfer Test PASSED: matched expected CSV");
        Ok(())
    })
}

fn build_direct_rpc_config(
    rpc_url: &str,
    contract_address: &str,
    start_block: u64,
    end_block: Option<u64>,
    health_port: u16,
) -> crate::test_suite::RindexerConfig {
    use crate::test_suite::{
        ContractConfig, ContractDetail, CsvConfig, EventConfig, GlobalConfig,
        NativeTransfersConfig, NetworkConfig, PostgresConfig, RindexerConfig, StorageConfig,
    };

    RindexerConfig {
        name: "direct_rpc_test".to_string(),
        project_type: "no-code".to_string(),
        config: serde_json::json!({}),
        timestamps: None,
        networks: vec![NetworkConfig {
            name: "mainnet".to_string(),
            chain_id: 1,
            rpc: rpc_url.to_string(),
        }],
        global: GlobalConfig { health_port },
        storage: StorageConfig {
            postgres: PostgresConfig { enabled: false },
            csv: CsvConfig { enabled: true },
        },
        native_transfers: NativeTransfersConfig { enabled: false },
        contracts: vec![ContractConfig {
            name: "ERC20".to_string(),
            details: vec![ContractDetail {
                network: "mainnet".to_string(),
                address: contract_address.to_string(),
                start_block: start_block.to_string(),
                end_block: end_block.map(|b| b.to_string()),
            }],
            abi: Some("./abis/ERC20.abi.json".to_string()),
            include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
        }],
    }
}
