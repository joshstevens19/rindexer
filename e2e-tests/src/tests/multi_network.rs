use anyhow::{Context, Result};
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::test_suite::TestContext;
use crate::tests::helpers::{
    self, derive_block_range_from_csv, format_address, generate_test_address, parse_transfer_csv,
    validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct MultiNetworkTests;

impl TestModule for MultiNetworkTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_multi_network_isolation",
            "Multi-network: mainnet rETH + anvil SimpleERC20, validate isolation and field accuracy",
            multi_network_isolation_test,
        )
        .with_timeout(900)]
    }
}

/// Test that two networks indexed in parallel produce correct, isolated results.
/// Anvil side: deploy contract + 5 transfers with varying amounts, validate all CSV fields.
/// Mainnet side: index rETH transfers, validate non-empty and no cross-contamination.
fn multi_network_isolation_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Multi-Network Isolation Test");

        let mainnet_rpc = match std::env::var("MAINNET_RPC_URL") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                return Err(crate::tests::test_runner::SkipTest(
                    "MAINNET_RPC_URL not set; skipping multi-network test".to_string(),
                )
                .into());
            }
        };

        let expected_csv = std::env::var("DIRECT_RPC_EXPECTED_CSV")
            .unwrap_or_else(|_| "data/rocketpooleth-transfer-small.csv".to_string());

        let (csv_start_block, _csv_end_block) = derive_block_range_from_csv(&expected_csv)
            .context("Failed to derive block range from expected CSV")?;
        let mainnet_start_block = csv_start_block;
        let mainnet_end_block = csv_start_block + 20;
        let reth_address = "0xae78736cd615f374d3085123a210448e74fc6393";

        info!("Mainnet blocks {} to {} (limited range)", mainnet_start_block, mainnet_end_block);

        // Deploy anvil contract + 5 transfers with varying amounts
        let anvil_contract = context.deploy_test_contract().await?;

        let amounts: Vec<u64> = vec![1000, 2000, 3000, 4000, 5000];
        let recipients: Vec<ethers::types::Address> = (0..5).map(generate_test_address).collect();

        for (recipient, amount) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&anvil_contract, recipient, U256::from(*amount)).await?;
            context.anvil.mine_block().await?;
        }

        let anvil_end_block = context.anvil.get_block_number().await?;
        info!("Anvil: {} blocks, {} transfers", anvil_end_block, amounts.len());

        let config = build_multi_network_config(
            context.health_port,
            MultiNetworkConfigParams {
                mainnet_rpc: &mainnet_rpc,
                anvil_rpc: &context.anvil.rpc_url,
                reth_address,
                anvil_contract: &anvil_contract,
                mainnet_start_block,
                mainnet_end_block,
                anvil_start_block: 0,
                anvil_end_block,
            },
        );

        context.start_rindexer(config).await?;

        let sync_timeout = std::env::var("MULTI_NETWORK_SYNC_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(600);

        // Wait for rETH CSV
        let reth_csv_path = helpers::produced_csv_path_for(context, "RocketPoolETH", "transfer");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(sync_timeout);

        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for rETH CSV"));
            }
            match parse_transfer_csv(&reth_csv_path) {
                Ok((_, rows)) if !rows.is_empty() => {
                    info!("rETH CSV has {} events", rows.len());
                    break;
                }
                _ => tokio::time::sleep(tokio::time::Duration::from_secs(2)).await,
            }
        }

        // Wait for anvil CSV
        let anvil_csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60);
        loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for anvil CSV"));
            }
            match parse_transfer_csv(&anvil_csv_path) {
                Ok((_, rows)) if rows.len() >= 6 => break, // 1 mint + 5 transfers
                _ => tokio::time::sleep(tokio::time::Duration::from_secs(1)).await,
            }
        }

        // === Validate anvil CSV with full field-level assertions ===
        let (anvil_headers, anvil_rows) = parse_transfer_csv(&anvil_csv_path)?;
        validate_csv_structure(&anvil_headers, &anvil_rows)?;

        // Row count: 1 mint + 5 transfers = 6
        if anvil_rows.len() != 6 {
            return Err(anyhow::anyhow!(
                "Anvil CSV: expected 6 rows (1 mint + 5 transfers), got {}",
                anvil_rows.len()
            ));
        }

        // Validate transfer rows (skip mint at index 0)
        for (i, row) in anvil_rows.iter().skip(1).enumerate() {
            let expected_to = format_address(&recipients[i]);
            let expected_value = amounts[i].to_string();

            if row.to != expected_to {
                return Err(anyhow::anyhow!(
                    "Anvil transfer {}: to should be {}, got {}",
                    i,
                    expected_to,
                    row.to
                ));
            }
            if row.value != expected_value {
                return Err(anyhow::anyhow!(
                    "Anvil transfer {}: value should be {}, got {}",
                    i,
                    expected_value,
                    row.value
                ));
            }
            if row.network != "anvil" {
                return Err(anyhow::anyhow!(
                    "Anvil transfer {}: network should be 'anvil', got '{}'",
                    i,
                    row.network
                ));
            }
        }

        // === Validate rETH CSV ===
        let (reth_headers, reth_rows) = parse_transfer_csv(&reth_csv_path)?;
        validate_csv_structure(&reth_headers, &reth_rows)?;

        if reth_rows.is_empty() {
            return Err(anyhow::anyhow!("rETH CSV has 0 rows"));
        }

        // All rETH rows must have network "ethereum"
        for (i, row) in reth_rows.iter().enumerate() {
            if row.network != "ethereum" {
                return Err(anyhow::anyhow!(
                    "rETH row {}: network should be 'ethereum', got '{}'",
                    i,
                    row.network
                ));
            }
        }

        // All rETH rows must have the rETH contract address
        let reth_lower = reth_address.to_lowercase();
        for (i, row) in reth_rows.iter().enumerate() {
            if row.contract_address != reth_lower {
                return Err(anyhow::anyhow!(
                    "rETH row {}: contract_address should be {}, got {}",
                    i,
                    reth_lower,
                    row.contract_address
                ));
            }
        }

        // === Cross-contamination check ===
        // No anvil rows should have network "ethereum" and vice versa
        let anvil_contract_lower = anvil_contract.to_lowercase();
        for row in &reth_rows {
            if row.contract_address == anvil_contract_lower {
                return Err(anyhow::anyhow!(
                    "Cross-contamination: rETH CSV contains anvil contract address"
                ));
            }
        }
        for row in &anvil_rows {
            if row.contract_address == reth_lower {
                return Err(anyhow::anyhow!(
                    "Cross-contamination: anvil CSV contains rETH contract address"
                ));
            }
        }

        info!(
            "Multi-Network Isolation Test PASSED: rETH ({} events, network=ethereum) + \
             anvil ({} events, network=anvil), no cross-contamination, all fields validated",
            reth_rows.len(),
            anvil_rows.len()
        );
        Ok(())
    })
}

struct MultiNetworkConfigParams<'a> {
    mainnet_rpc: &'a str,
    anvil_rpc: &'a str,
    reth_address: &'a str,
    anvil_contract: &'a str,
    mainnet_start_block: u64,
    mainnet_end_block: u64,
    anvil_start_block: u64,
    anvil_end_block: u64,
}

fn build_multi_network_config(
    health_port: u16,
    params: MultiNetworkConfigParams<'_>,
) -> crate::test_suite::RindexerConfig {
    use crate::test_suite::{
        ContractConfig, ContractDetail, CsvConfig, EventConfig, GlobalConfig,
        NativeTransfersConfig, NetworkConfig, RindexerConfig, StorageConfig,
    };

    RindexerConfig {
        name: "multi_network_test".to_string(),
        project_type: "no-code".to_string(),
        config: serde_json::json!({}),
        timestamps: None,
        networks: vec![
            NetworkConfig {
                name: "ethereum".to_string(),
                chain_id: 1,
                rpc: params.mainnet_rpc.to_string(),
            },
            NetworkConfig {
                name: "anvil".to_string(),
                chain_id: 31337,
                rpc: params.anvil_rpc.to_string(),
            },
        ],
        global: GlobalConfig { health_port },
        storage: StorageConfig {
            postgres: None,
            csv: CsvConfig { enabled: true },
            clickhouse: None,
        },
        native_transfers: NativeTransfersConfig { enabled: false },
        contracts: vec![
            ContractConfig {
                name: "RocketPoolETH".to_string(),
                details: vec![ContractDetail {
                    network: "ethereum".to_string(),
                    address: params.reth_address.to_string(),
                    start_block: params.mainnet_start_block.to_string(),
                    end_block: Some(params.mainnet_end_block.to_string()),
                }],
                abi: Some("./abis/ERC20.abi.json".to_string()),
                reorg_safe_distance: None,
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
            },
            ContractConfig {
                name: "SimpleERC20".to_string(),
                details: vec![ContractDetail {
                    network: "anvil".to_string(),
                    address: params.anvil_contract.to_string(),
                    start_block: params.anvil_start_block.to_string(),
                    end_block: Some(params.anvil_end_block.to_string()),
                }],
                abi: Some("./abis/SimpleERC20.abi.json".to_string()),
                reorg_safe_distance: None,
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
            },
        ],
    }
}
