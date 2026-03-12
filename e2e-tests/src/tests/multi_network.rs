use anyhow::{Context, Result};
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::anvil_setup::ANVIL_DEFAULT_PRIVATE_KEY;
use crate::test_suite::TestContext;
use crate::tests::helpers::{
    derive_block_range_from_csv, generate_test_address, load_tx_hashes_from_csv,
    produced_csv_path_for,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct MultiNetworkTests;

impl TestModule for MultiNetworkTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_multi_network_mixed",
            "Multi-network historic: mainnet rETH + anvil SimpleERC20",
            multi_network_mixed_test,
        )
        .with_timeout(900)]
    }
}

fn multi_network_mixed_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Multi-Network Test: mainnet rETH + anvil SimpleERC20");

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
            .unwrap_or_else(|_| "data/rocketpooleth-transfer.csv".to_string());

        let (csv_start_block, _csv_end_block) = derive_block_range_from_csv(&expected_csv)
            .context("Failed to derive block range from expected CSV")?;
        let mainnet_start_block = csv_start_block;
        let mainnet_end_block = csv_start_block + 20;
        let reth_address = "0xae78736cd615f374d3085123a210448e74fc6393";

        info!(
            "Testing mainnet blocks {} to {} (limited range)",
            mainnet_start_block, mainnet_end_block
        );

        let anvil_contract = context.deploy_test_contract().await?;

        let num_transfers = 5;
        for i in 0..num_transfers {
            feed_transfer_on_anvil(&context.anvil.rpc_url, &anvil_contract, i).await?;
            context.anvil.mine_block().await?;
        }

        let anvil_end_block = context.anvil.get_block_number().await?;
        info!("Anvil has {} blocks with {} transfers", anvil_end_block, num_transfers);

        let config = build_multi_network_config(context.health_port, MultiNetworkConfigParams {
            mainnet_rpc: &mainnet_rpc,
            anvil_rpc: &context.anvil.rpc_url,
            reth_address,
            anvil_contract: &anvil_contract,
            mainnet_start_block,
            mainnet_end_block,
            anvil_start_block: 0,
            anvil_end_block,
        });

        context.start_rindexer(config).await?;

        let sync_timeout = std::env::var("MULTI_NETWORK_SYNC_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(600);

        let reth_csv_path = produced_csv_path_for(context, "RocketPoolETH", "transfer");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(sync_timeout);

        let produced_reth_hashes = loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for rETH CSV"));
            }

            match load_tx_hashes_from_csv(&reth_csv_path) {
                Ok(hashes) if !hashes.is_empty() => {
                    info!("rETH CSV has {} events", hashes.len());
                    break hashes;
                }
                Ok(_) => {
                    info!("rETH CSV empty, waiting...");
                }
                Err(_) => {}
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        };

        let anvil_csv_path = produced_csv_path_for(context, "SimpleERC20", "transfer");
        let anvil_hashes =
            load_tx_hashes_from_csv(&anvil_csv_path).context("Failed to load Anvil CSV")?;

        let expected_anvil_count = num_transfers + 1;
        if anvil_hashes.len() < expected_anvil_count {
            return Err(anyhow::anyhow!(
                "Anvil CSV has {} rows, expected at least {}",
                anvil_hashes.len(),
                expected_anvil_count
            ));
        }

        info!(
            "Multi-Network Test PASSED: rETH ({} events) + anvil ({} transfers)",
            produced_reth_hashes.len(),
            anvil_hashes.len()
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
        NativeTransfersConfig, NetworkConfig, PostgresConfig, RindexerConfig, StorageConfig,
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
            postgres: PostgresConfig { enabled: false },
            csv: CsvConfig { enabled: true },
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
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
            },
        ],
    }
}

async fn feed_transfer_on_anvil(rpc_url: &str, contract_address: &str, nonce: usize) -> Result<()> {
    use ethers::middleware::MiddlewareBuilder;
    use ethers::providers::{Http, Middleware, Provider};
    use ethers::signers::{LocalWallet, Signer};
    use ethers::types::{TransactionRequest, U256};

    let base_provider = Provider::<Http>::try_from(rpc_url)?;
    let chain_id = base_provider.get_chainid().await?.as_u64();

    let wallet: LocalWallet = ANVIL_DEFAULT_PRIVATE_KEY.parse()?;
    let wallet = wallet.with_chain_id(chain_id);
    let signer_address = wallet.address();

    let provider = base_provider.with_signer(wallet);

    let contract_addr: ethers::types::Address = contract_address.parse()?;
    let recipient = generate_test_address(nonce as u64);
    let amount = U256::from(1000u64);

    // Encode transfer(address,uint256)
    let mut data = vec![0xa9, 0x05, 0x9c, 0xbb];
    let mut to_bytes = [0u8; 32];
    to_bytes[12..].copy_from_slice(recipient.as_bytes());
    data.extend_from_slice(&to_bytes);
    let mut value_bytes = [0u8; 32];
    let amount_bytes: [u8; 32] = amount.into();
    value_bytes.copy_from_slice(&amount_bytes);
    data.extend_from_slice(&value_bytes);

    let tx_nonce = provider.get_transaction_count(signer_address, None).await?;

    let tx = TransactionRequest {
        from: Some(signer_address),
        to: Some(contract_addr.into()),
        data: Some(data.into()),
        gas: Some(100000u64.into()),
        nonce: Some(tx_nonce),
        gas_price: Some(20000000000u128.into()),
        value: None,
        chain_id: None,
    };

    let _pending = provider.send_transaction(tx, None).await?;
    Ok(())
}
