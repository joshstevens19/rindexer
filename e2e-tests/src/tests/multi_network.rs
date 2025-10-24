use anyhow::{Result, Context};
use tracing::info;
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct MultiNetworkTests;

impl TestModule for MultiNetworkTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_multi_network_mixed",
                "Multi-network historic: mainnet rETH + anvil SimpleERC20",
                multi_network_mixed_test,
            ).with_timeout(900),
        ]
    }
}

fn multi_network_mixed_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Test Multi-Network: mainnet rETH historic + anvil SimpleERC20 historic");

        // Require MAINNET_RPC_URL
        let mainnet_rpc = match std::env::var("MAINNET_RPC_URL") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                return Err(crate::tests::test_runner::SkipTest("MAINNET_RPC_URL not set; skipping multi-network test".to_string()).into());
            }
        };

        // Use a small subset of blocks for multi-network test (not full CSV like direct_rpc)
        // Just test a few blocks to verify multi-network functionality
        let expected_csv = std::env::var("DIRECT_RPC_EXPECTED_CSV")
            .unwrap_or_else(|_| "data/rocketpooleth-transfer.csv".to_string());
        
        // Get start block from CSV but limit range to just 20 blocks for faster testing
        let (csv_start_block, _csv_end_block) = derive_block_range_from_csv(&expected_csv)
            .context("Failed to derive block range from expected CSV")?;
        let mainnet_start_block = csv_start_block;
        let mainnet_end_block = csv_start_block + 20; // Just 20 blocks instead of full range
        let reth_address = "0xae78736cd615f374d3085123a210448e74fc6393";
        
        info!("Testing mainnet blocks {} to {} (limited range for multi-network test)", mainnet_start_block, mainnet_end_block);

        // Deploy SimpleERC20 on anvil and pre-feed transfers
        info!("Deploying SimpleERC20 on Anvil and pre-feeding transfers...");
        let anvil_contract = context.deploy_test_contract().await?;
        
        // Pre-feed some transfers using direct contract calls
        let num_transfers = 5;
        for i in 0..num_transfers {
            feed_transfer_on_anvil(&context.anvil.rpc_url, &anvil_contract, i).await?;
            context.anvil.mine_block().await?; // Mine to advance nonce
        }
        
        // Get current anvil block number
        let anvil_end_block = context.anvil.get_block_number().await?;
        info!("Anvil has {} blocks with {} transfers", anvil_end_block, num_transfers);

        // Build multi-network config: mainnet rETH + anvil SimpleERC20 (both historic)
        let config = build_multi_network_config(
            &mainnet_rpc,
            &context.anvil.rpc_url,
            reth_address,
            &anvil_contract,
            mainnet_start_block,
            mainnet_end_block,
            0, // anvil starts at 0
            anvil_end_block,
        );

        context.start_rindexer(config).await?;

        // Wait for both networks to complete indexing
        let sync_timeout = std::env::var("MULTI_NETWORK_SYNC_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(600);
        
        info!("Waiting for historic sync to complete on both networks (timeout: {}s)", sync_timeout);
        
        // For multi-network test, just verify we got SOME events, not exact CSV match
        // (We're testing multi-network coordination, not full data accuracy like direct_rpc)
        let reth_csv_path = produced_csv_path_for(context, "RocketPoolETH", "transfer");
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(sync_timeout);
        
        info!("Polling for rETH CSV to have at least 1 event in the 20-block window...");
        let produced_reth_hashes = loop {
            if start.elapsed() > timeout {
                return Err(anyhow::anyhow!("Timeout waiting for rETH CSV"));
            }
            
            match load_tx_hashes_from_csv(&reth_csv_path) {
                Ok(hashes) if !hashes.is_empty() => {
                    info!("✓ rETH CSV has {} events", hashes.len());
                    break hashes;
                }
                Ok(_) => {
                    info!("rETH CSV empty, waiting for events...");
                }
                Err(_) => {}
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        };

        info!("✓ Multi-network mainnet indexing validated ({} rETH events)", produced_reth_hashes.len());

        // Validate anvil SimpleERC20 has expected transfers
        let anvil_csv_path = produced_csv_path_for(context, "SimpleERC20", "transfer");
        let anvil_hashes = load_tx_hashes_from_csv(&anvil_csv_path)
            .context("Failed to load Anvil CSV")?;
        
        // Expect deployment transfer + num_transfers = num_transfers + 1
        let expected_anvil_count = num_transfers + 1;
        if anvil_hashes.len() < expected_anvil_count {
            return Err(anyhow::anyhow!("Anvil CSV has {} rows, expected at least {}", anvil_hashes.len(), expected_anvil_count));
        }
        info!("✓ Anvil SimpleERC20 CSV validated ({} rows)", anvil_hashes.len());

        info!("✓ Test Multi-Network PASSED: mainnet rETH ({} events in 20 blocks) + anvil SimpleERC20 ({} transfers) indexed on separate networks", 
              produced_reth_hashes.len(), expected_anvil_count);
        Ok(())
    })
}

fn build_multi_network_config(
    mainnet_rpc: &str,
    anvil_rpc: &str,
    reth_address: &str,
    anvil_contract: &str,
    mainnet_start_block: u64,
    mainnet_end_block: u64,
    anvil_start_block: u64,
    anvil_end_block: u64,
) -> crate::test_suite::RindexerConfig {
    use crate::test_suite::{RindexerConfig, NetworkConfig, StorageConfig, PostgresConfig, CsvConfig, NativeTransfersConfig, ContractConfig, ContractDetail, EventConfig};

    RindexerConfig {
        name: "multi_network_test".to_string(),
        project_type: "no-code".to_string(),
        config: serde_json::json!({}),
        timestamps: None,
        networks: vec![
            NetworkConfig { name: "ethereum".to_string(), chain_id: 1, rpc: mainnet_rpc.to_string() },
            NetworkConfig { name: "anvil".to_string(), chain_id: 31337, rpc: anvil_rpc.to_string() },
        ],
        storage: StorageConfig { 
            postgres: PostgresConfig { enabled: false }, 
            csv: CsvConfig { enabled: true } 
        },
        native_transfers: NativeTransfersConfig { enabled: false },
        contracts: vec![
            // rETH on mainnet (historic)
            ContractConfig {
                name: "RocketPoolETH".to_string(),
                details: vec![ContractDetail {
                    network: "ethereum".to_string(),
                    address: reth_address.to_string(),
                    start_block: mainnet_start_block.to_string(),
                    end_block: Some(mainnet_end_block.to_string()),
                }],
                abi: Some("./abis/ERC20.abi.json".to_string()),
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
            },
            // SimpleERC20 on anvil (historic)
            ContractConfig {
                name: "SimpleERC20".to_string(),
                details: vec![ContractDetail {
                    network: "anvil".to_string(),
                    address: anvil_contract.to_string(),
                    start_block: anvil_start_block.to_string(),
                    end_block: Some(anvil_end_block.to_string()),
                }],
                abi: Some("./abis/SimpleERC20.abi.json".to_string()),
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
            },
        ],
    }
}

async fn feed_transfer_on_anvil(rpc_url: &str, contract_address: &str, nonce: usize) -> Result<()> {
    use ethers::types::{Address, U256, TransactionRequest};
    use ethers::providers::{Provider, Http, Middleware};
    use ethers::signers::{LocalWallet, Signer};
    use ethers::middleware::MiddlewareBuilder;
    
    let private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    let wallet: LocalWallet = private_key.parse()?;
    let signer_address = wallet.address();
    let provider = Provider::<Http>::try_from(rpc_url)?
        .with_signer(wallet);
    
    let contract_addr: Address = contract_address.parse()?;
    let recipient = generate_test_address(nonce as u64);
    let amount = U256::from(1000u64);
    
    // Encode transfer(address,uint256)
    let mut data = vec![0xa9, 0x05, 0x9c, 0xbb]; // transfer selector
    let mut to_bytes = [0u8; 32];
    to_bytes[12..].copy_from_slice(recipient.as_bytes());
    data.extend_from_slice(&to_bytes);
    let mut value_bytes = [0u8; 32];
    let amount_bytes: [u8; 32] = amount.into();
    value_bytes.copy_from_slice(&amount_bytes);
    data.extend_from_slice(&value_bytes);
    
    // Get nonce for the account
    let tx_nonce = provider.get_transaction_count(signer_address, None).await?;
    
    let tx = TransactionRequest {
        from: Some(signer_address),
        to: Some(contract_addr.into()),
        data: Some(data.into()),
        gas: Some(100000u64.into()),
        nonce: Some(tx_nonce),
        gas_price: Some(20000000000u128.into()),
        value: None,
        chain_id: Some(31337u64.into()),
    };
    
    let _pending = provider.send_transaction(tx, None).await?;
    Ok(())
}

fn generate_test_address(counter: u64) -> ethers::types::Address {
    let mut bytes = [0u8; 20];
    bytes[0] = 0x42;
    bytes[1..9].copy_from_slice(&counter.to_be_bytes());
    ethers::types::Address::from(bytes)
}

fn produced_csv_path_for(context: &TestContext, contract_name: &str, event_slug_lowercase: &str) -> String {
    let file_name = format!("{}-{}.csv", contract_name.to_lowercase(), event_slug_lowercase);
    let path = context
        .get_csv_output_path()
        .join(contract_name)
        .join(file_name);
    path.to_string_lossy().to_string()
}

fn load_tx_hashes_from_csv(path: &str) -> Result<std::collections::BTreeSet<String>> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open CSV at {}", path))?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| anyhow::anyhow!("CSV missing header"))?;
    let headers: Vec<&str> = header.split(',').collect();
    let tx_idx = headers.iter().position(|h| *h == "tx_hash")
        .ok_or_else(|| anyhow::anyhow!("tx_hash column not found"))?;
    
    let mut set: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for line in lines {
        if line.trim().is_empty() { continue; }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= tx_idx { continue; }
        let tx = cols[tx_idx].trim().to_lowercase();
        if tx.is_empty() { continue; }
        set.insert(tx);
    }
    Ok(set)
}

fn derive_block_range_from_csv(path: &str) -> Result<(u64, u64)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Cannot open expected CSV at {}", path))?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let mut lines = content.lines();
    let header = lines.next().ok_or_else(|| anyhow::anyhow!("CSV missing header"))?;
    let headers: Vec<&str> = header.split(',').collect();
    let block_idx = headers.iter().position(|h| *h == "block_number")
        .ok_or_else(|| anyhow::anyhow!("block_number column not found"))?;

    let mut min_b: Option<u64> = None;
    let mut max_b: Option<u64> = None;
    for line in lines {
        if line.trim().is_empty() { continue; }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() <= block_idx { continue; }
        if let Ok(b) = cols[block_idx].parse::<u64>() {
            min_b = Some(min_b.map_or(b, |m| m.min(b)));
            max_b = Some(max_b.map_or(b, |m| m.max(b)));
        }
    }
    match (min_b, max_b) {
        (Some(s), Some(e)) => Ok((s, e)),
        _ => Err(anyhow::anyhow!("Could not derive block range from CSV")),
    }
}

