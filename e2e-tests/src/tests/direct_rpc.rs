use anyhow::{Result, Context};
use tracing::{info, warn};
use std::pin::Pin;
use std::future::Future;

use crate::test_suite::TestContext;
use crate::tests::registry::{TestDefinition, TestModule};

pub struct DirectRpcTests;

impl TestModule for DirectRpcTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_direct_rpc",
                "Direct RPC realism: Rocket Pool rETH Transfer vs expected CSV",
                direct_rpc_test,
            ).with_timeout(900),
        ]
    }
}

fn direct_rpc_test(context: &mut TestContext) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Test 9: Direct RPC Realism (ERC20 Transfer)");

        // Require MAINNET_RPC_URL in env; skip cleanly if missing
        let mainnet_rpc = match std::env::var("MAINNET_RPC_URL") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => {
                return Err(crate::tests::test_runner::SkipTest("MAINNET_RPC_URL not set; skipping direct RPC test".to_string()).into());
            }
        };

        // Expected CSV file (default to provided rETH sample)
        let expected_csv = std::env::var("DIRECT_RPC_EXPECTED_CSV")
            .unwrap_or_else(|_| "data/rocketpooleth-transfer.csv".to_string());

        // Allow env override for contract and topic (default to rETH)
        let contract_address = std::env::var("DIRECT_RPC_CONTRACT_ADDRESS")
            .unwrap_or_else(|_| "0xae78736cd615f374d3085123a210448e74fc6393".to_string());
        // Topic not needed when comparing against expected CSV

        // Derive start/end from expected CSV to ensure determinism
        let (start_block, end_block_inclusive) = derive_block_range_from_csv(&expected_csv)
            .context("Failed to derive block range from expected CSV")?;
        let end_block = Some(end_block_inclusive);

        // Build config targeting MAINNET directly
        let config = build_direct_rpc_config(&mainnet_rpc, &contract_address, start_block, end_block);
        context.start_rindexer(config).await?;

        // Wait until historical indexing completes by log (configurable)
        let sync_timeout = std::env::var("DIRECT_RPC_SYNC_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(600);
        context.wait_for_sync_completion(sync_timeout).await?;

        // Compare produced CSV vs expected CSV using tx_hash only (log_index often empty in fixtures)
        let produced_csv_path = produced_csv_path_for(context, "ERC20", "transfer");
        let expected_hashes = load_tx_hashes_from_csv(&expected_csv)
            .context("Failed to load expected CSV tx hashes")?;
        let produced_hashes = load_tx_hashes_from_csv(&produced_csv_path)
            .with_context(|| format!("Failed to load produced CSV tx hashes from {:?}", produced_csv_path))?;

        info!("Expected rows={}, Produced rows={}", expected_hashes.len(), produced_hashes.len());
        if expected_hashes != produced_hashes {
            // If sizes differ or sets differ, attempt to debug by counts
            warn!("CSV mismatch: expected {} rows, produced {} rows", expected_hashes.len(), produced_hashes.len());
            return Err(anyhow::anyhow!("Produced CSV does not match expected CSV"));
        }

        info!("✓ Test 9 PASSED: Direct RPC Transfer matched expected CSV");
        Ok(())
    })
}

fn build_direct_rpc_config(rpc_url: &str, contract_address: &str, start_block: u64, end_block: Option<u64>) -> crate::test_suite::RindexerConfig {
    use crate::test_suite::{RindexerConfig, NetworkConfig, StorageConfig, PostgresConfig, CsvConfig, NativeTransfersConfig, ContractConfig, ContractDetail, EventConfig};

    RindexerConfig {
        name: "direct_rpc_test".to_string(),
        project_type: "no-code".to_string(),
        config: serde_json::json!({}),
        timestamps: None,
        networks: vec![NetworkConfig { name: "mainnet".to_string(), chain_id: 1, rpc: rpc_url.to_string() }],
        storage: StorageConfig { postgres: PostgresConfig { enabled: false }, csv: CsvConfig { enabled: true } },
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

// Helpers to load CSV pairs and derive range
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
    let tx_idx = headers.iter().position(|h| *h == "tx_hash").ok_or_else(|| anyhow::anyhow!("tx_hash column not found"))?;
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
    let block_idx = headers.iter().position(|h| *h == "block_number").ok_or_else(|| anyhow::anyhow!("block_number column not found"))?;

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

