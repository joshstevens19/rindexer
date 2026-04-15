use anyhow::{Context, Result};
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};
use tracing::info;

use crate::test_suite::{
    ContractConfig, ContractDetail, CsvConfig, EventConfig, GlobalConfig,
    NativeTransferNetworkDetail, NativeTransfersConfig, NetworkConfig, RindexerConfig,
    StorageConfig, TestContext,
};
use crate::tests::helpers::{
    generate_test_address, parse_transfer_csv, produced_csv_path_for, validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct NativeTransferTests;

impl TestModule for NativeTransferTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_native_transfer_csv_no_panic",
            "Native transfers + no-code CSV: Block-only trace batches must not panic the callback",
            native_transfer_csv_no_panic_test,
        )
        .with_timeout(120)]
    }
}

/// Regression test for a panic in the no_code trace callback at
/// `core/src/indexer/no_code.rs`.
///
/// `native_transfer_block_consumer` fires `trigger_event` with a `Vec<TraceResult::Block>`
/// batch before firing the `Vec<TraceResult::NativeTransfer>` batch. Previously the callback
/// filtered the incoming batch for `NativeTransfer` entries only and called `.next().unwrap()`
/// on the result, which panicked on the first Block-only batch.
///
/// With the fix, both variants produce the metadata the callback needs, so processing succeeds
/// and the CSV gets written. This test enables `native_transfers` against a local anvil node
/// and asserts the native-transfer CSV ends up with one row per plain ETH transfer we sent.
fn native_transfer_csv_no_panic_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Native Transfer no-code CSV regression test");

        // A contract is included to get a reliable `100.00% progress` sync signal and to
        // exercise the trace path alongside a normal event path. The contract deployment
        // emits a Transfer event in its constructor but does not move ETH, so the native
        // transfer CSV is unaffected by it.
        let contract_address = context.deploy_test_contract().await?;

        // Three plain ETH transfers to deterministic recipients, mined explicitly to avoid
        // relying on anvil's 1s auto-mine timing.
        let recipients: Vec<ethers::types::Address> = (0..3).map(generate_test_address).collect();
        let one_eth = U256::from(1_000_000_000_000_000_000u64);

        let mut expected_tx_hashes = Vec::new();
        for (i, recipient) in recipients.iter().enumerate() {
            let tx_hash = context.anvil.send_eth_transfer(recipient, one_eth).await?;
            context.anvil.mine_block().await?;
            info!("ETH transfer {}: 1 ETH to {:?} (tx={})", i, recipient, tx_hash);
            expected_tx_hashes.push(tx_hash);
        }

        let end_block = context.anvil.get_block_number().await?;
        info!("Chain tip after seeding: block {}", end_block);

        // Bound both the contract and native_transfers with end_block so neither enters
        // live phase. This keeps the test's scope limited to historical indexing only,
        // which is where the panic was triggered.
        let config = RindexerConfig {
            name: "native_transfer_no_panic".to_string(),
            project_type: "no-code".to_string(),
            config: serde_json::json!({}),
            timestamps: None,
            networks: vec![NetworkConfig {
                name: "anvil".to_string(),
                chain_id: 31337,
                rpc: context.anvil.rpc_url.clone(),
            }],
            global: GlobalConfig { health_port: context.health_port },
            storage: StorageConfig {
                postgres: None,
                csv: CsvConfig { enabled: true },
                clickhouse: None,
            },
            native_transfers: NativeTransfersConfig {
                enabled: true,
                networks: Some(vec![NativeTransferNetworkDetail {
                    network: "anvil".to_string(),
                    start_block: Some("0".to_string()),
                    end_block: Some(end_block.to_string()),
                }]),
                generate_csv: Some(true),
            },
            contracts: vec![ContractConfig {
                name: "SimpleERC20".to_string(),
                details: vec![ContractDetail {
                    network: "anvil".to_string(),
                    address: contract_address.clone(),
                    start_block: "0".to_string(),
                    end_block: Some(end_block.to_string()),
                }],
                abi: Some("./abis/SimpleERC20.abi.json".to_string()),
                reorg_safe_distance: None,
                include_events: Some(vec![EventConfig { name: "Transfer".to_string() }]),
                tables: None,
            }],
        };

        context.start_rindexer(config).await?;

        // This returns early on process death (catches a panic that kills rindexer) or
        // when progress hits 100% on any pipeline (contract or native transfer).
        context.wait_for_sync_completion(30).await?;

        // Contract's 100% progress typically fires before native transfer's, so poll the
        // native transfer CSV here until it has the expected rows. This also catches the
        // case where rindexer is still alive but the callback panicked on a spawned task
        // and the native pipeline never wrote anything.
        let csv_path = produced_csv_path_for(context, "EvmTraces", "nativetransfer");
        let expected_rows = recipients.len();

        let start = Instant::now();
        let poll_timeout = Duration::from_secs(30);
        loop {
            if start.elapsed() > poll_timeout {
                let current = std::fs::read_to_string(&csv_path).ok();
                return Err(anyhow::anyhow!(
                    "Timeout waiting for {} native transfer rows at {}. Current content: {:?}",
                    expected_rows,
                    csv_path,
                    current
                ));
            }
            if std::path::Path::new(&csv_path).exists() {
                if let Ok(content) = std::fs::read_to_string(&csv_path) {
                    let data_rows = content.lines().count().saturating_sub(1);
                    if data_rows >= expected_rows {
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let (headers, rows) =
            parse_transfer_csv(&csv_path).context("Failed to parse native transfer CSV")?;

        validate_csv_structure(&headers, &rows)
            .context("Native transfer CSV failed structural validation")?;

        if rows.len() != expected_rows {
            return Err(anyhow::anyhow!(
                "Expected exactly {} native transfer rows, got {}",
                expected_rows,
                rows.len()
            ));
        }

        // Each recipient must appear as a `to` address in exactly one row, and each
        // transaction we sent must appear exactly once. Order-independent checks so
        // the test is resilient to any minor ordering differences the pipeline might
        // introduce across variants.
        let got_to: std::collections::BTreeSet<String> =
            rows.iter().map(|r| r.to.clone()).collect();
        let expected_to: std::collections::BTreeSet<String> =
            recipients.iter().map(|r| format!("0x{}", hex::encode(r.as_bytes()))).collect();
        if got_to != expected_to {
            return Err(anyhow::anyhow!(
                "Row recipients don't match: got {:?}, expected {:?}",
                got_to,
                expected_to
            ));
        }

        let got_tx: std::collections::BTreeSet<String> =
            rows.iter().map(|r| r.tx_hash.clone()).collect();
        let expected_tx: std::collections::BTreeSet<String> =
            expected_tx_hashes.iter().cloned().collect();
        if got_tx != expected_tx {
            return Err(anyhow::anyhow!(
                "Row tx_hashes don't match: got {:?}, expected {:?}",
                got_tx,
                expected_tx
            ));
        }

        info!(
            "Native Transfer CSV regression test PASSED: {} rows, recipients and tx hashes match",
            rows.len()
        );
        Ok(())
    })
}
