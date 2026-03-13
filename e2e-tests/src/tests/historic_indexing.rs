use anyhow::Result;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::anvil_setup::ANVIL_DEPLOYER_ADDRESS;
use crate::test_suite::TestContext;
use crate::tests::helpers::{
    self, format_address, generate_test_address, parse_transfer_csv, validate_csv_structure,
};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct HistoricIndexingTests;

impl TestModule for HistoricIndexingTests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![TestDefinition::new(
            "test_historic_indexing_correctness",
            "Deploy contract + 5 transfers, validate every CSV field",
            historic_indexing_correctness_test,
        )
        .with_timeout(120)]
    }
}

fn historic_indexing_correctness_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Historic Indexing Correctness Test");

        // Deploy contract (creates mint Transfer from 0x0 -> deployer)
        let contract_address = context.deploy_test_contract().await?;

        // Execute 5 transfers with varying amounts (not constant!)
        let amounts: Vec<u64> = vec![1000, 2000, 3000, 4000, 5000];
        let recipients: Vec<ethers::types::Address> =
            (0..5).map(|i| generate_test_address(i)).collect();

        let mut tx_hashes = Vec::new();
        for (i, (recipient, amount)) in recipients.iter().zip(amounts.iter()).enumerate() {
            let tx_hash = context
                .anvil
                .send_transfer(&contract_address, recipient, U256::from(*amount))
                .await?;
            context.anvil.mine_block().await?;
            info!("Transfer {}: {} tokens to {}", i, amount, format_address(recipient));
            tx_hashes.push(tx_hash);
        }

        let end_block = context.anvil.get_block_number().await?;

        // Configure and start indexer with end_block (historic only)
        let mut config = context.create_contract_config(&contract_address);
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        context.start_rindexer(config).await?;
        context.wait_for_sync_completion(30).await?;

        // Parse and validate CSV
        let csv_path = helpers::produced_csv_path_for(context, "SimpleERC20", "transfer");
        let (headers, rows) = parse_transfer_csv(&csv_path)?;

        // 1. Structural validation (headers, formats, ordering, dedup)
        validate_csv_structure(&headers, &rows)?;
        info!("CSV structure validation passed ({} rows)", rows.len());

        // 2. Row count: exactly 6 (1 mint + 5 transfers)
        if rows.len() != 6 {
            return Err(anyhow::anyhow!("Expected 6 rows (1 mint + 5 transfers), got {}", rows.len()));
        }

        let deployer = ANVIL_DEPLOYER_ADDRESS;
        let zero_addr = "0x0000000000000000000000000000000000000000";
        let contract_lower = contract_address.to_lowercase();

        // 3. Mint event (row 0)
        let mint = &rows[0];
        if mint.from != zero_addr {
            return Err(anyhow::anyhow!("Mint from should be zero address, got: {}", mint.from));
        }
        if mint.to != deployer {
            return Err(anyhow::anyhow!("Mint to should be deployer {}, got: {}", deployer, mint.to));
        }
        if mint.contract_address != contract_lower {
            return Err(anyhow::anyhow!("Wrong contract_address in mint row"));
        }
        if mint.network != "anvil" {
            return Err(anyhow::anyhow!("Wrong network in mint row: {}", mint.network));
        }
        info!("Mint event validated: 0x0 -> deployer, value={}", mint.value);

        // 4. Transfer events (rows 1-5): validate fields match what we sent
        for (i, row) in rows.iter().skip(1).enumerate() {
            let expected_to = format_address(&recipients[i]);
            let expected_value = amounts[i].to_string();

            if row.from != deployer {
                return Err(anyhow::anyhow!(
                    "Transfer {}: from should be deployer, got: {}",
                    i,
                    row.from
                ));
            }
            if row.to != expected_to {
                return Err(anyhow::anyhow!(
                    "Transfer {}: to should be {}, got: {}",
                    i,
                    expected_to,
                    row.to
                ));
            }
            if row.value != expected_value {
                return Err(anyhow::anyhow!(
                    "Transfer {}: value should be {}, got: {}",
                    i,
                    expected_value,
                    row.value
                ));
            }
            if row.contract_address != contract_lower {
                return Err(anyhow::anyhow!("Transfer {}: wrong contract_address", i));
            }
            if row.network != "anvil" {
                return Err(anyhow::anyhow!("Transfer {}: wrong network: {}", i, row.network));
            }
        }

        // 5. Block numbers should be monotonically non-decreasing
        // (already checked by validate_ordering, but let's verify specific values)
        for window in rows.windows(2) {
            if window[1].block_number < window[0].block_number {
                return Err(anyhow::anyhow!(
                    "Block numbers not monotonic: {} then {}",
                    window[0].block_number,
                    window[1].block_number
                ));
            }
        }

        // 6. Verify block hashes match actual chain state
        for row in &rows {
            let block_info = context.anvil.get_block(row.block_number).await?;
            if row.block_hash != block_info.hash {
                return Err(anyhow::anyhow!(
                    "Block {} hash mismatch: CSV has '{}', chain has '{}'",
                    row.block_number,
                    row.block_hash,
                    block_info.hash
                ));
            }
        }

        info!(
            "Historic Indexing Correctness Test PASSED: 6 events, all fields validated, \
             block hashes verified against chain"
        );
        Ok(())
    })
}
