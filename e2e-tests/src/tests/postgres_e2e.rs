use anyhow::Result;
use ethers::middleware::MiddlewareBuilder;
use ethers::providers::{Http, Middleware, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::TransactionRequest;
use ethers::types::U256;
use std::future::Future;
use std::pin::Pin;
use tracing::info;

use crate::anvil_setup::{ANVIL_DEFAULT_PRIVATE_KEY, ANVIL_DEPLOYER_ADDRESS};
use crate::test_suite::{ContractConfig, ContractDetail, EventConfig, PostgresConfig, TestContext};
use crate::tests::helpers::{self, format_address, generate_test_address};
use crate::tests::registry::{TestDefinition, TestModule};

pub struct PostgresE2ETests;

impl TestModule for PostgresE2ETests {
    fn get_tests() -> Vec<TestDefinition> {
        vec![
            TestDefinition::new(
                "test_postgres_field_accuracy",
                "Postgres: deploy+5 transfers, validate every field in DB matches chain state",
                postgres_field_accuracy_test,
            )
            .with_timeout(240),
            TestDefinition::new(
                "test_postgres_indexed_string_topic_hash",
                "Postgres: verify indexed string topics are stored as keccak hex strings",
                postgres_indexed_string_topic_hash_test,
            )
            .with_timeout(240),
        ]
    }
}

/// Validates that Postgres storage captures all event fields accurately.
/// Deploys contract + 5 transfers with varying amounts, indexes into Postgres,
/// then queries and validates: from, to, value, block_number, tx_hash, log_index,
/// address casing, and row count.
fn postgres_field_accuracy_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Postgres Field Accuracy Test");

        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(container_name.clone());
        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        // Deploy contract + 5 transfers with varying amounts
        let contract_address = context.deploy_test_contract().await?;

        let amounts: Vec<u64> = vec![1000, 2000, 3000, 4000, 5000];
        let recipients: Vec<ethers::types::Address> = (0..5).map(generate_test_address).collect();

        for (recipient, amount) in recipients.iter().zip(amounts.iter()) {
            context.anvil.send_transfer(&contract_address, recipient, U256::from(*amount)).await?;
            context.anvil.mine_block().await?;
        }

        let end_block = context.anvil.get_block_number().await?;

        // Configure with Postgres enabled, CSV disabled
        let mut config = context.create_contract_config(&contract_address);
        config.storage.postgres = Some(crate::test_suite::PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;
        if let Some(contract) = config.contracts.get_mut(0) {
            if let Some(detail) = contract.details.get_mut(0) {
                detail.end_block = Some(end_block.to_string());
            }
        }

        // Start rindexer with Postgres env vars
        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;
        r.start_indexer().await?;

        context.rindexer = Some(r);
        context.wait_for_sync_completion(60).await?;

        // Connect to Postgres
        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Query all transfer rows ordered by block_number, log_index
        // Note: "from" and "to" are SQL reserved words, must be quoted
        let rows = client
            .query(
                "SELECT contract_address, \"from\", \"to\", value, \
                 tx_hash, block_number::BIGINT, block_hash, log_index, network \
                 FROM contract_test_simple_erc_20.transfer \
                 ORDER BY block_number ASC, log_index ASC",
                &[],
            )
            .await?;

        // Expected: 1 mint + 5 transfers = 6
        if rows.len() != 6 {
            return Err(anyhow::anyhow!(
                "Expected 6 rows in Postgres (1 mint + 5 transfers), got {}",
                rows.len()
            ));
        }
        info!("Postgres has {} rows", rows.len());

        let deployer = ANVIL_DEPLOYER_ADDRESS;
        let zero_addr = "0x0000000000000000000000000000000000000000";
        let contract_lower = contract_address.to_lowercase();

        // Validate mint row (row 0)
        let mint = &rows[0];
        let mint_from: String = mint.get("from");
        let mint_to: String = mint.get("to");
        let mint_contract: String = mint.get("contract_address");
        let mint_network: String = mint.get("network");

        if mint_from.to_lowercase() != zero_addr {
            return Err(anyhow::anyhow!("Mint from should be zero address, got: {}", mint_from));
        }
        if mint_to.to_lowercase() != deployer.to_lowercase() {
            return Err(anyhow::anyhow!(
                "Mint to should be deployer {}, got: {}",
                deployer,
                mint_to
            ));
        }
        if mint_contract.to_lowercase() != contract_lower {
            return Err(anyhow::anyhow!(
                "Mint contract_address should be {}, got: {}",
                contract_lower,
                mint_contract
            ));
        }
        if mint_network != "anvil" {
            return Err(anyhow::anyhow!("Mint network should be 'anvil', got: '{}'", mint_network));
        }
        info!("Mint row validated: 0x0 -> deployer");

        // Validate transfer rows (rows 1-5)
        for (i, row) in rows.iter().skip(1).enumerate() {
            let from: String = row.get("from");
            let to: String = row.get("to");
            let value: String = row.get("value");
            let contract_addr: String = row.get("contract_address");
            let network: String = row.get("network");
            let block_number: i64 = row.get("block_number");

            let expected_to = format_address(&recipients[i]);
            let expected_value = amounts[i].to_string();

            if from.to_lowercase() != deployer.to_lowercase() {
                return Err(anyhow::anyhow!(
                    "Transfer {}: from should be deployer, got: {}",
                    i,
                    from
                ));
            }
            if to.to_lowercase() != expected_to.to_lowercase() {
                return Err(anyhow::anyhow!(
                    "Transfer {}: to should be {}, got: {}",
                    i,
                    expected_to,
                    to
                ));
            }
            if value != expected_value {
                return Err(anyhow::anyhow!(
                    "Transfer {}: value should be {}, got: {}",
                    i,
                    expected_value,
                    value
                ));
            }
            if contract_addr.to_lowercase() != contract_lower {
                return Err(anyhow::anyhow!(
                    "Transfer {}: wrong contract_address: {}",
                    i,
                    contract_addr
                ));
            }
            if network != "anvil" {
                return Err(anyhow::anyhow!(
                    "Transfer {}: network should be 'anvil', got: '{}'",
                    i,
                    network
                ));
            }
            if block_number <= 0 {
                return Err(anyhow::anyhow!(
                    "Transfer {}: block_number should be positive, got: {}",
                    i,
                    block_number
                ));
            }
        }

        // Verify block_numbers are monotonically non-decreasing
        let block_numbers: Vec<i64> = rows.iter().map(|r| r.get("block_number")).collect();
        for window in block_numbers.windows(2) {
            if window[1] < window[0] {
                return Err(anyhow::anyhow!(
                    "Block numbers not monotonic: {} then {}",
                    window[0],
                    window[1]
                ));
            }
        }

        // Verify all tx_hashes are valid hex strings
        for (i, row) in rows.iter().enumerate() {
            let tx_hash: String = row.get("tx_hash");
            if !tx_hash.starts_with("0x") || tx_hash.len() != 66 {
                return Err(anyhow::anyhow!("Row {}: invalid tx_hash format: {}", i, tx_hash));
            }
        }

        // Verify no duplicate (tx_hash, log_index) pairs
        let mut identities: std::collections::BTreeSet<(String, String)> =
            std::collections::BTreeSet::new();
        for (i, row) in rows.iter().enumerate() {
            let tx_hash: String = row.get("tx_hash");
            let log_index: String = row.get("log_index");
            if !identities.insert((tx_hash.clone(), log_index.clone())) {
                return Err(anyhow::anyhow!(
                    "Row {}: duplicate (tx_hash, log_index): ({}, {})",
                    i,
                    tx_hash,
                    log_index
                ));
            }
        }

        info!(
            "Postgres Field Accuracy Test PASSED: 6 rows, all fields validated, \
             addresses correct, values match, no duplicates"
        );
        Ok(())
    })
}

fn postgres_indexed_string_topic_hash_test(
    context: &mut TestContext,
) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
    Box::pin(async move {
        info!("Running Postgres Indexed String Topic Hash Test");

        let (container_name, pg_port) = match crate::docker::start_postgres_container().await {
            Ok(v) => v,
            Err(e) => {
                return Err(crate::tests::test_runner::SkipTest(format!(
                    "Docker not available: {}",
                    e
                ))
                .into());
            }
        };
        context.register_container(container_name.clone());
        crate::docker::wait_for_postgres_ready(pg_port, 10).await?;

        let contract_address = context.anvil.deploy_contract("MessageEmitter").await?;

        let mut expected_rows = Vec::new();
        for _ in 0..3 {
            let tx_hash = send_emit_message(context, &contract_address).await?;
            context.anvil.mine_block().await?;
            let receipt = context.anvil.get_receipt(&tx_hash).await?;
            expected_rows.push(receipt);
        }

        let end_block = context.anvil.get_block_number().await?;

        let mut config = context.create_minimal_config();
        config.name = "message_emitter_test".to_string();
        config.storage.postgres = Some(PostgresConfig { enabled: true });
        config.storage.csv.enabled = false;
        config.contracts = vec![ContractConfig {
            name: "MessageEmitter".to_string(),
            details: vec![ContractDetail {
                network: "anvil".to_string(),
                address: contract_address.clone(),
                start_block: "0".to_string(),
                end_block: Some(end_block.to_string()),
            }],
            abi: Some("./abis/MessageEmitter.abi.json".to_string()),
            reorg_safe_distance: None,
            include_events: Some(vec![EventConfig { name: "MessageEmitted".to_string() }]),
            tables: None,
            streams: None,
        }];

        let mut r = crate::rindexer_client::RindexerInstance::new(
            &context.rindexer_binary,
            context.project_path.clone(),
        );
        for (k, v) in crate::docker::postgres_env_vars(pg_port) {
            r = r.with_env(&k, &v);
        }

        helpers::copy_abis_to_project(&context.project_path)?;
        let config_path = context.project_path.join("rindexer.yaml");
        let yaml = serde_yaml::to_string(&config)?;
        std::fs::write(&config_path, yaml)?;
        r.start_indexer().await?;

        context.rindexer = Some(r);
        context.wait_for_sync_completion(60).await?;

        let conn_str = format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            pg_port
        );
        let (client, connection) =
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client
            .query(
                "SELECT contract_address, indexed_message, unindexed_message, \
                 tx_hash, block_number::BIGINT, block_hash, log_index, network \
                 FROM message_emitter_test_message_emitter.message_emitted \
                 ORDER BY block_number ASC, log_index ASC",
                &[],
            )
            .await?;

        if rows.len() != expected_rows.len() {
            return Err(anyhow::anyhow!(
                "Expected {} rows in Postgres, got {}",
                expected_rows.len(),
                rows.len()
            ));
        }

        let expected_topic_hash =
            format!("0x{}", hex::encode(ethers::utils::keccak256("a message")));
        for (i, (row, receipt)) in rows.iter().zip(expected_rows.iter()).enumerate() {
            let stored_contract_address: String = row.get("contract_address");
            let indexed_message: String = row.get("indexed_message");
            let unindexed_message: String = row.get("unindexed_message");
            let tx_hash: String = row.get("tx_hash");
            let block_number: i64 = row.get("block_number");
            let block_hash: String = row.get("block_hash");
            let log_index: String = row.get("log_index");
            let network: String = row.get("network");

            if stored_contract_address.to_lowercase() != contract_address.to_lowercase() {
                return Err(anyhow::anyhow!(
                    "Row {}: wrong contract_address: {}",
                    i,
                    stored_contract_address
                ));
            }
            if indexed_message != expected_topic_hash {
                return Err(anyhow::anyhow!(
                    "Row {}: indexed_message should be {}, got {}",
                    i,
                    expected_topic_hash,
                    indexed_message
                ));
            }
            if unindexed_message != "a message" {
                return Err(anyhow::anyhow!(
                    "Row {}: unindexed_message should be 'a message', got {}",
                    i,
                    unindexed_message
                ));
            }
            if tx_hash != receipt.transaction_hash {
                return Err(anyhow::anyhow!(
                    "Row {}: tx_hash should be {}, got {}",
                    i,
                    receipt.transaction_hash,
                    tx_hash
                ));
            }
            if block_number != receipt.block_number as i64 {
                return Err(anyhow::anyhow!(
                    "Row {}: block_number should be {}, got {}",
                    i,
                    receipt.block_number,
                    block_number
                ));
            }
            if block_hash != receipt.block_hash {
                return Err(anyhow::anyhow!(
                    "Row {}: block_hash should be {}, got {}",
                    i,
                    receipt.block_hash,
                    block_hash
                ));
            }
            if log_index != receipt.log_index_start.to_string() {
                return Err(anyhow::anyhow!(
                    "Row {}: log_index should be {}, got {}",
                    i,
                    receipt.log_index_start,
                    log_index
                ));
            }
            if network != "anvil" {
                return Err(anyhow::anyhow!(
                    "Row {}: network should be 'anvil', got {}",
                    i,
                    network
                ));
            }
        }

        info!(
            "Postgres Indexed String Topic Hash Test PASSED: {} rows, indexed string stored as keccak hex",
            rows.len()
        );
        Ok(())
    })
}

async fn send_emit_message(context: &TestContext, contract_address: &str) -> Result<String> {
    let base_provider = Provider::<Http>::try_from(&context.anvil.rpc_url)?;
    let chain_id = base_provider.get_chainid().await?.as_u64();

    let wallet: LocalWallet = ANVIL_DEFAULT_PRIVATE_KEY.parse()?;
    let wallet = wallet.with_chain_id(chain_id);
    let signer_address = wallet.address();
    let provider = base_provider.with_signer(wallet);

    let contract_addr: ethers::types::Address = contract_address.parse()?;
    let nonce = provider.get_transaction_count(signer_address, None).await?;
    let tx = TransactionRequest {
        from: Some(signer_address),
        to: Some(contract_addr.into()),
        data: Some(vec![0xa1, 0x21, 0xff, 0x51].into()),
        gas: Some(100000u64.into()),
        nonce: Some(nonce),
        gas_price: Some(20000000000u128.into()),
        value: None,
        chain_id: None,
    };

    let pending = provider.send_transaction(tx, None).await?;
    Ok(format!("{:?}", pending.tx_hash()).to_lowercase())
}
