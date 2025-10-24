use anyhow::{Context, Result};
use ethers::middleware::MiddlewareBuilder;
use ethers::providers::{Http, Middleware, Provider};
use ethers::signers::{LocalWallet, Signer};
use ethers::types::{Address, TransactionRequest, U256};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::interval;
use tracing::{debug, info, warn};

pub struct LiveFeeder {
    anvil_url: String,
    private_key: String,
    contract_address: Option<Address>,
    tx_interval: Duration,
    mine_interval: Duration,
    stop_tx: Option<watch::Sender<bool>>,
}

impl LiveFeeder {
    pub fn new(anvil_url: String, private_key: String) -> Self {
        Self {
            anvil_url,
            private_key,
            contract_address: None,
            tx_interval: Duration::from_secs(2), // Submit tx every 2 seconds
            mine_interval: Duration::from_secs(1), // Mine block every 1 second
            stop_tx: None,
        }
    }

    pub fn with_contract(mut self, contract_address: Address) -> Self {
        self.contract_address = Some(contract_address);
        self
    }

    pub fn with_tx_interval(mut self, interval: Duration) -> Self {
        self.tx_interval = interval;
        self
    }

    pub fn with_mine_interval(mut self, interval: Duration) -> Self {
        self.mine_interval = interval;
        self
    }

    /// Start the live feeder in the background
    pub async fn start(&mut self) -> Result<()> {
        let (stop_tx, stop_rx) = watch::channel(false);
        self.stop_tx = Some(stop_tx);

        let anvil_url = self.anvil_url.clone();
        let private_key = self.private_key.clone();
        let contract_address = self.contract_address;
        let tx_interval = self.tx_interval;
        let mine_interval = self.mine_interval;

        info!(
            "Starting live feeder with tx_interval={:?}, mine_interval={:?}",
            tx_interval, mine_interval
        );

        // Spawn transaction submission task
        let tx_task = {
            let anvil_url = anvil_url.clone();
            let mut stop_rx = stop_rx.clone();
            tokio::spawn(async move {
                let mut tx_timer = interval(tx_interval);
                let mut tx_counter = 0u64;

                loop {
                    tokio::select! {
                        _ = tx_timer.tick() => {
                            if let Err(e) = Self::submit_test_transaction(&anvil_url, &private_key, contract_address, tx_counter).await {
                                warn!("Failed to submit transaction {}: {}", tx_counter, e);
                            } else {
                                debug!("Submitted transaction {}", tx_counter);
                                tx_counter += 1;
                            }
                        }
                        _ = stop_rx.changed() => {
                            if *stop_rx.borrow() {
                                info!("Transaction feeder stopped");
                                break;
                            }
                        }
                    }
                }
            })
        };

        // Spawn mining task
        let mine_task = {
            let anvil_url = anvil_url.clone();
            let mut stop_rx = stop_rx.clone();
            tokio::spawn(async move {
                let mut mine_timer = interval(mine_interval);
                let mut block_counter = 0u64;

                loop {
                    tokio::select! {
                        _ = mine_timer.tick() => {
                            if let Err(e) = Self::mine_block(&anvil_url).await {
                                warn!("Failed to mine block {}: {}", block_counter, e);
                            } else {
                                debug!("Mined block {}", block_counter);
                                block_counter += 1;
                            }
                        }
                        _ = stop_rx.changed() => {
                            if *stop_rx.borrow() {
                                info!("Mining feeder stopped");
                                break;
                            }
                        }
                    }
                }
            })
        };

        // Store the task handles so they keep running
        // We don't need to wait for them, they run in the background
        tokio::spawn(async move {
            let _ = tx_task.await;
        });
        tokio::spawn(async move {
            let _ = mine_task.await;
        });

        Ok(())
    }

    /// Stop the live feeder
    pub fn stop(&self) {
        if let Some(stop_tx) = &self.stop_tx {
            let _ = stop_tx.send(true);
        }
    }

    async fn submit_test_transaction(
        anvil_url: &str,
        private_key: &str,
        contract_address: Option<Address>,
        tx_counter: u64,
    ) -> Result<()> {
        // Create provider and derive the actual chain ID from the node
        let base_provider =
            Provider::<Http>::try_from(anvil_url).context("Failed to create provider")?;
        let chain_id = base_provider.get_chainid().await?.as_u64();

        // Prepare wallet configured with the correct chain ID
        let wallet: LocalWallet = private_key.parse().context("Invalid private key")?;
        let wallet = wallet.with_chain_id(chain_id);
        let signer_address = wallet.address();

        // Build signer-enabled provider
        let provider = base_provider.with_signer(wallet);

        if let Some(contract_addr) = contract_address {
            // Call the contract's transfer function to emit Transfer events
            let recipient = Self::generate_test_address(tx_counter);
            let transfer_amount = U256::from(1000u64); // Transfer 1000 tokens

            debug!(
                "Attempting contract transfer: to={}, amount={}, contract={}",
                recipient, transfer_amount, contract_addr
            );

            // Encode the transfer(address,uint256) function call
            let transfer_data = Self::encode_transfer_call(recipient, transfer_amount);
            debug!("Encoded transfer data: {:?}", hex::encode(&transfer_data));

            // Get the current nonce for the account
            let nonce = provider.get_transaction_count(signer_address, None).await?;

            let tx_request = TransactionRequest {
                from: Some(signer_address),
                to: Some(contract_addr.into()),
                data: Some(transfer_data.clone().into()),
                gas: Some(100000u64.into()),
                nonce: Some(nonce),
                gas_price: Some(20000000000u128.into()),
                value: None,
                chain_id: None, // let signer/provider supply the correct chain id
            };

            let pending_tx = match provider.send_transaction(tx_request, None).await {
                Ok(tx) => tx,
                Err(e) => {
                    warn!("Transaction failed with error: {:?}", e);
                    return Err(e).with_context(|| {
                        format!(
                            "Failed to send contract transaction to {} with data: {:?}",
                            contract_addr, transfer_data
                        )
                    });
                }
            };

            debug!("Contract transfer transaction submitted: {:?}", pending_tx.tx_hash());
        } else {
            // Fallback to ETH transfer if no contract address
            let recipient = Self::generate_test_address(tx_counter);
            let tx_request = TransactionRequest {
                from: Some(signer_address),
                to: Some(recipient.into()),
                value: Some(1000000000000000u64.into()), // 0.001 ETH
                gas: Some(21000u64.into()),
                gas_price: Some(20000000000u128.into()),
                nonce: Some(provider.get_transaction_count(signer_address, None).await?),
                data: None,
                chain_id: None, // let signer/provider supply the correct chain id
            };

            let pending_tx = provider
                .send_transaction(tx_request, None)
                .await
                .context("Failed to send ETH transaction")?;

            debug!("ETH transfer transaction submitted: {:?}", pending_tx.tx_hash());
        }

        Ok(())
    }

    async fn mine_block(anvil_url: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let mine_request = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "evm_mine",
            "params": [],
            "id": 1
        });

        let response = client
            .post(anvil_url)
            .header("Content-Type", "application/json")
            .json(&mine_request)
            .send()
            .await
            .context("Failed to send mine request")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Failed to mine block: HTTP {} - {}", status, body);
        }

        Ok(())
    }

    fn encode_transfer_call(to: Address, value: U256) -> Vec<u8> {
        // ABI encoding for transfer(address,uint256)
        // Function selector: transfer(address,uint256) = 0xa9059cbb
        let mut data = vec![0xa9, 0x05, 0x9c, 0xbb];

        // Encode address parameter (32 bytes, right-padded)
        let mut to_bytes = [0u8; 32];
        to_bytes[12..].copy_from_slice(to.as_bytes());
        data.extend_from_slice(&to_bytes);

        // Encode uint256 parameter (32 bytes)
        let mut value_bytes = [0u8; 32];
        let value_bytes_be: [u8; 32] = value.into();
        value_bytes.copy_from_slice(&value_bytes_be);
        data.extend_from_slice(&value_bytes);

        data
    }

    fn generate_test_address(tx_counter: u64) -> Address {
        // Generate a deterministic test address based on tx_counter
        let mut bytes = [0u8; 20];
        bytes[0] = 0x42; // Prefix to make it look like a real address
        bytes[1..8].copy_from_slice(&tx_counter.to_be_bytes()[..7]);
        Address::from(bytes)
    }
}

impl Drop for LiveFeeder {
    fn drop(&mut self) {
        self.stop();
    }
}
