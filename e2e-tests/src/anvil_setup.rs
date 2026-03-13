use anyhow::{Context, Result};
use std::net::TcpListener;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;
use tracing::{debug, error, info};

/// Default Anvil private key (account 0 from the standard mnemonic).
pub const ANVIL_DEFAULT_PRIVATE_KEY: &str =
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

/// Default Anvil deployer address (account 0).
pub const ANVIL_DEPLOYER_ADDRESS: &str = "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266";

/// Block metadata returned by `get_block`.
#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
}

/// Transaction receipt.
#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_hash: String,
    pub status: bool,
    pub log_index_start: u64,
    pub log_count: u64,
}

pub struct AnvilInstance {
    pub rpc_url: String,
    pub port: u16,
    process: Option<tokio::process::Child>,
}

impl AnvilInstance {
    /// Start a local Anvil instance on a dynamically allocated free port.
    /// Uses `--block-time 1` for auto-mining (suitable for live indexing tests).
    pub async fn start_local() -> Result<Self> {
        Self::start_with_args(&["--block-time", "1"]).await
    }

    /// Start a local Anvil instance with manual mining only.
    /// Uses `--no-mining` — blocks are only mined on explicit `evm_mine` calls.
    /// Use this for deterministic test scenarios where you need full control.
    #[allow(dead_code)]
    pub async fn start_local_manual_mining() -> Result<Self> {
        Self::start_with_args(&["--no-mining"]).await
    }

    /// Disable auto-mining. Transactions will stay in the mempool until `mine_block()`.
    pub async fn set_automine(&self, enabled: bool) -> Result<()> {
        self.rpc_call("evm_setAutomine", serde_json::json!([enabled])).await?;
        Ok(())
    }

    async fn start_with_args(extra_args: &[&str]) -> Result<Self> {
        let port = allocate_free_port()?;
        info!("Starting local Anvil instance on port {}", port);

        let mut cmd = TokioCommand::new("anvil");
        cmd.arg("--port")
            .arg(port.to_string())
            .arg("--chain-id")
            .arg("31337")
            .arg("--accounts")
            .arg("10")
            .arg("--balance")
            .arg("10000")
            .arg("--gas-limit")
            .arg("30000000")
            .arg("--gas-price")
            .arg("1000000000");

        for arg in extra_args {
            cmd.arg(arg);
        }

        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = cmd.spawn().context("Failed to start Anvil — is Foundry installed?")?;

        Self::start_log_streaming(&mut child).await;

        sleep(Duration::from_millis(500)).await;

        match child.try_wait()? {
            Some(status) => {
                return Err(anyhow::anyhow!("Anvil exited immediately with status: {}", status));
            }
            None => {
                info!("Anvil process started (pid={})", child.id().unwrap_or(0));
            }
        }

        let rpc_url = format!("http://127.0.0.1:{}", port);

        Self::wait_for_rpc_ready(&rpc_url).await?;

        Ok(Self { rpc_url, port, process: Some(child) })
    }

    async fn wait_for_rpc_ready(rpc_url: &str) -> Result<()> {
        let client = reqwest::Client::new();
        let mut attempts = 0;
        const MAX_ATTEMPTS: u32 = 30;

        while attempts < MAX_ATTEMPTS {
            if let Ok(response) = client
                .post(rpc_url)
                .json(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "eth_blockNumber",
                    "params": [],
                    "id": 1
                }))
                .send()
                .await
            {
                if response.status().is_success() {
                    info!("Anvil RPC is ready at {}", rpc_url);
                    return Ok(());
                }
            }

            attempts += 1;
            sleep(Duration::from_millis(200)).await;
        }

        Err(anyhow::anyhow!(
            "Anvil RPC failed to become ready at {} after {} attempts",
            rpc_url,
            MAX_ATTEMPTS
        ))
    }

    // -----------------------------------------------------------------------
    // JSON-RPC helpers
    // -----------------------------------------------------------------------

    /// Generic JSON-RPC call.
    pub async fn rpc_call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("RPC call {} failed: HTTP {}", method, response.status()));
        }

        let result: serde_json::Value = response.json().await?;
        if let Some(err) = result.get("error") {
            return Err(anyhow::anyhow!("RPC error in {}: {}", method, err));
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Block operations
    // -----------------------------------------------------------------------

    pub async fn mine_block(&self) -> Result<()> {
        self.rpc_call("evm_mine", serde_json::json!([])).await?;
        Ok(())
    }

    /// Mine N empty blocks at once.
    pub async fn mine_blocks(&self, count: u64) -> Result<()> {
        for _ in 0..count {
            self.mine_block().await?;
        }
        Ok(())
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        let result = self.rpc_call("eth_blockNumber", serde_json::json!([])).await?;
        let hex_value =
            result["result"].as_str().ok_or_else(|| anyhow::anyhow!("Invalid response format"))?;
        let block_number = u64::from_str_radix(hex_value.trim_start_matches("0x"), 16)?;
        Ok(block_number)
    }

    /// Get block metadata (hash, parent_hash) for a given block number.
    pub async fn get_block(&self, block_number: u64) -> Result<BlockInfo> {
        let result = self
            .rpc_call(
                "eth_getBlockByNumber",
                serde_json::json!([format!("0x{:x}", block_number), false]),
            )
            .await?;

        let block = &result["result"];
        if block.is_null() {
            return Err(anyhow::anyhow!("Block {} not found", block_number));
        }

        Ok(BlockInfo {
            number: block_number,
            hash: block["hash"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing block hash"))?
                .to_lowercase(),
            parent_hash: block["parentHash"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing parent hash"))?
                .to_lowercase(),
        })
    }

    // -----------------------------------------------------------------------
    // Snapshot / revert (for future reorg tests)
    // -----------------------------------------------------------------------

    /// Create a state snapshot. Returns snapshot ID.
    #[allow(dead_code)]
    pub async fn snapshot(&self) -> Result<String> {
        let result = self.rpc_call("evm_snapshot", serde_json::json!([])).await?;
        let id = result["result"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing snapshot ID"))?
            .to_string();
        Ok(id)
    }

    /// Revert to a snapshot. Block hashes after the snapshot point will change.
    #[allow(dead_code)]
    pub async fn revert_to_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.rpc_call("evm_revert", serde_json::json!([snapshot_id])).await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Transaction helpers
    // -----------------------------------------------------------------------

    /// Send an ERC20 transfer. Returns the tx hash.
    /// Does NOT auto-mine — call `mine_block()` afterwards if using manual mining.
    pub async fn send_transfer(
        &self,
        contract_address: &str,
        to: &ethers::types::Address,
        amount: ethers::types::U256,
    ) -> Result<String> {
        use ethers::middleware::MiddlewareBuilder;
        use ethers::providers::{Http, Middleware, Provider};
        use ethers::signers::{LocalWallet, Signer};
        use ethers::types::TransactionRequest;

        let base_provider = Provider::<Http>::try_from(&self.rpc_url)?;
        let chain_id = base_provider.get_chainid().await?.as_u64();

        let wallet: LocalWallet = ANVIL_DEFAULT_PRIVATE_KEY.parse()?;
        let wallet = wallet.with_chain_id(chain_id);
        let signer_address = wallet.address();
        let provider = base_provider.with_signer(wallet);

        let contract_addr: ethers::types::Address = contract_address.parse()?;
        let data = encode_transfer_call(*to, amount);
        let nonce = provider.get_transaction_count(signer_address, None).await?;

        let tx = TransactionRequest {
            from: Some(signer_address),
            to: Some(contract_addr.into()),
            data: Some(data.into()),
            gas: Some(100000u64.into()),
            nonce: Some(nonce),
            gas_price: Some(20000000000u128.into()),
            value: None,
            chain_id: None,
        };

        let pending = provider.send_transaction(tx, None).await?;
        let tx_hash = format!("{:?}", pending.tx_hash()).to_lowercase();
        Ok(tx_hash)
    }

    /// Send multiple ERC20 transfers without mining between them.
    /// All transactions sit in the mempool until the next `mine_block()`.
    /// Returns tx hashes in submission order.
    pub async fn send_transfers_no_mine(
        &self,
        contract_address: &str,
        transfers: &[(ethers::types::Address, ethers::types::U256)],
    ) -> Result<Vec<String>> {
        use ethers::middleware::MiddlewareBuilder;
        use ethers::providers::{Http, Middleware, Provider};
        use ethers::signers::{LocalWallet, Signer};
        use ethers::types::TransactionRequest;

        let base_provider = Provider::<Http>::try_from(&self.rpc_url)?;
        let chain_id = base_provider.get_chainid().await?.as_u64();

        let wallet: LocalWallet = ANVIL_DEFAULT_PRIVATE_KEY.parse()?;
        let wallet = wallet.with_chain_id(chain_id);
        let signer_address = wallet.address();
        let provider = base_provider.with_signer(wallet);

        let contract_addr: ethers::types::Address = contract_address.parse()?;
        let mut tx_hashes = Vec::new();

        // Get nonce once and manually increment — when automine is off,
        // get_transaction_count returns the confirmed nonce for all calls.
        let mut nonce = provider.get_transaction_count(signer_address, None).await?;

        for (to, amount) in transfers {
            let data = encode_transfer_call(*to, *amount);

            let tx = TransactionRequest {
                from: Some(signer_address),
                to: Some(contract_addr.into()),
                data: Some(data.into()),
                gas: Some(100000u64.into()),
                nonce: Some(nonce),
                gas_price: Some(20000000000u128.into()),
                value: None,
                chain_id: None,
            };

            let pending = provider.send_transaction(tx, None).await?;
            tx_hashes.push(format!("{:?}", pending.tx_hash()).to_lowercase());
            nonce += ethers::types::U256::one();
        }

        Ok(tx_hashes)
    }

    /// Get transaction receipt.
    pub async fn get_receipt(&self, tx_hash: &str) -> Result<TxReceipt> {
        let result = self
            .rpc_call("eth_getTransactionReceipt", serde_json::json!([tx_hash]))
            .await?;

        let receipt = &result["result"];
        if receipt.is_null() {
            return Err(anyhow::anyhow!("Receipt not found for {}", tx_hash));
        }

        let block_number_hex = receipt["blockNumber"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing blockNumber"))?;
        let status_hex = receipt["status"].as_str().unwrap_or("0x1");
        let logs = receipt["logs"].as_array();

        let log_index_start = logs
            .and_then(|l| l.first())
            .and_then(|l| l["logIndex"].as_str())
            .map(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).unwrap_or(0))
            .unwrap_or(0);

        let log_count = logs.map(|l| l.len() as u64).unwrap_or(0);

        Ok(TxReceipt {
            transaction_hash: tx_hash.to_lowercase(),
            block_number: u64::from_str_radix(block_number_hex.trim_start_matches("0x"), 16)?,
            block_hash: receipt["blockHash"]
                .as_str()
                .unwrap_or("")
                .to_lowercase(),
            status: status_hex != "0x0",
            log_index_start,
            log_count,
        })
    }

    // -----------------------------------------------------------------------
    // Contract deployment
    // -----------------------------------------------------------------------

    async fn start_log_streaming(child: &mut tokio::process::Child) {
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    debug!("[ANVIL] {}", line);
                }
            });
        }

        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    error!("[ANVIL ERROR] {}", line);
                }
            });
        }
    }

    /// Deploy a test contract using forge. Returns the contract address.
    pub async fn deploy_test_contract(&self) -> Result<String> {
        info!("Deploying SimpleERC20 test contract...");

        let e2e_tests_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let contract_path = e2e_tests_dir.join("contracts/SimpleERC20.sol:SimpleERC20");
        let output = std::process::Command::new("forge")
            .args([
                "create",
                "--rpc-url",
                &self.rpc_url,
                "--private-key",
                ANVIL_DEFAULT_PRIVATE_KEY,
                "--broadcast",
                &contract_path.to_string_lossy(),
            ])
            .current_dir(&e2e_tests_dir)
            .output()
            .context("Failed to run forge command — is Foundry installed?")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("Contract deployment failed: {}", stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let address_line = stdout
            .lines()
            .find(|line| line.contains("Deployed to:"))
            .ok_or_else(|| anyhow::anyhow!("Could not find contract address in forge output"))?;

        let address = address_line
            .split_whitespace()
            .last()
            .ok_or_else(|| anyhow::anyhow!("Could not parse contract address"))?;

        info!("Test contract deployed at: {}", address);
        Ok(address.to_string())
    }

    /// Gracefully stop the Anvil process.
    pub async fn stop(&mut self) {
        if let Some(mut child) = self.process.take() {
            info!("Stopping Anvil instance (port={})", self.port);
            if let Err(e) = child.kill().await {
                tracing::warn!("Failed to kill Anvil: {}", e);
            }
            let _ = tokio::time::timeout(Duration::from_secs(3), child.wait()).await;
        }
    }
}

impl Drop for AnvilInstance {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            if let Some(pid) = child.id() {
                let _ = std::process::Command::new("kill").arg(pid.to_string()).output();
            }
            let _ = child.start_kill();
        }
    }
}

fn allocate_free_port() -> Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Encode ERC20 transfer(address,uint256) call data.
fn encode_transfer_call(to: ethers::types::Address, value: ethers::types::U256) -> Vec<u8> {
    let mut data = vec![0xa9, 0x05, 0x9c, 0xbb]; // transfer selector
    let mut to_bytes = [0u8; 32];
    to_bytes[12..].copy_from_slice(to.as_bytes());
    data.extend_from_slice(&to_bytes);
    let mut value_bytes = [0u8; 32];
    let value_be: [u8; 32] = value.into();
    value_bytes.copy_from_slice(&value_be);
    data.extend_from_slice(&value_bytes);
    data
}
