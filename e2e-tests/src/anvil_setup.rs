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

pub struct AnvilInstance {
    pub rpc_url: String,
    pub port: u16,
    process: Option<tokio::process::Child>,
}

impl AnvilInstance {
    /// Start a local Anvil instance on a dynamically allocated free port.
    pub async fn start_local() -> Result<Self> {
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
            .arg("1000000000")
            .arg("--block-time")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

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

    pub async fn mine_block(&self) -> Result<()> {
        let client = reqwest::Client::new();

        let response = client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "evm_mine",
                "params": [],
                "id": 1
            }))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Failed to mine block"));
        }

        Ok(())
    }

    pub async fn get_block_number(&self) -> Result<u64> {
        let client = reqwest::Client::new();

        let response = client
            .post(&self.rpc_url)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1
            }))
            .send()
            .await?;

        let result: serde_json::Value = response.json().await?;
        let hex_value =
            result["result"].as_str().ok_or_else(|| anyhow::anyhow!("Invalid response format"))?;

        let block_number = u64::from_str_radix(hex_value.trim_start_matches("0x"), 16)?;
        Ok(block_number)
    }

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

    /// Deploy a test contract using forge.
    pub async fn deploy_test_contract(&self) -> Result<String> {
        info!("Deploying SimpleERC20 test contract...");

        let e2e_tests_dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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
            // Sync kill via PID — the only reliable way in Drop.
            if let Some(pid) = child.id() {
                let _ = std::process::Command::new("kill")
                    .arg(pid.to_string())
                    .output();
            }
            // Fallback: start_kill sends SIGKILL to the child. The future is
            // never polled but the signal is still sent.
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
