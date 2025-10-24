use anyhow::{Context, Result};
use std::path::PathBuf;
use tempfile::TempDir;
use tracing::{info, warn};

use crate::anvil_setup::AnvilInstance;
use crate::rindexer_client::RindexerInstance;
// Config structs for Rindexer
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RindexerConfig {
    pub name: String,
    pub project_type: String,
    pub config: serde_json::Value,
    pub timestamps: Option<serde_json::Value>,
    pub networks: Vec<NetworkConfig>,
    pub storage: StorageConfig,
    pub native_transfers: NativeTransfersConfig,
    pub contracts: Vec<ContractConfig>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NetworkConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct StorageConfig {
    pub postgres: PostgresConfig,
    pub csv: CsvConfig,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PostgresConfig {
    pub enabled: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CsvConfig {
    pub enabled: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NativeTransfersConfig {
    pub enabled: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ContractConfig {
    pub name: String,
    pub details: Vec<ContractDetail>,
    pub abi: Option<String>,
    pub include_events: Option<Vec<EventConfig>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ContractDetail {
    pub network: String,
    pub address: String,
    pub start_block: String,
    pub end_block: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EventConfig {
    pub name: String,
}
use crate::health_client::HealthClient;

/// Shared context for all tests - provides common infrastructure
pub struct TestContext {
    pub anvil: AnvilInstance,
    pub rindexer: Option<RindexerInstance>,
    pub graphql: Option<RindexerInstance>,
    pub test_contract_address: Option<String>,
    pub temp_dir: Option<TempDir>,
    pub project_path: PathBuf,
    pub rindexer_binary: String,
    pub health_client: Option<HealthClient>,
}

// TestSuite is now a separate struct for test results

impl TestContext {
    pub async fn new(rindexer_binary: String, anvil_port: u16, health_port: u16) -> Result<Self> {
        info!("Setting up fresh test context...");

        // Kill any existing Anvil processes and start fresh
        info!("Killing any existing Anvil processes...");
        let _ = std::process::Command::new("pkill").arg("-f").arg("anvil").output();

        // Wait for processes to be killed and port to be free
        wait_for_port_free(anvil_port, 10).await?;

        // Start a fresh Anvil instance
        let anvil = AnvilInstance::start_local(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
        )
        .await
        .context("Failed to start Anvil instance")?;

        info!("Anvil ready at: {}", anvil.rpc_url);

        // Create temporary directory for this test run
        let temp_dir = TempDir::new().context("Failed to create temporary directory")?;

        let project_path = temp_dir.path().join("test_project");
        std::fs::create_dir(&project_path).context("Failed to create project directory")?;

        Ok(Self {
            anvil,
            rindexer: None,
            graphql: None,
            test_contract_address: None,
            temp_dir: Some(temp_dir),
            project_path,
            rindexer_binary,
            health_client: Some(HealthClient::new(health_port)),
        })
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        info!("Cleaning up test suite...");

        // Stop Rindexer if running
        if let Some(mut rindexer) = self.rindexer.take() {
            if let Err(e) = rindexer.stop().await {
                warn!("Error stopping Rindexer: {}", e);
            }
        }
        // Stop GraphQL if running
        if let Some(mut graphql) = self.graphql.take() {
            if let Err(e) = graphql.stop().await {
                warn!("Error stopping GraphQL: {}", e);
            }
        }

        // Anvil will be cleaned up automatically when the process is dropped

        // TempDir will be cleaned up automatically on drop
        self.temp_dir.take();

        info!("Test suite cleanup completed");
        Ok(())
    }

    /// Deploy a test contract using the Anvil instance
    pub async fn deploy_test_contract(&mut self) -> Result<String> {
        let address = self.anvil.deploy_test_contract().await?;
        self.test_contract_address = Some(address.clone());
        Ok(address)
    }

    /// Create a minimal Rindexer configuration
    pub fn create_minimal_config(&self) -> RindexerConfig {
        crate::rindexer_client::RindexerInstance::create_minimal_config(&self.anvil.rpc_url)
    }

    /// Create a configuration with a specific contract
    pub fn create_contract_config(&self, contract_address: &str) -> RindexerConfig {
        crate::rindexer_client::RindexerInstance::create_contract_config(
            &self.anvil.rpc_url,
            contract_address,
        )
    }

    pub async fn start_rindexer(&mut self, config: RindexerConfig) -> Result<()> {
        // Create abis directory and copy all ABI files from repo abis/
        let abis_dir = self.project_path.join("abis");
        std::fs::create_dir_all(&abis_dir).context("Failed to create abis directory")?;

        if let Ok(entries) = std::fs::read_dir("abis") {
            for entry in entries.flatten() {
                if let Ok(file_type) = entry.file_type() {
                    if file_type.is_file() {
                        let src_path = entry.path();
                        if let Some(name) = src_path.file_name() {
                            let dst_path = abis_dir.join(name);
                            let _ = std::fs::copy(&src_path, &dst_path);
                        }
                    }
                }
            }
        }

        // Write the Rindexer configuration
        let config_path = self.project_path.join("rindexer.yaml");
        let config_yaml =
            serde_yaml::to_string(&config).context("Failed to serialize config to YAML")?;

        std::fs::write(&config_path, config_yaml).context("Failed to write config file")?;

        info!("Created Rindexer project at: {:?}", self.project_path);

        // Create Rindexer instance and start indexer
        let mut rindexer = RindexerInstance::new(&self.rindexer_binary, self.project_path.clone());

        rindexer.start_indexer().await.context("Failed to start Rindexer indexer")?;

        self.rindexer = Some(rindexer);
        info!("Rindexer started successfully");

        Ok(())
    }

    /// Wait for Rindexer sync completion based on log output
    pub async fn wait_for_sync_completion(&mut self, timeout_seconds: u64) -> Result<()> {
        if let Some(rindexer) = &mut self.rindexer {
            rindexer.wait_for_initial_sync_completion(timeout_seconds).await?;
            info!("✓ Rindexer sync completed (detected via logs)");
        }
        Ok(())
    }

    pub fn get_csv_output_path(&self) -> PathBuf {
        self.project_path.join("generated_csv")
    }

    pub fn is_rindexer_running(&self) -> bool {
        if let Some(rindexer) = &self.rindexer {
            rindexer.is_running()
        } else {
            false
        }
    }

    /// Wait for new events to appear in CSV output (for live indexing tests)
    pub async fn wait_for_new_events(
        &self,
        expected_min_events: usize,
        timeout_seconds: u64,
    ) -> Result<usize> {
        let csv_path =
            self.get_csv_output_path().join("SimpleERC20").join("simpleerc20-transfer.csv");

        if !csv_path.exists() {
            return Err(anyhow::anyhow!("CSV file does not exist yet: {:?}", csv_path));
        }

        let start_time = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_seconds);

        while start_time.elapsed() < timeout {
            if let Ok(content) = std::fs::read_to_string(&csv_path) {
                let lines: Vec<&str> = content.lines().collect();
                let event_count = if lines.len() > 1 { lines.len() - 1 } else { 0 }; // Subtract header

                if event_count >= expected_min_events {
                    info!(
                        "✓ Found {} events (expected at least {})",
                        event_count, expected_min_events
                    );
                    return Ok(event_count);
                }

                info!(
                    "Waiting for events: found {} of {} expected",
                    event_count, expected_min_events
                );
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        Err(anyhow::anyhow!(
            "Timeout waiting for {} events after {}s",
            expected_min_events,
            timeout_seconds
        ))
    }

    /// Get the current number of events in CSV output
    pub fn get_event_count(&self) -> Result<usize> {
        let csv_path =
            self.get_csv_output_path().join("SimpleERC20").join("simpleerc20-transfer.csv");

        if !csv_path.exists() {
            return Ok(0);
        }

        let content = std::fs::read_to_string(&csv_path)?;
        let lines: Vec<&str> = content.lines().collect();
        Ok(if lines.len() > 1 { lines.len() - 1 } else { 0 }) // Subtract header
    }
}

async fn wait_for_port_free(port: u16, max_attempts: u32) -> Result<()> {
    for attempt in 1..=max_attempts {
        // Try to connect to the port - if it fails, the port is free
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(_) => {
                // Port is still in use, wait a bit
                if attempt < max_attempts {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            Err(_) => {
                // Port is free, we can proceed
                return Ok(());
            }
        }
    }
    Err(anyhow::anyhow!("Port {} is still in use after {} attempts", port, max_attempts))
}
