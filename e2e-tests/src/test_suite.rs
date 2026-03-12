use anyhow::{Context, Result};
use std::path::PathBuf;
use tempfile::TempDir;
use tracing::{info, warn};

use crate::anvil_setup::AnvilInstance;
use crate::health_client::HealthClient;
use crate::rindexer_client::RindexerInstance;

// Config structs for Rindexer
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RindexerConfig {
    pub name: String,
    pub project_type: String,
    pub config: serde_json::Value,
    pub timestamps: Option<serde_json::Value>,
    pub networks: Vec<NetworkConfig>,
    pub global: GlobalConfig,
    pub storage: StorageConfig,
    pub native_transfers: NativeTransfersConfig,
    pub contracts: Vec<ContractConfig>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct GlobalConfig {
    pub health_port: u16,
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

/// Shared context for all tests — provides common infrastructure.
///
/// Each test gets a fresh `TestContext` with its own Anvil instance on a
/// random port, a fresh temp directory, and an isolated health port.
pub struct TestContext {
    pub anvil: AnvilInstance,
    pub rindexer: Option<RindexerInstance>,
    pub graphql: Option<RindexerInstance>,
    pub test_contract_address: Option<String>,
    pub temp_dir: Option<TempDir>,
    pub project_path: PathBuf,
    pub rindexer_binary: String,
    pub health_client: Option<HealthClient>,
    pub health_port: u16,
    /// Containers to clean up on drop (RAII guard).
    docker_containers: Vec<String>,
}

impl TestContext {
    pub async fn new(rindexer_binary: String) -> Result<Self> {
        info!("Setting up fresh test context...");

        // Start a fresh Anvil instance on a dynamic port (no pkill needed)
        let anvil = AnvilInstance::start_local()
            .await
            .context("Failed to start Anvil instance")?;

        info!("Anvil ready at: {}", anvil.rpc_url);

        // Allocate a dynamic health port
        let health_port = crate::docker::allocate_free_port()?;

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
            health_port,
            docker_containers: Vec::new(),
        })
    }

    /// Register a Docker container for automatic cleanup.
    pub fn register_container(&mut self, name: String) {
        self.docker_containers.push(name);
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        info!("Cleaning up test context...");

        if let Some(mut rindexer) = self.rindexer.take() {
            if let Err(e) = rindexer.stop().await {
                warn!("Error stopping rindexer: {}", e);
            }
        }
        if let Some(mut graphql) = self.graphql.take() {
            if let Err(e) = graphql.stop().await {
                warn!("Error stopping GraphQL: {}", e);
            }
        }

        self.anvil.stop().await;

        // Clean up any registered Docker containers
        for name in self.docker_containers.drain(..) {
            if let Err(e) = crate::docker::stop_postgres_container(&name).await {
                warn!("Error stopping container {}: {}", name, e);
            }
        }

        self.temp_dir.take();

        info!("Test context cleanup completed");
        Ok(())
    }

    pub async fn deploy_test_contract(&mut self) -> Result<String> {
        let address = self.anvil.deploy_test_contract().await?;
        self.test_contract_address = Some(address.clone());
        Ok(address)
    }

    pub fn create_minimal_config(&self) -> RindexerConfig {
        RindexerInstance::create_minimal_config(&self.anvil.rpc_url, self.health_port)
    }

    pub fn create_contract_config(&self, contract_address: &str) -> RindexerConfig {
        RindexerInstance::create_contract_config(&self.anvil.rpc_url, contract_address, self.health_port)
    }

    pub async fn start_rindexer(&mut self, config: RindexerConfig) -> Result<()> {
        // Copy ABIs using CARGO_MANIFEST_DIR for a stable base path
        let abis_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("abis");
        let abis_dst = self.project_path.join("abis");
        std::fs::create_dir_all(&abis_dst).context("Failed to create abis directory")?;

        if let Ok(entries) = std::fs::read_dir(&abis_src) {
            for entry in entries.flatten() {
                if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    if let Some(name) = entry.path().file_name() {
                        let _ = std::fs::copy(entry.path(), abis_dst.join(name));
                    }
                }
            }
        }

        let config_path = self.project_path.join("rindexer.yaml");
        let config_yaml =
            serde_yaml::to_string(&config).context("Failed to serialize config to YAML")?;
        std::fs::write(&config_path, config_yaml).context("Failed to write config file")?;

        info!("Created rindexer project at: {:?}", self.project_path);

        let mut rindexer = RindexerInstance::new(&self.rindexer_binary, self.project_path.clone());
        rindexer.start_indexer().await.context("Failed to start rindexer indexer")?;

        self.rindexer = Some(rindexer);
        info!("Rindexer started successfully");

        Ok(())
    }

    pub async fn wait_for_sync_completion(&mut self, timeout_seconds: u64) -> Result<()> {
        if let Some(rindexer) = &mut self.rindexer {
            rindexer.wait_for_initial_sync_completion(timeout_seconds).await?;
            info!("Rindexer sync completed (detected via logs)");
        }
        Ok(())
    }

    pub fn get_csv_output_path(&self) -> PathBuf {
        self.project_path.join("generated_csv")
    }

    pub fn is_rindexer_running(&self) -> bool {
        self.rindexer.as_ref().map(|r| r.is_running()).unwrap_or(false)
    }

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
                let event_count = if lines.len() > 1 { lines.len() - 1 } else { 0 };

                if event_count >= expected_min_events {
                    info!(
                        "Found {} events (expected at least {})",
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

    pub fn get_event_count(&self) -> Result<usize> {
        let csv_path =
            self.get_csv_output_path().join("SimpleERC20").join("simpleerc20-transfer.csv");

        if !csv_path.exists() {
            return Ok(0);
        }

        let content = std::fs::read_to_string(&csv_path)?;
        let lines: Vec<&str> = content.lines().collect();
        Ok(if lines.len() > 1 { lines.len() - 1 } else { 0 })
    }
}
