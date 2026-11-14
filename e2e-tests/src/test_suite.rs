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
pub struct ReorgHandlingConfig {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_size: Option<usize>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NetworkConfig {
    pub name: String,
    pub chain_id: u64,
    pub rpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reorg_handling: Option<ReorgHandlingConfig>,
    /// The `networks[].shared_tip_logs` stanza as written to the manifest (`enabled`,
    /// `empty_retry_deadline_ms`, `cache_blocks`); omitted means enabled with the defaults.
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "serialize_manifest_json")]
    pub shared_tip_logs: Option<serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct StorageConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresConfig>,
    pub csv: CsvConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<ClickHouseConfig>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ClickHouseConfig {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_each_run: Option<bool>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PostgresConfig {
    pub enabled: bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CsvConfig {
    pub enabled: bool,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct NativeTransfersConfig {
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub networks: Option<Vec<NativeTransferNetworkDetail>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "serialize_manifest_json")]
    pub reorg_safe_distance: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NativeTransferNetworkDetail {
    pub network: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_block: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_block: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ContractConfig {
    pub name: String,
    pub details: Vec<ContractDetail>,
    pub abi: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "serialize_manifest_json")]
    pub reorg_safe_distance: Option<serde_json::Value>,
    pub include_events: Option<Vec<EventConfig>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub streams: Option<serde_json::Value>,
}

/// Emit manifest JSON as ordinary YAML scalars and mappings.
///
/// `foundry-compilers` 0.21 enables `serde_json/arbitrary_precision`, and
/// `cargo build --bin e2e-tests` (how CI builds this harness) feature-unifies that
/// into this crate. A `serde_json` number then serializes to YAML as a one-field
/// map, `$serde_json::private::Number`, which the manifest rejects for `u64`
/// fields such as `networks[].shared_tip_logs.empty_retry_deadline_ms`.
fn serialize_manifest_json<S>(
    value: &Option<serde_json::Value>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        None => serializer.serialize_none(),
        Some(value) => serde::Serialize::serialize(&YamlJson(value), serializer),
    }
}

struct YamlJson<'a>(&'a serde_json::Value);

impl serde::Serialize for YamlJson<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            serde_json::Value::Null => serializer.serialize_unit(),
            serde_json::Value::Bool(value) => serializer.serialize_bool(*value),
            serde_json::Value::Number(value) => {
                if let Some(number) = value.as_u64() {
                    serializer.serialize_u64(number)
                } else if let Some(number) = value.as_i64() {
                    serializer.serialize_i64(number)
                } else if let Some(number) = value.as_f64() {
                    serializer.serialize_f64(number)
                } else {
                    serializer.serialize_str(&value.to_string())
                }
            }
            serde_json::Value::String(value) => serializer.serialize_str(value),
            serde_json::Value::Array(values) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(&YamlJson(value))?;
                }
                seq.end()
            }
            serde_json::Value::Object(values) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, &YamlJson(value))?;
                }
                map.end()
            }
        }
    }
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
    #[allow(dead_code)]
    pub async fn new(rindexer_binary: String) -> Result<Self> {
        Self::new_with_chain_id(rindexer_binary, 31337).await
    }

    pub async fn new_with_chain_id(rindexer_binary: String, chain_id: u64) -> Result<Self> {
        info!("Setting up fresh test context (chain_id={})...", chain_id);

        // Start a fresh Anvil instance on a dynamic port
        let anvil = AnvilInstance::start_local_with_chain_id(chain_id)
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

    #[allow(dead_code)]
    pub fn create_minimal_config(&self) -> RindexerConfig {
        RindexerInstance::create_minimal_config(&self.anvil.rpc_url, self.health_port)
    }

    pub fn create_contract_config(&self, contract_address: &str) -> RindexerConfig {
        RindexerInstance::create_contract_config(
            &self.anvil.rpc_url,
            contract_address,
            self.health_port,
        )
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

    #[allow(dead_code)]
    pub fn is_rindexer_running(&self) -> bool {
        self.rindexer.as_ref().map(|r| r.is_running()).unwrap_or(false)
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct ParsedTipLogs {
        enabled: bool,
        empty_retry_deadline_ms: u64,
        cache_blocks: usize,
    }

    #[derive(serde::Deserialize)]
    struct ParsedNetwork {
        shared_tip_logs: ParsedTipLogs,
    }

    #[test]
    fn manifest_json_numbers_stay_yaml_scalars() {
        let network = NetworkConfig {
            name: "anvil".to_string(),
            chain_id: 31337,
            rpc: "http://127.0.0.1:8545".to_string(),
            reorg_handling: None,
            shared_tip_logs: Some(serde_json::json!({
                "enabled": true,
                "empty_retry_deadline_ms": 1500,
                "cache_blocks": 8
            })),
        };
        let yaml = serde_yaml::to_string(&network).expect("network yaml");
        assert!(
            !yaml.contains("$serde_json::private::Number"),
            "numbers must stay YAML scalars so the manifest can parse them:\n{yaml}"
        );

        let parsed: ParsedNetwork = serde_yaml::from_str(&yaml).expect("parse emitted yaml");
        assert!(parsed.shared_tip_logs.enabled);
        assert_eq!(parsed.shared_tip_logs.empty_retry_deadline_ms, 1500);
        assert_eq!(parsed.shared_tip_logs.cache_blocks, 8);

        #[derive(serde::Serialize)]
        struct Distance {
            #[serde(serialize_with = "serialize_manifest_json")]
            reorg_safe_distance: Option<serde_json::Value>,
        }
        #[derive(serde::Deserialize)]
        struct ParsedDistance {
            reorg_safe_distance: u64,
        }
        let yaml =
            serde_yaml::to_string(&Distance { reorg_safe_distance: Some(serde_json::json!(2)) })
                .expect("distance yaml");
        assert!(!yaml.contains("$serde_json::private::Number"), "{yaml}");
        let parsed: ParsedDistance = serde_yaml::from_str(&yaml).expect("parse distance yaml");
        assert_eq!(parsed.reorg_safe_distance, 2);
    }
}
