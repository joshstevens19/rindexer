use std::fmt;
use std::time::Duration;

use alloy::primitives::U64;
use serde::de::Visitor;
use serde::{de, Deserialize, Deserializer, Serialize};

#[cfg(feature = "reth")]
use tokio::sync::broadcast::Sender;

use tokio::time::sleep;

use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};
#[cfg(feature = "reth")]
use super::reth::RethConfig;

#[cfg(feature = "reth")]
use crate::notifications::ChainStateNotification;

#[cfg(feature = "reth")]
use crate::reth::node::start_reth_node_with_exex;

#[cfg(feature = "reth")]
use reth::cli::Commands;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u64,

    pub rpc: String,

    /// Poll the latest block at a defined frequency. It is recommended that this frequency be a
    /// multiple faster than the networks block time to ensure fast indexing.
    ///
    /// If RPC use is a concern, this can be reduced at the cost of slower indexing of new logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_poll_frequency: Option<BlockPollFrequency>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub max_block_range: Option<U64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get_logs_settings: Option<GetLogsSettings>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_logs_bloom_checks: Option<bool>,

    /// Reth configuration for this network
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[cfg(feature = "reth")]
    pub reth: Option<RethConfig>,

    #[cfg(not(feature = "reth"))]
    pub reth: Option<()>,
}

impl Network {
    /// Get the IPC path for the Reth node
    #[cfg(feature = "reth")]
    pub fn get_reth_ipc_path(&self) -> Option<String> {
        let reth = self.reth.as_ref()?;
        let cli = reth.to_cli().ok()?;

        match &cli.command {
            Commands::Node(node_cmd) => Some(node_cmd.rpc.ipcpath.clone()),
            _ => None,
        }
    }
    /// Check if Reth is enabled for this network
    #[cfg(feature = "reth")]
    pub fn is_reth_enabled(&self) -> bool {
        self.reth.is_some()
    }

    /// Try to start the Reth node for this network
    ///
    /// Returns a Sender for the Reth node if it was started successfully.
    ///
    /// If Reth is not enabled, the function will return None.
    ///
    /// If the Reth node fails to start, the function will return an error.
    #[cfg(feature = "reth")]
    pub async fn try_start_reth_node(
        &self,
    ) -> Result<Option<Sender<ChainStateNotification>>, eyre::Error> {
        if !self.is_reth_enabled() {
            return Ok(None);
        }

        let reth_cli = self.reth.as_ref().unwrap().to_cli().map_err(|e| eyre::eyre!(e))?;
        let reth_tx = start_reth_node_with_exex(reth_cli)?;

        // Wait for IPC path to be ready if specified
        if let Some(ipc_path) = self.get_reth_ipc_path() {
            wait_for_ipc_ready(&ipc_path).await?;
        }
        println!("started reth node");

        Ok(Some(reth_tx))
    }
}

#[derive(Debug, Serialize, Clone)]
pub enum AddressFiltering {
    InMemory,
    MaxAddressPerGetLogsRequest(usize),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddressFilteringConfig {
    pub max_address_per_get_logs_request: usize,
}

impl<'de> Deserialize<'de> for AddressFiltering {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_yaml::Value::deserialize(deserializer)?;

        if let Some(s) = value.as_str() {
            if s == "in-memory" {
                return Ok(AddressFiltering::InMemory);
            }
        }

        // Try to deserialize as AddressFilteringConfig
        match AddressFilteringConfig::deserialize(value) {
            Ok(config) => Ok(AddressFiltering::MaxAddressPerGetLogsRequest(
                config.max_address_per_get_logs_request,
            )),
            Err(_) => Err(Error::custom("Invalid AddressFiltering format")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GetLogsSettings {
    pub address_filtering: AddressFiltering,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub enum BlockPollFrequency {
    /// Poll the latest block a defined rate for a network. E.g. 500ms.
    PollRateMs { millis: u64 },
    /// Poll the latest block at factor of the networks block time. E.g. 1/5th of a 2s block time.
    Division { divisor: u32 },
    /// Use very fast block polling designed to index blocks as close to realtime as possible.
    /// This will be at the expense of more RPC calls.
    Rapid,
    /// This will use an RPC balanced / optimised poll rate, it is currently defined as 1/3 of the
    /// networks block rate and is shorthand for `/3`, however may change in the future to represent
    /// use cases where we want to balance speed of indexing with rpc use automatically.
    RpcOptimized,
}

impl<'de> Deserialize<'de> for BlockPollFrequency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BlockPollFrequencyVisitor;

        impl<'de> Visitor<'de> for BlockPollFrequencyVisitor {
            type Value = BlockPollFrequency;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing block poll frequency")
            }

            fn visit_str<E>(self, value: &str) -> Result<BlockPollFrequency, E>
            where
                E: de::Error,
            {
                match value {
                    "rapid" => Ok(BlockPollFrequency::Rapid),
                    "optimized" => Ok(BlockPollFrequency::RpcOptimized),
                    _ if value.starts_with('/') => {
                        let divisor = value[1..].parse::<u32>().map_err(E::custom)?;
                        Ok(BlockPollFrequency::Division { divisor })
                    }
                    _ => {
                        let millis = value.parse::<u64>().map_err(E::custom)?;
                        Ok(BlockPollFrequency::PollRateMs { millis })
                    }
                }
            }
        }

        deserializer.deserialize_str(BlockPollFrequencyVisitor)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "reth")]
    use reth::cli::Commands;

    use serde_yaml;

    use super::*;

    #[test]
    fn test_network_defaults() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            "#,
        )
        .unwrap();

        assert_eq!(network.name, "ethereum");
        assert_eq!(network.chain_id, 1);
        assert_eq!(network.rpc, "https://mainnet.gateway.tenderly.co");
        assert_eq!(network.max_block_range, None);
        assert_eq!(network.compute_units_per_second, None);
        assert_eq!(network.block_poll_frequency, None);
    }

    #[test]
    fn test_network_block_poll_frequency() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "rapid"
            "#,
        )
        .unwrap();

        assert_eq!(network.block_poll_frequency, Some(BlockPollFrequency::Rapid));

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "/3"
            "#,
        )
        .unwrap();

        assert_eq!(network.block_poll_frequency, Some(BlockPollFrequency::Division { divisor: 3 }));

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: "1500"
            "#,
        )
        .unwrap();

        assert_eq!(
            network.block_poll_frequency,
            Some(BlockPollFrequency::PollRateMs { millis: 1500 })
        );

        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            block_poll_frequency: 100
            "#,
        )
        .unwrap();

        assert_eq!(
            network.block_poll_frequency,
            Some(BlockPollFrequency::PollRateMs { millis: 100 })
        );
    }

    #[cfg(feature = "reth")]
    #[test]
    fn test_network_with_reth_config() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            reth:
                enabled: true
                cli_args:
                    - --ipcpath /custom/reth.ipc
            "#,
        )
        .unwrap();

        assert!(network.reth.is_some());
        let reth = network.reth.as_ref().unwrap();
        assert!(reth.enabled);
        assert_eq!(reth.cli_args, vec!["--ipcpath /custom/reth.ipc"]);

        // Test get_reth_ipc_path
        let ipc_path = network.get_reth_ipc_path();
        assert_eq!(ipc_path, Some("/custom/reth.ipc".to_string()));
    }

    #[cfg(feature = "reth")]
    #[test]
    fn test_network_with_reth_config_no_ipc_path() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            reth:
                enabled: true
                cli_args:
                    - --authrpc.jwtsecret /Users/skanda/secrets/jwt.hex
                    - --authrpc.addr 127.0.0.1
                    - --authrpc.port 8551
                    - --datadir /Volumes/T9/reth
                    - --metrics 127.0.0.1:9001
                    - --chain sepolia
                    - --http
            "#,
        )
        .unwrap();

        assert!(network.reth.is_some());
        let reth = network.reth.as_ref().unwrap();
        assert!(reth.enabled);

        // Test get_reth_ipc_path
        let cli = reth.to_cli().unwrap();
        match &cli.command {
            Commands::Node(node_cmd) => {
                println!("node_cmd: {:?}", node_cmd.rpc.ipcpath);
                // Default IPC path when not specified
                assert!(!node_cmd.rpc.ipcdisable);
            }
            _ => panic!("Expected Node command"),
        }
    }

    #[cfg(feature = "reth")]
    #[test]
    fn test_network_reth_ipc_disabled() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            reth:
                enabled: true
                cli_args:
                    - --ipcdisable
            "#,
        )
        .unwrap();

        // Should return None when IPC is disabled
        let cli = network.reth.as_ref().unwrap().to_cli().unwrap();
        if let Commands::Node(node_cmd) = &cli.command {
            assert!(node_cmd.rpc.ipcdisable);
        }
        // Note: get_reth_ipc_path might still return a default path even with ipcdisable
    }

    #[cfg(feature = "reth")]
    #[test]
    fn test_network_no_reth_config() {
        let network: Network = serde_yaml::from_str(
            r#"
            name: ethereum
            chain_id: 1
            rpc: https://mainnet.gateway.tenderly.co
            "#,
        )
        .unwrap();

        assert!(network.reth.is_none());
        assert_eq!(network.get_reth_ipc_path(), None);
    }
}

/// Wait for IPC socket file to be ready
pub async fn wait_for_ipc_ready(ipc_path: &str) -> Result<(), eyre::Error> {
    use alloy::providers::{IpcConnect, Provider, ProviderBuilder};

    let max_retries = 60; // 60 seconds max wait
    let mut last_error = None;

    for i in 0..max_retries {
        // Try to connect to the IPC socket
        let ipc = IpcConnect::new(ipc_path.to_string());
        match ProviderBuilder::new().connect_ipc(ipc).await {
            Ok(provider) => {
                // Try a simple call to ensure it's really ready
                match provider.get_chain_id().await {
                    Ok(_) => {
                        tracing::info!("IPC socket at {} is ready", ipc_path);
                        return Ok(());
                    }
                    Err(e) => {
                        last_error = Some(format!("Connected but get_chain_id failed: {e}"));
                    }
                }
            }
            Err(e) => {
                last_error = Some(format!("Connection failed: {e}"));
            }
        }

        if i == 0 {
            tracing::info!("Waiting for IPC socket at {} to be ready...", ipc_path);
        } else if i % 5 == 0 {
            tracing::info!(
                "Still waiting for IPC socket at {} to be ready... ({}/{})",
                ipc_path,
                i,
                max_retries
            );
        }

        sleep(Duration::from_secs(1)).await;
    }

    Err(eyre::eyre!(
        "IPC socket at {} did not become ready after {} seconds. Last error: {:?}",
        ipc_path,
        max_retries,
        last_error
    ))
}
