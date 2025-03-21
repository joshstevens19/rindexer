use std::path::PathBuf;

use ethers::prelude::U64;
use reth::cli::Cli;
use serde::{Deserialize, Serialize};

use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u64,

    pub rpc: String,

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
    pub disable_logs_bloom_checks: Option<bool>,

    /// Reth configuration for this network
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reth: Option<RethConfig>,
}

/// Configuration for Reth node and ExEx
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RethConfig {
    /// Whether to enable Reth integration
    #[serde(default)]
    pub enabled: bool,

    /// CLI configuration for the Reth node
    #[serde(default)]
    pub cli_config: RethCliConfig,
}

/// CLI configuration for the Reth node
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RethCliConfig {
    /// Path to the Reth data directory
    pub data_dir: Option<PathBuf>,

    /// --authrpc.jwtsecret
    pub authrpc_jwtsecret: Option<String>,

    /// --authrpc.addr
    pub authrpc_addr: Option<String>,

    /// --authrpc.port
    pub authrpc_port: Option<u16>,

    /// --full
    pub full: bool,

    /// --metrics 127.0.0.1:9001
    pub metrics: Option<String>,

    /// --chain
    pub chain: Option<String>,

    /// --http
    pub http: bool,
}

impl RethCliConfig {
    pub fn to_reth_args(&self) -> Vec<String> {
        let mut args = vec![];
        args.push("reth".to_string());
        args.push("node".to_string());

        if let Some(data_dir) = &self.data_dir {
            args.push("--datadir".to_string());
            args.push(data_dir.to_string_lossy().into_owned());
        }

        if let Some(jwt) = &self.authrpc_jwtsecret {
            args.push("--authrpc.jwtsecret".to_string());
            args.push(jwt.clone());
        }

        if let Some(addr) = &self.authrpc_addr {
            args.push("--authrpc.addr".to_string());
            args.push(addr.clone());
        }

        if let Some(port) = &self.authrpc_port {
            args.push("--authrpc.port".to_string());
            args.push(port.to_string());
        }

        if self.full {
            args.push("--full".to_string());
        }

        if let Some(metrics) = &self.metrics {
            args.push("--metrics".to_string());
            args.push(metrics.clone());
        }

        if let Some(chain) = &self.chain {
            args.push("--chain".to_string());
            args.push(chain.clone());
        }

        if self.http {
            args.push("--http".to_string());
        }

        args
    }

    /// Convert the CLI configuration to a Reth CLI instance
    pub fn to_reth_cli(&self) -> Cli {
        Cli::try_parse_args_from(self.to_reth_args()).unwrap()
    }
}
