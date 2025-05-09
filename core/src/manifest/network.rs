use std::error;

use alloy::primitives::U64;
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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RethConfig {
    /// Whether to enable Reth integration
    pub enabled: bool,

    /// CLI configuration for the Reth node
    #[serde(skip)]
    pub cli_args: Option<Vec<String>>,
}

impl RethConfig {
    pub fn to_cli(&self) -> Result<Cli, Box<dyn error::Error>> {
        let mut reth_args = vec!["reth"];
        reth_args.extend(self.cli_args.as_ref().unwrap().iter().map(|s| s.as_str()));

        Cli::try_parse_args_from(reth_args).map_err(|e| Box::new(e) as Box<dyn error::Error>)
    }
}
