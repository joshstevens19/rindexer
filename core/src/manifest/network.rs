use ethers::prelude::U64;
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
}
