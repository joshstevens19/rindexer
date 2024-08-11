use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u64,

    pub rpc: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_block_range: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_logs_bloom_checks: Option<bool>,
}
