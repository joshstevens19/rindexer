use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u64,

    pub rpc: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,
}
