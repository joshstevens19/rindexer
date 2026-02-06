use serde::{Deserialize, Serialize};

use crate::manifest::contract::Contract;

pub fn default_health_port() -> u16 {
    8080
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Global {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<Contract>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub etherscan_api_key: Option<String>,

    #[serde(default = "default_health_port")]
    pub health_port: u16,
}

impl Default for Global {
    fn default() -> Self {
        Self {
            contracts: None,
            etherscan_api_key: None,
            health_port: default_health_port(),
        }
    }
}
