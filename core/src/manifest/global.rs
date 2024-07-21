use serde::{Deserialize, Serialize};

use crate::manifest::contract::Contract;

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Global {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<Contract>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub etherscan_api_key: Option<String>,
}
