use crate::manifest::contract::Contract;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Global {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<Contract>>,
}
