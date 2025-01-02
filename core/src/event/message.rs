use ethers::abi::Hash;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventMessage {
    pub event_name: String,
    pub event_data: Value,
    pub event_signature_hash: Hash,
    pub network: String,
}
