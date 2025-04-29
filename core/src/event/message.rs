use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventMessage {
    pub event_name: String,
    pub event_data: Value,
    pub event_signature_hash: B256,
    pub network: String,
}
