use alloy::primitives::B256;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventMessage {
    pub event_name: String,
    pub event_data: Value,
    pub event_signature_hash: B256,
    pub network: String,
    /// Source block number for the events carried in `event_data`. All events
    /// in a single `EventMessage` must originate from the same block so the
    /// finalized-delivery buffer can key by it. `block_number: 0` is reserved
    /// for synthetic messages (e.g., reorg notifications) that never buffer.
    pub block_number: u64,
}
