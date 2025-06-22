// Module for integrating Reth with ExEx into rindexer
use std::collections::HashMap;

use alloy_primitives::FixedBytes;
use tokio::sync::broadcast;

/// Log metadata for each returned log
#[derive(Clone, Debug)]
pub struct LogMetadata {
    pub block_timestamp: u64,
    pub block_hash: FixedBytes<32>,
    pub block_number: u64,
    pub tx_hash: FixedBytes<32>,
    pub tx_index: u64,
    pub log_index: usize,
    pub log_type: Option<String>,
    pub removed: bool,
}

pub struct RethChannels {
    // Map of network name to notification senders
    pub channels:
        HashMap<String, broadcast::Sender<crate::provider::notifications::ChainStateNotification>>,
}

impl RethChannels {
    pub fn new() -> Self {
        Self { channels: HashMap::new() }
    }

    pub fn insert(
        &mut self,
        network_name: String,
        notification_tx: broadcast::Sender<crate::provider::notifications::ChainStateNotification>,
    ) {
        self.channels.insert(network_name, notification_tx);
    }

    pub fn subscribe(
        &self,
        network_name: &str,
    ) -> Option<broadcast::Receiver<crate::provider::notifications::ChainStateNotification>> {
        self.channels.get(network_name).map(|tx| tx.subscribe())
    }
}

impl Default for RethChannels {
    fn default() -> Self {
        Self::new()
    }
}
