// Module for integrating Reth with ExEx into rindexer
use std::collections::HashMap;

use alloy_primitives::FixedBytes;
use tokio::sync::mpsc;

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
    // Map of network name to notification channels
    pub channels: HashMap<
        String,
        mpsc::UnboundedReceiver<crate::provider::notifications::ChainStateNotification>,
    >,
}

impl RethChannels {
    pub fn new() -> Self {
        Self { channels: HashMap::new() }
    }

    pub fn insert(
        &mut self,
        network_name: String,
        notification_rx: mpsc::UnboundedReceiver<
            crate::provider::notifications::ChainStateNotification,
        >,
    ) {
        self.channels.insert(network_name, notification_rx);
    }

    pub fn take(
        &mut self,
        network_name: &str,
    ) -> Option<mpsc::UnboundedReceiver<crate::provider::notifications::ChainStateNotification>>
    {
        self.channels.remove(network_name)
    }
}

impl Default for RethChannels {
    fn default() -> Self {
        Self::new()
    }
}
