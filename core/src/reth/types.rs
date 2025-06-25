// Module for integrating Reth with ExEx into rindexer
use std::{collections::HashMap, sync::Arc};

use alloy_primitives::FixedBytes;
use tokio::sync::broadcast;

use crate::provider::ChainStateNotification;

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
    pub channels: HashMap<String, broadcast::Sender<ChainStateNotification>>,
}

impl RethChannels {
    pub fn new() -> Self {
        Self { channels: HashMap::new() }
    }

    pub fn insert(
        &mut self,
        network_name: String,
        notification_tx: broadcast::Sender<ChainStateNotification>,
    ) {
        self.channels.insert(network_name, notification_tx);
    }

    pub fn subscribe(
        &self,
        network_name: &str,
    ) -> Option<broadcast::Receiver<ChainStateNotification>> {
        self.channels.get(network_name).map(|tx| tx.subscribe())
    }

    /// Check if there are any channels configured
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty()
    }

    /// Convert to Option<Arc<Self>> based on whether channels exist
    pub fn into_arc_option(self) -> Option<Arc<Self>> {
        if self.is_empty() {
            None
        } else {
            Some(Arc::new(self))
        }
    }
}

impl Default for RethChannels {
    fn default() -> Self {
        Self::new()
    }
}

/// Extension trait for cleaner subscription from Option<Arc<RethChannels>>
pub trait RethChannelsExt {
    fn subscribe_to_network(
        &self,
        network: &str,
    ) -> Option<broadcast::Receiver<ChainStateNotification>>;
}

impl RethChannelsExt for Option<Arc<RethChannels>> {
    fn subscribe_to_network(
        &self,
        network: &str,
    ) -> Option<broadcast::Receiver<ChainStateNotification>> {
        self.as_ref().and_then(|channels| channels.subscribe(network))
    }
}
