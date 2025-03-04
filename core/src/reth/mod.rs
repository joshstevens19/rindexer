// Module for integrating Reth with ExEx into rindexer
pub mod exex;
use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use alloy_primitives::BlockNumber;
pub use exex::start_reth_node_with_exex;
use reth_ethereum::provider::BlockReceipts;
use tokio::sync::{mpsc, oneshot, Mutex};

/// The type of ExEx stream to use.
#[derive(Clone, Debug)]
pub enum ExexType {
    /// The ExEx stream is used to backfill historical data.
    Backfill,
    /// The ExEx stream is used to process live data.
    Live,
}

/// The message type used to communicate with the ExEx.
pub struct RethBlockWithReceipts {
    /// The block receipts.
    pub block_receipts: BlockReceipts,
    /// The block timestamp.
    pub block_timestamp: u64,
    /// The type of ExEx stream that was used to get the block receipts.
    pub exex_type: ExexType,
}

/// Defines the mode of backfill operation.
#[derive(Clone, Copy)]
pub enum BackfillMode {
    /// Backfill a specific range and stop.
    PureBackfill,
    /// Backfill to the latest block and then process live blocks.
    BackfillWithLive,
}

/// Messages for controlling backfill operations.
pub enum BackfillMessage {
    Start {
        from_block: BlockNumber,
        to_block: Option<BlockNumber>,
        mode: BackfillMode,
        response_tx:
            oneshot::Sender<eyre::Result<(u64, mpsc::UnboundedReceiver<RethBlockWithReceipts>)>>,
    },
    Cancel {
        job_id: u64,
        response_tx: oneshot::Sender<eyre::Result<()>>,
    },
    Finish {
        job_id: u64,
    },
}

#[derive(Clone)]
pub struct RethChannels {
    // Map of network name to channel pairs
    pub channels: HashMap<String, mpsc::UnboundedSender<BackfillMessage>>,
}

impl RethChannels {
    pub fn new() -> Self {
        Self { channels: HashMap::new() }
    }

    pub fn insert(
        &mut self,
        network_name: String,
        backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
    ) {
        self.channels.insert(network_name, backfill_tx);
    }

    pub fn get(&self, network_name: &str) -> Option<&mpsc::UnboundedSender<BackfillMessage>> {
        self.channels.get(network_name)
    }

    pub fn get_mut(
        &mut self,
        network_name: &str,
    ) -> Option<&mut mpsc::UnboundedSender<BackfillMessage>> {
        self.channels.get_mut(network_name)
    }
}
