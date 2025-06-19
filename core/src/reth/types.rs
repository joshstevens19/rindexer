// Module for integrating Reth with ExEx into rindexer
use std::{collections::HashMap, iter::StepBy, ops::RangeInclusive, sync::Arc};

use alloy_primitives::FixedBytes;
use alloy::rpc::types::{Filter, Log};
use tokio::sync::{mpsc, oneshot};

/// The response to an ExExRequest
pub type ExExResponse = eyre::Result<(u64, mpsc::UnboundedReceiver<ExExReturnData>)>;
/// The type of the channel that forwards requests to the Reth node.
pub type ExExTx = mpsc::UnboundedSender<ExExRequest>;

/// ExExRequest is a request to the Execution Engine
#[allow(clippy::large_enum_variant)]
pub enum ExExRequest {
    /// Starts a new execution job with the given mode and filter
    Start { mode: ExExMode, filter: Filter, response_tx: oneshot::Sender<ExExResponse> },
    /// Cancels a job with the given ID
    Cancel { job_id: u64 },
    /// Finish a job with the given ID
    Finish { job_id: u64 },
}

/// The mode of the execution job
#[derive(Clone)]
pub enum ExExMode {
    /// Backfill only
    HistoricOnly,
    /// Backfill from a specific block to the latest block and then switch to live mode
    HistoricThenLive,
    /// Live only mode
    LiveOnly,
}

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

/// The data that is being returned
pub struct ExExReturnData {
    pub log: Log,
}

#[derive(Clone)]
pub struct RethChannels {
    // Map of network name to channel pairs
    pub channels: HashMap<String, Arc<ExExTx>>,
}

impl RethChannels {
    pub fn new() -> Self {
        Self { channels: HashMap::new() }
    }

    pub fn insert(&mut self, network_name: String, reth_tx: mpsc::UnboundedSender<ExExRequest>) {
        self.channels.insert(network_name, Arc::new(reth_tx));
    }

    pub fn get(&self, network_name: &str) -> Option<&Arc<ExExTx>> {
        self.channels.get(network_name)
    }

    pub fn get_mut(&mut self, network_name: &str) -> Option<&mut Arc<ExExTx>> {
        self.channels.get_mut(network_name)
    }
}

impl Default for RethChannels {
    fn default() -> Self {
        Self::new()
    }
}

/// An iterator that yields _inclusive_ block ranges of a given step size
#[derive(Debug)]
pub struct BlockRangeInclusiveIter {
    iter: StepBy<RangeInclusive<u64>>,
    step: u64,
    end: u64,
}

impl BlockRangeInclusiveIter {
    pub fn new(range: RangeInclusive<u64>, step: u64) -> Self {
        Self { end: *range.end(), iter: range.step_by(step as usize + 1), step }
    }
}

impl Iterator for BlockRangeInclusiveIter {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        let start = self.iter.next()?;
        let end = (start + self.step).min(self.end);
        if start > end {
            return None
        }
        Some((start, end))
    }
}
