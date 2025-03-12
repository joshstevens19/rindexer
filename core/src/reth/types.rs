// Module for integrating Reth with ExEx into rindexer
use std::{collections::HashMap, iter::StepBy, ops::RangeInclusive, sync::Arc};

use alloy_primitives::{BlockNumber, FixedBytes, Log};
use alloy_rpc_types::Filter;
use reth::providers::Chain;
use tokio::sync::{mpsc, oneshot};

/// The response to an ExExRequest
pub type ExExResponse = eyre::Result<(u64, mpsc::UnboundedReceiver<ExExReturnData>)>;
/// The type of the channel that forwards requests to the Reth node.
pub type ExExTx = mpsc::UnboundedSender<ExExRequest>;

/// ExExRequest is a request to the Execution Engine
pub enum ExExRequest {
    /// Starts a new execution job with the given mode and data type
    Start { mode: ExExMode, data_type: ExExDataType, response_tx: oneshot::Sender<ExExResponse> },
    /// Cancels a job with the given ID
    Cancel { job_id: u64 },
    /// Finish a job with the given ID
    Finish { job_id: u64 },
}

/// The mode of the execution job
#[derive(Clone)]
pub enum ExExMode {
    /// Backfill only from a specific block to a specific block
    HistoricOnly { from: BlockNumber, to: BlockNumber },
    /// Backfill from a specific block to the latest block and then switch to live mode
    HistoricThenLive { from: BlockNumber },
    /// Live only mode
    LiveOnly,
}

/// The type of data that is being requested
#[derive(Clone)]
pub enum ExExDataType {
    /// return the Chain. used for forwarding exex returns to the client as is.
    Chain,
    /// Filtered logs.
    FilteredLogs { filter: Filter },
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

/// The type of data that is being returned
#[derive(Clone)]
pub enum ExExReturnData {
    /// return the Chain. used for forwarding exex returns to the client as is.
    Chain { chain: Chain, source: DataSource },
    /// Filtered logs.
    Log { log: (Log, LogMetadata), source: DataSource },
}

/// The source of the data from the exex
#[derive(Clone, Debug)]
pub enum DataSource {
    /// Backfill
    Backfill,
    /// Live
    Live,
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

    pub fn insert(
        &mut self,
        network_name: String,
        backfill_tx: mpsc::UnboundedSender<ExExRequest>,
    ) {
        self.channels.insert(network_name, Arc::new(backfill_tx));
    }

    pub fn get(&self, network_name: &str) -> Option<&Arc<ExExTx>> {
        self.channels.get(network_name)
    }

    pub fn get_mut(&mut self, network_name: &str) -> Option<&mut Arc<ExExTx>> {
        self.channels.get_mut(network_name)
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
