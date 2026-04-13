//! # Return validated block timestamps
//!
//! We can return block-timestamps more intelligently than calling RPCs in two ways:
//!   1. Returned in logs
//!   2. Delta run-length encoded
//!   3. Fixed spaced block-timestamps (an extreme variant on case 1)
//!
//! ## Returned in logs
//!
//! This is the best way to get a timestamp. It requires no additional work. We simply receive the
//! timestamp from the log itself and can skip additional calculations or lookups.
//!
//! ## Delta run-length encoded
//!
//! Delta run-length encoding is an effective way to support block-timestamps that are not
//! necessarily sequential but generally follow a pattern.
//!
//! Most chains will have a roughly "fixed" block-time, and this can be used to encode the
//! block-timestamps more efficiently via "runs" of the delta between times.
//!
//! This process requires more upfront-work and more storage/memory, but can be a great way to save
//! on network requests and IO time.
//!
//! ## Fixed spaced block-timestamps
//!
//! These are the simplest of the networks, it is the most extreme delta-run-length encoding
//! and can therefore be optimized even more.
//!
//! Rather than storing "runs", we consider the whole chain to be a single "run" and can simply
//! calculate any timestamp for a block.
//!
//! Due to the lack of any strong guarantee, we can only do this up to a "known" block number where
//! the fixed-timestamp consistency has been validated. If at any time a chain breaks this pattern
//! we must drop back to delta run length encoding.

mod fetcher;
mod fixed;
mod runlencoder;

use crate::blockclock::fetcher::BlockFetcherError;
use crate::blockclock::fixed::SpacedNetwork;
use crate::provider::ChainProvider;
use alloy::rpc::types::Log;
pub use fetcher::BlockFetcher;
pub use runlencoder::DeltaEncoder;
use std::env;
use std::env::VarError;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockClockError {
    #[error("Failed to attach fetched timestamps: {0}")]
    BlockFetcherError(#[from] BlockFetcherError),

    #[error("Failed to get encoded filepath: {0}")]
    DeltaFilepathError(#[from] VarError),

    #[error("Failed to get decode encoded `.blockclock` file: {0}")]
    DeltaFileDecoderError(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct BlockClock {
    #[allow(unused)]
    network_id: u64,

    /// The run-length encoder to use for calculating timestamps in memory.
    ///
    /// Will be set to `None` if there is no backing file with pre-encoded data and the
    /// encoder lookup will be bypassed.
    fetcher: BlockFetcher,

    /// The run-length encoder to use for calculating timestamps in memory.
    ///
    /// Will be set to `None` if there is no backing file with pre-encoded data and the
    /// encoder lookup will be bypassed.
    runlencoder: Option<Arc<DeltaEncoder>>,
}

impl BlockClock {
    pub fn new(
        enabled: Option<bool>,
        sample_rate: Option<f32>,
        provider: Arc<dyn ChainProvider>,
    ) -> Self {
        let network_id = provider.chain().id();
        let fetcher = BlockFetcher::new(sample_rate, provider);

        // Filepath based blockclock increases startup time so only proceed to enable if
        // log timestamps are explicitly enabled.
        if !enabled.unwrap_or(false) {
            return Self { network_id, fetcher, runlencoder: None };
        }

        let filepath = get_blockclock_filepath(network_id);
        let runlencoder = filepath.and_then(|path| {
            path.is_file().then(|| {
                let coder = DeltaEncoder::from_file(network_id as u32, None, &path).ok()?;
                Some(coder)
            })?
        });

        Self { network_id, fetcher, runlencoder }
    }

    /// Attach timestamps to logs in the most efficient way possible.
    ///
    /// The following strategies are tried in order:
    ///
    /// 1. Use timestamps present in logs and return early
    /// 2. Use fixed-interval chains to compute timestamps and return
    /// 3. Use precomputed delta-encoded network timestamps
    /// 4. Fetch an optionally sampled-and-interpolated set of timestamps from RPC calls
    pub async fn attach_log_timestamps(&self, logs: Vec<Log>) -> Result<Vec<Log>, BlockClockError> {
        if logs.is_empty() {
            return Ok(logs);
        }

        // Heuristic to transform if it's too large, it's probably in milliseconds
        //
        // The timestamps can be either in milliseconds or seconds, it's unpredictable so we
        // need to check and convert to seconds of we detect milliseconds.
        let logs: Vec<Log> = logs
            .into_iter()
            .map(|mut l| {
                if let Some(ts) = l.block_timestamp {
                    if ts > 1_000_000_000_000 {
                        l.block_timestamp = Some(ts / 1000);
                    }
                }
                l
            })
            .collect();

        // 1. Use timestamps present in logs and return early.
        if logs.iter().all(|log| log.block_timestamp.is_some()) {
            return Ok(logs);
        }

        // 2. Use fixed-interval chains to compute timestamps and return
        let logs = if let Ok(spaced) = SpacedNetwork::try_from(&self.fetcher.provider.chain()) {
            logs.into_iter()
                .map(|mut log| {
                    log.block_timestamp = spaced.get_block_time(log.block_number.unwrap());
                    log
                })
                .collect()
        } else {
            logs
        };

        // 3. Use precomputed delta-encoded network timestamps
        let logs = if let Some(deltas) = &self.runlencoder {
            deltas.try_attach_log_timestamps(logs)
        } else {
            logs
        };

        // 4. Fetch an optionally sampled-and-interpolated set of timestamps from RPC calls
        let logs = self.fetcher.attach_log_timestamps(logs).await?;

        Ok(logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider::mock::MockChainProvider;
    use alloy::primitives::Log as PrimitiveLog;
    use alloy::rpc::types::Log;

    fn make_log_with_timestamp(block_number: u64, block_timestamp: Option<u64>) -> Log {
        Log {
            inner: PrimitiveLog { address: Default::default(), data: Default::default() },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    #[tokio::test]
    async fn empty_logs_returns_empty() {
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let result = clock.attach_log_timestamps(vec![]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn logs_with_timestamps_returned_unchanged() {
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let log = make_log_with_timestamp(100, Some(1_700_000_000));
        let result = clock.attach_log_timestamps(vec![log]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].block_timestamp, Some(1_700_000_000));
    }

    #[tokio::test]
    async fn millisecond_timestamps_normalized_to_seconds() {
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let log = make_log_with_timestamp(100, Some(1_700_000_000_000));
        let result = clock.attach_log_timestamps(vec![log]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].block_timestamp, Some(1_700_000_000));
    }

    #[test]
    fn new_with_enabled_false_runlencoder_is_none() {
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(Some(false), None, mock);
        assert!(clock.runlencoder.is_none());
    }

    #[test]
    fn new_with_enabled_none_runlencoder_is_none() {
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        assert!(clock.runlencoder.is_none());
    }

    #[test]
    fn new_with_enabled_true_no_file_runlencoder_is_none() {
        // No .blockclock file exists in test environment, so runlencoder stays None
        let mock = Arc::new(MockChainProvider::new(999_999));
        let clock = BlockClock::new(Some(true), None, mock);
        assert!(clock.runlencoder.is_none());
    }

    #[tokio::test]
    async fn mixed_logs_some_with_timestamps_some_without() {
        use alloy::network::{AnyHeader, AnyRpcBlock, AnyRpcHeader};
        use alloy::primitives::B256;
        use alloy::rpc::types::{Block, BlockTransactions};

        // Provide a block for block 101 so the fetcher can resolve its timestamp
        let block_101 = AnyRpcBlock::new(
            Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number: 101, timestamp: 1_700_000_012, ..Default::default() }
                        .seal(B256::ZERO),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        );

        let mock = Arc::new(MockChainProvider::new(1).with_blocks(vec![block_101]));
        let clock = BlockClock::new(None, None, mock);
        let log_with = make_log_with_timestamp(100, Some(1_700_000_000));
        let log_without = make_log_with_timestamp(101, None);
        let result = clock
            .attach_log_timestamps(vec![log_with, log_without])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        // The log that already had a timestamp keeps it
        let ts_100 = result.iter().find(|l| l.block_number == Some(100)).unwrap().block_timestamp;
        assert_eq!(ts_100, Some(1_700_000_000));
        // The log without a timestamp got it filled from the mock block
        let ts_101 = result.iter().find(|l| l.block_number == Some(101)).unwrap().block_timestamp;
        assert_eq!(ts_101, Some(1_700_000_012));
    }

    #[tokio::test]
    async fn timestamp_exactly_at_ms_boundary_is_normalized() {
        // 1_000_000_000_001 > 1_000_000_000_000 → treated as ms → divided by 1000
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let log = make_log_with_timestamp(100, Some(1_000_000_000_001));
        let result = clock.attach_log_timestamps(vec![log]).await.unwrap();
        assert_eq!(result[0].block_timestamp, Some(1_000_000_000));
    }

    #[tokio::test]
    async fn timestamp_exactly_at_ms_boundary_value_not_normalized() {
        // Exactly 1_000_000_000_000 is NOT > threshold, so it is treated as seconds
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let log = make_log_with_timestamp(100, Some(1_000_000_000_000));
        let result = clock.attach_log_timestamps(vec![log]).await.unwrap();
        assert_eq!(result[0].block_timestamp, Some(1_000_000_000_000));
    }

    #[tokio::test]
    async fn all_logs_with_timestamps_returns_early_without_fetch() {
        // Multiple logs all with timestamps → early-return path, no RPC calls needed
        let mock = Arc::new(MockChainProvider::new(1));
        let clock = BlockClock::new(None, None, mock);
        let logs = vec![
            make_log_with_timestamp(100, Some(1_700_000_000)),
            make_log_with_timestamp(101, Some(1_700_000_012)),
            make_log_with_timestamp(102, Some(1_700_000_024)),
        ];
        let result = clock.attach_log_timestamps(logs).await.unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].block_timestamp, Some(1_700_000_000));
        assert_eq!(result[1].block_timestamp, Some(1_700_000_012));
        assert_eq!(result[2].block_timestamp, Some(1_700_000_024));
    }
}

fn get_blockclock_filepath(network: u64) -> Option<PathBuf> {
    let filename = &format!("{}.blockclock", network);
    let mut paths = vec![];

    // Assume `resources` directory is in the same directory as the executable (installed)
    if let Ok(executable_path) = env::current_exe() {
        let mut path = executable_path.to_path_buf();
        path.pop(); // Remove the executable name
        path.push("resources");
        path.push("blockclock");
        path.push(filename);
        paths.push(path);

        // Also consider when running from within the `rindexer` directory
        let mut path = executable_path;
        path.pop(); // Remove the executable name
        path.pop(); // Remove the 'release' or 'debug' directory
        path.push("resources");
        path.push("blockclock");
        path.push(filename);
        paths.push(path);
    }

    // Check additional common paths
    if let Ok(home_dir) = env::var("HOME") {
        let mut path = PathBuf::from(home_dir);
        path.push(".rindexer");
        path.push("resources");
        path.push("blockclock");
        path.push(filename);
        paths.push(path);
    }

    // Return the first valid path
    for path in &paths {
        if path.exists() {
            return Some(path.to_path_buf());
        }
    }

    let extra_looking =
        paths.into_iter().next().expect("Failed to determine rindexer blockclock path");

    if !extra_looking.exists() {
        return None;
    }

    Some(extra_looking)
}
