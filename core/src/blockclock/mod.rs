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
use crate::provider::JsonRpcCachedProvider;
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

    #[error("Failed to get decode encoded `.blokclock` file: {0}")]
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
    pub fn new(sample_rate: Option<f32>, provider: Arc<JsonRpcCachedProvider>) -> Self {
        let network_id = provider.chain.id();
        let fetcher = BlockFetcher::new(sample_rate, provider);
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
    /// 2. Use fixed-interval chains to compute timestamps and return **(currently disabled)**
    /// 3. Use precomputed delta-encoded network timestamps
    /// 4. Fetch an optionally sampled-and-interpolated set of timestamps from RPC calls
    pub async fn attach_log_timestamps(&self, logs: Vec<Log>) -> Result<Vec<Log>, BlockClockError> {
        if logs.iter().all(|log| log.block_timestamp.is_some()) {
            return Ok(logs);
        }

        let logs = if let Some(deltas) = &self.runlencoder {
            deltas.try_attach_log_timestamps(logs)
        } else {
            logs
        };
        let logs = self.fetcher.attach_log_timestamps(logs).await?;

        Ok(logs)
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
