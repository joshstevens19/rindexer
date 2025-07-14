use crate::provider::{JsonRpcCachedProvider, ProviderError, RECOMMENDED_RPC_CHUNK_SIZE, RPC_CHUNK_SIZE};
use alloy::primitives::U64;
use alloy::rpc::types::Log;
use alloy_chains::Chain;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockClockError {
    #[error("Failed to attach timestamps: {0}")]
    ProviderError(#[from] ProviderError),

    #[error("{0} missing block {1} in requested range")]
    MissingBlockInRange(Chain, U64),
}

#[derive(Debug, Clone)]
pub struct BlockClock {
    pub enabled: bool,
    sample_rate: f32,
    provider: Arc<JsonRpcCachedProvider>,
}

impl BlockClock {
    pub fn new(
        enabled: bool,
        sample_rate: Option<f32>,
        provider: Arc<JsonRpcCachedProvider>,
    ) -> Self {
        let bounded_sampling = sample_rate.unwrap_or(1.0).max(0.001).min(1.0);
        Self { enabled, provider, sample_rate: bounded_sampling }
    }

    /// Given the defined confidence score and a starting block, get the maximum set of block
    /// timestamps available to us in a single provider request.
    fn block_range_samples(&self, from: u64, to: u64) -> Vec<u64> {
        let range = to - from;
        let batch = (RPC_CHUNK_SIZE / 2) as u64;

        // Smaller batches can be executed without sampling
        if range <= RECOMMENDED_RPC_CHUNK_SIZE as u64 {
            return (from..=to).collect::<Vec<u64>>();
        }

        // Because this method is also called on smaller ranges, we cannot always rely on the sample
        // rate to inform size effectively. We enforce a minimum sample range to help.
        let min_interval = 50;
        let min_sample_count = (range / min_interval).max(1);
        let sample_rate = self.sample_rate;

        let ideal_sample_count = (batch as f32 / sample_rate).ceil() as u64;
        let sample_count = ideal_sample_count.max(min_sample_count).min(batch);
        let step = (range / sample_count).max(1);
        let mut samples = Vec::with_capacity(sample_count as usize);

        samples.push(from);

        for i in 1..(sample_count - 1) {
            let block = from + i * step;
            if block >= to {
                break;
            }
            samples.push(block);
        }

        samples.push(to);
        samples.dedup();

        samples
    }

    /// Interpolates blocks between known "anchors"
    ///
    /// # Note
    ///
    /// This method works most accurately when blocks in the same window have similar block-times.
    /// However, this does not hold true for blocks near ethereum genesis block.
    fn interpolate(&self, anchors: &mut [(u64, u64)]) -> BTreeMap<u64, u64> {
        let mut dates = BTreeMap::new();

        if anchors.is_empty() {
            return dates;
        }

        if anchors.len() == 1 {
            dates.insert(anchors[0].0, anchors[0].1);
            return dates;
        }

        anchors.sort_by(|(block_a, _), (block_b, _)| block_a.partial_cmp(block_b).unwrap());

        let (first, _) = *anchors.first().unwrap();
        let (last, _) = *anchors.last().unwrap();
        let blocks = first..=last;

        for block in blocks.clone() {
            let (left, right) = anchors
                .windows(2)
                .find(|window| window[0].0 <= block && block <= window[1].0)
                .map(|w| (w[0], w[1]))
                .unwrap_or_else(|| {
                    (anchors[anchors.len().saturating_sub(2)], anchors[anchors.len() - 1])
                });

            // Cast to signed floats to allow for negative values in the case of a future block
            // being older than a past block (it has happened). See example:
            //  - https://optimistic.etherscan.io/block/464390
            //  - https://optimistic.etherscan.io/block/464400
            let (block_l, time_l) = (left.0 as f64, left.1 as f64);
            let (block_r, time_r) = (right.0 as f64, right.1 as f64);

            let range_avg = (time_r - time_l) / (block_r - block_l);
            let range_time = (block as f64 - block_l) * range_avg;
            let timestamp = time_l + range_time;

            assert!(timestamp > 0.0, "timestamp cannot be negative or zero");

            dates.insert(block, timestamp as u64);
        }

        dates
    }

    /// Accept an input of blocks we care to fetch, and go about obtaining those blocks in an
    /// efficient mannger whilst respecting the confidence configuration.
    ///
    /// This is a "reactive" method used for when we don't already have a store of "known block
    /// timestamp" estimations to pull from without going over the network.
    ///
    /// This method assumes the caller **will not** call out-of-range blocks.
    pub async fn get_blocks(&self, blocks: &[u64]) -> Result<BTreeMap<u64, u64>, BlockClockError> {
        let mut blocks = blocks.to_vec();

        blocks.sort_unstable();
        blocks.dedup();

        let first = blocks[0];
        let last = blocks[blocks.len() - 1];
        let sampling = self.block_range_samples(first, last);
        let blocks = blocks.into_iter().map(|b| U64::from(b)).collect::<Vec<U64>>();

        if blocks.len() <= sampling.len() {
            let block = self.provider.get_block_by_number_batch(&blocks, false).await?;
            let timestamps = block.iter().map(|n| (n.header.number, n.header.timestamp));
            let timestamps = BTreeMap::from_iter(timestamps);
            return Ok(timestamps);
        }

        let mut anchors = self
            .provider
            .get_block_by_number_batch(&blocks, false)
            .await?
            .iter()
            .map(|n| (n.header.number, n.header.timestamp))
            .collect::<Vec<_>>();
        let dates = self.interpolate(&mut anchors);

        for block in blocks {
            if !dates.contains_key(&block.to()) {
                return Err(BlockClockError::MissingBlockInRange(self.provider.chain, block));
            }
        }

        Ok(dates)
    }

    /// Intelligently attaches timestamps to any logs.
    ///
    /// - Will not make fetches if the log already has a timestamp
    /// - Will sample block ranges where doing so will minimize networking time
    /// - Will use local-first compressed "delta run-encoded" block timestamps where possible
    pub async fn attach_log_timestamps(&self, logs: Vec<Log>) -> Result<Vec<Log>, BlockClockError> {
        let blocks_without_ts = logs
            .iter()
            .filter_map(|n| if n.block_timestamp.is_none() { n.block_number } else { None })
            .collect::<Vec<_>>();

        if blocks_without_ts.is_empty() {
            return Ok(logs);
        };

        let timestamps = self.get_blocks(&blocks_without_ts).await?;
        let (logs_with_ts, logs_without_ts) = logs
            .into_iter()
            .map(|mut log| {
                if let Some(block_number) = log.block_number {
                    if let Some(timestamp) = timestamps.get(&block_number) {
                        log.block_timestamp = Some(*timestamp);
                    }
                }
                log
            })
            .partition::<Vec<_>, _>(|x| x.block_timestamp.is_some());

        if !logs_without_ts.is_empty() {
            return Box::pin(self.attach_log_timestamps(logs_without_ts)).await;
        };

        Ok(logs_with_ts)
    }
}
