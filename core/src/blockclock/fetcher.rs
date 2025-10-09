use crate::provider::{JsonRpcCachedProvider, ProviderError, RECOMMENDED_RPC_CHUNK_SIZE};
use alloy::primitives::U64;
use alloy::rpc::types::Log;
use alloy_chains::Chain;
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlockFetcherError {
    #[error("Failed to attach timestamps: {0}")]
    ProviderError(#[from] ProviderError),

    #[error("{0} missing block {1} in requested range")]
    MissingBlockInRange(Chain, U64),
}

#[derive(Debug, Clone)]
pub struct BlockFetcher {
    sample_rate: f32,
    pub(super) provider: Arc<JsonRpcCachedProvider>,
}

impl BlockFetcher {
    pub fn new(sample_rate: Option<f32>, provider: Arc<JsonRpcCachedProvider>) -> Self {
        let bounded_sampling = sample_rate.unwrap_or(1.0).clamp(0.001, 1.0);
        Self { provider, sample_rate: bounded_sampling }
    }

    /// Given the defined confidence score and a starting block, get the maximum set of block
    /// timestamps available to us in a single provider request.
    fn block_range_samples(&self, from: u64, to: u64) -> Vec<u64> {
        if from > to {
            return vec![];
        }

        if from == to {
            return vec![to];
        }

        let range = to - from;
        let total_blocks = range + 1;
        let desired_sample_count =
            ((total_blocks as f64) * self.sample_rate as f64).ceil() as usize;
        let sample_count = desired_sample_count.clamp(2, total_blocks as usize);

        // Small ranges can just be executed directly as there will be minimal overhead
        // and sampling doesn't add value in these cases necessary.
        if range <= (RECOMMENDED_RPC_CHUNK_SIZE as u64 / 2) {
            return (from..=to).collect();
        }

        if sample_count >= total_blocks as usize {
            return (from..=to).collect();
        }

        let step = total_blocks as f64 / (sample_count - 1) as f64;
        let mut samples = Vec::with_capacity(sample_count);
        samples.push(from);
        for i in 0..sample_count {
            let block = from + (i as f64 * step).round() as u64;
            if block > to {
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
    async fn get_blocks(&self, blocks: &[u64]) -> Result<BTreeMap<u64, u64>, BlockFetcherError> {
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
                return Err(BlockFetcherError::MissingBlockInRange(self.provider.chain, block));
            }
        }

        Ok(dates)
    }

    /// Intelligently attaches timestamps to any logs.
    ///
    /// - Will not make fetches if the log already has a timestamp
    /// - Will sample block ranges where doing so will minimize networking time
    /// - Will use local-first compressed "delta run-encoded" block timestamps where possible
    pub async fn attach_log_timestamps(
        &self,
        logs: Vec<Log>,
    ) -> Result<Vec<Log>, BlockFetcherError> {
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
            let mut completed = Box::pin(self.attach_log_timestamps(logs_without_ts)).await?;
            completed.extend(logs_with_ts);
            return Ok(completed);
        }

        Ok(logs_with_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_provider() -> Arc<JsonRpcCachedProvider> {
        JsonRpcCachedProvider::mock(1)
    }

    #[test]
    fn test_simple_interpolation() {
        let clock = BlockFetcher::new(None, mock_provider());

        // Lets assume perfect times, 1 block = 1 second.
        let mut anchors = vec![(1, 1), (10, 10), (20, 20), (30, 30), (10_000, 10_000)];
        let interpolated_blocks = clock.interpolate(&mut anchors);
        for (block, timestamp) in interpolated_blocks {
            assert_eq!(block, timestamp)
        }

        // Lets assume perfect times, 1 block = 5 second.
        let mut anchors = vec![(1, 5), (10_001, 50_005)];
        let interpolated_blocks = clock.interpolate(&mut anchors);
        for (block, timestamp) in interpolated_blocks {
            assert_eq!(block * 5, timestamp)
        }
    }

    #[test]
    fn test_ratio_sampling_min() {
        let clock = BlockFetcher::new(Some(1.0), mock_provider());
        let samples = clock.block_range_samples(1000, 1234);
        let actual_sampling_ratio = samples.len() as f32 / (1234 - 1000) as f32;

        assert!(actual_sampling_ratio >= 1.0);

        let clock = BlockFetcher::new(Some(0.1), mock_provider());
        let samples = clock.block_range_samples(1000, 1019);
        let actual_sampling_ratio = samples.len() as f32 / (1019 - 1000) as f32;

        assert!(actual_sampling_ratio >= 0.1);

        let clock = BlockFetcher::new(Some(0.1), mock_provider());
        let samples = clock.block_range_samples(1000, 1600);
        let actual_sampling_ratio = samples.len() as f32 / (1600 - 1000) as f32;

        assert!(actual_sampling_ratio >= 0.1);
        assert!(actual_sampling_ratio <= 0.15); // Ensure we don't oversample here either

        let clock = BlockFetcher::new(Some(0.5), mock_provider());
        let samples = clock.block_range_samples(1000, 1300);
        let actual_sampling_ratio = samples.len() as f32 / (1300 - 1000) as f32;

        assert!(actual_sampling_ratio >= 0.5);
        assert!(actual_sampling_ratio <= 0.6); // Ensure we don't oversample here either
    }

    #[test]
    fn test_sampling() {
        let clock = BlockFetcher::new(Some(0.1), mock_provider());

        assert_eq!(clock.block_range_samples(10, 10), vec![10]);
        assert_eq!(clock.block_range_samples(10, 11), vec![10, 11]);
        assert_eq!(clock.block_range_samples(100, 105), vec![100, 101, 102, 103, 104, 105]);

        // Handles bad input data without panic
        assert!(clock.block_range_samples(11, 10).is_empty());
    }

    #[test]
    fn test_sampling_unique() {
        let clock = BlockFetcher::new(Some(0.1), mock_provider());
        let samples = clock.block_range_samples(1000, 1600);
        let unique_samples = samples.iter().cloned().collect::<std::collections::HashSet<_>>();

        assert_eq!(unique_samples.len(), samples.len());
        assert!(samples.contains(&1000));
        assert!(samples.contains(&1600));

        let wide_samples = clock.block_range_samples(0, 20_000);
        assert!(wide_samples.contains(&0));
        assert!(wide_samples.contains(&20_000));
    }

    #[test]
    fn test_sampling_window_sequence() {
        let clock = BlockFetcher::new(Some(0.1), mock_provider());
        let samples = clock.block_range_samples(1000, 1600);

        assert!(samples.windows(2).all(|w| w[0] < w[1]));

        // Small range, sampling would result in too few blocks, min interval forces more
        // min_interval = 50 → expect at least one mid-sample
        let tight_samples = clock.block_range_samples(5000, 5050);
        let mid_steps: Vec<_> = tight_samples.windows(2).map(|w| w[1] - w[0]).collect();

        assert!(tight_samples.len() > 2);
        assert!(mid_steps.iter().any(|&step| step < 50));
    }
}
