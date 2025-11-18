//! # Run Length Encoder
//!
//! The run length encoder leverages the quality of "regularity" in blockchain timestamps.
//!
//! Most chains have consistent patterns or sequences of block time deltas. For example, polygon
//! is mostly consistent 2s blocktimes, but occasionally has a 6s timestamp.
//!
//! We can therefore make a two tradeoffs:
//!  1. Compute in exchange for storage bytes (disk; or RAM during runtime execution)
//!  2. Storage bytes in exchange for Network IO latency
//!
//! ## Encoding
//!
//! We encode the timestamps in a delta-run-length format, and then serialize it in a binary [`zstd`]
//! compressed file. We recommend ending with a `.blockclock` extension.
//!
//! This allows us to potentially use a few kB to store the timestamps of every chain on a network.
//!
//! ## Maximum Block
//!
//! One limitation of this approach is that we must compute the timestamps in advance. Therefore the
//! primary use-case for this encoding is to speed-up large-scale backfill operations.
//!
//! Backfills can often span many networks over long periods of time and require timestamps for tens
//! or hundreds of millions of block. This can add significant latency and so instead we can use a
//! tiny amount of CPU to compute these values instead.
//!
//! For optimal use it is beneficial to regularly extend the compressed binary files with more recent
//! blocks and so the encoding should be run and published semi-regularly.

use alloy::eips::BlockNumberOrTag;
use alloy::network::AnyRpcBlock;
use alloy::rpc::client::RpcClient;
use alloy::rpc::types::Log;
use anyhow::Context;
use bincode::{config, Decode, Encode};
use cfg_if::cfg_if;
use futures::future::try_join_all;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::select;
use tokio::time::{sleep, Instant};
use zstd::{Decoder, Encoder};

static BLOCKCLOCK_CACHE: Lazy<RwLock<HashMap<u32, Arc<DeltaEncoder>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// The maximum size of an RPC call for the application
const RPC_CHUNK_SIZE: usize = 1000;

/// The magic number of "spacing" at which we create indexed checkpoints.
///
/// The smaller the number, the faster the lookups at the cost of some "memory".
const MAGIC_INDEX_INTERVAL: u64 = 100_000;

/// A timestamp delta run length.
#[derive(Debug, Deserialize, Serialize, Encode, Decode)]
pub struct DeltaRunLen {
    /// The length for this specific delta run.
    len: u64,
    /// The difference between each timestamp in seconds (delta).
    delta: u64,
}

/// Metadata and [`DeltaRunLength`] associated with a networks timestamps.
#[derive(Debug, Deserialize, Serialize, Encode, Decode)]
pub struct EncodedDeltas {
    /// The network ID.
    network: u32,
    /// The timestamp of the first block in the chain.
    genesis_timestamp: Option<u64>,
    /// The maximum block number that has been encoded.
    max_block: u64,
    /// The maximum block timestamp that has been encoded, required for continuity when
    /// encoding across multiple runs.
    max_block_timestamp: Option<u64>,
    /// A continuous vector of delta run-lengths.
    runs: Vec<DeltaRunLen>,
}

impl EncodedDeltas {
    pub fn new(network: u32) -> Self {
        Self {
            network,
            genesis_timestamp: None,
            max_block: 0,
            max_block_timestamp: None,
            runs: vec![],
        }
    }
}

/// Delta run length encoder.
///
/// Polls a network's blockchain and persists the runs of timestamp deltas
/// to efficiently allow a timestamp lookup for non-fixed-length networks.
#[derive(Debug)]
pub struct DeltaEncoder {
    /// The network ID.
    network: u32,
    /// An RPC Client corresponding to the provided network.
    ///
    /// If rpc is set to `None`, we will only be able to `decode` from a file and compute
    /// timestamps. An `Error` will be returned on any attempt to encode new blocks.
    rpc: Option<RpcClient>,
    /// The run length encoded deltas.
    encoded_deltas: EncodedDeltas,
    /// A fully qualified Path to the file used for persistence.
    file_path: PathBuf,
    /// An index we built in-memory when deserializing the file, assists with fast lookups.
    ///
    /// `block_number → (run_index, cumulative_ts)`
    index: BTreeMap<u64, (usize, u64)>,
}

impl DeltaEncoder {
    /// Create a new [`DeltaEncoder`] for a network.
    pub fn new(network: u32, rpc_url: Option<&str>, file_path: impl Into<PathBuf>) -> Self {
        let client =
            rpc_url.map(|url| RpcClient::new_http(url.parse().expect("RPC URL is invalid")));
        let deltas = EncodedDeltas::new(network);

        Self {
            network,
            rpc: client,
            encoded_deltas: deltas,
            file_path: file_path.into(),
            index: BTreeMap::new(),
        }
    }

    /// Get a blockclock with the benefit of a global cache layer
    pub fn from_file(
        network: u32,
        rpc_url: Option<&str>,
        file_path: &PathBuf,
    ) -> anyhow::Result<Arc<Self>> {
        {
            let cache = BLOCKCLOCK_CACHE.read().unwrap();
            if let Some(clock) = cache.get(&network) {
                return Ok(clock.clone());
            }
        }

        let encoder = DeltaEncoder::from_file_inner(network, rpc_url, file_path)?;
        let encoder = Arc::new(encoder);

        let mut cache = BLOCKCLOCK_CACHE.write().unwrap();
        Ok(cache.entry(network).or_insert_with(|| encoder.clone()).clone())
    }

    /// Create or restore a [`DeltaEncoder`] for a network from the filesystem persistence.
    pub fn from_file_inner(
        network: u32,
        rpc_url: Option<&str>,
        file_path: &PathBuf,
    ) -> anyhow::Result<Self> {
        let encoded_deltas = if file_path.exists() {
            let file = File::open(file_path)?;
            let reader = BufReader::new(file);
            let mut zstd_decoder = Decoder::new(reader)?;

            tracing::info!("Preparing blockclock for network: {}", network);
            bincode::decode_from_std_read(&mut zstd_decoder, config::standard())?
        } else {
            return Ok(Self::new(network, rpc_url, file_path));
        };

        let mut encoder = Self {
            network,
            rpc: rpc_url.map(|url| RpcClient::new_http(url.parse().expect("RPC URL is invalid"))),
            encoded_deltas,
            file_path: file_path.clone(),
            index: BTreeMap::new(),
        };

        encoder.build_index(MAGIC_INDEX_INTERVAL);

        Ok(encoder)
    }

    /// Build the index when deserializing from the file.
    pub fn build_index(&mut self, interval: u64) {
        let Some(genesis) = self.encoded_deltas.genesis_timestamp else { return };

        let mut bnum = 0;
        let mut agg_ts = genesis;

        for (i, run) in self.encoded_deltas.runs.iter().enumerate() {
            if bnum % interval == 0 {
                self.index.insert(bnum, (i, agg_ts));
            }

            agg_ts += run.delta * run.len;
            bnum += run.len;
        }
    }

    /// Write the contents of the in-memory datastructure to disk in [`zstd`] encoded binary format.
    fn serialize_to_file(&self) -> anyhow::Result<()> {
        let bin_file = File::create(&self.file_path)?;
        let bin_writer = BufWriter::new(bin_file);
        let mut zstd_encoder = Encoder::new(bin_writer, 0)?;

        bincode::encode_into_std_write(
            &self.encoded_deltas,
            &mut zstd_encoder,
            config::standard(),
        )?;
        zstd_encoder.finish()?;

        cfg_if! {
            if #[cfg(feature = "debug-json")] {
                let json_path = self.file_path.with_extension("json");
                let json_file = File::create(json_path)?;
                let json_writer = BufWriter::new(json_file);
                serde_json::to_writer_pretty(json_writer, &self.encoded_deltas)?;
            }
        }

        Ok(())
    }

    /// Fetch a set of blocks and their timestamps from RPC calls.
    async fn get_block_by_number_batch(
        &self,
        block_numbers: &[u64],
        include_txs: bool,
    ) -> anyhow::Result<Vec<(u64, u64)>> {
        let chain = self.network;

        if block_numbers.is_empty() {
            return Ok(Vec::new());
        }

        let mut block_numbers = block_numbers.to_vec();
        block_numbers.dedup();

        let client = self.rpc.clone().context("cannot fetch batch when rpc client is None")?;

        // Use less than the max chunk as recommended in most provider docs
        let futures = block_numbers
            .chunks(RPC_CHUNK_SIZE)
            .map(|chunk| {
                let owned_chunk = chunk.to_vec();
                let client = client.clone();

                tokio::spawn(async move {
                    let mut batch = client.new_batch();
                    let mut request_futures = Vec::with_capacity(owned_chunk.len());

                    for block_num in owned_chunk {
                        let params = (BlockNumberOrTag::Number(block_num), include_txs);
                        let call = batch.add_call("eth_getBlockByNumber", &params)?;
                        request_futures.push(call)
                    }

                    if let Err(e) = batch.send().await {
                        tracing::error!(
                            "Failed to send {} batch 'eth_getBlockByNumber' request for {}: {:?}",
                            request_futures.len(),
                            chain,
                            e
                        );
                        return Err(e);
                    }

                    try_join_all(request_futures).await
                })
            })
            .collect::<Vec<_>>();

        let chunk_results: Vec<Result<Vec<AnyRpcBlock>, _>> = try_join_all(futures).await?;
        let results = chunk_results
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .map(|r| (r.header.number, r.header.timestamp))
            .collect();

        tracing::debug!(
            "Fetched blocks: {}..{}",
            block_numbers.iter().min().unwrap(),
            block_numbers.iter().max().unwrap()
        );

        Ok(results)
    }

    /// Get the latest block for the network.
    async fn get_head_block(&self) -> anyhow::Result<u64> {
        let client = self.rpc.clone().context("cannot fetch batch when rpc client is None")?;
        let res: String = client.request("eth_blockNumber", &()).await?;
        let hex = res.replace("0x", "");
        let block = u64::from_str_radix(&hex, 16)?;

        Ok(block)
    }

    /// Compress the runlen data.
    ///
    /// Accept a list of block numbers and their timestamps, update the run-lengths. Enforces that
    /// the incoming vec of blocks is sequential.
    fn encode_deltas(&mut self, mut blocks: Vec<(u64, u64)>) -> anyhow::Result<&EncodedDeltas> {
        blocks.sort_by_key(|(block, _)| *block);

        if blocks.is_empty() {
            return Ok(&self.encoded_deltas);
        }

        tracing::debug!("{:-<32}", "");
        tracing::debug!("{:<10} | {:<20}", "Block", "Timestamp");
        tracing::debug!("{:-<32}", "");

        for (block, timestamp) in &blocks {
            tracing::debug!("{:<10} | {:<20}", block, timestamp);
        }

        let first_block_in_batch = blocks.first().expect("non-zero already checked").0;
        let expected_block = self.encoded_deltas.max_block + 1;

        if first_block_in_batch != expected_block
            && self.encoded_deltas.max_block == 0
            && first_block_in_batch < 2
        {
            anyhow::bail!(
                "Deltas must be encoded in sequential order, expected {:?} got {:?}",
                expected_block,
                first_block_in_batch
            );
        }

        let mut previous_timestamp = if self.encoded_deltas.max_block == 0 {
            self.encoded_deltas.genesis_timestamp
        } else {
            self.encoded_deltas.max_block_timestamp
        };

        for (_block_num, block_ts) in &blocks {
            if let Some(prev_ts) = previous_timestamp {
                let delta = block_ts.saturating_sub(prev_ts);

                if let Some(last_run) = self.encoded_deltas.runs.last_mut() {
                    if last_run.delta == delta {
                        last_run.len += 1;
                    } else {
                        self.encoded_deltas.runs.push(DeltaRunLen { len: 1, delta });
                    }
                } else {
                    self.encoded_deltas.runs.push(DeltaRunLen { len: 1, delta });
                }
            }

            previous_timestamp = Some(*block_ts);
        }

        if let Some((last_block_num, last_block_ts)) = blocks.last() {
            self.encoded_deltas.max_block = *last_block_num;
            self.encoded_deltas.max_block_timestamp = Some(*last_block_ts);
        }

        Ok(&self.encoded_deltas)
    }

    /// Some RPC do not return timestamp in block `0` so we are forced to provide genesis time
    /// manually instead.
    pub fn genesis_time(&self) -> Option<u64> {
        match &self.network {
            1 => Some(1438269973),
            10 => Some(1610639500),
            42161 => Some(1622240000),
            _ => Some(0),
        }
    }

    /// Fetch and encode deltas and persist to a file path.
    async fn fetch_encode_persist(&mut self, batch_size: u64) -> anyhow::Result<()> {
        if self.encoded_deltas.genesis_timestamp.is_none() {
            match self.genesis_time() {
                Some(ts) => self.encoded_deltas.genesis_timestamp = Some(ts),
                None => {
                    if let Some(genesis) =
                        self.get_block_by_number_batch(&[0], false).await?.first()
                    {
                        if genesis.1 == 0 {
                            anyhow::bail!(
                                "Rpc returned a zero timestamp for genesis block. Please add \
                                network {} to list of manually defined genesis timestamps.",
                                self.network
                            );
                        }

                        self.encoded_deltas.genesis_timestamp = Some(genesis.1);
                    }
                }
            }

            tracing::info!(
                "Fetched genesis timestamp: {}",
                self.encoded_deltas.genesis_timestamp.unwrap()
            );
        }

        let next_block = self.encoded_deltas.max_block + 1;
        let end_block = next_block + batch_size - 1;
        let block_numbers: Vec<u64> = (next_block..=end_block).collect();

        let blocks = match self.get_block_by_number_batch(&block_numbers, false).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(
                    "Error fetching blocks {}-{} for network {}: {:?}",
                    next_block,
                    end_block,
                    self.network,
                    e
                );
                sleep(Duration::from_millis(1000)).await;
                return Ok(());
            }
        };

        self.encode_deltas(blocks)?;

        tracing::info!("Encoded blocks {}–{} (network {})", next_block, end_block, self.network);

        Ok(())
    }

    /// Continuously poll for new blocks, encode deltas, and persist to disk.
    pub async fn poll_encode_loop(&mut self, batch_size: u64) -> anyhow::Result<()> {
        let max_block_for_network = self.get_head_block().await?;
        let flush_duration_secs = 180;
        let mut flush_interval = Instant::now();

        tracing::info!(
            "Beginning poll-based block-timestamp encoding. Max block {max_block_for_network}."
        );

        loop {
            if self.encoded_deltas.max_block + batch_size >= max_block_for_network {
                tracing::info!("Exiting poll loop. Max block reached.");
                if let Err(e) = self.serialize_to_file() {
                    tracing::error!("Error flushing to disk: {:?}", e);
                }
                tracing::info!("Finished flushing file to disk.");
                break;
            }

            select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl+C received, exiting poll loop.");
                    if let Err(e) = self.serialize_to_file() {
                        tracing::error!("Error flushing to disk: {:?}", e);
                    }
                    tracing::info!("Finished flushing file to disk.");
                    break;
                }

                result = self.fetch_encode_persist(batch_size) => {
                    if flush_interval.elapsed().as_secs() >= flush_duration_secs {
                        match self.serialize_to_file() {
                            Ok(_) => tracing::info!("✅ Checkpoint reached. Flushed file to disk."),
                            Err(e) => tracing::error!("Error flushing to disk: {:?}", e)
                        }
                        flush_interval = Instant::now();
                    };

                    if let Err(e) = result {
                        tracing::error!("Error fetching and encoding blocks: {:?}", e)
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the timestamp for any block.
    ///
    /// Returns `Some(timestamp)` if we can compute the block or `None` when the block is outside
    /// of the range we have indexed and can safely compute.
    pub fn get_block_timestamp(&self, block_number: &u64) -> Option<u64> {
        let genesis = self.encoded_deltas.genesis_timestamp?;
        if *block_number > self.encoded_deltas.max_block {
            return None;
        }

        let (mut bnum, mut agg_ts, run_idx) =
            if let Some((&checkpoint, &(idx, ts))) = self.index.range(..=*block_number).last() {
                (checkpoint, ts, idx)
            } else {
                (0, genesis, 0)
            };

        for run in &self.encoded_deltas.runs[run_idx..] {
            if bnum + run.len >= *block_number {
                let offset = block_number - bnum;
                return Some(agg_ts + run.delta * offset);
            } else {
                agg_ts += run.delta * run.len;
                bnum += run.len;
            }
        }

        None
    }

    /// Returns a BTreeMap of block_number → timestamp for all block numbers
    /// in the input slice that can be resolved.
    pub fn get_block_timestamps(&self, block_numbers: &[u64]) -> BTreeMap<u64, u64> {
        let genesis = match self.encoded_deltas.genesis_timestamp {
            Some(ts) => ts,
            None => return BTreeMap::new(),
        };

        let max_block = self.encoded_deltas.max_block;

        // Sort and dedup the input for efficient lookup
        let mut requested: Vec<u64> =
            block_numbers.iter().copied().filter(|&b| b <= max_block).collect();

        if requested.is_empty() {
            return BTreeMap::new();
        }

        requested.sort_unstable();
        requested.dedup();

        // Prepare output
        let mut output = BTreeMap::new();

        // Use latest checkpoint <= max requested block
        let start_block = *requested.last().unwrap();
        let (mut bnum, mut agg_ts, run_idx) =
            if let Some((&checkpoint, &(idx, ts))) = self.index.range(..=start_block).last() {
                (checkpoint, ts, idx)
            } else {
                (0, genesis, 0)
            };

        // Walk runs and assign timestamps
        let mut req_idx = 0;

        for run in &self.encoded_deltas.runs[run_idx..] {
            let end = bnum + run.len;

            while req_idx < requested.len() {
                let blk = requested[req_idx];
                if blk < bnum {
                    // Earlier than current run start — should not happen
                    req_idx += 1;
                    continue;
                } else if blk < end {
                    let offset = blk - bnum;
                    output.insert(blk, agg_ts + run.delta * offset);
                    req_idx += 1;
                } else {
                    break;
                }
            }

            bnum = end;
            agg_ts += run.delta * run.len;
        }

        output
    }

    /// Attaches timestamps to any logs it can and returns the full set.
    ///
    /// This is a simple pass-through method and does not guarantee all logs in the batch
    /// will actually get timestamps attached.
    pub fn try_attach_log_timestamps(&self, logs: Vec<Log>) -> Vec<Log> {
        let blocks_without_ts = logs
            .iter()
            .filter_map(|n| if n.block_timestamp.is_none() { n.block_number } else { None })
            .collect::<Vec<_>>();

        if blocks_without_ts.is_empty() {
            return logs;
        };

        let timestamps = self.get_block_timestamps(&blocks_without_ts);
        let logs_with_maybe_ts = logs
            .into_iter()
            .map(|mut log| {
                if let Some(block_number) = log.block_number {
                    if let Some(timestamp) = timestamps.get(&block_number) {
                        log.block_timestamp = Some(*timestamp);
                    }
                }
                log
            })
            .collect();

        logs_with_maybe_ts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::random_range;
    use tempfile::tempdir;

    #[test]
    fn test_encode_deltas_run_length_encoding() {
        let mut encoder = DeltaEncoder {
            network: 1,
            index: BTreeMap::new(),
            rpc: None,
            encoded_deltas: EncodedDeltas::new(1),
            file_path: tempdir().unwrap().keep(),
        };

        let blocks = vec![(100, 1000), (101, 1012), (102, 1024), (103, 1036), (104, 1051)];
        let result = encoder.encode_deltas(blocks).unwrap();

        assert_eq!(result.network, 1);
        assert_eq!(result.max_block, 104);
        assert_eq!(result.runs.len(), 2);

        assert_eq!(result.runs[0].len, 3);
        assert_eq!(result.runs[0].delta, 12);

        assert_eq!(result.runs[1].len, 1);
        assert_eq!(result.runs[1].delta, 15);
    }

    #[test]
    fn test_serialize_and_deserialize_file_roundtrip() {
        let file_path = tempdir().unwrap().keep().join("block-deltas");
        let mut encoder = DeltaEncoder {
            network: 99,
            rpc: None,
            file_path: file_path.clone(),
            index: BTreeMap::new(),
            encoded_deltas: EncodedDeltas {
                network: 99,
                genesis_timestamp: Some(0),
                max_block: 0,
                max_block_timestamp: None,
                runs: vec![],
            },
        };
        let blocks = vec![(1, 10), (2, 20), (3, 30), (4, 31), (5, 32)];

        encoder.encode_deltas(blocks).unwrap();
        encoder.serialize_to_file().unwrap();

        let reloaded = DeltaEncoder::from_file(99, None, &file_path).unwrap();

        assert_eq!(reloaded.encoded_deltas.network, 99);
        assert_eq!(reloaded.encoded_deltas.max_block, 5);
        assert_eq!(reloaded.encoded_deltas.max_block_timestamp, Some(32));
        assert_eq!(reloaded.encoded_deltas.runs.len(), 2);
        assert_eq!(reloaded.encoded_deltas.runs[0].len, 3);
        assert_eq!(reloaded.encoded_deltas.runs[0].delta, 10);
        assert_eq!(reloaded.encoded_deltas.runs[1].len, 2);
        assert_eq!(reloaded.encoded_deltas.runs[1].delta, 1);

        assert_eq!(reloaded.get_block_timestamp(&0), Some(0));
        assert_eq!(reloaded.get_block_timestamp(&1), Some(10));
        assert_eq!(reloaded.get_block_timestamp(&2), Some(20));
        assert_eq!(reloaded.get_block_timestamp(&3), Some(30));
        assert_eq!(reloaded.get_block_timestamp(&4), Some(31));
        assert_eq!(reloaded.get_block_timestamp(&5), Some(32));
        assert_eq!(reloaded.get_block_timestamp(&6), None);
    }

    /// E2E Test
    ///
    /// This test should ideally be moved to an e2e test and run less frequently
    /// as it makes a number of real RPC calls and may experience network issues.
    #[tokio::test]
    #[ignore]
    async fn test_fetch_encode_persist() -> anyhow::Result<()> {
        // This should be filled in!
        let rpc_url = "https://polygon-mainnet.g.alchemy.com/v2/XXXXX";
        let network_id = 137;
        let file_path = tempdir()?.keep().join("block-deltas");

        if rpc_url.is_empty() {
            return Ok(());
        }

        let mut encoder = DeltaEncoder::new(network_id, Some(rpc_url), &file_path);
        encoder.fetch_encode_persist(100).await?;
        drop(encoder);

        let mut reloaded = DeltaEncoder::from_file_inner(network_id, Some(rpc_url), &file_path)?;

        reloaded.fetch_encode_persist(100).await?;

        assert_eq!(reloaded.encoded_deltas.network, network_id);
        assert_eq!(reloaded.encoded_deltas.max_block, 200);

        let range: Vec<u64> = (1..=200).collect();
        let blocks = reloaded.get_block_by_number_batch(&range, false).await?;

        for (block, actual_timestamp) in blocks {
            let timestamp = reloaded.get_block_timestamp(&block).unwrap();
            assert_eq!(actual_timestamp, timestamp, "Block {} timestamp mismatch", block);
        }

        Ok(())
    }

    /// E2E Test (Disabled, manually enable as required)
    ///
    /// Spotcheck the files on each run to ensure we don't contain errors.
    #[tokio::test]
    // #[ignore]
    async fn spotcheck_file() -> anyhow::Result<()> {
        let rpc_url = "https://arb-mainnet.g.alchemy.com/v2/LfCdXVQaAS7hctR3N78Sj";
        let network_id = 42161;

        let sample_count = 100;
        let base_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("core")
            .join("resources")
            .join("blockclock")
            .join(network_id.to_string())
            .with_extension("blockclock");

        println!("Base path: {:?}", base_path);

        if rpc_url.is_empty() {
            return Ok(());
        }

        let reloaded = DeltaEncoder::from_file(network_id, Some(rpc_url), &base_path)?;
        let mut samples = Vec::with_capacity(sample_count);

        for _ in 1..=sample_count {
            samples.push(random_range(1..=reloaded.encoded_deltas.max_block));
        }

        samples.push(231191);

        let blocks = reloaded.get_block_by_number_batch(&samples, false).await?;

        for (block, actual_timestamp) in blocks {
            let start = Instant::now();
            let timestamp = reloaded.get_block_timestamp(&block).unwrap();
            let end = start.elapsed();
            assert_eq!(actual_timestamp, timestamp, "Block {} timestamp mismatch", block);
            println!("Block {} timestamp: {} in {}ms", block, timestamp, end.as_millis());
        }

        Ok(())
    }
}
