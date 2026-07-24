//! Envio HyperSync backed implementation of [`ChainProvider`].
//!
//! HyperSync serves log queries orders of magnitude faster than `eth_getLogs`, but it is
//! not a JSON-RPC node: it cannot serve `eth_call`, receipts or traces, and its archive
//! height can lag slightly behind the chain head. [`HypersyncProvider`] therefore wraps
//! the network's [`JsonRpcCachedProvider`] and only routes historical log fetches to
//! HyperSync — every other request, and any log request past the archive height,
//! delegates to the RPC provider.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::network::{AnyRpcBlock, AnyTransactionReceipt};
use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, B256, U256, U64};
use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use alloy::rpc::types::Log;
use alloy_chains::Chain;
use async_trait::async_trait;
use hypersync_client::net_types::block::BlockField;
use hypersync_client::net_types::log::{LogField, LogFilter};
use hypersync_client::net_types::Query;
use hypersync_client::{Client, StreamConfig};
use tokio::sync::broadcast::Sender;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::event::RindexerEventFilter;
use crate::manifest::network::HypersyncConfig;
use crate::metrics::rpc as rpc_metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::{ChainProvider, JsonRpcCachedProvider, ProviderError, RetryClientError};

/// Default maximum block range per HyperSync logs request. HyperSync can serve far larger
/// ranges than an RPC node, but each `get_logs` call buffers the whole range in memory so
/// it needs a bound. Overridable via `hypersync.max_block_range` or `max_block_range`.
const DEFAULT_HYPERSYNC_MAX_BLOCK_RANGE: u64 = 50_000;

/// How long a cached archive height that is *behind* the requested block stays trusted
/// before we re-query `/height`. Heights only move forward, so a cached height that is
/// already past the requested block never needs refreshing.
const HEIGHT_CACHE_TTL: Duration = Duration::from_secs(2);

pub struct HypersyncProvider {
    client: Client,
    /// Fallback provider used for everything HyperSync cannot serve.
    rpc: Arc<JsonRpcCachedProvider>,
    max_block_range: Option<U64>,
    height_cache: Mutex<Option<(Instant, u64)>>,
}

impl Debug for HypersyncProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HypersyncProvider")
            .field("url", &self.client.url().as_str())
            .field("chain", &self.rpc.chain)
            .field("max_block_range", &self.max_block_range)
            .finish()
    }
}

/// Resolve the HyperSync API token from the manifest or well-known environment variables.
fn resolve_api_token(config: &HypersyncConfig) -> Option<String> {
    config
        .api_token
        .clone()
        .filter(|token| !token.trim().is_empty())
        .or_else(|| std::env::var("HYPERSYNC_API_TOKEN").ok())
        .or_else(|| std::env::var("ENVIO_API_TOKEN").ok())
        .filter(|token| !token.trim().is_empty())
}

pub async fn create_hypersync_provider(
    config: &HypersyncConfig,
    network_name: &str,
    chain_id: u64,
    network_max_block_range: Option<U64>,
    rpc: Arc<JsonRpcCachedProvider>,
) -> Result<Arc<HypersyncProvider>, RetryClientError> {
    let url = config.url.clone().unwrap_or_else(|| format!("https://{chain_id}.hypersync.xyz"));

    let api_token = resolve_api_token(config).ok_or_else(|| {
        RetryClientError::HypersyncClientCantBeCreated(
            network_name.to_string(),
            "no API token found. Set `hypersync.api_token` in the manifest or the \
             HYPERSYNC_API_TOKEN / ENVIO_API_TOKEN environment variable (create one at \
             https://envio.dev/app/api-tokens)"
                .to_string(),
        )
    })?;

    let client = Client::builder().url(&url).api_token(api_token).build().map_err(|e| {
        RetryClientError::HypersyncClientCantBeCreated(network_name.to_string(), format!("{e:#}"))
    })?;

    // Guards against pointing a network at the wrong HyperSync endpoint.
    let hypersync_chain_id = client.get_chain_id().await.map_err(|e| {
        RetryClientError::HypersyncClientCantBeCreated(
            network_name.to_string(),
            format!("could not reach {url}: {e:#}"),
        )
    })?;

    if hypersync_chain_id != chain_id {
        return Err(RetryClientError::InvalidClientChainId(url, chain_id, hypersync_chain_id));
    }

    let max_block_range = config
        .max_block_range
        .or(network_max_block_range)
        .or(Some(U64::from(DEFAULT_HYPERSYNC_MAX_BLOCK_RANGE)));

    debug!(
        "HyperSync enabled for network {} via {} (max_block_range: {:?})",
        network_name, url, max_block_range
    );

    Ok(Arc::new(HypersyncProvider { client, rpc, max_block_range, height_cache: Mutex::new(None) }))
}

impl HypersyncProvider {
    /// Whether the HyperSync archive has fully ingested `to_block`.
    ///
    /// Uses a cached height: heights only move forward, so a cached height at or past
    /// `to_block` is always trusted; otherwise it is refreshed at most every
    /// [`HEIGHT_CACHE_TTL`]. Returns `false` on error so callers fall back to RPC.
    async fn covers_block(&self, to_block: u64) -> bool {
        let mut cache = self.height_cache.lock().await;

        if let Some((fetched_at, height)) = *cache {
            if height >= to_block {
                return true;
            }
            if fetched_at.elapsed() < HEIGHT_CACHE_TTL {
                return false;
            }
        }

        match self.client.get_height().await {
            Ok(height) => {
                *cache = Some((Instant::now(), height));
                height >= to_block
            }
            Err(e) => {
                warn!("HyperSync height check failed, falling back to RPC: {e:#}");
                false
            }
        }
    }

    async fn get_logs_via_hypersync(
        &self,
        event_filter: &RindexerEventFilter,
        addresses: Option<Vec<Address>>,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<Log>, ProviderError> {
        let map_err = |e: anyhow::Error| ProviderError::CustomError(format!("hypersync: {e:#}"));

        let mut log_filter =
            LogFilter::all().and_topic0([event_filter.event_signature().0]).map_err(map_err)?;

        if let Some(addresses) = addresses {
            log_filter =
                log_filter.and_address(addresses.into_iter().map(|a| a.0 .0)).map_err(map_err)?;
        }

        for (idx, topic) in [event_filter.topic1(), event_filter.topic2(), event_filter.topic3()]
            .into_iter()
            .enumerate()
        {
            let values: Vec<[u8; 32]> = topic.iter().map(|t| t.0).collect();
            if !values.is_empty() {
                log_filter = match idx {
                    0 => log_filter.and_topic1(values),
                    1 => log_filter.and_topic2(values),
                    _ => log_filter.and_topic3(values),
                }
                .map_err(map_err)?;
            }
        }

        // Joined block numbers + timestamps let us stamp `block_timestamp` on each log,
        // which lets the block clock skip its `eth_getBlockByNumber` batches entirely.
        let query = Query::new()
            .from_block(from_block)
            .to_block_excl(to_block + 1)
            .where_logs(log_filter)
            .select_log_fields([
                LogField::Removed,
                LogField::LogIndex,
                LogField::TransactionIndex,
                LogField::TransactionHash,
                LogField::BlockHash,
                LogField::BlockNumber,
                LogField::Address,
                LogField::Data,
                LogField::Topic0,
                LogField::Topic1,
                LogField::Topic2,
                LogField::Topic3,
            ])
            .select_block_fields([BlockField::Number, BlockField::Timestamp]);

        // `collect` paginates internally until the full requested range is covered, so a
        // successful return always means complete coverage of [from_block, to_block].
        let response =
            self.client.collect(query, StreamConfig::default()).await.map_err(map_err)?;

        let block_timestamps: HashMap<u64, u64> = response
            .data
            .blocks
            .iter()
            .flatten()
            .filter_map(|block| {
                let number = block.number?;
                let timestamp = block.timestamp.as_ref()?;
                Some((number, U256::from_be_slice(timestamp.as_ref()).to::<u64>()))
            })
            .collect();

        let mut logs = response
            .data
            .logs
            .into_iter()
            .flatten()
            .map(|log| {
                let address = log
                    .address
                    .as_ref()
                    .map(|a| Address::from(FixedBytes::<20>::from(a)))
                    .unwrap_or_default();
                let topics: Vec<B256> = log.topics.iter().flatten().map(B256::from).collect();
                let data = log
                    .data
                    .as_ref()
                    .map(|d| Bytes::copy_from_slice(d.as_ref()))
                    .unwrap_or_default();
                let block_number = log.block_number.map(u64::from);

                Log {
                    inner: alloy::primitives::Log {
                        address,
                        data: alloy::primitives::LogData::new_unchecked(topics, data),
                    },
                    block_hash: log.block_hash.as_ref().map(B256::from),
                    block_number,
                    block_timestamp: block_number
                        .and_then(|number| block_timestamps.get(&number).copied()),
                    transaction_hash: log.transaction_hash.as_ref().map(B256::from),
                    transaction_index: log.transaction_index.map(u64::from),
                    log_index: log.log_index.map(u64::from),
                    removed: log.removed.unwrap_or(false),
                }
            })
            .collect::<Vec<_>>();

        // rindexer tracks sync progress off the last log's block number and handlers
        // assume chain order, so guarantee (block_number, log_index) ordering.
        logs.sort_by_key(|log| (log.block_number, log.log_index));

        Ok(logs)
    }
}

#[async_trait]
impl ChainProvider for HypersyncProvider {
    fn chain(&self) -> Chain {
        self.rpc.chain
    }

    fn max_block_range(&self) -> Option<U64> {
        self.max_block_range
    }

    fn chain_state_notification(&self) -> Option<Sender<ChainStateNotification>> {
        self.rpc.get_chain_state_notification()
    }

    async fn get_latest_block(&self) -> Result<Option<Arc<AnyRpcBlock>>, ProviderError> {
        self.rpc.get_latest_block().await
    }

    async fn get_block_number(&self) -> Result<U64, ProviderError> {
        self.rpc.get_block_number().await
    }

    async fn get_logs(
        &self,
        event_filter: &RindexerEventFilter,
    ) -> Result<Vec<Log>, ProviderError> {
        let from_block = event_filter.from_block().to::<u64>();
        let to_block = event_filter.to_block().to::<u64>();

        if from_block > to_block {
            return Ok(vec![]);
        }

        let addresses = event_filter.contract_addresses().await;

        // Same semantics as the RPC provider: an explicitly empty address set (e.g. a
        // factory with no known children yet) means there is nothing to fetch.
        let addresses = match addresses {
            Some(addresses) if addresses.is_empty() => return Ok(vec![]),
            Some(addresses) => Some(addresses.into_iter().collect::<Vec<_>>()),
            None => None,
        };

        // The archive lags the chain head by a few blocks, so near-tip requests (live
        // indexing) go to the RPC node, which is authoritative for the head.
        if !self.covers_block(to_block).await {
            return self.rpc.get_logs(event_filter).await;
        }

        let start = Instant::now();
        let result =
            self.get_logs_via_hypersync(event_filter, addresses, from_block, to_block).await;

        rpc_metrics::record_rpc_request(
            &self.rpc.chain.to_string(),
            "hypersync_getLogs",
            result.is_ok(),
            start.elapsed().as_secs_f64(),
        );

        result
    }

    async fn get_block_by_number_batch(
        &self,
        block_numbers: &[U64],
        include_txs: bool,
    ) -> Result<Vec<AnyRpcBlock>, ProviderError> {
        self.rpc.get_block_by_number_batch(block_numbers, include_txs).await
    }

    async fn get_block_by_number_batch_with_size(
        &self,
        block_numbers: &[U64],
        include_txs: bool,
        rpc_batch_size: Option<usize>,
    ) -> Result<Vec<AnyRpcBlock>, ProviderError> {
        self.rpc
            .get_block_by_number_batch_with_size(block_numbers, include_txs, rpc_batch_size)
            .await
    }

    async fn get_tx_receipts_batch(
        &self,
        hashes: &[TxHash],
    ) -> Result<Vec<AnyTransactionReceipt>, ProviderError> {
        self.rpc.get_tx_receipts_batch(hashes).await
    }

    async fn trace_block(
        &self,
        block_number: U64,
    ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
        self.rpc.trace_block(block_number).await
    }

    async fn debug_trace_block_by_number(
        &self,
        block_number: U64,
    ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
        self.rpc.debug_trace_block_by_number(block_number).await
    }

    async fn eth_call(
        &self,
        to: Address,
        data: Bytes,
        block_number: u64,
    ) -> Result<String, ProviderError> {
        self.rpc.eth_call(to, data, block_number).await
    }

    async fn eth_call_latest(&self, to: Address, data: Bytes) -> Result<String, ProviderError> {
        self.rpc.eth_call_latest(to, data).await
    }
}
