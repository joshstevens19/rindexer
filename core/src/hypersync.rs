//! Envio HyperSync backed implementation of [`ChainProvider`].
//!
//! HyperSync serves log queries orders of magnitude faster than `eth_getLogs`, but it is
//! not a JSON-RPC node: it cannot serve `eth_call`, receipts or traces, and its archive
//! height can lag slightly behind the chain head. [`HypersyncProvider`] therefore wraps
//! the network's [`JsonRpcCachedProvider`] and only routes historical log fetches to
//! HyperSync — every other request, and any log request past the archive height,
//! delegates to the RPC provider.
//!
//! With `hypersync.for: realtime` the provider also serves head ranges. It subscribes to
//! the endpoint's `/height/sse` stream, and a log request for a block the archive has
//! not ingested yet waits (bounded) for the archive to cover it — pushed over the stream
//! or seen by a poll running alongside it — before querying HyperSync. HyperSync validates every block against its `receiptsRoot` before serving
//! it, so a range HyperSync serves is complete for the block it served — something an
//! `eth_getLogs` response cannot attest. Ranges HyperSync cannot serve (stream
//! disconnected, wait timed out, query error) go to RPC exactly as without the flag, and
//! `rindexer_hypersync_head_fallback_total` counts them. RPC still supplies the tip
//! header, so parent-hash reorg detection and the bloom shortcut are unchanged.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use alloy::network::{AnyRpcBlock, AnyTransactionReceipt};
use alloy::primitives::{Address, Bytes, FixedBytes, TxHash, B256, U256, U64};
use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
use alloy::rpc::types::Log;
use alloy_chains::Chain;
use async_trait::async_trait;
use hypersync_client::arrow_reader::{BlockReader, LogReader, ReadError};
use hypersync_client::net_types::block::BlockField;
use hypersync_client::net_types::log::{LogField, LogFilter};
use hypersync_client::net_types::Query;
use hypersync_client::{Client, ClientConfig, HeightStreamEvent, StreamConfig};
use tokio::sync::broadcast::Sender;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::event::RindexerEventFilter;
use crate::manifest::network::{HypersyncConfig, HypersyncFor};
use crate::metrics::rpc as rpc_metrics;
use crate::notifications::ChainStateNotification;
use crate::provider::{ChainProvider, JsonRpcCachedProvider, ProviderError, RetryClientError};

/// Default maximum block range per HyperSync logs request. This bounds the *outer*
/// request rindexer issues, and is intentionally far wider than a single HyperSync
/// response: within it the client's stream decomposes the range into many
/// adaptively-sized concurrent requests (see [`StreamConfig`]). A bound is still needed
/// because each `get_logs` call buffers the whole range's logs in memory. Overridable
/// via `hypersync.max_block_range` or `max_block_range`.
const DEFAULT_HYPERSYNC_MAX_BLOCK_RANGE: u64 = 50_000;

/// How long a cached archive height that is *behind* the requested block stays trusted
/// before we re-query `/height`. Heights only move forward, so a cached height that is
/// already past the requested block never needs refreshing.
const HEIGHT_CACHE_TTL: Duration = Duration::from_secs(2);

/// Default internal request concurrency per logs request, overridable via
/// `hypersync.stream_concurrency`. Dense block ranges decompose into hundreds of
/// adaptively-sized requests, and measured sweeps put 20 ~10-20% faster than the
/// client default of 10, with diminishing returns beyond.
const DEFAULT_STREAM_CONCURRENCY: usize = 20;

/// How long a head-range log request waits for the archive to ingest its last block
/// before the range is served from RPC (`hypersync.for: realtime`). Ingest lag is
/// normally well under a second; this only bites when HyperSync has stalled.
const HEAD_WAIT: Duration = Duration::from_secs(5);

/// While a head request waits on the stream, `/height` is also polled at this cadence.
/// A stream that keeps its keep-alives flowing while its heights stop is the one failure
/// the client's staleness detector cannot see; the poll covers it (the same reason
/// HyperIndex polls alongside an unproven stream).
const HEAD_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Per-request timeout for that poll, with no client retries: a poll that hangs must not
/// outlive the wait it is helping.
const HEAD_POLL_TIMEOUT: Duration = Duration::from_secs(2);

/// Minimum interval between "HyperSync has not ingested block" warnings per network. A
/// stalled archive would otherwise warn on every head request of every event stream.
const HEAD_WAIT_WARN_INTERVAL: Duration = Duration::from_secs(30);

pub struct HypersyncProvider {
    client: Client,
    /// Fallback provider used for everything HyperSync cannot serve.
    rpc: Arc<JsonRpcCachedProvider>,
    max_block_range: Option<U64>,
    stream_config: StreamConfig,
    height_cache: Mutex<Option<(Instant, u64)>>,
    /// Head serving via `/height/sse`; only with `hypersync.for: realtime`.
    head: Option<Head>,
}

impl Debug for HypersyncProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HypersyncProvider")
            .field("url", &self.client.url().as_str())
            .field("chain", &self.rpc.chain)
            .field("max_block_range", &self.max_block_range)
            .field("head", &self.head.is_some())
            .finish()
    }
}

/// Head serving state for one network (`hypersync.for: realtime`).
struct Head {
    /// Started on the first head-range request rather than at provider creation, so a
    /// long backfill does not hold an idle stream open.
    feed: OnceLock<HeadFeed>,
    wait: Duration,
    last_wait_warning: std::sync::Mutex<Option<Instant>>,
}

/// What the `/height/sse` stream currently says about the archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadState {
    /// Stream opened but no height received yet.
    Connecting,
    /// Stream live; the archive has ingested up to this height.
    Connected(u64),
    /// Stream dropped; the client is reconnecting with backoff. Height checks fall back
    /// to polling `/height` and head requests go straight to RPC.
    Disconnected,
}

/// The `/height/sse` stream mirrored into a watch channel.
struct HeadFeed {
    state: watch::Receiver<HeadState>,
    /// Forwards stream events into `state`. Aborted on drop, which drops the client's
    /// event receiver; the client's own task then exits at its next send or read
    /// timeout (up to 15s), closing the connection.
    task: JoinHandle<()>,
}

impl Drop for HeadFeed {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Subscribes to the endpoint's `/height/sse` stream and mirrors it into a watch channel.
///
/// The client owns reconnection (exponential backoff, capped at 30s) and surfaces it as
/// `Reconnecting` events. Within one connection heights only move forward: the stream
/// re-emits the current head on every (re)connect and that re-emit must not wake
/// waiters. Across a reconnect the new height is taken as-is, since a load-balanced
/// fleet can route the new connection to an instance that is behind. On an archive
/// rollback the remembered height can briefly exceed what the archive holds; that is
/// safe because the query then errors on zero progress and the range goes to RPC.
fn spawn_head_feed(client: &Client, network: String) -> HeadFeed {
    let (tx, rx) = watch::channel(HeadState::Connecting);
    let mut events = client.stream_height();

    let task = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            match event {
                HeightStreamEvent::Connected => {
                    info!("HyperSync height stream connected for network {network}");
                }
                HeightStreamEvent::Height(height) => {
                    rpc_metrics::set_hypersync_archive_height(&network, height);
                    tx.send_if_modified(|state| match *state {
                        HeadState::Connected(known) if known >= height => false,
                        _ => {
                            *state = HeadState::Connected(height);
                            true
                        }
                    });
                }
                HeightStreamEvent::Reconnecting { delay, error_msg } => {
                    rpc_metrics::record_hypersync_stream_reconnect(&network);
                    warn!(
                        "HyperSync height stream for network {network} disconnected, reconnecting in {delay:?} (head requests use RPC until then): {error_msg}"
                    );
                    tx.send_if_modified(|state| {
                        let changed = *state != HeadState::Disconnected;
                        *state = HeadState::Disconnected;
                        changed
                    });
                }
            }
        }
    });

    HeadFeed { state: rx, task }
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

    // Identify rindexer in the standard User-Agent header, the same way HyperSync's
    // Python and Node bindings identify themselves — otherwise requests carry the
    // generic rust-client agent. No extra requests or data; it only names the client
    // already making them, which helps server-side operators support and debug.
    let client = Client::new_with_agent(
        ClientConfig { url: url.clone(), api_token, ..ClientConfig::default() },
        format!("rindexer/{}", env!("CARGO_PKG_VERSION")),
    )
    .map_err(|e| {
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

    info!(
        "HyperSync enabled for network {} via {} (max_block_range: {:?})",
        network_name, url, max_block_range
    );

    let head = if config.r#for == Some(HypersyncFor::Realtime) {
        info!("HyperSync serving the chain head for network {} ({}/height/sse)", network_name, url);
        Some(Head {
            feed: OnceLock::new(),
            wait: HEAD_WAIT,
            last_wait_warning: std::sync::Mutex::new(None),
        })
    } else {
        None
    };

    // Sizing knobs for the client's internal request stream. The concurrency and
    // response-target defaults matter most on dense ranges; `max_batch_size` matters on
    // sparse ranges, where unbounded density projection otherwise collapses the stream
    // to a single serial request covering the whole remaining range.
    let stream_config = StreamConfig {
        concurrency: config.stream_concurrency.unwrap_or(DEFAULT_STREAM_CONCURRENCY),
        batch_size: config.batch_size.unwrap_or(StreamConfig::default_batch_size()),
        max_batch_size: config.max_batch_size,
        response_bytes_target: config
            .response_bytes_target
            .unwrap_or(StreamConfig::default_response_bytes_target()),
        ..Default::default()
    };

    Ok(Arc::new(HypersyncProvider {
        client,
        rpc,
        max_block_range,
        stream_config,
        height_cache: Mutex::new(None),
        head,
    }))
}

impl HypersyncProvider {
    /// The head feed, started on first use.
    fn head_feed(&self) -> Option<&HeadFeed> {
        let head = self.head.as_ref()?;
        Some(head.feed.get_or_init(|| spawn_head_feed(&self.client, self.rpc.chain.to_string())))
    }

    /// Whether the HyperSync archive has fully ingested `to_block`.
    ///
    /// With a connected `/height/sse` feed the pushed height answers directly, with no
    /// request. Otherwise uses a cached height: heights only move forward, so a cached
    /// height at or past `to_block` is always trusted; otherwise it is refreshed at most
    /// every [`HEIGHT_CACHE_TTL`]. Returns `false` on error so callers fall back to RPC.
    async fn covers_block(&self, to_block: u64) -> bool {
        if let Some(feed) = self.head.as_ref().and_then(|head| head.feed.get()) {
            if let HeadState::Connected(height) = *feed.state.borrow() {
                return height >= to_block;
            }
        }

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

    /// Polls `/height` (no client retries) and records the answer in the poll cache.
    async fn poll_height(&self) -> Option<u64> {
        let height = self.client.health_check(Some(HEAD_POLL_TIMEOUT)).await.ok()?;
        *self.height_cache.lock().await = Some((Instant::now(), height));
        Some(height)
    }

    /// Whether HyperSync should serve a request ending at `to_block`.
    ///
    /// Without `hypersync.for: realtime` this is [`covers_block`](Self::covers_block).
    /// With it, a block the archive has not ingested yet is waited for while the height
    /// stream is live, up to [`HEAD_WAIT`], so head ranges are served from validated data
    /// rather than handed to RPC the moment they are requested. `/height` is polled
    /// alongside the stream during the wait, so a connected stream that has stopped
    /// delivering cannot hold a request past the archive actually having the block.
    /// Returns `false` (serve from RPC) immediately while the stream is disconnected,
    /// and on timeout.
    async fn wait_for_coverage(&self, to_block: u64) -> bool {
        if self.covers_block(to_block).await {
            return true;
        }
        let (Some(head), Some(feed)) = (self.head.as_ref(), self.head_feed()) else {
            return false;
        };
        let network = self.rpc.chain.to_string();

        let mut state = feed.state.clone();
        let on_stream = state.wait_for(|s| match s {
            HeadState::Connecting => false,
            HeadState::Connected(height) => *height >= to_block,
            HeadState::Disconnected => true,
        });
        let on_poll = async {
            loop {
                tokio::time::sleep(HEAD_POLL_INTERVAL).await;
                if self.poll_height().await.is_some_and(|h| h >= to_block) {
                    return;
                }
            }
        };

        let outcome = tokio::time::timeout(head.wait, async {
            tokio::select! {
                stream = on_stream => stream.map(|s| matches!(*s, HeadState::Connected(_))).unwrap_or(false),
                () = on_poll => true,
            }
        })
        .await;

        match outcome {
            Ok(true) => true,
            Ok(false) => {
                // Disconnected mid-wait, or the feed task is gone: don't sit out the
                // timeout on a stream nobody is writing to.
                rpc_metrics::record_hypersync_head_fallback(&network, "disconnected");
                false
            }
            Err(_) => {
                rpc_metrics::record_hypersync_head_fallback(&network, "timeout");
                let mut last = head.last_wait_warning.lock().unwrap_or_else(|e| e.into_inner());
                let warn_now = last.is_none_or(|t| t.elapsed() >= HEAD_WAIT_WARN_INTERVAL);
                if warn_now {
                    *last = Some(Instant::now());
                    warn!(
                        "HyperSync has not ingested block {to_block} for network {network} after {}ms, serving from RPC (further warnings suppressed for {}s)",
                        head.wait.as_millis(),
                        HEAD_WAIT_WARN_INTERVAL.as_secs()
                    );
                } else {
                    debug!(
                        "HyperSync has not ingested block {to_block} for network {network} after {}ms, serving from RPC",
                        head.wait.as_millis()
                    );
                }
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

        // `collect_arrow` paginates internally until the full requested range is covered,
        // so a successful return always means complete coverage of [from_block, to_block].
        // The arrow response is materialized straight into alloy types via the arrow row
        // readers, skipping the intermediate simple-types allocation pass.
        let response =
            self.client.collect_arrow(query, self.stream_config.clone()).await.map_err(map_err)?;

        let map_read =
            |e: ReadError| ProviderError::CustomError(format!("hypersync: arrow read: {e}"));

        let mut block_timestamps: HashMap<u64, u64> = HashMap::new();
        for batch in &response.data.blocks {
            for block in BlockReader::iter(batch) {
                let number = block.number().map_err(map_read)?;
                let timestamp = block.timestamp().map_err(map_read)?;
                block_timestamps
                    .insert(number, U256::from_be_slice(timestamp.as_ref()).to::<u64>());
            }
        }

        let total_logs = response.data.logs.iter().map(|batch| batch.num_rows()).sum();
        let mut logs: Vec<Log> = Vec::with_capacity(total_logs);

        for batch in &response.data.logs {
            for log in LogReader::iter(batch) {
                let address = log.address().map_err(map_read)?;
                let data = log.data().map_err(map_read)?;
                let block_number = u64::from(log.block_number().map_err(map_read)?);

                let mut topics: Vec<B256> = Vec::with_capacity(4);
                for topic in [log.topic0(), log.topic1(), log.topic2(), log.topic3()] {
                    match topic.map_err(map_read)? {
                        Some(topic) => topics.push(B256::from(&topic)),
                        None => break,
                    }
                }

                logs.push(Log {
                    inner: alloy::primitives::Log {
                        address: Address::from(FixedBytes::<20>::from(&address)),
                        data: alloy::primitives::LogData::new_unchecked(
                            topics,
                            Bytes::copy_from_slice(data.as_ref()),
                        ),
                    },
                    block_hash: Some(B256::from(&log.block_hash().map_err(map_read)?)),
                    block_number: Some(block_number),
                    block_timestamp: block_timestamps.get(&block_number).copied(),
                    transaction_hash: Some(B256::from(&log.transaction_hash().map_err(map_read)?)),
                    transaction_index: Some(u64::from(log.transaction_index().map_err(map_read)?)),
                    log_index: Some(u64::from(log.log_index().map_err(map_read)?)),
                    removed: log.removed().map_err(map_read)?.unwrap_or(false),
                });
            }
        }

        // rindexer tracks sync progress off the last log's block number and handlers
        // assume chain order, so guarantee (block_number, log_index) ordering.
        logs.sort_by_key(|log| (log.block_number, log.log_index));

        Ok(logs)
    }
}

#[async_trait]
impl ChainProvider for HypersyncProvider {
    fn chain(&self) -> Chain {
        // Cached on the wrapped provider at construction — not a network call.
        self.rpc.chain
    }

    fn max_block_range(&self) -> Option<U64> {
        self.max_block_range
    }

    fn chain_state_notification(&self) -> Option<Sender<ChainStateNotification>> {
        self.rpc.get_chain_state_notification()
    }

    // The tip header stays RPC-authoritative even with `for: realtime`: the HyperSync
    // archive height can lag the chain head, and the header's hash, parent hash and
    // bloom drive reorg detection and the bloom shortcut. The pushed height only decides
    // when HyperSync can serve a log range.

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

        // The archive lags the chain head by a few blocks. With `for: backfill`, near-tip
        // requests (live indexing) go to the RPC node; with `for: realtime` they wait for
        // HyperSync to ingest and validate the block first.
        if !self.wait_for_coverage(to_block).await {
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

        match result {
            Ok(logs) => Ok(logs),
            Err(e) => {
                // A HyperSync failure degrades to RPC rather than failing the fetch.
                // This also covers a load-balanced instance lagging the height we
                // checked: the client errors loudly on zero progress instead of
                // stalling, and the range is served by RPC. If the range is too wide
                // for the RPC node, its error feeds the fetch loop's usual adaptive
                // range-halving.
                warn!(
                    "HyperSync get_logs failed for blocks [{from_block}..{to_block}], falling back to RPC: {e:#}",
                );
                if self.head.is_some() {
                    rpc_metrics::record_hypersync_head_fallback(
                        &self.rpc.chain.to_string(),
                        "query_error",
                    );
                }
                self.rpc.get_logs(event_filter).await
            }
        }
    }

    // Blocks, receipts and traces could also be served from HyperSync, but faithfully
    // reconstructing `AnyRpcBlock`/`AnyTransactionReceipt` across chains is a large
    // conversion surface, and these paths are mostly cold here: logs already carry
    // joined timestamps, so the block clock's batch fetches are skipped entirely.
    // Serving them from HyperSync (e.g. for native transfers) is follow-up work.

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

#[cfg(test)]
mod tests {
    use alloy::transports::mock::Asserter;

    use super::*;

    /// A provider whose HyperSync endpoint refuses connections (nothing listens on the
    /// target port) and whose RPC side is a mock that serves pushed responses.
    fn provider_with_unreachable_hypersync() -> (HypersyncProvider, Asserter) {
        let (rpc, asserter) = JsonRpcCachedProvider::mock_with_asserter(1);
        let client = Client::builder()
            .url("http://127.0.0.1:9")
            .api_token("00000000-0000-0000-0000-000000000000")
            .max_num_retries(0)
            .build()
            .expect("building a client makes no network calls");

        let provider = HypersyncProvider {
            client,
            rpc,
            max_block_range: None,
            stream_config: StreamConfig::default(),
            height_cache: Mutex::new(None),
            head: None,
        };

        (provider, asserter)
    }

    /// Enables head serving with a feed whose state is driven by the returned sender,
    /// standing in for the `/height/sse` forwarding task. The wait is short so timeout
    /// paths are quick under `start_paused` time.
    fn with_head_feed(
        mut provider: HypersyncProvider,
    ) -> (HypersyncProvider, watch::Sender<HeadState>) {
        let (tx, rx) = watch::channel(HeadState::Connecting);
        let feed = HeadFeed { state: rx, task: tokio::spawn(async {}) };
        provider.head = Some(Head {
            feed: OnceLock::from(feed),
            wait: Duration::from_millis(200),
            last_wait_warning: std::sync::Mutex::new(None),
        });
        (provider, tx)
    }

    fn head_filter(block: u64) -> RindexerEventFilter {
        RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(block))
            .set_to_block(U64::from(block))
    }

    /// Captures the first HTTP request `create_hypersync_provider` sends (the chain-id
    /// check) and asserts it identifies rindexer in the User-Agent header. The provider
    /// creation itself is aborted once the request is observed — the client retries the
    /// deliberately-broken response with backoff, and none of that is under test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hypersync_requests_identify_rindexer_in_user_agent() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind capture listener");
        let addr = listener.local_addr().expect("listener addr");

        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let read = stream.read(&mut buf).unwrap_or(0);
                let _ = stream
                    .write_all(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n");
                let _ = tx.send(String::from_utf8_lossy(&buf[..read]).to_string());
            }
        });

        let (rpc, _asserter) = JsonRpcCachedProvider::mock_with_asserter(1);
        let config = HypersyncConfig {
            url: Some(format!("http://{addr}")),
            api_token: Some("00000000-0000-0000-0000-000000000000".to_string()),
            ..Default::default()
        };
        let create = tokio::spawn(async move {
            let _ = create_hypersync_provider(&config, "test", 1, None, rpc).await;
        });

        let request = tokio::task::spawn_blocking(move || {
            rx.recv_timeout(Duration::from_secs(10)).expect("no request captured")
        })
        .await
        .expect("capture task");
        create.abort();

        let expected = format!("user-agent: rindexer/{}", env!("CARGO_PKG_VERSION"));
        assert!(
            request.to_lowercase().contains(&expected),
            "expected `{expected}` in request:\n{request}"
        );
    }

    #[tokio::test]
    async fn get_logs_falls_back_to_rpc_when_height_check_fails() {
        let (provider, asserter) = provider_with_unreachable_hypersync();
        asserter.push_success(&Vec::<Log>::new());

        let logs = provider.get_logs(&RindexerEventFilter::empty_for_test()).await.unwrap();

        assert!(logs.is_empty(), "expected the mocked RPC response, got {logs:?}");
    }

    #[tokio::test]
    async fn get_logs_falls_back_to_rpc_when_hypersync_query_errors() {
        let (provider, asserter) = provider_with_unreachable_hypersync();
        // Seed the height cache so `covers_block` passes and the HyperSync query path
        // runs — and errors against the unreachable endpoint — instead of the request
        // being routed to RPC by the height gate.
        *provider.height_cache.lock().await = Some((Instant::now(), u64::MAX));
        asserter.push_success(&Vec::<Log>::new());

        let logs = provider.get_logs(&RindexerEventFilter::empty_for_test()).await.unwrap();

        assert!(logs.is_empty(), "expected the mocked RPC response, got {logs:?}");
    }

    #[tokio::test]
    async fn covers_block_trusts_cached_height_at_or_past_target() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        *provider.height_cache.lock().await = Some((Instant::now(), 100));

        // At or below the cached height: trusted without a network call.
        assert!(provider.covers_block(100).await);
        // Past the cached height within the TTL: not covered, no refresh attempted.
        assert!(!provider.covers_block(101).await);
    }

    #[tokio::test(start_paused = true)]
    async fn without_head_uncovered_requests_go_straight_to_rpc() {
        let (provider, asserter) = provider_with_unreachable_hypersync();
        *provider.height_cache.lock().await = Some((Instant::now(), 100));
        asserter.push_success(&Vec::<Log>::new());

        let started = tokio::time::Instant::now();
        provider.get_logs(&head_filter(101)).await.unwrap();

        assert_eq!(started.elapsed(), Duration::ZERO, "no wait with for: backfill");
    }

    #[tokio::test(start_paused = true)]
    async fn head_request_waits_for_the_feed_to_cover_the_block() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        head.send_replace(HeadState::Connected(100));

        let pusher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            head.send_replace(HeadState::Connected(101));
            head
        });

        let started = tokio::time::Instant::now();
        let covered = provider.wait_for_coverage(101).await;
        let _head = pusher.await.unwrap();

        assert!(covered, "the pushed height should have covered the block");
        assert_eq!(started.elapsed(), Duration::from_millis(50), "should return at the push");
    }

    #[tokio::test(start_paused = true)]
    async fn head_request_falls_back_to_rpc_when_the_feed_never_covers_the_block() {
        let (provider, asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        head.send_replace(HeadState::Connected(100));
        asserter.push_success(&Vec::<Log>::new());

        let started = tokio::time::Instant::now();
        let logs = provider.get_logs(&head_filter(101)).await.unwrap();

        assert!(logs.is_empty(), "expected the mocked RPC response, got {logs:?}");
        assert_eq!(started.elapsed(), Duration::from_millis(200), "should wait out the head wait");
    }

    #[tokio::test(start_paused = true)]
    async fn head_request_goes_straight_to_rpc_while_the_stream_is_disconnected() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        head.send_replace(HeadState::Disconnected);
        // A stale polled height must not be mistaken for coverage either.
        *provider.height_cache.lock().await = Some((Instant::now(), 100));

        let started = tokio::time::Instant::now();
        assert!(!provider.wait_for_coverage(101).await);
        assert_eq!(
            started.elapsed(),
            Duration::ZERO,
            "must not sit out the timeout while disconnected"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn head_request_stops_waiting_when_the_stream_disconnects() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        head.send_replace(HeadState::Connected(100));

        let dropper = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            head.send_replace(HeadState::Disconnected);
            head
        });

        let started = tokio::time::Instant::now();
        assert!(!provider.wait_for_coverage(101).await);
        let _head = dropper.await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_millis(30), "should return at the disconnect");
    }

    #[tokio::test(start_paused = true)]
    async fn head_request_waits_through_connecting_state() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        // Freshly started feed: no height yet. The first push covers the block.
        let pusher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            head.send_replace(HeadState::Connected(500));
            head
        });

        assert!(provider.wait_for_coverage(101).await);
        let _head = pusher.await.unwrap();
    }

    /// A stream that stays `Connected` but stops delivering must not hold a request:
    /// the poll running alongside it sees the archive height and lets the request go.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_request_is_released_by_the_poll_when_the_stream_goes_quiet() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        // A `/height` endpoint that always answers 500.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind height listener");
        let addr = listener.local_addr().expect("listener addr");
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut stream = stream;
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let body = r#"{"height":500}"#;
                let _ = stream.write_all(
                    format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}", body.len())
                        .as_bytes(),
                );
            }
        });

        let (rpc, _asserter) = JsonRpcCachedProvider::mock_with_asserter(1);
        let client = Client::builder()
            .url(format!("http://{addr}"))
            .api_token("00000000-0000-0000-0000-000000000000")
            .max_num_retries(0)
            .build()
            .expect("building a client makes no network calls");
        let provider = HypersyncProvider {
            client,
            rpc,
            max_block_range: None,
            stream_config: StreamConfig::default(),
            height_cache: Mutex::new(None),
            head: None,
        };
        let (provider, head) = with_head_feed(provider);
        // Connected, but the stream never advances past 100.
        head.send_replace(HeadState::Connected(100));
        // Long enough that only the poll can end the wait before the timeout.
        let provider = HypersyncProvider {
            head: provider.head.map(|h| Head { wait: Duration::from_secs(5), ..h }),
            ..provider
        };

        let started = Instant::now();
        assert!(provider.wait_for_coverage(101).await, "the poll should have covered the block");
        assert!(started.elapsed() >= HEAD_POLL_INTERVAL, "released before the first poll");
        assert!(started.elapsed() < Duration::from_secs(5), "waited out the timeout instead");
    }

    #[tokio::test]
    async fn covers_block_answers_from_pushed_height_without_polling() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);
        head.send_replace(HeadState::Connected(100));

        assert!(provider.covers_block(100).await);
        assert!(!provider.covers_block(101).await);

        // The pushed height short-circuits the check: `/height` was never polled, so the
        // poll cache is still empty.
        assert!(provider.height_cache.lock().await.is_none());

        head.send_replace(HeadState::Connected(101));
        assert!(provider.covers_block(101).await);
    }

    #[tokio::test]
    async fn covers_block_polls_while_feed_is_disconnected() {
        let (provider, _asserter) = provider_with_unreachable_hypersync();
        let (provider, head) = with_head_feed(provider);

        // Feed not connected yet: the cached/polled path decides.
        *provider.height_cache.lock().await = Some((Instant::now(), 100));
        assert!(provider.covers_block(100).await);
        assert!(!provider.covers_block(101).await);

        // Once connected the push wins, even over a stale cache.
        head.send_replace(HeadState::Connected(101));
        assert!(provider.covers_block(101).await);

        // A reconnect clears the pushed height and the polled path takes over again.
        head.send_replace(HeadState::Disconnected);
        assert!(!provider.covers_block(101).await);
    }

    /// The feed is not opened at provider creation; the first head-range request opens
    /// it. Against an unreachable endpoint the client reports a reconnect straight away,
    /// so the request goes to RPC without waiting out the head wait.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_feed_starts_on_first_head_request() {
        let (mut provider, _asserter) = provider_with_unreachable_hypersync();
        provider.head = Some(Head {
            feed: OnceLock::new(),
            wait: Duration::from_secs(5),
            last_wait_warning: std::sync::Mutex::new(None),
        });
        assert!(provider.head.as_ref().unwrap().feed.get().is_none());

        let started = Instant::now();
        assert!(!provider.wait_for_coverage(1).await);

        assert!(provider.head.as_ref().unwrap().feed.get().is_some(), "feed should be started");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "disconnect must short-circuit the wait"
        );
    }

    /// Serves one `/height/sse` connection from a raw TCP listener and asserts the feed
    /// mirrors the pushed heights: the first frame, a keep-alive ping, then an increase.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_feed_follows_the_height_stream() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind sse listener");
        let addr = listener.local_addr().expect("listener addr");

        let (request_tx, request_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                let read = stream.read(&mut buf).unwrap_or(0);
                let _ = request_tx.send(String::from_utf8_lossy(&buf[..read]).to_string());
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
                );
                for frame in [
                    "event: height\ndata: 100\n\n",
                    "event: ping\ndata: \n\n",
                    "event: height\ndata: 101\n\n",
                ] {
                    let _ = stream.write_all(frame.as_bytes());
                    let _ = stream.flush();
                    std::thread::sleep(Duration::from_millis(20));
                }
                // Hold the connection open so the client does not reconnect (and re-emit
                // the head) before the assertions run. Not load-bearing after that; the
                // thread dies with the test process.
                std::thread::sleep(Duration::from_secs(5));
            }
        });

        let client = Client::builder()
            .url(format!("http://{addr}"))
            .api_token("00000000-0000-0000-0000-000000000000")
            .build()
            .expect("building a client makes no network calls");

        let feed = spawn_head_feed(&client, "1".to_string());
        let mut state = feed.state.clone();

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if *state.borrow_and_update() == HeadState::Connected(101) {
                    break;
                }
                state.changed().await.expect("feed task dropped the sender");
            }
        })
        .await
        .expect("feed never reached the pushed height");

        let request = request_rx.recv_timeout(Duration::from_secs(1)).expect("no request");
        assert!(request.starts_with("GET /height/sse"), "unexpected request:\n{request}");
        assert!(
            request
                .to_lowercase()
                .contains("authorization: bearer 00000000-0000-0000-0000-000000000000"),
            "expected the API token on the stream request:\n{request}"
        );
    }
}
