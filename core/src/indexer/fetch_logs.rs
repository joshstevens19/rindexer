use crate::adaptive_concurrency::{AdaptiveConcurrency, ADAPTIVE_CONCURRENCY};
use crate::blockclock::BlockClock;
use crate::database::clickhouse::client::ClickhouseClient;
use crate::event::callback_registry::{EventCallbackRegistry, TraceCallbackRegistry};
use crate::helpers::{halved_block_number, is_relevant_block};
use crate::indexer::heartbeat::{HeartbeatAction, HeartbeatTracker};
use crate::indexer::reorg::{
    detect_and_handle_reorg, reorg_safe_distance_for_chain, ReorgContext, ReorgCoordinator,
};
use crate::indexer::tip_logs::{filter_logs_for_stream, SharedTipLogs, TipLookup};
use crate::metrics::indexing as metrics;
use crate::PostgresClient;
use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{reorg::handle_chain_notification, IndexingEventProgressStatus},
    is_running,
    provider::{ChainProvider, ProviderError},
};
use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::Log,
};
use lru::LruCache;
use rand::{random_bool, random_ratio};
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{error::Error, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio::{sync::mpsc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Metadata for a processed block, used for reorg detection via parent hash chain validation.
#[allow(dead_code)]
pub struct BlockMeta {
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
}

pub struct ReorgInfo {
    /// First block number that diverged from the canonical chain.
    pub fork_block: U64,
    /// Number of blocks affected by the reorg.
    pub depth: u64,
    /// Transaction hashes from blocks that were reorged out.
    /// Populated when available (e.g. from removed logs); empty otherwise.
    pub affected_tx_hashes: Vec<B256>,
}

pub struct FetchLogsResult {
    pub logs: Vec<Log>,
    pub from_block: U64,
    pub to_block: U64,
    /// If set, a reorg was detected. Consumer should clean up storage before re-indexing.
    pub reorg: Option<ReorgInfo>,
}

pub fn fetch_logs_stream(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
    reorg_coordinator: Option<Arc<Mutex<ReorgCoordinator>>>,
    trace_registry: Option<Arc<TraceCallbackRegistry>>,
) -> impl tokio_stream::Stream<Item = Result<FetchLogsResult, Box<dyn Error + Send>>> + Send + Unpin
{
    // If the sink is slower than the producer it can lead to unbounded memory growth and
    // a system OOM kill.
    //
    // To prevent this, we maintain a memory bound to give the system time to catch up and
    // backpressure the producer. Many RPC responses are large, so this is important.
    //
    // This is per network contract-event, so it should be relatively small.
    let channel_size = config.config().buffer.unwrap_or(4);

    debug!("{} Configured with {} event buffer", config.info_log_name(), channel_size);

    let (tx, rx) = mpsc::channel(channel_size);

    tokio::spawn(async move {
        let mut current_filter = config.to_event_filter().unwrap();

        let snapshot_to_block = current_filter.to_block();
        let from_block = current_filter.from_block();

        // add any max block range limitation before we start processing
        let original_max_limit = config.network_contract().cached_provider.max_block_range();
        let mut max_block_range_limitation =
            config.network_contract().cached_provider.max_block_range();

        // Parallel historical backfill path. Activated when fetch_concurrency > 1
        // and the event is not a factory event (factory needs sequential discovery).
        let use_parallel = matches!(
            config.config().fetch_concurrency,
            Some(n) if n > 1 && !config.is_factory_event()
        );

        if use_parallel {
            let concurrency = config.config().fetch_concurrency.unwrap();
            // Use inclusive block count so a range [a, a] counts as 1 block.
            let total_blocks =
                snapshot_to_block.saturating_sub(from_block).to::<u64>().saturating_add(1);

            // Fallback to sequential for small ranges (not worth the overhead).
            if total_blocks >= PARALLEL_MIN_BLOCKS {
                let ParallelFetchParams { chunk_size, effective_concurrency } =
                    plan_parallel_fetch(total_blocks, concurrency);

                info!(
                    "{} - Parallel fetch: {} workers, chunk_size: {} blocks, total: {} blocks",
                    config.info_log_name(),
                    effective_concurrency,
                    chunk_size,
                    total_blocks
                );

                let (worker_tx, mut worker_rx) =
                    mpsc::channel::<SequencedFetchBatch>(effective_concurrency * 2);

                let active_workers = Arc::new(AtomicUsize::new(0));
                let worker_done_notify = Arc::new(tokio::sync::Notify::new());
                let cancel_token = config.cancel_token().clone();

                // worker_tx is MOVED into the dispatcher so the channel is kept
                // alive only by the worker clones once dispatching finishes.
                let dispatcher_filter = current_filter.clone();
                let dispatcher_config = Arc::clone(&config);
                let dispatcher_cancel = cancel_token.clone();
                // Shared: a 429 from any worker (any event) shrinks live
                // concurrency for all of them.
                let dispatcher_controller = Arc::clone(&ADAPTIVE_CONCURRENCY);
                let dispatcher_active = Arc::clone(&active_workers);
                let dispatcher_notify = Arc::clone(&worker_done_notify);
                let dispatcher_handle = tokio::spawn(async move {
                    let mut next_from = from_block;
                    let mut sequence_id: u64 = 0;

                    while next_from <= snapshot_to_block {
                        if !is_running() || dispatcher_cancel.is_cancelled() {
                            break;
                        }

                        // Register `notified()` BEFORE the load — otherwise a
                        // worker finishing between load and await would be a
                        // lost wakeup.
                        loop {
                            let notified = dispatcher_notify.notified();
                            let active = dispatcher_active.load(Ordering::Acquire);
                            let limit =
                                dispatcher_controller.current().clamp(1, effective_concurrency);
                            if active < limit {
                                break;
                            }
                            notified.await;
                        }

                        dispatcher_active.fetch_add(1, Ordering::Release);

                        let sub_to = U64::from(std::cmp::min(
                            next_from.to::<u64>().saturating_add(chunk_size - 1),
                            snapshot_to_block.to::<u64>(),
                        ));

                        let worker_filter = dispatcher_filter
                            .clone()
                            .set_from_block(next_from)
                            .set_to_block(sub_to);

                        let worker_state = WorkerState {
                            sequence_id,
                            filter: worker_filter,
                            max_block_range_limitation: original_max_limit,
                            original_max_limit,
                        };

                        let wtx = worker_tx.clone();
                        let cfg = Arc::clone(&dispatcher_config);
                        let ct = dispatcher_cancel.clone();
                        let ctrl = Arc::clone(&dispatcher_controller);
                        let aw = Arc::clone(&dispatcher_active);
                        let wdn = Arc::clone(&dispatcher_notify);

                        tokio::spawn(async move {
                            parallel_worker(cfg, worker_state, wtx, ct, ctrl, aw, wdn).await;
                        });

                        next_from = sub_to + U64::from(1);
                        sequence_id = sequence_id.saturating_add(1);
                    }
                    // Load-bearing: reorder task terminates only when every
                    // worker_tx clone is dropped.
                    drop(worker_tx);
                });

                // Reorder buffer: forwards worker results in strict sequence_id order.
                let reorder_tx = tx.clone();
                let reorder_handle = tokio::spawn(async move {
                    let mut buffer = ReorderBuffer::new();
                    while let Some(batch) = worker_rx.recv().await {
                        for r in buffer.accept(batch) {
                            if reorder_tx.send(r).await.is_err() {
                                return;
                            }
                        }
                    }
                });

                if let Err(e) = dispatcher_handle.await {
                    error!("{} - Dispatcher task failed: {:?}", config.info_log_name(), e);
                }
                if let Err(e) = reorder_handle.await {
                    error!("{} - Reorder task failed: {:?}", config.info_log_name(), e);
                }

                info!(
                    "{} - {} - Finished parallel indexing historic events",
                    config.info_log_name(),
                    IndexingEventProgressStatus::completed_log()
                );

                if config.live_indexing() && !force_no_live_indexing {
                    let registry = config.registry();
                    let live_from = snapshot_to_block + U64::from(1);
                    let live_filter =
                        current_filter.clone().set_from_block(live_from).set_to_block(live_from);

                    live_indexing_stream(
                        config.timestamps(),
                        config.network_contract().block_clock.clone(),
                        config.network_contract().cached_provider.clone(),
                        &tx,
                        snapshot_to_block,
                        &config.topic_id(),
                        &config.indexing_distance_from_head(),
                        live_filter,
                        &config.info_log_name(),
                        &config.network_contract().network,
                        config.network_contract().disable_logs_bloom_checks,
                        original_max_limit,
                        config.cancel_token().clone(),
                        reorg_coordinator,
                        config.postgres(),
                        config.clickhouse(),
                        &registry,
                        trace_registry.as_deref(),
                    )
                    .await;
                }

                return;
            } else {
                info!(
                    "{} - Range too small ({} blocks) for parallel fetching, using sequential",
                    config.info_log_name(),
                    total_blocks
                );
            }
        }

        #[allow(clippy::unnecessary_unwrap)]
        if max_block_range_limitation.is_some() {
            current_filter = current_filter.set_to_block(calculate_process_historic_log_to_block(
                &from_block,
                &snapshot_to_block,
                &max_block_range_limitation,
            ));
            if random_ratio(1, 20) {
                warn!(
                    "{} - {} - max block range of {} applied - indexing will be slower than providers supplying the optimal ranges - https://rindexer.xyz/docs/references/rpc-node-providers#rpc-node-providers",
                    config.info_log_name(),
                    IndexingEventProgressStatus::syncing_log(),
                    max_block_range_limitation.unwrap()
                );
            }
        }

        while current_filter.from_block() <= snapshot_to_block {
            if !is_running() || config.cancel_token().is_cancelled() {
                break;
            }

            let result = fetch_historic_logs_stream(
                config.timestamps(),
                config.network_contract().block_clock.clone(),
                &config.network_contract().cached_provider,
                &tx,
                &config.topic_id(),
                current_filter.clone(),
                max_block_range_limitation,
                snapshot_to_block,
                &config.info_log_name(),
            )
            .await;

            // This check can be very noisy. We want to only sample this warning to notify
            // the user, rather than warn on every log fetch.
            if let Some(range) = max_block_range_limitation {
                if range.to::<u64>() < 5000 && random_ratio(1, 20) {
                    warn!(
                        "{} - RPC PROVIDER IS SLOW - Slow indexing mode enabled, max block range limitation: {} blocks - we advise using a faster provider who can predict the next block ranges.",
                        &config.info_log_name(),
                        range
                    );
                }
            }

            if let Some(result) = result {
                // Useful for occasionally breaking out of temporary limitations or parsing errors
                // that lock down to a `1` block limitation. Returns back to the original
                let new_max_block_range_limitation = if random_bool(0.10) {
                    original_max_limit
                } else {
                    result.max_block_range_limitation
                };

                current_filter = result.next;
                max_block_range_limitation = new_max_block_range_limitation;
            } else {
                break;
            }
        }

        info!(
            "{} - {} - Finished indexing historic events",
            &config.info_log_name(),
            IndexingEventProgressStatus::completed_log()
        );

        // Live indexing mode
        if config.live_indexing() && !force_no_live_indexing {
            let registry = config.registry();
            live_indexing_stream(
                config.timestamps(),
                config.network_contract().block_clock.clone(),
                config.network_contract().cached_provider.clone(),
                &tx,
                snapshot_to_block,
                &config.topic_id(),
                &config.indexing_distance_from_head(),
                current_filter,
                &config.info_log_name(),
                &config.network_contract().network,
                config.network_contract().disable_logs_bloom_checks,
                original_max_limit,
                config.cancel_token().clone(),
                reorg_coordinator,
                config.postgres(),
                config.clickhouse(),
                &registry,
                trace_registry.as_deref(),
            )
            .await;
        }
    });

    ReceiverStream::new(rx)
}

struct ProcessHistoricLogsStreamResult {
    pub next: RindexerEventFilter,
    pub max_block_range_limitation: Option<U64>,
}

#[allow(clippy::too_many_arguments)]
async fn fetch_historic_logs_stream<P: ChainProvider>(
    timestamps: bool,
    block_clock: BlockClock,
    cached_provider: &P,
    tx: &mpsc::Sender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    topic_id: &B256,
    current_filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
) -> Option<ProcessHistoricLogsStreamResult> {
    let from_block = current_filter.from_block();
    let to_block = current_filter.to_block();

    debug!(
        "{} - {} - Process historic events - blocks: {} - {}",
        info_log_name,
        IndexingEventProgressStatus::syncing_log(),
        from_block,
        to_block
    );

    if from_block > to_block {
        warn!(
            "{} - {} - from_block {:?} > to_block {:?}",
            info_log_name,
            IndexingEventProgressStatus::syncing_log(),
            from_block,
            to_block
        );

        return Some(ProcessHistoricLogsStreamResult {
            next: current_filter.set_from_block(to_block).set_to_block(to_block + U64::from(1)),
            max_block_range_limitation,
        });
    }

    debug!(
        "{} - {} - Processing filter: {:?}",
        info_log_name,
        IndexingEventProgressStatus::syncing_log(),
        current_filter
    );

    let sender = tx.reserve().await.ok()?;

    if tx.capacity() == 0 {
        debug!(
            "{} - {} - Log channel full, waiting for events to be processed.",
            info_log_name,
            IndexingEventProgressStatus::syncing_log(),
        );
    }

    match cached_provider.get_logs(&current_filter).await {
        Ok(logs) => {
            debug!(
                "{} - {} - topic_id {}, Logs: {} from {} to {}",
                info_log_name,
                IndexingEventProgressStatus::syncing_log(),
                topic_id,
                logs.len(),
                from_block,
                to_block
            );

            let logs_empty = logs.is_empty();
            // clone here over the full logs way less overhead
            let last_log = logs.last().cloned();

            if !logs_empty {
                info!(
                    "{} - {} - Fetched {} logs between: {} - {}",
                    info_log_name,
                    IndexingEventProgressStatus::syncing_log(),
                    logs.len(),
                    from_block,
                    to_block
                );
            }

            if timestamps {
                if let Ok(logs) = block_clock.attach_log_timestamps(logs).await {
                    sender.send(Ok(FetchLogsResult { logs, from_block, to_block, reorg: None }));
                } else {
                    return Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .set_from_block(from_block)
                            .set_to_block(halved_block_number(to_block, from_block)),
                        max_block_range_limitation,
                    });
                }
            } else {
                sender.send(Ok(FetchLogsResult { logs, from_block, to_block, reorg: None }));
            }

            if logs_empty {
                let next_from_block = to_block + U64::from(1);
                let new_to_block = if next_from_block > snapshot_to_block {
                    // Termination sentinel: the outer loop's
                    // `while current_filter.from_block() <= snapshot_to_block`
                    // check exits because `next_from_block > snapshot_to_block`.
                    // We still need to return `Some` so the caller applies the
                    // advanced `from_block` — otherwise
                    // `live_indexing_stream` inherits the stale filter and
                    // re-fetches the last historical block, double-dispatching
                    // its events through the callback pipeline.
                    next_from_block
                } else {
                    calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    )
                };

                debug!(
                    "{} - No events between {} - {}. Advancing from_block to {}.",
                    info_log_name, from_block, to_block, next_from_block
                );

                return Some(ProcessHistoricLogsStreamResult {
                    next: current_filter.set_from_block(next_from_block).set_to_block(new_to_block),
                    max_block_range_limitation,
                });
            }

            if let Some(last_log) = last_log {
                let next_from_block = U64::from(
                    last_log.block_number.expect("block number should always be present in a log")
                        + 1,
                );
                debug!(
                    "{} - {} - next_block {:?}",
                    info_log_name,
                    IndexingEventProgressStatus::syncing_log(),
                    next_from_block
                );
                let new_to_block = if next_from_block > snapshot_to_block {
                    // See comment on the `logs_empty` branch above — we must
                    // return `Some` so the caller advances `from_block` past
                    // the last processed block before handing off to live
                    // indexing.
                    next_from_block
                } else {
                    calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    )
                };

                return Some(ProcessHistoricLogsStreamResult {
                    next: current_filter.set_from_block(next_from_block).set_to_block(new_to_block),
                    max_block_range_limitation,
                });
            }
        }
        Err(err) => {
            // This is fundamental to the rindexer flow. We intentionally fetch a large block range
            // to get information on what the ideal block range should be.
            if let Some(retry_result) = retry_with_block_range(
                info_log_name,
                &err,
                from_block,
                to_block,
                max_block_range_limitation,
            )
            .await
            {
                // Log if we "overshrink"
                if retry_result.to - retry_result.from < U64::from(1000) {
                    debug!(
                        "{} - {} - Over-fetched {} to {}. Shrunk ({}): {} to {}{}",
                        info_log_name,
                        IndexingEventProgressStatus::syncing_log(),
                        from_block,
                        to_block,
                        retry_result.to - retry_result.from,
                        retry_result.from,
                        retry_result.to,
                        retry_result
                            .max_block_range
                            .map(|m| format!(" (max {m})"))
                            .unwrap_or("".to_owned()),
                    );
                }

                return Some(ProcessHistoricLogsStreamResult {
                    next: current_filter
                        .set_from_block(U64::from(retry_result.from))
                        .set_to_block(U64::from(retry_result.to)),
                    max_block_range_limitation: retry_result.max_block_range,
                });
            }

            let halved_to_block = halved_block_number(to_block, from_block);

            // Handle deserialization, networking, and other non-rpc related errors.
            error!(
                "{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                info_log_name,
                IndexingEventProgressStatus::syncing_log(),
                from_block,
                to_block,
                from_block,
                halved_to_block,
                err
            );

            return Some(ProcessHistoricLogsStreamResult {
                next: current_filter.set_from_block(from_block).set_to_block(halved_to_block),
                max_block_range_limitation,
            });
        }
    }

    None
}

/// Cap per-worker results to prevent unbounded memory growth.
const MAX_WORKER_RESULTS: usize = 1000;

/// Minimum total blocks to enable the parallel path. Below this we fall back
/// to the sequential implementation — the overhead of workers/reorder buffer
/// is not worth it for small ranges.
const PARALLEL_MIN_BLOCKS: u64 = 1000;

/// Minimum per-worker chunk size. Ensures each worker has meaningful work to
/// do rather than thrashing on single blocks.
const PARALLEL_MIN_CHUNK: u64 = 1000;

/// Maximum fetch_concurrency regardless of user config. Guards against
/// accidentally spawning hundreds of workers and overloading the RPC.
const PARALLEL_MAX_CONCURRENCY: usize = 32;

#[derive(Debug, PartialEq, Eq)]
struct ParallelFetchParams {
    chunk_size: u64,
    effective_concurrency: usize,
}

fn plan_parallel_fetch(total_blocks: u64, concurrency: usize) -> ParallelFetchParams {
    let capped = concurrency.clamp(1, PARALLEL_MAX_CONCURRENCY);
    // Each worker gets at least PARALLEL_MIN_CHUNK blocks; above that we
    // divide the range evenly across the requested number of workers.
    let chunk_size = std::cmp::max(PARALLEL_MIN_CHUNK, total_blocks / capped as u64);
    // Never spawn more workers than there are MIN_CHUNK-sized pieces. For a
    // 2500-block range with concurrency=10 this yields 2 workers, not 10.
    let effective_concurrency =
        std::cmp::min(capped, std::cmp::max(1, (total_blocks / PARALLEL_MIN_CHUNK) as usize));
    ParallelFetchParams { chunk_size, effective_concurrency }
}

struct SequencedFetchBatch {
    sequence_id: u64,
    results: Vec<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    is_final: bool,
}

/// In-order delivery buffer for parallel-worker batches.
///
/// Workers may complete out of order but consumers need strict block-order.
/// This buffer holds back-of-queue batches until the in-order prefix is
/// known, then emits the next contiguous run. Partial batches (is_final=false)
/// are forwarded as soon as their sequence_id is current but do NOT advance
/// the cursor — the cursor only moves when the final batch for that id arrives.
///
/// Worst-case memory: if the worker for `next_expected` stalls indefinitely
/// (without panicking — WorkerDropGuard handles the panic case by forcibly
/// sending a final error batch), completed later workers pile up here with
/// one `PendingSlot` per sequence_id. In practice the pipeline's cancel_token
/// or the worker's own is_running()/cancel check bounds the stall; the
/// bound is NOT the channel capacity (the reorder task drains it eagerly).
struct ReorderBuffer {
    next_expected: u64,
    pending: BTreeMap<u64, PendingSlot>,
}

struct PendingSlot {
    batches: Vec<SequencedFetchBatch>,
    finalized: bool,
}

impl ReorderBuffer {
    fn new() -> Self {
        Self { next_expected: 0, pending: BTreeMap::new() }
    }

    #[cfg(test)]
    fn next_expected(&self) -> u64 {
        self.next_expected
    }

    #[cfg(test)]
    fn pending_sequence_ids(&self) -> Vec<u64> {
        self.pending.keys().copied().collect()
    }

    /// Feed a batch into the buffer. Returns the list of results that are now
    /// ready to be forwarded downstream, in strict block order.
    fn accept(
        &mut self,
        batch: SequencedFetchBatch,
    ) -> Vec<Result<FetchLogsResult, Box<dyn Error + Send>>> {
        let mut out = Vec::new();
        let sid = batch.sequence_id;
        let is_final = batch.is_final;

        if sid == self.next_expected {
            out.extend(batch.results);

            if is_final {
                self.next_expected = self.next_expected.saturating_add(1);
                while let Some(slot) = self.pending.remove(&self.next_expected) {
                    for b in slot.batches {
                        out.extend(b.results);
                    }
                    if slot.finalized {
                        self.next_expected = self.next_expected.saturating_add(1);
                    } else {
                        break;
                    }
                }
            }
        } else {
            let slot = self
                .pending
                .entry(sid)
                .or_insert_with(|| PendingSlot { batches: Vec::new(), finalized: false });
            if is_final {
                slot.finalized = true;
            }
            slot.batches.push(batch);
        }

        out
    }
}

struct WorkerState {
    sequence_id: u64,
    filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    original_max_limit: Option<U64>,
}

/// Drop guard ensuring the reorder buffer always receives a message for every
/// sequence_id, even if the worker panics. Without this, a panicked worker
/// would leave the reorder buffer waiting forever, deadlocking the pipeline.
///
/// On failure to send (channel full/closed), cancels the pipeline via
/// `cancel_token` to prevent silent data gaps.
struct WorkerDropGuard {
    sequence_id: u64,
    tx: mpsc::Sender<SequencedFetchBatch>,
    cancel_token: CancellationToken,
    active_workers: Arc<AtomicUsize>,
    worker_done_notify: Arc<tokio::sync::Notify>,
    sent: bool,
}

impl Drop for WorkerDropGuard {
    fn drop(&mut self) {
        if !self.sent {
            let error_batch = SequencedFetchBatch {
                sequence_id: self.sequence_id,
                results: vec![Err(Box::new(std::io::Error::other(
                    "worker panicked or was cancelled without sending results",
                )) as Box<dyn Error + Send>)],
                is_final: true,
            };
            if self.tx.try_send(error_batch).is_err() {
                error!(
                    "WorkerDropGuard: failed to send panic error for sequence {}. \
                     Cancelling pipeline to prevent data gaps.",
                    self.sequence_id
                );
                self.cancel_token.cancel();
            }
        }
        self.active_workers.fetch_sub(1, Ordering::Release);
        self.worker_done_notify.notify_one();
    }
}

async fn parallel_worker(
    config: Arc<EventProcessingConfig>,
    mut state: WorkerState,
    tx: mpsc::Sender<SequencedFetchBatch>,
    cancel_token: CancellationToken,
    controller: Arc<AdaptiveConcurrency>,
    active_workers: Arc<AtomicUsize>,
    worker_done_notify: Arc<tokio::sync::Notify>,
) {
    let mut guard = WorkerDropGuard {
        sequence_id: state.sequence_id,
        tx: tx.clone(),
        cancel_token: cancel_token.clone(),
        active_workers: Arc::clone(&active_workers),
        worker_done_notify: Arc::clone(&worker_done_notify),
        sent: false,
    };

    let sub_range_end = state.filter.to_block();
    let mut current_filter = state.filter.clone();
    let mut results: Vec<Result<FetchLogsResult, Box<dyn Error + Send>>> = Vec::new();

    // Hoist per-worker constants out of the fetch loop — each one costs
    // heap allocations or Arc refcount bumps on every iteration.
    let timestamps = config.timestamps();
    let block_clock = config.network_contract().block_clock.clone();
    let cached_provider = Arc::clone(&config.network_contract().cached_provider);
    let info_log_name = config.info_log_name();

    // Bail out if the same sub-range fails this many times in a row. Prevents
    // a stuck single/tiny range from holding the reorder buffer open forever
    // when the provider keeps returning an unclassified error that
    // `halved_block_number` can't shrink any further (minimum range is 2).
    const MAX_STUCK_ITERATIONS: usize = 10;
    let mut stuck_iterations: usize = 0;
    let mut last_range: Option<(U64, U64)> = None;

    while current_filter.from_block() <= sub_range_end {
        if !is_running() || cancel_token.is_cancelled() {
            break;
        }

        controller.wait_for_backoff().await;

        let (maybe_result, next_state, error_kind) = fetch_logs_once(
            timestamps,
            &block_clock,
            cached_provider.as_ref(),
            current_filter.clone(),
            state.max_block_range_limitation,
            sub_range_end,
            &info_log_name,
        )
        .await;

        match maybe_result {
            Some(fetch_result) => {
                controller.record_success();
                results.push(Ok(fetch_result));

                if results.len() >= MAX_WORKER_RESULTS
                    && tx
                        .send(SequencedFetchBatch {
                            sequence_id: state.sequence_id,
                            results: std::mem::take(&mut results),
                            is_final: false,
                        })
                        .await
                        .is_err()
                {
                    // Downstream closed — no point continuing.
                    break;
                }
            }
            None => match error_kind {
                Some(FetchErrorKind::RateLimit) => controller.record_rate_limit(),
                Some(FetchErrorKind::Other) => controller.record_error(),
                None => {}
            },
        }

        match next_state {
            Some(next) => {
                let new_range = (next.next.from_block(), next.next.to_block());
                if last_range == Some(new_range) && error_kind.is_some() {
                    stuck_iterations += 1;
                    if stuck_iterations >= MAX_STUCK_ITERATIONS {
                        error!(
                            "{} - worker for sid={} stuck on range {}-{} after {} retries; \
                             failing this sub-range so downstream can advance",
                            info_log_name,
                            state.sequence_id,
                            new_range.0,
                            new_range.1,
                            stuck_iterations
                        );
                        results.push(Err(Box::new(std::io::Error::other(format!(
                            "fetch_logs: stuck on range {}-{} after {} retries",
                            new_range.0, new_range.1, stuck_iterations
                        ))) as Box<dyn Error + Send>));
                        break;
                    }
                } else {
                    stuck_iterations = 0;
                    last_range = Some(new_range);
                }
                current_filter = next.next;
                state.max_block_range_limitation = if random_bool(0.10) {
                    state.original_max_limit
                } else {
                    next.max_block_range_limitation
                };
            }
            None => break,
        }
    }

    // Send final batch (may be empty, but must carry is_final=true)
    let _ = tx
        .send(SequencedFetchBatch { sequence_id: state.sequence_id, results, is_final: true })
        .await;
    guard.sent = true;
    // Counter release + notify happen in WorkerDropGuard::drop when `guard`
    // goes out of scope here, keeping the cleanup path single-sourced.
}

/// Classification of a recoverable fetch error. Rate-limit errors warrant
/// the aggressive -50% scale-down + backoff in `record_rate_limit`; other
/// errors only warrant the gentler -10% in `record_error`. Raw HTTP 429s are
/// intercepted at the RPC layer (`layer_extensions.rs`), but provider-specific
/// throttle phrasings ("too many requests", "quota exceeded", etc.) can reach
/// the worker as generic errors — this is the safety net for those.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchErrorKind {
    RateLimit,
    Other,
}

fn classify_fetch_error(err: &ProviderError) -> FetchErrorKind {
    let s = err.to_string().to_lowercase();
    if s.contains("429")
        || s.contains("rate limit")
        || s.contains("rate-limit")
        || s.contains("too many requests")
        || s.contains("quota")
        || s.contains("throttle")
    {
        FetchErrorKind::RateLimit
    } else {
        FetchErrorKind::Other
    }
}

/// Pure fetch: get_logs + retry logic. No channel interaction.
/// Returns a three-tuple: (result, next_state, error_kind). `error_kind` is
/// `Some` iff this call hit a recoverable error, so callers can feed it to
/// the adaptive concurrency controller.
#[allow(clippy::too_many_arguments)]
async fn fetch_logs_once<P: ChainProvider + ?Sized>(
    timestamps: bool,
    block_clock: &BlockClock,
    cached_provider: &P,
    current_filter: RindexerEventFilter,
    max_block_range_limitation: Option<U64>,
    snapshot_to_block: U64,
    info_log_name: &str,
) -> (Option<FetchLogsResult>, Option<ProcessHistoricLogsStreamResult>, Option<FetchErrorKind>) {
    let from_block = current_filter.from_block();
    let to_block = current_filter.to_block();

    debug!(
        "{} - {} - Process historic events - blocks: {} - {}",
        info_log_name,
        IndexingEventProgressStatus::syncing_log(),
        from_block,
        to_block
    );

    if from_block > to_block {
        warn!(
            "{} - {} - from_block {:?} > to_block {:?}",
            info_log_name,
            IndexingEventProgressStatus::syncing_log(),
            from_block,
            to_block
        );

        return (
            None,
            Some(ProcessHistoricLogsStreamResult {
                next: current_filter.set_from_block(to_block).set_to_block(to_block + U64::from(1)),
                max_block_range_limitation,
            }),
            None,
        );
    }

    match cached_provider.get_logs(&current_filter).await {
        Ok(logs) => {
            let logs_empty = logs.is_empty();
            let last_log = logs.last().cloned();

            if !logs_empty {
                info!(
                    "{} - {} - Fetched {} logs between: {} - {}",
                    info_log_name,
                    IndexingEventProgressStatus::syncing_log(),
                    logs.len(),
                    from_block,
                    to_block
                );
            }

            let result = if timestamps {
                if let Ok(logs) = block_clock.attach_log_timestamps(logs).await {
                    Some(FetchLogsResult { logs, from_block, to_block, reorg: None })
                } else {
                    return (
                        None,
                        Some(ProcessHistoricLogsStreamResult {
                            next: current_filter
                                .set_from_block(from_block)
                                .set_to_block(halved_block_number(to_block, from_block)),
                            max_block_range_limitation,
                        }),
                        Some(FetchErrorKind::Other),
                    );
                }
            } else {
                Some(FetchLogsResult { logs, from_block, to_block, reorg: None })
            };

            if logs_empty {
                let next_from_block = to_block + U64::from(1);
                return if next_from_block > snapshot_to_block {
                    (result, None, None)
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );
                    (
                        result,
                        Some(ProcessHistoricLogsStreamResult {
                            next: current_filter
                                .set_from_block(next_from_block)
                                .set_to_block(new_to_block),
                            max_block_range_limitation,
                        }),
                        None,
                    )
                };
            }

            if let Some(last_log) = last_log {
                let next_from_block = U64::from(
                    last_log.block_number.expect("block number should always be present in a log")
                        + 1,
                );
                return if next_from_block > snapshot_to_block {
                    (result, None, None)
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );
                    (
                        result,
                        Some(ProcessHistoricLogsStreamResult {
                            next: current_filter
                                .set_from_block(next_from_block)
                                .set_to_block(new_to_block),
                            max_block_range_limitation,
                        }),
                        None,
                    )
                };
            }
        }
        Err(err) => {
            let kind = classify_fetch_error(&err);

            if let Some(retry_result) = retry_with_block_range(
                info_log_name,
                &err,
                from_block,
                to_block,
                max_block_range_limitation,
            )
            .await
            {
                return (
                    None,
                    Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .set_from_block(U64::from(retry_result.from))
                            .set_to_block(U64::from(retry_result.to)),
                        max_block_range_limitation: retry_result.max_block_range,
                    }),
                    Some(kind),
                );
            }

            let halved_to_block = halved_block_number(to_block, from_block);
            error!(
                "{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                info_log_name,
                IndexingEventProgressStatus::syncing_log(),
                from_block,
                to_block,
                from_block,
                halved_to_block,
                err
            );

            return (
                None,
                Some(ProcessHistoricLogsStreamResult {
                    next: current_filter.set_from_block(from_block).set_to_block(halved_to_block),
                    max_block_range_limitation,
                }),
                Some(kind),
            );
        }
    }

    (None, None, None)
}

/// What the shared tip-block cache did for a live window that ends at the tip.
enum TipWindowFetch {
    /// The window's logs: filtered from the cache, or the stream's own `eth_getLogs` when the
    /// cache could not serve the window.
    Fetched(Result<Vec<Log>, ProviderError>),
    /// A block of the window is still being fetched; wake on the cache's `notified` future.
    Wait,
}

/// Serves the live window of `current_filter`, which ends at the tip the stream polled
/// (`tip_hash`), from the shared cache. `contract_address` is the snapshot this window would
/// have sent to `eth_getLogs`; the stream's own call is the fallback, counted by reason.
async fn fetch_tip_window(
    tip_logs: &SharedTipLogs,
    cached_provider: &dyn ChainProvider,
    current_filter: &RindexerEventFilter,
    contract_address: &Option<HashSet<Address>>,
    tip_hash: B256,
    info_log_name: &str,
) -> TipWindowFetch {
    let from_block = current_filter.from_block();
    let to_block = current_filter.to_block();
    let fetched: Result<Vec<Log>, ProviderError> =
        match tip_logs.lookup(from_block.to::<u64>(), to_block.to::<u64>(), tip_hash) {
            TipLookup::ServeTip(logs) => {
                Ok(filter_logs_for_stream(&logs, contract_address, current_filter))
            }
            TipLookup::ServeWindow(blocks) => Ok(blocks
                .iter()
                .flat_map(|logs| filter_logs_for_stream(logs, contract_address, current_filter))
                .collect()),
            TipLookup::ServePrefixByRpcPlusTip(tip) => {
                let prefix = if from_block < to_block {
                    let prefix_filter =
                        current_filter.clone().set_to_block(to_block - U64::from(1));
                    cached_provider.get_logs(&prefix_filter).await
                } else {
                    Ok(Vec::new())
                };
                prefix.map(|mut logs| {
                    logs.extend(filter_logs_for_stream(&tip, contract_address, current_filter));
                    logs.sort_by_key(|log| (log.block_number, log.log_index));
                    logs
                })
            }
            TipLookup::Wait { oldest_pending, first_seen } => {
                let waited = first_seen.elapsed();
                if waited < tip_logs.stream_wait_budget() {
                    return TipWindowFetch::Wait;
                }
                metrics::record_shared_tip_logs_fallback(tip_logs.network(), "wait_timeout");
                warn!(
                    "{} - {} - waited {} ms for the shared logs of block {}; fetching it directly",
                    info_log_name,
                    IndexingEventProgressStatus::live_log(),
                    waited.as_millis(),
                    oldest_pending
                );
                cached_provider.get_logs(current_filter).await
            }
            TipLookup::Fallback(reason) => {
                metrics::record_shared_tip_logs_fallback(tip_logs.network(), reason.as_label());
                cached_provider.get_logs(current_filter).await
            }
        };
    TipWindowFetch::Fetched(fetched)
}

/// Handles live indexing mode, continuously checking for new blocks, ensuring they are
/// within a safe range, updating the filter, and sending the logs to the provided channel.
#[allow(clippy::too_many_arguments)]
async fn live_indexing_stream(
    timestamps: bool,
    block_clock: BlockClock,
    cached_provider: Arc<dyn ChainProvider>,
    tx: &mpsc::Sender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    last_seen_block_number: U64,
    topic_id: &B256,
    reorg_safe_distance: &U64,
    mut current_filter: RindexerEventFilter,
    info_log_name: &str,
    network: &str,
    disable_logs_bloom_checks: bool,
    original_max_limit: Option<U64>,
    cancel_token: CancellationToken,
    reorg_coordinator: Option<Arc<Mutex<ReorgCoordinator>>>,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    registry: &EventCallbackRegistry,
    trace_registry: Option<&TraceCallbackRegistry>,
) {
    let mut last_seen_block_number = last_seen_block_number;
    let mut log_response_to_large_to_block: Option<U64> = None;
    let tip_logs = cached_provider.shared_tip_logs();
    let mut heartbeat = HeartbeatTracker::new(Duration::from_secs(300));
    let target_iteration_duration = Duration::from_millis(200);

    // Channel for reth-provided reorg signals (feature-gated, None for HTTP RPC).
    // The spawned task converts ChainStateNotification → ReorgInfo and sends here;
    // the main loop try_recv()s to trigger the same recovery codepath as cache-based detection.
    let (reth_reorg_tx, mut reth_reorg_rx) = mpsc::unbounded_channel::<ReorgInfo>();

    if let Some(notifications) = cached_provider.chain_state_notification() {
        let info_log_name = info_log_name.to_string();
        let network = network.to_string();
        tokio::spawn(async move {
            let mut rx = notifications.subscribe();
            while let Ok(notification) = rx.recv().await {
                if let Some(reorg_info) =
                    handle_chain_notification(notification, &info_log_name, &network)
                {
                    let _ = reth_reorg_tx.send(reorg_info);
                }
            }
        });
    }

    // Local cache of recent block metadata (hash, parent_hash, timestamp).
    // Used for: (1) cheap timestamp lookups for logs, (2) reorg detection via parent hash
    // chain validation. 1024 entries at ~100KB memory cost and would cover worst case scenariots
    // for rollups having long-mechanisms like Polygon 1 epoch.
    let mut block_cache: LruCache<u64, BlockMeta> = LruCache::new(NonZeroUsize::new(1024).unwrap());

    loop {
        let iteration_start = Instant::now();

        if !is_running() || cancel_token.is_cancelled() {
            break;
        }

        // Reth reorg signal — instant detection via ExEx notification.
        if let Ok(reth_reorg) = reth_reorg_rx.try_recv() {
            let fork_block = reth_reorg.fork_block.to::<u64>();
            warn!(
                "{} - REORG (reth notification): depth={}, fork_block={}",
                info_log_name, reth_reorg.depth, fork_block
            );

            // `fork_block` is the first reorged block and `depth` is the
            // inclusive count — last reorged block is `fork_block + depth - 1`.
            // An exclusive-end range covers exactly the reorged span and
            // degenerates to a no-op when depth == 0.
            for b in fork_block..(fork_block + reth_reorg.depth) {
                block_cache.pop(&b);
            }
            if let Some(tip_logs) = tip_logs.as_deref() {
                tip_logs.invalidate_from(fork_block);
            }

            // Route through coordinator for full recovery (event deletion, checkpoint
            // rewind, derived table rollback, window update) when available.
            if let Some(coordinator) = reorg_coordinator.as_ref() {
                let last_reverted = fork_block + reth_reorg.depth.saturating_sub(1);
                // Mutex held across reorg handling (DB rollback, stream
                // publishes in parallel, user on_reorg callback firing). On a
                // real reorg this blocks the other indexing path for the
                // duration of handle_reorg, which is acceptable for isolation.
                // If latency becomes a concern, move handle_reorg out of the
                // hot path.
                let mut guard = coordinator.lock().await;
                match guard.on_exex_reorg(fork_block, last_reverted) {
                    Ok(task) => {
                        let reorg_ctx = ReorgContext {
                            postgres: postgres.as_deref(),
                            clickhouse: clickhouse.as_ref(),
                            registry: Some(registry),
                            trace_registry,
                        };
                        if let Err(e) = guard.handle_reorg(task, &reorg_ctx).await {
                            error!("{} - Failed to handle ExEx reorg: {:?}", info_log_name, e);
                        }
                    }
                    Err(e) => {
                        error!("{} - Invalid ExEx reorg range: {:?}", info_log_name, e);
                    }
                }
            }

            let _ = tx
                .send(Ok(FetchLogsResult {
                    logs: vec![],
                    from_block: U64::from(fork_block),
                    to_block: U64::from(fork_block),
                    reorg: Some(reth_reorg),
                }))
                .await;

            current_filter = current_filter.set_from_block(U64::from(fork_block));
            last_seen_block_number = U64::from(fork_block.saturating_sub(1));
            continue;
        }

        let latest_block = cached_provider.get_latest_block().await;
        match latest_block {
            Ok(latest_block) => {
                if let Some(latest_block) = latest_block {
                    // Keep block cache for timestamp lookups
                    block_cache.put(
                        latest_block.header.number,
                        BlockMeta {
                            hash: latest_block.header.hash,
                            parent_hash: latest_block.header.parent_hash,
                            timestamp: latest_block.header.timestamp,
                        },
                    );

                    let latest_tip = U64::from(latest_block.header.number);
                    match heartbeat.tick(latest_tip) {
                        HeartbeatAction::Silent => {}
                        HeartbeatAction::Alive => {
                            info!(
                                "{} - {} - Indexing alive - chain tip {}, last processed block {}",
                                info_log_name,
                                IndexingEventProgressStatus::live_log(),
                                latest_tip,
                                last_seen_block_number
                            );
                        }
                        HeartbeatAction::Stalled => {
                            warn!(
                                "{} - {} - RPC tip has not advanced past block {} in the last 5 minutes",
                                info_log_name,
                                IndexingEventProgressStatus::live_log(),
                                latest_tip
                            );
                        }
                    }

                    // Reorg detection via coordinator (parent hash validation)
                    if let Some(coordinator) = reorg_coordinator.as_ref() {
                        let log_prefix = format!(
                            "{} - {}",
                            info_log_name,
                            IndexingEventProgressStatus::live_log()
                        );
                        let reorg_ctx = ReorgContext {
                            postgres: postgres.as_deref(),
                            clickhouse: clickhouse.as_ref(),
                            registry: Some(registry),
                            trace_registry,
                        };
                        // Mutex held across reorg handling (DB rollback,
                        // stream publishes in parallel, user on_reorg callback
                        // firing). On a real reorg this blocks the other
                        // indexing path for the duration of handle_reorg,
                        // which is acceptable for isolation. If latency
                        // becomes a concern, move handle_reorg out of the hot
                        // path.
                        let mut guard = coordinator.lock().await;
                        match detect_and_handle_reorg(
                            &mut guard,
                            latest_block.header.number,
                            latest_block.header.hash,
                            latest_block.header.parent_hash,
                            &log_prefix,
                            &reorg_ctx,
                        )
                        .await
                        {
                            Ok(Some(fork_point)) => {
                                if let Some(tip_logs) = tip_logs.as_deref() {
                                    tip_logs.invalidate_from(fork_point);
                                }
                                current_filter =
                                    current_filter.set_from_block(U64::from(fork_point));
                                last_seen_block_number = U64::from(fork_point.saturating_sub(1));
                                continue;
                            }
                            Ok(None) => {}
                            Err(e) => {
                                error!(
                                    "{} - Reorg handling failed, pausing before retry: {:?}",
                                    info_log_name, e
                                );
                                tokio::time::sleep(Duration::from_secs(2)).await;
                                continue;
                            }
                        }
                    }

                    // Every stream that can read the cache records the header it polled: the
                    // first to see a block starts its shared fetch, the others compare a hash.
                    // Networks without a coordinator observe heads too. A stream held behind
                    // the tip by `reorg_safe_distance` never reads the cache, so it never
                    // starts a fetch either.
                    if let Some(tip_logs) =
                        tip_logs.as_deref().filter(|_| reorg_safe_distance.is_zero())
                    {
                        tip_logs.observe_head(
                            latest_block.header.number,
                            latest_block.header.hash,
                            latest_block.header.parent_hash,
                            latest_block.header.logs_bloom,
                        );
                    }

                    let latest_block_number = log_response_to_large_to_block
                        .unwrap_or(U64::from(latest_block.header.number));

                    // A reduced retry ceiling can equal `last_seen_block_number` while the
                    // filter cursor is still behind it. Only declare the stream caught up when
                    // the next block to fetch is also beyond the effective ceiling.
                    if last_seen_block_number == latest_block_number
                        && current_filter.from_block() > latest_block_number
                    {
                        debug!(
                            "{} - {} - No new blocks to process...",
                            info_log_name,
                            IndexingEventProgressStatus::live_log()
                        );
                    } else {
                        debug!(
                            "{} - {} - New block seen {} - Last seen block {}",
                            info_log_name,
                            IndexingEventProgressStatus::live_log(),
                            latest_block_number,
                            last_seen_block_number
                        );

                        let safe_block_number =
                            latest_block_number.saturating_sub(*reorg_safe_distance);
                        let from_block = current_filter.from_block();
                        if from_block > safe_block_number {
                            if reorg_safe_distance.is_zero() {
                                let block_distance = from_block - latest_block_number;
                                let is_outside_reorg_range = block_distance
                                    > reorg_safe_distance_for_chain(cached_provider.chain().id());

                                // it should never get under normal conditions outside the reorg range,
                                // therefore, we log an error as means RCP state is not in sync with the blockchain
                                if is_outside_reorg_range {
                                    error!(
                                        "{} - {} - LIVE INDEXING STREAM - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                                        info_log_name,
                                        IndexingEventProgressStatus::live_log(),
                                        latest_block_number,
                                        from_block
                                    );
                                } else {
                                    info!(
                                        "{} - {} - LIVE INDEXING STREAM - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                                        info_log_name,
                                        IndexingEventProgressStatus::live_log(),
                                        latest_block_number,
                                        from_block
                                    );
                                }
                            } else {
                                debug!(
                                    "{} - {} - LIVE INDEXING STREAM - not in safe reorg block range yet block: {} > range: {}",
                                    info_log_name,
                                    IndexingEventProgressStatus::live_log(),
                                    from_block,
                                    safe_block_number
                                );
                            }
                        } else {
                            let contract_address = current_filter.contract_addresses().await;

                            let to_block = if let Some(max_block_range) = original_max_limit {
                                (from_block + max_block_range).min(safe_block_number)
                            } else {
                                safe_block_number
                            };
                            // The bloom-filter shortcut only applies when the
                            // single block we're about to fetch IS `latest_block`.
                            // With `reorg_safe_distance > 0` the processed block
                            // lags behind the tip, so using `latest_block`'s
                            // bloom is wrong — and if the tip happens to be
                            // empty it would falsely skip a block that actually
                            // has matching logs.
                            let bloom_check_applies = from_block
                                == U64::from(latest_block.header.number)
                                && from_block == to_block
                                && !disable_logs_bloom_checks;
                            if bloom_check_applies
                                && !is_relevant_block(&contract_address, topic_id, &latest_block)
                            {
                                debug!(
                                    "{} - {} - Skipping block {} as it's not relevant",
                                    info_log_name,
                                    IndexingEventProgressStatus::live_log(),
                                    from_block
                                );
                                debug!(
                                        "{} - {} - Did not need to hit RPC as no events in {} block - LogsBloom for block checked",
                                        info_log_name,
                                        IndexingEventProgressStatus::live_log(),
                                        from_block
                                    );
                                if let Err(e) = tx
                                    .send(Ok(FetchLogsResult {
                                        logs: Vec::new(),
                                        from_block,
                                        to_block,
                                        reorg: None,
                                    }))
                                    .await
                                {
                                    error!(
                                        "{} - {} - Failed to send logs to stream consumer! Err: {}",
                                        info_log_name,
                                        IndexingEventProgressStatus::live_log(),
                                        e
                                    );
                                    break;
                                }
                                current_filter =
                                    current_filter.set_from_block(to_block + U64::from(1));
                                last_seen_block_number = to_block;
                            } else {
                                current_filter = current_filter.set_to_block(to_block);

                                debug!(
                                    "{} - {} - Processing live filter: {:?}",
                                    info_log_name,
                                    IndexingEventProgressStatus::live_log(),
                                    current_filter
                                );

                                // A window that ends at the header this stream polled takes
                                // its logs from the shared cache; any other window (a reduced
                                // retry ceiling, a safe distance) keeps its own call.
                                let fetched = match tip_logs
                                    .as_deref()
                                    .filter(|_| to_block == U64::from(latest_block.header.number))
                                {
                                    Some(tip_logs) => {
                                        // Register, then look up, then await: no wake-up is lost.
                                        let notified = tip_logs.notified();
                                        tokio::pin!(notified);
                                        notified.as_mut().enable();
                                        match fetch_tip_window(
                                            tip_logs,
                                            cached_provider.as_ref(),
                                            &current_filter,
                                            &contract_address,
                                            latest_block.header.hash,
                                            info_log_name,
                                        )
                                        .await
                                        {
                                            TipWindowFetch::Fetched(fetched) => fetched,
                                            TipWindowFetch::Wait => {
                                                let pacing = target_iteration_duration
                                                    .saturating_sub(iteration_start.elapsed())
                                                    .max(Duration::from_millis(50));
                                                tokio::select! {
                                                    _ = &mut notified => {}
                                                    _ = tokio::time::sleep(pacing) => {}
                                                }
                                                continue;
                                            }
                                        }
                                    }
                                    None => cached_provider.get_logs(&current_filter).await,
                                };

                                match fetched {
                                    Ok(logs) => {
                                        debug!(
                                            "{} - {} - Live topic_id {}, Logs: {} from {} to {}",
                                            info_log_name,
                                            IndexingEventProgressStatus::live_log(),
                                            topic_id,
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        debug!(
                                            "{} - {} - Fetched {} event logs - blocks: {} - {}",
                                            info_log_name,
                                            IndexingEventProgressStatus::live_log(),
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        // Reorg detection: check for removed logs
                                        // (RPC provider signals reorged events via removed=true)
                                        if logs.iter().any(|log| log.removed) {
                                            let min_removed_block = logs
                                                .iter()
                                                .filter(|l| l.removed)
                                                .filter_map(|l| l.block_number)
                                                .min()
                                                .unwrap_or(from_block.to::<u64>());

                                            let depth = from_block
                                                .to::<u64>()
                                                .saturating_sub(min_removed_block);
                                            metrics::record_reorg(network, depth);
                                            warn!(
                                                "{} - REORG (removed logs): fork_block={}, depth={}",
                                                info_log_name, min_removed_block, depth
                                            );

                                            // Invalidate cache for affected blocks
                                            for b in min_removed_block..=to_block.to::<u64>() {
                                                block_cache.pop(&b);
                                            }
                                            if let Some(tip_logs) = tip_logs.as_deref() {
                                                tip_logs.invalidate_from(min_removed_block);
                                            }

                                            // Route through coordinator for full recovery when available
                                            // (event deletion, checkpoint rewind, window update).
                                            // Fall back to sending ReorgInfo through the stream when
                                            // the coordinator is not configured.
                                            if let Some(coordinator) = reorg_coordinator.as_ref() {
                                                // Mutex held across reorg handling (DB rollback,
                                                // stream publishes in parallel, user on_reorg
                                                // callback firing). On a real reorg this blocks
                                                // the other indexing path for the duration of
                                                // handle_reorg, which is acceptable for
                                                // isolation. If latency becomes a concern, move
                                                // handle_reorg out of the hot path.
                                                let mut guard = coordinator.lock().await;
                                                match guard.try_create_reorg_task_for_block_range(
                                                    min_removed_block,
                                                    to_block.to::<u64>(),
                                                ) {
                                                    Ok(task) => {
                                                        let reorg_ctx = ReorgContext {
                                                            postgres: postgres.as_deref(),
                                                            clickhouse: clickhouse.as_ref(),
                                                            registry: Some(registry),
                                                            trace_registry,
                                                        };
                                                        if let Err(e) = guard
                                                            .handle_reorg(task, &reorg_ctx)
                                                            .await
                                                        {
                                                            error!(
                                                                "{} - Failed to handle removed-logs reorg: {}",
                                                                info_log_name, e
                                                            );
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!(
                                                            "{} - Invalid removed-logs reorg range: {:?}",
                                                            info_log_name, e
                                                        );
                                                    }
                                                }
                                            } else {
                                                let _ = tx
                                                    .send(Ok(FetchLogsResult {
                                                        logs: vec![],
                                                        from_block: U64::from(min_removed_block),
                                                        to_block: U64::from(min_removed_block),
                                                        reorg: Some(ReorgInfo {
                                                            fork_block: U64::from(
                                                                min_removed_block,
                                                            ),
                                                            depth,
                                                            affected_tx_hashes: vec![],
                                                        }),
                                                    }))
                                                    .await;
                                            }

                                            current_filter = current_filter
                                                .set_from_block(U64::from(min_removed_block));
                                            last_seen_block_number =
                                                U64::from(min_removed_block.saturating_sub(1));
                                            // Drain any pending reth signals to avoid double recovery
                                            while reth_reorg_rx.try_recv().is_ok() {}
                                            continue;
                                        }

                                        last_seen_block_number = to_block;

                                        let logs_empty = logs.is_empty();
                                        let last_log = logs.last().cloned();

                                        // Attach timestamp from cached block metadata to the logs
                                        // to prevent any further fetches.
                                        let logs = logs
                                            .into_iter()
                                            .map(|mut log| {
                                                if let Some(n) = log.block_number {
                                                    if let Some(meta) = block_cache.get(&n) {
                                                        log.block_timestamp = Some(meta.timestamp);
                                                    }
                                                }
                                                log
                                            })
                                            .collect::<Vec<_>>();

                                        if tx.capacity() == 0 {
                                            warn!(
                                                "{} - {} - Log channel full, live indexer will wait for events to be processed.",
                                                info_log_name,
                                                IndexingEventProgressStatus::live_log(),
                                            );
                                        }

                                        let logs = if timestamps {
                                            if let Ok(logs_with_ts) =
                                                block_clock.attach_log_timestamps(logs).await
                                            {
                                                logs_with_ts
                                            } else {
                                                error!(
                                                    "Error getting blocktime, will try again in 1s"
                                                );
                                                tokio::time::sleep(Duration::from_secs(1)).await;
                                                continue;
                                            }
                                        } else {
                                            logs
                                        };

                                        if let Err(e) = tx
                                            .send(Ok(FetchLogsResult {
                                                logs,
                                                from_block,
                                                to_block,
                                                reorg: None,
                                            }))
                                            .await
                                        {
                                            error!(
                                                "{} - {} - Failed to send logs to stream consumer! Err: {}",
                                                info_log_name,
                                                IndexingEventProgressStatus::live_log(),
                                                e
                                            );
                                            break;
                                        }

                                        // Clear any remaining references to reduce memory pressure
                                        log_response_to_large_to_block = None;

                                        if logs_empty {
                                            current_filter = current_filter
                                                .set_from_block(to_block + U64::from(1));
                                            debug!(
                                                "{} - {} - No events found between blocks {} - {}",
                                                info_log_name,
                                                IndexingEventProgressStatus::live_log(),
                                                from_block,
                                                to_block,
                                            );
                                        } else if let Some(last_log) = last_log {
                                            if let Some(last_log_block_number) =
                                                last_log.block_number
                                            {
                                                current_filter = current_filter.set_from_block(
                                                    U64::from(last_log_block_number + 1),
                                                );
                                            } else {
                                                error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        if let Some(retry_result) = retry_with_block_range(
                                            info_log_name,
                                            &err,
                                            from_block,
                                            to_block,
                                            original_max_limit,
                                        )
                                        .await
                                        {
                                            debug!(
                                                    "{} - {} - Overfetched from {} to {} - shrinking to block range: from {} to {}",
                                                    info_log_name,
                                                    IndexingEventProgressStatus::live_log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    retry_result.to
                                                    );

                                            log_response_to_large_to_block = Some(retry_result.to);
                                        } else {
                                            let halved_to_block =
                                                halved_block_number(to_block, from_block);

                                            error!(
                                                    "{} - {} - Unexpected error fetching logs in range {} - {}. Retry fetching {} - {}: {:?}",
                                                    info_log_name,
                                                    IndexingEventProgressStatus::live_log(),
                                                    from_block,
                                                    to_block,
                                                    from_block,
                                                    halved_to_block,
                                                    err
                                                );

                                            log_response_to_large_to_block = Some(halved_to_block);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    info!("WARNING - empty latest block returned from provider, will try again in 200ms");
                }
            }
            Err(e) => {
                error!(
                    "Error getting latest block, will try again in 1 second - err: {}",
                    e.to_string()
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        }

        let elapsed = iteration_start.elapsed();
        if elapsed < target_iteration_duration {
            tokio::time::sleep(target_iteration_duration - elapsed).await;
        }
    }
}

#[derive(Debug)]
struct RetryWithBlockRangeResult {
    from: U64,
    to: U64,
    // This is only populated if you are using an RPC provider
    // who doesn't give block ranges, this tends to be providers
    // which are a lot slower than others, expect these providers
    // to be slow
    max_block_range: Option<U64>,
}

/// Attempts to retry with a new block range based on the error message.
async fn retry_with_block_range(
    info_log_name: &str,
    error: &ProviderError,
    from_block: U64,
    to_block: U64,
    max_block_range_limitation: Option<U64>,
) -> Option<RetryWithBlockRangeResult> {
    let error_struct = match error {
        ProviderError::RequestFailed(json_rpc_err) => json_rpc_err.as_error_resp(),
        _ => None,
    };

    let (error_message, error_data) = if let Some(error) = error_struct {
        let error_message = error.message.to_string();
        let error_data_binding = error.data.as_ref().map(|data| data.to_string());
        let empty_string = String::from("");
        let error_data = error_data_binding.unwrap_or(empty_string);
        let trimmed = error_message.chars().take(5000).collect::<String>();

        (trimmed.to_lowercase(), error_data.to_lowercase())
    } else {
        let str_err = error.to_string();
        let trimmed = str_err.chars().take(5000).collect::<String>();
        debug!("Failed to parse structured error, trying with raw string: {}", &str_err);
        (trimmed.to_lowercase(), "".to_string())
    };

    // Thanks Ponder for the regex patterns - https://github.com/ponder-sh/ponder/blob/889096a3ef5f54a0c5a06df82b0da9cf9a113996/packages/utils/src/getLogsRetryHelper.ts#L34
    // Alchemy
    if let Ok(re) =
        Regex::new(r"this block range should work: \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)]")
    {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                let start_block_str = start_block.as_str();
                let end_block_str = end_block.as_str();
                if let (Ok(from), Ok(to)) = (
                    u64::from_str_radix(start_block_str, 16),
                    u64::from_str_radix(end_block_str, 16),
                ) {
                    if from > to {
                        warn!(
                            "{} Alchemy returned a negative block range {} to {}. Inverting.",
                            info_log_name, from, to
                        );

                        // Negative range fixed by inverting.
                        let to = U64::from(from);

                        return Some(RetryWithBlockRangeResult {
                            from: from_block,
                            to,
                            max_block_range: max_block_range_limitation,
                        });
                    }

                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
                        max_block_range: max_block_range_limitation,
                    });
                } else {
                    info!(
                        "{} Failed to parse block numbers {} and {}",
                        info_log_name, start_block_str, end_block_str
                    );
                }
            }
        }
    }

    // Infura, Thirdweb, zkSync, Tenderly
    if let Ok(re) = Regex::new(r"try with this block range \[0x([0-9a-fA-F]+),\s*0x([0-9a-fA-F]+)]")
    {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let (Some(start_block), Some(end_block)) = (captures.get(1), captures.get(2)) {
                if let (Ok(from), Ok(to)) = (
                    u64::from_str_radix(start_block.as_str(), 16),
                    u64::from_str_radix(end_block.as_str(), 16),
                ) {
                    return Some(RetryWithBlockRangeResult {
                        from: U64::from(from),
                        to: U64::from(to),
                        max_block_range: max_block_range_limitation,
                    });
                }
            }
        }
    }

    // Ankr
    if error_message.contains("block range is too wide") {
        // Use the minimum of original config or 3000
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, U64::from(3000)))
            .unwrap_or(U64::from(3000));

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
        });
    }

    // QuickNode, 1RPC, zkEVM, Blast, BlockPI
    if let Ok(re) = Regex::new(r"limited to a ([\d,.]+)") {
        if let Some(captures) = re.captures(&error_message).or_else(|| re.captures(&error_data)) {
            if let Some(range_str_match) = captures.get(1) {
                let range_str = range_str_match.as_str().replace(&['.', ','][..], "");
                if let Ok(range) = U64::from_str(&range_str) {
                    // Use the minimum of original config or provider suggestion
                    let suggested_range = max_block_range_limitation
                        .map(|original| std::cmp::min(original, range))
                        .unwrap_or(range);

                    return Some(RetryWithBlockRangeResult {
                        from: from_block,
                        to: from_block + suggested_range,
                        max_block_range: Some(suggested_range),
                    });
                }
            }
        }
    }

    // Base
    if error_message.contains("block range too large") {
        // Use the minimum of original config or 2000
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, U64::from(2000)))
            .unwrap_or(U64::from(2000));

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
        });
    }

    // Transient response errors, likely solved by halving the range or just retrying
    if error_message.contains("response is too big")
        || error_message.contains("error decoding response body")
    {
        let halved_to_block = halved_block_number(to_block, from_block);
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: halved_to_block,
            max_block_range: max_block_range_limitation,
        });
    }

    // We can't keep up with our own sending rate. This is rare, but we must backoff throughput.
    if error_message.contains("error sending request") {
        tokio::time::sleep(Duration::from_secs(1)).await;
        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: halved_block_number(to_block, from_block),
            max_block_range: max_block_range_limitation,
        });
    }

    // Fallback range
    if to_block > from_block {
        let diff = to_block - from_block;

        let mut block_range = FallbackBlockRange::from_diff(diff);
        let mut next_to_block = from_block + block_range.value();

        warn!(
            "{} Computed a fallback block range {:?}. Provider did not provide information in error: {:?}",
            info_log_name, block_range, error_message
        );

        if next_to_block == to_block {
            block_range = block_range.lower();
            next_to_block = from_block + block_range.value();
        }

        if next_to_block < from_block {
            error!(
                "{} Computed a negative fallback block range. Overriding to single block fetch.",
                info_log_name
            );

            return Some(RetryWithBlockRangeResult {
                from: from_block,
                to: halved_block_number(to_block, from_block),
                max_block_range: max_block_range_limitation,
            });
        }

        // Use the minimum of original config or fallback range
        let fallback_range = U64::from(block_range.value());
        let suggested_range = max_block_range_limitation
            .map(|original| std::cmp::min(original, fallback_range))
            .unwrap_or(fallback_range);

        return Some(RetryWithBlockRangeResult {
            from: from_block,
            to: from_block + suggested_range,
            max_block_range: Some(suggested_range),
        });
    }

    None
}

#[derive(Debug, PartialEq)]
enum FallbackBlockRange {
    Range5000,
    Range500,
    Range75,
    Range50,
    Range45,
    Range40,
    Range35,
    Range30,
    Range25,
    Range20,
    Range15,
    Range10,
    Range5,
    Range1,
}

impl FallbackBlockRange {
    fn value(&self) -> U64 {
        match self {
            FallbackBlockRange::Range5000 => U64::from(5000),
            FallbackBlockRange::Range500 => U64::from(500),
            FallbackBlockRange::Range75 => U64::from(75),
            FallbackBlockRange::Range50 => U64::from(50),
            FallbackBlockRange::Range45 => U64::from(45),
            FallbackBlockRange::Range40 => U64::from(40),
            FallbackBlockRange::Range35 => U64::from(35),
            FallbackBlockRange::Range30 => U64::from(30),
            FallbackBlockRange::Range25 => U64::from(25),
            FallbackBlockRange::Range20 => U64::from(20),
            FallbackBlockRange::Range15 => U64::from(15),
            FallbackBlockRange::Range10 => U64::from(10),
            FallbackBlockRange::Range5 => U64::from(5),
            FallbackBlockRange::Range1 => U64::from(1),
        }
    }

    fn lower(&self) -> FallbackBlockRange {
        match self {
            FallbackBlockRange::Range5000 => FallbackBlockRange::Range500,
            FallbackBlockRange::Range500 => FallbackBlockRange::Range75,
            FallbackBlockRange::Range75 => FallbackBlockRange::Range50,
            FallbackBlockRange::Range50 => FallbackBlockRange::Range45,
            FallbackBlockRange::Range45 => FallbackBlockRange::Range40,
            FallbackBlockRange::Range40 => FallbackBlockRange::Range35,
            FallbackBlockRange::Range35 => FallbackBlockRange::Range30,
            FallbackBlockRange::Range30 => FallbackBlockRange::Range25,
            FallbackBlockRange::Range25 => FallbackBlockRange::Range20,
            FallbackBlockRange::Range20 => FallbackBlockRange::Range15,
            FallbackBlockRange::Range15 => FallbackBlockRange::Range10,
            FallbackBlockRange::Range10 => FallbackBlockRange::Range5,
            FallbackBlockRange::Range5 => FallbackBlockRange::Range1,
            FallbackBlockRange::Range1 => FallbackBlockRange::Range1,
        }
    }

    fn from_diff(diff: U64) -> FallbackBlockRange {
        let diff = diff.as_limbs()[0];
        if diff >= 5000 {
            FallbackBlockRange::Range5000
        } else if diff >= 500 {
            FallbackBlockRange::Range500
        } else if diff >= 75 {
            FallbackBlockRange::Range75
        } else if diff >= 50 {
            FallbackBlockRange::Range50
        } else if diff >= 45 {
            FallbackBlockRange::Range45
        } else if diff >= 40 {
            FallbackBlockRange::Range40
        } else if diff >= 35 {
            FallbackBlockRange::Range35
        } else if diff >= 30 {
            FallbackBlockRange::Range30
        } else if diff >= 25 {
            FallbackBlockRange::Range25
        } else if diff >= 20 {
            FallbackBlockRange::Range20
        } else if diff >= 15 {
            FallbackBlockRange::Range15
        } else if diff >= 10 {
            FallbackBlockRange::Range10
        } else if diff >= 5 {
            FallbackBlockRange::Range5
        } else {
            FallbackBlockRange::Range1
        }
    }
}

fn calculate_process_historic_log_to_block(
    new_from_block: &U64,
    snapshot_to_block: &U64,
    max_block_range_limitation: &Option<U64>,
) -> U64 {
    if let Some(max_block_range_limitation) = max_block_range_limitation {
        let to_block = new_from_block + max_block_range_limitation;
        if to_block > *snapshot_to_block {
            *snapshot_to_block
        } else {
            to_block
        }
    } else {
        *snapshot_to_block
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockclock::BlockClock;
    use crate::event::contract_setup::AddressDetails;
    use crate::event::RindexerEventFilter;
    use crate::indexer::tip_logs::{BlockLogsSource, FallbackReason, SharedTipLogsSettings};
    use crate::metrics::definitions::{
        SHARED_TIP_LOGS_BLOCKS_TOTAL, SHARED_TIP_LOGS_FALLBACKS_TOTAL, SHARED_TIP_LOGS_SERVED_TOTAL,
    };
    use crate::provider::mock::MockChainProvider;
    use crate::provider::ChainProvider;
    use alloy::network::{AnyHeader, AnyRpcBlock, AnyRpcHeader, AnyTransactionReceipt};
    use alloy::primitives::Log as PrimitiveLog;
    use alloy::primitives::{Address, Bloom, Bytes, LogData, TxHash};
    use alloy::rpc::types::trace::parity::LocalizedTransactionTrace;
    use alloy::rpc::types::Log;
    use alloy::rpc::types::{BlockTransactions, ValueOrArray};
    use prometheus::CounterVec;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Debug)]
    struct RecordingLiveProvider {
        inner: MockChainProvider,
        // Capture the exact `(from_block, to_block)` windows requested by the
        // live loop so the test can assert against the RPC range calculation
        // directly.
        queried_ranges: Arc<StdMutex<Vec<(u64, u64)>>>,
        cancel_token: CancellationToken,
        fail_first_logs_request: bool,
        cancel_after_requests: usize,
        request_count: AtomicUsize,
        // Head polls answered before `get_latest_block` fails and cancels; `None` never fails.
        head_polls_before_failure: Option<usize>,
        head_polls: AtomicUsize,
    }

    impl RecordingLiveProvider {
        fn new(
            inner: MockChainProvider,
            queried_ranges: Arc<StdMutex<Vec<(u64, u64)>>>,
            cancel_token: CancellationToken,
        ) -> Self {
            Self {
                inner,
                queried_ranges,
                cancel_token,
                fail_first_logs_request: false,
                cancel_after_requests: 1,
                request_count: AtomicUsize::new(0),
                head_polls_before_failure: None,
                head_polls: AtomicUsize::new(0),
            }
        }

        /// A provider over `inner` with its own range recorder and cancellation token.
        fn recording(inner: MockChainProvider) -> Self {
            Self::new(inner, Arc::new(StdMutex::new(Vec::new())), CancellationToken::new())
        }

        fn with_transient_first_logs_error(mut self) -> Self {
            self.fail_first_logs_request = true;
            self.cancel_after_requests = 2;
            self
        }

        fn with_head_polls(mut self, answered: usize) -> Self {
            self.head_polls_before_failure = Some(answered);
            self
        }

        fn ranges(&self) -> Vec<(u64, u64)> {
            self.queried_ranges.lock().expect("recorded ranges mutex poisoned").clone()
        }
    }

    #[async_trait::async_trait]
    impl ChainProvider for RecordingLiveProvider {
        fn chain(&self) -> alloy_chains::Chain {
            self.inner.chain()
        }

        fn max_block_range(&self) -> Option<U64> {
            self.inner.max_block_range()
        }

        fn chain_state_notification(
            &self,
        ) -> Option<tokio::sync::broadcast::Sender<crate::notifications::ChainStateNotification>>
        {
            self.inner.chain_state_notification()
        }

        fn shared_tip_logs(&self) -> Option<Arc<SharedTipLogs>> {
            self.inner.shared_tip_logs()
        }

        async fn get_latest_block(&self) -> Result<Option<Arc<AnyRpcBlock>>, ProviderError> {
            let polls = self.head_polls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.head_polls_before_failure.is_some_and(|answered| polls > answered) {
                self.cancel_token.cancel();
                return Err(ProviderError::CustomError("head unavailable".to_string()));
            }
            self.inner.get_latest_block().await
        }

        async fn get_block_number(&self) -> Result<U64, ProviderError> {
            self.inner.get_block_number().await
        }

        async fn get_logs(
            &self,
            event_filter: &RindexerEventFilter,
        ) -> Result<Vec<Log>, ProviderError> {
            self.queried_ranges
                .lock()
                .expect("recorded ranges mutex poisoned")
                .push((event_filter.from_block().to::<u64>(), event_filter.to_block().to::<u64>()));

            let request_count = self.request_count.fetch_add(1, Ordering::SeqCst) + 1;
            if request_count >= self.cancel_after_requests {
                self.cancel_token.cancel();
            }

            if self.fail_first_logs_request && request_count == 1 {
                return Err(ProviderError::CustomError(
                    "block not found for eth_getLogs, requested toBlock 603 is not yet available"
                        .to_string(),
                ));
            }

            self.inner.get_logs(event_filter).await
        }

        async fn get_block_by_number_batch(
            &self,
            block_numbers: &[U64],
            include_txs: bool,
        ) -> Result<Vec<AnyRpcBlock>, ProviderError> {
            self.inner.get_block_by_number_batch(block_numbers, include_txs).await
        }

        async fn get_block_by_number_batch_with_size(
            &self,
            block_numbers: &[U64],
            include_txs: bool,
            rpc_batch_size: Option<usize>,
        ) -> Result<Vec<AnyRpcBlock>, ProviderError> {
            self.inner
                .get_block_by_number_batch_with_size(block_numbers, include_txs, rpc_batch_size)
                .await
        }

        async fn get_tx_receipts_batch(
            &self,
            hashes: &[TxHash],
        ) -> Result<Vec<AnyTransactionReceipt>, ProviderError> {
            self.inner.get_tx_receipts_batch(hashes).await
        }

        async fn trace_block(
            &self,
            block_number: U64,
        ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
            self.inner.trace_block(block_number).await
        }

        async fn debug_trace_block_by_number(
            &self,
            block_number: U64,
        ) -> Result<Vec<LocalizedTransactionTrace>, ProviderError> {
            self.inner.debug_trace_block_by_number(block_number).await
        }

        async fn eth_call(
            &self,
            to: Address,
            data: Bytes,
            block_number: u64,
        ) -> Result<String, ProviderError> {
            self.inner.eth_call(to, data, block_number).await
        }

        async fn eth_call_latest(&self, to: Address, data: Bytes) -> Result<String, ProviderError> {
            self.inner.eth_call_latest(to, data).await
        }
    }

    #[test]
    fn to_block_no_limit() {
        let result =
            calculate_process_historic_log_to_block(&U64::from(100), &U64::from(5000), &None);
        assert_eq!(result, U64::from(5000));
    }

    #[test]
    fn to_block_with_limit_within_snapshot() {
        let result = calculate_process_historic_log_to_block(
            &U64::from(100),
            &U64::from(5000),
            &Some(U64::from(1000)),
        );
        assert_eq!(result, U64::from(1100));
    }

    #[test]
    fn to_block_with_limit_exceeds_snapshot() {
        let result = calculate_process_historic_log_to_block(
            &U64::from(4500),
            &U64::from(5000),
            &Some(U64::from(1000)),
        );
        assert_eq!(result, U64::from(5000));
    }

    #[test]
    fn fallback_from_diff_large() {
        assert_eq!(FallbackBlockRange::from_diff(U64::from(10000)), FallbackBlockRange::Range5000);
    }

    #[test]
    fn fallback_from_diff_medium() {
        assert_eq!(FallbackBlockRange::from_diff(U64::from(500)), FallbackBlockRange::Range500);
    }

    #[test]
    fn fallback_from_diff_small() {
        assert_eq!(FallbackBlockRange::from_diff(U64::from(3)), FallbackBlockRange::Range1);
    }

    #[test]
    fn fallback_lower_chain() {
        let range = FallbackBlockRange::Range5000;
        assert_eq!(range.lower(), FallbackBlockRange::Range500);
        assert_eq!(range.lower().lower(), FallbackBlockRange::Range75);
    }

    #[test]
    fn fallback_lower_bottoms_at_1() {
        assert_eq!(FallbackBlockRange::Range1.lower(), FallbackBlockRange::Range1);
    }

    fn make_log_at_block(block_number: u64) -> Log {
        Log {
            inner: PrimitiveLog { address: Default::default(), data: Default::default() },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        }
    }

    fn make_block(number: u64) -> AnyRpcBlock {
        AnyRpcBlock::new(
            alloy::rpc::types::Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number, ..Default::default() }.seal(alloy::primitives::B256::ZERO),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    }

    #[tokio::test]
    async fn historic_empty_logs_advances_to_next_range() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (tx, _rx) = mpsc::channel(4);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(100))
            .set_to_block(U64::from(200));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            None,
            U64::from(500),
            "test",
        )
        .await;

        let result = result.expect("should return next range");
        assert_eq!(result.next.from_block(), U64::from(201));
    }

    #[tokio::test]
    async fn historic_with_logs_advances_past_last_log() {
        let logs = vec![make_log_at_block(150), make_log_at_block(175)];
        let mock = MockChainProvider::new(1).with_logs(logs);
        let (tx, _rx) = mpsc::channel(4);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(100))
            .set_to_block(U64::from(200));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            None,
            U64::from(500),
            "test",
        )
        .await;

        let result = result.expect("should return next range");
        // Next from_block should be last_log.block_number + 1 = 176
        assert_eq!(result.next.from_block(), U64::from(176));
    }

    #[tokio::test]
    async fn historic_from_greater_than_to_corrects() {
        let mock = MockChainProvider::new(1);
        let (tx, _rx) = mpsc::channel(4);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(300))
            .set_to_block(U64::from(200));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            None,
            U64::from(500),
            "test",
        )
        .await;

        let result = result.expect("should return corrected range");
        assert_eq!(result.next.from_block(), U64::from(200));
    }

    #[tokio::test]
    async fn historic_empty_logs_past_snapshot_advances_filter_past_snapshot() {
        let mock = MockChainProvider::new(1);
        let (tx, _rx) = mpsc::channel(4);
        // from=500, to=500, snapshot=500 → after processing, next_from is 501 which
        // is past the snapshot. MUST still return `Some` so the caller advances
        // `current_filter.from_block` to 501 before handing off to
        // `live_indexing_stream` — otherwise live re-fetches block 500 and
        // double-dispatches any events there.
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(500))
            .set_to_block(U64::from(500));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            None,
            U64::from(500),
            "test",
        )
        .await;

        let next =
            result.expect("termination must still return Some so caller advances from_block");
        assert_eq!(
            next.next.from_block(),
            U64::from(501),
            "from_block must advance to to_block+1 so the outer while-loop \
             (`from_block <= snapshot_to_block`) exits AND the filter handed to \
             live_indexing_stream starts on the next block",
        );
    }

    #[tokio::test]
    async fn historic_with_logs_at_snapshot_boundary_advances_filter_past_last_log() {
        // Regression test for the historical→live handoff stale-filter bug.
        //
        // Scenario: phase 2's historical sub-phase fetches a single batch
        // that contains an event at the snapshot_to_block itself, e.g.
        // from=7, to=7, snapshot=7, and block 7 has 1 matching log. Pre-fix,
        // this returned None, leaving `current_filter.from_block = 7` stale.
        // `live_indexing_stream` then re-fetched block 7 and the event was
        // dispatched twice (visible in the failing e2e harness as
        // "Found 1 duplicate tx_hash entries").
        let log = make_log_at_block(7);
        let mock = MockChainProvider::new(1).with_logs(vec![log]);
        let (tx, _rx) = mpsc::channel(4);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(7))
            .set_to_block(U64::from(7));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            None,
            U64::from(7),
            "test",
        )
        .await;

        let next = result.expect("final-batch completion must return Some to advance the filter");
        assert_eq!(
            next.next.from_block(),
            U64::from(8),
            "from_block must advance past the last-logged block so \
             live_indexing_stream does not re-fetch block 7"
        );
    }

    #[tokio::test]
    async fn historic_with_max_block_range_limits_next() {
        let mock = MockChainProvider::new(1);
        let (tx, _rx) = mpsc::channel(4);
        let filter = RindexerEventFilter::empty_for_test()
            .set_from_block(U64::from(100))
            .set_to_block(U64::from(200));

        let result = fetch_historic_logs_stream(
            false,
            BlockClock::new(None, None, Arc::new(MockChainProvider::new(1))),
            &mock,
            &tx,
            &B256::ZERO,
            filter,
            Some(U64::from(50)), // max range = 50
            U64::from(5000),
            "test",
        )
        .await;

        let result = result.expect("should return next range");
        // next from = 201, next to = 201 + 50 = 251
        assert_eq!(result.next.from_block(), U64::from(201));
        assert_eq!(result.next.to_block(), U64::from(251));
    }

    #[tokio::test]
    async fn live_indexing_respects_max_block_range() {
        let cancel_token = CancellationToken::new();
        let queried_ranges = Arc::new(StdMutex::new(Vec::new()));
        let configured_max_block_range = U64::from(5);
        let live_start_block = U64::from(1);
        let provider = Arc::new(RecordingLiveProvider::new(
            MockChainProvider::new(1)
                .with_blocks(vec![make_block(100)])
                .with_max_block_range(configured_max_block_range.to::<u64>()),
            queried_ranges.clone(),
            cancel_token.clone(),
        ));
        let block_clock = BlockClock::new(None, None, provider.clone());
        let (tx, _rx) = mpsc::channel(4);

        live_indexing_stream(
            false,
            block_clock,
            provider,
            &tx,
            U64::ZERO,
            &B256::ZERO,
            &U64::ZERO,
            RindexerEventFilter::empty_for_test()
                .set_from_block(live_start_block)
                .set_to_block(live_start_block),
            "test",
            "test",
            true,
            Some(configured_max_block_range),
            cancel_token,
            None,
            None,
            None,
            &EventCallbackRegistry::default(),
            None,
        )
        .await;

        let queried_ranges = queried_ranges.lock().expect("should lock");
        let expected_live_to_block = live_start_block + configured_max_block_range;
        assert_eq!(queried_ranges.len(), 1, "expected exactly one live get_logs request");
        assert_eq!(
            queried_ranges[0],
            (live_start_block.to::<u64>(), expected_live_to_block.to::<u64>()),
            "live indexing should cap the first get_logs request to from_block + max_block_range",
        );
    }

    #[tokio::test]
    async fn live_indexing_retries_when_retry_cap_equals_last_seen_block() {
        let cancel_token = CancellationToken::new();
        let queried_ranges = Arc::new(StdMutex::new(Vec::new()));
        let provider = Arc::new(
            RecordingLiveProvider::new(
                MockChainProvider::new(1)
                    .with_blocks(vec![make_block(603)])
                    .with_max_block_range(500),
                queried_ranges.clone(),
                cancel_token.clone(),
            )
            .with_transient_first_logs_error(),
        );
        let block_clock = BlockClock::new(None, None, provider.clone());
        let (tx, _rx) = mpsc::channel(4);

        tokio::time::timeout(
            Duration::from_secs(2),
            live_indexing_stream(
                false,
                block_clock,
                provider,
                &tx,
                U64::from(601),
                &B256::ZERO,
                &U64::ZERO,
                RindexerEventFilter::empty_for_test()
                    .set_from_block(U64::from(600))
                    .set_to_block(U64::from(601)),
                "test",
                "test",
                true,
                Some(U64::from(500)),
                cancel_token,
                None,
                None,
                None,
                &EventCallbackRegistry::default(),
                None,
            ),
        )
        .await
        .expect("live indexing stalled instead of retrying the reduced range");

        let queried_ranges = queried_ranges.lock().expect("should lock");
        assert_eq!(
            queried_ranges.as_slice(),
            &[(600, 603), (600, 601)],
            "the transient head error should retry the reduced range even when its cap equals last_seen_block_number",
        );
    }

    // --- shared tip logs: live-loop tests ---

    type TipAnswer = (Duration, Result<Vec<Log>, String>);

    /// Scripted unfiltered answers per block for the shared fetcher. An exhausted queue repeats
    /// its last answer; a block with no script never answers, so the fetcher's per-call timeout
    /// is what ends each of its attempts.
    #[derive(Debug, Default)]
    struct ScriptedTipSource {
        answers: StdMutex<HashMap<u64, VecDeque<TipAnswer>>>,
        calls: StdMutex<Vec<u64>>,
    }

    impl ScriptedTipSource {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn script(&self, block: u64, answers: Vec<TipAnswer>) {
            self.answers.lock().expect("script mutex").insert(block, answers.into());
        }

        fn calls(&self) -> Vec<u64> {
            self.calls.lock().expect("calls mutex").clone()
        }

        fn next_answer(&self, block: u64) -> Option<TipAnswer> {
            let mut answers = self.answers.lock().expect("script mutex");
            let queue = answers.get_mut(&block)?;
            if queue.len() > 1 {
                queue.pop_front()
            } else {
                queue.front().cloned()
            }
        }
    }

    #[async_trait::async_trait]
    impl BlockLogsSource for ScriptedTipSource {
        async fn block_logs(&self, block: u64) -> Result<Vec<Log>, ProviderError> {
            self.calls.lock().expect("calls mutex").push(block);
            let Some((delay, answer)) = self.next_answer(block) else {
                return std::future::pending().await;
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            answer.map_err(ProviderError::CustomError)
        }
    }

    /// Captures WARN lines; the test runtime is current-thread, so the live loop logs through it.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<StdMutex<Vec<u8>>>);

    impl CapturedLogs {
        fn install() -> (Self, tracing::subscriber::DefaultGuard) {
            let captured = Self::default();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(captured.clone())
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .finish();
            (captured, tracing::subscriber::set_default(subscriber))
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log mutex")).into_owned()
        }
    }

    impl<'a> MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log mutex").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    const TIP: u64 = 603;

    /// Block hashes chained by number: block `n` names `hash_of(n - 1)` as its parent.
    fn hash_of(number: u64) -> B256 {
        B256::repeat_byte(number as u8)
    }

    fn tip_hash() -> B256 {
        hash_of(TIP)
    }

    fn stream_address() -> Address {
        Address::repeat_byte(0xaa)
    }

    fn stream_topic() -> B256 {
        B256::repeat_byte(0x70)
    }

    fn tip_block(number: u64, hash: B256, logs_bloom: Bloom) -> AnyRpcBlock {
        let parent_hash = hash_of(number - 1);
        AnyRpcBlock::new(
            alloy::rpc::types::Block::new(
                AnyRpcHeader::from_sealed(
                    AnyHeader { number, parent_hash, logs_bloom, ..Default::default() }.seal(hash),
                ),
                BlockTransactions::Full(vec![]),
            )
            .into(),
        )
    }

    /// The tip as a busy chain serves it: a saturated bloom, positive for every stream.
    fn busy_tip() -> AnyRpcBlock {
        tip_block(TIP, tip_hash(), Bloom::repeat_byte(0xff))
    }

    fn log_at(block: u64, block_hash: B256, address: Address, topic0: B256, index: u64) -> Log {
        Log {
            inner: PrimitiveLog {
                address,
                data: LogData::new_unchecked(vec![topic0], Bytes::new()),
            },
            block_hash: Some(block_hash),
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: Some(index),
            removed: false,
        }
    }

    /// A log of the stream's address and topic in the tip block.
    fn tip_log(index: u64) -> Log {
        log_at(TIP, tip_hash(), stream_address(), stream_topic(), index)
    }

    /// The live loop stamps served logs with the cached header timestamp (0 for test headers).
    fn stamped(mut log: Log) -> Log {
        log.block_timestamp = Some(0);
        log
    }

    fn block_and_index(logs: &[Log]) -> Vec<(Option<u64>, Option<u64>)> {
        logs.iter().map(|log| (log.block_number, log.log_index)).collect()
    }

    fn stream_filter(from_block: u64) -> RindexerEventFilter {
        RindexerEventFilter::new_address_filter(
            &stream_topic(),
            "Ev",
            &AddressDetails {
                address: ValueOrArray::Value(stream_address()),
                indexed_filters: None,
            },
            U64::from(from_block),
            U64::from(from_block),
        )
        .expect("an address filter")
    }

    fn settings() -> SharedTipLogsSettings {
        SharedTipLogsSettings {
            empty_retry_deadline_ms: 7000,
            cache_blocks: 32,
            bloom_trusted: true,
        }
    }

    fn counter(series: &CounterVec, labels: &[&str]) -> f64 {
        series.with_label_values(labels).get()
    }

    const SERVED_MODES: [&str; 3] = ["tip", "window", "prefix_rpc_plus_tip"];
    const FALLBACK_REASONS: [&str; 5] =
        ["gave_up", "error", "hash_mismatch", "wait_timeout", "not_scheduled"];

    /// Runs `live_indexing_stream` without a reorg coordinator until its first batch, which is
    /// returned with the instant it arrived; the batch cancels the loop.
    async fn first_live_batch(
        provider: &Arc<RecordingLiveProvider>,
        filter: RindexerEventFilter,
        last_seen_block: u64,
        disable_logs_bloom_checks: bool,
        network: &str,
    ) -> (FetchLogsResult, Instant) {
        first_live_batch_at(
            provider,
            filter,
            last_seen_block,
            disable_logs_bloom_checks,
            network,
            0,
        )
        .await
    }

    /// `first_live_batch` with an explicit `reorg_safe_distance`.
    async fn first_live_batch_at(
        provider: &Arc<RecordingLiveProvider>,
        filter: RindexerEventFilter,
        last_seen_block: u64,
        disable_logs_bloom_checks: bool,
        network: &str,
        reorg_safe_distance: u64,
    ) -> (FetchLogsResult, Instant) {
        let reorg_safe_distance = U64::from(reorg_safe_distance);
        let block_clock = BlockClock::new(None, None, provider.clone());
        let cancel_token = provider.cancel_token.clone();
        let (tx, mut rx) = mpsc::channel(4);
        let topic_id = filter.event_signature();
        let registry = EventCallbackRegistry::default();
        let stream = live_indexing_stream(
            false,
            block_clock,
            provider.clone(),
            &tx,
            U64::from(last_seen_block),
            &topic_id,
            &reorg_safe_distance,
            filter,
            "test",
            network,
            disable_logs_bloom_checks,
            None,
            cancel_token.clone(),
            None,
            None,
            None,
            &registry,
            None,
        );
        let first_batch = async {
            let batch = rx.recv().await.expect("the live loop sends a batch");
            let batch = batch.expect("a batch rather than an error");
            cancel_token.cancel();
            (batch, Instant::now())
        };
        let (_, batch) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(stream, first_batch)
        })
        .await
        .expect("the live loop stops after its first batch");
        batch
    }

    #[tokio::test(start_paused = true)]
    async fn live_tip_window_is_served_from_shared_cache() {
        let net = "live-tip-served";
        let source = ScriptedTipSource::new();
        let wanted = tip_log(0);
        let other_address = log_at(TIP, tip_hash(), Address::repeat_byte(0xbb), stream_topic(), 1);
        let other_topic = log_at(TIP, tip_hash(), stream_address(), B256::repeat_byte(0x71), 2);
        source.script(
            TIP,
            vec![(Duration::ZERO, Ok(vec![wanted.clone(), other_address, other_topic]))],
        );
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1).with_blocks(vec![busy_tip()]).with_shared_tip_logs(tip_logs),
        ));

        let (batch, _) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!((batch.from_block, batch.to_block), (U64::from(TIP), U64::from(TIP)));
        assert_eq!(batch.logs, vec![stamped(wanted)], "exactly the stream's logs");
        assert!(provider.ranges().is_empty(), "no per-stream eth_getLogs");
        assert_eq!(source.calls(), vec![TIP], "one unfiltered call for the block");
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "tip"]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_tip_window_wakes_on_ready_not_on_pacing() {
        let net = "live-tip-wakes";
        let source = ScriptedTipSource::new();
        source.script(
            TIP,
            vec![(Duration::ZERO, Ok(vec![])), (Duration::ZERO, Ok(vec![tip_log(0)]))],
        );
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1).with_blocks(vec![busy_tip()]).with_shared_tip_logs(tip_logs),
        ));
        let start = Instant::now();

        let (batch, sent_at) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!(
            sent_at - start,
            Duration::from_millis(250),
            "sent at the Ready transition (one 250 ms empty retry), not at a 200 ms pacing tick"
        );
        assert_eq!(batch.logs, vec![stamped(tip_log(0))]);
        assert!(provider.ranges().is_empty());
        assert_eq!(source.calls(), vec![TIP, TIP]);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_tip_window_falls_back_after_gave_up() {
        let net = "live-tip-gave-up";
        let source = ScriptedTipSource::new();
        source.script(TIP, vec![(Duration::ZERO, Ok(vec![]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1)
                .with_blocks(vec![busy_tip()])
                .with_logs(vec![tip_log(0)])
                .with_shared_tip_logs(tip_logs),
        ));
        let start = Instant::now();

        let (batch, sent_at) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!(sent_at - start, Duration::from_secs(7), "after the fetcher's budget");
        assert_eq!(provider.ranges(), vec![(TIP, TIP)], "exactly one own eth_getLogs");
        assert_eq!(batch.logs, vec![stamped(tip_log(0))], "the own call served the stream");
        assert_eq!(source.calls().len(), 8);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "gave_up"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_FALLBACKS_TOTAL, &[net, "gave_up"]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_wait_budget_expires_when_fetcher_is_stuck() {
        let net = "live-tip-wait-timeout";
        let (logs, _guard) = CapturedLogs::install();
        let source = ScriptedTipSource::new();
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        // Nothing is scripted, so no block ever answers. Four earlier heads hold every fetch
        // permit until their budgets end at 10.25 s, the tip's own task starts only then, and
        // the stream's 13 s wait budget, which runs from the tip's first observation, ends first.
        for number in TIP - 4..TIP {
            tip_logs.observe_head(
                number,
                hash_of(number),
                hash_of(number - 1),
                Bloom::repeat_byte(0xff),
            );
        }
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1)
                .with_blocks(vec![busy_tip()])
                .with_logs(vec![tip_log(0)])
                .with_shared_tip_logs(tip_logs),
        ));
        let start = Instant::now();

        let (batch, sent_at) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        let waited = sent_at - start;
        assert!(
            waited >= Duration::from_secs(13) && waited < Duration::from_millis(13_250),
            "fell back at first_seen + 13 s, within one pacing tick: {waited:?}"
        );
        assert_eq!(provider.ranges(), vec![(TIP, TIP)]);
        assert_eq!(batch.logs, vec![stamped(tip_log(0))]);
        assert_eq!(counter(&SHARED_TIP_LOGS_FALLBACKS_TOTAL, &[net, "wait_timeout"]), 1.0);
        let text = logs.text();
        assert_eq!(text.matches("waited").count(), 1, "one WARN: {text}");
        assert!(
            text.contains("WARN") && text.contains("block 603"),
            "the WARN names the block: {text}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn live_multi_block_window_uses_rpc_prefix_plus_cached_tip() {
        let net = "live-tip-prefix";
        let source = ScriptedTipSource::new();
        // The fetcher sorts a block's logs by index; the stream sorts the merged window.
        source.script(TIP, vec![(Duration::ZERO, Ok(vec![tip_log(1), tip_log(0)]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let prefix = vec![
            log_at(TIP - 3, hash_of(TIP - 3), stream_address(), stream_topic(), 0),
            log_at(TIP - 1, hash_of(TIP - 1), stream_address(), stream_topic(), 0),
        ];
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1)
                .with_blocks(vec![busy_tip()])
                .with_logs(prefix)
                .with_shared_tip_logs(tip_logs),
        ));

        let (batch, _) =
            first_live_batch(&provider, stream_filter(TIP - 3), TIP - 4, false, "test").await;

        assert_eq!((batch.from_block, batch.to_block), (U64::from(TIP - 3), U64::from(TIP)));
        assert_eq!(provider.ranges(), vec![(TIP - 3, TIP - 1)], "own eth_getLogs for the prefix");
        assert_eq!(
            block_and_index(&batch.logs),
            vec![
                (Some(TIP - 3), Some(0)),
                (Some(TIP - 1), Some(0)),
                (Some(TIP), Some(0)),
                (Some(TIP), Some(1)),
            ]
        );
        assert_eq!(source.calls(), vec![TIP]);
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "prefix_rpc_plus_tip"]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn live_reorg_arms_invalidate_shared_cache() {
        let net = "live-tip-reorg";
        let source = ScriptedTipSource::new();
        let mut removed = tip_log(0);
        removed.removed = true;
        source.script(TIP, vec![(Duration::from_millis(100), Ok(vec![removed]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        // Two head polls: the one that schedules the block and the one that serves the removed
        // log; the third fails and stops the loop before it observes the head again.
        let provider = Arc::new(
            RecordingLiveProvider::recording(
                MockChainProvider::new(1)
                    .with_blocks(vec![busy_tip()])
                    .with_shared_tip_logs(tip_logs.clone()),
            )
            .with_head_polls(2),
        );

        let (batch, _) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!(
            batch.reorg.as_ref().map(|reorg| reorg.fork_block),
            Some(U64::from(TIP)),
            "the removed log is a reorg signal"
        );
        assert!(provider.ranges().is_empty());
        assert_eq!(
            tip_logs.lookup(TIP, TIP, tip_hash()),
            TipLookup::Fallback(FallbackReason::NotScheduled),
            "the reorg arm dropped the block from the cache"
        );

        // The next observation of the head schedules the block again.
        let notified = tip_logs.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tip_logs.observe_head(TIP, tip_hash(), hash_of(TIP - 1), Bloom::repeat_byte(0xff));
        notified.await;
        assert!(matches!(tip_logs.lookup(TIP, TIP, tip_hash()), TipLookup::ServeTip(_)));
        assert_eq!(source.calls(), vec![TIP, TIP]);
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_knob_keeps_per_stream_path() {
        let net = "live-tip-disabled";
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1).with_blocks(vec![busy_tip()]).with_logs(vec![tip_log(0)]),
        ));
        assert!(provider.shared_tip_logs().is_none(), "the knob is off: no handle");

        let (batch, _) = first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, net).await;

        assert_eq!(provider.ranges(), vec![(TIP, TIP)], "the stream's own tip eth_getLogs");
        assert_eq!(batch.logs, vec![stamped(tip_log(0))]);
        for mode in SERVED_MODES {
            assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, mode]), 0.0);
        }
        for reason in FALLBACK_REASONS {
            assert_eq!(counter(&SHARED_TIP_LOGS_FALLBACKS_TOTAL, &[net, reason]), 0.0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn safe_distance_stream_does_not_observe_heads() {
        let net = "live-tip-safe-distance";
        let source = ScriptedTipSource::new();
        source.script(TIP, vec![(Duration::ZERO, Ok(vec![tip_log(0)]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1)
                .with_blocks(vec![busy_tip()])
                .with_logs(vec![tip_log(0)])
                .with_shared_tip_logs(tip_logs.clone()),
        ));

        let (batch, _) =
            first_live_batch_at(&provider, stream_filter(TIP - 1), TIP - 2, false, net, 1).await;

        assert_eq!(
            (batch.from_block, batch.to_block),
            (U64::from(TIP - 1), U64::from(TIP - 1)),
            "the window stops one block behind the tip"
        );
        assert_eq!(provider.ranges(), vec![(TIP - 1, TIP - 1)], "the stream's own eth_getLogs");
        assert!(source.calls().is_empty(), "a stream that cannot read the cache starts no fetch");
        assert!(
            matches!(
                tip_logs.lookup(TIP, TIP, tip_hash()),
                TipLookup::Fallback(FallbackReason::NotScheduled)
            ),
            "the tip was never observed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn bloom_negative_skip_still_wins() {
        let net = "live-tip-bloom-skip";
        let source = ScriptedTipSource::new();
        source.script(TIP, vec![(Duration::ZERO, Ok(vec![tip_log(0)]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let quiet_tip = tip_block(TIP, tip_hash(), Bloom::repeat_byte(0x01));
        assert!(
            !is_relevant_block(
                &Some(HashSet::from([stream_address()])),
                &stream_topic(),
                &quiet_tip
            ),
            "the bloom is negative for the stream"
        );
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1).with_blocks(vec![quiet_tip]).with_shared_tip_logs(tip_logs),
        ));

        let (batch, _) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!((batch.from_block, batch.to_block), (U64::from(TIP), U64::from(TIP)));
        assert!(batch.logs.is_empty(), "the bloom skip answers the window empty");
        assert!(provider.ranges().is_empty(), "no own eth_getLogs");
        for mode in SERVED_MODES {
            assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, mode]), 0.0, "no lookup");
        }
        for reason in FALLBACK_REASONS {
            assert_eq!(counter(&SHARED_TIP_LOGS_FALLBACKS_TOTAL, &[net, reason]), 0.0);
        }
        // The block's shared fetch belongs to the network, not to this stream: it still ran.
        assert_eq!(source.calls(), vec![TIP]);
    }

    #[tokio::test(start_paused = true)]
    async fn observe_head_runs_without_a_coordinator() {
        // `first_live_batch` passes `reorg_coordinator = None`: the head is observed, fetched
        // once and served without one.
        let net = "live-tip-no-coordinator";
        let source = ScriptedTipSource::new();
        source.script(TIP, vec![(Duration::ZERO, Ok(vec![tip_log(0)]))]);
        let tip_logs = SharedTipLogs::new(net, source.clone(), settings());
        let provider = Arc::new(RecordingLiveProvider::recording(
            MockChainProvider::new(1)
                .with_blocks(vec![busy_tip()])
                .with_shared_tip_logs(tip_logs.clone()),
        ));

        let (batch, _) =
            first_live_batch(&provider, stream_filter(TIP), TIP - 1, false, "test").await;

        assert_eq!(batch.logs, vec![stamped(tip_log(0))]);
        assert_eq!(source.calls(), vec![TIP]);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 1.0);
        assert!(matches!(tip_logs.lookup(TIP, TIP, tip_hash()), TipLookup::ServeTip(_)));
        assert!(provider.ranges().is_empty());
    }

    // --- retry_with_block_range tests ---

    #[tokio::test]
    async fn retry_alchemy_block_range_parsing() {
        let error =
            ProviderError::CustomError("this block range should work: [0x100, 0x200]".to_string());
        let result = retry_with_block_range("test", &error, U64::from(0), U64::from(999), None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, U64::from(0x100));
        assert_eq!(result.to, U64::from(0x200));
        assert_eq!(result.max_block_range, None);
    }

    #[tokio::test]
    async fn retry_ankr_block_range_too_wide() {
        let error = ProviderError::CustomError("block range is too wide".to_string());
        let from = U64::from(500);
        let result = retry_with_block_range("test", &error, from, U64::from(10000), None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, from);
        assert_eq!(result.to, from + U64::from(3000));
        assert_eq!(result.max_block_range, Some(U64::from(3000)));
    }

    #[tokio::test]
    async fn retry_base_block_range_too_large() {
        let error = ProviderError::CustomError("block range too large".to_string());
        let from = U64::from(500);
        let result = retry_with_block_range("test", &error, from, U64::from(10000), None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, from);
        assert_eq!(result.to, from + U64::from(2000));
        assert_eq!(result.max_block_range, Some(U64::from(2000)));
    }

    #[tokio::test]
    async fn retry_quicknode_limited_to() {
        let error = ProviderError::CustomError("limited to a 10,000 block range".to_string());
        let from = U64::from(500);
        let result = retry_with_block_range("test", &error, from, U64::from(20000), None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, from);
        assert_eq!(result.to, from + U64::from(10000));
        assert_eq!(result.max_block_range, Some(U64::from(10000)));
    }

    #[tokio::test]
    async fn retry_response_too_big_halves_range() {
        let error = ProviderError::CustomError("response is too big".to_string());
        let from = U64::from(100);
        let to = U64::from(10100);
        // halved_block_number(10100, 100) = 100 + (10000 / 2) = 5100
        let expected_to = halved_block_number(to, from);
        let result = retry_with_block_range("test", &error, from, to, None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, from);
        assert_eq!(result.to, expected_to);
        assert_eq!(result.max_block_range, None);
    }

    #[tokio::test]
    async fn retry_fallback_unknown_error_uses_range5000() {
        let error = ProviderError::CustomError("some unknown rpc error".to_string());
        let from = U64::from(100);
        let to = U64::from(10100); // diff = 10000 → FallbackBlockRange::Range5000
        let result = retry_with_block_range("test", &error, from, to, None)
            .await
            .expect("should return a result");
        assert_eq!(result.from, from);
        assert_eq!(result.to, from + U64::from(5000));
        assert_eq!(result.max_block_range, Some(U64::from(5000)));
    }

    #[tokio::test]
    async fn retry_equal_from_to_returns_none() {
        let error = ProviderError::CustomError("some unknown error".to_string());
        let result =
            retry_with_block_range("test", &error, U64::from(100), U64::from(100), None).await;
        assert!(result.is_none());
    }

    // --- classify_fetch_error tests ---

    #[test]
    fn classify_rate_limit_signals() {
        for msg in [
            "HTTP 429 Too Many Requests",
            "rate limit exceeded",
            "request was rate-limited",
            "Too Many Requests",
            "monthly quota exceeded",
            "request throttled by upstream",
        ] {
            let err = ProviderError::CustomError(msg.to_string());
            assert_eq!(
                classify_fetch_error(&err),
                FetchErrorKind::RateLimit,
                "expected RateLimit for message: {msg}"
            );
        }
    }

    #[test]
    fn classify_non_rate_limit_is_other() {
        for msg in [
            "block range is too wide",
            "response is too big",
            "error decoding response body",
            "connection reset",
        ] {
            let err = ProviderError::CustomError(msg.to_string());
            assert_eq!(
                classify_fetch_error(&err),
                FetchErrorKind::Other,
                "expected Other for message: {msg}"
            );
        }
    }

    mod parallel {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio_util::sync::CancellationToken;

        #[test]
        fn plan_small_range_below_threshold_is_not_reached_but_returns_one_worker() {
            // Small ranges never reach plan_parallel_fetch (the caller filters them
            // via PARALLEL_MIN_BLOCKS), but plan must still produce a sane result
            // if called directly.
            let p = plan_parallel_fetch(500, 4);
            assert_eq!(p.chunk_size, 1000, "chunk_size floored at PARALLEL_MIN_CHUNK");
            assert_eq!(p.effective_concurrency, 1, "never spawn 0 workers");
        }

        #[test]
        fn plan_exact_threshold_yields_single_worker() {
            // 1000 blocks / 1000-block chunks = 1 worker regardless of requested N.
            let p = plan_parallel_fetch(1000, 4);
            assert_eq!(p.chunk_size, 1000);
            assert_eq!(p.effective_concurrency, 1);
        }

        #[test]
        fn plan_evenly_divisible_range_saturates_all_workers() {
            // 10000 blocks / 4 workers = 2500 per worker.
            let p = plan_parallel_fetch(10_000, 4);
            assert_eq!(p.chunk_size, 2500);
            assert_eq!(p.effective_concurrency, 4);
        }

        #[test]
        fn plan_concurrency_capped_at_max() {
            // Requesting 100 workers for a 1M-block range should cap at 32.
            let p = plan_parallel_fetch(1_000_000, 100);
            assert_eq!(p.effective_concurrency, 32, "capped to PARALLEL_MAX_CONCURRENCY");
            assert_eq!(p.chunk_size, 31_250);
        }

        #[test]
        fn plan_concurrency_limited_by_range_size() {
            // 3500-block range with requested 10 workers: each worker needs ≥1000
            // blocks, so we can only use 3.
            let p = plan_parallel_fetch(3_500, 10);
            assert_eq!(p.chunk_size, 1000, "floored at min chunk");
            assert_eq!(p.effective_concurrency, 3, "floor(3500/1000)=3");
        }

        #[test]
        fn plan_zero_concurrency_clamps_to_one() {
            let p = plan_parallel_fetch(10_000, 0);
            assert_eq!(p.effective_concurrency, 1);
            assert_eq!(p.chunk_size, 10_000);
        }

        #[test]
        fn plan_huge_range_no_overflow() {
            // Near-u64::MAX range must not overflow the chunk-size calc.
            let p = plan_parallel_fetch(u64::MAX / 2, 32);
            assert!(p.chunk_size > 0);
            assert_eq!(p.effective_concurrency, 32);
        }

        fn batch(sid: u64, blocks: &[u64], is_final: bool) -> SequencedFetchBatch {
            let results = blocks
                .iter()
                .map(|&b| {
                    Ok(FetchLogsResult {
                        logs: vec![],
                        from_block: U64::from(b),
                        to_block: U64::from(b),
                        reorg: None,
                    })
                })
                .collect();
            SequencedFetchBatch { sequence_id: sid, results, is_final }
        }

        fn drained_from_blocks(
            emitted: Vec<Result<FetchLogsResult, Box<dyn Error + Send>>>,
        ) -> Vec<u64> {
            emitted
                .into_iter()
                .map(|r| r.expect("test batch had no errors").from_block.to::<u64>())
                .collect()
        }

        #[test]
        fn reorder_in_order_finals_emit_immediately() {
            let mut buf = ReorderBuffer::new();
            let out0 = buf.accept(batch(0, &[0], true));
            let out1 = buf.accept(batch(1, &[1], true));
            let out2 = buf.accept(batch(2, &[2], true));

            assert_eq!(drained_from_blocks(out0), vec![0]);
            assert_eq!(drained_from_blocks(out1), vec![1]);
            assert_eq!(drained_from_blocks(out2), vec![2]);
            assert_eq!(buf.next_expected(), 3);
            assert!(buf.pending_sequence_ids().is_empty());
        }

        #[test]
        fn reorder_out_of_order_buffers_until_gap_closes() {
            let mut buf = ReorderBuffer::new();

            // Worker 2 finishes first — buffered.
            let out_a = buf.accept(batch(2, &[20], true));
            assert!(out_a.is_empty(), "sid 2 must wait while 0 and 1 are missing");
            assert_eq!(buf.pending_sequence_ids(), vec![2]);

            // Worker 0 finishes next — only 0 is emitted (1 is still missing).
            let out_b = buf.accept(batch(0, &[0], true));
            assert_eq!(drained_from_blocks(out_b), vec![0]);
            assert_eq!(buf.next_expected(), 1);

            // Worker 1 finishes — emits 1, then drains buffered 2 in one shot.
            let out_c = buf.accept(batch(1, &[10], true));
            assert_eq!(drained_from_blocks(out_c), vec![10, 20], "ordering preserved after drain");
            assert_eq!(buf.next_expected(), 3);
        }

        #[test]
        fn reorder_partial_batches_forwarded_but_do_not_advance_cursor() {
            let mut buf = ReorderBuffer::new();

            // Partial batch for sid 0 — forwarded but cursor stays at 0.
            let out_p1 = buf.accept(batch(0, &[0, 1], false));
            assert_eq!(drained_from_blocks(out_p1), vec![0, 1]);
            assert_eq!(buf.next_expected(), 0, "partial must NOT advance cursor");

            // Second partial for same sid — also forwarded.
            let out_p2 = buf.accept(batch(0, &[2], false));
            assert_eq!(drained_from_blocks(out_p2), vec![2]);
            assert_eq!(buf.next_expected(), 0);

            // Final batch for sid 0 — flushes and advances cursor.
            let out_f = buf.accept(batch(0, &[3], true));
            assert_eq!(drained_from_blocks(out_f), vec![3]);
            assert_eq!(buf.next_expected(), 1);
        }

        #[test]
        fn reorder_partial_then_final_for_buffered_sid() {
            let mut buf = ReorderBuffer::new();

            // Buffered partial then final for sid 1 while waiting on sid 0.
            buf.accept(batch(1, &[10], false));
            buf.accept(batch(1, &[11], true));
            assert_eq!(buf.pending_sequence_ids(), vec![1]);

            // sid 0 arrives — should flush 0 then both chunks of 1 in order.
            let out = buf.accept(batch(0, &[0], true));
            assert_eq!(
                drained_from_blocks(out),
                vec![0, 10, 11],
                "buffered partial + final of sid 1 must flush in original arrival order"
            );
            assert_eq!(buf.next_expected(), 2);
        }

        #[test]
        fn reorder_many_out_of_order_preserves_block_order() {
            let mut buf = ReorderBuffer::new();
            let mut emitted: Vec<u64> = Vec::new();

            // Arrive in reverse sequence order: 4, 3, 2, 1, 0 — each with one block.
            for sid in [4u64, 3, 2, 1, 0] {
                let out = buf.accept(batch(sid, &[sid * 10], true));
                emitted.extend(drained_from_blocks(out));
            }

            assert_eq!(emitted, vec![0, 10, 20, 30, 40]);
            assert_eq!(buf.next_expected(), 5);
        }

        #[test]
        fn reorder_empty_final_batch_still_advances() {
            let mut buf = ReorderBuffer::new();
            let out = buf.accept(batch(0, &[], true));
            assert!(out.is_empty());
            assert_eq!(buf.next_expected(), 1, "empty final batch still advances cursor");
        }

        #[tokio::test]
        async fn drop_guard_unsent_on_panic_sends_error_and_decrements_counter() {
            let (tx, mut rx) = mpsc::channel::<SequencedFetchBatch>(4);
            let cancel = CancellationToken::new();
            let active = Arc::new(AtomicUsize::new(1));
            let notify = Arc::new(tokio::sync::Notify::new());

            {
                let _guard = WorkerDropGuard {
                    sequence_id: 7,
                    tx: tx.clone(),
                    cancel_token: cancel.clone(),
                    active_workers: Arc::clone(&active),
                    worker_done_notify: Arc::clone(&notify),
                    sent: false,
                };
                // guard dropped at scope end without sent=true — simulates panic.
            }

            let batch = rx.try_recv().expect("panic batch must be sent");
            assert_eq!(batch.sequence_id, 7);
            assert!(batch.is_final, "panic batch must be final to unblock reorder buffer");
            assert_eq!(batch.results.len(), 1);
            assert!(batch.results[0].is_err(), "panic batch must carry an error");

            assert_eq!(active.load(Ordering::Acquire), 0, "counter must be decremented");
            assert!(!cancel.is_cancelled(), "cancel only fires when try_send fails");
        }

        #[tokio::test]
        async fn drop_guard_sent_true_skips_error_batch() {
            let (tx, mut rx) = mpsc::channel::<SequencedFetchBatch>(4);
            let cancel = CancellationToken::new();
            let active = Arc::new(AtomicUsize::new(1));
            let notify = Arc::new(tokio::sync::Notify::new());

            {
                let mut guard = WorkerDropGuard {
                    sequence_id: 3,
                    tx: tx.clone(),
                    cancel_token: cancel.clone(),
                    active_workers: Arc::clone(&active),
                    worker_done_notify: Arc::clone(&notify),
                    sent: false,
                };
                guard.sent = true; // normal exit path
            }

            assert!(rx.try_recv().is_err(), "no extra batch when worker exited cleanly");
            assert_eq!(
                active.load(Ordering::Acquire),
                0,
                "counter release is now single-sourced in Drop — always decrements"
            );
        }

        #[tokio::test]
        async fn drop_guard_closed_channel_cancels_pipeline() {
            let (tx, rx) = mpsc::channel::<SequencedFetchBatch>(1);
            drop(rx); // downstream closed before the worker can report.

            let cancel = CancellationToken::new();
            let active = Arc::new(AtomicUsize::new(1));
            let notify = Arc::new(tokio::sync::Notify::new());

            {
                let _guard = WorkerDropGuard {
                    sequence_id: 42,
                    tx: tx.clone(),
                    cancel_token: cancel.clone(),
                    active_workers: Arc::clone(&active),
                    worker_done_notify: Arc::clone(&notify),
                    sent: false,
                };
            }

            assert!(
                cancel.is_cancelled(),
                "guard must cancel pipeline when the error batch cannot be delivered"
            );
        }

        #[tokio::test]
        async fn drop_guard_decrements_counter_even_when_channel_is_full() {
            // If the reorder buffer is slow and the channel is saturated, try_send
            // fails, cancel fires, but the counter MUST still be released to
            // unblock the dispatcher.
            let (tx, _rx) = mpsc::channel::<SequencedFetchBatch>(1);
            // Fill the buffer so try_send fails.
            tx.try_send(SequencedFetchBatch { sequence_id: 0, results: vec![], is_final: false })
                .expect("first send fits");

            let cancel = CancellationToken::new();
            let active = Arc::new(AtomicUsize::new(1));
            let notify = Arc::new(tokio::sync::Notify::new());

            {
                let _guard = WorkerDropGuard {
                    sequence_id: 1,
                    tx: tx.clone(),
                    cancel_token: cancel.clone(),
                    active_workers: Arc::clone(&active),
                    worker_done_notify: Arc::clone(&notify),
                    sent: false,
                };
            }

            assert!(cancel.is_cancelled());
            assert_eq!(
                active.load(Ordering::Acquire),
                0,
                "counter must be released even on channel-full path or dispatcher deadlocks"
            );
        }

        #[tokio::test]
        async fn fetch_logs_once_empty_range_advances() {
            let mock = MockChainProvider::new(1);
            let filter = RindexerEventFilter::empty_for_test()
                .set_from_block(U64::from(100))
                .set_to_block(U64::from(200));

            let bc = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
            let (result, next, kind) =
                fetch_logs_once(false, &bc, &mock, filter, None, U64::from(500), "test").await;

            let r =
                result.expect("empty logs still return a result so sink can advance checkpoint");
            assert_eq!(r.from_block, U64::from(100));
            assert_eq!(r.to_block, U64::from(200));
            assert!(r.logs.is_empty());
            let next = next.expect("range not exhausted");
            assert_eq!(next.next.from_block(), U64::from(201));
            assert!(kind.is_none(), "success path reports no error kind");
        }

        #[tokio::test]
        async fn fetch_logs_once_past_snapshot_returns_no_next() {
            let mock = MockChainProvider::new(1);
            let filter = RindexerEventFilter::empty_for_test()
                .set_from_block(U64::from(500))
                .set_to_block(U64::from(500));

            let bc = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
            let (_result, next, _kind) =
                fetch_logs_once(false, &bc, &mock, filter, None, U64::from(500), "test").await;

            assert!(next.is_none(), "no further work past snapshot_to_block");
        }

        #[tokio::test]
        async fn fetch_logs_once_with_logs_advances_past_last_log() {
            let logs = vec![make_log_at_block(120), make_log_at_block(180)];
            let mock = MockChainProvider::new(1).with_logs(logs);
            let filter = RindexerEventFilter::empty_for_test()
                .set_from_block(U64::from(100))
                .set_to_block(U64::from(200));

            let bc = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
            let (result, next, _kind) =
                fetch_logs_once(false, &bc, &mock, filter, None, U64::from(500), "test").await;

            let r = result.expect("should return logs");
            assert_eq!(r.logs.len(), 2);
            assert!(r.reorg.is_none(), "historical fetch must never emit a reorg");
            let next = next.expect("more range remaining");
            assert_eq!(next.next.from_block(), U64::from(181));
        }

        #[tokio::test]
        async fn fetch_logs_once_from_gt_to_corrects_instead_of_failing() {
            let mock = MockChainProvider::new(1);
            let filter = RindexerEventFilter::empty_for_test()
                .set_from_block(U64::from(300))
                .set_to_block(U64::from(200));

            let bc = BlockClock::new(None, None, Arc::new(MockChainProvider::new(1)));
            let (result, next, _kind) =
                fetch_logs_once(false, &bc, &mock, filter, None, U64::from(500), "test").await;

            assert!(result.is_none(), "no logs emitted for inverted range");
            let next = next.expect("self-correction returns a fixed next filter");
            assert_eq!(next.next.from_block(), U64::from(200));
        }
    }
}
