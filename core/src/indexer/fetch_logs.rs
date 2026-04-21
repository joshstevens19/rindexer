use crate::adaptive_concurrency::{AdaptiveConcurrency, ADAPTIVE_CONCURRENCY};
use crate::blockclock::BlockClock;
use crate::database::clickhouse::client::ClickhouseClient;
use crate::event::callback_registry::{EventCallbackRegistry, TraceCallbackRegistry};
use crate::helpers::{halved_block_number, is_relevant_block};
use crate::indexer::heartbeat::{HeartbeatAction, HeartbeatTracker};
use crate::indexer::reorg::{
    detect_and_handle_reorg, reorg_safe_distance_for_chain, ReorgContext, ReorgCoordinator,
};
use crate::metrics::indexing as metrics;
use crate::PostgresClient;
use crate::{
    event::{config::EventProcessingConfig, RindexerEventFilter},
    indexer::{reorg::handle_chain_notification, IndexingEventProgressStatus},
    is_running,
    provider::{ChainProvider, ProviderError},
};
use alloy::{
    primitives::{B256, U64},
    rpc::types::Log,
};
use lru::LruCache;
use rand::{random_bool, random_ratio};
use regex::Regex;
use std::collections::BTreeMap;
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
                return if next_from_block > snapshot_to_block {
                    None
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );

                    debug!(
                        "{} - No events between {} - {}. Searching next {} blocks.",
                        info_log_name,
                        from_block,
                        to_block,
                        new_to_block - next_from_block
                    );

                    debug!(
                        "{} - {} - new_from_block {:?} new_to_block {:?}",
                        info_log_name,
                        IndexingEventProgressStatus::syncing_log(),
                        next_from_block,
                        new_to_block
                    );

                    Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .set_from_block(next_from_block)
                            .set_to_block(new_to_block),
                        max_block_range_limitation,
                    })
                };
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
                return if next_from_block > snapshot_to_block {
                    None
                } else {
                    let new_to_block = calculate_process_historic_log_to_block(
                        &next_from_block,
                        &snapshot_to_block,
                        &max_block_range_limitation,
                    );

                    debug!(
                        "{} - {} - new_from_block {:?} new_to_block {:?}",
                        info_log_name,
                        IndexingEventProgressStatus::syncing_log(),
                        next_from_block,
                        new_to_block
                    );

                    Some(ProcessHistoricLogsStreamResult {
                        next: current_filter
                            .set_from_block(next_from_block)
                            .set_to_block(new_to_block),
                        max_block_range_limitation,
                    })
                };
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

            for b in fork_block..=(fork_block + reth_reorg.depth) {
                block_cache.pop(&b);
            }

            // Route through coordinator for full recovery (event deletion, checkpoint
            // rewind, derived table rollback, window update) when available.
            if let Some(coordinator) = reorg_coordinator.as_ref() {
                let detection_point = fork_block + reth_reorg.depth;
                // Mutex held across reorg handling (DB rollback, stream
                // publishes in parallel, user on_reorg callback firing). On a
                // real reorg this blocks the other indexing path for the
                // duration of handle_reorg, which is acceptable for isolation.
                // If latency becomes a concern, move handle_reorg out of the
                // hot path.
                let mut guard = coordinator.lock().await;
                match guard.on_exex_reorg(detection_point, fork_block) {
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

                    let latest_block_number = log_response_to_large_to_block
                        .unwrap_or(U64::from(latest_block.header.number));

                    if last_seen_block_number == latest_block_number {
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

                            let to_block = safe_block_number;
                            if from_block == to_block
                                && !disable_logs_bloom_checks
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

                                match cached_provider.get_logs(&current_filter).await {
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
    use crate::event::RindexerEventFilter;
    use crate::provider::mock::MockChainProvider;
    use alloy::primitives::Log as PrimitiveLog;
    use alloy::rpc::types::Log;
    use tokio::sync::mpsc;

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
    async fn historic_empty_logs_past_snapshot_returns_none() {
        let mock = MockChainProvider::new(1);
        let (tx, _rx) = mpsc::channel(4);
        // from=500, to=500, snapshot=500 → after processing, next would be 501 > 500
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

        assert!(result.is_none(), "should signal completion when past snapshot");
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
