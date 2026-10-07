//! Shared per-network fetch of a tip block's logs.
//!
//! Each head block is read once per network with an unfiltered `eth_getLogs`, retried while a
//! block whose header proves it has logs answers empty, and every live stream whose window ends
//! at the tip filters that shared answer locally with [`filter_logs_for_stream`].

use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use alloy::primitives::{Address, Bloom, B256};
use alloy::rpc::types::{Filter, Log, ValueOrArray};
use async_trait::async_trait;
use lru::LruCache;
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, Semaphore};
use tokio::time::error::Elapsed;
use tokio::time::Instant;
use tracing::{debug, warn};

use crate::event::RindexerEventFilter;
use crate::is_running;
use crate::manifest::network::Network;
use crate::metrics::indexing as metrics;
use crate::provider::ProviderError;

/// Unfiltered calls a block may cost before the fetcher gives up on it.
const MAX_ATTEMPTS: u32 = 8;
/// Errors, timeouts and hash-mismatched answers are retried at most this many times, so a
/// provider that rejects the unfiltered call fails the block fast (streams fall back once)
/// instead of stalling every stream for the whole empty-answer budget.
const MAX_ERROR_ATTEMPTS: u32 = 3;
/// Bound on one unfiltered `eth_getLogs`.
const PER_CALL_TIMEOUT: Duration = Duration::from_secs(5);
/// Unfiltered calls in flight per network.
const IN_FLIGHT_PERMITS: usize = 4;
/// Slack a stream adds to the fetcher's budget before it fetches a block on its own.
const STREAM_WAIT_SLACK: Duration = Duration::from_secs(1);

/// Per-network settings of the shared fetcher, resolved from the manifest.
///
/// Every field is public and `Debug` renders a valid struct literal: the code generator writes
/// the resolved value into `networks.rs`.
#[derive(Debug, Clone)]
pub struct SharedTipLogsSettings {
    pub empty_retry_deadline_ms: u64,
    pub cache_blocks: usize,
    /// A non-zero header bloom proves the block has logs. `false` on a network that disables
    /// bloom checks: every head block is fetched and an empty answer is accepted without retry.
    pub bloom_trusted: bool,
}

impl SharedTipLogsSettings {
    /// `None` when the manifest disables the shared fetch for this network.
    pub fn resolve(network: &Network) -> Option<Self> {
        let config = network.shared_tip_logs.clone().unwrap_or_default();
        if !config.enabled {
            return None;
        }
        Some(Self {
            empty_retry_deadline_ms: config.empty_retry_deadline_ms,
            cache_blocks: config.cache_blocks,
            bloom_trusted: !network.disable_logs_bloom_checks.unwrap_or(false),
        })
    }
}

/// A block's complete log set: `eth_getLogs(fromBlock = toBlock = block)` with no address and
/// no topics.
// `async_trait` boxes the future as `#[must_use]` on top of the `Result`, which clippy on
// Rust 1.99 reports as a double `must_use`.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait BlockLogsSource: Send + Sync + fmt::Debug {
    async fn block_logs(&self, block: u64) -> Result<Vec<Log>, ProviderError>;
}

/// Where a head block stands.
#[derive(Debug, Clone, PartialEq)]
enum TipStatus {
    Pending { attempts: u32 },
    Ready { logs: Arc<Vec<Log>>, attempts: u32 },
    GaveUp { attempts: u32 },
    Failed { attempts: u32, error: String },
}

/// One head block observed by the live streams, keyed by number in the cache.
#[derive(Debug, Clone)]
struct TipBlockEntry {
    number: u64,
    hash: B256,
    parent_hash: B256,
    /// The instant the block was first observed as head: the fetcher's deadline and the streams'
    /// wait budget both run from it.
    first_seen: Instant,
    status: TipStatus,
}

impl TipBlockEntry {
    fn is_pending(&self) -> bool {
        matches!(self.status, TipStatus::Pending { .. })
    }
}

/// What a stream whose window `[from, to]` ends at the tip should do.
#[derive(Debug, Clone, PartialEq)]
pub enum TipLookup {
    /// Every block of the window is cached and chain-linked: the logs of each block, ascending.
    ServeWindow(Vec<Arc<Vec<Log>>>),
    /// A single-block window served from the cache.
    ServeTip(Arc<Vec<Log>>),
    /// The stream fetches `[from, to - 1]` itself and appends these tip logs.
    ServePrefixByRpcPlusTip(Arc<Vec<Log>>),
    /// A block of the window is still being fetched; await [`SharedTipLogs::notified`].
    Wait { oldest_pending: u64, first_seen: Instant },
    /// The stream issues its own `eth_getLogs` for the window.
    Fallback(FallbackReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    GaveUp,
    Error,
    HashMismatch,
    NotScheduled,
}

impl FallbackReason {
    /// The `reason` label of `rindexer_shared_tip_logs_fallbacks_total`.
    pub fn as_label(self) -> &'static str {
        match self {
            Self::GaveUp => "gave_up",
            Self::Error => "error",
            Self::HashMismatch => "hash_mismatch",
            Self::NotScheduled => "not_scheduled",
        }
    }
}

/// The tip-block log cache of one network and the fetch task behind each entry.
///
/// `state` is a plain mutex that is never held across an await; `changed` wakes waiting streams
/// on every fetch exit and every invalidation.
pub struct SharedTipLogs {
    network: String,
    source: Arc<dyn BlockLogsSource>,
    empty_retry_deadline: Duration,
    bloom_trusted: bool,
    state: Mutex<TipState>,
    in_flight: Semaphore,
    changed: Notify,
    me: Weak<Self>,
}

struct TipState {
    entries: LruCache<u64, TipBlockEntry>,
}

/// Verdict on one unfiltered call, judged against the retry budget.
enum Verdict {
    Ready { logs: Vec<Log>, outcome: &'static str },
    Retry { empty: bool },
    GaveUp,
    Failed { error: String, outcome: &'static str },
}

/// Wakes every waiting stream when a fetch task exits, whatever the exit path.
struct NotifyOnExit<'a>(&'a Notify);

impl Drop for NotifyOnExit<'_> {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

impl fmt::Debug for SharedTipLogs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedTipLogs")
            .field("network", &self.network)
            .field("empty_retry_deadline", &self.empty_retry_deadline)
            .field("bloom_trusted", &self.bloom_trusted)
            .finish_non_exhaustive()
    }
}

impl SharedTipLogs {
    pub fn new(
        network: &str,
        source: Arc<dyn BlockLogsSource>,
        settings: SharedTipLogsSettings,
    ) -> Arc<Self> {
        let capacity = NonZeroUsize::new(settings.cache_blocks).unwrap_or(NonZeroUsize::MIN);
        Arc::new_cyclic(|me| Self {
            network: network.to_string(),
            source,
            empty_retry_deadline: Duration::from_millis(settings.empty_retry_deadline_ms),
            bloom_trusted: settings.bloom_trusted,
            state: Mutex::new(TipState { entries: LruCache::new(capacity) }),
            in_flight: Semaphore::new(IN_FLIGHT_PERMITS),
            changed: Notify::new(),
            me: Weak::clone(me),
        })
    }

    /// How long a stream may wait on a block, from the block's `first_seen`, before it fetches
    /// the block on its own.
    pub fn stream_wait_budget(&self) -> Duration {
        self.empty_retry_deadline + PER_CALL_TIMEOUT + STREAM_WAIT_SLACK
    }

    /// The `network` label of every `rindexer_shared_tip_logs_*` series this cache records.
    pub fn network(&self) -> &str {
        &self.network
    }

    /// Wake-up for state changes: create it, call `enable()`, then [`Self::lookup`], then await.
    pub fn notified(&self) -> Notified<'_> {
        self.changed.notified()
    }

    /// Records the header a live stream just polled and starts the shared fetch of a new block.
    ///
    /// A same-height hash change or a parent-hash mismatch drops the stale entries from that
    /// height up. A zero bloom on a bloom-trusting network is served empty with no call.
    pub fn observe_head(&self, number: u64, hash: B256, parent_hash: B256, logs_bloom: Bloom) {
        let mut invalidated = false;
        let scheduled = {
            let mut state = self.lock();
            match state.entries.peek(&number).map(|entry| entry.hash == hash) {
                Some(true) => return,
                Some(false) => {
                    self.drop_from(&mut state, number);
                    invalidated = true;
                }
                None => {}
            }
            if let Some(parent_number) = number.checked_sub(1) {
                let parent_replaced = state
                    .entries
                    .peek(&parent_number)
                    .is_some_and(|parent| parent.hash != parent_hash);
                if parent_replaced {
                    self.drop_from(&mut state, parent_number);
                    invalidated = true;
                }
            }
            let status = if self.bloom_trusted && logs_bloom == Bloom::ZERO {
                TipStatus::Ready { logs: Arc::new(Vec::new()), attempts: 0 }
            } else {
                TipStatus::Pending { attempts: 0 }
            };
            let entry =
                TipBlockEntry { number, hash, parent_hash, first_seen: Instant::now(), status };
            let scheduled = entry.is_pending();
            self.insert(&mut state, entry);
            scheduled
        };
        if invalidated {
            self.changed.notify_waiters();
        }
        if !scheduled {
            metrics::record_shared_tip_logs_block(&self.network, "empty_bloom_zero");
            return;
        }
        if let Some(me) = self.me.upgrade() {
            tokio::spawn(me.fetch(number, hash));
        }
    }

    /// What a stream whose window `[from, to]` ends at the tip it polled (`expected_tip_hash`)
    /// should do. Reads touch the LRU so recent blocks stay resident.
    pub fn lookup(&self, from: u64, to: u64, expected_tip_hash: B256) -> TipLookup {
        let mut state = self.lock();
        let Some(tip) = state.entries.get(&to) else {
            return TipLookup::Fallback(FallbackReason::NotScheduled);
        };
        if tip.hash != expected_tip_hash {
            return TipLookup::Fallback(FallbackReason::HashMismatch);
        }
        let tip_logs = match &tip.status {
            TipStatus::Pending { .. } => {
                return TipLookup::Wait { oldest_pending: to, first_seen: tip.first_seen };
            }
            TipStatus::GaveUp { .. } => return TipLookup::Fallback(FallbackReason::GaveUp),
            TipStatus::Failed { .. } => return TipLookup::Fallback(FallbackReason::Error),
            TipStatus::Ready { logs, .. } => Arc::clone(logs),
        };
        let tip_parent_hash = tip.parent_hash;
        if from >= to {
            self.record_served("tip");
            return TipLookup::ServeTip(tip_logs);
        }
        if let Some(mut window) = Self::linked_ready_prefix(&mut state, from, to, tip_parent_hash) {
            window.push(tip_logs);
            self.record_served("window");
            return TipLookup::ServeWindow(window);
        }
        if let Some((oldest_pending, first_seen)) = Self::oldest_pending(&state, from, to) {
            return TipLookup::Wait { oldest_pending, first_seen };
        }
        self.record_served("prefix_rpc_plus_tip");
        TipLookup::ServePrefixByRpcPlusTip(tip_logs)
    }

    /// Drops every cached block from `number` up (a reorg signal) and wakes waiting streams.
    pub fn invalidate_from(&self, number: u64) {
        {
            let mut state = self.lock();
            self.drop_from(&mut state, number);
        }
        self.changed.notify_waiters();
    }

    /// The fetch task of one scheduled block: unfiltered calls until the block is `Ready`, the
    /// budget is spent, or the block was dropped under it.
    async fn fetch(self: Arc<Self>, number: u64, hash: B256) {
        let _notify = NotifyOnExit(&self.changed);
        let Ok(_permit) = self.in_flight.acquire().await else {
            return;
        };
        let Some(first_seen) = self.first_seen(number, hash) else {
            return;
        };
        let deadline = first_seen + self.empty_retry_deadline;
        let mut attempt = 0;
        let mut empty_retries = 0;
        let mut error_attempts = 0;
        loop {
            if !is_running() {
                let error = "shutdown".to_string();
                self.store_status(number, hash, TipStatus::Failed { attempts: attempt, error });
                return;
            }
            if !self.is_current(number, hash) {
                return;
            }
            attempt += 1;
            let answer =
                tokio::time::timeout(PER_CALL_TIMEOUT, self.source.block_logs(number)).await;
            metrics::record_shared_tip_logs_request(&self.network, request_status(&answer));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let retry_allowed = attempt < MAX_ATTEMPTS && remaining > Duration::ZERO;
            let error_retry_allowed = retry_allowed && error_attempts < MAX_ERROR_ATTEMPTS;
            // The final attempt is issued at the deadline whatever the call latency was.
            let pause = if attempt + 1 == MAX_ATTEMPTS {
                remaining
            } else {
                retry_delay(attempt).min(remaining)
            };
            match self.judge(answer, hash, retry_allowed, error_retry_allowed) {
                Verdict::Ready { logs, outcome } => {
                    let count = logs.len();
                    let status = TipStatus::Ready { logs: Arc::new(logs), attempts: attempt };
                    if self.settle(number, hash, first_seen, status, outcome) {
                        if empty_retries > 0 {
                            metrics::record_shared_tip_logs_recovered(&self.network);
                        }
                        debug!(
                            "shared tip logs - {} - block {} ({}) ready with {} logs after {} attempt(s) in {} ms",
                            self.network,
                            number,
                            hash,
                            count,
                            attempt,
                            first_seen.elapsed().as_millis()
                        );
                    }
                    return;
                }
                Verdict::Retry { empty } => {
                    if empty {
                        empty_retries += 1;
                        metrics::record_shared_tip_logs_empty_retry(&self.network);
                    } else {
                        error_attempts += 1;
                    }
                    self.store_status(number, hash, TipStatus::Pending { attempts: attempt });
                    tokio::time::sleep(pause).await;
                }
                Verdict::GaveUp => {
                    let status = TipStatus::GaveUp { attempts: attempt };
                    if self.settle(number, hash, first_seen, status, "gave_up") {
                        warn!(
                            "shared tip logs - {} - block {} ({}) still answered empty after {} unfiltered eth_getLogs over {} ms; streams fall back to their own eth_getLogs for it (compare the block against another RPC)",
                            self.network,
                            number,
                            hash,
                            attempt,
                            first_seen.elapsed().as_millis()
                        );
                    }
                    return;
                }
                Verdict::Failed { error, outcome } => {
                    let status = TipStatus::Failed { attempts: attempt, error: error.clone() };
                    if self.settle(number, hash, first_seen, status, outcome) {
                        warn!(
                            "shared tip logs - {} - block {} ({}) failed after {} unfiltered eth_getLogs over {} ms: {}; streams fall back to their own eth_getLogs for it",
                            self.network,
                            number,
                            hash,
                            attempt,
                            first_seen.elapsed().as_millis(),
                            error
                        );
                    }
                    return;
                }
            }
        }
    }

    fn judge(
        &self,
        answer: Result<Result<Vec<Log>, ProviderError>, Elapsed>,
        hash: B256,
        retry_allowed: bool,
        error_retry_allowed: bool,
    ) -> Verdict {
        match answer {
            Ok(Ok(logs)) if logs.is_empty() && !self.bloom_trusted => {
                Verdict::Ready { logs, outcome: "empty_unverified" }
            }
            Ok(Ok(logs)) if logs.is_empty() => {
                if retry_allowed {
                    Verdict::Retry { empty: true }
                } else {
                    Verdict::GaveUp
                }
            }
            // Another upstream (or pod) still serves a different block at this height: ask
            // again before giving the block up to per-stream calls.
            Ok(Ok(logs)) if logs.iter().any(|log| log.block_hash != Some(hash)) => {
                if error_retry_allowed {
                    Verdict::Retry { empty: false }
                } else {
                    Verdict::Failed { error: "hash mismatch".to_string(), outcome: "hash_mismatch" }
                }
            }
            Ok(Ok(mut logs)) => {
                logs.sort_by_key(|log| log.log_index);
                Verdict::Ready { logs, outcome: "ready" }
            }
            Ok(Err(_)) | Err(_) if error_retry_allowed => Verdict::Retry { empty: false },
            Ok(Err(error)) => Verdict::Failed { error: error.to_string(), outcome: "error" },
            Err(_) => Verdict::Failed {
                error: format!("no answer within {} ms", PER_CALL_TIMEOUT.as_millis()),
                outcome: "error",
            },
        }
    }

    /// The logs of `[from, to)` oldest first when every block is cached, `Ready` and linked by
    /// parent hash up to the tip.
    fn linked_ready_prefix(
        state: &mut TipState,
        from: u64,
        to: u64,
        tip_parent_hash: B256,
    ) -> Option<Vec<Arc<Vec<Log>>>> {
        let mut prefix = Vec::new();
        let mut expected_hash = tip_parent_hash;
        for number in (from..to).rev() {
            let entry = state.entries.get(&number).filter(|entry| entry.hash == expected_hash)?;
            let TipStatus::Ready { logs, .. } = &entry.status else {
                return None;
            };
            prefix.push(Arc::clone(logs));
            expected_hash = entry.parent_hash;
        }
        prefix.reverse();
        Some(prefix)
    }

    /// The oldest block of `[from, to)` still being fetched, with its `first_seen`.
    fn oldest_pending(state: &TipState, from: u64, to: u64) -> Option<(u64, Instant)> {
        state
            .entries
            .iter()
            .filter(|(number, entry)| (from..to).contains(*number) && entry.is_pending())
            .map(|(number, entry)| (*number, entry.first_seen))
            .min_by_key(|(number, _)| *number)
    }

    fn lock(&self) -> MutexGuard<'_, TipState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn insert(&self, state: &mut TipState, entry: TipBlockEntry) {
        let number = entry.number;
        if let Some((evicted_number, evicted)) = state.entries.push(number, entry) {
            if evicted_number != number && evicted.is_pending() {
                metrics::record_shared_tip_logs_block(&self.network, "invalidated");
            }
        }
        self.update_gauge(state);
    }

    /// Drops every entry from `from` up; a dropped `Pending` block is its terminal outcome.
    fn drop_from(&self, state: &mut TipState, from: u64) {
        let doomed: Vec<u64> = state
            .entries
            .iter()
            .map(|(number, _)| *number)
            .filter(|number| *number >= from)
            .collect();
        for number in doomed {
            if let Some(entry) = state.entries.pop(&number) {
                debug!(
                    "shared tip logs - {} - dropping block {} ({}) from the cache",
                    self.network, entry.number, entry.hash
                );
                if entry.is_pending() {
                    metrics::record_shared_tip_logs_block(&self.network, "invalidated");
                }
            }
        }
        self.update_gauge(state);
    }

    fn first_seen(&self, number: u64, hash: B256) -> Option<Instant> {
        self.lock()
            .entries
            .peek(&number)
            .filter(|entry| entry.hash == hash)
            .map(|entry| entry.first_seen)
    }

    fn is_current(&self, number: u64, hash: B256) -> bool {
        self.first_seen(number, hash).is_some()
    }

    /// Writes `status` when the entry is still the block the task was spawned for.
    fn store_status(&self, number: u64, hash: B256, status: TipStatus) -> bool {
        let mut state = self.lock();
        match state.entries.get_mut(&number) {
            Some(entry) if entry.hash == hash => {
                entry.status = status;
                true
            }
            _ => false,
        }
    }

    /// Stores a terminal state and records it; a block dropped under the task stores nothing.
    fn settle(
        &self,
        number: u64,
        hash: B256,
        first_seen: Instant,
        status: TipStatus,
        outcome: &str,
    ) -> bool {
        let stored = self.store_status(number, hash, status);
        if stored {
            metrics::record_shared_tip_logs_block(&self.network, outcome);
            metrics::record_shared_tip_logs_fetch_seconds(
                &self.network,
                first_seen.elapsed().as_secs_f64(),
            );
        }
        stored
    }

    fn update_gauge(&self, state: &TipState) {
        metrics::set_shared_tip_logs_cache_blocks(&self.network, state.entries.len());
    }

    fn record_served(&self, mode: &str) {
        metrics::record_shared_tip_logs_served(&self.network, mode);
    }

    #[cfg(test)]
    fn status_of(&self, number: u64) -> Option<TipStatus> {
        self.lock().entries.peek(&number).map(|entry| entry.status.clone())
    }
}

fn request_status(answer: &Result<Result<Vec<Log>, ProviderError>, Elapsed>) -> &'static str {
    match answer {
        Ok(Ok(_)) => "success",
        Ok(Err(_)) => "error",
        Err(_) => "timeout",
    }
}

/// 250 ms, 500 ms, 750 ms, then 1 s between attempts.
fn retry_delay(attempt: u32) -> Duration {
    Duration::from_millis(250 * u64::from(attempt)).min(Duration::from_secs(1))
}

/// The logs of `logs` that the stream's own `eth_getLogs` would have returned: the filter's
/// block range, topic0 and indexed topics, and the `addresses` snapshot the live loop took for
/// this window. `Some(empty)` is a factory with no known child, which `get_logs` answers with no
/// call at all. Order is kept and `removed` logs stay so the stream's reorg arm still sees them.
pub fn filter_logs_for_stream(
    logs: &[Log],
    addresses: &Option<HashSet<Address>>,
    filter: &RindexerEventFilter,
) -> Vec<Log> {
    let rpc_filter = Filter::new()
        .event_signature(filter.event_signature())
        .topic1(filter.topic1())
        .topic2(filter.topic2())
        .topic3(filter.topic3())
        .from_block(filter.from_block())
        .to_block(filter.to_block());
    let rpc_filter = match addresses {
        Some(addresses) if addresses.is_empty() => return Vec::new(),
        Some(addresses) => {
            rpc_filter.address(ValueOrArray::Array(addresses.iter().copied().collect()))
        }
        None => rpc_filter,
    };
    logs.iter().filter(|log| rpc_filter.rpc_matches(log)).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::contract_setup::{AddressDetails, FilterDetails};
    use crate::manifest::contract::EventInputIndexedFilters;
    use crate::metrics::definitions::{
        SHARED_TIP_LOGS_BLOCKS_TOTAL, SHARED_TIP_LOGS_CACHE_BLOCKS,
        SHARED_TIP_LOGS_EMPTY_RETRIES_TOTAL, SHARED_TIP_LOGS_RECOVERED_TOTAL,
        SHARED_TIP_LOGS_REQUESTS_TOTAL, SHARED_TIP_LOGS_SERVED_TOTAL,
    };
    use crate::provider::mock::MockChainProvider;
    use crate::provider::ChainProvider;
    use alloy::primitives::{Bytes, Log as PrimitiveLog, LogData, U256, U64};
    use futures::FutureExt;
    use prometheus::CounterVec;
    use std::collections::{HashMap, VecDeque};
    use tracing_subscriber::fmt::MakeWriter;

    #[derive(Debug, Clone)]
    enum Answer {
        Now(Result<Vec<Log>, String>),
        After(Duration, Result<Vec<Log>, String>),
    }

    /// Scripted answers per block; an exhausted queue repeats its last answer.
    #[derive(Debug, Default)]
    struct ScriptedBlockLogsSource {
        answers: Mutex<HashMap<u64, VecDeque<Answer>>>,
        calls: Mutex<Vec<(u64, Instant)>>,
    }

    impl ScriptedBlockLogsSource {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn script(&self, block: u64, answers: Vec<Answer>) {
            self.answers.lock().unwrap().insert(block, answers.into());
        }

        fn calls(&self) -> Vec<u64> {
            self.calls.lock().unwrap().iter().map(|(block, _)| *block).collect()
        }

        fn call_offsets_ms(&self, start: Instant) -> Vec<u128> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|(_, at)| at.duration_since(start).as_millis())
                .collect()
        }

        fn next_answer(&self, block: u64) -> Answer {
            let mut answers = self.answers.lock().unwrap();
            let queue = answers.get_mut(&block).expect("no scripted answer for the block");
            if queue.len() > 1 {
                queue.pop_front().expect("queue is not empty")
            } else {
                queue.front().cloned().expect("a scripted block keeps its last answer")
            }
        }
    }

    #[async_trait]
    impl BlockLogsSource for ScriptedBlockLogsSource {
        async fn block_logs(&self, block: u64) -> Result<Vec<Log>, ProviderError> {
            self.calls.lock().unwrap().push((block, Instant::now()));
            let (delay, answer) = match self.next_answer(block) {
                Answer::Now(answer) => (Duration::ZERO, answer),
                Answer::After(delay, answer) => (delay, answer),
            };
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            answer.map_err(ProviderError::CustomError)
        }
    }

    /// Thread-local tracing capture; the test runtime is current-thread, so the fetch tasks log
    /// through it too.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

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
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
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
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn hash(seed: u8) -> B256 {
        B256::repeat_byte(seed)
    }

    fn bloom_nonzero() -> Bloom {
        Bloom::repeat_byte(0x01)
    }

    fn topic(value: u64) -> B256 {
        B256::from(U256::from(value))
    }

    fn settings(bloom_trusted: bool) -> SharedTipLogsSettings {
        SharedTipLogsSettings { empty_retry_deadline_ms: 7000, cache_blocks: 32, bloom_trusted }
    }

    fn log_at(
        block: u64,
        block_hash: B256,
        address: Address,
        topics: Vec<B256>,
        index: u64,
    ) -> Log {
        Log {
            inner: PrimitiveLog { address, data: LogData::new_unchecked(topics, Bytes::new()) },
            block_hash: Some(block_hash),
            block_number: Some(block),
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: Some(index),
            removed: false,
        }
    }

    fn counter(series: &CounterVec, labels: &[&str]) -> f64 {
        series.with_label_values(labels).get()
    }

    fn empty() -> Arc<Vec<Log>> {
        Arc::new(Vec::new())
    }

    /// Lets spawned fetch tasks run up to their next sleep without moving the paused clock.
    async fn settle_tasks() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    /// Observes a block that schedules a fetch and waits for that fetch task to exit. The task
    /// cannot run before the waiter is enabled: the runtime is current-thread.
    async fn observe_and_settle(
        tip: &SharedTipLogs,
        number: u64,
        block_hash: B256,
        parent_hash: B256,
        bloom: Bloom,
    ) {
        tip.observe_head(number, block_hash, parent_hash, bloom);
        let notified = tip.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        notified.await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_bloom_block_is_ready_without_rpc() {
        let net = "tip-logs-zero-bloom";
        let source = ScriptedBlockLogsSource::new();
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));

        tip.observe_head(10, hash(10), hash(9), Bloom::ZERO);
        settle_tasks().await;

        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::ServeTip(empty()));
        assert!(source.calls().is_empty());
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "empty_bloom_zero"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "tip"]), 1.0);
        assert_eq!(SHARED_TIP_LOGS_CACHE_BLOCKS.with_label_values(&[net]).get(), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn fresh_block_empty_then_nonempty_is_recovered() {
        let net = "tip-logs-recovered";
        let source = ScriptedBlockLogsSource::new();
        let log = log_at(10, hash(10), Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(
            10,
            vec![
                Answer::Now(Ok(vec![])),
                Answer::Now(Ok(vec![])),
                Answer::Now(Ok(vec![log.clone()])),
            ],
        );
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));
        let start = Instant::now();

        let notified = tip.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        tip.observe_head(10, hash(10), hash(9), bloom_nonzero());
        settle_tasks().await;

        assert_eq!(
            tip.lookup(10, 10, hash(10)),
            TipLookup::Wait { oldest_pending: 10, first_seen: start }
        );
        assert_eq!(tip.status_of(10), Some(TipStatus::Pending { attempts: 1 }));

        notified.await;

        assert_eq!(start.elapsed(), Duration::from_millis(750), "woken at the Ready transition");
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::ServeTip(Arc::new(vec![log])));
        assert_eq!(
            tip.status_of(10).map(|status| matches!(status, TipStatus::Ready { attempts: 3, .. })),
            Some(true)
        );
        assert_eq!(source.calls(), vec![10, 10, 10]);
        assert_eq!(source.call_offsets_ms(start), vec![0, 250, 750]);
        assert_eq!(counter(&SHARED_TIP_LOGS_EMPTY_RETRIES_TOTAL, &[net]), 2.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_RECOVERED_TOTAL, &[net]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "success"]), 3.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn empty_past_budget_gives_up() {
        let net = "tip-logs-gave-up";
        let (logs, _guard) = CapturedLogs::install();
        let source = ScriptedBlockLogsSource::new();
        source.script(10, vec![Answer::Now(Ok(vec![]))]);
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));
        let start = Instant::now();

        observe_and_settle(&tip, 10, hash(10), hash(9), bloom_nonzero()).await;

        assert_eq!(
            source.call_offsets_ms(start),
            vec![0, 250, 750, 1500, 2500, 3500, 4500, 7000],
            "the eighth attempt lands at the deadline"
        );
        assert_eq!(tip.status_of(10), Some(TipStatus::GaveUp { attempts: 8 }));
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::Fallback(FallbackReason::GaveUp));
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "gave_up"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_EMPTY_RETRIES_TOTAL, &[net]), 7.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "success"]), 8.0);
        let warning = logs.text();
        assert!(warning.contains("WARN"), "a WARN line is logged: {warning}");
        assert!(warning.contains("block 10"), "the WARN names the block: {warning}");
        assert!(warning.contains(&hash(10).to_string()), "the WARN names the hash: {warning}");
    }

    #[tokio::test(start_paused = true)]
    async fn bloomless_network_fetches_every_block_and_accepts_empty() {
        let net = "tip-logs-bloomless";
        let source = ScriptedBlockLogsSource::new();
        source.script(10, vec![Answer::Now(Ok(vec![]))]);
        let tip = SharedTipLogs::new(net, source.clone(), settings(false));

        observe_and_settle(&tip, 10, hash(10), hash(9), Bloom::ZERO).await;

        assert_eq!(source.calls(), vec![10]);
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::ServeTip(empty()));
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "empty_unverified"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "empty_bloom_zero"]), 0.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_EMPTY_RETRIES_TOTAL, &[net]), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn hash_mismatch_is_never_served() {
        let net = "tip-logs-hash-mismatch";
        let source = ScriptedBlockLogsSource::new();
        let foreign = log_at(10, hash(0xee), Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(10, vec![Answer::Now(Ok(vec![foreign]))]);
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));

        observe_and_settle(&tip, 10, hash(10), hash(9), bloom_nonzero()).await;

        assert_eq!(
            tip.status_of(10),
            Some(TipStatus::Failed { attempts: 4, error: "hash mismatch".to_string() }),
            "a mismatched answer is re-asked three times, then failed"
        );
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::Fallback(FallbackReason::Error));
        assert_eq!(source.calls(), vec![10, 10, 10, 10]);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "hash_mismatch"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 0.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "tip"]), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn errors_and_timeouts_retry_then_fail() {
        let net = "tip-logs-errors";
        let source = ScriptedBlockLogsSource::new();
        let log = log_at(10, hash(10), Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(
            10,
            vec![
                Answer::Now(Err("boom".to_string())),
                Answer::After(Duration::from_secs(6), Ok(vec![])),
                Answer::Now(Ok(vec![log.clone()])),
            ],
        );
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));
        let start = Instant::now();

        observe_and_settle(&tip, 10, hash(10), hash(9), bloom_nonzero()).await;

        assert_eq!(
            source.call_offsets_ms(start),
            vec![0, 250, 5750],
            "timeout at 5 s, then 500 ms"
        );
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::ServeTip(Arc::new(vec![log])));
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "error"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "timeout"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "success"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_RECOVERED_TOTAL, &[net]), 0.0, "no empty retry");

        source.script(11, vec![Answer::Now(Err("boom".to_string()))]);
        let start = Instant::now();
        observe_and_settle(&tip, 11, hash(11), hash(10), bloom_nonzero()).await;

        assert_eq!(
            start.elapsed(),
            Duration::from_millis(1500),
            "the call and three re-asks at 0, 250, 750 and 1500 ms, then failed without using the budget"
        );
        assert_eq!(
            tip.status_of(11),
            Some(TipStatus::Failed { attempts: 4, error: "Unknown error: boom".to_string() })
        );
        assert_eq!(tip.lookup(11, 11, hash(11)), TipLookup::Fallback(FallbackReason::Error));
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "error"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_REQUESTS_TOTAL, &[net, "error"]), 5.0);
    }

    #[tokio::test(start_paused = true)]
    async fn invalidate_from_drops_entries_and_notifies() {
        let net = "tip-logs-invalidate";
        let source = ScriptedBlockLogsSource::new();
        let log = log_at(10, hash(10), Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(10, vec![Answer::After(Duration::from_secs(2), Ok(vec![log]))]);
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));

        tip.observe_head(10, hash(10), hash(9), bloom_nonzero());
        settle_tasks().await;
        assert!(matches!(tip.lookup(10, 10, hash(10)), TipLookup::Wait { oldest_pending: 10, .. }));

        let first_waiter = tip.notified();
        tokio::pin!(first_waiter);
        first_waiter.as_mut().enable();

        tip.invalidate_from(10);

        assert!(first_waiter.now_or_never().is_some(), "invalidation wakes the waiter");
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::Fallback(FallbackReason::NotScheduled));
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "invalidated"]), 1.0);
        assert_eq!(SHARED_TIP_LOGS_CACHE_BLOCKS.with_label_values(&[net]).get(), 0.0);

        let second_waiter = tip.notified();
        tokio::pin!(second_waiter);
        second_waiter.as_mut().enable();

        // Release the delayed answer: the task finds its block gone and exits without storing.
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(second_waiter.now_or_never().is_some(), "the exit guard wakes the waiter");
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::Fallback(FallbackReason::NotScheduled));
        assert_eq!(source.calls(), vec![10]);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "invalidated"]), 1.0);
        assert_eq!(counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "ready"]), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn same_height_replacement_and_parent_mismatch_drop_stale_entries() {
        let net = "tip-logs-replacement";
        let source = ScriptedBlockLogsSource::new();
        let (h1, h2) = (hash(0x11), hash(0x12));
        let log_h1 = log_at(10, h1, Address::repeat_byte(1), vec![topic(1)], 0);
        let log_h2 = log_at(10, h2, Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(
            10,
            vec![Answer::Now(Ok(vec![log_h1.clone()])), Answer::Now(Ok(vec![log_h2.clone()]))],
        );
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));

        observe_and_settle(&tip, 10, h1, hash(9), bloom_nonzero()).await;
        assert_eq!(tip.lookup(10, 10, h1), TipLookup::ServeTip(Arc::new(vec![log_h1])));

        observe_and_settle(&tip, 10, h2, hash(9), bloom_nonzero()).await;
        assert_eq!(tip.lookup(10, 10, h1), TipLookup::Fallback(FallbackReason::HashMismatch));
        assert_eq!(tip.lookup(10, 10, h2), TipLookup::ServeTip(Arc::new(vec![log_h2])));
        assert_eq!(source.calls(), vec![10, 10], "the replacement is fetched again");

        tip.observe_head(11, hash(11), h1, Bloom::ZERO);
        settle_tasks().await;

        assert_eq!(tip.lookup(10, 10, h2), TipLookup::Fallback(FallbackReason::NotScheduled));
        assert_eq!(tip.lookup(11, 11, hash(11)), TipLookup::ServeTip(empty()));
        assert_eq!(tip.lookup(10, 11, hash(11)), TipLookup::ServePrefixByRpcPlusTip(empty()));
        assert_eq!(
            counter(&SHARED_TIP_LOGS_BLOCKS_TOTAL, &[net, "invalidated"]),
            0.0,
            "only Ready entries were dropped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn window_lookup_rules() {
        let net = "tip-logs-window";
        let source = ScriptedBlockLogsSource::new();
        let tip = SharedTipLogs::new(net, source.clone(), settings(true));

        for number in 10..=12 {
            tip.observe_head(number, hash(number as u8), hash(number as u8 - 1), Bloom::ZERO);
        }
        assert_eq!(
            tip.lookup(10, 12, hash(12)),
            TipLookup::ServeWindow(vec![empty(), empty(), empty()])
        );
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "window"]), 1.0);
        assert_eq!(
            tip.lookup(10, 12, hash(0xff)),
            TipLookup::Fallback(FallbackReason::HashMismatch)
        );
        assert_eq!(
            tip.lookup(9, 12, hash(12)),
            TipLookup::ServePrefixByRpcPlusTip(empty()),
            "9 was never observed"
        );

        // 14 is observed before 13, and the 13 that arrives is not the parent 14 named.
        tip.observe_head(14, hash(14), hash(13), Bloom::ZERO);
        tip.observe_head(13, hash(0x33), hash(12), Bloom::ZERO);
        assert_eq!(tip.lookup(12, 14, hash(14)), TipLookup::ServePrefixByRpcPlusTip(empty()));
        assert_eq!(tip.lookup(13, 14, hash(14)), TipLookup::ServePrefixByRpcPlusTip(empty()));
        assert_eq!(counter(&SHARED_TIP_LOGS_SERVED_TOTAL, &[net, "prefix_rpc_plus_tip"]), 3.0);

        let log = log_at(20, hash(20), Address::repeat_byte(1), vec![topic(1)], 0);
        source.script(20, vec![Answer::After(Duration::from_secs(2), Ok(vec![log.clone()]))]);
        let first_seen = Instant::now();
        tip.observe_head(20, hash(20), hash(19), bloom_nonzero());
        tip.observe_head(21, hash(21), hash(20), Bloom::ZERO);
        settle_tasks().await;

        assert_eq!(
            tip.lookup(20, 21, hash(21)),
            TipLookup::Wait { oldest_pending: 20, first_seen }
        );
        assert_eq!(
            tip.lookup(19, 21, hash(21)),
            TipLookup::Wait { oldest_pending: 20, first_seen },
            "a pending block wins over a missing one"
        );

        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(
            tip.lookup(20, 21, hash(21)),
            TipLookup::ServeWindow(vec![Arc::new(vec![log]), empty()])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lru_cap_holds() {
        let net = "tip-logs-lru";
        let source = ScriptedBlockLogsSource::new();
        let settings = SharedTipLogsSettings { cache_blocks: 4, ..settings(true) };
        let tip = SharedTipLogs::new(net, source.clone(), settings);

        for number in 1..=10u8 {
            tip.observe_head(u64::from(number), hash(number), hash(number - 1), Bloom::ZERO);
        }

        assert_eq!(SHARED_TIP_LOGS_CACHE_BLOCKS.with_label_values(&[net]).get(), 4.0);
        assert_eq!(tip.lookup(1, 1, hash(1)), TipLookup::Fallback(FallbackReason::NotScheduled));
        assert_eq!(tip.lookup(6, 6, hash(6)), TipLookup::Fallback(FallbackReason::NotScheduled));
        assert_eq!(tip.lookup(10, 10, hash(10)), TipLookup::ServeTip(empty()));
        assert_eq!(tip.lookup(7, 10, hash(10)), TipLookup::ServeWindow(vec![empty(); 4]));
        assert_eq!(tip.lookup(6, 10, hash(10)), TipLookup::ServePrefixByRpcPlusTip(empty()));
        assert!(source.calls().is_empty());
    }

    #[tokio::test]
    async fn filter_logs_for_stream_matches_get_logs() {
        let block_hash = hash(10);
        let (topic0, other0) = (hash(0xa0), hash(0xb0));
        let (a1, a2, a3) =
            (Address::repeat_byte(1), Address::repeat_byte(2), Address::repeat_byte(3));
        let mut removed = log_at(10, block_hash, a1, vec![topic0, topic(1), topic(2), topic(3)], 5);
        removed.removed = true;
        let logs = vec![
            log_at(10, block_hash, a1, vec![topic0, topic(1), topic(2), topic(3)], 0),
            log_at(10, block_hash, a2, vec![topic0, topic(1)], 1),
            log_at(10, block_hash, a3, vec![other0, topic(1), topic(2), topic(3)], 2),
            log_at(10, block_hash, a1, vec![topic0, topic(9), topic(2), topic(3)], 3),
            log_at(10, block_hash, a2, vec![topic0], 4),
            removed,
        ];
        let pick = |indexes: &[usize]| indexes.iter().map(|i| logs[*i].clone()).collect::<Vec<_>>();
        let oracle = MockChainProvider::new(1).with_logs(logs.clone());
        let block = U64::from(10);

        let by_address = RindexerEventFilter::new_address_filter(
            &topic0,
            "Ev",
            &AddressDetails { address: ValueOrArray::Array(vec![a1, a2]), indexed_filters: None },
            block,
            block,
        )
        .unwrap();
        let addresses = by_address.contract_addresses().await;
        assert_eq!(addresses.as_ref().map(HashSet::len), Some(2));
        let expected = oracle.get_logs(&by_address).await.unwrap();
        assert_eq!(
            expected,
            pick(&[0, 1, 3, 4, 5]),
            "address and topic0, order kept, removed kept"
        );
        assert_eq!(filter_logs_for_stream(&logs, &addresses, &by_address), expected);
        assert!(
            filter_logs_for_stream(&logs, &Some(HashSet::new()), &by_address).is_empty(),
            "Some(empty) is no logs"
        );

        let indexed = |i1: Option<Vec<u64>>, i2: Option<Vec<u64>>, i3: Option<Vec<u64>>| {
            let strings = |values: Vec<u64>| values.iter().map(u64::to_string).collect::<Vec<_>>();
            EventInputIndexedFilters {
                event_name: "Ev".to_string(),
                indexed_1: i1.map(strings),
                indexed_2: i2.map(strings),
                indexed_3: i3.map(strings),
            }
        };
        let any_address = |indexed_filters: Option<EventInputIndexedFilters>| {
            RindexerEventFilter::new_filter(
                &topic0,
                "Ev",
                &FilterDetails { events: ValueOrArray::Value("Ev".to_string()), indexed_filters },
                block,
                block,
            )
            .unwrap()
        };

        let unconstrained = any_address(None);
        let expected = oracle.get_logs(&unconstrained).await.unwrap();
        assert_eq!(expected, pick(&[0, 1, 3, 4, 5]));
        assert_eq!(
            filter_logs_for_stream(&logs, &None, &unconstrained),
            expected,
            "None is any address"
        );

        let single = any_address(Some(indexed(Some(vec![1]), None, None)));
        assert_eq!(filter_logs_for_stream(&logs, &None, &single), pick(&[0, 1, 5]));

        let several = any_address(Some(indexed(Some(vec![1, 9]), None, None)));
        assert_eq!(filter_logs_for_stream(&logs, &None, &several), pick(&[0, 1, 3, 5]));

        let deep = any_address(Some(indexed(None, Some(vec![2]), Some(vec![3]))));
        assert_eq!(
            filter_logs_for_stream(&logs, &None, &deep),
            pick(&[0, 3, 5]),
            "a constrained position with no log topic is a miss"
        );

        let all_three = any_address(Some(indexed(Some(vec![1]), Some(vec![2]), Some(vec![3]))));
        assert_eq!(filter_logs_for_stream(&logs, &None, &all_three), pick(&[0, 5]));
        let addresses = Some(HashSet::from([a2]));
        assert!(filter_logs_for_stream(&logs, &addresses, &all_three).is_empty());

        let other_block =
            any_address(None).set_from_block(U64::from(11)).set_to_block(U64::from(11));
        assert!(
            filter_logs_for_stream(&logs, &None, &other_block).is_empty(),
            "the block range is honoured"
        );
    }

    #[test]
    fn resolve_settings_follows_manifest() {
        use crate::manifest::network::SharedTipLogsConfig;

        let mut network: Network = serde_yaml::from_str(
            r#"
            name: polygon
            chain_id: 137
            rpc: https://polygon.example.com
            "#,
        )
        .unwrap();

        let settings = SharedTipLogsSettings::resolve(&network).expect("enabled by default");
        assert_eq!(settings.empty_retry_deadline_ms, 7000);
        assert_eq!(settings.cache_blocks, 32);
        assert!(settings.bloom_trusted);

        network.disable_logs_bloom_checks = Some(true);
        network.shared_tip_logs = Some(SharedTipLogsConfig {
            empty_retry_deadline_ms: 1500,
            cache_blocks: 8,
            ..SharedTipLogsConfig::default()
        });
        let settings = SharedTipLogsSettings::resolve(&network).expect("still enabled");
        assert_eq!(settings.empty_retry_deadline_ms, 1500);
        assert_eq!(settings.cache_blocks, 8);
        assert!(!settings.bloom_trusted);

        network.shared_tip_logs =
            Some(SharedTipLogsConfig { enabled: false, ..SharedTipLogsConfig::default() });
        assert!(SharedTipLogsSettings::resolve(&network).is_none());
    }
}
