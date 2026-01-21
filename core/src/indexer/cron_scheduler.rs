//! Cron scheduler for table operations.
//!
//! This module provides scheduled (cron) triggers for custom tables,
//! allowing operations to run on a time-based schedule instead of
//! (or in addition to) event triggers.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::indexer::last_synced::{
    get_last_synced_cron_block, update_last_synced_cron_block, CronSyncConfig,
};
use crate::is_running;
use crate::manifest::contract::{parse_interval, ColumnType, Table, TableCronMapping};
use crate::manifest::core::Manifest;
use crate::provider::JsonRpcCachedProvider;
use alloy::primitives::U64;

use super::tables::{TableRowData, TxMetadata};

// ============================================================================
// Adaptive Rate Limiter - Scales both concurrency AND batch size
// ============================================================================

/// Maximum concurrent RPC calls. Paid nodes handle 100+ easily.
const MAX_RPC_CONCURRENCY: usize = 100;

/// Starting concurrent RPC calls. Start tiny so free nodes can complete.
const INITIAL_RPC_CONCURRENCY: usize = 2;

/// Minimum concurrent RPC calls.
const MIN_RPC_CONCURRENCY: usize = 1;

/// Maximum batch size (blocks per batch). Enterprise nodes work great at 1000+.
const MAX_BATCH_SIZE: u64 = 1000;

/// Starting batch size. Start small so free nodes complete quickly.
const INITIAL_BATCH_SIZE: u64 = 50;

/// Minimum batch size.
const MIN_BATCH_SIZE: u64 = 25;

/// Delay between batches when rate limited (milliseconds).
const RATE_LIMIT_DELAY_MS: u64 = 1000;

/// Expected average RPC call latency in milliseconds (for healthy nodes).
/// Used to detect rate limiting by timing.
const EXPECTED_CALL_LATENCY_MS: u64 = 150;

/// If batch takes longer than this multiplier of expected time, it's rate limited.
const RATE_LIMIT_TIME_MULTIPLIER: f64 = 3.0;

/// Number of successful batches required before scaling up after a rate limit event.
/// Prevents oscillation between scaling up and down.
const COOLDOWN_BATCHES_AFTER_RATE_LIMIT: u32 = 5;

/// Adaptive rate limiter that adjusts BOTH concurrency AND batch size.
///
/// Detects rate limiting by TIMING - if a batch takes much longer than expected
/// (based on batch_size / concurrency * expected_latency), we're being throttled.
/// This works even when the provider retries internally and eventually succeeds.
pub struct AdaptiveRateLimiter {
    /// Current concurrency level.
    concurrency: AtomicU32,
    /// Current batch size.
    batch_size: AtomicU64,
    /// Whether we've logged the initial throttle warning.
    logged_throttle_warning: AtomicU32,
    /// Cooldown counter - must be 0 before we can scale up again.
    /// Prevents oscillation after rate limiting.
    cooldown_remaining: AtomicU32,
}

impl AdaptiveRateLimiter {
    /// Create a new adaptive rate limiter starting small.
    pub fn new() -> Self {
        Self {
            concurrency: AtomicU32::new(INITIAL_RPC_CONCURRENCY as u32),
            batch_size: AtomicU64::new(INITIAL_BATCH_SIZE),
            logged_throttle_warning: AtomicU32::new(0),
            cooldown_remaining: AtomicU32::new(0),
        }
    }

    /// Get current concurrency level.
    pub fn concurrency(&self) -> usize {
        self.concurrency.load(Ordering::Relaxed) as usize
    }

    /// Get current batch size.
    pub fn batch_size(&self) -> u64 {
        self.batch_size.load(Ordering::Relaxed)
    }

    /// Call after each batch completes with the actual duration.
    /// Detects rate limiting by timing - if batch took much longer than expected,
    /// we're being throttled (even if provider retried internally and succeeded).
    /// Returns delay before next batch.
    pub fn after_batch_with_duration(&self, actual_duration: Duration) -> Duration {
        let current_conc = self.concurrency.load(Ordering::Relaxed) as usize;
        let current_batch = self.batch_size.load(Ordering::Relaxed);

        // Calculate expected duration: (batch_size / concurrency) * call_latency
        // This is how many "waves" of concurrent calls times the latency per wave
        let waves = (current_batch as f64 / current_conc as f64).ceil();
        let expected_ms = waves * EXPECTED_CALL_LATENCY_MS as f64;
        let actual_ms = actual_duration.as_millis() as f64;

        // If actual time is much greater than expected, we're rate limited
        let is_rate_limited = actual_ms > expected_ms * RATE_LIMIT_TIME_MULTIPLIER;

        if is_rate_limited {
            // Rate limited - scale DOWN both concurrency and batch size
            let new_conc = (current_conc / 2).max(MIN_RPC_CONCURRENCY);
            let new_batch = (current_batch / 2).max(MIN_BATCH_SIZE);

            self.concurrency.store(new_conc as u32, Ordering::Relaxed);
            self.batch_size.store(new_batch, Ordering::Relaxed);

            // Set cooldown - require N successful batches before scaling up again
            self.cooldown_remaining.store(COOLDOWN_BATCHES_AFTER_RATE_LIMIT, Ordering::Relaxed);

            // Only log the first throttle warning with full context
            if self.logged_throttle_warning.swap(1, Ordering::Relaxed) == 0 {
                warn!(
                    "⚠️  RPC rate limited (batch took {:.1}s, expected {:.1}s) - scaling down to concurrency={}, batch_size={} (free nodes throttle, paid nodes scale up)",
                    actual_ms / 1000.0, expected_ms / 1000.0, new_conc, new_batch
                );
            } else {
                warn!(
                    "⚠️  Rate limited ({:.1}s vs {:.1}s expected) - concurrency={}, batch_size={} (cooldown {} batches)",
                    actual_ms / 1000.0, expected_ms / 1000.0, new_conc, new_batch, COOLDOWN_BATCHES_AFTER_RATE_LIMIT
                );
            }

            Duration::from_millis(RATE_LIMIT_DELAY_MS)
        } else {
            // Successful batch - check cooldown before scaling up
            let cooldown = self.cooldown_remaining.load(Ordering::Relaxed);

            if cooldown > 0 {
                // Still in cooldown period - don't scale up yet
                self.cooldown_remaining.store(cooldown - 1, Ordering::Relaxed);
                return Duration::ZERO;
            }

            // Cooldown complete - can scale up
            let at_max_conc = current_conc >= MAX_RPC_CONCURRENCY;
            let at_max_batch = current_batch >= MAX_BATCH_SIZE;

            if !at_max_conc || !at_max_batch {
                // Double both
                let new_conc = if !at_max_conc {
                    (current_conc * 2).min(MAX_RPC_CONCURRENCY)
                } else {
                    current_conc
                };
                let new_batch = if !at_max_batch {
                    (current_batch * 2).min(MAX_BATCH_SIZE)
                } else {
                    current_batch
                };

                self.concurrency.store(new_conc as u32, Ordering::Relaxed);
                self.batch_size.store(new_batch, Ordering::Relaxed);

                if new_conc == MAX_RPC_CONCURRENCY && new_batch == MAX_BATCH_SIZE {
                    info!(
                        "🚀 Reached maximum speed: concurrency={}, batch_size={}",
                        new_conc, new_batch
                    );
                }
            }

            Duration::ZERO
        }
    }
}

impl Default for AdaptiveRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Cron Scheduler Types
// ============================================================================

/// Enum representing the type of schedule for cron tasks.
#[derive(Debug, Clone)]
pub enum CronSchedule {
    /// Fixed interval (e.g., every 5 minutes)
    Interval(Duration),
    /// Cron expression (e.g., "*/5 * * * *")
    Cron(Box<croner::Cron>),
}

/// A single cron task configuration
#[derive(Debug, Clone)]
pub struct CronTask {
    pub contract_name: String,
    pub table: Table,
    pub full_table_name: String,
    pub cron_entry: TableCronMapping,
    pub cron_index: usize,
    pub network: String,
    pub contract_address: Address,
    pub schedule: CronSchedule,
    pub indexer_name: String,
}

/// The cron scheduler manages and executes scheduled table operations.
pub struct CronScheduler {
    pub tasks: Vec<CronTask>,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
    pub providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
}

impl CronScheduler {
    /// Create a new cron scheduler from the manifest.
    ///
    /// This scans all contracts and tables for cron configurations and
    /// builds a list of tasks to execute.
    pub fn new(
        manifest: &Manifest,
        postgres: Option<Arc<PostgresClient>>,
        clickhouse: Option<Arc<ClickhouseClient>>,
        providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
    ) -> Self {
        let mut tasks = Vec::new();

        for contract in &manifest.contracts {
            let Some(tables) = &contract.tables else {
                continue;
            };

            for table in tables {
                let Some(cron_entries) = &table.cron else {
                    continue;
                };

                for (cron_index, cron_entry) in cron_entries.iter().enumerate() {
                    // Determine which networks to run on
                    let networks: Vec<String> = if let Some(network) = &cron_entry.network {
                        vec![network.clone()]
                    } else {
                        contract.details.iter().map(|d| d.network.clone()).collect()
                    };

                    // Parse the schedule (only required for live cron, not historical sync)
                    // Historical sync with start_block doesn't need interval/schedule
                    let schedule = if cron_entry.start_block.is_some()
                        && cron_entry.interval.is_none()
                        && cron_entry.schedule.is_none()
                    {
                        // Historical sync only - use a dummy schedule (won't be used for live)
                        CronSchedule::Interval(Duration::from_secs(60))
                    } else if let Some(interval_str) = &cron_entry.interval {
                        match parse_interval(interval_str) {
                            Ok(duration) => CronSchedule::Interval(duration),
                            Err(e) => {
                                error!(
                                    "Invalid interval '{}' for table '{}': {}",
                                    interval_str, table.name, e
                                );
                                continue;
                            }
                        }
                    } else if let Some(cron_expr) = &cron_entry.schedule {
                        match croner::Cron::new(cron_expr).parse() {
                            Ok(cron) => CronSchedule::Cron(Box::new(cron)),
                            Err(e) => {
                                error!(
                                    "Invalid cron expression '{}' for table '{}': {}",
                                    cron_expr, table.name, e
                                );
                                continue;
                            }
                        }
                    } else {
                        error!(
                            "Cron entry for table '{}' has neither interval nor schedule",
                            table.name
                        );
                        continue;
                    };

                    for network in networks {
                        // Get contract address for this network
                        let contract_address = contract
                            .details
                            .iter()
                            .find(|d| d.network == network)
                            .and_then(|d| d.address.as_ref())
                            .and_then(|addr| match addr {
                                alloy::rpc::types::ValueOrArray::Value(a) => Some(*a),
                                alloy::rpc::types::ValueOrArray::Array(arr) => arr.first().copied(),
                            })
                            .unwrap_or_default();

                        let full_table_name = crate::database::generate::generate_table_full_name(
                            &manifest.name,
                            &contract.name,
                            &table.name,
                        );

                        tasks.push(CronTask {
                            contract_name: contract.name.clone(),
                            table: table.clone(),
                            full_table_name,
                            cron_entry: cron_entry.clone(),
                            cron_index,
                            network,
                            contract_address,
                            schedule: schedule.clone(),
                            indexer_name: manifest.name.clone(),
                        });
                    }
                }
            }
        }

        info!("Cron scheduler initialized with {} tasks", tasks.len());

        Self { tasks, postgres, clickhouse, providers }
    }

    /// Check if there are any cron tasks to run.
    pub fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }

    /// Start the cron scheduler - runs until shutdown.
    ///
    /// This spawns a task for each cron configuration and waits for all to complete.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.tasks.is_empty() {
            info!("No cron tasks configured, scheduler exiting");
            return Ok(());
        }

        let postgres = self.postgres;
        let clickhouse = self.clickhouse;
        let providers = self.providers;

        let mut handles = Vec::new();

        for task in self.tasks {
            let postgres = postgres.clone();
            let clickhouse = clickhouse.clone();
            let providers = providers.clone();

            let handle = tokio::spawn(async move {
                run_cron_task(task, postgres, clickhouse, providers).await;
            });

            handles.push(handle);
        }

        // Wait for all tasks (they run until shutdown)
        futures::future::join_all(handles).await;

        info!("Cron scheduler stopped");
        Ok(())
    }
}

/// Maximum number of retry attempts for failed cron operations.
const MAX_RETRIES: u32 = 3;

/// Initial backoff delay for retries (doubles each attempt).
const INITIAL_BACKOFF_SECS: u64 = 2;

/// Run a single cron task until shutdown.
///
/// If the cron has `start_block` configured, historical sync runs first,
/// iterating through blocks from start_block (or last synced) to end_block (or latest).
/// After historical sync completes:
/// - If `end_block` was specified, the task stops (no live cron).
/// - Otherwise, live cron continues with the configured schedule.
async fn run_cron_task(
    task: CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) {
    info!(
        "Starting cron task for table '{}' on network '{}' with schedule {:?}",
        task.table.name,
        task.network,
        task.cron_entry.interval.as_ref().or(task.cron_entry.schedule.as_ref())
    );

    // Check if historical sync is needed
    if task.cron_entry.start_block.is_some() {
        let historical_completed =
            run_historical_sync(&task, postgres.clone(), clickhouse.clone(), providers.clone())
                .await;

        if !historical_completed {
            debug!(
                "Historical sync for table '{}' on network '{}' interrupted by shutdown",
                task.table.name, task.network
            );
            return;
        }

        // If end_block was specified, stop here (no live cron)
        if task.cron_entry.end_block.is_some() {
            info!(
                "Historical cron sync completed for table '{}' on network '{}' - end_block reached, stopping",
                task.table.name, task.network
            );
            return;
        }

        info!(
            "Historical cron sync completed for table '{}' on network '{}' - switching to live mode",
            task.table.name, task.network
        );
    }

    // Live cron mode
    run_live_cron_loop(&task, postgres, clickhouse, providers).await;
}

/// Run historical sync for a cron task using batched operations.
///
/// This batches multiple blocks together for efficient RPC calls via multicall3
/// and batched database writes. Returns true if completed successfully, false if interrupted.
async fn run_historical_sync(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) -> bool {
    let start_block = task.cron_entry.start_block.unwrap().to::<u64>();
    let block_interval = task.cron_entry.block_interval.map(|b| b.to::<u64>()).unwrap_or(1); // Default to every block

    // Get end block (either specified or latest)
    let provider = providers.get(&task.network);
    let end_block = if let Some(end) = task.cron_entry.end_block {
        end.to::<u64>()
    } else if let Some(p) = provider {
        p.get_block_number().await.ok().map(|b| b.to::<u64>()).unwrap_or(start_block)
    } else {
        start_block
    };

    // Get last synced block from database
    let sync_config = CronSyncConfig {
        postgres: &postgres,
        clickhouse: &clickhouse,
        indexer_name: &task.indexer_name,
        contract_name: &task.contract_name,
        table_name: &task.table.name,
        cron_index: task.cron_index,
        network: &task.network,
    };

    let last_synced = get_last_synced_cron_block(sync_config).await;

    // Determine starting point
    let mut current_block = match last_synced {
        Some(last) => {
            // Resume from last synced block + interval
            let next = last.to::<u64>() + block_interval;
            if next > end_block {
                info!(
                    "Historical cron sync for table '{}' on network '{}' already complete (last synced: {}, end: {})",
                    task.table.name, task.network, last, end_block
                );
                return true;
            }
            next
        }
        None => start_block,
    };

    info!(
        "Starting historical cron sync for table '{}' on network '{}' from block {} to {} (interval: {})",
        task.table.name, task.network, current_block, end_block, block_interval
    );

    let start_time = std::time::Instant::now();

    // Create adaptive rate limiter for this sync
    let rate_limiter = Arc::new(AdaptiveRateLimiter::new());

    // Process blocks in batches
    while current_block <= end_block {
        if !is_running() {
            return false;
        }

        // Collect blocks for this batch (batch size is dynamic based on rate limiting)
        let current_batch_size = rate_limiter.batch_size() as usize;
        let mut batch_blocks: Vec<u64> = Vec::with_capacity(current_batch_size);
        let mut block = current_block;
        while block <= end_block && batch_blocks.len() < current_batch_size {
            batch_blocks.push(block);
            block += block_interval;
        }

        if batch_blocks.is_empty() {
            break;
        }

        let batch_start = batch_blocks[0];
        let batch_end = *batch_blocks.last().unwrap();

        // Track batch timing to detect rate limiting
        let batch_start_time = std::time::Instant::now();

        // Execute batch and get last successful block
        let result = execute_cron_operations_batch(
            task,
            postgres.clone(),
            clickhouse.clone(),
            providers.clone(),
            &batch_blocks,
            rate_limiter.clone(),
        )
        .await;

        let batch_duration = batch_start_time.elapsed();

        match result {
            Ok(last_block) => {
                // Save progress after batch
                if let Some(b) = last_block {
                    let concurrency = rate_limiter.concurrency();
                    let batch_sz = rate_limiter.batch_size();
                    info!(
                        "Cron batch complete: table='{}', blocks {}-{} ({} ops), concurrency={}, batch_size={}, {:.1}s, saved at block {}",
                        task.table.name, batch_start, batch_end, batch_blocks.len(), concurrency, batch_sz, batch_duration.as_secs_f64(), b
                    );
                    update_last_synced_cron_block(
                        &postgres,
                        &clickhouse,
                        &task.indexer_name,
                        &task.contract_name,
                        &task.table.name,
                        task.cron_index,
                        &task.network,
                        U64::from(b),
                    )
                    .await;
                }
            }
            Err(e) => {
                error!(
                    "Historical cron batch failed for table '{}' on network '{}' blocks {}-{}: {}",
                    task.table.name, task.network, batch_start, batch_end, e
                );
            }
        }

        // Check timing to detect rate limiting and adjust concurrency/batch size
        let delay = rate_limiter.after_batch_with_duration(batch_duration);
        if !delay.is_zero() {
            debug!("Rate limit backoff: sleeping {:?}", delay);
            tokio::time::sleep(delay).await;
        }

        current_block = batch_end + block_interval;
    }

    let elapsed = start_time.elapsed();
    info!(
        "Historical cron sync completed for table '{}' on network '{}' in {:?}",
        task.table.name, task.network, elapsed
    );

    true
}

/// Execute cron operations for a batch of blocks with parallel RPC calls.
///
/// This executes all $call operations in parallel, then writes results to the
/// database in a batch. Returns the last successful block number.
async fn execute_cron_operations_batch(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
    blocks: &[u64],
    rate_limiter: Arc<AdaptiveRateLimiter>,
) -> Result<Option<u64>, String> {
    use futures::stream::{FuturesUnordered, StreamExt};
    use std::collections::HashSet;

    if blocks.is_empty() {
        return Ok(None);
    }

    let provider = match providers.get(&task.network) {
        Some(p) => p.clone(),
        None => return Err(format!("No provider for network '{}'", task.network)),
    };

    // Step 1: Collect all unique $call patterns and their column mappings
    let mut call_patterns: Vec<(String, String, ColumnType)> = Vec::new(); // (value_ref, column_name, column_type)

    for operation in &task.cron_entry.operations {
        for (column_name, value_ref) in &operation.where_clause {
            if value_ref.starts_with("$call(") {
                if let Some(col) = task.table.columns.iter().find(|c| &c.name == column_name) {
                    call_patterns.push((
                        value_ref.clone(),
                        column_name.clone(),
                        col.resolved_type().clone(),
                    ));
                }
            }
        }
        for set_col in &operation.set {
            let val = set_col.effective_value();
            if val.starts_with("$call(") {
                if let Some(col) = task.table.columns.iter().find(|c| c.name == set_col.column) {
                    call_patterns.push((
                        val.to_string(),
                        set_col.column.clone(),
                        col.resolved_type().clone(),
                    ));
                }
            }
        }
    }

    // Deduplicate call patterns (same call might be used for multiple columns)
    let unique_calls: HashSet<String> = call_patterns.iter().map(|(v, _, _)| v.clone()).collect();

    // Step 2: Execute all RPC calls in parallel for all blocks
    // Key: (block_number, call_pattern) -> Result
    let call_cache: Arc<
        tokio::sync::RwLock<HashMap<(u64, String), Option<EthereumSqlTypeWrapper>>>,
    > = Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    // Use adaptive concurrency from rate limiter
    let current_concurrency = rate_limiter.concurrency();
    let semaphore = Arc::new(tokio::sync::Semaphore::new(current_concurrency));
    let mut futures = FuturesUnordered::new();

    for &block_number in blocks {
        for call_pattern in &unique_calls {
            let provider = provider.clone();
            let contract_address = task.contract_address;
            let network = task.network.clone();
            let call_pattern = call_pattern.clone();
            let cache = call_cache.clone();
            let sem = semaphore.clone();

            // Find the column type for this call pattern
            let column_type = call_patterns
                .iter()
                .find(|(v, _, _)| v == &call_pattern)
                .map(|(_, _, t)| t.clone())
                .unwrap_or(ColumnType::String);

            futures.push(async move {
                let _permit = sem.acquire().await.unwrap();

                let timestamp = U256::from(chrono::Utc::now().timestamp() as u64);
                let tx_metadata = TxMetadata {
                    block_number,
                    block_timestamp: Some(timestamp),
                    tx_hash: B256::ZERO,
                    block_hash: B256::ZERO,
                    contract_address,
                    log_index: U256::ZERO,
                    tx_index: 0,
                };

                let result = extract_cron_value(
                    &call_pattern,
                    &tx_metadata,
                    &contract_address,
                    &column_type,
                    Some(provider.as_ref()),
                    &network,
                )
                .await;

                let mut cache_guard = cache.write().await;
                cache_guard.insert((block_number, call_pattern), result);
            });
        }
    }

    // Wait for all RPC calls to complete
    while futures.next().await.is_some() {
        if !is_running() {
            return Ok(None);
        }
    }

    // Step 3: Build all rows using cached results
    let cache = call_cache.read().await;
    let mut all_rows: Vec<TableRowData> = Vec::with_capacity(blocks.len());
    let mut last_successful_block: Option<u64> = None;

    for &block_number in blocks {
        if !is_running() {
            break;
        }

        let timestamp = U256::from(chrono::Utc::now().timestamp() as u64);
        let tx_metadata = TxMetadata {
            block_number,
            block_timestamp: Some(timestamp),
            tx_hash: B256::ZERO,
            block_hash: B256::ZERO,
            contract_address: task.contract_address,
            log_index: U256::ZERO,
            tx_index: 0,
        };

        for operation in &task.cron_entry.operations {
            let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

            // Add where clause columns
            for (column_name, value_ref) in &operation.where_clause {
                if value_ref.starts_with("$call(") {
                    if let Some(value) =
                        cache.get(&(block_number, value_ref.clone())).and_then(|v| v.clone())
                    {
                        columns.insert(column_name.clone(), value);
                    }
                } else {
                    // Handle non-$call values
                    let column_def = task.table.columns.iter().find(|c| &c.name == column_name);
                    if let Some(column_def) = column_def {
                        if let Some(value) = extract_cron_value_sync(
                            value_ref,
                            &tx_metadata,
                            &task.contract_address,
                            column_def.resolved_type(),
                        ) {
                            columns.insert(column_name.clone(), value);
                        }
                    }
                }
            }

            // Add set columns
            for set_col in &operation.set {
                let val = set_col.effective_value();
                if val.starts_with("$call(") {
                    if let Some(value) =
                        cache.get(&(block_number, val.to_string())).and_then(|v| v.clone())
                    {
                        columns.insert(set_col.column.clone(), value);
                    }
                } else {
                    // Handle non-$call values
                    let column_def = task.table.columns.iter().find(|c| c.name == set_col.column);
                    if let Some(column_def) = column_def {
                        if let Some(value) = extract_cron_value_sync(
                            val,
                            &tx_metadata,
                            &task.contract_address,
                            column_def.resolved_type(),
                        ) {
                            columns.insert(set_col.column.clone(), value);
                        }
                    }
                }
            }

            if columns.is_empty() {
                continue;
            }

            // Add auto-injected metadata columns
            let sequence_id = crate::manifest::contract::compute_sequence_id(block_number, 0, 0);
            columns.insert(
                crate::manifest::contract::injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                EthereumSqlTypeWrapper::U128(sequence_id),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::BLOCK_NUMBER.to_string(),
                EthereumSqlTypeWrapper::U64BigInt(block_number),
            );

            if task.table.timestamp {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        columns.insert(
                            crate::manifest::contract::injected_columns::BLOCK_TIMESTAMP
                                .to_string(),
                            EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)),
                        );
                    }
                }
            }

            columns.insert(
                crate::manifest::contract::injected_columns::TX_HASH.to_string(),
                EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::BLOCK_HASH.to_string(),
                EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
            );
            columns.insert(
                crate::manifest::contract::injected_columns::CONTRACT_ADDRESS.to_string(),
                EthereumSqlTypeWrapper::Address(task.contract_address),
            );

            all_rows.push(TableRowData { columns, network: task.network.clone() });
        }

        last_successful_block = Some(block_number);
    }

    drop(cache); // Release the read lock

    if all_rows.is_empty() {
        return Ok(last_successful_block);
    }

    // Step 4: Write all rows to database in batch
    let operation = &task.cron_entry.operations[0];

    info!("Tables::{} - {:?} - {} rows", task.table.name, operation.operation_type, all_rows.len());

    if let Some(postgres) = &postgres {
        super::tables::execute_postgres_operation_internal(
            postgres,
            &task.full_table_name,
            &task.table,
            operation,
            &all_rows,
            None,
        )
        .await?;
    }

    if let Some(clickhouse) = &clickhouse {
        super::tables::execute_clickhouse_operation_internal(
            clickhouse,
            &task.full_table_name,
            &task.table,
            operation,
            &all_rows,
        )
        .await?;
    }

    Ok(last_successful_block)
}

// Note: Prefetching is handled by the VIEW_CALL_CACHE in extract_cron_value
// The batch processing collects rows and writes them in one database operation

/// Run live cron loop for a task.
async fn run_live_cron_loop(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) {
    loop {
        if !is_running() {
            debug!("Cron task for table '{}' stopping due to shutdown", task.table.name);
            break;
        }

        // Wait for next trigger
        let wait_duration = match &task.schedule {
            CronSchedule::Interval(duration) => *duration,
            CronSchedule::Cron(cron) => {
                // Calculate next occurrence from now
                let now = chrono::Utc::now();
                match cron.find_next_occurrence(&now, false) {
                    Ok(next) => {
                        let duration = next - now;
                        duration.to_std().unwrap_or(Duration::from_secs(60))
                    }
                    Err(e) => {
                        warn!(
                            "Could not find next cron occurrence for table '{}': {}, using 60s default",
                            task.table.name, e
                        );
                        Duration::from_secs(60)
                    }
                }
            }
        };

        debug!(
            "Cron task for table '{}' waiting {:?} until next execution",
            task.table.name, wait_duration
        );

        // Sleep in chunks to check for shutdown
        let sleep_chunk = Duration::from_secs(1);
        let mut remaining = wait_duration;

        while remaining > Duration::ZERO {
            if !is_running() {
                debug!("Cron task for table '{}' stopping due to shutdown", task.table.name);
                return;
            }

            let sleep_time = remaining.min(sleep_chunk);
            tokio::time::sleep(sleep_time).await;
            remaining = remaining.saturating_sub(sleep_time);
        }

        if !is_running() {
            break;
        }

        // Execute the cron operations with retry and exponential backoff
        let mut retry_count = 0;
        let mut last_error: Option<String> = None;

        while retry_count <= MAX_RETRIES {
            if !is_running() {
                break;
            }

            match execute_cron_operations(
                task,
                postgres.clone(),
                clickhouse.clone(),
                providers.clone(),
            )
            .await
            {
                Ok(()) => {
                    // Success - clear any previous error state
                    if retry_count > 0 {
                        info!(
                            "Cron task for table '{}' succeeded after {} retry attempt(s)",
                            task.table.name, retry_count
                        );
                    }
                    break;
                }
                Err(e) => {
                    last_error = Some(e.clone());
                    retry_count += 1;

                    if retry_count <= MAX_RETRIES {
                        // Calculate exponential backoff: 2s, 4s, 8s
                        let backoff_secs = INITIAL_BACKOFF_SECS * (1 << (retry_count - 1));
                        let backoff = Duration::from_secs(backoff_secs);

                        warn!(
                            "Cron task failed for table '{}' on network '{}': {}. Retrying in {:?} (attempt {}/{})",
                            task.table.name, task.network, e, backoff, retry_count, MAX_RETRIES
                        );

                        // Sleep with shutdown check
                        let mut backoff_remaining = backoff;
                        while backoff_remaining > Duration::ZERO {
                            if !is_running() {
                                return;
                            }
                            let sleep_time = backoff_remaining.min(Duration::from_secs(1));
                            tokio::time::sleep(sleep_time).await;
                            backoff_remaining = backoff_remaining.saturating_sub(sleep_time);
                        }
                    }
                }
            }
        }

        // If we exhausted all retries, log the final error
        if retry_count > MAX_RETRIES {
            if let Some(e) = last_error {
                error!(
                    "Cron task failed for table '{}' on network '{}' after {} retries: {}",
                    task.table.name, task.network, MAX_RETRIES, e
                );
            }
        }
    }
}

/// Execute the cron operations for a task.
async fn execute_cron_operations(
    task: &CronTask,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<HashMap<String, Arc<JsonRpcCachedProvider>>>,
) -> Result<(), String> {
    let provider = providers.get(&task.network);

    // Get latest block number for this network
    let latest_block = if let Some(p) = provider {
        p.get_block_number().await.ok().map(|b| b.to::<u64>()).unwrap_or(0)
    } else {
        warn!(
            "No provider found for network '{}' in cron task for table '{}'",
            task.network, task.table.name
        );
        0
    };

    let now = chrono::Utc::now();

    // Create synthetic metadata (no event data)
    let tx_metadata = TxMetadata {
        block_number: latest_block,
        block_timestamp: Some(U256::from(now.timestamp() as u64)),
        tx_hash: B256::ZERO,
        block_hash: B256::ZERO,
        contract_address: task.contract_address,
        log_index: U256::ZERO,
        tx_index: 0,
    };

    debug!(
        "Executing cron operations for table '{}' on network '{}' at block {}",
        task.table.name, task.network, latest_block
    );

    // Process each operation
    for operation in &task.cron_entry.operations {
        let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

        // Get provider for view calls
        let provider_ref = provider.map(|p| p.as_ref());

        // Add where clause columns
        for (column_name, value_ref) in &operation.where_clause {
            let column_def = task.table.columns.iter().find(|c| &c.name == column_name);

            if let Some(column_def) = column_def {
                if let Some(value) = extract_cron_value(
                    value_ref,
                    &tx_metadata,
                    &task.contract_address,
                    column_def.resolved_type(),
                    provider_ref,
                    &task.network,
                )
                .await
                {
                    columns.insert(column_name.clone(), value);
                }
            }
        }

        // Add set columns with their values
        for set_col in &operation.set {
            let column_def = task.table.columns.iter().find(|c| c.name == set_col.column);

            if let Some(column_def) = column_def {
                if let Some(value) = extract_cron_value(
                    set_col.effective_value(),
                    &tx_metadata,
                    &task.contract_address,
                    column_def.resolved_type(),
                    provider_ref,
                    &task.network,
                )
                .await
                {
                    columns.insert(set_col.column.clone(), value);
                }
            }
        }

        if columns.is_empty() {
            debug!(
                "No columns extracted for cron operation on table '{}', skipping",
                task.table.name
            );
            continue;
        }

        // Add auto-injected metadata columns
        // For cron, we use a synthetic sequence_id based on timestamp to ensure uniqueness
        let sequence_id = crate::manifest::contract::compute_sequence_id(
            latest_block,
            0, // no tx_index for cron
            0, // no log_index for cron
        );

        columns.insert(
            crate::manifest::contract::injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
            EthereumSqlTypeWrapper::U128(sequence_id),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::BLOCK_NUMBER.to_string(),
            EthereumSqlTypeWrapper::U64BigInt(latest_block),
        );
        // Only add timestamp if table has timestamp: true
        if task.table.timestamp {
            columns.insert(
                crate::manifest::contract::injected_columns::BLOCK_TIMESTAMP.to_string(),
                EthereumSqlTypeWrapper::DateTime(now),
            );
        }
        columns.insert(
            crate::manifest::contract::injected_columns::TX_HASH.to_string(),
            EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::BLOCK_HASH.to_string(),
            EthereumSqlTypeWrapper::StringChar(format!("{:?}", B256::ZERO)),
        );
        columns.insert(
            crate::manifest::contract::injected_columns::CONTRACT_ADDRESS.to_string(),
            EthereumSqlTypeWrapper::Address(task.contract_address),
        );

        let rows = vec![TableRowData { columns, network: task.network.clone() }];

        // Execute the operation
        if let Some(postgres) = &postgres {
            super::tables::execute_postgres_operation_internal(
                postgres,
                &task.full_table_name,
                &task.table,
                operation,
                &rows,
                None, // No SQL condition for cron
            )
            .await?;
        }

        if let Some(clickhouse) = &clickhouse {
            super::tables::execute_clickhouse_operation_internal(
                clickhouse,
                &task.full_table_name,
                &task.table,
                operation,
                &rows,
            )
            .await?;
        }
    }

    info!(
        "Cron operations completed for table '{}' on network '{}' at block {}",
        task.table.name, task.network, latest_block
    );

    Ok(())
}

/// Extract value for cron operations (no event data available).
///
/// This is similar to `extract_value_from_event_async` but only supports:
/// - $call(...) - view function calls
/// - $contract - contract address
/// - $rindexer_* - built-in metadata
/// - Literals
async fn extract_cron_value(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
    provider: Option<&JsonRpcCachedProvider>,
    network: &str,
) -> Option<EthereumSqlTypeWrapper> {
    // Handle $call(...) - view function calls
    if value_ref.starts_with("$call(") {
        let provider = provider?;
        // Use existing view call infrastructure from tables module
        return super::tables::execute_view_call_for_cron(
            value_ref,
            tx_metadata,
            contract_address,
            column_type,
            provider,
            network,
        )
        .await;
    }

    // Handle $contract - contract address
    if value_ref == "$contract" {
        return Some(EthereumSqlTypeWrapper::Address(*contract_address));
    }

    // Handle $rindexer_* built-ins
    if let Some(field_name) = value_ref.strip_prefix('$') {
        match field_name {
            "rindexer_block_number" => {
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number));
            }
            "rindexer_block_timestamp" | "rindexer_timestamp" => {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        return Some(EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)));
                    }
                }
                return None;
            }
            "rindexer_tx_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.tx_hash
                )));
            }
            "rindexer_block_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.block_hash
                )));
            }
            "rindexer_contract_address" => {
                return Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address));
            }
            _ => {
                // Unknown $ reference - this shouldn't happen after validation
                warn!("Unknown cron value reference: {}", value_ref);
                return None;
            }
        }
    }

    // Handle literal values
    super::tables::parse_literal_value_for_column(value_ref, column_type)
}

/// Extract value for cron operations synchronously (no RPC calls).
///
/// This handles all non-$call values:
/// - $contract - contract address
/// - $rindexer_* - built-in metadata
/// - Literals
fn extract_cron_value_sync(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
) -> Option<EthereumSqlTypeWrapper> {
    // $call requires async - should not be passed here
    if value_ref.starts_with("$call(") {
        return None;
    }

    // Handle $contract - contract address
    if value_ref == "$contract" {
        return Some(EthereumSqlTypeWrapper::Address(*contract_address));
    }

    // Handle $rindexer_* built-ins
    if let Some(field_name) = value_ref.strip_prefix('$') {
        match field_name {
            "rindexer_block_number" => {
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number));
            }
            "rindexer_block_timestamp" | "rindexer_timestamp" => {
                if let Some(ts) = tx_metadata.block_timestamp {
                    if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                        return Some(EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)));
                    }
                }
                return None;
            }
            "rindexer_tx_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.tx_hash
                )));
            }
            "rindexer_block_hash" => {
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.block_hash
                )));
            }
            "rindexer_contract_address" => {
                return Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address));
            }
            _ => {
                // Unknown $ reference
                return None;
            }
        }
    }

    // Handle literal values
    super::tables::parse_literal_value_for_column(value_ref, column_type)
}

/// Check if any tables in the manifest have cron triggers.
pub fn manifest_has_cron_tables(manifest: &Manifest) -> bool {
    manifest.contracts.iter().filter_map(|c| c.tables.as_ref()).flatten().any(|t| t.has_cron())
}
