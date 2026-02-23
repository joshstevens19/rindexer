use alloy::primitives::{B256, U64};

use futures::future::join_all;
use futures::StreamExt;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{debug, error, info, warn};

use crate::helpers::is_relevant_block;
use crate::indexer::reorg::{
    find_fork_point, handle_reorg_recovery, reorg_safe_distance_for_chain,
};
use crate::metrics::indexing as metrics;
use crate::provider::JsonRpcCachedProvider;
use crate::{
    event::{
        callback_registry::EventResult, config::EventProcessingConfig, BuildRindexerFilterError,
        RindexerEventFilter,
    },
    indexer::{
        dependency::{ContractEventsDependenciesConfig, EventDependencies},
        fetch_logs::{fetch_logs_stream, BlockMeta, FetchLogsResult, ReorgInfo},
        last_synced::update_progress_and_last_synced_task,
        progress::IndexingEventProgressStatus,
        task_tracker::{indexing_event_processed, indexing_event_processing},
    },
    is_running,
    provider::ProviderError,
};

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),

    #[error("Could not get block number from provider: {0}")]
    ProviderCallError(#[from] ProviderError),
}

/// Processes an event that doesn't have dependencies.
/// First processes historical logs, then starts live indexing if the event is configured for live indexing.
/// This function returns immediately without waiting for the indexing to complete.
pub async fn process_non_blocking_event(
    config: EventProcessingConfig,
) -> Result<(), ProcessEventError> {
    debug!("{} - Processing non blocking event", config.info_log_name());

    process_event_logs(Arc::new(config), false, false).await?;

    Ok(())
}

/// Processes historical logs for a blocking event that has dependencies.
/// This function waits for the indexing to complete before returning.
pub async fn process_blocking_event_historical_data(
    config: Arc<EventProcessingConfig>,
) -> Result<(), Box<ProviderError>> {
    debug!("{} - Processing blocking event historical data", config.info_log_name());

    process_event_logs(config, true, true).await?;

    Ok(())
}

/// note block_until_indexed:
/// Whether to wait for all indexing tasks to complete for an event before returning
//  (needed for dependency indexing)
async fn process_event_logs(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
    block_until_indexed: bool,
) -> Result<(), Box<ProviderError>> {
    // The concurrency with which we can call the trigger. If the indexer is running in-order
    // we can only call one at a time, otherwise we can call multiple in parallel based on what is
    // best for the application.
    //
    // We default to `2`, but the user will ideally override this based on the logic in the handler.
    // TODO: this feature is not safe need to review it
    let callback_concurrency = if config.index_event_in_order() {
        1usize
    } else {
        config.config().callback_concurrency.unwrap_or(1)
    };

    let callback_permits = Arc::new(Semaphore::new(callback_concurrency));

    let mut logs_stream = fetch_logs_stream(Arc::clone(&config), force_no_live_indexing);
    let mut tasks = Vec::new();

    while let Some(result) = logs_stream.next().await {
        let task = handle_logs_result(Arc::clone(&config), callback_permits.clone(), result)
            .await
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;

        if block_until_indexed {
            task.await.map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
        } else {
            tasks.push(task);
        }
    }

    // Wait for all remaining tasks to complete
    if !tasks.is_empty() {
        futures::future::try_join_all(tasks)
            .await
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractsEventsWithDependenciesError {
    #[error("{0}")]
    ProcessContractEventsWithDependenciesError(#[from] ProcessContractEventsWithDependenciesError),

    #[error("{0}")]
    JoinError(#[from] JoinError),
}

pub async fn process_contracts_events_with_dependencies(
    contracts_events_config: Vec<ContractEventsDependenciesConfig>,
) -> Result<(), ProcessContractsEventsWithDependenciesError> {
    let mut handles: Vec<JoinHandle<Result<(), ProcessContractEventsWithDependenciesError>>> =
        Vec::new();

    for contract_events in contracts_events_config {
        let handle = tokio::spawn(async move {
            process_contract_events_with_dependencies(
                contract_events.event_dependencies,
                Arc::new(contract_events.events_config),
            )
            .await
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(inner_result) => inner_result?,
            Err(join_error) => {
                return Err(ProcessContractsEventsWithDependenciesError::JoinError(join_error))
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractEventsWithDependenciesError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),

    #[error("Event config not found for contract: {0} and event: {1}")]
    EventConfigNotFound(String, String),

    #[error("Could not run all the logs processes {0}")]
    JoinError(#[from] JoinError),
}

#[derive(Clone)]
pub struct OrderedLiveIndexingDetails {
    pub filter: RindexerEventFilter,
    pub last_seen_block_number: U64,
    pub last_no_new_block_log_time: Instant,
}

async fn process_contract_events_with_dependencies(
    dependencies: EventDependencies,
    events_processing_config: Arc<Vec<Arc<EventProcessingConfig>>>,
) -> Result<(), ProcessContractEventsWithDependenciesError> {
    let mut stack = vec![dependencies.tree];

    let live_indexing_events =
        Arc::new(Mutex::new(HashMap::<String, EventDependenciesIndexingConfig>::new()));

    while let Some(current_tree) = stack.pop() {
        let mut tasks = vec![];

        for dependency in &current_tree.contract_events {
            // multi network can have many of the same event names so we need to get them all
            let event_processing_configs = events_processing_config
                .iter()
                .filter(|e| {
                    // TODO - this is a hacky way to check if it's a filter event
                    (e.contract_name() == dependency.contract_name
                        || e.contract_name().replace("Filter", "") == dependency.contract_name)
                        && e.event_name() == dependency.event_name
                })
                .cloned()
                .collect::<Vec<Arc<EventProcessingConfig>>>();

            for event_processing_config in event_processing_configs {
                let task = tokio::spawn({
                    let live_indexing_events = Arc::clone(&live_indexing_events);
                    async move {
                        process_blocking_event_historical_data(Arc::clone(
                            &event_processing_config,
                        ))
                        .await?;

                        if event_processing_config.live_indexing() {
                            let network_contract = event_processing_config.network_contract();

                            let mut live_indexing_events = live_indexing_events.lock().await;
                            let entry = live_indexing_events
                                .entry(network_contract.network.clone())
                                .or_insert_with(|| EventDependenciesIndexingConfig {
                                    network: network_contract.network.clone(),
                                    cached_provider: network_contract.cached_provider.clone(),
                                    events: Vec::new(),
                                });

                            let rindexer_event_filter =
                                event_processing_config.to_event_filter()?;

                            entry.events.push((
                                Arc::clone(&event_processing_config),
                                rindexer_event_filter,
                            ));
                        }

                        Ok::<(), ProcessContractEventsWithDependenciesError>(())
                    }
                });
                tasks.push(task);
            }
        }

        let results = join_all(tasks).await;
        for result in results {
            match result {
                Ok(result) => match result {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error processing logs due to dependencies error: {:?}", e);
                        return Err(e);
                    }
                },
                Err(e) => {
                    error!("Error processing logs: {:?}", e);
                    return Err(ProcessContractEventsWithDependenciesError::JoinError(e));
                }
            }
        }

        // If there are more dependencies to process, push the next level onto the stack
        if let Some(next_tree) = &*current_tree.then {
            stack.push(Arc::clone(next_tree));
        }
    }

    let live_indexing_events = live_indexing_events.lock().await;
    if live_indexing_events.is_empty() {
        return Ok(());
    }

    let live_indexing_tasks = live_indexing_events
        .values()
        .map(|config| tokio::spawn(live_indexing_for_contract_event_dependencies(config.clone())))
        .collect::<Vec<_>>();

    futures::future::try_join_all(live_indexing_tasks).await?;

    Ok(())
}

#[derive(Clone)]
pub struct EventDependenciesIndexingConfig {
    pub network: String,
    pub cached_provider: Arc<JsonRpcCachedProvider>,
    pub events: Vec<(Arc<EventProcessingConfig>, RindexerEventFilter)>,
}

// TODO - this is a similar to live_indexing_stream but has to be a bit different we should merge
// code
#[allow(clippy::type_complexity)]
async fn live_indexing_for_contract_event_dependencies(
    EventDependenciesIndexingConfig { cached_provider, events, network }: EventDependenciesIndexingConfig,
) {
    debug!(
        "Live indexing events on {} in order: {}",
        network,
        events
            .iter()
            .map(|(config, _)| format!("{}::{}", config.contract_name(), config.event_name()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut ordering_live_indexing_details_map: HashMap<
        B256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::with_capacity(events.len());

    for (config, event_filter) in events.iter() {
        let mut filter = event_filter.clone();
        let last_seen_block_number = filter.to_block();
        let next_block_number = last_seen_block_number + U64::from(1);

        filter = filter.set_from_block(next_block_number).set_to_block(next_block_number);

        ordering_live_indexing_details_map.insert(
            config.id(),
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
                last_no_new_block_log_time: Instant::now(),
            })),
        );
    }

    // this is used for less busy chains to make sure they know rindexer is still alive
    let log_no_new_block_interval = Duration::from_secs(300);
    let target_iteration_duration = Duration::from_millis(200);
    let callback_permits = Arc::new(Semaphore::new(1));

    // Use the first event's cancel_token -- all events in this generation share the same token.
    let generation_cancel = events.first().map(|(config, _)| config.cancel_token().clone());

    // Block metadata cache for reorg detection via parent hash chain validation.
    let mut block_cache: LruCache<u64, BlockMeta> = LruCache::new(NonZeroUsize::new(1024).unwrap());

    loop {
        if !is_running() {
            break;
        }
        if generation_cancel.as_ref().is_some_and(|t| t.is_cancelled()) {
            info!("Hot-reload: generation cancelled, stopping live indexing for {}", network);
            break;
        }

        let iteration_start = Instant::now();

        // a consistent latest block number across all events in the batch is required to avoid race conditions
        let latest_block = match cached_provider.get_latest_block().await {
            Ok(Some(block)) => block,
            Ok(None) => {
                error!("Empty latest block returned from provider, will try again in 200ms");

                tokio::time::sleep(Duration::from_millis(200)).await;

                continue;
            }
            Err(error) => {
                error!(
                    "Failed to get latest block, will try again in 1 second - error: {}",
                    error.to_string()
                );

                tokio::time::sleep(Duration::from_secs(1)).await;

                continue;
            }
        };
        let latest_block_number = U64::from(latest_block.header.number);

        // Reorg detection #1: tip hash changed (same block, different hash)
        let tip_reorged = block_cache
            .peek(&latest_block.header.number)
            .map(|cached| cached.hash != latest_block.header.hash)
            .unwrap_or(false);

        block_cache.put(
            latest_block.header.number,
            BlockMeta {
                hash: latest_block.header.hash,
                parent_hash: latest_block.header.parent_hash,
                timestamp: latest_block.header.timestamp,
            },
        );

        // Reorg detection #2: parent hash chain discontinuity
        let parent_mismatch = if !tip_reorged && latest_block.header.number > 0 {
            block_cache
                .peek(&(latest_block.header.number - 1))
                .map(|cached| cached.hash != latest_block.header.parent_hash)
                .unwrap_or(false)
        } else {
            false
        };

        if tip_reorged || parent_mismatch {
            let reason = if tip_reorged { "tip hash changed" } else { "parent hash mismatch" };
            let fork_block =
                find_fork_point(&block_cache, &cached_provider, latest_block.header.number).await;
            let depth = latest_block.header.number.saturating_sub(fork_block);
            metrics::record_reorg(&network, depth);
            warn!(
                "Dependency indexer on {} - REORG ({}): depth={}, fork_block={}",
                network, reason, depth, fork_block
            );

            // Invalidate cached hashes for reorged blocks
            for b in fork_block..=latest_block.header.number {
                block_cache.pop(&b);
            }

            // Recovery: delete reorged events and rewind checkpoints for all events
            let reorg_info = ReorgInfo { fork_block: U64::from(fork_block), depth, affected_tx_hashes: vec![] };
            for (config, _) in events.iter() {
                let _affected_hashes = handle_reorg_recovery(config, &reorg_info).await;
                let mut details = ordering_live_indexing_details_map
                    .get(&config.id())
                    .expect("ordering_live_indexing_details_map")
                    .lock()
                    .await;
                details.filter = details.filter.clone().set_from_block(U64::from(fork_block));
                details.last_seen_block_number = U64::from(fork_block.saturating_sub(1));
            }
            continue;
        }

        for (config, _) in events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(&config.id())
                .expect("Failed to get ordering_live_indexing_details_map")
                .lock()
                .await
                .clone();

            if ordering_live_indexing_details.last_seen_block_number == latest_block_number {
                debug!(
                    "{} - {} - No new blocks to process...",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log()
                );
                if ordering_live_indexing_details.last_no_new_block_log_time.elapsed()
                    >= log_no_new_block_interval
                {
                    info!(
                        "{} - {} - No new blocks published in the last 5 minutes - latest block number {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        latest_block_number
                    );
                    ordering_live_indexing_details.last_no_new_block_log_time = Instant::now();
                    *ordering_live_indexing_details_map
                        .get(&config.id())
                        .expect("Failed to get ordering_live_indexing_details_map")
                        .lock()
                        .await = ordering_live_indexing_details;
                }
                continue;
            }
            debug!(
                "{} - {} - New block seen {} - Last seen block {}",
                &config.info_log_name(),
                IndexingEventProgressStatus::Live.log(),
                latest_block_number,
                ordering_live_indexing_details.last_seen_block_number
            );
            let reorg_safe_distance = &config.indexing_distance_from_head();
            let safe_block_number = latest_block_number - reorg_safe_distance;
            let from_block = ordering_live_indexing_details.filter.from_block();

            // check reorg distance and skip if not safe
            if from_block > safe_block_number {
                if reorg_safe_distance.is_zero() {
                    let block_distance = from_block - latest_block_number;
                    let is_outside_reorg_range =
                        block_distance > reorg_safe_distance_for_chain(cached_provider.chain.id());

                    // it should never get under normal conditions outside the reorg range,
                    // therefore, we log an error as means RCP state is not in sync with the blockchain
                    if is_outside_reorg_range {
                        error!(
                            "{} - {} - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                            &config.info_log_name(),
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            from_block
                        );
                    } else {
                        info!(
                            "{} - {} - RPC has gone back on latest block: rpc returned {}, last seen: {}",
                            &config.info_log_name(),
                            IndexingEventProgressStatus::Live.log(),
                            latest_block_number,
                            from_block
                        );
                    }

                    continue;
                } else {
                    info!(
                        "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        from_block,
                        safe_block_number
                    );
                    continue;
                }
            }

            let to_block = safe_block_number;
            if from_block == to_block
                && !config.network_contract().disable_logs_bloom_checks
                && !is_relevant_block(
                    &ordering_live_indexing_details.filter.contract_addresses().await,
                    &config.topic_id(),
                    &latest_block,
                )
            {
                debug!(
                    "{} - {} - Skipping block {} as it's not relevant",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );
                debug!(
                    "{} - {} - Did not need to hit RPC as no events in {} block - LogsBloom for block checked",
                    &config.info_log_name(),
                    IndexingEventProgressStatus::Live.log(),
                    from_block
                );

                ordering_live_indexing_details.filter =
                    ordering_live_indexing_details.filter.set_from_block(to_block + U64::from(1));

                ordering_live_indexing_details.last_seen_block_number = to_block;
                *ordering_live_indexing_details_map
                    .get(&config.id())
                    .expect("Failed to get ordering_live_indexing_details_map")
                    .lock()
                    .await = ordering_live_indexing_details;
                continue;
            }

            ordering_live_indexing_details.filter =
                ordering_live_indexing_details.filter.set_to_block(to_block);

            debug!(
                "{} - {} - Processing live filter: {:?}",
                &config.info_log_name(),
                IndexingEventProgressStatus::Live.log(),
                ordering_live_indexing_details.filter
            );

            match cached_provider.get_logs(&ordering_live_indexing_details.filter).await {
                Ok(logs) => {
                    debug!(
                        "{} - {} - Live id {} topic_id {}, Logs: {} from {} to {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        &config.id(),
                        &config.topic_id(),
                        logs.len(),
                        from_block,
                        to_block
                    );

                    debug!(
                        "{} - {} - Fetched {} event logs - blocks: {} - {}",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        logs.len(),
                        from_block,
                        to_block
                    );

                    // Reorg detection: check for removed logs from RPC
                    if logs.iter().any(|log| log.removed) {
                        let min_removed_block = logs
                            .iter()
                            .filter(|l| l.removed)
                            .filter_map(|l| l.block_number)
                            .min()
                            .unwrap_or(from_block.to::<u64>());
                        let depth = from_block.to::<u64>().saturating_sub(min_removed_block);
                        metrics::record_reorg(&network, depth);
                        warn!(
                            "{} - REORG (removed logs): fork_block={}, depth={}",
                            config.info_log_name(),
                            min_removed_block,
                            depth
                        );

                        let affected_tx_hashes: Vec<B256> = logs
                            .iter()
                            .filter(|l| l.removed)
                            .filter_map(|l| l.transaction_hash)
                            .collect::<std::collections::HashSet<_>>()
                            .into_iter()
                            .collect();
                        let reorg_info =
                            ReorgInfo { fork_block: U64::from(min_removed_block), depth, affected_tx_hashes };
                        let _affected_hashes = handle_reorg_recovery(config, &reorg_info).await;

                        // Rewind cursor for this event
                        ordering_live_indexing_details.filter = ordering_live_indexing_details
                            .filter
                            .set_from_block(U64::from(min_removed_block));
                        ordering_live_indexing_details.last_seen_block_number =
                            U64::from(min_removed_block.saturating_sub(1));
                        *ordering_live_indexing_details_map
                            .get(&config.id())
                            .expect("ordering_live_indexing_details_map")
                            .lock()
                            .await = ordering_live_indexing_details;
                        continue;
                    }

                    let logs_empty = logs.is_empty();
                    // clone here over the full logs way less overhead
                    let last_log = logs.last().cloned();

                    let fetched_logs =
                        Ok(FetchLogsResult { logs, from_block, to_block, reorg: None });

                    let result = handle_logs_result(
                        Arc::clone(config),
                        callback_permits.clone(),
                        fetched_logs,
                    )
                    .await;

                    match result {
                        Ok(task) => {
                            let complete = task.await;
                            if let Err(e) = complete {
                                error!(
                                    "{} - {} - Error indexing task: {} - will try again in 200ms",
                                    &config.info_log_name(),
                                    IndexingEventProgressStatus::Live.log(),
                                    e
                                );
                                break;
                            }
                            ordering_live_indexing_details.last_seen_block_number = to_block;
                            if logs_empty {
                                ordering_live_indexing_details.filter =
                                    ordering_live_indexing_details
                                        .filter
                                        .set_from_block(to_block + U64::from(1));
                                debug!(
                                    "{} - {} - No events found between blocks {} - {}",
                                    &config.info_log_name(),
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block,
                                    to_block
                                );
                            } else if let Some(last_log) = last_log {
                                if let Some(last_log_block_number) = last_log.block_number {
                                    ordering_live_indexing_details.filter =
                                        ordering_live_indexing_details
                                            .filter
                                            .set_from_block(U64::from(last_log_block_number + 1));
                                } else {
                                    error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                }
                            }

                            *ordering_live_indexing_details_map
                                .get(&config.id())
                                .expect("Failed to get ordering_live_indexing_details_map")
                                .lock()
                                .await = ordering_live_indexing_details;
                        }
                        Err(err) => {
                            error!(
                                "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                &config.info_log_name(),
                                IndexingEventProgressStatus::Live.log(),
                                err
                            );
                            break;
                        }
                    }
                }
                Err(err) => {
                    error!(
                        "{} - {} - Error fetching logs: {} - will try again in 200ms",
                        &config.info_log_name(),
                        IndexingEventProgressStatus::Live.log(),
                        err
                    );
                    break;
                }
            }
        }

        let elapsed = iteration_start.elapsed();
        if elapsed < target_iteration_duration {
            tokio::time::sleep(target_iteration_duration - elapsed).await;
        }
    }
}

async fn trigger_event(
    config: Arc<EventProcessingConfig>,
    fn_data: Vec<EventResult>,
    to_block: U64,
) {
    indexing_event_processing();

    // Record events processed metric
    let event_count = fn_data.len() as u64;
    if event_count > 0 {
        metrics::record_events_indexed(
            &config.network_contract().network,
            &config.contract_name(),
            &config.event_name(),
            event_count,
            to_block.to::<u64>(),
            None, // Latest chain block updated elsewhere
        );
    }

    let should_update_progress = if fn_data.is_empty() {
        #[allow(clippy::needless_bool)]
        if !is_running() || config.cancel_token().is_cancelled() {
            false
        } else {
            true
        }
    } else {
        config.trigger_event(fn_data.clone()).await.is_ok()
    };

    if should_update_progress {
        // TODO: There is a double-index race condition here. If we get a crash or failure between
        //       triggering the event and syncing the last updated block, we may double index.
        update_progress_and_last_synced_task(config, to_block, indexing_event_processed).await;
    } else {
        indexing_event_processed();
    }
}

async fn handle_logs_result(
    config: Arc<EventProcessingConfig>,
    callback_permits: Arc<Semaphore>,
    result: Result<FetchLogsResult, Box<dyn std::error::Error + Send>>,
) -> Result<JoinHandle<()>, Box<dyn std::error::Error + Send>> {
    match result {
        Ok(result) => {
            // Handle reorg recovery before processing logs
            if let Some(reorg) = &result.reorg {
                let _affected_hashes = handle_reorg_recovery(&config, reorg).await;
                return Ok(tokio::spawn(async {}));
            }

            debug!("{} - Processing {} logs", config.info_log_name(), result.logs.len());

            let fn_data = result
                .logs
                .into_iter()
                .map(|log| {
                    EventResult::new(
                        Arc::clone(&config.network_contract()),
                        log,
                        result.from_block,
                        result.to_block,
                    )
                })
                .collect::<Vec<_>>();

            if let Ok(permit) = callback_permits.clone().acquire_owned().await {
                let task = tokio::spawn(async move {
                    trigger_event(config, fn_data, result.to_block).await;
                    drop(permit)
                });

                Ok(task)
            } else {
                trigger_event(config, fn_data, result.to_block).await;
                Ok(tokio::spawn(async {}))
            }
        }
        Err(e) => {
            error!(
                "[{}] - {} - {} - Error fetching logs: {}",
                config.network_contract().network,
                config.event_name(),
                IndexingEventProgressStatus::Live.log(),
                e
            );
            Err(e)
        }
    }
}
