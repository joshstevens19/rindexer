use std::{collections::HashMap, sync::Arc, time::Duration};

use async_std::prelude::StreamExt;
use ethers::{
    prelude::ProviderError,
    types::{H256, U64},
};
use futures::future::join_all;
use tokio::{
    sync::{Mutex, MutexGuard},
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{debug, error, info};

use crate::{
    event::{
        callback_registry::EventResult, config::EventProcessingConfig, BuildRindexerFilterError,
        RindexerEventFilter,
    },
    indexer::{
        dependency::{ContractEventsDependenciesConfig, EventDependencies},
        fetch_logs::{fetch_logs_stream, FetchLogsResult},
        last_synced::update_progress_and_last_synced,
        log_helpers::is_relevant_block,
        progress::IndexingEventProgressStatus,
    },
};

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),
}

pub async fn process_event(config: EventProcessingConfig) -> Result<(), ProcessEventError> {
    debug!("{} - Processing events", config.info_log_name);

    process_event_logs(Arc::new(config), false).await?;

    Ok(())
}

async fn process_event_logs(
    config: Arc<EventProcessingConfig>,
    force_no_live_indexing: bool,
) -> Result<(), Box<ProviderError>> {
    let mut logs_stream = fetch_logs_stream(Arc::clone(&config), force_no_live_indexing);

    while let Some(result) = logs_stream.next().await {
        handle_logs_result(Arc::clone(&config), result)
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

#[derive(Debug, Clone)]
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
        Arc::new(Mutex::new(Vec::<(Arc<EventProcessingConfig>, RindexerEventFilter)>::new()));

    while let Some(current_tree) = stack.pop() {
        let mut tasks = vec![];

        for dependency in &current_tree.contract_events {
            let event_processing_config = Arc::clone(&events_processing_config);
            let dependency = dependency.clone();
            let live_indexing_events = Arc::clone(&live_indexing_events);

            let task = tokio::spawn(async move {
                let event_processing_config = event_processing_config
                    .iter()
                    .find(|e| {
                        // TODO - this is a hacky way to check if it's a filter event
                        (e.contract_name == dependency.contract_name ||
                            e.contract_name.replace("Filter", "") == dependency.contract_name) &&
                            e.event_name == dependency.event_name
                    })
                    .ok_or(ProcessContractEventsWithDependenciesError::EventConfigNotFound(
                        dependency.contract_name,
                        dependency.event_name,
                    ))?;

                // forces live indexing off as it has to handle it a bit differently
                process_event_logs(Arc::clone(event_processing_config), true).await?;

                if event_processing_config.live_indexing {
                    let rindexer_event_filter = event_processing_config.to_event_filter()?;
                    live_indexing_events
                        .lock()
                        .await
                        .push((Arc::clone(event_processing_config), rindexer_event_filter));
                }

                Ok::<(), ProcessContractEventsWithDependenciesError>(())
            });

            tasks.push(task);
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

    live_indexing_for_contract_event_dependencies(&live_indexing_events).await;

    Ok(())
}

// TODO - this is a similar to live_indexing_stream but has to be a bit different we should merge
// code
#[allow(clippy::type_complexity)]
async fn live_indexing_for_contract_event_dependencies<'a>(
    live_indexing_events: &'a MutexGuard<
        'a,
        Vec<(Arc<EventProcessingConfig>, RindexerEventFilter)>,
    >,
) {
    let mut ordering_live_indexing_details_map: HashMap<
        H256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::new();

    for (config, event_filter) in live_indexing_events.iter() {
        let mut filter = event_filter.clone();
        let last_seen_block_number = filter.get_to_block();
        let next_block_number = last_seen_block_number + 1;

        filter = filter.set_from_block(next_block_number).set_to_block(next_block_number);

        ordering_live_indexing_details_map.insert(
            config.topic_id,
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
                last_no_new_block_log_time: Instant::now(),
            })),
        );
    }

    // this is used for less busy chains to make sure they know rindexer is still alive
    let log_no_new_block_interval = Duration::from_secs(20);

    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;

        for (config, _) in live_indexing_events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(&config.topic_id)
                .expect("Failed to get ordering_live_indexing_details_map")
                .lock()
                .await
                .clone();

            let latest_block = &config.network_contract.cached_provider.get_latest_block().await;

            match latest_block {
                Ok(latest_block) => {
                    if let Some(latest_block) = latest_block {
                        if let Some(latest_block_number) = latest_block.number {
                            if ordering_live_indexing_details.last_seen_block_number ==
                                latest_block_number
                            {
                                debug!(
                                    "{} - {} - No new blocks to process...",
                                    &config.info_log_name,
                                    IndexingEventProgressStatus::Live.log()
                                );
                                if ordering_live_indexing_details
                                    .last_no_new_block_log_time
                                    .elapsed() >=
                                    log_no_new_block_interval
                                {
                                    info!(
                                        "{} - {} - No new blocks published in the last 20 seconds - latest block number {}",
                                        &config.info_log_name,
                                        IndexingEventProgressStatus::Live.log(),
                                        latest_block_number
                                    );
                                    ordering_live_indexing_details.last_no_new_block_log_time =
                                        Instant::now();
                                    *ordering_live_indexing_details_map
                                        .get(&config.topic_id)
                                        .expect("Failed to get ordering_live_indexing_details_map")
                                        .lock()
                                        .await = ordering_live_indexing_details;
                                }
                                continue;
                            }
                            info!(
                                "{} - {} - New block seen {} - Last seen block {}",
                                &config.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                latest_block_number,
                                ordering_live_indexing_details.last_seen_block_number
                            );
                            let reorg_safe_distance = &config.indexing_distance_from_head;
                            let safe_block_number = latest_block_number - reorg_safe_distance;
                            let from_block = ordering_live_indexing_details.filter.get_from_block();
                            // check reorg distance and skip if not safe
                            if from_block > safe_block_number {
                                info!(
                                    "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                                    &config.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block,
                                    safe_block_number
                                );
                                continue;
                            }

                            let to_block = safe_block_number;
                            if from_block == to_block &&
                                !config.network_contract.disable_logs_bloom_checks &&
                                !is_relevant_block(
                                    &ordering_live_indexing_details.filter.raw_filter().address,
                                    &config.topic_id,
                                    latest_block,
                                )
                            {
                                debug!(
                                    "{} - {} - Skipping block {} as it's not relevant",
                                    &config.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block
                                );
                                info!(
                                    "{} - {} - Did not need to hit RPC as no events in {} block - LogsBloom for block checked",
                                    &config.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block
                                );

                                ordering_live_indexing_details.filter =
                                    ordering_live_indexing_details
                                        .filter
                                        .set_from_block(to_block + 1);

                                ordering_live_indexing_details.last_seen_block_number = to_block;
                                *ordering_live_indexing_details_map
                                    .get(&config.topic_id)
                                    .expect("Failed to get ordering_live_indexing_details_map")
                                    .lock()
                                    .await = ordering_live_indexing_details;
                                continue;
                            }

                            ordering_live_indexing_details.filter =
                                ordering_live_indexing_details.filter.set_to_block(to_block);

                            debug!(
                                "{} - {} - Processing live filter: {:?}",
                                &config.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                ordering_live_indexing_details.filter
                            );

                            let semaphore_client = Arc::clone(&config.semaphore);
                            let permit = semaphore_client.acquire_owned().await;

                            if let Ok(permit) = permit {
                                match config
                                    .network_contract
                                    .cached_provider
                                    .get_logs(&ordering_live_indexing_details.filter)
                                    .await
                                {
                                    Ok(logs) => {
                                        debug!(
                                            "{} - {} - Live topic_id {}, Logs: {} from {} to {}",
                                            &config.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            &config.topic_id,
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        debug!(
                                            "{} - {} - Fetched {} event logs - blocks: {} - {}",
                                            &config.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        let logs_empty = logs.is_empty();
                                        // clone here over the full logs way less overhead
                                        let last_log = logs.last().cloned();

                                        let fetched_logs =
                                            Ok(FetchLogsResult { logs, from_block, to_block });

                                        let result =
                                            handle_logs_result(Arc::clone(config), fetched_logs)
                                                .await;

                                        match result {
                                            Ok(_) => {
                                                ordering_live_indexing_details
                                                    .last_seen_block_number = to_block;
                                                if logs_empty {
                                                    ordering_live_indexing_details.filter =
                                                        ordering_live_indexing_details
                                                            .filter
                                                            .set_from_block(to_block + 1);
                                                    info!(
                                                        "{} - {} - No events found between blocks {} - {}",
                                                        &config.info_log_name,
                                                        IndexingEventProgressStatus::Live.log(),
                                                        from_block,
                                                        to_block
                                                    );
                                                } else if let Some(last_log) = last_log {
                                                    if let Some(last_log_block_number) =
                                                        last_log.inner.block_number
                                                    {
                                                        ordering_live_indexing_details.filter =
                                                            ordering_live_indexing_details
                                                                .filter
                                                                .set_from_block(
                                                                    last_log_block_number +
                                                                        U64::from(1),
                                                                );
                                                    } else {
                                                        error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                                    }
                                                }

                                                *ordering_live_indexing_details_map
                                                    .get(&config.topic_id)
                                                    .expect("Failed to get ordering_live_indexing_details_map")
                                                    .lock()
                                                    .await = ordering_live_indexing_details;

                                                drop(permit);
                                            }
                                            Err(err) => {
                                                error!(
                                                    "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                                    &config.info_log_name,
                                                    IndexingEventProgressStatus::Live.log(),
                                                    err
                                                );
                                                drop(permit);
                                                break;
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        error!(
                                            "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                            &config.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            err
                                        );
                                        drop(permit);
                                        break;
                                    }
                                }
                            }
                        } else {
                            info!("WARNING - empty latest block returned from provider, will try again in 200ms");
                        }
                    } else {
                        info!("WARNING - empty latest block returned from provider, will try again in 200ms");
                    }
                }
                Err(error) => {
                    error!(
                        "Failed to get latest block, will try again in 200ms - error: {}",
                        error.to_string()
                    );
                }
            }
        }
    }
}

async fn handle_logs_result(
    config: Arc<EventProcessingConfig>,
    result: Result<FetchLogsResult, Box<dyn std::error::Error + Send>>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    match result {
        Ok(result) => {
            debug!("Processing logs {} - length {}", config.event_name, result.logs.len());

            let fn_data = result
                .logs
                .into_iter()
                .map(|log| {
                    EventResult::new(
                        Arc::clone(&config.network_contract),
                        log,
                        result.from_block,
                        result.to_block,
                    )
                })
                .collect::<Vec<_>>();

            if !fn_data.is_empty() {
                if config.index_event_in_order {
                    config.trigger_event(fn_data).await;
                    update_progress_and_last_synced(config, result.to_block);
                } else {
                    tokio::spawn(async move {
                        config.trigger_event(fn_data).await;
                        update_progress_and_last_synced(config, result.to_block);
                    });
                }
            }

            Ok(())
        }
        Err(e) => {
            error!("Error fetching logs: {:?}", e);
            Err(e)
        }
    }
}
