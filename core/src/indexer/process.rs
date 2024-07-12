use crate::event::callback_registry::{EventCallbackRegistry, EventResult};
use crate::event::config::EventProcessingConfig;
use crate::event::contract_setup::NetworkContract;
use crate::event::{BuildRindexerFilterError, RindexerEventFilter};
use crate::indexer::dependency::{
    ContractEventsDependenciesConfig, EventDependencies, EventsDependencyTree,
};
use crate::indexer::fetch_logs::{fetch_logs_stream, FetchLogsResult, LiveIndexingDetails};
use crate::indexer::last_synced::update_progress_and_last_synced;
use crate::indexer::log_helpers::is_relevant_block;
use crate::indexer::progress::{IndexingEventProgressStatus, IndexingEventsProgressState};
use crate::manifest::storage::CsvDetails;
use crate::PostgresClient;
use async_std::prelude::StreamExt;
use ethers::prelude::ProviderError;
use ethers::types::{H256, U64};
use futures::future::join_all;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinError, JoinHandle};
use tracing::{debug, error, info};

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractEventsWithDependenciesError {
    #[error("{0}")]
    ProcessEventsWithDependenciesError(#[from] ProcessEventsWithDependenciesError),

    #[error("{0}")]
    JoinError(#[from] JoinError),
}

pub async fn process_contract_events_with_dependencies(
    contract_events_config: Vec<ContractEventsDependenciesConfig>,
) -> Result<(), ProcessContractEventsWithDependenciesError> {
    let mut handles: Vec<JoinHandle<Result<(), ProcessEventsWithDependenciesError>>> = Vec::new();

    for contract_events in contract_events_config {
        let handle = tokio::spawn(async move {
            process_events_with_dependencies(
                contract_events.event_dependencies,
                contract_events.events_config,
            )
            .await
        });
        handles.push(handle);
    }

    let results = join_all(handles).await;

    for result in results {
        match result {
            Ok(inner_result) => inner_result.map_err(
                ProcessContractEventsWithDependenciesError::ProcessEventsWithDependenciesError,
            )?,
            Err(join_error) => {
                return Err(ProcessContractEventsWithDependenciesError::JoinError(
                    join_error,
                ))
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventsWithDependenciesError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),

    #[error("Event config not found")]
    EventConfigNotFound,

    #[error("Could not run all the logs processes {0}")]
    JoinError(#[from] JoinError),

    #[error("Could not parse topic id: {0}")]
    CouldNotParseTopicId(String),
}

async fn process_events_with_dependencies(
    dependencies: EventDependencies,
    events_processing_config: Vec<Arc<EventProcessingConfig>>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    process_events_dependency_tree(dependencies.tree, Arc::new(events_processing_config)).await
}

#[derive(Debug, Clone)]
struct OrderedLiveIndexingDetails {
    pub filter: RindexerEventFilter,
    pub last_seen_block_number: U64,
}

async fn process_events_dependency_tree(
    tree: Arc<EventsDependencyTree>,
    events_processing_config: Arc<Vec<Arc<EventProcessingConfig>>>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    let mut stack = vec![tree];

    let live_indexing_events = Arc::new(Mutex::new(Vec::<(ProcessLogsParams, H256)>::new()));

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
                        e.contract_name == dependency.contract_name
                            && e.event_name == dependency.event_name
                    })
                    .ok_or(ProcessEventsWithDependenciesError::EventConfigNotFound)?;

                let filter = RindexerEventFilter::new(
                    &event_processing_config.topic_id,
                    &event_processing_config.event_name,
                    &event_processing_config
                        .network_contract
                        .indexing_contract_setup,
                    event_processing_config.start_block,
                    event_processing_config.end_block,
                )?;

                let logs_params = ProcessLogsParams {
                    project_path: event_processing_config.project_path.clone(),
                    indexer_name: event_processing_config.indexer_name.clone(),
                    contract_name: event_processing_config.contract_name.clone(),
                    info_log_name: event_processing_config.info_log_name.clone(),
                    topic_id: event_processing_config.topic_id.clone(),
                    event_name: event_processing_config.event_name.clone(),
                    network_contract: Arc::clone(&event_processing_config.network_contract),
                    filter,
                    registry: Arc::clone(&event_processing_config.registry),
                    progress: Arc::clone(&event_processing_config.progress),
                    database: event_processing_config.database.clone(),
                    csv_details: event_processing_config.csv_details.clone(),
                    execute_events_logs_in_order: event_processing_config.index_event_in_order,
                    // sync the historic ones first and live indexing is added to the stack to process after
                    live_indexing: false,
                    indexing_distance_from_head: event_processing_config
                        .indexing_distance_from_head,
                    semaphore: Arc::clone(&event_processing_config.semaphore),
                };

                process_logs(logs_params.clone()).await?;

                if event_processing_config.live_indexing {
                    let topic_id =
                        event_processing_config
                            .topic_id
                            .parse::<H256>()
                            .map_err(|e| {
                                ProcessEventsWithDependenciesError::CouldNotParseTopicId(
                                    e.to_string(),
                                )
                            })?;
                    live_indexing_events
                        .lock()
                        .await
                        .push((logs_params.clone(), topic_id));
                }

                Ok::<(), ProcessEventsWithDependenciesError>(())
            });

            tasks.push(task);
        }

        let results = join_all(tasks).await;
        for result in results {
            if let Err(e) = result {
                error!("Error processing logs: {:?}", e);
                return Err(ProcessEventsWithDependenciesError::JoinError(e));
            }
        }

        // If there are more dependencies to process, push the next level onto the stack
        if let Some(next_tree) = &*current_tree.then {
            stack.push(Arc::clone(next_tree));
        }
    }

    let mut ordering_live_indexing_details_map: HashMap<
        H256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::new();
    let live_indexing_events = live_indexing_events.lock().await;
    for (log, topic) in live_indexing_events.iter() {
        let mut filter = log.filter.clone();
        let last_seen_block_number = filter.get_to_block();
        let next_block_number = last_seen_block_number + 1;

        filter = filter
            .set_from_block(next_block_number)
            .set_to_block(next_block_number);

        ordering_live_indexing_details_map.insert(
            *topic,
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
            })),
        );
    }

    if live_indexing_events.is_empty() {
        return Ok(());
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        for (logs_params, parsed_topic_id) in live_indexing_events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(parsed_topic_id)
                .expect("Failed to get ordering_live_indexing_details_map")
                .lock()
                .await
                .clone();

            let latest_block = logs_params
                .network_contract
                .cached_provider
                .get_latest_block()
                .await;

            match latest_block {
                Ok(latest_block) => {
                    if let Some(latest_block) = latest_block {
                        if let Some(latest_block_number) = latest_block.number {
                            let reorg_safe_distance = logs_params.indexing_distance_from_head;
                            if ordering_live_indexing_details.last_seen_block_number
                                == latest_block_number
                            {
                                debug!(
                                    "{} - {} - No new blocks to process...",
                                    logs_params.info_log_name,
                                    IndexingEventProgressStatus::Live.log()
                                );
                                continue;
                            }
                            info!(
                                "{} - {} - New block seen {} - Last seen block {}",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                latest_block_number,
                                ordering_live_indexing_details.last_seen_block_number
                            );
                            let safe_block_number = latest_block_number - reorg_safe_distance;

                            let from_block = ordering_live_indexing_details.filter.get_from_block();

                            // check reorg distance and skip if not safe
                            if from_block > safe_block_number {
                                info!(
                                    "{} - {} - not in safe reorg block range yet block: {} > range: {}",
                                    logs_params.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block,
                                    safe_block_number
                                );
                                continue;
                            }

                            let to_block = safe_block_number;

                            if from_block == to_block
                                && !is_relevant_block(
                                    &ordering_live_indexing_details.filter.raw_filter().address,
                                    parsed_topic_id,
                                    &latest_block,
                                )
                            {
                                debug!(
                                    "{} - {} - Skipping block {} as it's not relevant",
                                    logs_params.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block
                                );
                                info!(
                                    "{} - {} - LogsBloom check - No events found in the block {}",
                                    logs_params.info_log_name,
                                    IndexingEventProgressStatus::Live.log(),
                                    from_block
                                );

                                ordering_live_indexing_details.filter =
                                    ordering_live_indexing_details
                                        .filter
                                        .set_from_block(to_block + 1);

                                ordering_live_indexing_details.last_seen_block_number = to_block;
                                *ordering_live_indexing_details_map
                                    .get(parsed_topic_id)
                                    .expect("Failed to get ordering_live_indexing_details_map")
                                    .lock()
                                    .await = ordering_live_indexing_details;
                                continue;
                            }

                            ordering_live_indexing_details.filter =
                                ordering_live_indexing_details.filter.set_to_block(to_block);

                            debug!(
                                "{} - {} - Processing live filter: {:?}",
                                logs_params.info_log_name,
                                IndexingEventProgressStatus::Live.log(),
                                ordering_live_indexing_details.filter
                            );

                            let semaphore_client = Arc::clone(&logs_params.semaphore);
                            let permit = semaphore_client.acquire_owned().await;

                            if let Ok(permit) = permit {
                                match logs_params
                                    .network_contract
                                    .cached_provider
                                    .get_logs(&ordering_live_indexing_details.filter)
                                    .await
                                {
                                    Ok(logs) => {
                                        debug!(
                                            "{} - {} - Live topic_id {}, Logs: {} from {} to {}",
                                            logs_params.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            logs_params.topic_id,
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        debug!(
                                            "{} - {} - Fetched {} event logs - blocks: {} - {}",
                                            logs_params.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            logs.len(),
                                            from_block,
                                            to_block
                                        );

                                        let fetched_logs = Ok(FetchLogsResult {
                                            logs: logs.clone(),
                                            from_block,
                                            to_block,
                                        });

                                        let result = handle_logs_result(
                                            logs_params.project_path.clone(),
                                            logs_params.indexer_name.clone(),
                                            logs_params.contract_name.clone(),
                                            logs_params.event_name.clone(),
                                            logs_params.topic_id.clone(),
                                            logs_params.execute_events_logs_in_order,
                                            Arc::clone(&logs_params.progress),
                                            Arc::clone(&logs_params.network_contract),
                                            logs_params.database.clone(),
                                            logs_params.csv_details.clone(),
                                            Arc::clone(&logs_params.registry),
                                            fetched_logs,
                                        )
                                        .await;

                                        match result {
                                            Ok(_) => {
                                                ordering_live_indexing_details
                                                    .last_seen_block_number = to_block;
                                                if logs.is_empty() {
                                                    ordering_live_indexing_details.filter =
                                                        ordering_live_indexing_details
                                                            .filter
                                                            .set_from_block(to_block + 1);
                                                    info!(
                                                        "{} - {} - No events found between blocks {} - {}",
                                                        logs_params.info_log_name,
                                                        IndexingEventProgressStatus::Live.log(),
                                                        from_block,
                                                        to_block
                                                    );
                                                } else if let Some(last_log) = logs.last() {
                                                    if let Some(last_log_block_number) =
                                                        last_log.block_number
                                                    {
                                                        ordering_live_indexing_details.filter =
                                                            ordering_live_indexing_details
                                                                .filter
                                                                .set_from_block(
                                                                    last_log_block_number
                                                                        + U64::from(1),
                                                                );
                                                    } else {
                                                        error!("Failed to get last log block number the provider returned null (should never happen) - try again in 200ms");
                                                    }
                                                }

                                                *ordering_live_indexing_details_map
                                                    .get(parsed_topic_id)
                                                    .expect("Failed to get ordering_live_indexing_details_map")
                                                    .lock()
                                                    .await = ordering_live_indexing_details;

                                                drop(permit);
                                            }
                                            Err(err) => {
                                                error!(
                                                    "{} - {} - Error fetching logs: {} - will try again in 200ms",
                                                    logs_params.info_log_name,
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
                                            logs_params.info_log_name,
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

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(#[from] Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(#[from] BuildRindexerFilterError),
}

pub async fn process_event(
    event_processing_config: EventProcessingConfig,
) -> Result<(), ProcessEventError> {
    debug!(
        "{} - Processing events",
        event_processing_config.info_log_name
    );

    let filter = RindexerEventFilter::new(
        &event_processing_config.topic_id,
        &event_processing_config.event_name,
        &event_processing_config
            .network_contract
            .indexing_contract_setup,
        event_processing_config.start_block,
        event_processing_config.end_block,
    )?;

    process_logs(ProcessLogsParams {
        project_path: event_processing_config.project_path,
        indexer_name: event_processing_config.indexer_name,
        contract_name: event_processing_config.contract_name,
        info_log_name: event_processing_config.info_log_name,
        topic_id: event_processing_config.topic_id,
        event_name: event_processing_config.event_name,
        network_contract: event_processing_config.network_contract,
        filter,
        registry: event_processing_config.registry,
        progress: event_processing_config.progress,
        database: event_processing_config.database,
        csv_details: event_processing_config.csv_details,
        execute_events_logs_in_order: event_processing_config.index_event_in_order,
        live_indexing: event_processing_config.live_indexing,
        indexing_distance_from_head: event_processing_config.indexing_distance_from_head,
        semaphore: event_processing_config.semaphore,
    })
    .await?;

    Ok(())
}

#[derive(Clone)]
pub struct ProcessLogsParams {
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    info_log_name: String,
    topic_id: String,
    event_name: String,
    network_contract: Arc<NetworkContract>,
    filter: RindexerEventFilter,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    execute_events_logs_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
    semaphore: Arc<Semaphore>,
}

async fn process_logs(params: ProcessLogsParams) -> Result<(), Box<ProviderError>> {
    let provider = Arc::clone(&params.network_contract.cached_provider);
    let mut logs_stream = fetch_logs_stream(
        provider,
        params
            .topic_id
            .parse::<H256>()
            .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?,
        params.filter,
        params.info_log_name,
        if params.live_indexing {
            Some(LiveIndexingDetails {
                indexing_distance_from_head: params.indexing_distance_from_head,
            })
        } else {
            None
        },
        params.semaphore,
    );

    while let Some(result) = logs_stream.next().await {
        handle_logs_result(
            params.project_path.clone(),
            params.indexer_name.clone(),
            params.contract_name.clone(),
            params.event_name.clone(),
            params.topic_id.clone(),
            params.execute_events_logs_in_order,
            Arc::clone(&params.progress),
            Arc::clone(&params.network_contract),
            params.database.clone(),
            params.csv_details.clone(),
            Arc::clone(&params.registry),
            result,
        )
        .await
        .map_err(|e| Box::new(ProviderError::CustomError(e.to_string())))?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_logs_result(
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    event_name: String,
    topic_id: String,
    execute_events_logs_in_order: bool,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    registry: Arc<EventCallbackRegistry>,
    result: Result<FetchLogsResult, Box<dyn std::error::Error + Send>>,
) -> Result<(), Box<dyn std::error::Error + Send>> {
    match result {
        Ok(result) => {
            let fn_data = result
                .logs
                .iter()
                .map(|log| EventResult::new(Arc::clone(&network_contract), log))
                .collect::<Vec<_>>();

            debug!(
                "Processing logs {} - length {}",
                event_name,
                result.logs.len()
            );

            if !fn_data.is_empty() {
                if execute_events_logs_in_order {
                    registry.trigger_event(&topic_id, fn_data).await;
                    update_progress_and_last_synced(
                        project_path.clone(),
                        indexer_name.clone(),
                        contract_name,
                        event_name.clone(),
                        progress,
                        network_contract,
                        database,
                        csv_details,
                        result.to_block,
                    );
                } else {
                    tokio::spawn(async move {
                        registry.trigger_event(&topic_id, fn_data).await;
                        update_progress_and_last_synced(
                            project_path,
                            indexer_name.clone(),
                            contract_name,
                            event_name.clone(),
                            progress,
                            network_contract,
                            database,
                            csv_details,
                            result.to_block,
                        );
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
