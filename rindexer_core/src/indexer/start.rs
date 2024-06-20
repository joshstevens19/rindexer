use ethers::prelude::BlockNumber;
use ethers::providers::ProviderError;
use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use futures::future::{join_all, try_join_all};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::database::postgres::PostgresConnectionError;
use crate::generator::event_callback_registry::{
    EventCallbackRegistry, EventInformation, EventResult, IndexingContractSetup, NetworkContract,
};
use crate::helpers::camel_to_snake;
use crate::indexer::fetch_logs::{
    fetch_logs_stream, is_relevant_block, FetchLogsStream, LiveIndexingDetails,
};
use crate::indexer::progress::IndexingEventsProgressState;
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::indexer::IndexingEventProgressStatus;
use crate::manifest::yaml::{DependencyEventTree, Manifest};
use crate::{EthereumSqlTypeWrapper, PostgresClient};

struct EventProcessingConfig {
    indexer_name: String,
    contract_name: String,
    info_log_name: String,
    topic_id: String,
    event_name: String,
    network_contract: Arc<NetworkContract>,
    start_block: U64,
    end_block: U64,
    semaphore: Arc<Semaphore>,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Option<Arc<PostgresClient>>,
    index_event_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
}

#[derive(Debug, Clone)]
pub struct DependencyTree {
    pub events_name: Vec<String>,
    pub then: Box<Option<Arc<DependencyTree>>>,
}

impl DependencyTree {
    pub fn from_dependency_event_tree(event_tree: DependencyEventTree) -> Self {
        Self {
            events_name: event_tree.events,
            then: match event_tree.then {
                Some(children) if !children.is_empty() => Box::new(Some(Arc::new(
                    DependencyTree::from_dependency_event_tree(children[0].clone()),
                ))),
                _ => Box::new(None),
            },
        }
    }
}
#[derive(Debug, Clone)]
pub struct ContractEventDependencies {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
}

#[derive(Debug, Clone)]
pub struct EventDependencies {
    pub tree: Arc<DependencyTree>,
    pub dependency_event_names: Vec<String>,
}

impl EventDependencies {
    pub fn has_dependency(&self, event_name: &str) -> bool {
        self.dependency_event_names
            .contains(&event_name.to_string())
    }
}

struct ContractEventsConfig {
    pub contract_name: String,
    pub event_dependencies: EventDependencies,
    pub events_config: Vec<EventProcessingConfig>,
}

#[derive(thiserror::Error, Debug)]
pub enum CombinedLogEventProcessingError {
    #[error("{0}")]
    DependencyError(ProcessContractEventsWithDependenciesError),
    #[error("{0}")]
    NonBlockingError(ProcessEventError),
    #[error("{0}")]
    JoinError(tokio::task::JoinError),
}

impl From<ProcessContractEventsWithDependenciesError> for CombinedLogEventProcessingError {
    fn from(err: ProcessContractEventsWithDependenciesError) -> CombinedLogEventProcessingError {
        CombinedLogEventProcessingError::DependencyError(err)
    }
}

impl From<ProcessEventError> for CombinedLogEventProcessingError {
    fn from(err: ProcessEventError) -> CombinedLogEventProcessingError {
        CombinedLogEventProcessingError::NonBlockingError(err)
    }
}

impl From<JoinError> for CombinedLogEventProcessingError {
    fn from(err: JoinError) -> CombinedLogEventProcessingError {
        CombinedLogEventProcessingError::JoinError(err)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StartIndexingError {
    #[error("Could not run all index handlers join error: {0}")]
    CouldNotRunAllIndexHandlersJoin(JoinError),

    #[error("Could not run all index handlers {0}")]
    CouldNotRunAllIndexHandlers(ProcessEventError),

    #[error("{0}")]
    PostgresConnectionError(PostgresConnectionError),

    #[error("Could not get block number from provider: {0}")]
    GetBlockNumberError(ProviderError),

    #[error("Could not get chain id from provider: {0}")]
    GetChainIdError(ProviderError),

    #[error("Could not process event sequentially: {0}")]
    ProcessEventSequentiallyError(ProcessEventError),

    #[error("{0}")]
    CombinedError(CombinedLogEventProcessingError),
}

pub async fn start_indexing(
    manifest: &Manifest,
    dependencies: Vec<ContractEventDependencies>,
    registry: Arc<EventCallbackRegistry>,
) -> Result<(), StartIndexingError> {
    println!("dependencies... {:?}", dependencies);
    let start = Instant::now();

    let database = if manifest.storage.postgres_enabled() {
        let postgres = PostgresClient::new().await;
        match postgres {
            Ok(postgres) => Some(Arc::new(postgres)),
            Err(e) => {
                error!("Error connecting to Postgres: {:?}", e);
                return Err(StartIndexingError::PostgresConnectionError(e));
            }
        }
    } else {
        None
    };
    let event_progress_state = IndexingEventsProgressState::monitor(registry.events.clone()).await;

    // we can bring this into the yaml file later if required
    let semaphore = Arc::new(Semaphore::new(100));

    // any events which are non-blocking and can be fired in parallel
    let mut non_blocking_process_events = Vec::new();
    let mut dependency_event_processing_configs: Vec<ContractEventsConfig> = Vec::new();

    for event in registry.events.clone() {
        fn event_info(event: &EventInformation, message: &str) {
            debug!("{} - {}", event.info_log_name(), message);
        }

        for contract in event.contract.details.clone() {
            event_info(
                &event,
                &format!("Processing event on network {}", contract.network),
            );
            let latest_block = contract
                .provider
                .get_block_number()
                .await
                .map_err(StartIndexingError::GetBlockNumberError)?;
            let live_indexing = contract.end_block.is_none();
            let last_known_start_block = get_last_synced_block_number(
                database.clone(),
                &event.indexer_name,
                &event.contract.name,
                &event.event_name,
                &contract.network,
            )
            .await;

            let start_block =
                last_known_start_block.unwrap_or(contract.start_block.unwrap_or(latest_block));
            let mut indexing_distance_from_head = U64::zero();
            let mut end_block =
                std::cmp::min(contract.end_block.unwrap_or(latest_block), latest_block);

            if let Some(end_block) = contract.end_block {
                if end_block > latest_block {
                    error!("{} - end_block supplied in yaml - {} is higher then latest - {} - end_block now will be {}", event.info_log_name(), end_block, latest_block, latest_block);
                }
            }

            if event.contract.reorg_safe_distance {
                let chain_id = contract
                    .provider
                    .get_chainid()
                    .await
                    .map_err(StartIndexingError::GetChainIdError)?;
                let reorg_safe_distance = reorg_safe_distance_for_chain(&chain_id);
                let safe_block_number = latest_block - reorg_safe_distance;
                if end_block > safe_block_number {
                    end_block = safe_block_number;
                }
                indexing_distance_from_head = reorg_safe_distance;
            }

            if live_indexing {
                event_info(
                    &event,
                    &format!("Start block: {} and then will live index", start_block),
                );
            } else {
                event_info(
                    &event,
                    &format!("Start block: {}, End Block: {}", start_block, end_block),
                );
            }

            let event_processing_config = EventProcessingConfig {
                indexer_name: event.indexer_name.clone(),
                contract_name: event.contract.name.clone(),
                info_log_name: event.info_log_name(),
                topic_id: event.topic_id.clone(),
                event_name: event.event_name.clone(),
                network_contract: Arc::new(contract),
                start_block,
                end_block,
                semaphore: semaphore.clone(),
                registry: registry.clone(),
                progress: event_progress_state.clone(),
                database: database.clone(),
                live_indexing,
                index_event_in_order: event.index_event_in_order,
                indexing_distance_from_head,
            };

            if dependencies
                .iter()
                .find(|d| d.contract_name == event.contract.name)
                .map_or(false, |deps| {
                    deps.event_dependencies.has_dependency(&event.event_name)
                })
            {
                let contract_events_config = dependency_event_processing_configs
                    .iter_mut()
                    .find(|c| c.contract_name == event.contract.name);

                match contract_events_config {
                    Some(contract_events_config) => {
                        contract_events_config
                            .events_config
                            .push(event_processing_config);
                    }
                    None => {
                        dependency_event_processing_configs.push(ContractEventsConfig {
                            contract_name: event.contract.name.clone(),
                            event_dependencies: dependencies
                                .iter()
                                .find(|d| d.contract_name == event.contract.name)
                                .unwrap()
                                .event_dependencies
                                .clone(),
                            events_config: vec![event_processing_config],
                        });
                    }
                }
            } else {
                let process_event = tokio::spawn(process_event(event_processing_config));
                non_blocking_process_events.push(process_event);
            }
        }
    }

    println!(
        "dependency_event_processing_configs: {:?}",
        dependency_event_processing_configs
            .iter()
            .map(|e| (e.contract_name.clone(), e.event_dependencies.clone()))
            .collect::<Vec<_>>()
    );

    let dependency_handle: JoinHandle<Result<(), ProcessContractEventsWithDependenciesError>> =
        tokio::spawn(process_contract_events_with_dependencies(
            dependency_event_processing_configs,
        ));

    // Create a vector to hold both types of handles
    let mut handles: Vec<JoinHandle<Result<(), CombinedLogEventProcessingError>>> = Vec::new();

    // Add dependency_handle to handles
    handles.push(tokio::spawn(async {
        dependency_handle
            .await
            .map_err(CombinedLogEventProcessingError::from)
            .and_then(|res| res.map_err(CombinedLogEventProcessingError::from))
    }));

    println!(
        "non_blocking_process_events: {:?}",
        non_blocking_process_events.len()
    );

    // Add non-blocking process events handles to handles
    for handle in non_blocking_process_events {
        handles.push(tokio::spawn(async {
            handle
                .await
                .map_err(CombinedLogEventProcessingError::from)
                .and_then(|res| res.map_err(CombinedLogEventProcessingError::from))
        }));
    }

    // Use try_join_all to wait for all the futures to complete
    let results = try_join_all(handles)
        .await
        .map_err(StartIndexingError::CouldNotRunAllIndexHandlersJoin)?;
    // Handle the results
    for result in results {
        match result {
            Ok(()) => {}
            Err(e) => return Err(StartIndexingError::CombinedError(e)),
        }
    }

    let duration = start.elapsed();

    info!("Indexing complete - time taken: {:?}", duration);

    info!("Will shutdown in 30 seconds..");

    // to avoid the thread closing before the stream is consumed
    // lets just sit here for 30 seconds to avoid the race
    // probably a better way to handle this but hey
    // TODO handle this nicer
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessContractEventsWithDependenciesError {
    #[error("{0}")]
    ProcessEventsWithDependenciesError(ProcessEventsWithDependenciesError),

    #[error("{0}")]
    JoinError(JoinError),
}

async fn process_contract_events_with_dependencies(
    contract_events_config: Vec<ContractEventsConfig>,
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
    ProcessLogs(Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(BuildFilterError),

    #[error("Event config not found")]
    EventConfigNotFound,
}

async fn process_events_with_dependencies(
    dependencies: EventDependencies,
    events_processing_config: Vec<EventProcessingConfig>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    process_dependency_tree(dependencies.tree, Arc::new(events_processing_config)).await
}

#[derive(Debug, Clone)]
struct OrderedLiveIndexingDetails {
    pub filter: Filter,
    pub last_seen_block_number: U64,
}

async fn process_dependency_tree(
    tree: Arc<DependencyTree>,
    events_processing_config: Arc<Vec<EventProcessingConfig>>,
) -> Result<(), ProcessEventsWithDependenciesError> {
    let mut stack = vec![tree];

    let live_indexing_events = Arc::new(Mutex::new(Vec::<(ProcessLogsParams, H256)>::new()));

    while let Some(current_tree) = stack.pop() {
        println!("Processing: {:?}", current_tree.events_name);

        let mut tasks = vec![];

        for dependency in &current_tree.events_name {
            println!("Processing: {:?}", dependency);

            let event_processing_config = events_processing_config.clone(); // Clone the Arc
            let dependency = dependency.clone();
            let live_indexing_events = Arc::clone(&live_indexing_events);

            let task = tokio::spawn(async move {
                let event_processing_config = event_processing_config
                    .iter()
                    .find(|e| e.event_name == dependency)
                    .ok_or(ProcessEventsWithDependenciesError::EventConfigNotFound)?;

                let filter = build_filter(
                    &event_processing_config.topic_id,
                    &event_processing_config
                        .network_contract
                        .indexing_contract_setup,
                    event_processing_config.start_block,
                    event_processing_config.end_block,
                )
                .map_err(ProcessEventsWithDependenciesError::BuildFilterError)?;

                let logs_params = ProcessLogsParams {
                    indexer_name: event_processing_config.indexer_name.clone(),
                    contract_name: event_processing_config.contract_name.clone(),
                    info_log_name: event_processing_config.info_log_name.clone(),
                    topic_id: event_processing_config.topic_id.clone(),
                    event_name: event_processing_config.event_name.clone(),
                    network_contract: event_processing_config.network_contract.clone(),
                    filter,
                    registry: event_processing_config.registry.clone(),
                    progress: event_processing_config.progress.clone(),
                    database: event_processing_config.database.clone(),
                    execute_events_logs_in_order: event_processing_config.index_event_in_order,
                    live_indexing: false, // sync the historic ones first
                    indexing_distance_from_head: event_processing_config
                        .indexing_distance_from_head,
                    semaphore: event_processing_config.semaphore.clone(),
                };

                process_logs(logs_params.clone())
                    .await
                    .map_err(ProcessEventsWithDependenciesError::ProcessLogs)?;
                
                if logs_params.live_indexing {
                    let topic_id = event_processing_config.topic_id.parse::<H256>().unwrap();
                    live_indexing_events
                        .lock()
                        .await
                        .push((logs_params.clone(), topic_id));
                }

                Ok::<(), ProcessEventsWithDependenciesError>(())
            });

            tasks.push(task);
        }

        // Await all tasks to complete
        let results = join_all(tasks).await;
        for result in results {
            if let Err(e) = result {
                // Handle individual task errors if needed
                eprintln!("Error processing dependency: {:?}", e);
            }
        }

        // If there are more dependencies to process, push the next level onto the stack
        if let Some(next_tree) = &*current_tree.then {
            stack.push(next_tree.clone());
        }
    }

    let mut ordering_live_indexing_details_map: HashMap<
        H256,
        Arc<Mutex<OrderedLiveIndexingDetails>>,
    > = HashMap::new();
    let live_indexing_events = live_indexing_events.lock().await;
    for (log, topic) in live_indexing_events.iter() {
        let mut filter = log.filter.clone();
        let last_seen_block_number = filter.get_to_block().unwrap();
        let next_block_number = last_seen_block_number + 1;
        filter = filter
            .from_block(next_block_number)
            .to_block(next_block_number);
        ordering_live_indexing_details_map.insert(
            *topic,
            Arc::new(Mutex::new(OrderedLiveIndexingDetails {
                filter,
                last_seen_block_number,
            })),
        );
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        for (logs_params, parsed_topic_id) in live_indexing_events.iter() {
            let mut ordering_live_indexing_details = ordering_live_indexing_details_map
                .get(parsed_topic_id)
                .unwrap()
                .lock()
                .await
                .clone();

            let reorg_safe_distance = logs_params.indexing_distance_from_head;
            if let Some(latest_block) = logs_params
                .network_contract
                .provider
                .get_block(BlockNumber::Latest)
                .await
                .unwrap()
            {
                let latest_block_number = latest_block.number.unwrap();
                if ordering_live_indexing_details.last_seen_block_number == latest_block_number {
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

                let from_block = ordering_live_indexing_details.filter.get_from_block().unwrap();
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
                        &ordering_live_indexing_details.filter.address,
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
                    ordering_live_indexing_details.filter = ordering_live_indexing_details
                        .filter
                        .from_block(to_block + 1);
                    ordering_live_indexing_details.last_seen_block_number = to_block;
                    *ordering_live_indexing_details_map
                        .get(parsed_topic_id)
                        .unwrap()
                        .lock()
                        .await = ordering_live_indexing_details;
                    continue;
                }

                ordering_live_indexing_details.filter =
                    ordering_live_indexing_details.filter.to_block(to_block);

                debug!(
                    "{} - {} - Processing live filter: {:?}",
                    logs_params.info_log_name,
                    IndexingEventProgressStatus::Live.log(),
                    ordering_live_indexing_details.filter
                );

                let semaphore_client = logs_params.semaphore.clone();
                let permit = semaphore_client.acquire_owned().await;

                if let Ok(permit) = permit {
                    match logs_params
                        .network_contract
                        .provider
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

                            let fetched_logs = Ok(FetchLogsStream {
                                logs: logs.clone(),
                                from_block,
                                to_block,
                            });

                            let result = handle_logs_result(
                                logs_params.indexer_name.clone(),
                                logs_params.contract_name.clone(),
                                logs_params.event_name.clone(),
                                logs_params.topic_id.clone(),
                                logs_params.execute_events_logs_in_order,
                                logs_params.progress.clone(),
                                logs_params.network_contract.clone(),
                                logs_params.database.clone(),
                                logs_params.registry.clone(),
                                fetched_logs,
                            )
                            .await;

                            match result {
                                Ok(_) => {
                                    ordering_live_indexing_details.last_seen_block_number =
                                        to_block;
                                    if logs.is_empty() {
                                        ordering_live_indexing_details.filter =
                                            ordering_live_indexing_details
                                                .filter
                                                .from_block(to_block + 1);
                                        info!(
                                            "{} - {} - No events found between blocks {} - {}",
                                            logs_params.info_log_name,
                                            IndexingEventProgressStatus::Live.log(),
                                            from_block,
                                            to_block
                                        );
                                    } else if let Some(last_log) = logs.last() {
                                        ordering_live_indexing_details.filter =
                                            ordering_live_indexing_details.filter.from_block(
                                                last_log.block_number.unwrap() + U64::from(1),
                                            );
                                    }

                                    *ordering_live_indexing_details_map
                                        .get(parsed_topic_id)
                                        .unwrap()
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
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessEventError {
    #[error("Could not process logs: {0}")]
    ProcessLogs(Box<ProviderError>),

    #[error("Could not build filter: {0}")]
    BuildFilterError(BuildFilterError),
}

async fn process_event(
    event_processing_config: EventProcessingConfig,
) -> Result<(), ProcessEventError> {
    debug!(
        "{} - Processing events",
        event_processing_config.info_log_name
    );

    let filter = build_filter(
        &event_processing_config.topic_id,
        &event_processing_config
            .network_contract
            .indexing_contract_setup,
        event_processing_config.start_block,
        event_processing_config.end_block,
    )
    .map_err(ProcessEventError::BuildFilterError)?;

    process_logs(ProcessLogsParams {
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
        execute_events_logs_in_order: event_processing_config.index_event_in_order,
        live_indexing: event_processing_config.live_indexing,
        indexing_distance_from_head: event_processing_config.indexing_distance_from_head,
        semaphore: event_processing_config.semaphore,
    })
    .await
    .map_err(ProcessEventError::ProcessLogs)?;

    Ok(())
}

/// Parameters for processing logs.
#[derive(Clone)]
pub struct ProcessLogsParams {
    indexer_name: String,
    contract_name: String,
    info_log_name: String,
    topic_id: String,
    event_name: String,
    network_contract: Arc<NetworkContract>,
    filter: Filter,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Option<Arc<PostgresClient>>,
    execute_events_logs_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
    semaphore: Arc<Semaphore>,
}

/// Processes logs based on the given parameters.
///
/// # Arguments
///
/// * `params` - The parameters for processing logs.
///
/// # Returns
///
/// A `Result` indicating success or failure.
async fn process_logs(params: ProcessLogsParams) -> Result<(), Box<ProviderError>> {
    let provider = Arc::new(params.network_contract.provider.clone());
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
            params.indexer_name.clone(),
            params.contract_name.clone(),
            params.event_name.clone(),
            params.topic_id.clone(),
            params.execute_events_logs_in_order,
            params.progress.clone(),
            params.network_contract.clone(),
            params.database.clone(),
            params.registry.clone(),
            result,
        )
        .await?;
    }

    Ok(())
}

/// Handles the result of fetching logs.
///
/// # Arguments
///
/// * `indexer_name` - The name of the indexer.
/// * `event_name` - The name of the event.
/// * `topic_id` - The ID of the topic.
/// * `execute_events_logs_in_order` - Whether to execute logs in order.
/// * `progress` - The progress state.
/// * `network_contract` - The network contract.
/// * `database` - The database client.
/// * `registry` - The event callback registry.
/// * `result` - The result of fetching logs.
///
/// # Returns
///
/// A `Result` indicating success or failure.
#[allow(clippy::too_many_arguments)]
async fn handle_logs_result(
    indexer_name: String,
    contract_name: String,
    event_name: String,
    topic_id: String,
    execute_events_logs_in_order: bool,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    registry: Arc<EventCallbackRegistry>,
    result: Result<FetchLogsStream, Box<ProviderError>>,
) -> Result<(), Box<ProviderError>> {
    match result {
        Ok(result) => {
            let fn_data = result
                .logs
                .iter()
                .map(|log| EventResult::new(network_contract.clone(), log))
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
                        indexer_name.clone(),
                        contract_name,
                        event_name.clone(),
                        progress,
                        network_contract,
                        database,
                        result.to_block,
                    );
                } else {
                    tokio::spawn(async move {
                        registry.trigger_event(&topic_id, fn_data).await;
                        update_progress_and_last_synced(
                            indexer_name.clone(),
                            contract_name,
                            event_name.clone(),
                            progress,
                            network_contract,
                            database,
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

/// Retrieves the last synced block number from the database.
///
/// # Arguments
///
/// * `database` - The database client.
/// * `indexer_name` - The name of the indexer.
/// * `contract_name` - The name of the contract.
/// * `event_name` - The name of the event.
/// * `network` - The network.
///
/// # Returns
///
/// An `Option` containing the last synced block number, if available.
async fn get_last_synced_block_number(
    database: Option<Arc<PostgresClient>>,
    indexer_name: &str,
    contract_name: &str,
    event_name: &str,
    network: &str,
) -> Option<U64> {
    match database {
        Some(database) => {
            let query = format!(
                "SELECT last_synced_block FROM rindexer_internal.{}_{}_{} WHERE network = $1",
                camel_to_snake(indexer_name),
                camel_to_snake(contract_name),
                camel_to_snake(event_name)
            );

            let row = database.query_one(&query, &[&network]).await;
            match row {
                Ok(row) => {
                    // TODO UNCOMMENT
                    // let result: Decimal = row.get("last_synced_block");
                    // Some(U64::from_dec_str(&result.to_string()).unwrap())
                    None
                }
                Err(e) => {
                    error!("Error fetching last synced block: {:?}", e);
                    None
                }
            }
        }
        None => None,
    }
}

/// Updates the progress and the last synced block number.
///
/// # Arguments
///
/// * `indexer_name` - The name of the indexer.
/// * `event_name` - The name of the event.
/// * `progress` - The progress state.
/// * `network_contract` - The network contract.
/// * `database` - The database client.
/// * `to_block` - The block number to update to.
fn update_progress_and_last_synced(
    indexer_name: String,
    contract_name: String,
    event_name: String,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    to_block: U64,
) {
    tokio::spawn(async move {
        progress
            .lock()
            .await
            .update_last_synced_block(&network_contract.id, to_block);

        if let Some(database) = database {
            let result = database
                .execute(
                    &format!(
                        "UPDATE rindexer_internal.{}_{}_{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                        camel_to_snake(&indexer_name),
                        camel_to_snake(&contract_name),
                        camel_to_snake(&event_name)
                    ),
                    &[
                        &EthereumSqlTypeWrapper::U64(to_block),
                        &network_contract.network,
                    ],
                )
                .await;

            if let Err(e) = result {
                error!("Error updating last synced block: {:?}", e);
            }
        }
    });
}

#[derive(thiserror::Error, Debug)]
pub enum BuildFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,

    #[error("Topic0 is valid format")]
    Topic0InvalidFormat,
}

/// Builds a filter for fetching logs.
///
/// # Arguments
///
/// * `topic_id` - The ID of the topic.
/// * `indexing_contract_setup` - The setup of the indexing contract.
/// * `current_block` - The current block number.
/// * `next_block` - The next block number.
///
/// # Returns
///
/// A `Filter` for fetching logs.
fn build_filter(
    topic_id: &str,
    indexing_contract_setup: &IndexingContractSetup,
    current_block: U64,
    next_block: U64,
) -> Result<Filter, BuildFilterError> {
    match indexing_contract_setup {
        IndexingContractSetup::Address(address) => {
            let address = address
                .parse::<Address>()
                .map_err(|_| BuildFilterError::AddressInvalidFormat)?;
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(Filter::new()
                .address(address)
                .topic0(topic0)
                .from_block(current_block)
                .to_block(next_block))
        }
        IndexingContractSetup::Filter(filter) => {
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(filter.extend_filter_indexed(
                Filter::new()
                    .topic0(topic0)
                    .from_block(current_block)
                    .to_block(next_block),
            ))
        }
        IndexingContractSetup::Factory(factory) => {
            let address = factory
                .address
                .parse::<Address>()
                .map_err(|_| BuildFilterError::AddressInvalidFormat)?;
            let topic0 = topic_id
                .parse::<H256>()
                .map_err(|_| BuildFilterError::Topic0InvalidFormat)?;

            Ok(Filter::new()
                .address(address)
                .topic0(topic0)
                .from_block(current_block)
                .to_block(next_block))
        }
    }
}
