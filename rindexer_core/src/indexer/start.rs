use ethers::providers::ProviderError;
use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinError;
use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::database::postgres::PostgresConnectionError;
use crate::generator::event_callback_registry::{
    EventCallbackRegistry, EventInformation, EventResult, IndexingContractSetup, NetworkContract,
};
use crate::helpers::camel_to_snake;
use crate::indexer::fetch_logs::{fetch_logs_stream, FetchLogsStream, LiveIndexingDetails};
use crate::indexer::progress::IndexingEventsProgressState;
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::manifest::yaml::Manifest;
use crate::{EthereumSqlTypeWrapper, PostgresClient};

/// Settings for controlling concurrent processing of events.
pub struct ConcurrentSettings {
    max_concurrency: usize,
}

impl Default for ConcurrentSettings {
    fn default() -> Self {
        Self {
            max_concurrency: 100,
        }
    }
}

/// Settings for starting the indexing process.
pub struct StartIndexingSettings {
    concurrent: Option<ConcurrentSettings>,
    // TODO ADD TO YAML FILE
    execute_in_event_order: bool,
}

impl Default for StartIndexingSettings {
    fn default() -> Self {
        Self {
            concurrent: Some(ConcurrentSettings::default()),
            execute_in_event_order: false,
        }
    }
}

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
}

/// Starts the indexing process based on the provided settings and registry.
///
/// # Arguments
///
/// * `registry` - The event callback registry.
/// * `settings` - The settings for starting the indexing process.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub async fn start_indexing(
    manifest: &Manifest,
    registry: Arc<EventCallbackRegistry>,
    settings: StartIndexingSettings,
) -> Result<(), StartIndexingError> {
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

    let semaphore = Arc::new(Semaphore::new(
        settings
            .concurrent
            .map_or(ConcurrentSettings::default().max_concurrency, |c| {
                c.max_concurrency
            }),
    ));

    let mut handles = Vec::new();

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

            if settings.execute_in_event_order {
                process_event(event_processing_config)
                    .await
                    .map_err(StartIndexingError::ProcessEventSequentiallyError)?
            } else {
                let handle = tokio::spawn(process_event(event_processing_config));
                handles.push(handle);
            }
        }
    }

    for handle in handles {
        handle
            .await
            .map_err(StartIndexingError::CouldNotRunAllIndexHandlersJoin)?
            .map_err(StartIndexingError::CouldNotRunAllIndexHandlers)?;
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
        "{} - Processing event sequentially",
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
