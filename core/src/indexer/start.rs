use ethers::providers::ProviderError;
use ethers::types::U64;
use futures::future::try_join_all;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::task::{JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{debug, error, info};

use crate::database::postgres::PostgresConnectionError;
use crate::generator::event_callback_registry::{EventCallbackRegistry, EventInformation};
use crate::indexer::fetch_logs::{
    get_last_synced_block_number, process_contract_events_with_dependencies, process_event,
    ContractEventDependencies, ContractEventsConfig, EventProcessingConfig,
    ProcessContractEventsWithDependenciesError, ProcessEventError,
};
use crate::indexer::progress::IndexingEventsProgressState;
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::manifest::yaml::Manifest;
use crate::PostgresClient;

#[derive(thiserror::Error, Debug)]
pub enum CombinedLogEventProcessingError {
    #[error("{0}")]
    DependencyError(ProcessContractEventsWithDependenciesError),
    #[error("{0}")]
    NonBlockingError(ProcessEventError),
    #[error("{0}")]
    JoinError(JoinError),
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
    project_path: &PathBuf,
    dependencies: Vec<ContractEventDependencies>,
    registry: Arc<EventCallbackRegistry>,
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
                .cached_provider
                .get_block_number()
                .await
                .map_err(StartIndexingError::GetBlockNumberError)?;

            let live_indexing = contract.end_block.is_none();

            let contract_csv_enabled = manifest
                .contracts
                .iter()
                .find(|c| c.name == event.contract.name)
                .map_or(false, |c| c.generate_csv.unwrap_or(true));

            let last_known_start_block = get_last_synced_block_number(
                &project_path,
                database.clone(),
                &manifest.storage.csv,
                manifest.storage.csv_enabled() && contract_csv_enabled,
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
                    .cached_provider
                    .get_chain_id()
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
                project_path: project_path.clone(),
                indexer_name: event.indexer_name.clone(),
                contract_name: event.contract.name.clone(),
                info_log_name: event.info_log_name(),
                topic_id: event.topic_id.clone(),
                event_name: event.event_name.clone(),
                network_contract: Arc::new(contract),
                start_block,
                end_block,
                semaphore: Arc::clone(&semaphore),
                registry: Arc::clone(&registry),
                progress: Arc::clone(&event_progress_state),
                database: database.clone(),
                csv_details: manifest.storage.csv.clone(),
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

    let dependency_handle: JoinHandle<Result<(), ProcessContractEventsWithDependenciesError>> =
        tokio::spawn(process_contract_events_with_dependencies(
            dependency_event_processing_configs,
        ));

    let mut handles: Vec<JoinHandle<Result<(), CombinedLogEventProcessingError>>> = Vec::new();

    handles.push(tokio::spawn(async {
        dependency_handle
            .await
            .map_err(CombinedLogEventProcessingError::from)
            .and_then(|res| res.map_err(CombinedLogEventProcessingError::from))
    }));

    for handle in non_blocking_process_events {
        handles.push(tokio::spawn(async {
            handle
                .await
                .map_err(CombinedLogEventProcessingError::from)
                .and_then(|res| res.map_err(CombinedLogEventProcessingError::from))
        }));
    }

    let results = try_join_all(handles)
        .await
        .map_err(StartIndexingError::CouldNotRunAllIndexHandlersJoin)?;

    for result in results {
        match result {
            Ok(()) => {}
            Err(e) => return Err(StartIndexingError::CombinedError(e)),
        }
    }

    let duration = start.elapsed();

    info!("Indexing complete - time taken: {:?}", duration);

    // to avoid the thread closing before the stream is consumed
    // lets just sit here for 30 seconds to avoid the race
    // probably a better way to handle this but hey
    // TODO - handle this nicer
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    Ok(())
}
