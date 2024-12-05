use std::{path::Path, sync::Arc};

use ethers::{providers::ProviderError, types::U64};
use futures::future::try_join_all;
use tokio::{
    sync::Semaphore,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{error, info};

use crate::{
    database::postgres::client::PostgresConnectionError,
    event::{
        callback_registry::EventCallbackRegistry, config::EventProcessingConfig,
        contract_setup::NetworkContract,
    },
    indexer::{
        dependency::ContractEventsDependenciesConfig,
        last_synced::{get_last_synced_block_number, SyncConfig},
        process::{
            process_contracts_events_with_dependencies, process_event,
            ProcessContractsEventsWithDependenciesError, ProcessEventError,
        },
        progress::IndexingEventsProgressState,
        reorg::reorg_safe_distance_for_chain,
        ContractEventDependencies,
    },
    manifest::core::Manifest,
    PostgresClient,
};

#[derive(thiserror::Error, Debug)]
pub enum CombinedLogEventProcessingError {
    #[error("{0}")]
    DependencyError(#[from] ProcessContractsEventsWithDependenciesError),
    #[error("{0}")]
    NonBlockingError(#[from] ProcessEventError),
    #[error("{0}")]
    JoinError(#[from] JoinError),
}

#[derive(thiserror::Error, Debug)]
pub enum StartIndexingError {
    #[error("Could not run all index handlers join error: {0}")]
    CouldNotRunAllIndexHandlersJoin(#[from] JoinError),

    #[error("Could not run all index handlers {0}")]
    CouldNotRunAllIndexHandlers(#[from] ProcessEventError),

    #[error("{0}")]
    PostgresConnectionError(#[from] PostgresConnectionError),

    #[error("Could not get block number from provider: {0}")]
    GetBlockNumberError(#[from] ProviderError),

    #[error("Could not get chain id from provider: {0}")]
    GetChainIdError(ProviderError),

    #[error("Could not process event sequentially: {0}")]
    ProcessEventSequentiallyError(ProcessEventError),

    #[error("{0}")]
    CombinedError(#[from] CombinedLogEventProcessingError),

    #[error("The start block set for {0} is higher than the latest block: {1} - start block: {2}")]
    StartBlockIsHigherThanLatestBlockError(String, U64, U64),

    #[error("The end block set for {0} is higher than the latest block: {1} - end block: {2}")]
    EndBlockIsHigherThanLatestBlockError(String, U64, U64),
}

pub struct ProcessedNetworkContract {
    pub id: String,
    pub processed_up_to: U64,
}

pub async fn start_indexing(
    manifest: &Manifest,
    project_path: &Path,
    dependencies: &[ContractEventDependencies],
    no_live_indexing_forced: bool,
    registry: Arc<EventCallbackRegistry>,
) -> Result<Vec<ProcessedNetworkContract>, StartIndexingError> {
    let start = Instant::now();

    let database = initialize_database(manifest).await?;
    let event_progress_state = IndexingEventsProgressState::monitor(&registry.events).await;

    // we can bring this into the yaml file later if required
    let semaphore = Arc::new(Semaphore::new(100));
    // need this to keep track of dependency_events cross contracts and events
    let mut event_processing_configs: Vec<Arc<EventProcessingConfig>> = vec![];
    // any events which are non-blocking and can be fired in parallel
    let mut non_blocking_process_events = Vec::new();
    let mut dependency_event_processing_configs: Vec<ContractEventsDependenciesConfig> = Vec::new();
    // if you are doing advanced dependency events where other contracts depend on the processing of
    // this contract you will need to apply the dependency after the processing of the other
    // contract to avoid ordering issues
    let mut apply_cross_contract_dependency_events_config_after_processing = Vec::new();

    let mut processed_network_contracts: Vec<ProcessedNetworkContract> = Vec::new();

    for event in registry.events.iter() {
        let stream_details = manifest
            .contracts
            .iter()
            .find(|c| c.name == event.contract.name)
            .and_then(|c| c.streams.as_ref());

        for network_contract in event.contract.details.iter() {
            let config = SyncConfig {
                project_path,
                database: &database,
                csv_details: &manifest.storage.csv,
                contract_csv_enabled: manifest.contract_csv_enabled(&event.contract.name),
                stream_details: &stream_details,
                indexer_name: &event.indexer_name,
                contract_name: &event.contract.name,
                event_name: &event.event_name,
                network: &network_contract.network,
            };

            let latest_block = network_contract.cached_provider.get_block_number().await?;

            if let Some(start_block) = network_contract.start_block {
                if start_block > latest_block {
                    error!("{} - start_block supplied in yaml - {} {} is higher then latest block number - {}", event.info_log_name(), network_contract.network, start_block, latest_block);
                    return Err(StartIndexingError::StartBlockIsHigherThanLatestBlockError(
                        event.info_log_name().to_string(),
                        start_block,
                        latest_block,
                    ));
                }
            }

            if let Some(end_block) = network_contract.end_block {
                if end_block > latest_block {
                    error!("{} - end_block supplied in yaml - {} {} is higher then latest block number - {}", event.info_log_name(), network_contract.network, end_block, latest_block);
                    return Err(StartIndexingError::EndBlockIsHigherThanLatestBlockError(
                        event.info_log_name().to_string(),
                        end_block,
                        latest_block,
                    ));
                }
            }

            let last_known_start_block = if network_contract.start_block.is_some() {
                let last_synced_block = get_last_synced_block_number(config).await;

                if let Some(value) = last_synced_block {
                    let start_from = value + 1;
                    info!(
                        "{} Found last synced block number - {:?} rindexer will start up from {:?}",
                        event.info_log_name(),
                        value,
                        start_from
                    );
                    Some(start_from)
                } else {
                    None
                }
            } else {
                None
            };

            let start_block = last_known_start_block
                .unwrap_or(network_contract.start_block.unwrap_or(latest_block));
            info!("{} start_block is {}", event.info_log_name(), start_block);
            let end_block =
                std::cmp::min(network_contract.end_block.unwrap_or(latest_block), latest_block);
            if let Some(end_block) = network_contract.end_block {
                if end_block > latest_block {
                    error!("{} - end_block supplied in yaml - {} is higher then latest - {} - end_block now will be {}", event.info_log_name(), end_block, latest_block, latest_block);
                }
            }

            let (end_block, indexing_distance_from_head) = calculate_safe_block_number(
                event.contract.reorg_safe_distance,
                network_contract,
                latest_block,
                end_block,
            )
            .await?;

            // push status to the processed state
            processed_network_contracts.push(ProcessedNetworkContract {
                id: network_contract.id.clone(),
                processed_up_to: end_block,
            });

            let event_processing_config = EventProcessingConfig {
                id: event.id.clone(),
                project_path: project_path.to_path_buf(),
                indexer_name: event.indexer_name.clone(),
                contract_name: event.contract.name.clone(),
                info_log_name: event.info_log_name(),
                topic_id: event.topic_id,
                event_name: event.event_name.clone(),
                network_contract: Arc::new(network_contract.clone()),
                start_block,
                end_block,
                semaphore: Arc::clone(&semaphore),
                registry: Arc::clone(&registry),
                progress: Arc::clone(&event_progress_state),
                database: database.clone(),
                csv_details: manifest.storage.csv.clone(),
                stream_last_synced_block_file_path: stream_details
                    .as_ref()
                    .map(|s| s.get_streams_last_synced_block_path()),
                live_indexing: if no_live_indexing_forced {
                    false
                } else {
                    network_contract.is_live_indexing()
                },
                index_event_in_order: event.index_event_in_order,
                indexing_distance_from_head,
            };

            let dependencies_status = ContractEventDependencies::dependencies_status(
                &event_processing_config.contract_name,
                &event_processing_config.event_name,
                dependencies,
            );

            if dependencies_status.has_dependency_in_other_contracts_multiple_times() {
                panic!("Multiple dependencies of the same event on different contracts not supported yet - please raise an issue if you need this feature");
            }

            if dependencies_status.has_dependencies() {
                let event_processing_config_arc = Arc::new(event_processing_config);
                event_processing_configs.push(Arc::clone(&event_processing_config_arc));

                if let Some(dependency_in_other_contract) =
                    dependencies_status.get_first_dependencies_in_other_contracts()
                {
                    apply_cross_contract_dependency_events_config_after_processing
                        .push((dependency_in_other_contract, event_processing_config_arc));

                    continue;
                }

                ContractEventsDependenciesConfig::add_to_event_or_new_entry(
                    &mut dependency_event_processing_configs,
                    event_processing_config_arc,
                    dependencies,
                );
            } else {
                let process_event = tokio::spawn(process_event(event_processing_config, false));
                non_blocking_process_events.push(process_event);
            }
        }
    }

    // apply dependency events config after processing to avoid ordering issues
    for apply in apply_cross_contract_dependency_events_config_after_processing {
        let (dependency_in_other_contract, event_processing_config) = apply;
        ContractEventsDependenciesConfig::add_to_event_or_panic(
            &dependency_in_other_contract,
            &mut dependency_event_processing_configs,
            event_processing_config,
        );
    }

    let dependency_handle: JoinHandle<Result<(), ProcessContractsEventsWithDependenciesError>> =
        tokio::spawn(process_contracts_events_with_dependencies(
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

    let results = try_join_all(handles).await?;

    for result in results {
        match result {
            Ok(()) => {}
            Err(e) => return Err(StartIndexingError::CombinedError(e)),
        }
    }

    let duration = start.elapsed();

    info!("Historical indexing complete - time taken: {:?}", duration);

    Ok(processed_network_contracts)
}

async fn initialize_database(
    manifest: &Manifest,
) -> Result<Option<Arc<PostgresClient>>, StartIndexingError> {
    if manifest.storage.postgres_enabled() {
        match PostgresClient::new().await {
            Ok(postgres) => Ok(Some(Arc::new(postgres))),
            Err(e) => {
                error!("Error connecting to Postgres: {:?}", e);
                Err(StartIndexingError::PostgresConnectionError(e))
            }
        }
    } else {
        Ok(None)
    }
}

async fn calculate_safe_block_number(
    reorg_safe_distance: bool,
    network_contract: &NetworkContract,
    latest_block: U64,
    mut end_block: U64,
) -> Result<(U64, U64), StartIndexingError> {
    let mut indexing_distance_from_head = U64::zero();
    if reorg_safe_distance {
        let chain_id = network_contract
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
    Ok((end_block, indexing_distance_from_head))
}
