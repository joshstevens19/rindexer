use std::{
    path::Path,
    sync::Arc,
};

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
    event::{callback_registry::EventCallbackRegistry, config::EventProcessingConfig},
    indexer::{
        dependency::ContractEventsDependenciesConfig,
        last_synced::get_last_synced_block_number,
        process::{
            process_contract_events_with_dependencies, process_event,
            ProcessContractEventsWithDependenciesError, ProcessEventError,
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
    DependencyError(#[from] ProcessContractEventsWithDependenciesError),
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
    let mut apply_cross_contract_dependency_events_config_after_processing: Vec<(
        String,
        Arc<EventProcessingConfig>,
    )> = Vec::new();

    let mut processed_network_contracts: Vec<ProcessedNetworkContract> = Vec::new();

    for event in registry.events.iter() {
        for network_contract in event.contract.details.iter() {
            let latest_block = network_contract.cached_provider.get_block_number().await?;

            let live_indexing =
                if no_live_indexing_forced { false } else { network_contract.end_block.is_none() };

            let contract_csv_enabled = manifest
                .contracts
                .iter()
                .find(|c| c.name == event.contract.name)
                .map_or(false, |c| c.generate_csv.unwrap_or(true));

            // if they are doing live indexing we just always go from the latest block
            let last_known_start_block = if network_contract.start_block.is_some() {
                get_last_synced_block_number(
                    project_path,
                    &database,
                    &manifest.storage.csv,
                    manifest.storage.csv_enabled() && contract_csv_enabled,
                    &event.indexer_name,
                    &event.contract.name,
                    &event.event_name,
                    &network_contract.network,
                )
                .await
            } else {
                None
            };

            let start_block = last_known_start_block
                .unwrap_or(network_contract.start_block.unwrap_or(latest_block));
            let mut indexing_distance_from_head = U64::zero();
            let mut end_block =
                std::cmp::min(network_contract.end_block.unwrap_or(latest_block), latest_block);

            if let Some(end_block) = network_contract.end_block {
                if end_block > latest_block {
                    error!("{} - end_block supplied in yaml - {} is higher then latest - {} - end_block now will be {}", event.info_log_name(), end_block, latest_block, latest_block);
                }
            }

            if event.contract.reorg_safe_distance {
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

            // push status to the processed state
            processed_network_contracts.push(ProcessedNetworkContract {
                id: network_contract.id.clone(),
                processed_up_to: end_block,
            });

            let event_processing_config = EventProcessingConfig {
                project_path: project_path.to_path_buf(),
                indexer_name: event.indexer_name.clone(),
                contract_name: event.contract.name.clone(),
                info_log_name: event.info_log_name(),
                topic_id: event.topic_id.clone(),
                event_name: event.event_name.clone(),
                network_contract: Arc::new(network_contract.clone()),
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

            let dependencies_status = ContractEventDependencies::dependencies_status(
                &event.contract.name,
                &event.event_name,
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
                    &event.contract.name,
                    &mut dependency_event_processing_configs,
                    event_processing_config_arc,
                    dependencies,
                );
            } else {
                let process_event = tokio::spawn(process_event(event_processing_config));
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
