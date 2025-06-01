use std::{path::Path, sync::Arc, time::Duration};

use alloy::primitives::U64;
use futures::future::try_join_all;
use tokio::{
    join,
    sync::Semaphore,
    task::{JoinError, JoinHandle},
    time::{sleep, Instant},
};
use tracing::{error, info, warn};

use crate::{database::postgres::client::PostgresConnectionError, event::{
    callback_registry::{EventCallbackRegistry, TraceCallbackRegistry},
    config::{EventProcessingConfig, TraceProcessingConfig},
}, generate_random_id, indexer::{
    dependency::ContractEventsDependenciesConfig,
    last_synced::{get_last_synced_block_number, SyncConfig},
    native_transfer::{
        native_transfer_block_consumer, native_transfer_block_fetch, EVENT_NAME,
        NATIVE_TRANSFER_CONTRACT_NAME,
    },
    process::{
        process_contracts_events_with_dependencies, process_event,
        ProcessContractsEventsWithDependenciesError, ProcessEventError,
    },
    progress::IndexingEventsProgressState,
    reorg::reorg_safe_distance_for_chain,
    ContractEventDependencies,
}, manifest::core::Manifest, provider::{JsonRpcCachedProvider, ProviderError}, PostgresClient};
use crate::event::config::{ContractEventProcessingConfig, FactoryEventProcessingConfig};

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

    #[error("Encountered unknown error: {0}")]
    UnknownError(String),
}

#[derive(Clone)]
pub struct ProcessedNetworkContract {
    pub id: String,
    pub processed_up_to: U64,
}

async fn get_start_end_block(
    provider: Arc<JsonRpcCachedProvider>,
    manifest_start_block: Option<U64>,
    manifest_end_block: Option<U64>,
    config: SyncConfig<'_>,
    event_name: &str,
    network: &str,
    reorg_safe_distance: bool,
) -> Result<(U64, U64, U64), StartIndexingError> {
    let latest_block = provider.get_block_number().await?;

    if let Some(start_block) = manifest_start_block {
        if start_block > latest_block {
            error!(
                "{} - start_block supplied in yaml - {} {} is higher then latest block number - {}",
                event_name, network, start_block, latest_block
            );
            return Err(StartIndexingError::StartBlockIsHigherThanLatestBlockError(
                event_name.to_string(),
                start_block,
                latest_block,
            ));
        }
    }

    if let Some(end_block) = manifest_end_block {
        if end_block > latest_block {
            error!(
                "{} - end_block supplied in yaml - {} {} is higher then latest block number - {}",
                event_name, network, end_block, latest_block
            );
            return Err(StartIndexingError::EndBlockIsHigherThanLatestBlockError(
                event_name.to_string(),
                end_block,
                latest_block,
            ));
        }
    }

    let last_known_start_block = if manifest_start_block.is_some() {
        let last_synced_block = get_last_synced_block_number(config).await;

        if let Some(value) = last_synced_block {
            let start_from = value + U64::from(1);
            info!(
                "{} Found last synced block number - {:?} rindexer will start up from {:?}",
                event_name, value, start_from
            );
            Some(start_from)
        } else {
            None
        }
    } else {
        None
    };

    let start_block =
        last_known_start_block.unwrap_or(manifest_start_block.unwrap_or(latest_block));
    let end_block = std::cmp::min(manifest_end_block.unwrap_or(latest_block), latest_block);

    info!("[{}] {} Starting block number - {}", network, event_name, start_block);

    if let Some(end_block) = manifest_end_block {
        if end_block > latest_block {
            error!("{} - end_block supplied in yaml - {} is higher then latest - {} - end_block now will be {}", event_name, end_block, latest_block, latest_block);
        }
    }

    let (end_block, indexing_distance_from_head) =
        calculate_safe_block_number(reorg_safe_distance, &provider, latest_block, end_block)
            .await?;

    Ok((start_block, end_block, indexing_distance_from_head))
}

pub async fn start_indexing_traces(
    manifest: &Manifest,
    project_path: &Path,
    database: Option<Arc<PostgresClient>>,
    trace_registry: Arc<TraceCallbackRegistry>,
) -> Result<Vec<JoinHandle<Result<(), ProcessEventError>>>, StartIndexingError> {
    let mut non_blocking_process_events = Vec::new();
    let trace_progress_state =
        IndexingEventsProgressState::monitor_traces(&trace_registry.events).await;

    for event in trace_registry.events.iter() {
        let stream_details = manifest
            .contracts
            .iter()
            .find(|c| c.name == event.contract_name)
            .and_then(|c| c.streams.as_ref());

        for network in event.trace_information.details.iter() {
            let sync_config = SyncConfig {
                project_path,
                database: &database,
                csv_details: &manifest.storage.csv,
                contract_csv_enabled: manifest.contract_csv_enabled(&event.contract_name),
                stream_details: &stream_details,
                indexer_name: &event.indexer_name,
                contract_name: &event.contract_name,
                event_name: &event.event_name,
                network: &network.network,
            };

            let (block_tx, mut block_rx) = tokio::sync::mpsc::channel(8192);
            let network_name = network.network.clone();
            let (start_block, end_block, indexing_distance_from_head) = get_start_end_block(
                network.cached_provider.clone(),
                network.start_block,
                network.end_block,
                sync_config,
                &event.info_log_name(),
                &network.network,
                event.trace_information.reorg_safe_distance,
            )
            .await?;

            let config = Arc::new(TraceProcessingConfig {
                id: event.id.to_string(),
                project_path: project_path.to_path_buf(),
                start_block,
                end_block,
                indexer_name: event.indexer_name.clone(),
                contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
                event_name: EVENT_NAME.to_string(),
                network: network_name.to_string(),
                progress: trace_progress_state.clone(),
                database: database.clone(),
                csv_details: None,
                registry: trace_registry.clone(),
                method: network.method,
                stream_last_synced_block_file_path: None,
            });

            let native_transfer_handle = tokio::spawn(native_transfer_block_fetch(
                network.cached_provider.clone(),
                block_tx,
                start_block,
                network.end_block,
                indexing_distance_from_head,
                network_name.clone(),
            ));

            non_blocking_process_events.push(native_transfer_handle);

            let provider = network.cached_provider.clone();
            let config = config.clone();
            let native_transfer_consumer_handle = tokio::spawn(async move {
                // TODO: It would be nice to make the concurrent requests dynamic based on provider
                // speeds and limits. For now we can just increase slowly on success, and reduce
                // concurrency on failure.
                let mut max_concurrent_requests: usize = 100;
                let mut buffer: Vec<U64> = Vec::with_capacity(max_concurrent_requests);

                loop {
                    let recv = block_rx.recv_many(&mut buffer, max_concurrent_requests).await;

                    if recv == 0 {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }

                    let processed_block = native_transfer_block_consumer(
                        provider.clone(),
                        &buffer[..recv],
                        &network_name,
                        config.clone(),
                    )
                    .await;

                    // If this has an error we need to not and reconsume the blocks. We don't have
                    // to worry about double-publish because the failure point
                    // is on the provider call itself, which is before publish.
                    if let Err(e) = processed_block {
                        // On error, reset to original or half the search space.
                        max_concurrent_requests = std::cmp::max(100, max_concurrent_requests / 2);

                        warn!(
                            "Could not process '{}' block traces. Likely too early for {}..{}, Retrying: {}",
                            network_name,
                            &buffer.first().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                            &buffer.last().map(|n| n.as_limbs()[0]).unwrap_or_else(|| 0),
                            e.to_string().chars().take(2000).collect::<String>(),
                        );
                        continue;
                    } else {
                        buffer.clear();
                        max_concurrent_requests = (max_concurrent_requests as f64 * 1.25) as usize;
                    };
                }
            });

            non_blocking_process_events.push(native_transfer_consumer_handle);
        }
    }

    Ok(non_blocking_process_events)
}

pub async fn start_indexing_contract_events(
    manifest: &Manifest,
    project_path: &Path,
    database: Option<Arc<PostgresClient>>,
    registry: Arc<EventCallbackRegistry>,
    dependencies: &[ContractEventDependencies],
    no_live_indexing_forced: bool,
) -> Result<
    (
        Vec<JoinHandle<Result<(), ProcessEventError>>>,
        Vec<ProcessedNetworkContract>,
        Vec<(String, Arc<EventProcessingConfig>)>,
        Vec<ContractEventsDependenciesConfig>,
    ),
    StartIndexingError,
> {
    let event_progress_state = IndexingEventsProgressState::monitor(&registry.events).await;
    let semaphore = Arc::new(Semaphore::new(100));

    // need this to keep track of dependency_events cross contracts and events
    // if you are doing advanced dependency events where other contracts depend on the processing of
    // this contract you will need to apply the dependency after the processing of the other
    // contract to avoid ordering issues
    let mut apply_cross_contract_dependency_events_config_after_processing = Vec::new();
    let mut non_blocking_process_events = Vec::new();
    let mut processed_network_contracts: Vec<ProcessedNetworkContract> = Vec::new();
    let mut dependency_event_processing_configs: Vec<ContractEventsDependenciesConfig> = Vec::new();
    
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

            let (start_block, end_block, indexing_distance_from_head) = get_start_end_block(
                network_contract.cached_provider.clone(),
                network_contract.start_block,
                network_contract.end_block,
                config,
                &event.info_log_name(),
                &network_contract.network,
                event.contract.reorg_safe_distance,
            )
            .await?;

            // push status to the processed state
            processed_network_contracts.push(ProcessedNetworkContract {
                id: network_contract.id.clone(),
                processed_up_to: end_block,
            });

            let event_processing_config = ContractEventProcessingConfig {
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

            if let Some(factory_details) = network_contract.indexing_contract_setup.factory_details() {
                let factory_event_processing_config = FactoryEventProcessingConfig {
                    address: factory_details.address.clone(),
                    input_name: factory_details.input_name.clone(),
                    contract_name: factory_details.name.clone(),
                    project_path: project_path.to_path_buf(),
                    indexer_name: event.indexer_name.clone(),
                    event: factory_details.event(project_path),
                    network_contract: Arc::new(network_contract.clone()),
                    start_block,
                    end_block,
                    semaphore: Arc::clone(&semaphore),
                    progress: Arc::clone(&event_progress_state),
                    database: database.clone(),
                    csv_details: manifest.storage.csv.clone(),
                    stream_last_synced_block_file_path: stream_details
                        .as_ref()
                        .map(|s| s.get_streams_last_synced_block_path()),
                    live_indexing: event_processing_config.live_indexing,
                    index_event_in_order: event.index_event_in_order,
                    indexing_distance_from_head,
                };

                ContractEventsDependenciesConfig::add_to_event_or_new_entry(
                    &mut dependency_event_processing_configs,
                    Arc::new(event_processing_config.into()),
                    dependencies,
                );

                apply_cross_contract_dependency_events_config_after_processing
                    .push((event.contract.name.clone(), Arc::new(factory_event_processing_config.into())));

                continue;
            }

            // TODO: Fix above dependencies status
            let dependencies_status = ContractEventDependencies::dependencies_status(
                &event_processing_config.contract_name,
                &event_processing_config.event_name,
                dependencies,
            );

            if dependencies_status.has_dependency_in_other_contracts_multiple_times() {
                panic!("Multiple dependencies of the same event on different contracts not supported yet - please raise an issue if you need this feature");
            }

            if dependencies_status.has_dependencies() {
                if let Some(dependency_in_other_contract) =
                    dependencies_status.get_first_dependencies_in_other_contracts()
                {
                    apply_cross_contract_dependency_events_config_after_processing
                        .push((dependency_in_other_contract, Arc::new(event_processing_config.into())));

                    continue;
                }

                ContractEventsDependenciesConfig::add_to_event_or_new_entry(
                    &mut dependency_event_processing_configs,
                    Arc::new(event_processing_config.into()),
                    dependencies,
                );
            } else {
                let process_event = tokio::spawn(process_event(event_processing_config.into(), false));
                non_blocking_process_events.push(process_event);
            }
        }
    }

    Ok((
        non_blocking_process_events,
        processed_network_contracts,
        apply_cross_contract_dependency_events_config_after_processing,
        dependency_event_processing_configs,
    ))
}

pub async fn start_indexing(
    manifest: &Manifest,
    project_path: &Path,
    dependencies: &[ContractEventDependencies],
    no_live_indexing_forced: bool,
    registry: Arc<EventCallbackRegistry>,
    trace_registry: Arc<TraceCallbackRegistry>,
) -> Result<Vec<ProcessedNetworkContract>, StartIndexingError> {
    let start = Instant::now();
    let database = initialize_database(manifest).await?;

    // any events which are non-blocking and can be fired in parallel
    let mut non_blocking_process_events = Vec::new();

    // Start the sub-indexers concurrently to ensure fast startup times
    let (trace_indexer_handles, contract_events_indexer) = join!(
        start_indexing_traces(manifest, project_path, database.clone(), trace_registry.clone()),
        start_indexing_contract_events(
            manifest,
            project_path,
            database.clone(),
            registry.clone(),
            dependencies,
            no_live_indexing_forced,
        )
    );

    let (
        non_blocking_contract_handles,
        processed_network_contracts,
        apply_cross_contract_dependency_events_config_after_processing,
        mut dependency_event_processing_configs,
    ) = contract_events_indexer?;
    
    non_blocking_process_events.extend(trace_indexer_handles?);
    non_blocking_process_events.extend(non_blocking_contract_handles);

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

pub async fn initialize_database(
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

pub async fn calculate_safe_block_number(
    reorg_safe_distance: bool,
    provider: &Arc<JsonRpcCachedProvider>,
    latest_block: U64,
    mut end_block: U64,
) -> Result<(U64, U64), StartIndexingError> {
    let mut indexing_distance_from_head = U64::ZERO;
    if reorg_safe_distance {
        let chain_id =
            provider.get_chain_id().await.map_err(StartIndexingError::GetChainIdError)?;
        let reorg_safe_distance = reorg_safe_distance_for_chain(&chain_id);
        let safe_block_number = latest_block - reorg_safe_distance;
        if end_block > safe_block_number {
            end_block = safe_block_number;
        }
        indexing_distance_from_head = reorg_safe_distance;
    }
    Ok((end_block, indexing_distance_from_head))
}
