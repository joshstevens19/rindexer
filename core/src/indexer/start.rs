use std::{path::Path, sync::Arc};

use alloy::primitives::U64;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::{
    join,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tracing::{error, info};

use crate::database::clickhouse::client::{ClickhouseClient, ClickhouseConnectionError};
use crate::event::config::{ContractEventProcessingConfig, FactoryEventProcessingConfig};
use crate::indexer::native_transfer::native_transfer_block_processor;
use crate::indexer::Indexer;
use crate::{
    database::postgres::client::PostgresConnectionError,
    event::{
        callback_registry::{EventCallbackRegistry, TraceCallbackRegistry},
        config::{EventProcessingConfig, TraceProcessingConfig},
    },
    indexer::{
        dependency::ContractEventsDependenciesConfig,
        last_synced::{get_last_synced_block_number, SyncConfig},
        native_transfer::{native_transfer_block_fetch, NATIVE_TRANSFER_CONTRACT_NAME},
        process::{
            process_contracts_events_with_dependencies, process_non_blocking_event,
            ProcessContractsEventsWithDependenciesError, ProcessEventError,
        },
        progress::IndexingEventsProgressState,
        reorg::reorg_safe_distance_for_chain,
        ContractEventDependencies,
    },
    manifest::core::Manifest,
    provider::{JsonRpcCachedProvider, ProviderError},
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

    #[error("{0}")]
    ClickhouseConnectionError(#[from] ClickhouseConnectionError),

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

    info!("{}::{} Starting block number - {}", event_name, network, start_block);

    if let Some(end_block) = manifest_end_block {
        if end_block > latest_block {
            error!("{} - end_block supplied in yaml - {} is higher then latest - {} - end_block now will be {}", event_name, end_block, latest_block, latest_block);
        }
    }

    let (end_block, indexing_distance_from_head) =
        calculate_safe_block_number(reorg_safe_distance, &provider, latest_block, end_block);

    Ok((start_block, end_block, indexing_distance_from_head))
}

pub async fn start_indexing_traces(
    manifest: &Manifest,
    project_path: &Path,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    indexer: &Indexer,
    trace_registry: Arc<TraceCallbackRegistry>,
) -> Result<Vec<JoinHandle<Result<(), ProcessEventError>>>, StartIndexingError> {
    if !manifest.native_transfers.enabled {
        info!("Native transfer indexing disabled!");
        return Ok(vec![]);
    }

    let mut non_blocking_process_events = Vec::new();
    let trace_progress_state =
        IndexingEventsProgressState::monitor_traces(&trace_registry.events).await;

    // Group events by network to create one pipeline per network
    let mut network_events: std::collections::HashMap<
        String,
        Vec<&crate::event::callback_registry::TraceCallbackRegistryInformation>,
    > = std::collections::HashMap::new();

    for event in trace_registry.events.iter() {
        for network in event.trace_information.details.iter() {
            network_events.entry(network.network.clone()).or_default().push(event);
        }
    }

    // Create one pipeline per network
    for (network_name, events) in network_events {
        // Get the first event's network details (they should all be the same for a given network)
        let first_event = events.first().unwrap();
        let network_details = first_event
            .trace_information
            .details
            .iter()
            .find(|n| n.network == network_name)
            .unwrap();

        let stream_details = indexer
            .contracts
            .iter()
            .find(|c| c.name == first_event.contract_name)
            .and_then(|c| c.streams.as_ref());

        let sync_config = SyncConfig {
            project_path,
            postgres: &postgres,
            clickhouse: &clickhouse,
            csv_details: &manifest.storage.csv,
            contract_csv_enabled: manifest.contract_csv_enabled(&first_event.contract_name),
            stream_details: &stream_details,
            indexer_name: &first_event.indexer_name,
            contract_name: &first_event.contract_name,
            event_name: &first_event.event_name,
            network: &network_name,
        };

        let (block_tx, block_rx) = tokio::sync::mpsc::channel(4096);
        let (start_block, end_block, indexing_distance_from_head) = get_start_end_block(
            network_details.cached_provider.clone(),
            network_details.start_block,
            network_details.end_block,
            sync_config,
            &format!("TraceEvents[{}]", network_name),
            &network_name,
            first_event.trace_information.reorg_safe_distance,
        )
        .await?;

        // Create a shared registry for this network's events
        let network_registry = Arc::new(TraceCallbackRegistry {
            events: events.iter().map(|e| (*e).clone()).collect(),
        });

        let config = Arc::new(TraceProcessingConfig {
            id: first_event.id.clone(), // Use the first event's ID for progress tracking
            project_path: project_path.to_path_buf(),
            start_block,
            end_block,
            indexer_name: first_event.indexer_name.clone(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: "TraceEvents".to_string(),
            network: network_name.clone(),
            progress: trace_progress_state.clone(),
            postgres: postgres.clone(),
            csv_details: None,
            registry: network_registry,
            method: network_details.method,
            stream_last_synced_block_file_path: None,
        });

        let block_fetch_handle = tokio::spawn(native_transfer_block_fetch(
            network_details.cached_provider.clone(),
            block_tx,
            start_block,
            network_details.end_block,
            indexing_distance_from_head,
            network_name.clone(),
        ));

        non_blocking_process_events.push(block_fetch_handle);

        let provider = network_details.cached_provider.clone();
        let config = config.clone();

        let block_processor_handle =
            tokio::spawn(native_transfer_block_processor(network_name, provider, config, block_rx));

        non_blocking_process_events.push(block_processor_handle);
    }

    Ok(non_blocking_process_events)
}

#[allow(clippy::too_many_arguments)]
pub async fn start_indexing_contract_events(
    manifest: &Manifest,
    project_path: &Path,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    indexer: &Indexer,
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

    let mut apply_cross_contract_dependency_events_config_after_processing = Vec::new();
    let mut non_blocking_process_events = Vec::new();
    let mut processed_network_contracts: Vec<ProcessedNetworkContract> = Vec::new();
    let mut dependency_event_processing_configs: Vec<ContractEventsDependenciesConfig> = Vec::new();

    let mut block_tasks = FuturesUnordered::new();

    if let Some(true) = manifest.timestamps {
        info!("Block timestamps enabled globally!");
    }

    for event in registry.events.iter() {
        let stream_details = indexer
            .contracts
            .iter()
            .find(|c| c.name == event.contract.name)
            .and_then(|c| c.streams.as_ref());

        for network_contract in event.contract.details.iter() {
            let event = event.clone();
            let network_contract = network_contract.clone();
            let project_path = project_path.to_path_buf();
            let postgres = postgres.clone();
            let clickhouse = clickhouse.clone();
            let manifest_csv_details = manifest.storage.csv.clone();
            let registry = Arc::clone(&registry);
            let event_progress_state = Arc::clone(&event_progress_state);
            let dependencies = dependencies.to_vec();

            block_tasks.push(async move {
                let config = SyncConfig {
                    project_path: &project_path,
                    postgres: &postgres,
                    clickhouse: &clickhouse,
                    csv_details: &manifest_csv_details,
                    contract_csv_enabled: manifest.contract_csv_enabled(&event.contract.name),
                    stream_details: &stream_details,
                    indexer_name: &event.indexer_name,
                    contract_name: &event.contract.name,
                    event_name: &event.event_name,
                    network: &network_contract.network,
                };

                let result = get_start_end_block(
                    network_contract.cached_provider.clone(),
                    network_contract.start_block,
                    network_contract.end_block,
                    config,
                    &event.info_log_name(),
                    &network_contract.network,
                    event.contract.reorg_safe_distance,
                )
                .await;

                result.map(|blocks| {
                    (
                        event,
                        network_contract,
                        stream_details,
                        blocks,
                        project_path,
                        postgres,
                        clickhouse,
                        manifest_csv_details,
                        registry,
                        event_progress_state,
                        no_live_indexing_forced,
                        dependencies,
                    )
                })
            });
        }
    }

    while let Some(res) = block_tasks.next().await {
        let (
            event,
            network_contract,
            stream_details,
            (start_block, end_block, indexing_distance_from_head),
            project_path,
            postgres,
            clickhouse,
            manifest_csv_details,
            registry,
            event_progress_state,
            no_live_indexing_forced,
            dependencies,
        ) = res?;

        processed_network_contracts.push(ProcessedNetworkContract {
            id: network_contract.id.clone(),
            processed_up_to: end_block,
        });

        // TODO: doesnt work with factory atm so leave overrides to fix later as breaks the world
        // let contract = manifest
        //     .contracts
        //     .iter()
        //     .find(|c| {
        //         format!("{}Filter", c.name) == event.contract.name || c.name == event.contract.name
        //     })
        //     .unwrap();

        // let timestamp_enabled_for_event = contract
        //     .include_events
        //     .iter()
        //     .flatten()
        //     .find(|a| a.name == event.event_name)
        //     .unwrap()
        //     .timestamps;

        // match timestamp_enabled_for_event {
        //     Some(true) => info!("Timestamps enabled for event: {}", event.event_name),
        //     Some(false) => info!("Timestamps disabled for event: {}", event.event_name),
        //     None => {}
        // };

        let event_processing_config: EventProcessingConfig = match event.is_factory_filter_event() {
            true => {
                let factory_details = network_contract
                    .indexing_contract_setup
                    .factory_details()
                    .expect("Factory event contract must have a factory details");

                FactoryEventProcessingConfig {
                    id: event.id.clone(),
                    address: factory_details.address.clone(),
                    input_name: factory_details.input_name.clone(),
                    contract_name: factory_details.contract_name.clone(),
                    project_path: project_path.clone(),
                    indexer_name: event.indexer_name.clone(),
                    event: factory_details.event.clone(),
                    network_contract: Arc::new(network_contract.clone()),
                    start_block,
                    end_block,
                    registry: Arc::clone(&registry),
                    progress: Arc::clone(&event_progress_state),
                    clickhouse: clickhouse.clone(),
                    postgres: postgres.clone(),
                    config: manifest.config.clone(),
                    csv_details: manifest_csv_details.clone(),
                    // timestamps: timestamp_enabled_for_event
                    //     .unwrap_or(manifest.timestamps.unwrap_or(false)),
                    timestamps: manifest.timestamps.unwrap_or(false),
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
                }
                .into()
            }
            false => ContractEventProcessingConfig {
                id: event.id.clone(),
                project_path: project_path.clone(),
                indexer_name: event.indexer_name.clone(),
                contract_name: event.contract.name.clone(),
                topic_id: event.topic_id,
                event_name: event.event_name.clone(),
                network_contract: Arc::new(network_contract.clone()),
                start_block,
                end_block,
                registry: Arc::clone(&registry),
                progress: Arc::clone(&event_progress_state),
                postgres: postgres.clone(),
                clickhouse: clickhouse.clone(),
                csv_details: manifest_csv_details.clone(),
                config: manifest.config.clone(),
                // timestamps: timestamp_enabled_for_event
                //     .unwrap_or(manifest.timestamps.unwrap_or(false)),
                timestamps: manifest.timestamps.unwrap_or(false),
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
            }
            .into(),
        };

        let dependencies_status = ContractEventDependencies::dependencies_status(
            &event_processing_config.contract_name(),
            &event_processing_config.event_name(),
            &dependencies,
        );

        if dependencies_status.has_dependency_in_other_contracts_multiple_times() {
            panic!("Multiple dependencies of the same event on different contracts not supported yet - please raise an issue if you need this feature");
        }

        if dependencies_status.has_dependencies() {
            if let Some(dependency_in_other_contract) =
                dependencies_status.get_first_dependencies_in_other_contracts()
            {
                apply_cross_contract_dependency_events_config_after_processing
                    .push((dependency_in_other_contract, Arc::new(event_processing_config)));

                continue;
            }

            ContractEventsDependenciesConfig::add_to_event_or_new_entry(
                &mut dependency_event_processing_configs,
                Arc::new(event_processing_config),
                &dependencies,
            );
        } else {
            let process_event = tokio::spawn(process_non_blocking_event(event_processing_config));
            non_blocking_process_events.push(process_event);
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
    let clickhouse = initialize_clickhouse(manifest).await?;

    // any events which are non-blocking and can be fired in parallel
    let mut non_blocking_process_events = Vec::new();

    let indexer = manifest.to_indexer();

    // Start the sub-indexers concurrently to ensure fast startup times
    let (trace_indexer_handles, contract_events_indexer) = join!(
        start_indexing_traces(
            manifest,
            project_path,
            database.clone(),
            clickhouse.clone(),
            &indexer,
            trace_registry.clone()
        ),
        start_indexing_contract_events(
            manifest,
            project_path,
            database.clone(),
            clickhouse.clone(),
            &indexer,
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

pub async fn initialize_clickhouse(
    manifest: &Manifest,
) -> Result<Option<Arc<ClickhouseClient>>, StartIndexingError> {
    if manifest.storage.clickhouse_enabled() {
        match ClickhouseClient::new().await {
            Ok(clickhouse) => Ok(Some(Arc::new(clickhouse))),
            Err(e) => {
                error!("Error connecting to Clickhouse: {:?}", e);
                Err(StartIndexingError::ClickhouseConnectionError(e))
            }
        }
    } else {
        Ok(None)
    }
}

pub fn calculate_safe_block_number(
    reorg_safe_distance: bool,
    provider: &Arc<JsonRpcCachedProvider>,
    latest_block: U64,
    mut end_block: U64,
) -> (U64, U64) {
    let mut indexing_distance_from_head = U64::ZERO;
    if reorg_safe_distance {
        let chain_id = provider.chain.id();
        let reorg_safe_distance = reorg_safe_distance_for_chain(chain_id);
        let safe_block_number = latest_block - reorg_safe_distance;
        if end_block > safe_block_number {
            end_block = safe_block_number;
        }
        indexing_distance_from_head = reorg_safe_distance;
    }
    (end_block, indexing_distance_from_head)
}
