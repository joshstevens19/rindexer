use std::{collections::HashMap, path::Path, sync::Arc};

use alloy::primitives::U64;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::{
    join,
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::database::clickhouse::client::{ClickhouseClient, ClickhouseConnectionError};
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::generate_internal_event_table_name;
use crate::event::config::{ContractEventProcessingConfig, FactoryEventProcessingConfig};
use crate::helpers::{camel_to_snake, format_duration};
use crate::indexer::native_transfer::native_transfer_block_processor;
use crate::indexer::reorg::{
    reorg_safe_distance_for_chain, BlockChainWindow, EventTableInfo, ReorgBlockHashPersistence,
    ReorgContext, ReorgCoordinator,
};
use crate::indexer::Indexer;
use crate::manifest::network::ReorgHandlingConfig;
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
        ContractEventDependencies,
    },
    manifest::{contract::ReorgSafeDistance, core::Manifest},
    provider::{ChainProvider, ProviderError},
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
    provider: &dyn ChainProvider,
    manifest_start_block: Option<U64>,
    manifest_end_block: Option<U64>,
    config: SyncConfig<'_>,
    event_name: &str,
    network: &str,
    reorg_safe_distance: Option<ReorgSafeDistance>,
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

    let (end_block, indexing_distance_from_head) = calculate_safe_block_number(
        reorg_safe_distance,
        provider.chain().id(),
        latest_block,
        end_block,
    );

    Ok((start_block, end_block, indexing_distance_from_head))
}

#[allow(clippy::too_many_arguments)]
async fn start_indexing_traces(
    manifest: &Manifest,
    project_path: &Path,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    indexer: &Indexer,
    trace_registry: Arc<TraceCallbackRegistry>,
    cancel_token: CancellationToken,
    progress: Arc<IndexingEventsProgressState>,
) -> Result<Vec<JoinHandle<Result<(), ProcessEventError>>>, StartIndexingError> {
    if !manifest.native_transfers.enabled {
        info!("Native transfer indexing disabled!");
        return Ok(vec![]);
    }

    let mut non_blocking_process_events = Vec::new();

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
            &*network_details.cached_provider,
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
            chain_id: network_details.cached_provider.chain().id(),
            project_path: project_path.to_path_buf(),
            start_block,
            end_block,
            indexer_name: first_event.indexer_name.clone(),
            contract_name: NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
            event_name: "TraceEvents".to_string(),
            network: network_name.clone(),
            progress: progress.clone(),
            postgres: postgres.clone(),
            csv_details: None,
            registry: network_registry,
            method: network_details.method,
            stream_last_synced_block_file_path: None,
            cancel_token: cancel_token.clone(),
        });

        let block_fetch_handle = tokio::spawn(native_transfer_block_fetch(
            network_details.cached_provider.clone(),
            block_tx,
            start_block,
            network_details.end_block,
            indexing_distance_from_head,
            network_name.clone(),
            cancel_token.clone(),
            postgres.clone(),
            first_event.indexer_name.clone(),
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
async fn start_indexing_contract_events(
    manifest: &Manifest,
    project_path: &Path,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    indexer: &Indexer,
    registry: Arc<EventCallbackRegistry>,
    dependencies: &[ContractEventDependencies],
    no_live_indexing_forced: bool,
    cancel_token: CancellationToken,
    progress: Arc<IndexingEventsProgressState>,
) -> Result<
    (
        Vec<JoinHandle<Result<(), ProcessEventError>>>,
        Vec<ProcessedNetworkContract>,
        Vec<(String, Arc<EventProcessingConfig>)>,
        Vec<ContractEventsDependenciesConfig>,
    ),
    StartIndexingError,
> {
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
            let progress = Arc::clone(&progress);
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
                    &*network_contract.cached_provider,
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
                        progress,
                        no_live_indexing_forced,
                        dependencies,
                    )
                })
            });
        }
    }

    // Build per-network reorg handling config lookup (includes chain_id for window size resolution)
    let reorg_configs: HashMap<String, (ReorgHandlingConfig, u64)> = manifest
        .networks
        .iter()
        .filter_map(|n| {
            n.reorg_handling.as_ref().and_then(|cfg| {
                if cfg.enabled {
                    Some((n.name.clone(), (cfg.clone(), n.chain_id)))
                } else {
                    None
                }
            })
        })
        .collect();

    // Build per-network event tables for reorg rollback.
    // Includes all contract events deployed on each network.
    let mut network_event_tables: HashMap<String, Vec<EventTableInfo>> = HashMap::new();
    for event in registry.events.iter() {
        for network_contract in event.contract.details.iter() {
            let schema =
                generate_indexer_contract_schema_name(&event.indexer_name, &event.contract.name);
            let table_name = camel_to_snake(&event.event_name);
            let checkpoint_table = generate_internal_event_table_name(&schema, &event.event_name);
            network_event_tables
                .entry(network_contract.network.clone())
                .or_default()
                .push(EventTableInfo::new(schema, table_name, checkpoint_table));
        }
    }

    // Shared persistence per invocation (shared across all coordinators)
    let reorg_persistence =
        Arc::new(ReorgBlockHashPersistence::new(postgres.clone(), clickhouse.clone()));

    // Build one ReorgCoordinator per network (shared across all events on that network).
    // The first non-blocking event on each network takes ownership; subsequent events get None.
    let mut network_coordinators: HashMap<String, ReorgCoordinator> = HashMap::new();
    if !no_live_indexing_forced {
        for (network_name, (reorg_config, chain_id)) in &reorg_configs {
            let window_size = reorg_config
                .window_size
                .unwrap_or_else(|| 2 * reorg_safe_distance_for_chain(*chain_id) as usize);
            let event_tables = network_event_tables.get(network_name).cloned().unwrap_or_default();

            let window = match reorg_persistence.load(network_name, window_size).await {
                Ok(window) => {
                    info!(
                        "Loaded {} blocks into reorg window for network {}",
                        window.len(),
                        network_name,
                    );
                    window
                }
                Err(e) => {
                    warn!(
                        "Failed to load reorg window from persistence for {}: {}. Using empty window.",
                        network_name, e
                    );
                    BlockChainWindow::new(window_size)
                }
            };

            // Get a provider for this network from any registry event targeting it
            let provider = registry
                .events
                .iter()
                .flat_map(|e| e.contract.details.iter())
                .find(|nc| nc.network == *network_name)
                .map(|nc| nc.cached_provider.clone());

            if let Some(provider) = provider {
                let mut coordinator = ReorgCoordinator::new(
                    network_name.clone(),
                    window,
                    Arc::clone(&reorg_persistence),
                    provider,
                    event_tables,
                );

                // Get streams_clients for this network (if any event has one)
                let startup_streams_clients = registry
                    .events
                    .iter()
                    .find(|e| e.contract.details.iter().any(|d| d.network == *network_name))
                    .map(|e| e.streams_clients.clone());

                // Run startup validation
                match coordinator.validate_on_startup().await {
                    Ok(Some(startup_task)) => {
                        warn!(
                            "Startup reorg detected on {} (fork_point: {}, depth: {}). Executing rollback before indexing.",
                            network_name,
                            startup_task.fork_point,
                            startup_task.detection_point - startup_task.fork_point + 1,
                        );
                        let reorg_ctx = ReorgContext {
                            postgres: postgres.as_deref(),
                            clickhouse: clickhouse.as_ref(),
                            registry: None,
                            streams_clients: startup_streams_clients
                                .as_ref()
                                .and_then(|a| a.as_ref().as_ref()),
                        };
                        if let Err(e) = coordinator.handle_reorg(startup_task, &reorg_ctx).await {
                            error!(
                                "Failed to execute startup reorg rollback for {}: {}",
                                network_name, e
                            );
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!(
                            "Startup reorg validation failed for {}: {}. Proceeding without validation.",
                            network_name, e
                        );
                    }
                }

                network_coordinators.insert(network_name.clone(), coordinator);
            }
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
            progress,
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
                    progress: Arc::clone(&progress),
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
                    cancel_token: cancel_token.clone(),
                    tables: event.tables.clone(),
                    reorg_sender: event.reorg_sender.clone(),
                    streams_clients: event.streams_clients.clone(),
                    contract_abi: Some(event.contract.abi.clone()),
                    providers: event.providers.clone(),
                    constants: event.constants.clone(),
                    multicall_addresses: event.multicall_addresses.clone(),
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
                progress: Arc::clone(&progress),
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
                cancel_token: cancel_token.clone(),
                tables: event.tables.clone(),
                reorg_sender: event.reorg_sender.clone(),
                streams_clients: event.streams_clients.clone(),
                contract_abi: Some(event.contract.abi.clone()),
                providers: event.providers.clone(),
                constants: event.constants.clone(),
                multicall_addresses: event.multicall_addresses.clone(),
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
            // Take ownership of the per-network coordinator for the FIRST event on each
            // network. Subsequent events on the same network get None — only one event
            // per network drives reorg detection.
            let reorg_coordinator =
                if event_processing_config.live_indexing() && !no_live_indexing_forced {
                    let network_name = event_processing_config.network_contract().network.clone();
                    network_coordinators.remove(&network_name)
                } else {
                    None
                };

            let process_event = tokio::spawn(process_non_blocking_event(
                event_processing_config,
                reorg_coordinator,
            ));
            non_blocking_process_events.push(process_event);
        }
    }

    // Build per-network reorg coordinators for dependency events.
    // Any coordinators left over in network_coordinators (not consumed by non-blocking events)
    // are available for dependency events. Build new ones for networks only used in dependencies.
    if !no_live_indexing_forced {
        // Collect all networks needed by dependency events
        let dep_networks: std::collections::HashSet<String> = dependency_event_processing_configs
            .iter()
            .flat_map(|dep| {
                dep.events_config
                    .iter()
                    .filter(|e| e.live_indexing())
                    .map(|e| e.network_contract().network.clone())
            })
            .collect();

        // Build coordinators for networks that weren't already consumed
        let mut dep_coordinators: HashMap<String, ReorgCoordinator> = HashMap::new();
        for network_name in &dep_networks {
            // Try to take a leftover from the non-blocking build
            if let Some(coord) = network_coordinators.remove(network_name) {
                dep_coordinators.insert(network_name.clone(), coord);
                continue;
            }

            // Otherwise build a fresh one
            if let Some((reorg_config, chain_id)) = reorg_configs.get(network_name) {
                let window_size = reorg_config
                    .window_size
                    .unwrap_or_else(|| reorg_safe_distance_for_chain(*chain_id) as usize);
                let event_tables =
                    network_event_tables.get(network_name).cloned().unwrap_or_default();

                let window = match reorg_persistence.load(network_name, window_size).await {
                    Ok(window) => {
                        info!(
                            "Dependency events - Loaded {} blocks into reorg window for network {}",
                            window.len(),
                            network_name,
                        );
                        window
                    }
                    Err(e) => {
                        warn!(
                            "Dependency events - Failed to load reorg window for {}: {}. Using empty window.",
                            network_name, e
                        );
                        BlockChainWindow::new(window_size)
                    }
                };

                // Get a provider from any dependency event config for this network
                let provider = dependency_event_processing_configs
                    .iter()
                    .flat_map(|dep| dep.events_config.iter())
                    .find(|e| e.network_contract().network == *network_name)
                    .map(|e| e.network_contract().cached_provider.clone());

                if let Some(provider) = provider {
                    let mut coordinator = ReorgCoordinator::new(
                        network_name.clone(),
                        window,
                        Arc::clone(&reorg_persistence),
                        provider,
                        event_tables,
                    );

                    // Get streams_clients for this network from dependency events
                    let dep_streams_clients = dependency_event_processing_configs
                        .iter()
                        .flat_map(|dep| dep.events_config.iter())
                        .find(|e| e.network_contract().network == *network_name)
                        .map(|e| e.streams_clients());

                    match coordinator.validate_on_startup().await {
                        Ok(Some(startup_task)) => {
                            warn!(
                                "Dependency events - Startup reorg detected on {} (fork_point: {}, depth: {}). Executing rollback.",
                                network_name,
                                startup_task.fork_point,
                                startup_task.detection_point - startup_task.fork_point + 1,
                            );
                            let reorg_ctx = ReorgContext {
                                postgres: postgres.as_deref(),
                                clickhouse: clickhouse.as_ref(),
                                registry: None,
                                streams_clients: dep_streams_clients
                                    .as_ref()
                                    .and_then(|a| a.as_ref().as_ref()),
                            };
                            if let Err(e) = coordinator.handle_reorg(startup_task, &reorg_ctx).await
                            {
                                error!(
                                    "Dependency events - Failed to execute startup reorg rollback for {}: {}",
                                    network_name, e
                                );
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            error!(
                                "Dependency events - Startup reorg validation failed for {}: {}. Proceeding without validation.",
                                network_name, e
                            );
                        }
                    }

                    dep_coordinators.insert(network_name.clone(), coordinator);
                }
            }
        }

        // Inject per-network coordinators into each dependency config group
        for dep_config in &mut dependency_event_processing_configs {
            let networks: std::collections::HashSet<String> = dep_config
                .events_config
                .iter()
                .filter(|e| e.live_indexing())
                .map(|e| e.network_contract().network.clone())
                .collect();

            for network_name in networks {
                if let Some(coord) = dep_coordinators.remove(&network_name) {
                    dep_config.reorg_coordinators.insert(network_name, coord);
                }
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

pub async fn start_historical_indexing(
    manifest: &Manifest,
    project_path: &Path,
    dependencies: &[ContractEventDependencies],
    registry: Arc<EventCallbackRegistry>,
    trace_registry: Arc<TraceCallbackRegistry>,
    cancel_token: CancellationToken,
    progress: Arc<IndexingEventsProgressState>,
) -> Result<Vec<ProcessedNetworkContract>, StartIndexingError> {
    info!("Historical indexing started");

    let start = Instant::now();

    let result = start_indexing(
        manifest,
        project_path,
        dependencies,
        true,
        registry,
        trace_registry,
        cancel_token,
        progress,
    )
    .await?;

    let duration = start.elapsed();

    info!("Historical indexing completed - time taken: {}", format_duration(duration));

    Ok(result)
}

pub async fn start_live_indexing(
    manifest: &Manifest,
    project_path: &Path,
    dependencies: &[ContractEventDependencies],
    registry: Arc<EventCallbackRegistry>,
    trace_registry: Arc<TraceCallbackRegistry>,
    cancel_token: CancellationToken,
    progress: Arc<IndexingEventsProgressState>,
) -> Result<Vec<ProcessedNetworkContract>, StartIndexingError> {
    info!("Live indexing started");

    start_indexing(
        manifest,
        project_path,
        dependencies,
        false,
        registry,
        trace_registry,
        cancel_token,
        progress,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn start_indexing(
    manifest: &Manifest,
    project_path: &Path,
    dependencies: &[ContractEventDependencies],
    no_live_indexing_forced: bool,
    registry: Arc<EventCallbackRegistry>,
    trace_registry: Arc<TraceCallbackRegistry>,
    cancel_token: CancellationToken,
    progress: Arc<IndexingEventsProgressState>,
) -> Result<Vec<ProcessedNetworkContract>, StartIndexingError> {
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
            trace_registry.clone(),
            cancel_token.clone(),
            progress.clone(),
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
            cancel_token.clone(),
            progress.clone(),
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
    reorg_safe_distance: Option<ReorgSafeDistance>,
    chain_id: u64,
    latest_block: U64,
    mut end_block: U64,
) -> (U64, U64) {
    let mut indexing_distance_from_head = U64::ZERO;
    if let Some(ref config) = reorg_safe_distance {
        if let Some(distance) = config.resolve(chain_id) {
            let safe_distance = U64::from(distance);
            let safe_block_number = latest_block.saturating_sub(safe_distance);
            if end_block > safe_block_number {
                end_block = safe_block_number;
            }
            indexing_distance_from_head = safe_distance;
        }
    }
    (end_block, indexing_distance_from_head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::last_synced::SyncConfig;
    use crate::manifest::contract::ReorgSafeDistance;
    use crate::provider::mock::MockChainProvider;
    use std::path::Path;

    fn empty_sync_config() -> SyncConfig<'static> {
        SyncConfig {
            project_path: Path::new("/tmp/test"),
            postgres: &None,
            clickhouse: &None,
            csv_details: &None,
            stream_details: &None,
            contract_csv_enabled: false,
            indexer_name: "test_indexer",
            contract_name: "test_contract",
            event_name: "test_event",
            network: "ethereum",
        }
    }

    #[test]
    fn safe_block_no_reorg_distance() {
        let (end, distance) =
            calculate_safe_block_number(None, 1, U64::from(1000), U64::from(1000));
        assert_eq!(end, U64::from(1000));
        assert_eq!(distance, U64::ZERO);
    }

    #[test]
    fn safe_block_reorg_disabled() {
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Enabled(false)),
            1,
            U64::from(1000),
            U64::from(1000),
        );
        assert_eq!(end, U64::from(1000));
        assert_eq!(distance, U64::ZERO);
    }

    #[test]
    fn safe_block_custom_distance_clamps_end() {
        // latest=1000, end=1000, distance=20 → safe_block=980, end clamped to 980
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Custom(20)),
            1,
            U64::from(1000),
            U64::from(1000),
        );
        assert_eq!(end, U64::from(980));
        assert_eq!(distance, U64::from(20));
    }

    #[test]
    fn safe_block_end_already_below_safe() {
        // latest=1000, end=500, distance=20 → safe_block=980, end stays 500
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Custom(20)),
            1,
            U64::from(1000),
            U64::from(500),
        );
        assert_eq!(end, U64::from(500));
        assert_eq!(distance, U64::from(20));
    }

    #[test]
    fn safe_block_enabled_true_uses_chain_default() {
        // Ethereum mainnet (chain_id=1) should have a non-zero default distance
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Enabled(true)),
            1, // ethereum mainnet
            U64::from(10000),
            U64::from(10000),
        );
        assert!(distance > U64::ZERO);
        assert!(end < U64::from(10000));
    }

    #[tokio::test]
    async fn start_block_higher_than_latest_errors() {
        let mock = MockChainProvider::new(1).with_block_number(100);
        let result = get_start_end_block(
            &mock,
            Some(U64::from(200)), // start > latest
            None,
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(StartIndexingError::StartBlockIsHigherThanLatestBlockError(..))
        ));
    }

    #[tokio::test]
    async fn end_block_higher_than_latest_errors() {
        let mock = MockChainProvider::new(1).with_block_number(100);
        let result = get_start_end_block(
            &mock,
            Some(U64::from(50)),
            Some(U64::from(200)), // end > latest
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await;

        assert!(matches!(
            result,
            Err(StartIndexingError::EndBlockIsHigherThanLatestBlockError(..))
        ));
    }

    #[tokio::test]
    async fn normal_range_returns_start_and_end() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (start, end, distance) = get_start_end_block(
            &mock,
            Some(U64::from(100)),
            Some(U64::from(500)),
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(100));
        assert_eq!(end, U64::from(500));
        assert_eq!(distance, U64::ZERO);
    }

    #[tokio::test]
    async fn end_block_clamped_to_latest() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (_, end, _) = get_start_end_block(
            &mock,
            Some(U64::from(100)),
            None, // no manifest end → defaults to latest
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(end, U64::from(1000));
    }

    #[tokio::test]
    async fn reorg_safe_distance_applied() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (start, end, distance) = get_start_end_block(
            &mock,
            Some(U64::from(100)),
            None,
            empty_sync_config(),
            "Test",
            "ethereum",
            Some(ReorgSafeDistance::Custom(50)),
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(100));
        assert_eq!(end, U64::from(950)); // 1000 - 50
        assert_eq!(distance, U64::from(50));
    }

    #[tokio::test]
    async fn no_start_block_defaults_to_latest() {
        let mock = MockChainProvider::new(1).with_block_number(500);
        let (start, end, _) = get_start_end_block(
            &mock,
            None, // no manifest start → defaults to latest
            None,
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(500));
        assert_eq!(end, U64::from(500));
    }

    #[tokio::test]
    async fn start_block_equals_end_block_single_block_range() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (start, end, distance) = get_start_end_block(
            &mock,
            Some(U64::from(500)),
            Some(U64::from(500)),
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(500));
        assert_eq!(end, U64::from(500));
        assert_eq!(distance, U64::ZERO);
    }

    #[tokio::test]
    async fn start_block_zero_genesis() {
        let mock = MockChainProvider::new(1).with_block_number(1000);
        let (start, end, _) = get_start_end_block(
            &mock,
            Some(U64::ZERO),
            Some(U64::from(100)),
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(start, U64::ZERO);
        assert_eq!(end, U64::from(100));
    }

    #[tokio::test]
    async fn very_large_block_numbers() {
        let large = 18_000_000u64;
        let mock = MockChainProvider::new(1).with_block_number(large);
        let (start, end, distance) = get_start_end_block(
            &mock,
            Some(U64::from(17_000_000u64)),
            Some(U64::from(large)),
            empty_sync_config(),
            "Test",
            "ethereum",
            None,
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(17_000_000u64));
        assert_eq!(end, U64::from(large));
        assert_eq!(distance, U64::ZERO);
    }

    #[tokio::test]
    async fn reorg_safe_distance_larger_than_range_clamps_to_zero() {
        // latest=100, distance=200 → safe_block = saturating_sub → 0
        // end (100) > safe_block (0) so end is clamped to 0
        let mock = MockChainProvider::new(1).with_block_number(100);
        let (start, end, distance) = get_start_end_block(
            &mock,
            Some(U64::from(10)),
            None,
            empty_sync_config(),
            "Test",
            "ethereum",
            Some(ReorgSafeDistance::Custom(200)),
        )
        .await
        .unwrap();

        assert_eq!(start, U64::from(10));
        assert_eq!(end, U64::ZERO); // clamped due to saturating_sub
        assert_eq!(distance, U64::from(200));
    }

    #[test]
    fn safe_block_distance_larger_than_latest_saturates_to_zero() {
        // latest=50, distance=100 → saturating_sub = 0, end clamped to 0
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Custom(100)),
            1,
            U64::from(50),
            U64::from(50),
        );
        assert_eq!(end, U64::ZERO);
        assert_eq!(distance, U64::from(100));
    }

    #[test]
    fn safe_block_end_exactly_equals_safe_block() {
        // latest=1000, distance=20 → safe_block=980, end=980 → no clamp needed
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Custom(20)),
            1,
            U64::from(1000),
            U64::from(980),
        );
        assert_eq!(end, U64::from(980));
        assert_eq!(distance, U64::from(20));
    }

    #[test]
    fn safe_block_custom_zero_distance_no_change() {
        // distance=0 → safe_block = latest, end unchanged
        let (end, distance) = calculate_safe_block_number(
            Some(ReorgSafeDistance::Custom(0)),
            1,
            U64::from(1000),
            U64::from(1000),
        );
        assert_eq!(end, U64::from(1000));
        assert_eq!(distance, U64::ZERO);
    }
}
