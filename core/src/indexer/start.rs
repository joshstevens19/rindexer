use std::{collections::HashMap, path::Path, sync::Arc};

use alloy::primitives::U64;
use futures::future::try_join_all;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio::{
    task::{JoinError, JoinHandle},
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::database::clickhouse::client::{ClickhouseClient, ClickhouseConnectionError};
use crate::database::generate::generate_indexer_contract_schema_name;
use crate::database::postgres::generate::generate_internal_event_table_name;
use crate::database::DatabaseBackends;
use crate::event::config::{ContractEventProcessingConfig, FactoryEventProcessingConfig};
use crate::helpers::{camel_to_snake, format_duration};
use crate::indexer::native_transfer::native_transfer_block_processor;
use crate::indexer::reorg::{
    reorg_safe_distance_for_chain, BlockChainWindow, DerivedColumnJournal, DerivedColumnRollback,
    DerivedTableInfo, DerivedTableRollbackOp, EventTableInfo, ReorgBlockHashPersistence,
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

    #[error("Invalid configuration: {0}")]
    ConfigError(#[from] anyhow::Error),
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
    network_coordinators: &HashMap<String, Arc<Mutex<ReorgCoordinator>>>,
    no_live_indexing_forced: bool,
) -> Result<Vec<JoinHandle<Result<(), ProcessEventError>>>, StartIndexingError> {
    if !manifest.native_transfers.enabled {
        info!("Native transfer indexing disabled!");
        return Ok(vec![]);
    }

    // Historical pass defers to the live pass to avoid double-spawning the NT
    // pipeline. But when no live pass will follow (e.g. every contract and NT
    // network has an end_block), this IS NT's only chance to run — so fall
    // through and spawn it here.
    if no_live_indexing_forced && manifest.has_any_live_indexing() {
        info!("Native transfer indexing deferred to live pass to prevent double-spawn");
        return Ok(vec![]);
    }

    let mut non_blocking_process_events = Vec::new();

    // Group events by network to create one pipeline per network
    let mut network_events: HashMap<
        String,
        Vec<&crate::event::callback_registry::TraceCallbackRegistryInformation>,
    > = HashMap::new();

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

        let databases = DatabaseBackends::new(postgres.clone(), clickhouse.clone())
            .with_config(
                manifest.storage.write_policy.clone(),
                manifest.storage.circuit_breaker.clone(),
                manifest.storage.max_batch_size,
            );
        let sync_config = SyncConfig {
            project_path,
            databases: &databases,
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
            on_reorg: trace_registry.on_reorg.clone(),
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
            databases: databases.clone(),
            csv_details: None,
            registry: network_registry,
            method: network_details.method,
            stream_last_synced_block_file_path: None,
            cancel_token: cancel_token.clone(),
        });

        let reorg_coordinator = network_coordinators.get(&network_name).cloned();

        let block_fetch_handle = tokio::spawn(native_transfer_block_fetch(
            network_details.cached_provider.clone(),
            block_tx,
            start_block,
            network_details.end_block,
            indexing_distance_from_head,
            network_name.clone(),
            cancel_token.clone(),
            databases.clone(),
            first_event.indexer_name.clone(),
            reorg_coordinator,
            trace_registry.clone(),
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

/// Find a provider for a network in the trace registry. Used as a fallback when
/// the primary lookup (either `registry.events` or `dependency_event_processing_configs`)
/// has no entry for the network — native-transfer-only networks live only in the
/// trace registry.
fn find_provider_in_trace_registry(
    trace_registry: &TraceCallbackRegistry,
    network_name: &str,
) -> Option<Arc<dyn ChainProvider>> {
    trace_registry
        .events
        .iter()
        .flat_map(|e| e.trace_information.details.iter())
        .find(|nd| nd.network == network_name)
        .map(|nd| nd.cached_provider.clone())
}

/// Collect every distinct `Arc<Option<StreamsClients>>` configured on the given
/// network across contract events and native-transfer trace events. Dedup is
/// by `Arc::as_ptr` pointer identity so two pipelines sharing the same instance
/// only publish once. Entries whose inner `Option` is `None` are skipped.
/// Compute the effective `reorg_safe_distance` for `network_name` and register
/// it on every `StreamsClients` instance that serves the network, so any
/// finalized-delivery buffer knows how far behind head to wait before
/// flushing. Called from both coordinator-construction sites in
/// `start_indexing_contract_events`.
fn register_network_reorg_distance_on_streams(
    manifest: &Manifest,
    network_name: &str,
    chain_id: u64,
    streams_clients: &[Arc<Option<crate::streams::StreamsClients>>],
) {
    let safe_distance = crate::indexer::reorg::finalized_buffer_distance_for_network(
        manifest,
        network_name,
        chain_id,
    );
    for clients_arc in streams_clients {
        if let Some(clients) = clients_arc.as_ref().as_ref() {
            clients.register_network_reorg_distance(network_name.to_string(), safe_distance);
        }
    }
}

/// Reject `delivery: finalized` on any network that isn't in
/// `network_coordinators`. Such a network has no live coordinator to drive
/// flush/discard, so buffered events would accumulate forever (or until a
/// reorg clears them). Returns a `ConfigError` listing every offending
/// `(stream_type, endpoint, network)` triple.
fn validate_finalized_delivery_targets(
    manifest: &Manifest,
    network_coordinators: &HashMap<String, Arc<Mutex<ReorgCoordinator>>>,
) -> Result<(), StartIndexingError> {
    let mut errors: Vec<String> = Vec::new();

    let mut check = |source: String, streams: &Option<crate::manifest::stream::StreamsConfig>| {
        let Some(streams) = streams else { return };
        for (stream_type, endpoint, networks) in streams.finalized_delivery_targets() {
            for n in &networks {
                if !network_coordinators.contains_key(n) {
                    errors.push(format!(
                        "stream config '{endpoint}' (stream_type: {stream_type}, {source}) \
                         requests delivery: finalized on network '{n}', but that network has \
                         no live indexing (reorg coordinator). Either enable live indexing or \
                         change delivery to 'instant'."
                    ));
                }
            }
        }
    };

    for contract in manifest.all_contracts() {
        check(format!("contract '{}'", contract.name), &contract.streams);
    }
    if manifest.native_transfers.enabled {
        check("native_transfers".to_string(), &manifest.native_transfers.streams);
    }

    if !errors.is_empty() {
        return Err(StartIndexingError::ConfigError(anyhow::anyhow!(errors.join("\n"))));
    }
    Ok(())
}

fn collect_streams_clients_for_network(
    registry: &EventCallbackRegistry,
    trace_registry: &TraceCallbackRegistry,
    network_name: &str,
) -> Vec<Arc<Option<crate::streams::StreamsClients>>> {
    let mut out: Vec<Arc<Option<crate::streams::StreamsClients>>> = Vec::new();

    let mut push = |arc_outer: &Arc<Option<crate::streams::StreamsClients>>| {
        if arc_outer.as_ref().is_none() {
            return;
        }
        let ptr = Arc::as_ptr(arc_outer);
        if out.iter().any(|existing| Arc::as_ptr(existing) == ptr) {
            return;
        }
        out.push(Arc::clone(arc_outer));
    };

    for event in &registry.events {
        if event.contract.details.iter().any(|d| d.network == network_name) {
            push(&event.streams_clients);
        }
    }
    for trace_event in &trace_registry.events {
        if trace_event.trace_information.details.iter().any(|d| d.network == network_name) {
            push(&trace_event.streams_clients);
        }
    }

    out
}

/// Build derived-table rollback + journal entries for an event's tables and merge
/// them into `accumulator` keyed by `network`. Shared between contract events and
/// native-transfer trace events so both sources contribute to reorg rollback.
fn build_derived_tables_for_event(
    event_name: &str,
    indexer_name: &str,
    contract_name: &str,
    network: &str,
    tables: &[crate::indexer::tables::TableRuntime],
    accumulator: &mut HashMap<String, Vec<DerivedTableInfo>>,
) -> anyhow::Result<()> {
    let schema = generate_indexer_contract_schema_name(indexer_name, contract_name);
    let event_table_full = format!("{}.{}", schema, camel_to_snake(event_name));

    for tr in tables.iter() {
        let derived = accumulator.entry(network.to_string()).or_default();

        let event_table_name = event_table_full.clone();
        let mut rollback_ops: Vec<DerivedTableRollbackOp> = Vec::new();
        let mut journal_columns: Vec<DerivedColumnJournal> = Vec::new();

        for table_event in &tr.table.events {
            if table_event.event != event_name {
                continue;
            }
            for operation in &table_event.operations {
                use crate::manifest::contract::OperationType;
                if !matches!(
                    operation.operation_type,
                    OperationType::Upsert | OperationType::Update
                ) {
                    continue;
                }

                let mut where_columns: Vec<(String, String)> = operation
                    .where_clause
                    .iter()
                    .filter_map(|(col, val)| {
                        val.strip_prefix('$').map(|field| (col.clone(), camel_to_snake(field)))
                    })
                    .collect();
                where_columns.sort_by(|a, b| a.0.cmp(&b.0));

                let where_col_names: Vec<String> =
                    where_columns.iter().map(|(col, _)| col.clone()).collect();

                let columns: Vec<DerivedColumnRollback> = operation
                    .set
                    .iter()
                    .filter(|set_col| {
                        set_col.action.reverse().is_some() && set_col.event_field_name().is_some()
                    })
                    .map(|set_col| {
                        DerivedColumnRollback::try_new(
                            set_col.column.clone(),
                            camel_to_snake(set_col.event_field_name().unwrap()),
                            set_col.action.clone(),
                        )
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;

                // Collect non-reversible columns for journal-based recalculation
                for set_col in &operation.set {
                    if set_col.action.reverse().is_some() {
                        continue; // handled by rollback_ops
                    }
                    if !journal_columns.iter().any(|jc| jc.derived_column == set_col.column) {
                        journal_columns.push(DerivedColumnJournal::try_new(
                            set_col.column.clone(),
                            set_col.action.clone(),
                            where_col_names.clone(),
                        )?);
                    }
                }

                if !columns.is_empty() {
                    rollback_ops.push(DerivedTableRollbackOp::try_new(
                        event_table_name.clone(),
                        where_columns,
                        columns,
                        operation.condition().map(String::from),
                    )?);
                }
            }
        }

        // Merge into existing entry or create a new one
        if let Some(existing) = derived.iter_mut().find(|d| d.full_table_name == tr.full_table_name)
        {
            existing.rollback_ops.extend(rollback_ops);
            for jc in journal_columns {
                if !existing.journal_columns.iter().any(|e| e.derived_column == jc.derived_column) {
                    existing.journal_columns.push(jc);
                }
            }
        } else {
            derived.push(DerivedTableInfo::try_new(
                tr.full_table_name.clone(),
                tr.table.cross_chain,
                rollback_ops,
                journal_columns,
            )?);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn start_indexing_contract_events(
    manifest: &Manifest,
    project_path: &Path,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    indexer: &Indexer,
    registry: Arc<EventCallbackRegistry>,
    trace_registry: Arc<TraceCallbackRegistry>,
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
        HashMap<String, Arc<Mutex<ReorgCoordinator>>>,
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
            let write_policy = manifest.storage.write_policy.clone();
            let circuit_breaker = manifest.storage.circuit_breaker.clone();
            let max_batch_size = manifest.storage.max_batch_size;
            let registry = Arc::clone(&registry);
            let progress = Arc::clone(&progress);
            let dependencies = dependencies.to_vec();

            block_tasks.push(async move {
                let databases = DatabaseBackends::new(postgres.clone(), clickhouse.clone())
                    .with_config(write_policy, circuit_breaker, max_batch_size);
                let config = SyncConfig {
                    project_path: &project_path,
                    databases: &databases,
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
                        databases,
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

    // Build per-network event tables and derived tables for reorg rollback.
    let mut network_event_tables: HashMap<String, Vec<EventTableInfo>> = HashMap::new();
    let mut network_derived_tables: HashMap<String, Vec<DerivedTableInfo>> = HashMap::new();
    for event in registry.events.iter() {
        for network_contract in event.contract.details.iter() {
            let schema =
                generate_indexer_contract_schema_name(&event.indexer_name, &event.contract.name);
            let table_name = camel_to_snake(&event.event_name);
            let checkpoint_table = generate_internal_event_table_name(&schema, &event.event_name);
            network_event_tables.entry(network_contract.network.clone()).or_default().push(
                EventTableInfo::try_new(
                    schema,
                    table_name,
                    checkpoint_table,
                    event.indexer_name.clone(),
                    event.contract.name.clone(),
                    event.event_name.clone(),
                )?,
            );

            build_derived_tables_for_event(
                &event.event_name,
                &event.indexer_name,
                &event.contract.name,
                &network_contract.network,
                &event.tables,
                &mut network_derived_tables,
            )?;
        }
    }

    // Register native transfer tables for reorg rollback (if native transfers are enabled).
    // Native transfers use the virtual contract name "EvmTraces" with event "NativeTransfer".
    if manifest.native_transfers.enabled {
        if let Some(networks) = &manifest.native_transfers.networks {
            for nt_detail in networks {
                let schema = generate_indexer_contract_schema_name(
                    &manifest.name,
                    NATIVE_TRANSFER_CONTRACT_NAME,
                );
                let table_name = camel_to_snake("NativeTransfer");
                let checkpoint_table =
                    generate_internal_event_table_name(&schema, "NativeTransfer");
                network_event_tables.entry(nt_detail.network.clone()).or_default().push(
                    EventTableInfo::try_new(
                        schema,
                        table_name,
                        checkpoint_table,
                        manifest.name.clone(),
                        NATIVE_TRANSFER_CONTRACT_NAME.to_string(),
                        "NativeTransfer".to_string(),
                    )?,
                );
            }
        }

        // Mirror the contract-events pass for native-transfer derived tables: every
        // trace event contributes rollback ops for each configured network.
        for trace_event in trace_registry.events.iter() {
            for network_detail in trace_event.trace_information.details.iter() {
                build_derived_tables_for_event(
                    &trace_event.event_name,
                    &trace_event.indexer_name,
                    &trace_event.contract_name,
                    &network_detail.network,
                    &trace_event.tables,
                    &mut network_derived_tables,
                )?;
            }
        }
    }

    // Shared persistence per invocation (shared across all coordinators)
    let reorg_persistence =
        Arc::new(ReorgBlockHashPersistence::new(postgres.clone(), clickhouse.clone()));

    // Build one ReorgCoordinator per network (shared across all events on that network).
    // The first non-blocking event on each network takes ownership; subsequent events get None.
    //
    // Startup reorg validation MUST run before any indexing begins — including the
    // historical phase — so that stale checkpoints are corrected before events are
    // fetched.  Coordinators are only kept for live indexing; when we are in the
    // historical-only pass they are dropped after validation.
    let mut network_coordinators: HashMap<String, Arc<Mutex<ReorgCoordinator>>> = HashMap::new();
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
                BlockChainWindow::try_new(window_size)?
            }
        };

        // Get a provider for this network from any registry event targeting it.
        // Native-transfer-only networks have no entry in `registry.events`, so fall back
        // to the trace registry for those providers.
        let provider = registry
            .events
            .iter()
            .flat_map(|e| e.contract.details.iter())
            .find(|nc| &nc.network == network_name)
            .map(|nc| nc.cached_provider.clone())
            .or_else(|| find_provider_in_trace_registry(&trace_registry, network_name));

        if let Some(provider) = provider {
            let derived_tables =
                network_derived_tables.get(network_name).cloned().unwrap_or_default();
            let streams_clients =
                collect_streams_clients_for_network(&registry, &trace_registry, network_name);
            register_network_reorg_distance_on_streams(
                manifest,
                network_name,
                *chain_id,
                &streams_clients,
            );

            let mut coordinator = ReorgCoordinator::new(
                network_name.clone(),
                window,
                Arc::clone(&reorg_persistence),
                provider,
                event_tables,
                derived_tables,
                streams_clients,
            )?;

            // Run startup validation
            match coordinator.validate_on_startup().await {
                Ok(Some(startup_task)) => {
                    warn!(
                        "Startup reorg detected on {} (fork_point: {}, depth: {}). Executing rollback before indexing.",
                        network_name,
                        startup_task.fork_point,
                        startup_task.detection_point.saturating_sub(startup_task.fork_point) + 1,
                    );
                    let reorg_ctx = ReorgContext {
                        postgres: postgres.as_deref(),
                        clickhouse: clickhouse.as_ref(),
                        registry: Some(&registry),
                        trace_registry: Some(&trace_registry),
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

            // Keep the coordinator in the map so non-blocking tasks (contract events
            // and native-transfer fetchers) can share it during live indexing. In a
            // pure historical pass the coordinator served its purpose (startup
            // validation) and can be dropped.
            if !no_live_indexing_forced {
                network_coordinators
                    .insert(network_name.clone(), Arc::new(Mutex::new(coordinator)));
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
            databases,
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
                    databases: databases.clone(),
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
                databases: databases.clone(),
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
            // DESIGN: One ReorgCoordinator per network, shared via Arc<Mutex<_>> across
            // all tasks that can observe a new block for that network (contract-event
            // pipelines and the native-transfer block fetcher). The coordinator holds
            // ALL event tables for the network, so reorg rollbacks cover every table
            // regardless of which task drives detection. The Mutex serializes
            // `on_new_block`/`handle_reorg` calls so concurrent detection is idempotent.
            let reorg_coordinator =
                if event_processing_config.live_indexing() && !no_live_indexing_forced {
                    let network_name = event_processing_config.network_contract().network.clone();
                    network_coordinators.get(&network_name).cloned()
                } else {
                    None
                };

            let process_event = tokio::spawn(process_non_blocking_event(
                event_processing_config,
                reorg_coordinator,
                Some(trace_registry.clone()),
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
        let mut dep_coordinators: HashMap<String, Arc<Mutex<ReorgCoordinator>>> = HashMap::new();
        for network_name in &dep_networks {
            // Share the network's coordinator if one already exists
            if let Some(coord) = network_coordinators.get(network_name).cloned() {
                dep_coordinators.insert(network_name.clone(), coord);
                continue;
            }

            // Otherwise build a fresh one
            if let Some((reorg_config, chain_id)) = reorg_configs.get(network_name) {
                let window_size = reorg_config
                    .window_size
                    .unwrap_or_else(|| 2 * reorg_safe_distance_for_chain(*chain_id) as usize);
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
                        BlockChainWindow::try_new(window_size)?
                    }
                };

                // Get a provider from any dependency event config for this network.
                // Fall back to the trace registry so native-transfer-only networks still
                // have a provider available when this branch is reached.
                let provider = dependency_event_processing_configs
                    .iter()
                    .flat_map(|dep| dep.events_config.iter())
                    .find(|e| e.network_contract().network == *network_name)
                    .map(|e| e.network_contract().cached_provider.clone())
                    .or_else(|| find_provider_in_trace_registry(&trace_registry, network_name));

                if let Some(provider) = provider {
                    let derived_tables =
                        network_derived_tables.get(network_name).cloned().unwrap_or_default();
                    let streams_clients = collect_streams_clients_for_network(
                        &registry,
                        &trace_registry,
                        network_name,
                    );
                    register_network_reorg_distance_on_streams(
                        manifest,
                        network_name,
                        *chain_id,
                        &streams_clients,
                    );

                    let mut coordinator = ReorgCoordinator::new(
                        network_name.clone(),
                        window,
                        Arc::clone(&reorg_persistence),
                        provider,
                        event_tables,
                        derived_tables,
                        streams_clients,
                    )?;

                    match coordinator.validate_on_startup().await {
                        Ok(Some(startup_task)) => {
                            warn!(
                                "Dependency events - Startup reorg detected on {} (fork_point: {}, depth: {}). Executing rollback.",
                                network_name,
                                startup_task.fork_point,
                                startup_task.detection_point.saturating_sub(startup_task.fork_point) + 1,
                            );
                            let reorg_ctx = ReorgContext {
                                postgres: postgres.as_deref(),
                                clickhouse: clickhouse.as_ref(),
                                registry: Some(&registry),
                                trace_registry: Some(&trace_registry),
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

                    let shared = Arc::new(Mutex::new(coordinator));
                    dep_coordinators.insert(network_name.clone(), shared.clone());
                    // Also share with the non-blocking network map so any native-transfer
                    // task running on this network can reach the same coordinator.
                    network_coordinators.insert(network_name.clone(), shared);
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
        network_coordinators,
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

    // Contract events must complete their setup first so the per-network reorg
    // coordinators exist before native-transfer tasks look them up. The returned
    // `network_coordinators` map is shared (each entry is Arc<Mutex<_>>), so
    // both pipelines observe the same coordinator for any given network.
    let contract_events_indexer = start_indexing_contract_events(
        manifest,
        project_path,
        database.clone(),
        clickhouse.clone(),
        &indexer,
        registry.clone(),
        trace_registry.clone(),
        dependencies,
        no_live_indexing_forced,
        cancel_token.clone(),
        progress.clone(),
    )
    .await;

    let (
        non_blocking_contract_handles,
        processed_network_contracts,
        apply_cross_contract_dependency_events_config_after_processing,
        mut dependency_event_processing_configs,
        network_coordinators,
    ) = contract_events_indexer?;

    // Reject `delivery: finalized` on any network without a live-indexing
    // reorg coordinator: without one, buffered events would never flush and
    // would silently pile up until the process restarts.
    //
    // During the historical leg of a historical+live run the coordinator map
    // is intentionally empty (it's rebuilt for the live leg), so validating
    // here would reject every finalized config. Defer to the live leg in that
    // case; historical-only runs (no live leg to follow) validate now because
    // no future pass will.
    if !no_live_indexing_forced || !manifest.has_any_live_indexing() {
        validate_finalized_delivery_targets(manifest, &network_coordinators)?;
    }

    let trace_indexer_handles = start_indexing_traces(
        manifest,
        project_path,
        database.clone(),
        clickhouse.clone(),
        &indexer,
        trace_registry.clone(),
        cancel_token.clone(),
        progress.clone(),
        &network_coordinators,
        no_live_indexing_forced,
    )
    .await?;

    non_blocking_process_events.extend(trace_indexer_handles);
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
            trace_registry.clone(),
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

    // --- Provider / streams_clients lookup tests ---
    //
    // These cover the fallback from `registry.events` → `trace_registry.events` for
    // native-transfer-only networks (Task 1, Step 2 verification).

    use crate::event::callback_registry::{
        EventCallbackRegistry, TraceCallbackRegistry, TraceCallbackRegistryInformation,
    };
    use crate::event::contract_setup::{NetworkTrace, TraceInformation};
    use crate::manifest::native_transfer::TraceProcessingMethod;
    use crate::provider::ChainProvider;
    use futures::future::{BoxFuture, FutureExt};

    fn trace_registry_with_network(
        network: &str,
        provider: Arc<dyn ChainProvider>,
    ) -> TraceCallbackRegistry {
        let noop_callback: Arc<
            dyn Fn(
                    Vec<crate::event::callback_registry::TraceResult>,
                )
                    -> BoxFuture<'static, crate::event::callback_registry::EventCallbackResult<()>>
                + Send
                + Sync,
        > = Arc::new(|_| async { Ok(()) }.boxed());

        let trace_info = TraceInformation {
            name: "NativeTransfer".to_string(),
            details: vec![NetworkTrace {
                id: "anvil-trace".to_string(),
                network: network.to_string(),
                cached_provider: provider,
                start_block: None,
                end_block: None,
                method: TraceProcessingMethod::EthGetBlockByNumber,
            }],
            reorg_safe_distance: None,
        };

        let info = TraceCallbackRegistryInformation {
            id: "test-id".to_string(),
            indexer_name: "test_indexer".to_string(),
            event_name: "NativeTransfer".to_string(),
            contract_name: "EvmTraces".to_string(),
            trace_information: trace_info,
            callback: noop_callback,
            tables: Arc::new(Vec::new()),
            streams_clients: Arc::new(None),
        };

        TraceCallbackRegistry { events: vec![info], on_reorg: vec![] }
    }

    #[test]
    fn provider_lookup_falls_back_to_trace_registry() {
        // Network "anvil" has no contract events but IS present as a native-transfer
        // target. The trace-registry helper must resolve to the configured provider;
        // both coordinator loops (primary + dep-events) call this helper as their
        // fallback, so a single test covers both callsites.
        let mock_provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(31337).with_block_number(100));
        let trace_registry = trace_registry_with_network("anvil", mock_provider.clone());

        let found = find_provider_in_trace_registry(&trace_registry, "anvil");

        assert!(found.is_some(), "expected provider to be found via trace registry fallback");
        // Identity check: same Arc instance (contract/trace chain provider is the same pointer).
        assert!(Arc::ptr_eq(&found.unwrap(), &mock_provider));
    }

    #[test]
    fn provider_lookup_returns_none_for_unknown_network() {
        // Network "mainnet" is not present in the trace registry → None.
        let mock_provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(31337).with_block_number(100));
        let trace_registry = trace_registry_with_network("anvil", mock_provider);

        let found = find_provider_in_trace_registry(&trace_registry, "mainnet");

        assert!(found.is_none(), "expected no provider for unknown network");
    }

    #[test]
    fn dep_events_provider_fallback_uses_same_helper() {
        // Regression test for code-review I1: both the primary and dep-events loops
        // must resolve native-transfer-only networks via `find_provider_in_trace_registry`.
        // With `dependency_event_processing_configs` empty (i.e. the primary `.iter()`
        // step returns nothing for this network), the fallback must still find the
        // trace-registry provider — i.e. the helper is a drop-in replacement for the
        // previously-inlined `or_else(|| trace_registry.events.iter()...)` closure.
        let mock_provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(31337).with_block_number(100));
        let trace_registry = trace_registry_with_network("anvil", mock_provider.clone());

        // Simulate the dep-events primary lookup yielding None, then the fallback.
        let primary: Option<Arc<dyn ChainProvider>> = None;
        let resolved =
            primary.or_else(|| find_provider_in_trace_registry(&trace_registry, "anvil"));

        assert!(resolved.is_some(), "dep-events fallback must resolve via trace registry");
        assert!(Arc::ptr_eq(&resolved.unwrap(), &mock_provider));
    }

    #[test]
    fn collect_streams_clients_filters_none_entries() {
        // The trace-registry fixture uses `Arc::new(None)` for `streams_clients`.
        // `collect_streams_clients_for_network` must skip entries whose inner
        // Option is None so the coordinator never iterates an absent client.
        let mock_provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(31337).with_block_number(100));
        let trace_registry = trace_registry_with_network("anvil", mock_provider);
        let registry = EventCallbackRegistry::new();

        let collected = collect_streams_clients_for_network(&registry, &trace_registry, "anvil");

        assert!(collected.is_empty(), "entries with Arc::new(None) must be filtered out");
    }

    #[test]
    fn collect_streams_clients_skips_other_networks() {
        // An event on a different network must not appear in the collected
        // vector — the network filter is strict.
        let mock_provider: Arc<dyn ChainProvider> =
            Arc::new(MockChainProvider::new(31337).with_block_number(100));
        let trace_registry = trace_registry_with_network("anvil", mock_provider);
        let registry = EventCallbackRegistry::new();

        let collected = collect_streams_clients_for_network(&registry, &trace_registry, "mainnet");

        assert!(collected.is_empty(), "no entry matches network 'mainnet'");
    }

    // --- Derived-table rollback build tests ---
    //
    // Verifies that `build_derived_tables_for_event` is symmetric for contract
    // events and native-transfer trace events: both sources populate
    // `network_derived_tables` with rollback ops that target their respective
    // source table.

    #[test]
    fn build_derived_tables_for_native_transfer_populates_accumulator() {
        use crate::indexer::tables::TableRuntime;
        use crate::manifest::contract::{
            OperationType, SetAction, SetColumn, Table, TableColumn, TableEventMapping,
            TableOperation,
        };

        // Minimal derived table: one upsert op keyed by `account` that adds
        // `$value` to `total_sent` when a NativeTransfer event fires.
        let table = Table {
            name: "balances".to_string(),
            global: false,
            cross_chain: false,
            columns: vec![
                TableColumn {
                    name: "account".to_string(),
                    column_type: None,
                    nullable: false,
                    default: None,
                },
                TableColumn {
                    name: "total_sent".to_string(),
                    column_type: None,
                    nullable: false,
                    default: None,
                },
            ],
            events: vec![TableEventMapping {
                event: "NativeTransfer".to_string(),
                iterate: Vec::new(),
                operations: vec![TableOperation {
                    operation_type: OperationType::Upsert,
                    where_clause: {
                        let mut m = HashMap::new();
                        m.insert("account".to_string(), "$from".to_string());
                        m
                    },
                    if_condition: None,
                    filter: None,
                    set: vec![SetColumn {
                        column: "total_sent".to_string(),
                        action: SetAction::Add,
                        value: Some("$value".to_string()),
                    }],
                }],
            }],
            cron: None,
            timestamp: false,
            database: None,
        };

        let runtime = TableRuntime::new(table, "test_indexer", NATIVE_TRANSFER_CONTRACT_NAME);
        let tables = vec![runtime];

        let mut accumulator: HashMap<String, Vec<DerivedTableInfo>> = HashMap::new();
        build_derived_tables_for_event(
            "NativeTransfer",
            "test_indexer",
            NATIVE_TRANSFER_CONTRACT_NAME,
            "anvil",
            &tables,
            &mut accumulator,
        )
        .expect("build_derived_tables_for_event should succeed");

        let anvil_entries =
            accumulator.get("anvil").expect("accumulator should contain anvil entry");
        assert_eq!(anvil_entries.len(), 1, "one derived table expected");

        let entry = &anvil_entries[0];
        assert!(
            entry.full_table_name.ends_with("balances"),
            "full_table_name should end with derived table name, got: {}",
            entry.full_table_name,
        );
        assert_eq!(
            entry.rollback_ops.len(),
            1,
            "one rollback op expected for the upsert-with-add column",
        );
        let op = &entry.rollback_ops[0];
        assert!(
            op.event_table.contains("native_transfer"),
            "rollback op should target the native_transfer source table, got: {}",
            op.event_table,
        );
        assert!(entry.journal_columns.is_empty(), "no non-reversible columns in this fixture");
    }
}
