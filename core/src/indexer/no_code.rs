use std::{
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use alloy::{
    dyn_abi::DynSolValue,
    json_abi::{Event, JsonAbi},
};
use colored::Colorize;
use serde_json::Value;
use tokio_postgres::types::Type as PgType;
use tracing::{debug, error, info, warn};

use super::native_transfer::{NATIVE_TRANSFER_ABI, NATIVE_TRANSFER_CONTRACT_NAME};
use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::clickhouse::setup::{setup_clickhouse, SetupClickhouseError};
use crate::database::generate::generate_event_table_full_name;
use crate::database::sql_type_wrapper::{
    map_ethereum_wrapper_to_json, map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper,
};
use crate::database::sqlite::client::SqliteClient;
use crate::database::sqlite::setup::{setup_sqlite, SetupSqliteError};
use crate::manifest::contract::Contract;
use crate::{
    abi::{ABIItem, CreateCsvFileForEvent, EventInfo, ParamTypeError, ReadAbiError},
    chat::ChatClients,
    database::postgres::{
        client::PostgresClient,
        generate::generate_column_names_only_with_base_properties,
        setup::{setup_postgres, SetupPostgresError},
    },
    event::{
        callback_registry::{
            noop_decoder, CallbackResult, EventCallbackRegistry, EventCallbackRegistryInformation,
            EventCallbackType, TraceCallbackRegistry, TraceCallbackRegistryInformation,
            TraceCallbackType, TxInformation,
        },
        contract_setup::{ContractInformation, CreateContractInformationError, TraceInformation},
        EventMessage,
    },
    generate_random_id,
    manifest::{
        contract::ParseAbiError,
        core::Manifest,
        yaml::{read_manifest, ReadManifestError},
    },
    provider::{CreateNetworkProvider, RetryClientError},
    setup_info_logger,
    streams::StreamsClients,
    types::core::LogParam,
    AsyncCsvAppender, FutureExt, IndexingDetails, StartDetails, StartNoCodeDetails,
};
use crate::{
    event::callback_registry::TraceResult,
    helpers::{map_log_params_to_raw_values, parse_log},
};

#[derive(thiserror::Error, Debug)]
pub enum SetupNoCodeError {
    #[error("Could not work out project path from the parent of the manifest")]
    NoProjectPathFoundUsingParentOfManifestPath,

    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(#[from] ReadManifestError),

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(#[from] SetupPostgresError),

    #[error("{0}")]
    RetryClientError(#[from] RetryClientError),

    #[error("Could not process indexers: {0}")]
    ProcessIndexersError(#[from] ProcessIndexersError),

    #[error("Could not setup clickhouse: {0}")]
    SetupClickhouseError(#[from] SetupClickhouseError),

    #[error("Could not setup sqlite: {0}")]
    SetupSqliteError(#[from] SetupSqliteError),

    #[error("You have graphql disabled as well as indexer so nothing can startup")]
    NothingToStartNoCode,
}

pub async fn setup_no_code(
    details: StartNoCodeDetails<'_>,
) -> Result<StartDetails<'_>, SetupNoCodeError> {
    if !details.indexing_details.enabled && !details.graphql_details.enabled {
        return Err(SetupNoCodeError::NothingToStartNoCode);
    }
    let project_path = details.manifest_path.parent();

    match project_path {
        Some(project_path) => {
            let mut manifest = read_manifest(details.manifest_path)?;
            setup_info_logger();

            info!("Starting rindexer no code");

            let mut postgres: Option<Arc<PostgresClient>> = None;
            if manifest.storage.postgres_enabled() {
                postgres = Some(Arc::new(setup_postgres(project_path, &manifest).await?));
            }

            let mut clickhouse: Option<Arc<ClickhouseClient>> = None;
            if manifest.storage.clickhouse_enabled() {
                clickhouse = Some(Arc::new(setup_clickhouse(project_path, &manifest).await?));
            }

            let mut sqlite: Option<Arc<SqliteClient>> = None;
            if manifest.storage.sqlite_enabled() {
                sqlite = Some(Arc::new(setup_sqlite(project_path, &manifest).await?));
            }

            if !details.indexing_details.enabled {
                return Ok(StartDetails {
                    manifest_path: details.manifest_path,
                    indexing_details: None,
                    graphql_details: details.graphql_details,
                });
            }

            let network_providers = CreateNetworkProvider::create(&manifest).await?;
            info!(
                "Networks enabled: {}",
                network_providers
                    .iter()
                    .map(|result| result.network_name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", ")
            );

            let events = process_events(
                project_path,
                &manifest,
                postgres.clone(),
                clickhouse.clone(),
                sqlite.clone(),
                &network_providers,
            )
            .await?;

            let registry = EventCallbackRegistry { events };
            info!(
                "Events registered to index:{}",
                registry
                    .events
                    .iter()
                    .map(|event| event.info_log_name())
                    .collect::<Vec<String>>()
                    .join(", ")
            );

            let trace_events = process_trace_events(
                project_path,
                &mut manifest,
                postgres,
                clickhouse,
                sqlite,
                &network_providers,
            )
            .await?;
            let trace_registry = TraceCallbackRegistry { events: trace_events };

            if manifest.has_enabled_native_transfers() {
                info!(
                    "Native token transfers to index: {}",
                    manifest
                        .native_transfers
                        .networks
                        .unwrap_or_default()
                        .iter()
                        .map(|network| network.network.clone())
                        .collect::<Vec<String>>()
                        .join(", ")
                );
            }

            Ok(StartDetails {
                manifest_path: details.manifest_path,
                indexing_details: Some(IndexingDetails {
                    registry,
                    trace_registry,
                    event_stream: None,
                }),
                graphql_details: details.graphql_details,
            })
        }
        None => Err(SetupNoCodeError::NoProjectPathFoundUsingParentOfManifestPath),
    }
}

#[derive(Clone)]
struct NoCodeCallbackParams {
    event_info: EventInfo,
    indexer_name: String,
    contract_name: String,
    event: Event,
    index_event_in_order: bool,
    csv: Option<Arc<AsyncCsvAppender>>,
    postgres: Option<Arc<PostgresClient>>,
    sqlite: Option<Arc<SqliteClient>>,
    sql_event_table_name: String,
    sql_column_names: Vec<String>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    streams_clients: Arc<Option<StreamsClients>>,
    chat_clients: Arc<Option<ChatClients>>,
}

struct EventCallbacks {
    event_callback: EventCallbackType,
    trace_callback: TraceCallbackType,
}

fn no_code_callback(params: Arc<NoCodeCallbackParams>) -> EventCallbacks {
    let shared_callback = Arc::new(move |results| {
        let params = Arc::clone(&params);

        async move {
            let event_length = match &results {
                CallbackResult::Event(event) => event.len(),
                CallbackResult::Trace(event) => event.len(),
            };

            if event_length == 0 {
                debug!(
                    "{} {}: {} - {}",
                    params.indexer_name,
                    params.contract_name,
                    params.event_info.name,
                    "NO EVENTS".red()
                );

                return Ok(());
            }

            // TODO
            // Remove unwrap
            let (from_block, to_block) = match &results {
                CallbackResult::Event(event) => {
                    let first = event.first().ok_or("No events found")?;
                    (first.found_in_request.from_block, first.found_in_request.to_block)
                }
                CallbackResult::Trace(event) => {
                    // Filter to only NativeTransfer events and get the first one
                    let native_transfer = event
                        .iter()
                        .filter_map(|result| match result {
                            TraceResult::NativeTransfer { found_in_request, .. } => {
                                Some(found_in_request)
                            }
                            TraceResult::Block { .. } => None,
                        })
                        .next();
                    
                    match native_transfer {
                        Some(transfer) => (transfer.from_block, transfer.to_block),
                        None => {
                            debug!(
                                "{} {}: {} - {}",
                                params.indexer_name,
                                params.contract_name,
                                params.event_info.name,
                                "NO NATIVE TRANSFER EVENTS (only Block events)".red()
                            );
                            return Ok(());
                        }
                    }
                }
            };

            let network = match &results {
                CallbackResult::Event(event) => {
                    event.first().ok_or("No events found")?.tx_information.network.clone()
                }
                CallbackResult::Trace(event) => {
                    // Filter to only NativeTransfer events and get the first one
                    let network = event
                        .iter()
                        .filter_map(|result| match result {
                            TraceResult::NativeTransfer { tx_information, .. } => {
                                Some(&tx_information.network)
                            }
                            TraceResult::Block { .. } => None,
                        })
                        .next();
                    
                    match network {
                        Some(net) => net.clone(),
                        None => {
                            // This shouldn't happen as we already checked for NativeTransfer above
                            // but handle it gracefully just in case
                            return Ok(());
                        }
                    }
                }
            };

            let mut indexed_count = 0;
            let mut sql_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = Vec::new();
            let mut sql_bulk_column_types: Vec<PgType> = Vec::new();
            let mut csv_bulk_data: Vec<Vec<String>> = Vec::new();

            // stream and chat info
            let mut event_message_data: Vec<Value> = Vec::new();

            let owned_results = match &results {
                CallbackResult::Event(events) => events
                    .iter()
                    .filter_map(|result| {
                        let log = parse_log(&params.event, &result.log)?;

                        let address = result.tx_information.address;
                        let transaction_hash = result.tx_information.transaction_hash;
                        let block_number = result.tx_information.block_number;
                        let block_timestamp = result
                            .tx_information
                            .block_timestamp
                            .and_then(|ts| chrono::DateTime::from_timestamp(ts.to(), 0));
                        let block_hash = result.tx_information.block_hash;
                        let network = result.tx_information.network.to_string();
                        let chain_id = result.tx_information.chain_id;
                        let transaction_index = result.tx_information.transaction_index;
                        let log_index = result.tx_information.log_index;

                        let event_parameters: Vec<EthereumSqlTypeWrapper> =
                            map_log_params_to_ethereum_wrapper(
                                &params.event_info.inputs,
                                &log.params,
                            );

                        let contract_address = EthereumSqlTypeWrapper::Address(address);
                        let end_global_parameters = vec![
                            EthereumSqlTypeWrapper::B256(transaction_hash),
                            EthereumSqlTypeWrapper::U64(block_number),
                            EthereumSqlTypeWrapper::DateTimeNullable(block_timestamp),
                            EthereumSqlTypeWrapper::B256(block_hash),
                            EthereumSqlTypeWrapper::String(network.to_string()),
                            EthereumSqlTypeWrapper::U64(transaction_index),
                            EthereumSqlTypeWrapper::U256(log_index),
                        ];

                        Some((
                            log.params,
                            address,
                            transaction_hash,
                            log_index,
                            transaction_index,
                            block_number,
                            result.tx_information.block_timestamp,
                            block_hash,
                            network,
                            chain_id,
                            contract_address,
                            event_parameters,
                            end_global_parameters,
                        ))
                    })
                    .collect::<Vec<_>>(),
                CallbackResult::Trace(events) => events
                    .iter()
                    .filter_map(|result| {
                        match result {
                            TraceResult::NativeTransfer {
                                from, to, value, tx_information, ..
                            } => {
                                let log_params = vec![
                                    LogParam::new("from".to_string(), DynSolValue::Address(*from)),
                                    LogParam::new("to".to_string(), DynSolValue::Address(*to)),
                                    LogParam::new(
                                        "value".to_string(),
                                        DynSolValue::Uint(*value, 256),
                                    ),
                                ];

                                let address = tx_information.address;
                                let transaction_hash = tx_information.transaction_hash;
                                let block_number = tx_information.block_number;
                                let block_timestamp = tx_information
                                    .block_timestamp
                                    .and_then(|ts| chrono::DateTime::from_timestamp(ts.to(), 0));
                                let block_hash = tx_information.block_hash;
                                let network = tx_information.network.to_string();
                                let chain_id = tx_information.chain_id;
                                let transaction_index = tx_information.transaction_index;
                                let log_index = tx_information.log_index;

                                let event_parameters: Vec<EthereumSqlTypeWrapper> =
                                    map_log_params_to_ethereum_wrapper(
                                        &params.event_info.inputs,
                                        &log_params,
                                    );

                                let contract_address = EthereumSqlTypeWrapper::Address(address);
                                let end_global_parameters = vec![
                                    EthereumSqlTypeWrapper::B256(transaction_hash),
                                    EthereumSqlTypeWrapper::U64(block_number),
                                    EthereumSqlTypeWrapper::DateTimeNullable(block_timestamp),
                                    EthereumSqlTypeWrapper::B256(block_hash),
                                    EthereumSqlTypeWrapper::String(network.to_string()),
                                    EthereumSqlTypeWrapper::U64(transaction_index),
                                    EthereumSqlTypeWrapper::U256(log_index),
                                ];

                                Some((
                                    log_params,
                                    address,
                                    transaction_hash,
                                    log_index,
                                    transaction_index,
                                    block_number,
                                    tx_information.block_timestamp,
                                    block_hash,
                                    network,
                                    chain_id,
                                    contract_address,
                                    event_parameters,
                                    end_global_parameters,
                                ))
                            }
                            TraceResult::Block { .. } => None, // Skip block events in no-code mode
                        }
                    })
                    .collect::<Vec<_>>(),
            };

            for (
                log_params,
                address,
                transaction_hash,
                log_index,
                transaction_index,
                block_number,
                block_timestamp,
                block_hash,
                network,
                chain_id,
                contract_address,
                event_parameters,
                end_global_parameters,
            ) in owned_results
            {
                if params.streams_clients.is_some() || params.chat_clients.is_some() {
                    let event_result = map_ethereum_wrapper_to_json(
                        &params.event_info.inputs,
                        &event_parameters,
                        &TxInformation {
                            network: network.clone(),
                            chain_id,
                            address,
                            block_hash,
                            block_number,
                            transaction_hash,
                            block_timestamp,
                            log_index,
                            transaction_index,
                        },
                        false,
                    );
                    event_message_data.push(event_result);
                }

                let mut all_params: Vec<EthereumSqlTypeWrapper> = vec![contract_address];
                all_params.extend(event_parameters);
                all_params.extend(end_global_parameters);

                // Set column types dynamically based on first result
                if sql_bulk_column_types.is_empty() {
                    sql_bulk_column_types =
                        all_params.iter().map(|param| param.to_type()).collect();
                }

                if params.postgres.is_some()
                    || params.clickhouse.is_some()
                    || params.sqlite.is_some()
                {
                    sql_bulk_data.push(all_params);
                }

                if params.csv.is_some() {
                    let mut csv_data: Vec<String> = vec![format!("{:?}", address)];

                    let raw_values = map_log_params_to_raw_values(&log_params);

                    for param in raw_values {
                        csv_data.push(param);
                    }

                    csv_data.push(format!("{transaction_hash:?}"));
                    csv_data.push(format!("{block_number:?}"));
                    csv_data.push(format!("{block_hash:?}"));
                    csv_data.push(network);

                    csv_bulk_data.push(csv_data);
                }

                indexed_count += 1;
            }

            if let Some(postgres) = &params.postgres {
                if !sql_bulk_data.is_empty() {
                    if let Err(e) = postgres
                        .insert_bulk(
                            &params.sql_event_table_name,
                            &params.sql_column_names,
                            &sql_bulk_data,
                        )
                        .await
                    {
                        error!(
                            "{}::{} - Error performing postgres bulk insert: {}",
                            params.contract_name, params.event_info.name, e
                        );
                        return Err(e.to_string());
                    }
                }
            }

            if let Some(clickhouse) = &params.clickhouse {
                if !sql_bulk_data.is_empty() {
                    if let Err(e) = clickhouse
                        .insert_bulk(
                            &params.sql_event_table_name,
                            &params.sql_column_names,
                            &sql_bulk_data,
                        )
                        .await
                    {
                        error!(
                            "{}::{} - Error performing clickhouse bulk insert: {}",
                            params.contract_name, params.event_info.name, e
                        );
                        return Err(e.to_string());
                    };
                }
            }

            if let Some(sqlite) = &params.sqlite {
                if !sql_bulk_data.is_empty() {
                    if let Err(e) = sqlite
                        .insert_bulk(
                            &params.sql_event_table_name,
                            &params.sql_column_names,
                            &sql_bulk_data,
                        )
                        .await
                    {
                        error!(
                            "{}::{} - Error performing sqlite bulk insert: {}",
                            params.contract_name, params.event_info.name, e
                        );
                        return Err(e.to_string());
                    }
                }
            }

            if let Some(csv) = &params.csv {
                if !csv_bulk_data.is_empty() {
                    if let Err(e) = csv.append_bulk(csv_bulk_data).await {
                        return Err(e.to_string());
                    }
                }
            }

            let event_message = EventMessage {
                event_name: params.event_info.name.clone(),
                event_data: Value::Array(event_message_data),
                event_signature_hash: params.event.selector(),
                network: network.clone(),
            };

            if let Some(streams_clients) = params.streams_clients.as_ref() {
                let stream_id = format!(
                    "{}-{}-{}-{}-{}",
                    params.contract_name, params.event_info.name, network, from_block, to_block
                );

                let is_trace_event = match results {
                    CallbackResult::Event(_) => false,
                    CallbackResult::Trace(_) => true,
                };

                match streams_clients
                    .stream(stream_id, &event_message, params.index_event_in_order, is_trace_event)
                    .await
                {
                    Ok(streamed) => {
                        if streamed > 0 {
                            info!(
                                "{}::{} - {} - {} events {}",
                                params.contract_name,
                                params.event_info.name,
                                "STREAMED".green(),
                                streamed,
                                format!(
                                    "- blocks: {} - {} - network: {}",
                                    from_block, to_block, network
                                )
                            );
                        }
                    }
                    Err(e) => {
                        error!("Error streaming event: {}", e);
                        return Err(e.to_string());
                    }
                }
            }

            if let Some(chat_clients) = params.chat_clients.as_ref() {
                if !chat_clients.is_in_block_range_to_send(&from_block, &to_block) {
                    warn!(
                        "{}::{} - {} - messages has a max 10 block range due the rate limits - {}",
                        params.contract_name,
                        params.event_info.name,
                        "CHAT_MESSAGES_DISABLED".yellow(),
                        format!("- blocks: {} - {} - network: {}", from_block, to_block, network)
                    );
                } else {
                    match chat_clients
                        .send_message(
                            &event_message,
                            params.index_event_in_order,
                            &from_block,
                            &to_block,
                        )
                        .await
                    {
                        Ok(messages_sent) => {
                            if messages_sent > 0 {
                                info!(
                                    "{}::{} - {} - {} events {}",
                                    params.contract_name,
                                    params.event_info.name,
                                    "CHAT_MESSAGES_SENT".green(),
                                    messages_sent,
                                    format!(
                                        "- blocks: {} - {} - network: {}",
                                        from_block, to_block, network
                                    )
                                );
                            }
                        }
                        Err(e) => {
                            error!("Error sending chat messages: {}", e);
                            return Err(e.to_string());
                        }
                    }
                }
            }

            info!(
                "{}::{} - {} - {} events {}",
                params.contract_name,
                params.event_info.name,
                "INDEXED".green(),
                indexed_count,
                format!("- blocks: {} - {} - network: {}", from_block, to_block, network)
            );

            Ok(())
        }
        .boxed()
    });

    let callback = Arc::clone(&shared_callback);
    let event_callback: EventCallbackType =
        Arc::new(move |events| callback(CallbackResult::Event(events)));

    let callback = Arc::clone(&shared_callback);
    let trace_callback: TraceCallbackType =
        Arc::new(move |traces| callback(CallbackResult::Trace(traces)));

    EventCallbacks { trace_callback, event_callback }
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessIndexersError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("Could not read ABI items: {0}")]
    CouldNotReadAbiItems(#[from] ReadAbiError),

    #[error("Could not append headers to csv: {0}")]
    CsvHeadersAppendError(#[from] csv::Error),

    #[error("{0}")]
    CreateContractInformationError(#[from] CreateContractInformationError),

    #[error("{0}")]
    CreateCsvFileForEventError(#[from] CreateCsvFileForEvent),

    #[error("{0}")]
    ParamTypeError(#[from] ParamTypeError),

    #[error("Event name not found in ABI for contract: {0} - event: {1}")]
    EventNameNotFoundInAbi(String, String),

    #[error("Contract name is reserved: {0}. Please use another contract name.")]
    ContractNameConflict(String),

    #[error("{0}")]
    ParseAbiError(#[from] ParseAbiError),
}

pub async fn process_events(
    project_path: &Path,
    manifest: &Manifest,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    sqlite: Option<Arc<SqliteClient>>,
    network_providers: &[CreateNetworkProvider],
) -> Result<Vec<EventCallbackRegistryInformation>, ProcessIndexersError> {
    let mut events: Vec<EventCallbackRegistryInformation> = vec![];

    for mut contract in manifest.all_contracts().clone() {
        let contract_events = process_contract(
            project_path,
            manifest,
            postgres.clone(),
            clickhouse.clone(),
            sqlite.clone(),
            network_providers,
            &mut contract,
        )
        .await?;

        events.extend(contract_events);
    }

    Ok(events)
}

async fn process_contract(
    project_path: &Path,
    manifest: &Manifest,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    sqlite: Option<Arc<SqliteClient>>,
    network_providers: &[CreateNetworkProvider],
    contract: &mut Contract,
) -> Result<Vec<EventCallbackRegistryInformation>, ProcessIndexersError> {
    if contract.name.to_lowercase() == NATIVE_TRANSFER_CONTRACT_NAME.to_lowercase() {
        return Err(ProcessIndexersError::ContractNameConflict(contract.name.to_string()));
    }

    // TODO - this could be shared with `get_abi_items`
    let abi_str = contract.parse_abi(project_path)?;
    let abi: JsonAbi = serde_json::from_str(&abi_str)?;
    let is_filter = contract.identify_and_modify_filter();
    let abi_items = ABIItem::get_abi_items(project_path, contract, is_filter)?;
    let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;

    let mut events: Vec<EventCallbackRegistryInformation> = vec![];

    for event_info in event_names {
        let event_name = event_info.name.clone();
        let event = abi
            .events
            .get(&event_name)
            .and_then(|events| events.first())
            .ok_or_else(|| {
                ProcessIndexersError::EventNameNotFoundInAbi(
                    contract.name.clone(),
                    event_name.clone(),
                )
            })?
            .clone();

        let contract_information = ContractInformation::create(
            project_path,
            contract,
            network_providers,
            noop_decoder(),
            manifest,
        )?;

        let mut csv: Option<Arc<AsyncCsvAppender>> = None;
        if contract.generate_csv.unwrap_or(true) && manifest.storage.csv_enabled() {
            let csv_path =
                manifest.storage.csv.as_ref().map_or(PathBuf::from("generated_csv"), |c| {
                    PathBuf::from(c.path.strip_prefix("./").unwrap())
                });

            let headers: Vec<String> = event_info.csv_headers_for_event();
            let csv_path_str = csv_path.to_str().expect("Failed to convert csv path to string");
            let csv_path =
                event_info.create_csv_file_for_event(project_path, &contract.name, csv_path_str)?;
            let csv_appender = AsyncCsvAppender::new(&csv_path);
            if !Path::new(&csv_path).exists() {
                csv_appender.append_header(headers).await?;
            }

            csv = Some(Arc::new(csv_appender));
        }

        let sql_column_names = generate_column_names_only_with_base_properties(&event_info.inputs);
        let sql_event_table_name =
            generate_event_table_full_name(&manifest.name, &contract.name, &event_info.name);

        let streams_client = if let Some(streams) = &contract.streams {
            Some(StreamsClients::new(streams.clone()).await)
        } else {
            None
        };

        let chat_clients = if let Some(chats) = &contract.chat {
            Some(ChatClients::new(chats.clone()).await)
        } else {
            None
        };

        let index_event_in_order = contract
            .index_event_in_order
            .as_ref()
            .is_some_and(|vec| vec.contains(&event_info.name));

        let event = EventCallbackRegistryInformation {
            id: generate_random_id(10),
            indexer_name: manifest.name.clone(),
            event_name: event_info.name.clone(),
            index_event_in_order,
            topic_id: event_info.topic_id(),
            contract: contract_information,
            callback: no_code_callback(Arc::new(NoCodeCallbackParams {
                event_info,
                indexer_name: manifest.name.clone(),
                contract_name: contract.name.clone(),
                event: event.clone(),
                index_event_in_order,
                csv,
                postgres: postgres.clone(),
                sqlite: sqlite.clone(),
                clickhouse: clickhouse.clone(),
                sql_event_table_name,
                sql_column_names,
                streams_clients: Arc::new(streams_client),
                chat_clients: Arc::new(chat_clients),
            }))
            .event_callback,
        };

        events.push(event);
    }

    Ok(events)
}

pub async fn process_trace_events(
    project_path: &Path,
    manifest: &mut Manifest,
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    sqlite: Option<Arc<SqliteClient>>,
    network_providers: &[CreateNetworkProvider],
) -> Result<Vec<TraceCallbackRegistryInformation>, ProcessIndexersError> {
    let mut events: Vec<TraceCallbackRegistryInformation> = vec![];

    if !manifest.has_enabled_native_transfers() {
        return Ok(events);
    }

    let abi_str = NATIVE_TRANSFER_ABI;
    let abi: JsonAbi = serde_json::from_str(abi_str)?;

    #[allow(clippy::useless_conversion)]
    let abi_items: Vec<ABIItem> = serde_json::from_str(abi_str)?;
    let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;

    let contract = &manifest.native_transfers;
    let contract_name = NATIVE_TRANSFER_CONTRACT_NAME.to_string();

    for event_info in event_names {
        let event_name = event_info.name.clone();
        let event = &abi
            .events
            .iter()
            .find(|(name, _)| *name == &event_name)
            .map(|(_, event)| event)
            .ok_or_else(|| {
                ProcessIndexersError::EventNameNotFoundInAbi(
                    contract_name.clone(),
                    event_name.clone(),
                )
            })?
            .first()
            .ok_or_else(|| {
                ProcessIndexersError::EventNameNotFoundInAbi(
                    contract_name.clone(),
                    event_name.clone(),
                )
            })?
            .clone();

        let trace_information =
            TraceInformation::create(manifest.native_transfers.clone(), network_providers)?;

        let mut csv: Option<Arc<AsyncCsvAppender>> = None;
        if contract.generate_csv.unwrap_or(true) && manifest.storage.csv_enabled() {
            let csv_path =
                manifest.storage.csv.as_ref().map_or(PathBuf::from("generated_csv"), |c| {
                    PathBuf::from(c.path.strip_prefix("./").unwrap())
                });

            let headers: Vec<String> = event_info.csv_headers_for_event();
            let csv_path_str = csv_path.to_str().expect("Failed to convert csv path to string");
            let csv_path =
                event_info.create_csv_file_for_event(project_path, &contract_name, csv_path_str)?;
            let csv_appender = AsyncCsvAppender::new(&csv_path);
            if !Path::new(&csv_path).exists() {
                csv_appender.append_header(headers).await?;
            }

            csv = Some(Arc::new(csv_appender));
        }

        let sql_column_names = generate_column_names_only_with_base_properties(&event_info.inputs);
        let sql_event_table_name =
            generate_event_table_full_name(&manifest.name, &contract_name, &event_info.name);

        let streams_client = if let Some(streams) = &contract.streams {
            Some(StreamsClients::new(streams.clone()).await)
        } else {
            None
        };

        let chat_clients = if let Some(chats) = &contract.chat {
            Some(ChatClients::new(chats.clone()).await)
        } else {
            None
        };

        let callback_params = Arc::new(NoCodeCallbackParams {
            event_info: event_info.clone(),
            indexer_name: manifest.name.clone(),
            contract_name: contract_name.clone(),
            event: event.clone(),
            index_event_in_order: false,
            csv,
            postgres: postgres.clone(),
            sqlite: sqlite.clone(),
            clickhouse: clickhouse.clone(),
            sql_event_table_name,
            sql_column_names,
            streams_clients: Arc::new(streams_client),
            chat_clients: Arc::new(chat_clients),
        });

        let event = TraceCallbackRegistryInformation {
            id: generate_random_id(10),
            indexer_name: manifest.name.clone(),
            event_name: event_info.name.clone(),
            contract_name: contract_name.clone(),
            trace_information: trace_information.clone(),
            callback: no_code_callback(callback_params).trace_callback,
        };

        events.push(event);
    }

    Ok(events)
}
