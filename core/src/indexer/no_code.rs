use std::{io, path::Path, sync::Arc};

use colored::Colorize;
use ethers::abi::{Abi, Contract as EthersContract, Event};
use serde_json::Value;
use tokio_postgres::types::Type as PgType;
use tracing::{debug, error, info, warn};

use crate::{
    abi::{ABIItem, CreateCsvFileForEvent, EventInfo, ParamTypeError, ReadAbiError},
    chat::ChatClients,
    database::postgres::{
        client::PostgresClient,
        generate::{
            generate_column_names_only_with_base_properties, generate_event_table_full_name,
        },
        setup::{setup_postgres, SetupPostgresError},
        sql_type_wrapper::{
            map_ethereum_wrapper_to_json, map_log_params_to_ethereum_wrapper,
            EthereumSqlTypeWrapper,
        },
    },
    event::{
        callback_registry::{
            noop_decoder, EventCallbackRegistry, EventCallbackRegistryInformation,
            EventCallbackType, TxInformation,
        },
        contract_setup::{ContractInformation, CreateContractInformationError},
        EventMessage,
    },
    generate_random_id,
    indexer::log_helpers::{map_log_params_to_raw_values, parse_log},
    manifest::{
        contract::ParseAbiError,
        core::Manifest,
        yaml::{read_manifest, ReadManifestError},
    },
    provider::{CreateNetworkProvider, RetryClientError},
    setup_info_logger,
    streams::StreamsClients,
    AsyncCsvAppender, FutureExt, IndexingDetails, StartDetails, StartNoCodeDetails,
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

            if !details.indexing_details.enabled {
                return Ok(StartDetails {
                    manifest_path: details.manifest_path,
                    indexing_details: None,
                    graphql_details: details.graphql_details,
                });
            }

            let network_providers = CreateNetworkProvider::create(&manifest)?;
            info!(
                "Networks enabled: {}",
                network_providers
                    .iter()
                    .map(|result| result.network_name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", ")
            );

            let events =
                process_events(project_path, &mut manifest, postgres, &network_providers).await?;

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

            Ok(StartDetails {
                manifest_path: details.manifest_path,
                indexing_details: Some(IndexingDetails { registry }),
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
    postgres_event_table_name: String,
    postgres_column_names: Vec<String>,
    streams_clients: Arc<Option<StreamsClients>>,
    chat_clients: Arc<Option<ChatClients>>,
}

fn no_code_callback(params: Arc<NoCodeCallbackParams>) -> EventCallbackType {
    Arc::new(move |results| {
        let params = Arc::clone(&params);

        async move {
            let event_length = results.len();
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

            let (from_block, to_block) = match results.first() {
                Some(first) => (first.found_in_request.from_block, first.found_in_request.to_block),
                None => {
                    let error_message = "Unexpected error: no first event despite non-zero length.";
                    error!("{}", error_message);
                    return Err(error_message.to_string());
                }
            };

            let network = results.first().unwrap().tx_information.network.clone();

            let mut indexed_count = 0;
            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = Vec::new();
            let mut postgres_bulk_column_types: Vec<PgType> = Vec::new();
            let mut csv_bulk_data: Vec<Vec<String>> = Vec::new();

            // stream and chat info
            let mut event_message_data: Vec<Value> = Vec::new();

            // Collect owned results to avoid lifetime issues
            let owned_results: Vec<_> = results
                .iter()
                .filter_map(|result| {
                    let log = parse_log(&params.event, &result.log)?;

                    let address = result.tx_information.address;
                    let transaction_hash = result.tx_information.transaction_hash;
                    let block_number = result.tx_information.block_number;
                    let block_hash = result.tx_information.block_hash;
                    let network = result.tx_information.network.to_string();
                    let transaction_index = result.tx_information.transaction_index;
                    let log_index = result.tx_information.log_index;

                    let event_parameters: Vec<EthereumSqlTypeWrapper> =
                        map_log_params_to_ethereum_wrapper(&params.event_info.inputs, &log.params);

                    let contract_address = EthereumSqlTypeWrapper::Address(address);
                    let end_global_parameters = vec![
                        EthereumSqlTypeWrapper::H256(transaction_hash),
                        EthereumSqlTypeWrapper::U64(block_number),
                        EthereumSqlTypeWrapper::H256(block_hash),
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
                        block_hash,
                        network,
                        contract_address,
                        event_parameters,
                        end_global_parameters,
                    ))
                })
                .collect();

            for (
                log_params,
                address,
                transaction_hash,
                log_index,
                transaction_index,
                block_number,
                block_hash,
                network,
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
                            address,
                            block_hash,
                            block_number,
                            transaction_hash,
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
                if postgres_bulk_column_types.is_empty() {
                    postgres_bulk_column_types =
                        all_params.iter().map(|param| param.to_type()).collect();
                }

                postgres_bulk_data.push(all_params);

                if params.csv.is_some() {
                    let mut csv_data: Vec<String> = vec![format!("{:?}", address)];

                    let raw_values = map_log_params_to_raw_values(&log_params);

                    for param in raw_values {
                        csv_data.push(param);
                    }

                    csv_data.push(format!("{:?}", transaction_hash));
                    csv_data.push(format!("{:?}", block_number));
                    csv_data.push(format!("{:?}", block_hash));
                    csv_data.push(network);

                    csv_bulk_data.push(csv_data);
                }

                indexed_count += 1;
            }

            if let Some(postgres) = &params.postgres {
                let bulk_data_length = postgres_bulk_data.len();
                if bulk_data_length > 0 {
                    // anything over 100 events is considered bulk and goes the COPY route
                    if bulk_data_length > 100 {
                        if let Err(e) = postgres
                            .bulk_insert_via_copy(
                                &params.postgres_event_table_name,
                                &params.postgres_column_names,
                                &postgres_bulk_column_types,
                                &postgres_bulk_data,
                            )
                            .await
                        {
                            error!(
                                "{}::{} - Error performing bulk insert: {}",
                                params.contract_name, params.event_info.name, e
                            );
                            return Err(e.to_string());
                        }
                    } else if let Err(e) = postgres
                        .bulk_insert(
                            &params.postgres_event_table_name,
                            &params.postgres_column_names,
                            &postgres_bulk_data,
                        )
                        .await
                    {
                        error!(
                            "{}::{} - Error performing bulk insert: {}",
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
                network: network.clone(),
            };

            if let Some(streams_clients) = params.streams_clients.as_ref() {
                let stream_id = format!(
                    "{}-{}-{}-{}-{}",
                    params.contract_name, params.event_info.name, network, from_block, to_block
                );

                match streams_clients
                    .stream(stream_id, &event_message, params.index_event_in_order)
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
    })
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

    #[error("{0}")]
    ParseAbiError(#[from] ParseAbiError),
}

pub async fn process_events(
    project_path: &Path,
    manifest: &mut Manifest,
    postgres: Option<Arc<PostgresClient>>,
    network_providers: &[CreateNetworkProvider],
) -> Result<Vec<EventCallbackRegistryInformation>, ProcessIndexersError> {
    let mut events: Vec<EventCallbackRegistryInformation> = vec![];

    for contract in &mut manifest.contracts {
        // TODO - this could be shared with `get_abi_items`
        let abi_str = contract.parse_abi(project_path)?;
        let abi: Abi = serde_json::from_str(&abi_str)?;

        #[allow(clippy::useless_conversion)]
        let abi_gen = EthersContract::from(abi);

        let is_filter = contract.identify_and_modify_filter();
        let abi_items = ABIItem::get_abi_items(project_path, contract, is_filter)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;

        for event_info in event_names {
            let event_name = event_info.name.clone();
            let event = &abi_gen
                .events
                .iter()
                .find(|(name, _)| *name == &event_name)
                .map(|(_, event)| event)
                .ok_or_else(|| {
                    ProcessIndexersError::EventNameNotFoundInAbi(
                        contract.name.clone(),
                        event_name.clone(),
                    )
                })?
                .first()
                .ok_or_else(|| {
                    ProcessIndexersError::EventNameNotFoundInAbi(
                        contract.name.clone(),
                        event_name.clone(),
                    )
                })?
                .clone();

            let contract_information =
                ContractInformation::create(contract, network_providers, noop_decoder())?;

            let mut csv: Option<Arc<AsyncCsvAppender>> = None;
            if contract.generate_csv.unwrap_or(true) && manifest.storage.csv_enabled() {
                let csv_path = manifest.storage.csv.as_ref().map_or("./generated_csv", |c| &c.path);
                let headers: Vec<String> = event_info.csv_headers_for_event();

                let csv_path =
                    event_info.create_csv_file_for_event(project_path, contract, csv_path)?;
                let csv_appender = AsyncCsvAppender::new(&csv_path);
                if !Path::new(&csv_path).exists() {
                    csv_appender.append_header(headers).await?;
                }

                csv = Some(Arc::new(csv_appender));
            }

            let postgres_column_names =
                generate_column_names_only_with_base_properties(&event_info.inputs);
            let postgres_event_table_name =
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
                .map_or(false, |vec| vec.contains(&event_info.name));

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
                    postgres_event_table_name,
                    postgres_column_names,
                    streams_clients: Arc::new(streams_client),
                    chat_clients: Arc::new(chat_clients),
                })),
            };

            events.push(event);
        }
    }

    Ok(events)
}
