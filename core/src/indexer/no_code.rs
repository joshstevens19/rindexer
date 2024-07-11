use std::path::Path;
use std::sync::Arc;
use std::{fs, io};

use colored::Colorize;
use ethers::abi::{Abi, Contract as EthersContract, Event};
use tokio_postgres::types::Type as PgType;
use tracing::{debug, error, info};

use crate::abi::{ABIItem, CreateCsvFileForEvent, EventInfo, ParamTypeError, ReadAbiError};
use crate::database::postgres::client::PostgresClient;
use crate::database::postgres::generate::{
    generate_column_names_only_with_base_properties, generate_event_table_full_name,
};
use crate::database::postgres::setup::{setup_postgres, SetupPostgresError};
use crate::database::postgres::sql_type_wrapper::{
    map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper,
};
use crate::event::callback_registry::{
    noop_decoder, EventCallbackRegistry, EventCallbackRegistryInformation, EventCallbackType,
};
use crate::event::contract_setup::{ContractInformation, CreateContractInformationError};
use crate::helpers::get_full_path;
use crate::indexer::log_helpers::{map_log_params_to_raw_values, parse_log};
use crate::manifest::core::Manifest;
use crate::manifest::yaml::{read_manifest, ReadManifestError};
use crate::provider::{CreateNetworkProvider, RetryClientError};
use crate::{
    setup_info_logger, AsyncCsvAppender, FutureExt, IndexingDetails, StartDetails,
    StartNoCodeDetails,
};

#[derive(thiserror::Error, Debug)]
pub enum SetupNoCodeError {
    #[error("Could not work out project path from the parent of the manifest")]
    NoProjectPathFoundUsingParentOfManifestPath,

    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(ReadManifestError),

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(SetupPostgresError),

    #[error("{0}")]
    RetryClientError(RetryClientError),

    #[error("Could not process indexers: {0}")]
    ProcessIndexersError(ProcessIndexersError),

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
            let mut manifest = read_manifest(details.manifest_path)
                .map_err(SetupNoCodeError::CouldNotReadManifest)?;
            setup_info_logger();

            info!("Starting rindexer no code");

            let mut postgres: Option<Arc<PostgresClient>> = None;
            if manifest.storage.postgres_enabled() {
                postgres = Some(Arc::new(
                    setup_postgres(project_path, &manifest)
                        .await
                        .map_err(SetupNoCodeError::SetupPostgresError)?,
                ));
            }

            if !details.indexing_details.enabled {
                return Ok(StartDetails {
                    manifest_path: details.manifest_path,
                    indexing_details: None,
                    graphql_details: details.graphql_details,
                });
            }

            let network_providers = CreateNetworkProvider::create(&manifest)
                .map_err(SetupNoCodeError::RetryClientError)?;
            info!(
                "Networks enabled: {}",
                network_providers
                    .iter()
                    .map(|result| result.network_name.as_str())
                    .collect::<Vec<&str>>()
                    .join(", ")
            );

            let events = process_events(project_path, &mut manifest, postgres, &network_providers)
                .await
                .map_err(SetupNoCodeError::ProcessIndexersError)?;

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
    csv: Option<Arc<AsyncCsvAppender>>,
    postgres: Option<Arc<PostgresClient>>,
    postgres_event_table_name: String,
    postgres_column_names: Vec<String>,
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

            let from_block = match results.first() {
                Some(first) => first.tx_information.block_number,
                None => {
                    let error_message = "Unexpected error: no first event despite non-zero length.";
                    error!("{}", error_message);
                    return Err(error_message.to_string());
                }
            };

            let to_block = match results.last() {
                Some(last) => last.tx_information.block_number,
                None => {
                    let error_message = "Unexpected error: no last event despite non-zero length.";
                    error!("{}", error_message);
                    return Err(error_message.to_string());
                }
            };

            let mut indexed_count = 0;
            let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = Vec::new();
            let mut postgres_bulk_column_types: Vec<PgType> = Vec::new();
            let mut csv_bulk_data: Vec<Vec<String>> = Vec::new();

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
                    let tx_index = result.tx_information.transaction_index;
                    let log_index = result.tx_information.log_index;

                    let event_parameters: Vec<EthereumSqlTypeWrapper> =
                        map_log_params_to_ethereum_wrapper(&params.event_info.inputs, &log.params);

                    let contract_address = EthereumSqlTypeWrapper::Address(address);
                    let end_global_parameters = vec![
                        EthereumSqlTypeWrapper::H256(transaction_hash),
                        EthereumSqlTypeWrapper::U64(block_number),
                        EthereumSqlTypeWrapper::H256(block_hash),
                        EthereumSqlTypeWrapper::String(network.to_string()),
                        EthereumSqlTypeWrapper::U64(tx_index),
                        EthereumSqlTypeWrapper::U256(log_index),
                    ];

                    Some((
                        log.params,
                        address,
                        transaction_hash,
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
                block_number,
                block_hash,
                network,
                contract_address,
                event_parameters,
                end_global_parameters,
            ) in owned_results
            {
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

            info!(
                "{}::{} - {} - {} events {}",
                params.contract_name,
                params.event_info.name,
                "INDEXED".green(),
                indexed_count,
                format!("- blocks: {} - {}", from_block, to_block)
            );

            Ok(())
        }
        .boxed()
    })
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessIndexersError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),

    #[error("Could not read ABI items: {0}")]
    CouldNotReadAbiItems(ReadAbiError),

    #[error("Could not append headers to csv: {0}")]
    CsvHeadersAppendError(csv::Error),

    #[error("{0}")]
    CreateContractInformationError(CreateContractInformationError),

    #[error("{0}")]
    CreateCsvFileForEventError(CreateCsvFileForEvent),

    #[error("{0}")]
    ParamTypeError(ParamTypeError),

    #[error("Event name not found in ABI for contract: {0} - event: {1}")]
    EventNameNotFoundInAbi(String, String),
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
        let full_path = get_full_path(project_path, &contract.abi);
        let abi_str =
            fs::read_to_string(full_path).map_err(ProcessIndexersError::CouldNotReadAbiString)?;

        let abi: Abi =
            serde_json::from_str(&abi_str).map_err(ProcessIndexersError::CouldNotReadAbiJson)?;

        #[allow(clippy::useless_conversion)]
        let abi_gen = EthersContract::from(abi);

        let is_filter = contract.identify_and_modify_filter();
        let abi_items = ABIItem::get_abi_items(project_path, contract, is_filter)
            .map_err(ProcessIndexersError::CouldNotReadAbiItems)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)
            .map_err(ProcessIndexersError::ParamTypeError)?;

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
                ContractInformation::create(contract, network_providers, noop_decoder())
                    .map_err(ProcessIndexersError::CreateContractInformationError)?;

            let mut csv: Option<Arc<AsyncCsvAppender>> = None;
            if contract.generate_csv.unwrap_or(true) && manifest.storage.csv_enabled() {
                let csv_path = manifest
                    .storage
                    .csv
                    .as_ref()
                    .map_or("./generated_csv", |c| &c.path);
                let headers: Vec<String> = event_info.csv_headers_for_event();

                let csv_path = event_info
                    .create_csv_file_for_event(project_path, contract, csv_path)
                    .map_err(ProcessIndexersError::CreateCsvFileForEventError)?;
                let csv_appender = AsyncCsvAppender::new(&csv_path);
                if !Path::new(&csv_path).exists() {
                    csv_appender
                        .append_header(headers)
                        .await
                        .map_err(ProcessIndexersError::CsvHeadersAppendError)?;
                }

                csv = Some(Arc::new(csv_appender));
            }

            let postgres_column_names =
                generate_column_names_only_with_base_properties(&event_info.inputs);
            let postgres_event_table_name =
                generate_event_table_full_name(&manifest.name, &contract.name, &event_info.name);

            let event = EventCallbackRegistryInformation {
                indexer_name: manifest.name.clone(),
                event_name: event_info.name.clone(),
                index_event_in_order: contract
                    .index_event_in_order
                    .as_ref()
                    .map_or(false, |vec| vec.contains(&event_info.name)),
                topic_id: event_info.topic_id(),
                contract: contract_information,
                callback: no_code_callback(Arc::new(NoCodeCallbackParams {
                    event_info,
                    indexer_name: manifest.name.clone(),
                    contract_name: contract.name.clone(),
                    event: event.clone(),
                    csv,
                    postgres: postgres.clone(),
                    postgres_event_table_name,
                    postgres_column_names,
                })),
            };

            events.push(event);
        }
    }

    Ok(events)
}
