use std::fs;
use std::path::Path;
use std::sync::Arc;

use colored::Colorize;
use ethers::abi::{Abi, Contract as EthersContract, Event};
use futures::future::BoxFuture;
use tokio_postgres::types::Type as PgType;
use tracing::{debug, error, info};

use crate::database::postgres::{
    event_table_full_name, generate_column_names_only_with_base_properties,
    map_log_params_to_ethereum_wrapper, SetupPostgresError,
};
use crate::generator::build::identify_and_modify_filter;
use crate::generator::event_callback_registry::{
    noop_decoder, ContractInformation, Decoder, EventCallbackRegistry, EventInformation,
    EventResult, NetworkContract,
};
use crate::generator::{
    create_csv_file_for_event, csv_headers_for_event, extract_event_names_and_signatures_from_abi,
    get_abi_items, CreateCsvFileForEvent, EventInfo, ParamTypeError, ReadAbiError,
};
use crate::helpers::get_full_path;
use crate::indexer::log_helpers::{map_log_params_to_raw_values, parse_log};
use crate::manifest::yaml::{read_manifest, Contract, Manifest, ReadManifestError};
use crate::provider::{create_client, JsonRpcCachedProvider, RetryClientError};
use crate::{
    generate_random_id, setup_info_logger, setup_postgres, AsyncCsvAppender,
    EthereumSqlTypeWrapper, FutureExt, IndexingDetails, PostgresClient, StartDetails,
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
}

pub async fn setup_no_code(details: StartNoCodeDetails) -> Result<StartDetails, SetupNoCodeError> {
    let project_path = details.manifest_path.parent();
    match project_path {
        Some(project_path) => {
            let mut manifest = read_manifest(&details.manifest_path)
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
                    graphql_server: details.graphql_details.settings,
                });
            }

            let network_providers =
                create_network_providers(&manifest).map_err(SetupNoCodeError::RetryClientError)?;
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
                graphql_server: details.graphql_details.settings,
            })
        }
        None => Err(SetupNoCodeError::NoProjectPathFoundUsingParentOfManifestPath),
    }
}

#[derive(Debug)]
pub struct CreateNetworkProvider {
    pub network_name: String,
    pub client: Arc<JsonRpcCachedProvider>,
}

fn create_network_providers(
    manifest: &Manifest,
) -> Result<Vec<CreateNetworkProvider>, RetryClientError> {
    let mut result: Vec<CreateNetworkProvider> = vec![];
    for network in &manifest.networks {
        let provider = create_client(&network.rpc, network.compute_units_per_second)?;
        result.push(CreateNetworkProvider {
            network_name: network.name.clone(),
            client: provider,
        });
    }

    Ok(result)
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

type NoCodeCallbackResult = Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync>;

fn no_code_callback(params: Arc<NoCodeCallbackParams>) -> NoCodeCallbackResult {
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
                return;
            }
            let from_block = match results.first() {
                Some(first) => first.tx_information.block_number,
                None => {
                    error!("Unexpected error: no first event despite non-zero length.");
                    return;
                }
            };
            let to_block = match results.last() {
                Some(last) => last.tx_information.block_number,
                None => {
                    error!("Unexpected error: no last event despite non-zero length.");
                    return;
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
                    }
                }
            }

            if let Some(csv) = &params.csv {
                if !csv_bulk_data.is_empty() {
                    csv.append_bulk(csv_bulk_data).await.unwrap();
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
        }
        .boxed()
    })
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessIndexersError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(std::io::Error),

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
) -> Result<Vec<EventInformation>, ProcessIndexersError> {
    let mut events: Vec<EventInformation> = vec![];

    for contract in &mut manifest.contracts {
        // TODO - this could be shared with `get_abi_items`
        let full_path = get_full_path(project_path, &contract.abi);
        let abi_str =
            fs::read_to_string(full_path).map_err(ProcessIndexersError::CouldNotReadAbiString)?;

        let abi: Abi =
            serde_json::from_str(&abi_str).map_err(ProcessIndexersError::CouldNotReadAbiJson)?;

        #[allow(clippy::useless_conversion)]
        let abi_gen = EthersContract::from(abi);

        let is_filter = identify_and_modify_filter(contract);
        let abi_items = get_abi_items(project_path, contract, is_filter)
            .map_err(ProcessIndexersError::CouldNotReadAbiItems)?;
        let event_names = extract_event_names_and_signatures_from_abi(&abi_items)
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
                create_contract_information(contract, network_providers, noop_decoder())
                    .map_err(ProcessIndexersError::CreateContractInformationError)?;

            let mut csv: Option<Arc<AsyncCsvAppender>> = None;
            if contract.generate_csv.unwrap_or(true) && manifest.storage.csv_enabled() {
                let csv_path = manifest
                    .storage
                    .csv
                    .as_ref()
                    .map_or("./generated_csv", |c| &c.path);
                let headers: Vec<String> = csv_headers_for_event(&event_info);

                let csv_path =
                    create_csv_file_for_event(project_path, contract, &event_info, csv_path)
                        .map_err(ProcessIndexersError::CreateCsvFileForEventError)?;
                let csv_appender = AsyncCsvAppender::new(csv_path.clone());
                if !Path::new(&csv_path).exists() {
                    csv_appender
                        .append_header(headers)
                        .await
                        .map_err(ProcessIndexersError::CsvHeadersAppendError)?;
                }

                csv = Some(Arc::new(csv_appender));
            }

            let event = EventInformation {
                indexer_name: manifest.name.clone(),
                event_name: event_info.name.clone(),
                index_event_in_order: contract
                    .index_event_in_order
                    .as_ref()
                    .map_or(false, |vec| vec.contains(&event_info.name)),
                topic_id: event_info.topic_id(),
                contract: contract_information,
                callback: no_code_callback(Arc::new(NoCodeCallbackParams {
                    event_info: event_info.clone(),
                    indexer_name: manifest.name.clone(),
                    contract_name: contract.name.clone(),
                    event: event.clone(),
                    csv: csv.clone(),
                    postgres: postgres.clone(),
                    postgres_event_table_name: event_table_full_name(
                        &manifest.name,
                        &contract.name,
                        &event_info.name,
                    ),
                    postgres_column_names: generate_column_names_only_with_base_properties(
                        &event_info.inputs,
                    ),
                })),
            };

            events.push(event);
        }
    }

    Ok(events)
}

#[derive(thiserror::Error, Debug)]
pub enum CreateContractInformationError {
    #[error("Can not find network {0} from providers")]
    CanNotFindNetworkFromProviders(String),
}

fn create_contract_information(
    contract: &Contract,
    network_providers: &[CreateNetworkProvider],
    decoder: Decoder,
) -> Result<ContractInformation, CreateContractInformationError> {
    let mut details = vec![];
    for c in &contract.details {
        let provider = network_providers
            .iter()
            .find(|item| item.network_name == *c.network);

        match provider {
            None => {
                return Err(
                    CreateContractInformationError::CanNotFindNetworkFromProviders(
                        c.network.clone(),
                    ),
                );
            }
            Some(provider) => {
                details.push(NetworkContract {
                    id: generate_random_id(10),
                    network: c.network.clone(),
                    cached_provider: Arc::clone(&provider.client),
                    decoder: decoder.clone(),
                    indexing_contract_setup: c.indexing_contract_setup(),
                    start_block: c.start_block,
                    end_block: c.end_block,
                });
            }
        }
    }

    Ok(ContractInformation {
        name: contract.name.clone(),
        details,
        abi: contract.abi.clone(),
        reorg_safe_distance: contract.reorg_safe_distance.unwrap_or_default(),
    })
}
