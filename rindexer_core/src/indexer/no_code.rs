use futures::pin_mut;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_std::prelude::StreamExt;
use colored::Colorize;
use ethers::abi::{Abi, Contract as EthersContract, Event, RawLog};
use ethers::prelude::Http;
use ethers::providers::{Provider, RetryClient};
use ethers::types::{Bytes, H256};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, info};

use crate::database::postgres::{
    event_table_full_name, generate_bulk_insert_statement, generate_columns_names_only,
    map_log_token_to_ethereum_wrapper, SetupPostgresError,
};
use crate::generator::build::identify_and_modify_filter;
use crate::generator::event_callback_registry::{
    ContractInformation, Decoder, EventCallbackRegistry, EventInformation, EventResult,
    NetworkContract,
};
use crate::generator::{
    create_csv_file_for_event, csv_headers_for_event, extract_event_names_and_signatures_from_abi,
    get_abi_items, CreateCsvFileForEvent, ParamTypeError, ReadAbiError,
};
use crate::manifest::yaml::{read_manifest, Contract, Manifest, ReadManifestError};
use crate::provider::{create_retry_client, RetryClientError};
use crate::{
    generate_random_id, setup_logger, setup_postgres, AsyncCsvAppender, EthereumSqlTypeWrapper,
    FutureExt, IndexingDetails, PostgresClient, StartDetails, StartNoCodeDetails,
};

#[derive(thiserror::Error, Debug)]
pub enum SetupNoCodeError {
    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(ReadManifestError),

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(SetupPostgresError),

    #[error("{0}")]
    RetryClientError(RetryClientError),

    #[error("Could not process indexers: {0}")]
    ProcessIndexersError(ProcessIndexersError),

    #[error("Could not read manifest path parent")]
    NoParentInManifestPath,
}

pub async fn setup_no_code(details: StartNoCodeDetails) -> Result<StartDetails, SetupNoCodeError> {
    let mut manifest =
        read_manifest(&details.manifest_path).map_err(SetupNoCodeError::CouldNotReadManifest)?;
    setup_logger(LevelFilter::INFO);

    info!("Starting rindexer no code");

    let mut postgres: Option<Arc<PostgresClient>> = None;
    if manifest.storage.postgres_enabled() {
        postgres = Some(Arc::new(
            setup_postgres(&manifest)
                .await
                .map_err(SetupNoCodeError::SetupPostgresError)?,
        ));
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

    let parent = details.manifest_path.parent();
    match parent {
        Some(parent) => {
            let events = process_events(parent, &mut manifest, postgres, &network_providers)
                .await
                .map_err(SetupNoCodeError::ProcessIndexersError)?;

            let registry = EventCallbackRegistry { events };
            info!(
                "Events registered to index:\n {}",
                registry
                    .events
                    .iter()
                    .map(|event| event.info_log_name())
                    .collect::<Vec<String>>()
                    .join("\n")
            );

            Ok(StartDetails {
                manifest_path: details.manifest_path,
                indexing_details: Some(IndexingDetails {
                    registry,
                    settings: details.indexing_settings.unwrap_or_default(),
                }),
                graphql_server: details.graphql_server,
            })
        }
        None => Err(SetupNoCodeError::NoParentInManifestPath),
    }
}

#[derive(Debug)]
pub struct CreateNetworkProvider {
    pub network_name: String,
    pub provider: Arc<Provider<RetryClient<Http>>>,
}

fn create_network_providers(
    manifest: &Manifest,
) -> Result<Vec<CreateNetworkProvider>, RetryClientError> {
    let mut result: Vec<CreateNetworkProvider> = vec![];
    for network in &manifest.networks {
        let provider = create_retry_client(&network.url)?;
        result.push(CreateNetworkProvider {
            network_name: network.name.clone(),
            provider,
        });
    }

    Ok(result)
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

async fn bulk_insert_via_copy(
    client: &PostgresClient,
    data: Vec<Vec<&(dyn ToSql + Sync)>>,
    table_name: &str,
    column_names: &[String],
    column_types: &[Type],
) -> Result<(), Box<dyn Error>> {
    let stmt = format!(
        "COPY {} (contract_address, {}, \"tx_hash\", \"block_number\", \"block_hash\") FROM STDIN WITH (FORMAT binary)",
        table_name, column_names.join(", "),
    );
    let sink = client.copy_in(&stmt).await?;

    let writer = BinaryCopyInWriter::new(sink, column_types);
    pin_mut!(writer);

    for row in data.iter() {
        writer.as_mut().write(row).await?;
    }

    writer.finish().await?;

    Ok(())
}

#[derive(Clone)]
struct NoCodeCallbackParams {
    event_name: String,
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
        let params = params.clone();

        async move {
            let event_length = results.len();
            if event_length == 0 {
                debug!(
                    "{} {}: {} - {}",
                    params.indexer_name,
                    params.contract_name,
                    params.event_name,
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

            let mut bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = Vec::new();
            let mut bulk_column_types: Vec<Type> = Vec::new();
            let mut csv_tasks = FuturesUnordered::new();

            // Collect owned results to avoid lifetime issues
            let owned_results: Vec<_> = results
                .iter()
                .map(|result| {
                    let log = match params.event.parse_log(RawLog {
                        topics: result.log.topics.clone(),
                        data: result.log.data.to_vec(),
                    }) {
                        Ok(log) => log,
                        Err(err) => {
                            error!("Error parsing log: {}", err.to_string().red());
                            return None;
                        }
                    };

                    let address = result.tx_information.address;
                    let transaction_hash = result.tx_information.transaction_hash;
                    let block_number = result.tx_information.block_number;
                    let block_hash = result.tx_information.block_hash;

                    let event_parameters: Vec<EthereumSqlTypeWrapper> = log
                        .params
                        .iter()
                        .filter_map(|param| map_log_token_to_ethereum_wrapper(&param.value))
                        .collect();

                    let contract_address = EthereumSqlTypeWrapper::Address(address);
                    let end_global_parameters = vec![
                        EthereumSqlTypeWrapper::H256(transaction_hash),
                        EthereumSqlTypeWrapper::U64(block_number),
                        EthereumSqlTypeWrapper::H256(block_hash),
                    ];

                    Some((
                        log.params,
                        address,
                        transaction_hash,
                        block_number,
                        block_hash,
                        contract_address,
                        event_parameters,
                        end_global_parameters,
                    ))
                })
                .collect::<Option<Vec<_>>>()
                .unwrap_or_default();

            for (
                log_params,
                address,
                transaction_hash,
                block_number,
                block_hash,
                contract_address,
                event_parameters,
                end_global_parameters,
            ) in owned_results
            {
                let mut all_params: Vec<EthereumSqlTypeWrapper> = vec![contract_address];
                all_params.extend(event_parameters);
                all_params.extend(end_global_parameters);

                // Set column types dynamically based on first result
                if bulk_column_types.is_empty() {
                    bulk_column_types = all_params.iter().map(|param| param.to_type()).collect();
                }

                bulk_data.push(all_params);

                if let Some(csv) = &params.csv {
                    let mut csv_data: Vec<String> = vec![format!("{}", address)];

                    for param in &log_params {
                        csv_data.push(format!("{:?}", param.value.to_string()));
                    }

                    csv_data.push(format!("{:?}", transaction_hash));
                    csv_data.push(format!("{:?}", block_number));
                    csv_data.push(format!("{:?}", block_hash));

                    let csv_clone = csv.clone();
                    csv_tasks.push(Box::pin(async move {
                        if let Err(e) = csv_clone.append(csv_data).await {
                            error!("Error writing CSV to disk: {}", e);
                        }
                    }));
                }
            }

            if let Some(postgres) = &params.postgres {
                // anything over 100 events is considered bulk and goes the COPY route
                if event_length > 100 {
                    if !bulk_data.is_empty() {
                        let bulk_data_refs: Vec<Vec<&(dyn ToSql + Sync)>> = bulk_data
                            .iter()
                            .map(|row| {
                                row.iter()
                                    .map(|param| param as &(dyn ToSql + Sync))
                                    .collect()
                            })
                            .collect();
                        if let Err(e) = bulk_insert_via_copy(
                            postgres,
                            bulk_data_refs,
                            &params.postgres_event_table_name,
                            &params.postgres_column_names,
                            &bulk_column_types,
                        )
                        .await
                        {
                            error!(
                                "{}::{} - Error performing bulk insert: {}",
                                params.contract_name, params.event_name, e
                            );
                        }
                    }
                } else {
                    let (query, params) = generate_bulk_insert_statement(
                        &params.postgres_event_table_name,
                        &params.postgres_column_names,
                        &bulk_data,
                    );
                    if let Err(e) = postgres.execute(&query, &params).await {
                        error!("Error performing bulk insert: {}", e);
                    }
                }
            }

            while (csv_tasks.next().await).is_some() {}

            info!(
                "{}::{} - {} - {} events {}",
                params.contract_name,
                params.event_name,
                "INDEXED".green(),
                event_length,
                format!("- blocks: {} - {}", from_block, to_block)
            );
        }
        .boxed()
    })
}

pub async fn process_events(
    project_path: &Path,
    manifest: &mut Manifest,
    postgres: Option<Arc<PostgresClient>>,
    network_providers: &[CreateNetworkProvider],
) -> Result<Vec<EventInformation>, ProcessIndexersError> {
    let mut events: Vec<EventInformation> = vec![];

    for contract in &mut manifest.contracts {
        let abi_str = fs::read_to_string(&contract.abi)
            .map_err(ProcessIndexersError::CouldNotReadAbiString)?;

        let abi: Abi =
            serde_json::from_str(&abi_str).map_err(ProcessIndexersError::CouldNotReadAbiJson)?;

        #[allow(clippy::useless_conversion)]
        let abi_gen = EthersContract::from(abi);

        let is_filter = identify_and_modify_filter(contract);
        let abi_items = get_abi_items(contract, is_filter)
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

            let contract_information = create_contract_information(
                contract,
                network_providers,
                Arc::new(move |_topics: Vec<H256>, _data: Bytes| {
                    // TODO empty decoder for now to avoid decoder being an option - come back to look at it
                    Arc::new(String::new())
                }),
            )
            .map_err(ProcessIndexersError::CreateContractInformationError)?;

            // 1. generate CSV header if required
            let mut csv: Option<Arc<AsyncCsvAppender>> = None;
            if contract.generate_csv && manifest.storage.csv_enabled() {
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
                topic_id: event_info.topic_id(),
                contract: contract_information,
                callback: no_code_callback(Arc::new(NoCodeCallbackParams {
                    event_name: event_info.name.clone(),
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
                    postgres_column_names: generate_columns_names_only(&event_info.inputs),
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
                    provider: provider.provider.clone(),
                    decoder: decoder.clone(),
                    indexing_contract_setup: c.indexing_contract_setup(),
                    start_block: c.start_block,
                    end_block: c.end_block,
                    polling_every: c.polling_every,
                });
            }
        }
    }

    Ok(ContractInformation {
        name: contract.name.clone(),
        details,
        abi: contract.abi.clone(),
        reorg_safe_distance: contract.reorg_safe_distance,
    })
}
