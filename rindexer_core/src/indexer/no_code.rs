use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_std::prelude::StreamExt;
use colored::Colorize;
use ethers::abi::{Abi, Contract as EthersContract, RawLog};
use ethers::prelude::Http;
use ethers::providers::{Provider, RetryClient};
use ethers::types::{Bytes, H256};
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use tokio_postgres::types::ToSql;
use tracing::level_filters::LevelFilter;
use tracing::{debug, error, info};

use crate::database::postgres::{
    generated_insert_query_for_event, map_log_token_to_ethereum_wrapper, SetupPostgresError,
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
            let events = process_indexers(parent, &mut manifest, postgres, &network_providers)
                .await
                .map_err(SetupNoCodeError::ProcessIndexersError)?;

            let registry = EventCallbackRegistry { events };
            info!(
                "Events registered to index: {}",
                registry
                    .events
                    .iter()
                    .map(|event| event.info_log_name())
                    .collect::<Vec<String>>()
                    .join(", ")
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
}

pub async fn process_indexers(
    project_path: &Path,
    manifest: &mut Manifest,
    postgres: Option<Arc<PostgresClient>>,
    network_providers: &[CreateNetworkProvider],
) -> Result<Vec<EventInformation>, ProcessIndexersError> {
    let mut events: Vec<EventInformation> = vec![];

    for indexer in &mut manifest.indexers {
        for contract in &mut indexer.contracts {
            let abi_str = fs::read_to_string(&contract.abi)
                .map_err(ProcessIndexersError::CouldNotReadAbiString)?;

            let abi: Abi = serde_json::from_str(&abi_str)
                .map_err(ProcessIndexersError::CouldNotReadAbiJson)?;

            #[allow(clippy::useless_conversion)]
            let abi_gen = EthersContract::from(abi);

            let is_filter = identify_and_modify_filter(contract);
            let abi_items = get_abi_items(contract, is_filter)
                .map_err(ProcessIndexersError::CouldNotReadAbiItems)?;
            let event_names = extract_event_names_and_signatures_from_abi(&abi_items)
                .map_err(ProcessIndexersError::ParamTypeError)?;

            for event_info in event_names {
                let event_name = event_info.name.clone();
                let results = &abi_gen
                    .events
                    .iter()
                    .find(|(name, _)| *name == &event_name)
                    .map(|(_, event)| event)
                    .expect("Event not found");

                let event = results.first().expect("Event not found").clone();

                let decoder: Decoder = Arc::new(move |_topics: Vec<H256>, _data: Bytes| {
                    // TODO empty decoder for now to avoid decoder being an option - come back to look at it
                    Arc::new(String::new())
                });

                let contract_information =
                    create_contract_information(contract, network_providers, decoder)
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

                // 2. generate the SQL insert if required
                let postgres_event_table_sql: Arc<Option<String>> = if postgres.is_some() {
                    Arc::new(Some(generated_insert_query_for_event(
                        &event_info,
                        &indexer.name,
                        &contract.name,
                    )))
                } else {
                    Arc::new(None)
                };

                let name = event_info.name.clone();
                let indexer_name = indexer.name.clone();
                let contract_name = contract.name.clone();
                let event_outer = event.clone();
                let postgres_outer = postgres.clone();

                let callback: Arc<
                    dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync,
                > = Arc::new(move |results| {
                    let name_clone = name.clone();
                    let indexer_name = indexer_name.clone();
                    let contract_name = contract_name.clone();
                    let event_clone = event_outer.clone();
                    let csv_clone = csv.clone();
                    let postgres = postgres_outer.clone();
                    let postgres_event_table_sql = postgres_event_table_sql.clone();

                    async move {
                        let event_length = results.len();
                        if event_length == 0 {
                            debug!(
                                "{} {}: {} - {}",
                                indexer_name,
                                contract_name,
                                name_clone,
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

                        let mut futures = FuturesUnordered::new();

                        for result in results {
                            let event_clone = event_clone.clone();
                            let csv_clone = csv_clone.clone();
                            let postgres = postgres.clone();
                            let postgres_event_table_sql = postgres_event_table_sql.clone();

                            futures.push(async move {
                                let log = event_clone
                                    .parse_log(RawLog {
                                        topics: result.log.topics,
                                        data: result.log.data.to_vec(),
                                    })
                                    .map_err(|err| {
                                        let error_message =
                                            format!("Error parsing log: {}", err.to_string().red());
                                        error!(error_message)
                                    })
                                    .unwrap();

                                let address = &result.tx_information.address;
                                let transaction_hash = &result.tx_information.transaction_hash;
                                let block_number = &result.tx_information.block_number;
                                let block_hash = &result.tx_information.block_hash;

                                if let Some(postgres) = &postgres {
                                    if let Some(event_table_sql) = &*postgres_event_table_sql {
                                        let event_parameters: Vec<EthereumSqlTypeWrapper> = log
                                            .params
                                            .iter()
                                            .filter_map(|param| {
                                                // TODO! handle all param types
                                                map_log_token_to_ethereum_wrapper(&param.value)
                                            })
                                            .collect();

                                        let contract_address =
                                            [EthereumSqlTypeWrapper::Address(address)];
                                        let end_global_parameters = [
                                            EthereumSqlTypeWrapper::H256(transaction_hash),
                                            EthereumSqlTypeWrapper::U64(block_number),
                                            EthereumSqlTypeWrapper::H256(block_hash),
                                        ];

                                        let all_params: Vec<&EthereumSqlTypeWrapper> =
                                            contract_address
                                                .iter()
                                                .chain(event_parameters.iter())
                                                .chain(end_global_parameters.iter())
                                                .collect();

                                        let params: Vec<&(dyn ToSql + Sync)> = all_params
                                            .iter()
                                            .map(|&param| param as &(dyn ToSql + Sync))
                                            .collect();

                                        let db_result =
                                            postgres.execute(event_table_sql, &params).await;
                                        if let Err(e) = db_result {
                                            error!("Error inserting into database: {}", e);
                                        }
                                    }
                                }

                                if let Some(csv) = &csv_clone {
                                    let mut csv_data: Vec<String> = vec![];
                                    csv_data.push(format!("{:?}", address));

                                    // TODO! handle all param types
                                    for param in &log.params {
                                        csv_data.push(format!("{:?}", param.value.to_string()));
                                    }

                                    csv_data.push(format!("{:?}", transaction_hash));
                                    csv_data.push(format!("{:?}", block_number));
                                    csv_data.push(format!("{:?}", block_hash));

                                    let csv_result = csv.append(csv_data).await;
                                    if let Err(e) = csv_result {
                                        error!("Error writing CSV to disk: {}", e);
                                    }
                                }
                            });
                        }

                        // run all the futures
                        while futures.next().await.is_some() {}

                        info!(
                            "{} {}: {} - {} - {} events {}",
                            indexer_name,
                            contract_name,
                            name_clone,
                            "INDEXED".green(),
                            event_length,
                            format!("- blocks: {} - {}", from_block, to_block)
                        );
                    }
                    .boxed()
                });

                let event = EventInformation {
                    indexer_name: indexer.name.clone(),
                    event_name: event_info.name.clone(),
                    topic_id: event_info.topic_id(),
                    contract: contract_information,
                    callback,
                };

                events.push(event);
            }
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
