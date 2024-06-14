use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
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
use tracing::{error, info};

use crate::database::postgres::{
    generated_insert_query_for_event, map_log_token_to_ethereum_wrapper,
};
use crate::generator::build::identify_and_modify_filter;
use crate::generator::event_callback_registry::{
    ContractInformation, Decoder, EventCallbackRegistry, EventInformation, EventResult,
    NetworkContract,
};
use crate::generator::{
    create_csv_file_for_event, csv_headers_for_event, extract_event_names_and_signatures_from_abi,
    get_abi_items,
};
use crate::manifest::yaml::{read_manifest, Contract, Manifest};
use crate::provider::create_retry_client;
use crate::{
    generate_random_id, setup_logger, setup_postgres, AsyncCsvAppender, EthereumSqlTypeWrapper,
    FutureExt, GraphQLServerDetails, IndexingDetails, PostgresClient, StartDetails,
    StartNoCodeDetails,
};

pub async fn setup_no_code(details: StartNoCodeDetails) -> Result<StartDetails, Box<dyn Error>> {
    let mut manifest = read_manifest(&details.manifest_path)?;
    setup_logger(LevelFilter::INFO);

    info!("Starting rindexer no code");

    let mut postgres: Option<Arc<PostgresClient>> = None;
    if manifest.storage.postgres_enabled() {
        postgres = Some(Arc::new(setup_postgres(&manifest).await?));
    }

    let network_providers = create_network_providers(&manifest);
    info!(
        "Networks enabled: {}",
        network_providers
            .iter()
            .map(|(name, _)| name.as_str()) // Assuming `name` is a String and you need &str
            .collect::<Vec<&str>>()
            .join(", ")
    );

    let events = process_indexers(
        details.manifest_path.parent().unwrap(),
        &mut manifest,
        postgres,
        &network_providers,
    )
    .await?;
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

fn create_network_providers(
    manifest: &Manifest,
) -> Vec<(String, Arc<Provider<RetryClient<Http>>>)> {
    manifest
        .networks
        .iter()
        .map(|network| {
            (
                network.name.clone(),
                create_retry_client(&network.url).unwrap(),
            )
        })
        .collect()
}

pub async fn process_indexers(
    project_path: &Path,
    manifest: &mut Manifest,
    postgres: Option<Arc<PostgresClient>>,
    network_providers: &[(String, Arc<Provider<RetryClient<Http>>>)],
) -> Result<Vec<EventInformation>, Box<dyn Error>> {
    let mut events: Vec<EventInformation> = vec![];

    for indexer in &mut manifest.indexers {
        for contract in &mut indexer.contracts {
            let abi_str = fs::read_to_string(&contract.abi)?;
            let abi: Abi = serde_json::from_str(&abi_str).expect("Failed to parse ABI");
            #[allow(clippy::useless_conversion)]
            let abi_gen = EthersContract::from(abi);

            let is_filter = identify_and_modify_filter(contract);
            let abi_items = get_abi_items(contract, is_filter)?;
            let event_names = extract_event_names_and_signatures_from_abi(&abi_items)?;

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
                    create_contract_information(contract, network_providers, decoder);

                // 1. generate CSV header if required
                let mut csv: Option<Arc<AsyncCsvAppender>> = None;
                if contract.generate_csv && manifest.storage.csv_enabled() {
                    let headers: Vec<String> = csv_headers_for_event(&event_info);
                    let csv_path = create_csv_file_for_event(
                        project_path,
                        contract,
                        &event_info,
                        &manifest.storage.csv,
                    );
                    let csv_appender = AsyncCsvAppender::new(csv_path.clone());
                    if !Path::new(&csv_path).exists() {
                        csv_appender.append_header(headers).await.unwrap();
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
                        let from_block = if match &results.first() {
                            Some(result) => result.tx_information.block_number.is_some(),
                            None => false,
                        } {
                            results.first().unwrap().tx_information.block_number
                        } else {
                            None
                        };
                        let to_block = if match &results.last() {
                            Some(result) => result.tx_information.block_number.is_some(),
                            None => false,
                        } {
                            results.last().unwrap().tx_information.block_number
                        } else {
                            None
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
                                    .map_err(|err| format!("Error parsing log: {}", err))
                                    .unwrap();

                                let address = &result.tx_information.address;
                                let transaction_hash =
                                    result.tx_information.transaction_hash.as_ref().unwrap();
                                let block_number =
                                    result.tx_information.block_number.as_ref().unwrap();
                                let block_hash = result.tx_information.block_hash.as_ref().unwrap();

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

                                    csv.append(csv_data).await.unwrap();
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
                            if from_block.is_some() && to_block.is_some() {
                                format!("- blocks: {} - {}", from_block.unwrap(), to_block.unwrap())
                            } else {
                                String::new()
                            }
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

fn create_contract_information(
    contract: &Contract,
    network_providers: &[(String, Arc<Provider<RetryClient<Http>>>)],
    decoder: Decoder,
) -> ContractInformation {
    ContractInformation {
        name: contract.name.clone(),
        details: contract
            .details
            .iter()
            .map(|c| {
                let provider = network_providers
                    .iter()
                    .find(|(name, _)| name == &c.network)
                    .unwrap()
                    .1
                    .clone();

                NetworkContract {
                    id: generate_random_id(10),
                    network: c.network.clone(),
                    provider,
                    decoder: decoder.clone(),
                    indexing_contract_setup: c.indexing_contract_setup(),
                    start_block: c.start_block,
                    end_block: c.end_block,
                    polling_every: c.polling_every,
                }
            })
            .collect(),
        abi: contract.abi.clone(),
        reorg_safe_distance: contract.reorg_safe_distance,
    }
}
