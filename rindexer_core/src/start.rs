use ethers::abi::{Abi, Contract as EthersContract, RawLog};
use ethers::prelude::Http;
use ethers::providers::{Provider, RetryClient};
use ethers::types::{Bytes, H256};
use futures::future::BoxFuture;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;

use crate::api::start_graphql_server;
use crate::database::postgres::create_tables_for_indexer_sql;
use crate::generator::build::identify_and_modify_filter;
use crate::generator::event_callback_registry::{
    ContractInformation, Decoder, EventCallbackRegistry, EventInformation, EventResult,
    NetworkContract,
};
use crate::generator::{extract_event_names_and_signatures_from_abi, get_abi_items};
use crate::indexer::start::{start_indexing, StartIndexingSettings};
use crate::manifest::yaml::{read_manifest, Contract, Manifest};
use crate::provider::create_retry_client;
use crate::{generate_random_id, FutureExt, GraphQLServerDetails, PostgresClient};

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
    pub settings: StartIndexingSettings,
}

pub struct StartDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_server: Option<GraphQLServerDetails>,
}

pub async fn start_rindexer(details: StartDetails) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(&details.manifest_path)?;

    if let Some(graphql_server) = details.graphql_server {
        let _ = start_graphql_server(&manifest.indexers, graphql_server.settings)?;
        if details.indexing_details.is_none() {
            signal::ctrl_c().await.expect("failed to listen for event");
            return Ok(());
        }
    }

    if let Some(indexing_details) = details.indexing_details {
        if manifest.storage.postgres_enabled() {
            setup_postgres(&manifest).await?;
        }

        let _ = start_indexing(
            &manifest,
            indexing_details.registry.complete(),
            indexing_details.settings,
        )
        .await;
    }

    Ok(())
}

pub struct StartNoCodeDetails {
    pub manifest_path: PathBuf,
    pub indexing_settings: Option<StartIndexingSettings>,
    pub graphql_server: Option<GraphQLServerDetails>,
}

pub async fn start_rindexer_no_code(details: StartNoCodeDetails) -> Result<(), Box<dyn Error>> {
    let mut manifest = read_manifest(&details.manifest_path)?;
    
    if manifest.storage.postgres_enabled() {
        setup_postgres(&manifest).await?;
    }

    let network_providers = create_network_providers(&manifest);

    let events = process_indexers(&mut manifest, &network_providers).await?;
    let registry = EventCallbackRegistry { events };

    start_rindexer(StartDetails {
        manifest_path: details.manifest_path,
        indexing_details: Some(IndexingDetails {
            registry,
            settings: details.indexing_settings.unwrap_or_default(),
        }),
        graphql_server: details.graphql_server,
    })
    .await
}

async fn setup_postgres(manifest: &Manifest) -> Result<(), Box<dyn Error>> {
    let client = PostgresClient::new().await?;

    for indexer in &manifest.indexers {
        let sql = create_tables_for_indexer_sql(indexer);
        println!("{}", sql);
        client.batch_execute(&sql).await?;
    }

    Ok(())
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

async fn process_indexers(
    manifest: &mut Manifest,
    network_providers: &[(String, Arc<Provider<RetryClient<Http>>>)],
) -> Result<Vec<EventInformation>, Box<dyn Error>> {
    let mut events: Vec<EventInformation> = vec![];

    for indexer in &mut manifest.indexers {
        if manifest.storage.postgres_enabled() {
            let client = PostgresClient::new().await?;

            let sql = create_tables_for_indexer_sql(indexer);
            println!("{}", sql);
            client.batch_execute(&sql).await?;
        }

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

                let decoder: Decoder = Arc::new(move |topics: Vec<H256>, data: Bytes| {
                    Arc::new(
                        event
                            .parse_log(RawLog {
                                topics,
                                data: data.to_vec(),
                            })
                            .map(|log| {
                                log.params
                                    .into_iter()
                                    .map(|param| param.value)
                                    .collect::<Vec<_>>()
                            })
                            .map_err(|err| format!("Error parsing log: {}", err)),
                    )
                });

                let contract_information =
                    create_contract_information(contract, network_providers, decoder);

                let name = event_info.name.clone();
                let callback: Arc<
                    dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync,
                > = Arc::new(move |result| {
                    println!("Event name: {:?} - results {:?}", name, result);
                    let name_clone = name.clone();
                    async move {
                        println!("Event name: {:?} - results {:?}", name_clone, result);
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
