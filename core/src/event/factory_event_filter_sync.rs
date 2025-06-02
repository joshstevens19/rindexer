use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use alloy::primitives::{Address};
use alloy::rpc::types::ValueOrArray;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;
use tracing::error;
use crate::{AsyncCsvAppender, PostgresClient};
use crate::event::callback_registry::EventResult;
use crate::event::config::{FactoryEventProcessingConfig};
use crate::helpers::{get_full_path, parse_log};
use crate::manifest::storage::CsvDetails;
use crate::simple_file_formatters::csv::AsyncCsvReader;
use mini_moka::sync::Cache;

#[derive(thiserror::Error, Debug)]
pub enum UpdateKnownFactoryDeployedAddressesError {
    #[error("Could not write addresses to csv: {0}")]
    CsvWriteError(#[from] csv::Error),

    #[error("Could not write addresses to cache: {0}")]
    CacheWriteError(String),
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KnownFactoryDeployedAddressesCacheKey {
    contract_name: String,
    network: String,
    event_name: String,
}

// Avoid growing memory, clean up idle keys in case no events are emitted for a specific key
const CACHE_IDLE_DURATION: Duration = Duration::from_secs(60 * 60 * 5);

type FactoryDeployedAddressesCache = Cache<KnownFactoryDeployedAddressesCacheKey, Vec<Address>>;

static IN_MEMORY_CACHE: OnceLock<Arc<FactoryDeployedAddressesCache>> = OnceLock::new();

fn get_in_memory_cache() -> &'static Arc<FactoryDeployedAddressesCache> {
    IN_MEMORY_CACHE.get_or_init(|| Arc::new(Cache::builder().time_to_idle(CACHE_IDLE_DURATION).build()))
}

fn build_known_factory_address_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> String {
    let path = full_path.join(contract_name).join("known-factory-addresses").join(format!(
        "{}-{}-{}.csv",
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase()
    ));

    path.to_string_lossy().into_owned()
}

pub async fn update_known_factory_deployed_addresses(
    config: &FactoryEventProcessingConfig,
    events: &Vec<EventResult>,
) -> Result<(), UpdateKnownFactoryDeployedAddressesError> {
    let addresses: Vec<Address> = events.iter().map(|event|
        parse_log(&config.event, &event.log)
            .and_then(|log| log.params.into_iter().find(|log| log.name == config.input_name))
            .and_then(|param| param.value.as_address())
    ).collect::<Option<Vec<_>>>().unwrap();

    // update in memory cache of factory addresses
    let cache = get_in_memory_cache();

    let cache_key = KnownFactoryDeployedAddressesCacheKey {
        contract_name: config.contract_name.clone(),
        network: config.network_contract.network.clone(),
        event_name: config.event.name.clone(),
    };

    let current_value = cache.get(&cache_key);
    cache.insert(cache_key, [current_value.unwrap_or_default(), addresses.clone()].concat());

    // if let Some(database) = &config.database() {
        //     let schema =
        //         generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
        //     let table_name = generate_internal_event_table_name(&schema, &config.event_name());
        //     let query = format!(
        //         "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
        //         table_name
        //     );
        //     let result = database
        //         .execute(
        //             &query,
        //             &[&EthereumSqlTypeWrapper::U64(to_block), &config.network_contract().network],
        //         )
        //         .await;
        //
        //     if let Err(e) = result {
        //         error!("Error updating last synced block: {:?}", e);
        //     }
        // } else
        //

    if let Some(csv_details) = &config.csv_details {
        let full_path = get_full_path(&config.project_path, &csv_details.path).unwrap();

        let csv_path = build_known_factory_address_file(&full_path, &config.contract_name,
                                                             &config.network_contract.network,
                                                             &config.event.name);
        let csv_appender = AsyncCsvAppender::new(&csv_path);

        if !Path::new(&csv_path).exists() {
            csv_appender.append_header(vec!["factory_deployed_address".to_string()]).await?;
        }

        csv_appender.append_bulk(addresses.iter().map(|address| vec![address.to_string()]).collect::<Vec<_>>()).await?;

        return Ok(())
    }

    unreachable!("Can't update known factory deployed addresses without database or csv details")
}

#[derive(thiserror::Error, Debug)]
pub enum GetKnownFactoryDeployedAddressesError {
    #[error("Could not read addresses from csv: {0}")]
    CsvReadError(#[from] csv::Error),

    #[error("Could not read addresses from cache: {0}")]
    CacheReadError(String),
}

#[derive(Clone)]
pub struct GetKnownFactoryDeployedAddressesParams {
    pub project_path: PathBuf,
    pub contract_address: ValueOrArray<Address>,
    pub contract_name: String,
    pub event_name: String,
    pub network: String,

    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
}

pub async fn get_known_factory_deployed_addresses(
    params: &GetKnownFactoryDeployedAddressesParams,
) -> Result<Option<Vec<Address>>, GetKnownFactoryDeployedAddressesError> {
    // check cache first
    let cache = get_in_memory_cache();

    let cache_key = KnownFactoryDeployedAddressesCacheKey {
        contract_name: params.contract_name.clone(),
        network: params.network.clone(),
        event_name: params.event_name.clone(),
    };

    if let Some(v) = cache.get(&cache_key) {
        return Ok(Some(v.clone()));
    }

    // if let Some(database) = &config.database() {
    //     let schema =
    //         generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
    //     let table_name = generate_internal_event_table_name(&schema, &config.event_name());
    //     let query = format!(
    //         "UPDATE rindexer_internal.{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
    //         table_name
    //     );
    //     let result = database
    //         .execute(
    //             &query,
    //             &[&EthereumSqlTypeWrapper::U64(to_block), &config.network_contract().network],
    //         )
    //         .await;
    //
    //     if let Err(e) = result {
    //         error!("Error updating last synced block: {:?}", e);
    //     }
    // } else
    //

    if let Some(csv_details) = &params.csv_details {
        let full_path = get_full_path(&params.project_path, &csv_details.path).unwrap();

        let csv_path = build_known_factory_address_file(&full_path, &params.contract_name,
                                                        &params.network,
                                                        &params.event_name);

        if !Path::new(&csv_path).exists() {
            return Ok(None);
        }

        let csv_reader = AsyncCsvReader::new(&csv_path);

        let data = csv_reader.read_all().await?;

        return Ok(Some(data.into_iter().map(|row| row[0].parse::<Address>().unwrap()).collect()))
    }

    unreachable!("Can't get known factory deployed addresses without database or csv details")
}
