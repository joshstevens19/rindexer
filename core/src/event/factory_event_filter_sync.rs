use crate::database::postgres::client::PostgresError;
use crate::database::postgres::generate::{
    generate_internal_factory_event_table_name, GenerateInternalFactoryEventTableNameParams,
};
use crate::event::callback_registry::EventResult;
use crate::event::config::FactoryEventProcessingConfig;
use crate::helpers::{get_full_path, parse_log};
use crate::manifest::storage::CsvDetails;
use crate::simple_file_formatters::csv::AsyncCsvReader;
use crate::{AsyncCsvAppender, EthereumSqlTypeWrapper, PostgresClient};
use alloy::primitives::Address;
use mini_moka::sync::Cache;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};
use tracing::error;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KnownFactoryDeployedAddress {
    factory_address: Address,
    address: Address,
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateKnownFactoryDeployedAddressesError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error("Could not write addresses to csv: {0}")]
    CsvWrite(#[from] csv::Error),

    #[error("Could not write addresses to postgres: {0}")]
    PostgresWrite(String),

    #[error("Could not parse logs")]
    LogsParse,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
struct KnownFactoryDeployedAddressesCacheKey {
    contract_name: String,
    network: String,
    event_name: String,
    input_name: String,
}

type FactoryDeployedAddressesCache = Cache<KnownFactoryDeployedAddressesCacheKey, HashSet<Address>>;

static IN_MEMORY_CACHE: OnceLock<Arc<FactoryDeployedAddressesCache>> = OnceLock::new();

fn get_in_memory_cache() -> &'static Arc<FactoryDeployedAddressesCache> {
    IN_MEMORY_CACHE.get_or_init(|| Arc::new(Cache::builder().build()))
}

fn build_known_factory_address_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
    input_name: &str,
) -> String {
    let path = full_path.join(contract_name).join("known-factory-addresses").join(format!(
        "{}-{}-{}-{}.csv",
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase(),
        input_name.to_lowercase()
    ));

    path.to_string_lossy().into_owned()
}

fn get_known_factory_deployed_addresses_cache(
    key: &KnownFactoryDeployedAddressesCacheKey,
) -> Option<HashSet<Address>> {
    let cache = get_in_memory_cache();

    cache.get(key)
}

fn set_known_factory_deployed_addresses_cache(
    key: KnownFactoryDeployedAddressesCacheKey,
    value: HashSet<Address>,
) {
    let cache = get_in_memory_cache();

    cache.insert(key, value);
}

fn invalidate_known_factory_deployed_addresses_cache(key: &KnownFactoryDeployedAddressesCacheKey) {
    let cache = get_in_memory_cache();

    cache.invalidate(key);
}

pub async fn update_known_factory_deployed_addresses(
    config: &FactoryEventProcessingConfig,
    events: &[EventResult],
) -> Result<(), UpdateKnownFactoryDeployedAddressesError> {
    let addresses: HashSet<KnownFactoryDeployedAddress> = events
        .iter()
        .map(|event| {
            parse_log(&config.event, &event.log)
                .and_then(|log| log.get_param_value(&config.input_name))
                .and_then(|value| value.as_address())
                .map(|address| KnownFactoryDeployedAddress {
                    factory_address: event.tx_information.address,
                    address,
                })
        })
        .collect::<Option<HashSet<_>>>()
        .ok_or(UpdateKnownFactoryDeployedAddressesError::LogsParse)?;

    // update in memory cache of factory addresses
    let key = KnownFactoryDeployedAddressesCacheKey {
        contract_name: config.contract_name.clone(),
        network: config.network_contract.network.clone(),
        event_name: config.event.name.clone(),
        input_name: config.input_name.clone(),
    };
    invalidate_known_factory_deployed_addresses_cache(&key);

    if let Some(database) = &config.database {
        let params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: config.indexer_name.clone(),
            contract_name: config.contract_name.clone(),
            event_name: config.event.name.clone(),
            input_name: config.input_name.clone(),
        };
        let table_name = generate_internal_factory_event_table_name(&params);

        database
            .insert_bulk(
                &format!("rindexer_internal.{}", table_name),
                &[
                    "factory_address".to_string(),
                    "factory_deployed_address".to_string(),
                    "network".to_string(),
                ],
                &addresses
                    .clone()
                    .into_iter()
                    .map(|item| {
                        vec![
                            EthereumSqlTypeWrapper::Address(item.factory_address),
                            EthereumSqlTypeWrapper::Address(item.address),
                            EthereumSqlTypeWrapper::String(config.network_contract.network.clone()),
                        ]
                    })
                    .collect::<Vec<_>>(),
            )
            .await
            .map_err(UpdateKnownFactoryDeployedAddressesError::PostgresWrite)?;

        return Ok(());
    }

    if let Some(csv_details) = &config.csv_details {
        let full_path = get_full_path(&config.project_path, &csv_details.path)?;

        let csv_path = build_known_factory_address_file(
            &full_path,
            &config.contract_name,
            &config.network_contract.network,
            &config.event.name,
            &config.input_name,
        );
        let csv_appender = AsyncCsvAppender::new(&csv_path);

        if !Path::new(&csv_path).exists() {
            csv_appender
                .append_header(vec![
                    "factory_address".to_string(),
                    "factory_deployed_address".to_string(),
                ])
                .await?;
        }

        csv_appender
            .append_bulk(
                addresses
                    .iter()
                    .map(|item| vec![item.factory_address.to_string(), item.address.to_string()])
                    .collect::<Vec<_>>(),
            )
            .await?;

        return Ok(());
    }

    unreachable!("Can't update known factory deployed addresses without database or csv details")
}

#[derive(thiserror::Error, Debug)]
pub enum GetKnownFactoryDeployedAddressesError {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error("Could not read addresses from csv: {0}")]
    CsvRead(#[from] csv::Error),

    #[error("Could not read addresses from postgres: {0}")]
    PostgresRead(#[from] PostgresError),
}

#[derive(Clone)]
pub struct GetKnownFactoryDeployedAddressesParams {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub input_name: String,
    pub network: String,

    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
}

pub async fn get_known_factory_deployed_addresses(
    params: &GetKnownFactoryDeployedAddressesParams,
) -> Result<Option<HashSet<Address>>, GetKnownFactoryDeployedAddressesError> {
    // check cache first
    let key = KnownFactoryDeployedAddressesCacheKey {
        contract_name: params.contract_name.clone(),
        network: params.network.clone(),
        event_name: params.event_name.clone(),
        input_name: params.input_name.clone(),
    };

    if let Some(cache) = get_known_factory_deployed_addresses_cache(&key) {
        return Ok(Some(cache));
    }

    if let Some(database) = &params.database {
        let table_params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: params.indexer_name.clone(),
            contract_name: params.contract_name.clone(),
            event_name: params.event_name.clone(),
            input_name: params.input_name.clone(),
        };
        let table_name = generate_internal_factory_event_table_name(&table_params);
        let query = format!(
            "SELECT factory_deployed_address FROM rindexer_internal.{} WHERE network = $1",
            table_name
        );
        let result = database
            .query(&query, &[&EthereumSqlTypeWrapper::String(params.network.clone())])
            .await?;

        let values = result
            .into_iter()
            .map(|row| {
                Address::from_str(row.get("factory_deployed_address"))
                    .expect("Factory deployed address not a valid ethereum address")
            })
            .collect::<HashSet<_>>();

        set_known_factory_deployed_addresses_cache(key, values.clone());

        return Ok(Some(values));
    }

    if let Some(csv_details) = &params.csv_details {
        let full_path = get_full_path(&params.project_path, &csv_details.path)?;

        let csv_path = build_known_factory_address_file(
            &full_path,
            &params.contract_name,
            &params.network,
            &params.event_name,
            &params.input_name,
        );

        if !Path::new(&csv_path).exists() {
            return Ok(None);
        }

        let csv_reader = AsyncCsvReader::new(&csv_path);

        let data = csv_reader.read_all().await?;

        // extracting only 'factory_deployed_address' from the csv row
        let values = data
            .into_iter()
            .map(|row| {
                row[1]
                    .parse::<Address>()
                    .expect("Factory deployed address not a valid ethereum address")
            })
            .collect::<HashSet<_>>();

        set_known_factory_deployed_addresses_cache(key, values.clone());

        return Ok(Some(values));
    }

    unreachable!("Can't get known factory deployed addresses without database or csv details")
}
