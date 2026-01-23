use crate::database::clickhouse::client::ClickhouseError;
use crate::database::generate::generate_internal_factory_event_table_name_no_shorten;
use crate::database::postgres::client::PostgresError;
use crate::database::{
    generate::generate_internal_factory_event_table_name,
    postgres::generate::GenerateInternalFactoryEventTableNameParams,
};
use crate::event::callback_registry::EventResult;
use crate::event::config::FactoryEventProcessingConfig;
use crate::helpers::{get_full_path, parse_log};
use crate::manifest::storage::CsvDetails;
use crate::simple_file_formatters::csv::AsyncCsvReader;
use crate::{AsyncCsvAppender, ClickhouseClient, EthereumSqlTypeWrapper, PostgresClient};
use alloy::primitives::Address;
use mini_moka::sync::Cache;
use serde::Deserialize;
use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, OnceLock};

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

    #[error("Could not write addresses to clickhouse: {0}")]
    ClickhouseWrite(#[from] ClickhouseError),

    #[error("Could not parse logs")]
    LogsParse,
}

#[derive(PartialEq, Eq, Hash)]
struct KnownFactoryDeployedAddressesCacheKey {
    contract_name: String,
    network: String,
    event_name: String,
    input_names: Vec<String>,
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
    input_names: &[String],
) -> String {
    let path = full_path.join(contract_name).join("known-factory-addresses").join(format!(
        "{}-{}-{}-{}.csv",
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase(),
        input_names.iter().map(|v| v.to_lowercase()).collect::<Vec<String>>().join("-")
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
            parse_log(&config.event, &event.log).and_then(|log| {
                config
                    .input_names()
                    .iter()
                    .map(|name| {
                        log.get_param_value(name).and_then(|value| value.as_address()).map(
                            |address| KnownFactoryDeployedAddress {
                                factory_address: event.tx_information.address,
                                address,
                            },
                        )
                    })
                    .collect::<Option<Vec<_>>>()
            })
        })
        .try_fold(HashSet::new(), |mut acc, items| match items {
            Some(items) => {
                acc.extend(items);

                Some(acc)
            }
            None => None,
        })
        .ok_or(UpdateKnownFactoryDeployedAddressesError::LogsParse)?;

    // invalidate in memory cache of factory addresses
    let key = KnownFactoryDeployedAddressesCacheKey {
        contract_name: config.contract_name.clone(),
        network: config.network_contract.network.clone(),
        event_name: config.event.name.clone(),
        input_names: config.input_names(),
    };
    invalidate_known_factory_deployed_addresses_cache(&key);

    if let Some(postgres) = &config.postgres {
        let params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: config.indexer_name.clone(),
            contract_name: config.contract_name.clone(),
            event_name: config.event.name.clone(),
            input_names: config.input_names().clone(),
        };
        let table_name = generate_internal_factory_event_table_name(&params);

        postgres
            .insert_bulk(
                &format!("rindexer_internal.{table_name}"),
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

    if let Some(clickhouse) = &config.clickhouse {
        let params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: config.indexer_name.clone(),
            contract_name: config.contract_name.clone(),
            event_name: config.event.name.clone(),
            input_names: config.input_names().clone(),
        };
        let table_name = generate_internal_factory_event_table_name_no_shorten(&params);

        clickhouse
            .insert_bulk(
                &format!("rindexer_internal.{table_name}"),
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
            .await?;

        return Ok(());
    }

    if let Some(csv_details) = &config.csv_details {
        let full_path = get_full_path(&config.project_path, &csv_details.path)?;

        let csv_path = build_known_factory_address_file(
            &full_path,
            &config.contract_name,
            &config.network_contract.network,
            &config.event.name,
            &config.input_names(),
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

    #[error("Could not read addresses from clickhouse: {0}")]
    ClickhouseRead(#[from] clickhouse::error::Error),
}

#[derive(Clone)]
pub struct GetKnownFactoryDeployedAddressesParams {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub input_names: Vec<String>,
    pub network: String,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
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
        input_names: params.input_names.clone(),
    };

    if let Some(cache) = get_known_factory_deployed_addresses_cache(&key) {
        return Ok(Some(cache));
    }

    if let Some(database) = &params.postgres {
        let table_params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: params.indexer_name.clone(),
            contract_name: params.contract_name.clone(),
            event_name: params.event_name.clone(),
            input_names: params.input_names.clone(),
        };
        let table_name = generate_internal_factory_event_table_name(&table_params);
        let query = format!(
            "SELECT factory_deployed_address FROM rindexer_internal.{table_name} WHERE network = $1"
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

    if let Some(database) = &params.clickhouse {
        let table_params = GenerateInternalFactoryEventTableNameParams {
            indexer_name: params.indexer_name.clone(),
            contract_name: params.contract_name.clone(),
            event_name: params.event_name.clone(),
            input_names: params.input_names.clone(),
        };
        let table_name = generate_internal_factory_event_table_name_no_shorten(&table_params);
        let query = format!(
            r#"
            SELECT toString(factory_deployed_address) AS factory_deployed_address
            FROM rindexer_internal.{table_name} FINAL
            WHERE network = ?
            "#
        );

        #[derive(Debug, clickhouse::Row, Deserialize)]
        struct FactoryDeployedAddresses {
            factory_deployed_address: String,
        }

        let result: Vec<FactoryDeployedAddresses> =
            database.conn.query(&query).bind(params.network.clone()).fetch_all().await?;

        let values = result
            .into_iter()
            .map(|row| {
                Address::from_str(&row.factory_deployed_address)
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
            &params.input_names,
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

/// Parameters for getting factory addresses with their birth blocks.
pub struct GetFactoryAddressesWithBirthBlocksParams {
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub input_names: Vec<String>,
    pub network: String,
    pub postgres: Option<Arc<PostgresClient>>,
    pub clickhouse: Option<Arc<ClickhouseClient>>,
}

/// Get factory-deployed addresses along with their birth blocks (block where they were created).
/// This queries the event table directly to get the block number.
pub async fn get_factory_addresses_with_birth_blocks(
    params: &GetFactoryAddressesWithBirthBlocksParams,
) -> Result<std::collections::HashMap<Address, u64>, GetKnownFactoryDeployedAddressesError> {
    use crate::database::generate::generate_indexer_contract_schema_name;
    use crate::helpers::camel_to_snake;
    use std::collections::HashMap;

    // Generate schema name: {indexer_name}_{contract_name}
    let schema_name =
        generate_indexer_contract_schema_name(&params.indexer_name, &params.contract_name);
    // Event table name is snake_case of event name
    let table_name = camel_to_snake(&params.event_name);
    // Column for the address is snake_case of first input_name
    let address_column =
        params.input_names.first().map(|n| camel_to_snake(n)).unwrap_or_else(|| "pool".to_string());

    if let Some(database) = &params.postgres {
        let query = format!(
            r#"SELECT "{address_column}", block_number FROM {schema_name}.{table_name} WHERE network = $1"#
        );
        tracing::info!(
            "get_factory_addresses_with_birth_blocks query: {} (network={})",
            query,
            params.network
        );
        let result = match database
            .query(&query, &[&EthereumSqlTypeWrapper::String(params.network.clone())])
            .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Query failed: {} - Error: {:?}", query, e);
                return Err(e.into());
            }
        };

        let values: HashMap<Address, u64> = result
            .into_iter()
            .filter_map(|row| {
                let addr_str: &str = row.get(&*address_column);
                let block: rust_decimal::Decimal = row.get("block_number");
                let block_u64 = block.to_string().parse::<u64>().ok()?;
                Address::from_str(addr_str).ok().map(|addr| (addr, block_u64))
            })
            .collect();

        return Ok(values);
    }

    if let Some(database) = &params.clickhouse {
        #[derive(Debug, clickhouse::Row, Deserialize)]
        struct AddressWithBlock {
            address: String,
            block_number: u64,
        }

        let query = format!(
            r#"SELECT toString({address_column}) AS address, block_number FROM {schema_name}.{table_name} FINAL WHERE network = ?"#
        );

        let result: Vec<AddressWithBlock> =
            database.conn.query(&query).bind(params.network.clone()).fetch_all().await?;

        let values: HashMap<Address, u64> = result
            .into_iter()
            .filter_map(|row| {
                Address::from_str(&row.address).ok().map(|addr| (addr, row.block_number))
            })
            .collect();

        return Ok(values);
    }

    Ok(std::collections::HashMap::new())
}
