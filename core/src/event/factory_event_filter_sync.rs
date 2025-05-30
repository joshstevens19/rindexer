use std::path::{Path, PathBuf};
use std::sync::Arc;
use alloy::primitives::{Address};
use alloy::rpc::types::ValueOrArray;
use tokio::io::AsyncWriteExt;
use tracing::error;
use crate::{AsyncCsvAppender, PostgresClient};
use crate::event::callback_registry::EventResult;
use crate::event::config::{FactoryEventProcessingConfig};
use crate::helpers::{get_full_path, parse_log};
use crate::manifest::storage::CsvDetails;
use crate::simple_file_formatters::csv::AsyncCsvReader;

#[derive(thiserror::Error, Debug)]
pub enum UpdateKnownFactoryDeployedAddressesError {
    #[error("Could not write addresses to csv: {0}")]
    CsvWriteError(#[from] csv::Error),
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

    println!("ADDRESS {:?}", events.len());
    println!("ADDRESS {:?}", addresses);

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
) -> Result<Vec<Address>, GetKnownFactoryDeployedAddressesError> {
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
        // if !Path::new(&csv_path).exists() {
        //     csv_reader.append_header(vec!["factory_deployed_address".to_string()]).await.unwrap();
        // }

        let csv_reader = AsyncCsvReader::new(&csv_path);

        let data = csv_reader.read_all().await?;

        println!("GET ADDRESS {:?}", data);

        return Ok(data.into_iter().map(|row| row[0].parse::<Address>().unwrap()).collect())
    }

    unreachable!("Can't get known factory deployed addresses without database or csv details")
}
