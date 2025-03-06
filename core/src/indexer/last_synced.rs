use std::{path::Path, sync::Arc};

use ethers::prelude::U64;
use rust_decimal::Decimal;
use tokio::{
    fs,
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};
use tracing::error;

use crate::{
    event::config::EventProcessingConfig,
    helpers::{camel_to_snake, get_full_path},
    manifest::{storage::CsvDetails, stream::StreamsConfig},
    EthereumSqlTypeWrapper, PostgresClient,
};

async fn get_last_synced_block_number_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> Result<Option<U64>, UpdateLastSyncedBlockNumberFile> {
    let file_path =
        build_last_synced_block_number_file(full_path, contract_name, network, event_name);

    let path = Path::new(&file_path);

    if !path.exists() {
        return Ok(None);
    }

    let file = File::open(path).await?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();

    if reader.read_line(&mut line).await? > 0 {
        let value = line.trim();
        let parse = U64::from_dec_str(value);
        return match parse {
            Ok(value) => Ok(Some(value)),
            Err(e) => {
                Err(UpdateLastSyncedBlockNumberFile::ParseError(value.to_string(), e.to_string()))
            }
        };
    }

    Ok(None)
}

fn build_last_synced_block_number_file(
    full_path: &Path,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> String {
    let path = full_path.join(contract_name).join("last-synced-blocks").join(format!(
        "{}-{}-{}.txt",
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase()
    ));

    path.to_string_lossy().into_owned()
}

pub struct SyncConfig<'a> {
    pub project_path: &'a Path,
    pub database: &'a Option<Arc<PostgresClient>>,
    pub csv_details: &'a Option<CsvDetails>,
    pub stream_details: &'a Option<&'a StreamsConfig>,
    pub contract_csv_enabled: bool,
    pub indexer_name: &'a str,
    pub contract_name: &'a str,
    pub event_name: &'a str,
    pub network: &'a str,
}

pub async fn get_last_synced_block_number(config: SyncConfig<'_>) -> Option<U64> {
    // Check CSV file for last seen block as no database enabled
    if config.database.is_none() && config.contract_csv_enabled {
        if let Some(csv_details) = config.csv_details {
            return if let Ok(result) = get_last_synced_block_number_file(
                &get_full_path(config.project_path, &csv_details.path).unwrap_or_else(|_| {
                    panic!("failed to get full path {}", config.project_path.display())
                }),
                config.contract_name,
                config.network,
                config.event_name,
            )
            .await
            {
                if let Some(value) = result {
                    if value.is_zero() {
                        return None;
                    }
                }

                result
            } else {
                error!("Error fetching last synced block from CSV");
                None
            }
        }
    }

    // Then check streams if no csv or database to find out last synced block
    if config.database.is_none() && !config.contract_csv_enabled && config.stream_details.is_some()
    {
        let stream_details = config.stream_details.as_ref().unwrap();

        // create the path if it does not exist
        stream_details
            .create_full_streams_last_synced_block_path(config.project_path, config.contract_name)
            .await;

        return if let Ok(result) = get_last_synced_block_number_file(
            &config
                .project_path
                .join(stream_details.get_streams_last_synced_block_path())
                .canonicalize()
                .expect("Failed to canonicalize path"),
            config.contract_name,
            config.network,
            config.event_name,
        )
        .await
        {
            if let Some(value) = result {
                if value.is_zero() {
                    return None;
                }
            }

            result
        } else {
            error!("Error fetching last synced block from stream");
            None
        }
    }

    // Query database for last synced block
    if let Some(database) = config.database {
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{}_{}_{} WHERE network = $1",
            camel_to_snake(config.indexer_name),
            camel_to_snake(config.contract_name),
            camel_to_snake(config.event_name)
        );

        match database.query_one(&query, &[&config.network]).await {
            Ok(row) => {
                let result: Decimal = row.get("last_synced_block");
                let parsed = U64::from_dec_str(&result.to_string())
                    .expect("Failed to parse last_synced_block");
                if parsed.is_zero() {
                    None
                } else {
                    Some(parsed)
                }
            }
            Err(e) => {
                error!("Error fetching last synced block: {:?}", e);
                None
            }
        }
    } else {
        None
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateLastSyncedBlockNumberFile {
    #[error("File IO error: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("Failed to parse block number: {0} err: {0}")]
    ParseError(String, String),
}

async fn update_last_synced_block_number_for_file(
    config: &Arc<EventProcessingConfig>,
    full_path: &Path,
    to_block: U64,
) -> Result<(), UpdateLastSyncedBlockNumberFile> {
    let file_path = build_last_synced_block_number_file(
        full_path,
        &config.contract_name,
        &config.network_contract.network,
        &config.event_name,
    );

    let last_block = get_last_synced_block_number_file(
        full_path,
        &config.contract_name,
        &config.network_contract.network,
        &config.event_name,
    )
    .await?;

    let to_block_higher_then_last_block =
        if let Some(last_block_value) = last_block { to_block > last_block_value } else { true };

    if last_block.is_none() || to_block_higher_then_last_block {
        let temp_file_path = format!("{}.tmp", file_path);

        let mut file = File::create(&temp_file_path).await?;
        file.write_all(to_block.to_string().as_bytes()).await?;
        file.sync_all().await?;

        fs::rename(temp_file_path, file_path).await?;
    }

    Ok(())
}

pub fn update_progress_and_last_synced_task(
    config: Arc<EventProcessingConfig>,
    to_block: U64,
    on_complete: impl FnOnce() + Send + 'static,
) {
    tokio::spawn(async move {
        let update_last_synced_block_result = config
            .progress
            .lock()
            .await
            .update_last_synced_block(&config.network_contract.id, to_block);

        if let Err(e) = update_last_synced_block_result {
            error!("Error updating last synced block: {:?}", e);
        }

        if let Some(database) = &config.database {
            let result = database
                .execute(
                    &format!(
                        "UPDATE rindexer_internal.{}_{}_{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                        camel_to_snake(&config.indexer_name),
                        camel_to_snake(&config.contract_name),
                        camel_to_snake(&config.event_name)
                    ),
                    &[
                        &EthereumSqlTypeWrapper::U64(to_block),
                        &config.network_contract.network,
                    ],
                )
                .await;

            if let Err(e) = result {
                error!("Error updating last synced block: {:?}", e);
            }
        } else if let Some(csv_details) = &config.csv_details {
            if let Err(e) = update_last_synced_block_number_for_file(
                &config,
                &get_full_path(&config.project_path, &csv_details.path).unwrap_or_else(|_| {
                    panic!("failed to get full path {}", config.project_path.display())
                }),
                to_block,
            )
            .await
            {
                error!(
                    "Error updating last synced block to CSV - path - {} error - {:?}",
                    csv_details.path, e
                );
            }
        } else if let Some(stream_last_synced_block_file_path) =
            &config.stream_last_synced_block_file_path
        {
            if let Err(e) = update_last_synced_block_number_for_file(
                &config,
                &config
                    .project_path
                    .join(stream_last_synced_block_file_path)
                    .canonicalize()
                    .expect("Failed to canonicalize path"),
                to_block,
            )
            .await
            {
                error!(
                    "Error updating last synced block to stream - path - {} error - {:?}",
                    stream_last_synced_block_file_path, e
                );
            }
        }

        on_complete();
    });
}
