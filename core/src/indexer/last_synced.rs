use crate::event::contract_setup::NetworkContract;
use crate::helpers::{camel_to_snake, get_full_path};
use crate::indexer::progress::IndexingEventsProgressState;
use crate::manifest::storage::CsvDetails;
use crate::{EthereumSqlTypeWrapper, PostgresClient};
use ethers::prelude::U64;
use rust_decimal::Decimal;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tracing::error;

fn build_last_synced_block_number_for_csv(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> String {
    format!(
        "{}/{}/last-synced-blocks/{}-{}-{}.txt",
        get_full_path(project_path, &csv_details.path).display(),
        contract_name,
        contract_name.to_lowercase(),
        network.to_lowercase(),
        event_name.to_lowercase()
    )
}

async fn get_last_synced_block_number_for_csv(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
) -> Result<Option<U64>, UpdateLastSyncedBlockNumberCsv> {
    let file_path = build_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    );
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
            Err(e) => Err(UpdateLastSyncedBlockNumberCsv::ParseError(
                value.to_string(),
                e.to_string(),
            )),
        };
    }

    Ok(None)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_last_synced_block_number(
    project_path: &Path,
    database: Option<Arc<PostgresClient>>,
    csv_details: &Option<CsvDetails>,
    contract_csv_enabled: bool,
    indexer_name: &str,
    contract_name: &str,
    event_name: &str,
    network: &str,
) -> Option<U64> {
    // check CSV file for last seen block
    if database.is_none() && contract_csv_enabled {
        if let Some(csv_details) = csv_details {
            let result = get_last_synced_block_number_for_csv(
                project_path,
                csv_details,
                contract_name,
                network,
                event_name,
            )
            .await;

            match result {
                Ok(result) => return result,
                Err(e) => {
                    error!("Error fetching last synced block from CSV: {:?}", e);
                }
            }

            return None;
        }
    }

    match database {
        Some(database) => {
            let query = format!(
                "SELECT last_synced_block FROM rindexer_internal.{}_{}_{} WHERE network = $1",
                camel_to_snake(indexer_name),
                camel_to_snake(contract_name),
                camel_to_snake(event_name)
            );

            let row = database.query_one(&query, &[&network]).await;
            match row {
                Ok(row) => {
                    let result: Decimal = row.get("last_synced_block");
                    Some(
                        U64::from_dec_str(&result.to_string())
                            .expect("Failed to parse last_synced_block"),
                    )
                }
                Err(e) => {
                    error!("Error fetching last synced block: {:?}", e);
                    None
                }
            }
        }
        None => None,
    }
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateLastSyncedBlockNumberCsv {
    #[error("File IO error: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("Failed to parse block number: {0} err: {0}")]
    ParseError(String, String),
}

async fn update_last_synced_block_number_for_csv_to_file(
    project_path: &Path,
    csv_details: &CsvDetails,
    contract_name: &str,
    network: &str,
    event_name: &str,
    to_block: U64,
) -> Result<(), UpdateLastSyncedBlockNumberCsv> {
    let file_path = build_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    );

    let last_block = get_last_synced_block_number_for_csv(
        project_path,
        csv_details,
        contract_name,
        network,
        event_name,
    )
    .await?;

    let to_block_higher_then_last_block = if let Some(last_block_value) = last_block {
        to_block > last_block_value
    } else {
        true
    };

    if last_block.is_none() || to_block_higher_then_last_block {
        let temp_file_path = format!("{}.tmp", file_path);

        let mut file = File::create(&temp_file_path).await?;
        file.write_all(to_block.to_string().as_bytes()).await?;
        file.sync_all().await?;

        fs::rename(temp_file_path, file_path).await?;
    }

    Ok(())
}

/// Updates the progress and the last synced block number
#[allow(clippy::too_many_arguments)]
pub fn update_progress_and_last_synced(
    project_path: PathBuf,
    indexer_name: String,
    contract_name: String,
    event_name: String,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Option<Arc<PostgresClient>>,
    csv_details: Option<CsvDetails>,
    to_block: U64,
) {
    tokio::spawn(async move {
        let update_last_synced_block_result = progress
            .lock()
            .await
            .update_last_synced_block(&network_contract.id, to_block);

        if let Err(e) = update_last_synced_block_result {
            error!("Error updating last synced block: {:?}", e);
        }

        if let Some(database) = database {
            let result = database
                .execute(
                    &format!(
                        "UPDATE rindexer_internal.{}_{}_{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                        camel_to_snake(&indexer_name),
                        camel_to_snake(&contract_name),
                        camel_to_snake(&event_name)
                    ),
                    &[
                        &EthereumSqlTypeWrapper::U64(to_block),
                        &network_contract.network,
                    ],
                )
                .await;

            if let Err(e) = result {
                error!("Error updating last synced block: {:?}", e);
            }
        } else if let Some(csv_details) = csv_details {
            if let Err(e) = update_last_synced_block_number_for_csv_to_file(
                &project_path,
                &csv_details,
                &contract_name,
                &network_contract.network,
                &event_name,
                to_block,
            )
            .await
            {
                error!("Error updating last synced block to CSV: {:?}", e);
            }
        }
    });
}
