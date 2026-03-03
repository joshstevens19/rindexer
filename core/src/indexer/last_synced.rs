use alloy::primitives::U64;
use clickhouse::Row;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::time::Duration;
use std::{path::Path, str::FromStr, sync::Arc};
use tokio::{
    fs,
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};
use tracing::{debug, error};

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::generate::{
    generate_internal_cron_table_name, generate_internal_cron_table_name_no_shorten,
    generate_internal_event_table_name_no_shorten,
};
use crate::{
    database::{
        generate::generate_indexer_contract_schema_name,
        postgres::generate::generate_internal_event_table_name,
    },
    event::config::{EventProcessingConfig, TraceProcessingConfig},
    helpers::get_full_path,
    manifest::{storage::CsvDetails, stream::StreamsConfig},
    metrics::indexing as metrics,
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
        let parse = U64::from_str(value);
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
    pub postgres: &'a Option<Arc<PostgresClient>>,
    pub clickhouse: &'a Option<Arc<ClickhouseClient>>,
    pub csv_details: &'a Option<CsvDetails>,
    pub stream_details: &'a Option<&'a StreamsConfig>,
    pub contract_csv_enabled: bool,
    pub indexer_name: &'a str,
    pub contract_name: &'a str,
    pub event_name: &'a str,
    pub network: &'a str,
}

pub async fn get_last_synced_block_number(config: SyncConfig<'_>) -> Option<U64> {
    // 1. Database storage takes priority (matches write-side priority in
    //    update_progress_and_last_synced_task which uses postgres > clickhouse > csv > stream)

    // Query Postgres for last synced block
    if let Some(postgres) = config.postgres {
        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table_name = generate_internal_event_table_name(&schema, config.event_name);
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{table_name} WHERE network = $1"
        );

        return match postgres.query_one(&query, &[&config.network]).await {
            Ok(row) => {
                let result: Decimal = row.get("last_synced_block");
                let parsed =
                    U64::from_str(&result.to_string()).expect("Failed to parse last_synced_block");
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
        };
    }

    // Query ClickHouse for last synced block
    if let Some(clickhouse) = config.clickhouse {
        #[derive(Row, Deserialize)]
        struct LastBlock {
            last_synced_block: u64,
        }

        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table_name = generate_internal_event_table_name_no_shorten(&schema, config.event_name);
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{table_name} FINAL WHERE network = '{}'",
            config.network
        );

        let row = clickhouse.query_one::<LastBlock>(&query).await;

        return match row {
            Ok(row) => {
                let result = row.last_synced_block.to_string();
                let parsed =
                    U64::from_str(&result.to_string()).expect("Failed to parse last_synced_block");

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
        };
    }

    // 2. File-based fallbacks (only when no database storage is configured)

    // Check CSV file for last seen block
    if config.contract_csv_enabled {
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
            };
        }
    }

    // Check stream files for last seen block
    if config.stream_details.is_some() {
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
        };
    }

    None
}

#[derive(thiserror::Error, Debug)]
pub enum UpdateLastSyncedBlockNumberFile {
    #[error("File IO error: {0}")]
    FileIo(#[from] std::io::Error),

    #[error("Failed to parse block number: {0} err: {0}")]
    ParseError(String, String),
}

async fn update_last_synced_block_number_for_file(
    contract_name: &str,
    network: &str,
    event_name: &str,
    full_path: &Path,
    to_block: U64,
) -> Result<(), UpdateLastSyncedBlockNumberFile> {
    let file_path =
        build_last_synced_block_number_file(full_path, contract_name, network, event_name);

    let last_block =
        get_last_synced_block_number_file(full_path, contract_name, network, event_name).await?;

    let to_block_higher_then_last_block =
        if let Some(last_block_value) = last_block { to_block > last_block_value } else { true };

    if last_block.is_none() || to_block_higher_then_last_block {
        let temp_file_path = format!("{file_path}.tmp");

        let mut file = File::create(&temp_file_path).await?;
        file.write_all(to_block.to_string().as_bytes()).await?;
        file.sync_all().await?;

        fs::rename(temp_file_path, file_path).await?;
    }

    Ok(())
}

/// Update the last indexed block.
///
/// Note: this is an async task and should be awaited rather than spawned in the background
/// to prevent overloading concurrent "update" statements on the database which will increase
/// lock contention and slow down the system.
pub async fn update_progress_and_last_synced_task(
    config: Arc<EventProcessingConfig>,
    to_block: U64,
    on_complete: impl FnOnce() + Send + 'static,
) {
    let update_last_synced_block_result = tokio::time::timeout(Duration::from_millis(100), async {
        config
            .progress()
            .lock()
            .await
            .update_last_synced_block(&config.network_contract().id, to_block)
    })
    .await;

    // We don't want the in-memory progress reporter to hold up processing. Under high-ingest
    // workloads, contention can be high enough to hang here.
    match update_last_synced_block_result {
        Ok(Err(e)) => error!("Error updating in-mem last synced block result: {:?}", e),
        Err(_) => debug!("Timeout in update_last_synced_block_result, completing early"),
        _ => {}
    };

    let latest = config
        .network_contract()
        .cached_provider
        .get_latest_block()
        .await
        .ok()
        .flatten()
        .map(|b| b.header.number)
        .unwrap_or(0);

    // Record block progress metrics
    let network = &config.network_contract().network;
    let contract = &config.contract_name();
    let event = &config.event_name();
    let to_block_u64 = to_block.to::<u64>();

    metrics::set_last_synced_block(network, contract, event, to_block_u64);
    if latest > 0 {
        metrics::set_latest_chain_block(network, latest);
        metrics::set_blocks_behind(network, contract, event, to_block_u64, latest);
    }

    if let Some(postgres) = &config.postgres() {
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
        let table_name = generate_internal_event_table_name(&schema, &config.event_name());
        let network = &config.network_contract().network;
        let query = format!(
            "UPDATE rindexer_internal.{table_name} SET last_synced_block = {to_block} WHERE network = '{network}' AND {to_block} > last_synced_block;
             UPDATE rindexer_internal.latest_block SET block = {latest} WHERE network = '{network}' AND {latest} > block;"
        );

        let result = postgres.batch_execute(&query).await;

        if let Err(e) = result {
            error!("Error updating db last synced block: {:?}", e);
        }
    } else if let Some(clickhouse) = &config.clickhouse() {
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name(), &config.contract_name());
        let table_name =
            generate_internal_event_table_name_no_shorten(&schema, &config.event_name());
        let network = &config.network_contract().network;
        let query = format!(
            r#"
            INSERT INTO rindexer_internal.{table_name} (network, last_synced_block) VALUES ('{network}', {to_block});
            INSERT INTO rindexer_internal.latest_block (network, block) VALUES ('{network}', {latest});
            "#
        );

        let result = clickhouse.execute_batch(&query).await;

        if let Err(e) = result {
            error!("Error updating clickhouse last synced block: {:?}", e);
        }
    } else if let Some(csv_details) = &config.csv_details() {
        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name(),
            &config.network_contract().network,
            &config.event_name(),
            &get_full_path(&config.project_path(), &csv_details.path).unwrap_or_else(|_| {
                panic!("failed to get full path {}", config.project_path().display())
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
        &config.stream_last_synced_block_file_path()
    {
        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name(),
            &config.network_contract().network,
            &config.event_name(),
            &config
                .project_path()
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
}

pub async fn evm_trace_update_progress_and_last_synced_task(
    config: Arc<TraceProcessingConfig>,
    to_block: U64,
    on_complete: impl FnOnce() + Send + 'static,
) {
    let update_last_synced_block_result = tokio::time::timeout(Duration::from_millis(100), async {
        config.progress.lock().await.update_last_synced_block(&config.id, to_block)
    })
    .await;

    // We don't want the in-memory progress reporter to hold up processing. Under high-ingest
    // workloads, contention can be high enough to hang here.
    match update_last_synced_block_result {
        Ok(Err(e)) => error!("Error updating in-mem last synced trace result: {:?}", e),
        Err(_) => debug!("Timeout in update_last_synced_block_result, completing early"),
        _ => {}
    }

    if let Some(postgres) = &config.postgres {
        // Use the native_transfer table for all trace events since they share the same pipeline
        let schema =
            generate_indexer_contract_schema_name(&config.indexer_name, &config.contract_name);
        let table_name = generate_internal_event_table_name(&schema, "native_transfer");
        let query = format!(
                "UPDATE rindexer_internal.{table_name} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block"
            );
        let result = postgres
            .execute(&query, &[&EthereumSqlTypeWrapper::U64(to_block.to()), &config.network])
            .await;

        if let Err(e) = result {
            error!("Error updating last synced trace block db: {:?}", e);
        }
    }

    if let Some(csv_details) = &config.csv_details {
        if let Err(e) = update_last_synced_block_number_for_file(
            &config.contract_name,
            &config.network,
            &config.event_name,
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
            &config.contract_name,
            &config.network,
            &config.event_name,
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
}

// ============================================================================
// Cron Sync State Functions
// ============================================================================

/// Configuration for cron sync state operations.
pub struct CronSyncConfig<'a> {
    pub postgres: &'a Option<Arc<PostgresClient>>,
    pub clickhouse: &'a Option<Arc<ClickhouseClient>>,
    pub indexer_name: &'a str,
    pub contract_name: &'a str,
    pub table_name: &'a str,
    pub cron_index: usize,
    pub network: &'a str,
}

/// Get the last synced block for a cron entry from the database.
///
/// Returns `None` if the block is 0 (never synced) or on error.
pub async fn get_last_synced_cron_block(config: CronSyncConfig<'_>) -> Option<U64> {
    // Query PostgreSQL for last synced block
    if let Some(postgres) = config.postgres {
        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table_name =
            generate_internal_cron_table_name(&schema, config.table_name, config.cron_index);
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{table_name} WHERE network = $1"
        );

        return match postgres.query_one(&query, &[&config.network]).await {
            Ok(row) => {
                let result: Decimal = row.get("last_synced_block");
                let parsed =
                    U64::from_str(&result.to_string()).expect("Failed to parse last_synced_block");
                if parsed.is_zero() {
                    None
                } else {
                    Some(parsed)
                }
            }
            Err(e) => {
                error!("Error fetching cron last synced block: {:?}", e);
                None
            }
        };
    }

    // Query ClickHouse for last synced block
    if let Some(clickhouse) = config.clickhouse {
        #[derive(Row, Deserialize)]
        struct LastBlock {
            last_synced_block: u64,
        }

        let schema =
            generate_indexer_contract_schema_name(config.indexer_name, config.contract_name);
        let table_name = generate_internal_cron_table_name_no_shorten(
            &schema,
            config.table_name,
            config.cron_index,
        );
        let query = format!(
            "SELECT last_synced_block FROM rindexer_internal.{table_name} FINAL WHERE network = '{}'",
            config.network
        );

        let row = clickhouse.query_one::<LastBlock>(&query).await;

        return match row {
            Ok(row) => {
                let result = row.last_synced_block;
                if result == 0 {
                    None
                } else {
                    Some(U64::from(result))
                }
            }
            Err(e) => {
                error!("Error fetching cron last synced block from clickhouse: {:?}", e);
                None
            }
        };
    }

    None
}

/// Update the last synced block for a cron entry in the database.
#[allow(clippy::too_many_arguments)]
pub async fn update_last_synced_cron_block(
    postgres: &Option<Arc<PostgresClient>>,
    clickhouse: &Option<Arc<ClickhouseClient>>,
    indexer_name: &str,
    contract_name: &str,
    table_name: &str,
    cron_index: usize,
    network: &str,
    to_block: U64,
) {
    debug!(
        "update_last_synced_cron_block called: indexer={}, contract={}, table={}, cron_index={}, network={}, to_block={}",
        indexer_name, contract_name, table_name, cron_index, network, to_block
    );

    if let Some(postgres) = postgres {
        let schema = generate_indexer_contract_schema_name(indexer_name, contract_name);
        let internal_table_name =
            generate_internal_cron_table_name(&schema, table_name, cron_index);
        let query = format!(
            "UPDATE rindexer_internal.{internal_table_name} SET last_synced_block = {to_block} WHERE network = '{network}' AND {to_block} > last_synced_block"
        );

        debug!("Cron sync update query: {}", query);

        let result = postgres.batch_execute(&query).await;

        match &result {
            Ok(_) => debug!("Cron sync update successful for table {}", internal_table_name),
            Err(e) => error!("Error updating cron last synced block in postgres: {:?}", e),
        }
    } else {
        debug!("update_last_synced_cron_block: postgres is None");
    }

    if let Some(clickhouse) = clickhouse {
        let schema = generate_indexer_contract_schema_name(indexer_name, contract_name);
        let internal_table_name =
            generate_internal_cron_table_name_no_shorten(&schema, table_name, cron_index);
        let query = format!(
            "INSERT INTO rindexer_internal.{internal_table_name} (network, last_synced_block) VALUES ('{network}', {to_block})"
        );

        let result = clickhouse.execute(&query).await;

        if let Err(e) = result {
            error!("Error updating cron last synced block in clickhouse: {:?}", e);
        }
    }
}
