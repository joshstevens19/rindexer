use crate::database::clickhouse::client::{
    ClickhouseClient, ClickhouseConnectionError, ClickhouseError,
};
use crate::database::clickhouse::generate::{
    drop_tables_for_indexer_clickhouse, generate_tables_for_indexer_clickhouse,
};
use crate::database::generate::GenerateTablesForIndexerSqlError;
use crate::manifest::core::Manifest;
use std::path::Path;
use tracing::info;

#[allow(clippy::enum_variant_names)]
#[derive(thiserror::Error, Debug)]
pub enum SetupClickhouseError {
    #[error("Clickhouse connection error {0}")]
    ClickhouseConnectionError(#[from] ClickhouseConnectionError),
    #[error("Failed to generate tables for indexer: {0}")]
    ClickhouseTableGenerationError(#[from] GenerateTablesForIndexerSqlError),
    #[error("Clickhouse execution error {0}")]
    ClickhouseExecutionError(#[from] ClickhouseError),
}

pub async fn setup_clickhouse(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<ClickhouseClient, SetupClickhouseError> {
    info!("Setting up clickhouse");

    let client =
        ClickhouseClient::new().await.map_err(SetupClickhouseError::ClickhouseConnectionError)?;
    let database_name = client.get_database_name();
    let disable_event_tables = manifest.storage.clickhouse_disable_create_tables();

    if manifest.storage.clickhouse_drop_each_run() {
        info!(
            "`drop_each_run` enabled so dropping all data for {} before starting",
            &manifest.name
        );
        let sql =
            drop_tables_for_indexer_clickhouse(project_path, &manifest.to_indexer(), database_name);
        client.execute_batch(sql.as_str()).await?;
        info!("Dropped all data for {}", manifest.name);
    }

    if disable_event_tables {
        info!(
            "Creating internal rindexer tables for {} in database: {}",
            manifest.name, database_name
        );
    } else {
        info!("Creating tables for {} in database: {}", manifest.name, database_name);
    }

    let sql = generate_tables_for_indexer_clickhouse(
        project_path,
        &manifest.to_indexer(),
        database_name,
        disable_event_tables,
    )
    .map_err(SetupClickhouseError::ClickhouseTableGenerationError)?;

    client.execute_batch(sql.as_str()).await?;

    if disable_event_tables {
        info!("Created internal rindexer tables for {}", manifest.name);
    } else {
        info!("Created tables for {}", manifest.name);
    }

    Ok(client)
}
