use std::path::Path;
use futures::TryFutureExt;
use regex::Regex;
use tracing::{debug, info};
use crate::database::clickhouse::client::{ClickhouseClient, ClickhouseConnectionError, ClickhouseError};
use crate::database::clickhouse::generate::generate_tables_for_indexer_clickhouse;
use crate::database::common_sql::generate::GenerateTablesForIndexerSqlError;
use crate::manifest::core::Manifest;

#[derive(thiserror::Error, Debug)]
pub enum SetupClickhouseError {
    #[error("Clickhouse connection error {0}")]
    ClickhouseConnectionError(#[from] ClickhouseConnectionError),
    #[error("Failed to generate tables for indexer: {0}")]
    ClickhouseTableGenerationError(#[from] GenerateTablesForIndexerSqlError),
    #[error("Clickhouse execution error {0}")]
    ClickhouseExecutionError(#[from] ClickhouseError)
}

pub async fn setup_clickhouse(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<ClickhouseClient, SetupClickhouseError> {
    info!("Setting up clickhouse");

    let client = ClickhouseClient::new().await.map_err(SetupClickhouseError::ClickhouseConnectionError)?;

    let sql = generate_tables_for_indexer_clickhouse(
        project_path,
        &manifest.to_indexer(),
        false
    ).map_err(SetupClickhouseError::ClickhouseTableGenerationError)?;

    client.execute_batch(sql.as_str()).await?;
    info!("Created tables for {}", manifest.name);

    Ok(client)
}