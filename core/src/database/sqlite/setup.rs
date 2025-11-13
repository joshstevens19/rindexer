use std::path::Path;

use tracing::{debug, info};

use crate::database::sqlite::generate::{
    drop_tables_for_indexer_sql, generate_tables_for_indexer_sql, GenerateTablesForIndexerSqlError,
};
use crate::{
    database::sqlite::client::{SqliteClient, SqliteConnectionError, SqliteError},
    manifest::core::Manifest,
};

#[derive(thiserror::Error, Debug)]
pub enum SetupSqliteError {
    #[error("{0}")]
    SqliteConnection(#[from] SqliteConnectionError),

    #[error("{0}")]
    SqliteError(#[from] SqliteError),

    #[error("Error creating tables for indexer: {0}")]
    GeneratingTables(#[from] GenerateTablesForIndexerSqlError),
}

pub async fn setup_sqlite(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<SqliteClient, SetupSqliteError> {
    info!("Setting up SQLite");

    let client = SqliteClient::new().await?;
    let disable_event_tables = manifest.storage.sqlite_disable_create_tables();

    if manifest.storage.sqlite_drop_each_run() {
        info!(
            "`drop_each_run` enabled so dropping all data for {} before starting",
            &manifest.name
        );
        let sql = drop_tables_for_indexer_sql(project_path, &manifest.to_indexer());
        client.batch_execute(sql.as_str()).await?;
        info!("Dropped all data for {}", manifest.name);
    }

    if disable_event_tables {
        info!("Creating internal rindexer tables for {}", manifest.name);
    } else {
        info!("Creating tables for {}", manifest.name);
    }

    let sql = generate_tables_for_indexer_sql(
        project_path,
        &manifest.to_indexer(),
        disable_event_tables,
    )?;

    debug!("{}", sql);
    client.batch_execute(sql.as_str()).await?;

    if disable_event_tables {
        info!("Created tables for {}", manifest.name);
    } else {
        info!("Created internal rindexer tables for {}", manifest.name);
    }

    Ok(client)
}

