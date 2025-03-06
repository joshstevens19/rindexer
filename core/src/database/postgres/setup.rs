use std::path::Path;

use tracing::{debug, info};

use crate::{
    database::postgres::{
        client::{PostgresClient, PostgresConnectionError, PostgresError},
        generate::{generate_tables_for_indexer_sql, GenerateTablesForIndexerSqlError},
    },
    drop_tables_for_indexer_sql,
    manifest::core::Manifest,
};

#[derive(thiserror::Error, Debug)]
pub enum SetupPostgresError {
    #[error("{0}")]
    PostgresConnection(#[from] PostgresConnectionError),

    #[error("{0}")]
    PostgresError(#[from] PostgresError),

    #[error("Error creating tables for indexer: {0}")]
    GeneratingTables(#[from] GenerateTablesForIndexerSqlError),
}

pub async fn setup_postgres(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<PostgresClient, SetupPostgresError> {
    info!("Setting up postgres");
    let client = PostgresClient::new().await?;

    let disable_event_tables = manifest.storage.postgres_disable_create_tables();

    if manifest.storage.postgres_drop_each_run() {
        info!(
            "`drop_each_run` enabled so dropping all data for {} before starting",
            &manifest.name
        );
        let sql = drop_tables_for_indexer_sql(project_path, &manifest.to_indexer());
        client.batch_execute(sql.as_str()).await?;
        info!("Dropped all data for {}", manifest.name);
    }

    if !disable_event_tables {
        info!("Creating tables for {}", manifest.name);
    } else {
        info!("Creating internal rindexer tables for {}", manifest.name);
    }
    let sql = generate_tables_for_indexer_sql(
        project_path,
        &manifest.to_indexer(),
        disable_event_tables,
    )?;
    debug!("{}", sql);
    client.batch_execute(sql.as_str()).await?;
    if !disable_event_tables {
        info!("Created tables for {}", manifest.name);
    } else {
        info!("Created internal rindexer tables for {}", manifest.name);
    }

    Ok(client)
}
