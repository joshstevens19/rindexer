use std::path::Path;

use tracing::{debug, info};

use crate::{
    database::postgres::{
        client::{PostgresClient, PostgresConnectionError, PostgresError},
        generate::{generate_tables_for_indexer_sql, GenerateTablesForIndexerSqlError},
    },
    manifest::core::{Manifest, ProjectType},
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

    // No-code will ignore this as it must have tables if postgres used
    if !manifest.storage.postgres_disable_create_tables() ||
        manifest.project_type == ProjectType::NoCode
    {
        info!("Creating tables for {}", manifest.name);
        let sql = generate_tables_for_indexer_sql(project_path, &manifest.to_indexer())?;
        debug!("{}", sql);
        client.batch_execute(sql.as_str()).await?;
        info!("Created tables for {}", manifest.name);
    }

    Ok(client)
}
