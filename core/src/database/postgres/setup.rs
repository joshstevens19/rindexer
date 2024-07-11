use crate::database::postgres::client::{PostgresClient, PostgresConnectionError, PostgresError};
use crate::database::postgres::generate::{
    generate_tables_for_indexer_sql, GenerateTablesForIndexerSqlError,
};
use crate::manifest::core::{Manifest, ProjectType};
use std::path::Path;
use tracing::{debug, info};

#[derive(thiserror::Error, Debug)]
pub enum SetupPostgresError {
    #[error("{0}")]
    PostgresConnection(PostgresConnectionError),

    #[error("{0}")]
    PostgresError(PostgresError),

    #[error("Error creating tables for indexer: {0}")]
    GeneratingTables(GenerateTablesForIndexerSqlError),
}

pub async fn setup_postgres(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<PostgresClient, SetupPostgresError> {
    info!("Setting up postgres");
    let client = PostgresClient::new()
        .await
        .map_err(SetupPostgresError::PostgresConnection)?;

    // No-code will ignore this as it must have tables if postgres used
    if !manifest.storage.postgres_disable_create_tables()
        || manifest.project_type == ProjectType::NoCode
    {
        info!("Creating tables for {}", manifest.name);
        let sql = generate_tables_for_indexer_sql(project_path, &manifest.to_indexer())
            .map_err(SetupPostgresError::GeneratingTables)?;
        debug!("{}", sql);
        client
            .batch_execute(sql.as_str())
            .await
            .map_err(SetupPostgresError::PostgresError)?;
        info!("Created tables for {}", manifest.name);
    }

    Ok(client)
}
