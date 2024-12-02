use std::path::Path;
use futures::TryFutureExt;
use tracing::{debug, info};
use crate::database::clickhouse::client::{ClickhouseClient, ClickhouseConnectionError};
use crate::manifest::core::Manifest;

#[derive(thiserror::Error, Debug)]
pub enum SetupClickhouseError {
    #[error("{0}")]
    ClickhouseConnection(#[from] ClickhouseConnectionError),
}

pub async fn setup_clickhouse(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<ClickhouseClient, SetupClickhouseError> {
    info!("Setting up clickhouse");
    let client = ClickhouseClient::new().await?;

  /*  let sql = generate_tables_for_indexer_clickhouse(
        project_path,
        &manifest.to_indexer()
    )?;

    debug!("{}", sql);
    client.batch_execute(sql.as_str()).await?;
*/
    info!("Created tables for {}", manifest.name);

    Ok(client)
}
