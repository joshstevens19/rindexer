use std::error::Error;
use std::path::PathBuf;

use tokio::signal;

use crate::api::{start_graphql_server, GraphQLServerSettings};
use crate::database::postgres::create_tables_for_indexer_sql;
use crate::generator::event_callback_registry::EventCallbackRegistry;
use crate::indexer::start::{start_indexing, StartIndexingSettings};
use crate::manifest::yaml::read_manifest;
use crate::PostgresClient;

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
    pub settings: StartIndexingSettings,
}

pub struct GraphQLServerDetails {
    pub settings: GraphQLServerSettings,
}

pub struct StartDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_server: Option<GraphQLServerDetails>,
}

pub async fn start_rindexer(details: StartDetails) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(&details.manifest_path)?;

    if let Some(graphql_server) = details.graphql_server {
        let _ = start_graphql_server(&manifest.indexers, graphql_server.settings)?;
        if details.indexing_details.is_none() {
            signal::ctrl_c().await.expect("failed to listen for event");
            return Ok(());
        }
    }

    if let Some(indexing_details) = details.indexing_details {
        if manifest.storage.postgres_enabled() {
            let client = PostgresClient::new().await?;

            for indexer in &manifest.indexers {
                let sql = create_tables_for_indexer_sql(indexer);
                println!("{}", sql);
                client.batch_execute(&sql).await?;
            }
        }

        let _ = start_indexing(
            indexing_details.registry.complete(),
            indexing_details.settings,
        )
        .await;
    }

    Ok(())
}
