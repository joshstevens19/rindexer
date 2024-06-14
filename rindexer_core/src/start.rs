use std::error::Error;
use std::path::PathBuf;
use tokio::signal;
use tracing::info;
use tracing::level_filters::LevelFilter;

use crate::api::start_graphql_server;
use crate::generator::event_callback_registry::EventCallbackRegistry;
use crate::indexer::no_code::setup_no_code;
use crate::indexer::start::{start_indexing, StartIndexingSettings};
use crate::manifest::yaml::{read_manifest, ProjectType};
use crate::{setup_logger, setup_postgres, GraphQLServerDetails};

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
    pub settings: StartIndexingSettings,
}

pub struct StartDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_server: Option<GraphQLServerDetails>,
}

pub async fn start_rindexer(details: StartDetails) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(&details.manifest_path)?;

    if manifest.project_type != ProjectType::NoCode {
        setup_logger(LevelFilter::INFO);
        info!("Starting rindexer rust project");
    }

    if let Some(graphql_server) = details.graphql_server {
        let _ = start_graphql_server(&manifest.indexers, graphql_server.settings)?;
        if details.indexing_details.is_none() {
            signal::ctrl_c().await.expect("failed to listen for event");
            return Ok(());
        }
    }

    if let Some(indexing_details) = details.indexing_details {
        // setup postgres is already called in no-code startup
        if manifest.project_type != ProjectType::NoCode && manifest.storage.postgres_enabled() {
            setup_postgres(&manifest).await?;
        }

        let _ = start_indexing(
            &manifest,
            indexing_details.registry.complete(),
            indexing_details.settings,
        )
        .await;
    }

    Ok(())
}

pub struct StartNoCodeDetails {
    pub manifest_path: PathBuf,
    pub indexing_settings: Option<StartIndexingSettings>,
    pub graphql_server: Option<GraphQLServerDetails>,
}

pub async fn start_rindexer_no_code(details: StartNoCodeDetails) -> Result<(), Box<dyn Error>> {
    let start_details = setup_no_code(details).await?;
    start_rindexer(start_details).await
}
