use std::{path::PathBuf, sync::Arc};

use tokio::signal;
use tracing::{error, info};

use crate::{
    api::{start_graphql_server, GraphqlOverrideSettings, StartGraphqlServerError},
    database::postgres::{
        client::PostgresConnectionError,
        indexes::{ApplyPostgresIndexesError, PostgresIndexResult},
        relationship::{ApplyAllRelationships, Relationship},
        setup::{setup_postgres, SetupPostgresError},
    },
    event::callback_registry::EventCallbackRegistry,
    indexer::{
        no_code::{setup_no_code, SetupNoCodeError},
        start::{start_indexing, StartIndexingError},
        ContractEventDependencies, ContractEventDependenciesMapFromRelationshipsError,
    },
    initiate_shutdown,
    logger::mark_shutdown_started,
    manifest::{
        core::ProjectType,
        storage::RelationshipsAndIndexersError,
        yaml::{read_manifest, ReadManifestError},
    },
    setup_info_logger,
};

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
}

pub struct StartDetails<'a> {
    pub manifest_path: &'a PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_details: GraphqlOverrideSettings,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerError {
    #[error("Could not work out project path from the parent of the manifest")]
    NoProjectPathFoundUsingParentOfManifestPath,

    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(#[from] ReadManifestError),

    #[error("Could not start graphql error {0}")]
    CouldNotStartGraphqlServer(#[from] StartGraphqlServerError),

    #[error("Failed to listen to graphql socket")]
    FailedToListenToGraphqlSocket,

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(#[from] SetupPostgresError),

    #[error("Could not start indexing: {0}")]
    CouldNotStartIndexing(#[from] StartIndexingError),

    #[error("{0}")]
    PostgresConnectionError(#[from] PostgresConnectionError),

    #[error("{0}")]
    ApplyRelationshipError(#[from] ApplyAllRelationships),

    #[error("Could not apply indexes: {0}")]
    ApplyPostgresIndexesError(#[from] ApplyPostgresIndexesError),

    #[error("{0}")]
    ContractEventDependenciesMapFromRelationshipsError(
        #[from] ContractEventDependenciesMapFromRelationshipsError,
    ),

    #[error("{0}")]
    RelationshipsAndIndexersError(#[from] RelationshipsAndIndexersError),

    #[error("Shutdown handler failed with error: {0}")]
    ShutdownHandlerFailed(String),
}

async fn handle_shutdown(signal: &str) {
    // Mark shutdown state only once, at the very beginning of the shutdown process
    mark_shutdown_started();
    info!("Received {} signal gracefully shutting down...", signal);
    initiate_shutdown().await;
    // These info! calls work because they're before/after the shutdown process
    info!("Graceful shutdown completed for {}", signal);
    std::process::exit(0);
}

pub async fn start_rindexer(details: StartDetails<'_>) -> Result<(), StartRindexerError> {
    let project_path = details.manifest_path.parent();
    match project_path {
        Some(project_path) => {
            let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate())
                .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;
            let mut sigint = signal::unix::signal(signal::unix::SignalKind::interrupt())
                .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;
            let mut sigquit = signal::unix::signal(signal::unix::SignalKind::quit())
                .map_err(|e| StartRindexerError::ShutdownHandlerFailed(e.to_string()))?;

            let shutdown_handle = tokio::spawn(async move {
                tokio::select! {
                    _ = sigterm.recv() => handle_shutdown("SIGTERM").await,
                    _ = sigint.recv() => handle_shutdown("SIGINT (Ctrl+C)").await,
                    _ = sigquit.recv() => handle_shutdown("SIGQUIT").await,
                }
            });

            let manifest = Arc::new(read_manifest(details.manifest_path)?);

            if manifest.project_type != ProjectType::NoCode {
                setup_info_logger();
                info!("Starting rindexer rust project");
            }

            // Spawn a separate task for the GraphQL server if specified
            let graphql_server_handle =
                if details.graphql_details.enabled && manifest.storage.postgres_enabled() {
                    let manifest_clone = Arc::clone(&manifest);
                    let indexer = manifest_clone.to_indexer();
                    let mut graphql_settings = manifest.graphql.clone().unwrap_or_default();
                    if let Some(override_port) = &details.graphql_details.override_port {
                        graphql_settings.set_port(*override_port);
                    }
                    Some(tokio::spawn(async move {
                        if let Err(e) = start_graphql_server(&indexer, &graphql_settings).await {
                            error!("Failed to start GraphQL server: {:?}", e);
                        }
                    }))
                } else {
                    None
                };

            if graphql_server_handle.is_none() && details.graphql_details.enabled {
                error!("GraphQL can not run without postgres storage enabled, you have tried to run GraphQL which will now be skipped.");
            }

            if let Some(mut indexing_details) = details.indexing_details {
                let postgres_enabled = &manifest.storage.postgres_enabled();

                // setup postgres is already called in no-code startup
                if manifest.project_type != ProjectType::NoCode && *postgres_enabled {
                    setup_postgres(project_path, &manifest).await?;
                }

                let (relationships, postgres_indexes) = manifest
                    .storage
                    .create_relationships_and_indexes(
                        project_path,
                        &manifest.name,
                        &manifest.contracts,
                    )
                    .await?;

                let mut dependencies: Vec<ContractEventDependencies> =
                    ContractEventDependencies::parse(&manifest);

                let processed_network_contracts = start_indexing(
                    &manifest,
                    project_path,
                    &dependencies,
                    // we index all the historic data first before then applying FKs
                    !relationships.is_empty(),
                    indexing_details.registry.complete(),
                )
                .await?;

                // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we need
                // to handle this
                info!(
                    "Applying indexes if any back to the database as historic resync is complete"
                );
                PostgresIndexResult::apply_indexes(postgres_indexes).await?;

                if !relationships.is_empty() {
                    // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we
                    // need to handle this
                    info!("Applying constraints relationships back to the database as historic resync is complete");
                    Relationship::apply_all(&relationships).await?;

                    if manifest.has_any_contracts_live_indexing() {
                        info!("Starting live indexing now relationship re-applied..");

                        if dependencies.is_empty() {
                            dependencies =
                                ContractEventDependencies::map_from_relationships(&relationships)?;
                        } else {
                            info!("Manual dependency_events found, skipping auto-applying the dependency_events with the relationships");
                        }

                        start_indexing(
                            &manifest,
                            project_path,
                            &dependencies,
                            false,
                            indexing_details
                                .registry
                                .reapply_after_historic(processed_network_contracts),
                        )
                        .await
                        .map_err(StartRindexerError::CouldNotStartIndexing)?;
                    }
                }

                // Do not need now with the main shutdown keeping around in-case
                // if details.graphql_details.enabled {
                //     signal::ctrl_c()
                //         .await
                //         .map_err(|_| StartRindexerError::FailedToListenToGraphqlSocket)?;
                // }
            }

            // Await the GraphQL server task if it was started
            if let Some(handle) = graphql_server_handle {
                handle.await.unwrap_or_else(|e| {
                    error!("GraphQL server task failed: {:?}", e);
                });
            }

            shutdown_handle.await.map_err(|e| {
                error!("Shutdown handler failed: {:?}", e);
                StartRindexerError::ShutdownHandlerFailed(e.to_string())
            })?;

            Ok(())
        }
        None => Err(StartRindexerError::NoProjectPathFoundUsingParentOfManifestPath),
    }
}

pub struct IndexerNoCodeDetails {
    pub enabled: bool,
}

pub struct StartNoCodeDetails<'a> {
    pub manifest_path: &'a PathBuf,
    pub indexing_details: IndexerNoCodeDetails,
    pub graphql_details: GraphqlOverrideSettings,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerNoCode {
    #[error("{0}")]
    StartRindexerError(#[from] StartRindexerError),

    #[error("{0}")]
    SetupNoCodeError(#[from] SetupNoCodeError),
}

pub async fn start_rindexer_no_code(
    details: StartNoCodeDetails<'_>,
) -> Result<(), StartRindexerNoCode> {
    let start_details = setup_no_code(details).await?;

    start_rindexer(start_details).await.map_err(StartRindexerNoCode::StartRindexerError)
}
