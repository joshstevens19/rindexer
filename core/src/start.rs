use std::path::PathBuf;
use tokio::signal;
use tracing::{error, info};

use crate::api::{start_graphql_server, GraphqlOverrideSettings, StartGraphqlServerError};
use crate::database::postgres::client::PostgresConnectionError;
use crate::database::postgres::indexes::{ApplyPostgresIndexesError, PostgresIndexResult};
use crate::database::postgres::relationship::{ApplyAllRelationships, Relationship};
use crate::database::postgres::setup::{setup_postgres, SetupPostgresError};
use crate::event::callback_registry::EventCallbackRegistry;
use crate::indexer::no_code::{setup_no_code, SetupNoCodeError};
use crate::indexer::start::{start_indexing, StartIndexingError};
use crate::indexer::{
    ContractEventDependencies, ContractEventDependenciesMapFromRelationshipsError,
};
use crate::manifest::core::ProjectType;
use crate::manifest::storage::RelationshipsAndIndexersError;
use crate::manifest::yaml::{read_manifest, ReadManifestError};
use crate::setup_info_logger;

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
}

pub struct StartDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_details: GraphqlOverrideSettings,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerError {
    #[error("Could not work out project path from the parent of the manifest")]
    NoProjectPathFoundUsingParentOfManifestPath,

    #[error("Could not read manifest: {0}")]
    CouldNotReadManifest(ReadManifestError),

    #[error("Could not start graphql error {0}")]
    CouldNotStartGraphqlServer(StartGraphqlServerError),

    #[error("Failed to listen to graphql socket")]
    FailedToListenToGraphqlSocket,

    #[error("Could not setup postgres: {0}")]
    SetupPostgresError(SetupPostgresError),

    #[error("Could not start indexing: {0}")]
    CouldNotStartIndexing(StartIndexingError),

    #[error("{0}")]
    PostgresConnectionError(PostgresConnectionError),

    #[error("{0}")]
    ApplyRelationshipError(ApplyAllRelationships),

    #[error("Could not apply indexes: {0}")]
    ApplyPostgresIndexesError(ApplyPostgresIndexesError),

    #[error("{0}")]
    ContractEventDependenciesMapFromRelationshipsError(
        ContractEventDependenciesMapFromRelationshipsError,
    ),

    #[error("{0}")]
    RelationshipsAndIndexersError(RelationshipsAndIndexersError),
}

pub async fn start_rindexer(details: StartDetails) -> Result<(), StartRindexerError> {
    let project_path = details.manifest_path.parent();
    match project_path {
        Some(project_path) => {
            let manifest = read_manifest(&details.manifest_path)
                .map_err(StartRindexerError::CouldNotReadManifest)?;

            if manifest.project_type != ProjectType::NoCode {
                setup_info_logger();
                info!("Starting rindexer rust project");
            }

            // Spawn a separate task for the GraphQL server if specified
            let graphql_server_handle = if details.graphql_details.enabled {
                let manifest_clone = manifest.clone();
                let indexer = manifest_clone.to_indexer();
                let mut graphql_settings = manifest_clone.graphql.unwrap_or_default();
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

            if let Some(mut indexing_details) = details.indexing_details {
                let postgres_enabled = &manifest.storage.postgres_enabled();

                // setup postgres is already called in no-code startup
                if manifest.project_type != ProjectType::NoCode && *postgres_enabled {
                    setup_postgres(project_path, &manifest)
                        .await
                        .map_err(StartRindexerError::SetupPostgresError)?;
                }

                let (relationships, postgres_indexes) = manifest
                    .storage
                    .create_relationships_and_indexes(
                        project_path,
                        &manifest.name,
                        &manifest.contracts,
                    )
                    .await
                    .map_err(StartRindexerError::RelationshipsAndIndexersError)?;

                let mut dependencies: Vec<ContractEventDependencies> =
                    ContractEventDependencies::parse(&manifest);

                let processed_network_contracts = start_indexing(
                    &manifest,
                    &project_path.to_path_buf(),
                    &dependencies,
                    // we index all the historic data first before then applying FKs
                    !relationships.is_empty(),
                    indexing_details.registry.complete(),
                )
                .await
                .map_err(StartRindexerError::CouldNotStartIndexing)?;

                // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we need to handle this
                info!(
                    "Applying indexes if any back to the database as historic resync is complete"
                );
                PostgresIndexResult::apply_indexes(postgres_indexes)
                    .await
                    .map_err(StartRindexerError::ApplyPostgresIndexesError)?;

                if !relationships.is_empty() {
                    // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we need to handle this
                    info!("Applying constraints relationships back to the database as historic resync is complete");
                    Relationship::apply_all(&relationships)
                        .await
                        .map_err(StartRindexerError::ApplyRelationshipError)?;

                    if manifest.has_any_contracts_live_indexing() {
                        info!("Starting live indexing now relationship re-applied..");

                        if dependencies.is_empty() {
                            dependencies = ContractEventDependencies::map_from_relationships(&relationships).map_err(StartRindexerError::ContractEventDependenciesMapFromRelationshipsError)?;
                        } else {
                            info!("Manual dependency_events found, skipping auto-applying the dependency_events with the relationships");
                        }

                        start_indexing(
                            &manifest,
                            &project_path.to_path_buf(),
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

                // keep graphql alive even if indexing has finished
                if details.graphql_details.enabled {
                    signal::ctrl_c()
                        .await
                        .map_err(|_| StartRindexerError::FailedToListenToGraphqlSocket)?;
                } else {
                    info!("rindexer resync is complete");
                    // to avoid the thread closing before the stream is consumed
                    // lets just sit here for 5 seconds to avoid the race
                    // 100% a better way to handle this
                    // TODO - handle this nicer
                    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                }
            }

            // Await the GraphQL server task if it was started
            if let Some(handle) = graphql_server_handle {
                handle.await.unwrap_or_else(|e| {
                    error!("GraphQL server task failed: {:?}", e);
                });
            }
        }
        None => {
            return Err(StartRindexerError::NoProjectPathFoundUsingParentOfManifestPath);
        }
    }

    Ok(())
}

pub struct IndexerNoCodeDetails {
    pub enabled: bool,
}

pub struct StartNoCodeDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: IndexerNoCodeDetails,
    pub graphql_details: GraphqlOverrideSettings,
}

#[derive(thiserror::Error, Debug)]
pub enum StartRindexerNoCode {
    #[error("{0}")]
    StartRindexerError(StartRindexerError),

    #[error("{0}")]
    SetupNoCodeError(SetupNoCodeError),
}

pub async fn start_rindexer_no_code(
    details: StartNoCodeDetails,
) -> Result<(), StartRindexerNoCode> {
    let start_details = setup_no_code(details)
        .await
        .map_err(StartRindexerNoCode::SetupNoCodeError)?;

    start_rindexer(start_details)
        .await
        .map_err(StartRindexerNoCode::StartRindexerError)
}
