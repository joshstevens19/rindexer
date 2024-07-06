use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tracing::{error, info};

use crate::api::{start_graphql_server, StartGraphqlServerError};
use crate::database::postgres::{
    create_relationships, CreateRelationshipError, PostgresConnectionError, PostgresError,
    Relationship, SetupPostgresError,
};
use crate::generator::event_callback_registry::EventCallbackRegistry;
use crate::indexer::no_code::{setup_no_code, SetupNoCodeError};
use crate::indexer::start::{start_indexing, StartIndexingError};
use crate::indexer::{ContractEventDependencies, EventDependencies, EventsDependencyTree};
use crate::manifest::yaml::{read_manifest, ProjectType, ReadManifestError};
use crate::{setup_info_logger, setup_postgres, GraphQLServerDetails, PostgresClient};

pub struct IndexingDetails {
    pub registry: EventCallbackRegistry,
}

pub struct StartDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: Option<IndexingDetails>,
    pub graphql_server: Option<GraphQLServerDetails>,
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

    #[error("Yaml relationship error: {0}")]
    RelationshipError(CreateRelationshipError),

    #[error("{0}")]
    RelationshipPostgresConnectionError(PostgresConnectionError),

    #[error("Could not apply relationship - {0}")]
    ApplyRelationshipError(PostgresError),
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
            let graphql_server_handle = if let Some(graphql_server) = &details.graphql_server {
                let manifest_clone = manifest.clone();
                let graphql_settings = graphql_server.settings.clone();
                Some(tokio::spawn(async move {
                    if let Err(e) =
                        start_graphql_server(&manifest_clone.to_indexer(), &graphql_settings).await
                    {
                        error!("Failed to start GraphQL server: {:?}", e);
                    }
                }))
            } else {
                None
            };

            if let Some(indexing_details) = details.indexing_details {
                let postgres_enabled = &manifest.storage.postgres_enabled();

                // setup postgres is already called in no-code startup
                if manifest.project_type != ProjectType::NoCode && *postgres_enabled {
                    setup_postgres(project_path, &manifest)
                        .await
                        .map_err(StartRindexerError::SetupPostgresError)?;
                }

                // setup relationships
                let mut relationships: Vec<Relationship> = vec![];
                if *postgres_enabled && !manifest.storage.postgres_disable_create_tables() {
                    if let Some(storage) = &manifest.storage.postgres {
                        let mapped_relationships = &storage.relationships;
                        if let Some(mapped_relationships) = mapped_relationships {
                            info!("Temp dropping constraints relationships from the database for historic indexing for speed reasons");
                            let relationships_result = create_relationships(
                                project_path,
                                &manifest.name,
                                &manifest.contracts,
                                mapped_relationships,
                            )
                            .await;
                            match relationships_result {
                                Ok(result) => {
                                    // info!("Relationships created successfully - {:?}", result);
                                    relationships = result;
                                }
                                Err(e) => {
                                    return Err(StartRindexerError::RelationshipError(e));
                                }
                            }
                        }
                    }
                }

                let mut dependencies: Vec<ContractEventDependencies> = vec![];
                for contract in &manifest.contracts {
                    if let Some(dependency) = contract.dependency_events.clone() {
                        let dependency_event_names = dependency.collect_dependency_events();
                        let dependency_tree =
                            EventsDependencyTree::from_dependency_event_tree(dependency);
                        dependencies.push(ContractEventDependencies {
                            contract_name: contract.name.clone(),
                            event_dependencies: EventDependencies {
                                tree: Arc::new(dependency_tree),
                                dependency_event_names,
                            },
                        });
                    }
                }

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

                if !relationships.is_empty() {
                    // TODO if graphql isn't up yet, and we apply this on graphql wont refresh we need to handle this
                    info!("Applying constraints relationships back to the database as historic resync is complete");
                    let client = PostgresClient::new()
                        .await
                        .map_err(StartRindexerError::RelationshipPostgresConnectionError)?;

                    for relationship in relationships {
                        relationship
                            .apply(&client)
                            .await
                            .map_err(StartRindexerError::ApplyRelationshipError)?;
                    }

                    let live_indexing_present = manifest
                        .contracts
                        .iter()
                        .filter(|c| c.details.iter().any(|p| p.end_block.is_none()))
                        .count()
                        > 0;

                    if live_indexing_present {
                        info!("Starting live indexing now relationship re-applied..");

                        let mut callbacks = indexing_details.registry.clone();
                        callbacks.events.iter_mut().for_each(|e| {
                            e.contract.details.iter_mut().for_each(|d| {
                                if d.end_block.is_none() {
                                    if let Some(processed_block) =
                                        processed_network_contracts.iter().find(|c| c.id == d.id)
                                    {
                                        d.start_block = Some(processed_block.processed_up_to);
                                    }
                                }
                            });
                        });

                        // Retain only the details with `end_block.is_none()`
                        callbacks.events.iter_mut().for_each(|e| {
                            e.contract.details.retain(|d| d.end_block.is_none());
                        });

                        // Retain only the events that have details with `end_block.is_none()`
                        callbacks.events.retain(|e| !e.contract.details.is_empty());

                        start_indexing(
                            &manifest,
                            &project_path.to_path_buf(),
                            &dependencies,
                            false,
                            callbacks.complete(),
                        )
                        .await
                        .map_err(StartRindexerError::CouldNotStartIndexing)?;
                    }
                }

                // keep graphql alive even if indexing has finished
                if details.graphql_server.is_some() {
                    signal::ctrl_c()
                        .await
                        .map_err(|_| StartRindexerError::FailedToListenToGraphqlSocket)?;
                } else {
                    // to avoid the thread closing before the stream is consumed
                    // lets just sit here for 30 seconds to avoid the race
                    // probably a better way to handle this but hey
                    // TODO - handle this nicer
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
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

pub struct GraphqlNoCodeDetails {
    pub enabled: bool,
    pub settings: Option<GraphQLServerDetails>,
}

pub struct StartNoCodeDetails {
    pub manifest_path: PathBuf,
    pub indexing_details: IndexerNoCodeDetails,
    pub graphql_details: GraphqlNoCodeDetails,
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
