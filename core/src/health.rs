#[cfg(feature = "postgres")]
use crate::database::postgres::client::PostgresClient;
use crate::{
    indexer::task_tracker::active_indexing_count,
    manifest::core::Manifest,
    metrics::metrics_handler,
    system_state::{get_reload_state, is_running, ReloadState},
};
use axum::{extract::State, http::StatusCode, response::Json, routing::get, Router};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
#[cfg(feature = "postgres")]
use tracing::error;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: HealthStatusType,
    pub timestamp: String,
    pub services: HealthServices,
    pub indexing: IndexingStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthServices {
    pub database: HealthStatusType,
    pub indexing: HealthStatusType,
    pub sync: HealthStatusType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingStatus {
    pub active_tasks: usize,
    pub is_running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatusType {
    Healthy,
    Unhealthy,
    Unknown,
    NotConfigured,
    Disabled,
    NoData,
    Stopped,
    Reloading,
}

#[derive(Clone)]
pub struct HealthServerState {
    pub manifest: Arc<Manifest>,
    #[cfg(feature = "postgres")]
    pub postgres_client: Option<Arc<PostgresClient>>,
}

pub struct HealthServer {
    port: u16,
    state: HealthServerState,
}

impl HealthServer {
    pub fn new(
        port: u16,
        manifest: Arc<Manifest>,
        #[cfg(feature = "postgres")] postgres_client: Option<Arc<PostgresClient>>,
    ) -> Self {
        Self {
            port,
            state: HealthServerState {
                manifest,
                #[cfg(feature = "postgres")]
                postgres_client,
            },
        }
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/metrics", get(metrics_handler))
            .with_state(self.state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr).await?;

        info!("🩺 Health server started on http://0.0.0.0:{}/health", self.port);
        info!("📊 Metrics available at http://0.0.0.0:{}/metrics", self.port);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_handler(
    State(state): State<HealthServerState>,
) -> Result<(StatusCode, Json<HealthStatus>), StatusCode> {
    let database_health = check_database_health(&state).await;
    let indexing_health = check_indexing_health();
    let sync_health = check_sync_health(&state).await;

    let overall_status = determine_overall_status(&database_health, &indexing_health, &sync_health);

    let health_status =
        build_health_status(overall_status, database_health, indexing_health, sync_health);

    let status_code = if matches!(health_status.status, HealthStatusType::Healthy) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((status_code, Json(health_status)))
}

fn build_health_status(
    overall_status: HealthStatusType,
    database_health: HealthStatusType,
    indexing_health: HealthStatusType,
    sync_health: HealthStatusType,
) -> HealthStatus {
    HealthStatus {
        status: overall_status,
        timestamp: chrono::Utc::now().to_rfc3339(),
        services: HealthServices {
            database: database_health,
            indexing: indexing_health,
            sync: sync_health,
        },
        indexing: IndexingStatus {
            active_tasks: active_indexing_count(),
            is_running: is_running(),
        },
    }
}

#[cfg(feature = "postgres")]
async fn check_database_health(state: &HealthServerState) -> HealthStatusType {
    if !state.manifest.storage.postgres_enabled() {
        return HealthStatusType::Disabled;
    }

    match &state.postgres_client {
        Some(client) => match client.query_one("SELECT 1", &[]).await {
            Ok(_) => HealthStatusType::Healthy,
            Err(e) => {
                error!("Database health check failed: {}", e);
                HealthStatusType::Unhealthy
            }
        },
        None => HealthStatusType::NotConfigured,
    }
}

#[cfg(not(feature = "postgres"))]
async fn check_database_health(_state: &HealthServerState) -> HealthStatusType {
    HealthStatusType::Disabled
}

fn check_indexing_health() -> HealthStatusType {
    if matches!(get_reload_state(), ReloadState::Reloading) {
        return HealthStatusType::Reloading;
    }
    if is_running() {
        HealthStatusType::Healthy
    } else {
        HealthStatusType::Stopped
    }
}

async fn check_sync_health(state: &HealthServerState) -> HealthStatusType {
    #[cfg(feature = "postgres")]
    if state.manifest.storage.postgres_enabled() {
        return check_postgres_sync_health(state).await;
    }
    if state.manifest.storage.csv_enabled() {
        check_csv_sync_health(state)
    } else {
        HealthStatusType::Disabled
    }
}

#[cfg(feature = "postgres")]
async fn check_postgres_sync_health(state: &HealthServerState) -> HealthStatusType {
    match &state.postgres_client {
        Some(client) => {
            match client.query_one_or_none(
                r#"SELECT 1 FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'rindexer_internal') AND table_name NOT LIKE 'latest_block' AND table_name NOT LIKE '%_last_known_%' AND table_name NOT LIKE '%_last_run_%' LIMIT 1"#,
                &[]
            ).await {
                Ok(Some(_)) => HealthStatusType::Healthy,
                Ok(None) => HealthStatusType::NoData,
                Err(e) => {
                    error!("Sync health check failed: {}", e);
                    HealthStatusType::Unhealthy
                }
            }
        }
        None => HealthStatusType::NotConfigured,
    }
}

fn check_csv_sync_health(state: &HealthServerState) -> HealthStatusType {
    match &state.manifest.storage.csv {
        Some(csv_details) => {
            let csv_path = std::path::Path::new(&csv_details.path);
            if !csv_path.exists() {
                return HealthStatusType::NoData;
            }

            match std::fs::read_dir(csv_path) {
                Ok(entries) => {
                    let csv_files: Vec<_> = entries
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "csv"))
                        .collect();

                    if csv_files.is_empty() {
                        HealthStatusType::NoData
                    } else {
                        HealthStatusType::Healthy
                    }
                }
                Err(_) => HealthStatusType::Unhealthy,
            }
        }
        None => HealthStatusType::NotConfigured,
    }
}

fn determine_overall_status(
    database: &HealthStatusType,
    indexing: &HealthStatusType,
    sync: &HealthStatusType,
) -> HealthStatusType {
    if matches!(database, HealthStatusType::Unhealthy | HealthStatusType::NotConfigured)
        || matches!(indexing, HealthStatusType::Stopped)
        || matches!(sync, HealthStatusType::Unhealthy | HealthStatusType::NotConfigured)
    {
        HealthStatusType::Unhealthy
    } else if matches!(sync, HealthStatusType::NoData) {
        // Sync NoData is acceptable when no event tables exist yet
        HealthStatusType::Healthy
    } else {
        HealthStatusType::Healthy
    }
}

pub async fn start_health_server(
    port: u16,
    manifest: Arc<Manifest>,
    #[cfg(feature = "postgres")] postgres_client: Option<Arc<PostgresClient>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let health_server = HealthServer::new(
        port,
        manifest,
        #[cfg(feature = "postgres")]
        postgres_client,
    );
    health_server.start().await
}
