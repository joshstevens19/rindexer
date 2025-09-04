use std::{
    net::SocketAddr,
    sync::Arc,
};

use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::{
    database::postgres::client::PostgresClient,
    indexer::task_tracker::active_indexing_count,
    manifest::core::Manifest,
    system_state::is_running,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub timestamp: String,
    pub services: HealthServices,
    pub indexing: IndexingStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthServices {
    pub database: String,
    pub indexing: String,
    pub sync: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexingStatus {
    pub active_tasks: usize,
    pub is_running: bool,
}

#[derive(Clone)]
pub struct HealthServerState {
    pub manifest: Arc<Manifest>,
    pub postgres_client: Option<Arc<PostgresClient>>,
}

pub struct HealthServer {
    port: u16,
    state: HealthServerState,
}

impl HealthServer {
    pub fn new(port: u16, manifest: Arc<Manifest>, postgres_client: Option<Arc<PostgresClient>>) -> Self {
        Self {
            port,
            state: HealthServerState {
                manifest,
                postgres_client,
            },
        }
    }

    pub async fn start(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let app = Router::new()
            .route("/health", get(health_handler))
            .with_state(self.state);

        let addr = SocketAddr::from(([0, 0, 0, 0], self.port));
        let listener = TcpListener::bind(addr).await?;

        info!("🩺 Health server started on http://0.0.0.0:{}/health", self.port);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn health_handler(State(state): State<HealthServerState>) -> Result<Json<HealthStatus>, StatusCode> {
    let mut health_status = HealthStatus {
        status: "healthy".to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        services: HealthServices {
            database: "unknown".to_string(),
            indexing: "unknown".to_string(),
            sync: "unknown".to_string(),
        },
        indexing: IndexingStatus {
            active_tasks: active_indexing_count(),
            is_running: is_running(),
        },
    };

    // Check database connection if PostgreSQL is enabled
    if state.manifest.storage.postgres_enabled() {
        match &state.postgres_client {
            Some(client) => {
                match client.query_one("SELECT 1", &[]).await {
                    Ok(_) => {
                        health_status.services.database = "healthy".to_string();
                    }
                    Err(e) => {
                        health_status.services.database = "unhealthy".to_string();
                        health_status.status = "unhealthy".to_string();
                        error!("Database health check failed: {}", e);
                    }
                }
            }
            None => {
                health_status.services.database = "not_configured".to_string();
                health_status.status = "unhealthy".to_string();
            }
        }
    } else {
        health_status.services.database = "disabled".to_string();
    }

    // Check indexing status
    if health_status.indexing.is_running {
        health_status.services.indexing = "healthy".to_string();
    } else {
        health_status.services.indexing = "stopped".to_string();
        health_status.status = "unhealthy".to_string();
    }

    // Check sync status (basic check for event tables if using PostgreSQL)
    if state.manifest.storage.postgres_enabled() {
        match &state.postgres_client {
            Some(client) => {
                match client.query_one(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE '%_events'",
                    &[]
                ).await {
                    Ok(row) => {
                        let count: i64 = row.get(0);
                        if count > 0 {
                            health_status.services.sync = "healthy".to_string();
                        } else {
                            health_status.services.sync = "no_data".to_string();
                        }
                    }
                    Err(e) => {
                        health_status.services.sync = "unhealthy".to_string();
                        health_status.status = "unhealthy".to_string();
                        error!("Sync health check failed: {}", e);
                    }
                }
            }
            None => {
                health_status.services.sync = "not_configured".to_string();
            }
        }
    } else {
        // For CSV storage, check if the output directory exists and has files
        if state.manifest.storage.csv_enabled() {
            match &state.manifest.storage.csv {
                Some(csv_details) => {
                    let csv_path = std::path::Path::new(&csv_details.path);
                    if csv_path.exists() {
                        // Check if directory has CSV files
                        match std::fs::read_dir(csv_path) {
                            Ok(entries) => {
                                let csv_files: Vec<_> = entries
                                    .filter_map(|entry| entry.ok())
                                    .filter(|entry| {
                                        entry.path().extension()
                                            .map_or(false, |ext| ext == "csv")
                                    })
                                    .collect();
                                
                                if !csv_files.is_empty() {
                                    health_status.services.sync = "healthy".to_string();
                                } else {
                                    health_status.services.sync = "no_data".to_string();
                                }
                            }
                            Err(_) => {
                                health_status.services.sync = "unhealthy".to_string();
                                health_status.status = "unhealthy".to_string();
                            }
                        }
                    } else {
                        health_status.services.sync = "no_data".to_string();
                    }
                }
                None => {
                    health_status.services.sync = "not_configured".to_string();
                }
            }
        } else {
            health_status.services.sync = "disabled".to_string();
        }
    }

    let _status_code = if health_status.status == "healthy" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok(Json(health_status))
}

pub async fn start_health_server(
    port: u16,
    manifest: Arc<Manifest>,
    postgres_client: Option<Arc<PostgresClient>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let health_server = HealthServer::new(port, manifest, postgres_client);
    health_server.start().await
}
