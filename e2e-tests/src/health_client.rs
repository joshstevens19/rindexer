use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info};

#[derive(Debug, Deserialize, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub timestamp: String,
    pub services: HealthServices,
    pub indexing: Option<IndexingStatus>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HealthServices {
    pub database: String,
    pub indexing: String,
    pub sync: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IndexingStatus {
    pub active_tasks: u32,
    pub is_running: bool,
}

pub struct HealthClient {
    client: Client,
    base_url: String,
}

impl HealthClient {
    pub fn new(port: u16) -> Self {
        Self { client: Client::new(), base_url: format!("http://localhost:{}", port) }
    }

    pub async fn get_health(&self) -> Result<HealthResponse> {
        let url = format!("{}/health", self.base_url);
        debug!("Checking health at: {}", url);

        let response = self
            .client
            .get(&url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .context("Failed to send health request")?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!("Health endpoint returned status: {}", response.status()));
        }

        let health: HealthResponse =
            response.json().await.context("Failed to parse health response")?;

        Ok(health)
    }

    /// Wait until the health endpoint responds with HTTP 200, regardless of body schema
    pub async fn wait_for_up(&self, timeout_seconds: u64) -> Result<()> {
        info!("Waiting for health endpoint HTTP 200 (timeout: {}s)", timeout_seconds);
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);
        let url = format!("{}/health", self.base_url);
        while start_time.elapsed() < timeout {
            let resp = self.client.get(&url).timeout(Duration::from_secs(3)).send().await;
            if let Ok(r) = resp {
                if r.status().is_success() {
                    info!("✓ Health endpoint is up (HTTP 200)");
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Err(anyhow::anyhow!("Health endpoint did not return HTTP 200 within {}s", timeout_seconds))
    }
}
