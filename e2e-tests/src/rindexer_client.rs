use anyhow::{Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub struct RindexerInstance {
    process: Option<tokio::process::Child>,
    pub project_path: PathBuf,
    pub binary_path: String,
    pub sync_completed: Arc<AtomicBool>,
    pub reorg_detected: Arc<AtomicBool>,
    pub reorg_recovery_complete: Arc<AtomicBool>,
    pub env: HashMap<String, String>,
    pub graphql_url: Arc<Mutex<Option<String>>>,
}

impl Clone for RindexerInstance {
    fn clone(&self) -> Self {
        Self {
            process: None,
            project_path: self.project_path.clone(),
            binary_path: self.binary_path.clone(),
            sync_completed: self.sync_completed.clone(),
            reorg_detected: self.reorg_detected.clone(),
            reorg_recovery_complete: self.reorg_recovery_complete.clone(),
            env: self.env.clone(),
            graphql_url: self.graphql_url.clone(),
        }
    }
}

impl RindexerInstance {
    pub fn new(binary_path: &str, project_path: PathBuf) -> Self {
        Self {
            process: None,
            project_path,
            binary_path: binary_path.to_string(),
            sync_completed: Arc::new(AtomicBool::new(false)),
            reorg_detected: Arc::new(AtomicBool::new(false)),
            reorg_recovery_complete: Arc::new(AtomicBool::new(false)),
            env: HashMap::new(),
            graphql_url: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_string(), value.to_string());
        self
    }

    fn resolve_binary_path(&self) -> Result<PathBuf> {
        let binary_path = if self.binary_path.starts_with("../") {
            let current_dir = std::env::current_dir()?;
            current_dir.join(&self.binary_path).canonicalize()?
        } else {
            PathBuf::from(&self.binary_path)
        };

        if !binary_path.exists() {
            return Err(anyhow::anyhow!("Rindexer binary not found at: {}", binary_path.display()));
        }
        Ok(binary_path)
    }

    pub async fn start_indexer(&mut self) -> Result<()> {
        info!("Starting rindexer indexer from: {:?}", self.project_path);

        let binary_path = self.resolve_binary_path()?;

        let mut cmd = TokioCommand::new(&binary_path);
        cmd.current_dir(&self.project_path)
            .arg("start")
            .arg("indexer")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !self.env.is_empty() {
            cmd.envs(self.env.clone());
        }

        let mut child = cmd.spawn().context("Failed to start rindexer indexer")?;

        Self::start_log_streaming_with_reorg_detection(
            &mut child,
            self.sync_completed.clone(),
            self.reorg_detected.clone(),
            self.reorg_recovery_complete.clone(),
            self.graphql_url.clone(),
        )
        .await;

        sleep(Duration::from_millis(500)).await;

        match child.try_wait()? {
            Some(status) => {
                if status.success() {
                    info!("Rindexer indexer completed immediately (no events to index)");
                } else {
                    return Err(anyhow::anyhow!(
                        "Rindexer indexer exited with error status: {}",
                        status
                    ));
                }
            }
            None => {
                info!("Rindexer indexer started (pid={})", child.id().unwrap_or(0));
            }
        }

        self.process = Some(child);
        Ok(())
    }

    pub async fn start_all(&mut self) -> Result<()> {
        info!("Starting rindexer all services from: {:?}", self.project_path);

        let binary_path = self.resolve_binary_path()?;

        let mut cmd = TokioCommand::new(&binary_path);
        cmd.current_dir(&self.project_path)
            .arg("start")
            .arg("all")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !self.env.is_empty() {
            cmd.envs(self.env.clone());
        }

        let mut child = cmd.spawn().context("Failed to start rindexer all services")?;

        Self::start_log_streaming_with_reorg_detection(
            &mut child,
            self.sync_completed.clone(),
            self.reorg_detected.clone(),
            self.reorg_recovery_complete.clone(),
            self.graphql_url.clone(),
        )
        .await;

        sleep(Duration::from_millis(1000)).await;

        match child.try_wait()? {
            Some(status) => {
                if status.success() {
                    info!("Rindexer all services completed immediately");
                } else {
                    return Err(anyhow::anyhow!(
                        "Rindexer all services exited with error status: {}",
                        status
                    ));
                }
            }
            None => {
                info!("Rindexer all services started (pid={})", child.id().unwrap_or(0));
            }
        }

        self.process = Some(child);
        Ok(())
    }

    /// Check if the rindexer process is still alive via `try_wait`.
    #[allow(dead_code)]
    pub fn is_running(&self) -> bool {
        // We need interior mutability to call try_wait, so we check the PID
        // is present. For a true liveness check, use `check_running()`.
        self.process.is_some()
    }

    /// Mutable check — actually probes the child process.
    #[allow(dead_code)]
    pub fn check_running(&mut self) -> bool {
        if let Some(ref mut child) = self.process {
            match child.try_wait() {
                Ok(None) => true, // still running
                Ok(Some(_)) => {
                    self.process = None;
                    false
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }

    pub async fn wait_for_initial_sync_completion(&mut self, timeout_seconds: u64) -> Result<()> {
        info!("Waiting for rindexer initial sync (timeout: {}s)", timeout_seconds);

        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);

        while start_time.elapsed() < timeout {
            if self.sync_completed.load(Ordering::Relaxed) {
                info!("Rindexer initial sync completed");
                return Ok(());
            }

            if let Some(process) = &mut self.process {
                if let Some(status) = process.try_wait()? {
                    return Err(anyhow::anyhow!("Rindexer process exited with status: {}", status));
                }
            }

            sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow::anyhow!("Timeout waiting for initial sync after {}s", timeout_seconds))
    }

    pub async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.process.take() {
            info!("Stopping rindexer instance");

            if let Err(e) = child.kill().await {
                warn!("Failed to kill rindexer process: {}", e);
            }

            let timeout = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;

            match timeout {
                Ok(Ok(status)) => {
                    info!("Rindexer terminated with status: {:?}", status);
                }
                Ok(Err(e)) => {
                    warn!("Error waiting for rindexer: {}", e);
                }
                Err(_) => {
                    warn!("Rindexer did not terminate within 5s, force killing");
                    if let Some(pid) = child.id() {
                        let _ = std::process::Command::new("kill")
                            .arg("-9")
                            .arg(pid.to_string())
                            .output();
                    }
                }
            }
        }
        Ok(())
    }

    async fn start_log_streaming_with_reorg_detection(
        child: &mut tokio::process::Child,
        sync_completed: Arc<AtomicBool>,
        reorg_detected: Arc<AtomicBool>,
        reorg_recovery_complete: Arc<AtomicBool>,
        graphql_url: Arc<Mutex<Option<String>>>,
    ) {
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            let sync_completed_clone = sync_completed.clone();
            let reorg_detected_clone = reorg_detected.clone();
            let reorg_recovery_clone = reorg_recovery_complete.clone();
            let graphql_url_clone = graphql_url.clone();
            let url_regex = Regex::new(r"https?://[^\s]+").ok();

            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    println!("{}", line);
                    debug!("[RINDEXER] {}", line);

                    if line.contains("COMPLETED - Finished indexing historic events")
                        || line.contains("100.00% progress")
                        || line.contains("Historical indexing complete")
                    {
                        info!("[RINDEXER] Detected sync completion: {}", line);
                        sync_completed_clone.store(true, Ordering::Relaxed);
                    }

                    // Reorg detection signals
                    if line.contains("REORG") {
                        info!("[RINDEXER] Reorg detected: {}", line);
                        reorg_detected_clone.store(true, Ordering::Relaxed);
                    }
                    if line.contains("Reorg recovery complete") {
                        info!("[RINDEXER] Reorg recovery complete: {}", line);
                        reorg_recovery_clone.store(true, Ordering::Relaxed);
                    }

                    if (line.contains("GraphQL")
                        || line.contains("graphql")
                        || line.contains("HTTP"))
                        && line.contains("http")
                    {
                        if let Some(re) = &url_regex {
                            if let Some(mat) = re.find(&line) {
                                let url = line[mat.start()..mat.end()].to_string();
                                let mut guard = graphql_url_clone.lock().unwrap();
                                if guard.is_none() {
                                    *guard = Some(url);
                                }
                            }
                        }
                    }
                }
            });
        }

        if let Some(stderr) = child.stderr.take() {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();

            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    eprintln!("{}", line);
                    error!("[RINDEXER ERROR] {}", line);
                }
            });
        }
    }

    pub async fn wait_for_graphql_url(&self, timeout_seconds: u64) -> Option<String> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);
        loop {
            if let Some(url) = self.get_graphql_url() {
                return Some(url);
            }
            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub fn get_graphql_url(&self) -> Option<String> {
        self.graphql_url.lock().ok().and_then(|g| g.clone())
    }

    /// Wait for reorg recovery to complete (detected from rindexer logs).
    pub async fn wait_for_reorg_recovery(&self, timeout_seconds: u64) -> Result<()> {
        info!("Waiting for reorg recovery (timeout: {}s)", timeout_seconds);
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);

        while start.elapsed() < timeout {
            if self.reorg_recovery_complete.load(Ordering::Relaxed) {
                info!("Reorg recovery completed");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        // Check if at least reorg was detected
        if self.reorg_detected.load(Ordering::Relaxed) {
            return Err(anyhow::anyhow!(
                "Reorg was detected but recovery did not complete within {}s",
                timeout_seconds
            ));
        }
        Err(anyhow::anyhow!("No reorg detected within {}s", timeout_seconds))
    }

    /// Reset reorg flags for a new reorg cycle.
    #[allow(dead_code)]
    pub fn reset_reorg_flags(&self) {
        self.reorg_detected.store(false, Ordering::Relaxed);
        self.reorg_recovery_complete.store(false, Ordering::Relaxed);
    }
}

/// Configuration creation utilities
impl RindexerInstance {
    pub fn create_minimal_config(
        anvil_rpc_url: &str,
        health_port: u16,
    ) -> crate::test_suite::RindexerConfig {
        crate::test_suite::RindexerConfig {
            name: "minimal_test".to_string(),
            project_type: "no-code".to_string(),
            config: serde_json::json!({}),
            timestamps: None,
            networks: vec![crate::test_suite::NetworkConfig {
                name: "anvil".to_string(),
                chain_id: 31337,
                rpc: anvil_rpc_url.to_string(),
            }],
            global: crate::test_suite::GlobalConfig { health_port },
            storage: crate::test_suite::StorageConfig {
                postgres: None,
                csv: crate::test_suite::CsvConfig { enabled: true },
                clickhouse: None,
            },
            native_transfers: crate::test_suite::NativeTransfersConfig { enabled: false },
            contracts: vec![],
        }
    }

    pub fn create_contract_config(
        anvil_rpc_url: &str,
        contract_address: &str,
        health_port: u16,
    ) -> crate::test_suite::RindexerConfig {
        let mut config = Self::create_minimal_config(anvil_rpc_url, health_port);
        config.name = "contract_test".to_string();
        config.contracts = vec![crate::test_suite::ContractConfig {
            name: "SimpleERC20".to_string(),
            details: vec![crate::test_suite::ContractDetail {
                network: "anvil".to_string(),
                address: contract_address.to_string(),
                start_block: "0".to_string(),
                end_block: None,
            }],
            abi: Some("./abis/SimpleERC20.abi.json".to_string()),
            reorg_safe_distance: None,
            include_events: Some(vec![crate::test_suite::EventConfig {
                name: "Transfer".to_string(),
            }]),
            tables: None,
        }];
        config
    }
}

impl Drop for RindexerInstance {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            info!("Shutting down rindexer instance");
            if let Some(pid) = child.id() {
                let _ = std::process::Command::new("kill").arg(pid.to_string()).output();
            }
            let _ = child.start_kill();
        }
    }
}
