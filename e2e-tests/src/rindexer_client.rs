use anyhow::{Context, Result};
use regex::Regex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
pub struct RindexerInstance {
    pub process: Option<tokio::process::Child>,
    pub project_path: PathBuf,
    pub binary_path: String,
    pub sync_completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub env: HashMap<String, String>,
    pub graphql_url: Arc<Mutex<Option<String>>>,
}

impl Clone for RindexerInstance {
    fn clone(&self) -> Self {
        Self {
            process: None, // do not clone running process
            project_path: self.project_path.clone(),
            binary_path: self.binary_path.clone(),
            sync_completed: self.sync_completed.clone(),
            env: self.env.clone(),
            graphql_url: self.graphql_url.clone(),
        }
    }
}

impl RindexerInstance {
    /// Create a new Rindexer instance (doesn't start any services)
    pub fn new(binary_path: &str, project_path: PathBuf) -> Self {
        Self {
            process: None,
            project_path,
            binary_path: binary_path.to_string(),
            sync_completed: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            env: HashMap::new(),
            graphql_url: Arc::new(Mutex::new(None)),
        }
    }

    /// Provide environment variables to the rindexer process
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env.insert(key.to_string(), value.to_string());
        self
    }

    /// Start the indexer service
    pub async fn start_indexer(&mut self) -> Result<()> {
        info!("Starting Rindexer indexer service from project: {:?}", self.project_path);
        info!("Using Rindexer binary: {}", self.binary_path);

        // Check if binary exists and convert to absolute path
        let binary_path = if self.binary_path.starts_with("../") {
            // Convert relative path to absolute
            let current_dir = std::env::current_dir()?;
            current_dir.join(&self.binary_path).canonicalize()?
        } else {
            std::path::PathBuf::from(&self.binary_path)
        };

        if !binary_path.exists() {
            return Err(anyhow::anyhow!("Rindexer binary not found at: {}", binary_path.display()));
        }

        let mut cmd = TokioCommand::new(&binary_path);
        cmd.current_dir(&self.project_path)
            .arg("start")
            .arg("indexer")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !self.env.is_empty() {
            cmd.envs(self.env.clone());
        }

        info!("Executing command: {:?}", cmd);

        let mut child = cmd.spawn().context("Client: Failed to start Rindexer indexer")?;
        debug!("Client: Rindexer indexer process started");

        // Start log streaming for Rindexer with completion detection
        Self::start_log_streaming_with_completion_detection(
            &mut child,
            self.sync_completed.clone(),
            self.graphql_url.clone(),
        )
        .await;
        debug!("Client: Log streaming started");

        // Wait for Rindexer to start
        sleep(Duration::from_millis(500)).await;

        // Check if process is still running
        match child.try_wait()? {
            Some(status) => {
                if status.success() {
                    info!("Rindexer indexer completed successfully (likely no events to index)");
                } else {
                    // Try to read stderr to get more details
                    if let Some(mut stderr) = child.stderr.take() {
                        let mut stderr_output = String::new();
                        if (tokio::io::AsyncReadExt::read_to_string(
                            &mut stderr,
                            &mut stderr_output,
                        )
                        .await)
                            .is_ok()
                            && !stderr_output.is_empty()
                        {
                            error!("Rindexer stderr: {}", stderr_output);
                        }
                    }
                    return Err(anyhow::anyhow!(
                        "Rindexer indexer exited with error status: {}",
                        status
                    ));
                }
            }
            None => {
                info!("Rindexer indexer process started successfully and is still running");
            }
        }

        self.process = Some(child);
        Ok(())
    }

    /// Start both indexer and GraphQL services
    pub async fn start_all(&mut self) -> Result<()> {
        info!("Starting Rindexer all services from project: {:?}", self.project_path);

        // Resolve binary path like start_indexer
        let binary_path = if self.binary_path.starts_with("../") {
            let current_dir = std::env::current_dir()?;
            current_dir.join(&self.binary_path).canonicalize()?
        } else {
            std::path::PathBuf::from(&self.binary_path)
        };
        if !binary_path.exists() {
            return Err(anyhow::anyhow!("Rindexer binary not found at: {}", binary_path.display()));
        }

        let mut cmd = TokioCommand::new(&binary_path);
        cmd.current_dir(&self.project_path)
            .arg("start")
            .arg("all")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !self.env.is_empty() {
            cmd.envs(self.env.clone());
        }

        let mut child = cmd.spawn().context("Failed to start Rindexer all services")?;

        // Start log streaming for Rindexer with completion detection
        Self::start_log_streaming_with_completion_detection(
            &mut child,
            self.sync_completed.clone(),
            self.graphql_url.clone(),
        )
        .await;

        // Wait for Rindexer to start
        sleep(Duration::from_millis(1000)).await;

        // Check if process is still running
        match child.try_wait()? {
            Some(status) => {
                if status.success() {
                    info!("Rindexer all services completed successfully");
                } else {
                    return Err(anyhow::anyhow!(
                        "Rindexer all services exited with error status: {}",
                        status
                    ));
                }
            }
            None => {
                info!("Rindexer all services process started successfully and is still running");
            }
        }

        self.process = Some(child);
        Ok(())
    }

    /// Check if the Rindexer process is currently running
    pub fn is_running(&self) -> bool {
        if let Some(_process) = &self.process {
            // We can't call try_wait() here because it requires &mut
            // Just check if the process exists
            true
        } else {
            false
        }
    }

    /// Wait for initial sync completion (detected via logs)
    pub async fn wait_for_initial_sync_completion(&mut self, timeout_seconds: u64) -> Result<()> {
        info!("Waiting for Rindexer initial sync completion (timeout: {}s)", timeout_seconds);

        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(timeout_seconds);

        while start_time.elapsed() < timeout {
            // Check if sync is completed
            if self.sync_completed.load(std::sync::atomic::Ordering::Relaxed) {
                info!("✓ Rindexer initial sync completed (detected via logs)");
                return Ok(());
            }

            // Check if process is still running
            if let Some(process) = &mut self.process {
                if let Some(status) = process.try_wait()? {
                    return Err(anyhow::anyhow!("Rindexer process exited with status: {}", status));
                }
            }

            // Wait a bit for logs to accumulate
            sleep(Duration::from_millis(500)).await;
        }

        Err(anyhow::anyhow!(
            "Timeout waiting for initial sync completion after {}s",
            timeout_seconds
        ))
    }

    /// Stop the running Rindexer process
    pub async fn stop(&mut self) -> Result<()> {
        if let Some(mut child) = self.process.take() {
            info!("Stopping Rindexer instance");

            // First try graceful termination
            if let Err(e) = child.kill().await {
                warn!("Failed to kill Rindexer process: {}", e);
            }

            // Wait for process to terminate with timeout
            let timeout =
                tokio::time::timeout(std::time::Duration::from_secs(5), child.wait()).await;

            match timeout {
                Ok(Ok(status)) => {
                    info!("Rindexer process terminated with status: {:?}", status);
                }
                Ok(Err(e)) => {
                    warn!("Error waiting for Rindexer process: {}", e);
                }
                Err(_) => {
                    warn!("Rindexer process did not terminate within 5 seconds");
                    // Force kill if still running
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

    /// Start log streaming with completion detection for Rindexer processes
    async fn start_log_streaming_with_completion_detection(
        child: &mut tokio::process::Child,
        sync_completed: std::sync::Arc<std::sync::atomic::AtomicBool>,
        graphql_url: Arc<Mutex<Option<String>>>,
    ) {
        if let Some(stdout) = child.stdout.take() {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            let sync_completed_clone = sync_completed.clone();
            let graphql_url_clone = graphql_url.clone();
            let url_regex = Regex::new(r"https?://[^\s]+").ok();

            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    // Print the raw Rindexer output to terminal
                    println!("{}", line);

                    // Also log it for debugging
                    debug!("[RINDEXER] {}", line);

                    // Check for completion messages
                    if line.contains("COMPLETED - Finished indexing historic events")
                        || line.contains("100.00% progress")
                        || line.contains("Historical indexing complete")
                    {
                        info!("[RINDEXER] Detected sync completion: {}", line);
                        sync_completed_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                    }

                    // Capture GraphQL URL if present in logs
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
                    // Print stderr to terminal as well
                    eprintln!("{}", line);
                    error!("[RINDEXER ERROR] {}", line);
                }
            });
        }
    }

    /// Wait for GraphQL URL to be discovered from logs
    pub async fn wait_for_graphql_url(&self, timeout_seconds: u64) -> Option<String> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_seconds);
        loop {
            if let Some(url) = self.get_graphql_url() {
                return Some(url);
            }
            if start.elapsed() > timeout {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    pub fn get_graphql_url(&self) -> Option<String> {
        self.graphql_url.lock().ok().and_then(|g| g.clone())
    }
}

/// Configuration creation utilities
impl RindexerInstance {
    /// Create a minimal Rindexer configuration
    pub fn create_minimal_config(anvil_rpc_url: &str) -> crate::test_suite::RindexerConfig {
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
            storage: crate::test_suite::StorageConfig {
                postgres: crate::test_suite::PostgresConfig { enabled: false },
                csv: crate::test_suite::CsvConfig { enabled: true },
            },
            native_transfers: crate::test_suite::NativeTransfersConfig { enabled: false },
            contracts: vec![],
        }
    }

    /// Create a configuration with a specific contract
    pub fn create_contract_config(
        anvil_rpc_url: &str,
        contract_address: &str,
    ) -> crate::test_suite::RindexerConfig {
        let mut config = Self::create_minimal_config(anvil_rpc_url);
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
            include_events: Some(vec![crate::test_suite::EventConfig {
                name: "Transfer".to_string(),
            }]),
        }];
        config
    }
}

impl Drop for RindexerInstance {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            info!("Shutting down Rindexer instance");
            std::mem::drop(child.kill());

            // Force kill if we have a PID
            if let Some(pid) = child.id() {
                let _ = std::process::Command::new("kill").arg("-9").arg(pid.to_string()).output();
            }
        }
    }
}
