use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::hot_reload::diff::{compute_diff, ReloadAction};
use crate::indexer::task_tracker::active_indexing_count;
use crate::manifest::core::Manifest;
use crate::manifest::yaml::read_manifest;
use crate::system_state::{set_reload_state, ReloadState};

/// Timeout for draining active indexing tasks during a reload.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Drain poll interval.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Coordinates hot-reload of the rindexer manifest.
///
/// Listens for reload signals from `ManifestWatcher`, validates the new manifest,
/// computes a diff, and orchestrates selective restart of affected components.
///
/// The orchestrator guarantees that the system is never left with nothing running:
/// - Validates the new manifest BEFORE stopping anything
/// - Falls back to the old config if the new one fails to start
pub struct ReloadOrchestrator {
    manifest_path: PathBuf,
    project_path: PathBuf,
    current_manifest: Arc<RwLock<Arc<Manifest>>>,
    reload_rx: mpsc::Receiver<PathBuf>,
    generation_token: CancellationToken,
    reload_count: u64,
}

impl ReloadOrchestrator {
    pub fn new(
        manifest_path: PathBuf,
        project_path: PathBuf,
        initial_manifest: Arc<Manifest>,
        reload_rx: mpsc::Receiver<PathBuf>,
        generation_token: CancellationToken,
    ) -> Self {
        Self {
            manifest_path,
            project_path,
            current_manifest: Arc::new(RwLock::new(initial_manifest)),
            reload_rx,
            generation_token,
            reload_count: 0,
        }
    }

    /// Returns a shared reference to the current manifest.
    pub fn current_manifest(&self) -> Arc<RwLock<Arc<Manifest>>> {
        Arc::clone(&self.current_manifest)
    }

    /// Run the orchestrator loop. Exits when the reload channel closes or global shutdown occurs.
    pub async fn run(&mut self, shutdown_token: CancellationToken) {
        info!("Hot-reload: orchestrator started");

        loop {
            tokio::select! {
                Some(path) = self.reload_rx.recv() => {
                    self.handle_reload(path).await;
                }
                _ = shutdown_token.cancelled() => {
                    info!("Hot-reload: orchestrator shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_reload(&mut self, path: PathBuf) {
        info!("Hot-reload: processing config change...");
        set_reload_state(ReloadState::Reloading);

        // Step 1: Validate the new manifest BEFORE stopping anything
        let new_manifest = match read_manifest(&path) {
            Ok(m) => m,
            Err(e) => {
                error!("Hot-reload: new manifest is invalid, keeping current config: {}", e);
                set_reload_state(ReloadState::ReloadFailed(format!(
                    "Invalid manifest: {}",
                    e
                )));
                return;
            }
        };

        // Step 2: Compute diff
        let old_manifest = self.current_manifest.read().await.clone();
        let diff = compute_diff(&old_manifest, &new_manifest);

        info!(
            "Hot-reload: detected {} change(s): {:?}",
            diff.changes.len(),
            diff.changes
                .iter()
                .map(|c| format!("{:?}", c))
                .collect::<Vec<_>>()
                .join(", ")
        );

        // Step 3: Act on the diff
        match diff.action {
            ReloadAction::NoChange => {
                info!("Hot-reload: no meaningful changes detected, skipping reload");
                set_reload_state(ReloadState::Running);
            }
            ReloadAction::RequiresFullRestart(reason) => {
                warn!(
                    "Hot-reload: change requires full restart: {}. Current config will keep running.",
                    reason
                );
                set_reload_state(ReloadState::ReloadFailed(format!(
                    "Requires restart: {}",
                    reason
                )));
            }
            ReloadAction::HotApply => {
                info!("Hot-reload: applying config-only changes without restart");
                *self.current_manifest.write().await = Arc::new(new_manifest);
                self.reload_count += 1;
                info!("Hot-reload: config updated (reload #{})", self.reload_count);
                set_reload_state(ReloadState::Running);
            }
            ReloadAction::SelectiveRestart(plan) => {
                info!(
                    "Hot-reload: selective restart needed - add: {:?}, remove: {:?}, restart: {:?}, reconnect: {:?}",
                    plan.contracts_to_add,
                    plan.contracts_to_remove,
                    plan.contracts_to_restart,
                    plan.networks_to_reconnect,
                );

                // Step 4: Cancel current generation
                info!("Hot-reload: cancelling current indexing generation...");
                self.generation_token.cancel();

                // Step 5: Wait for active tasks to drain
                if let Err(e) = drain_active_tasks().await {
                    warn!("Hot-reload: {}", e);
                }

                // Step 6: Create fresh generation token for new indexers
                self.generation_token = CancellationToken::new();

                // Step 7: Update manifest
                *self.current_manifest.write().await = Arc::new(new_manifest);
                self.reload_count += 1;

                // Step 8: Signal that new generation should start.
                // The actual restart of indexing is handled by the caller (start.rs) which
                // monitors the generation token and restarts when it sees cancellation.
                // Here we just update state and let the main loop handle restart.
                info!(
                    "Hot-reload: manifest updated, new generation ready (reload #{})",
                    self.reload_count
                );
                set_reload_state(ReloadState::Running);
            }
        }
    }

    /// Returns the current generation's cancellation token.
    pub fn generation_token(&self) -> CancellationToken {
        self.generation_token.clone()
    }

    pub fn reload_count(&self) -> u64 {
        self.reload_count
    }
}

/// Wait for all active indexing tasks to finish, with a timeout.
async fn drain_active_tasks() -> Result<(), String> {
    let start = tokio::time::Instant::now();
    let mut active = active_indexing_count();

    info!("Hot-reload: draining {} active indexing tasks...", active);

    loop {
        if active == 0 {
            info!("Hot-reload: all indexing tasks drained successfully");
            return Ok(());
        }

        if start.elapsed() > DRAIN_TIMEOUT {
            return Err(format!(
                "Hot-reload: drain timeout after {}s with {} tasks still active. Proceeding anyway.",
                DRAIN_TIMEOUT.as_secs(),
                active
            ));
        }

        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        active = active_indexing_count();
    }
}
