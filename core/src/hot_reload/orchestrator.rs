use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::api::stop_graphql_server;
use crate::hot_reload::diff::{compute_diff, ReloadAction};
use crate::logger::mark_shutdown_started;
use crate::manifest::core::Manifest;
use crate::manifest::yaml::read_manifest;
use crate::system_state::{initiate_shutdown, set_reload_state, ReloadState};

/// Exit code used to signal the process manager that a restart is needed.
/// 75 = EX_TEMPFAIL (sysexits.h), conventionally means "try again later".
pub const RELOAD_EXIT_CODE: i32 = 75;

/// Coordinates hot-reload of the rindexer manifest.
///
/// Listens for reload signals from `ManifestWatcher`, validates the new manifest,
/// computes a diff, and triggers a graceful process restart when changes are detected.
///
/// The orchestrator validates the new YAML before taking any action — invalid configs
/// are rejected and the current process keeps running.
pub struct ReloadOrchestrator {
    manifest_path: PathBuf,
    current_manifest: Arc<RwLock<Arc<Manifest>>>,
    reload_rx: mpsc::Receiver<PathBuf>,
}

impl ReloadOrchestrator {
    pub fn new(
        manifest_path: PathBuf,
        _project_path: PathBuf,
        initial_manifest: Arc<Manifest>,
        reload_rx: mpsc::Receiver<PathBuf>,
        _generation_token: CancellationToken,
    ) -> Self {
        Self {
            manifest_path,
            current_manifest: Arc::new(RwLock::new(initial_manifest)),
            reload_rx,
        }
    }

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

    async fn handle_reload(&mut self, _path: PathBuf) {
        info!("Hot-reload: processing config change...");
        set_reload_state(ReloadState::Reloading);

        // Step 1: Validate the new manifest BEFORE doing anything
        let new_manifest = match read_manifest(&self.manifest_path) {
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
            ReloadAction::HotApply | ReloadAction::SelectiveRestart(_) => {
                info!("Hot-reload: valid config change detected, restarting process...");
                set_reload_state(ReloadState::Reloading);

                // Graceful shutdown: release ports and stop active tasks before exiting
                mark_shutdown_started();
                stop_graphql_server();
                initiate_shutdown().await;

                info!(
                    "Hot-reload: exiting with code {} for process manager to restart",
                    RELOAD_EXIT_CODE
                );
                std::process::exit(RELOAD_EXIT_CODE);
            }
        }
    }
}
