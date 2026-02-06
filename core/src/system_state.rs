use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    time::Duration,
};

use once_cell::sync::Lazy;
use tracing::info;

use crate::indexer::task_tracker::active_indexing_count;

static IS_RUNNING: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

pub async fn initiate_shutdown() {
    IS_RUNNING.store(false, Ordering::SeqCst);
    let mut active = active_indexing_count();

    info!("Starting shutdown with {} active tasks", active);

    loop {
        if active == 0 {
            info!("All active indexing tasks finished shutting down system...");
            break;
        }

        info!("{} active indexing tasks pending.. shutting them down gracefully", active);
        tokio::time::sleep(Duration::from_millis(100)).await;
        active = active_indexing_count();
    }

    info!("Shutdown complete");
}

pub fn is_running() -> bool {
    IS_RUNNING.load(Ordering::SeqCst)
}

// --- Hot-reload state ---

/// Tracks the current reload state for the health endpoint and orchestrator.
#[derive(Debug, Clone)]
pub enum ReloadState {
    /// Normal operation.
    Running,
    /// A reload is in progress.
    Reloading,
    /// The last reload attempt failed. Contains the error message.
    ReloadFailed(String),
}

static RELOAD_STATE: Lazy<Mutex<ReloadState>> = Lazy::new(|| Mutex::new(ReloadState::Running));

pub fn set_reload_state(state: ReloadState) {
    if let Ok(mut s) = RELOAD_STATE.lock() {
        *s = state;
    }
}

pub fn get_reload_state() -> ReloadState {
    RELOAD_STATE.lock().map(|s| s.clone()).unwrap_or(ReloadState::Running)
}
