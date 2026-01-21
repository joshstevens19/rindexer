use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use once_cell::sync::Lazy;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::indexer::task_tracker::active_indexing_count;

static IS_RUNNING: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(true));

/// Maximum time to wait for graceful shutdown before forcing exit
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn initiate_shutdown() {
    IS_RUNNING.store(false, Ordering::SeqCst);

    let mut active = active_indexing_count();
    let start = Instant::now();

    info!("Starting shutdown with {} active tasks", active);

    loop {
        if active == 0 {
            info!("All active indexing tasks finished shutting down system...");
            break;
        }

        if start.elapsed() > SHUTDOWN_TIMEOUT {
            warn!("Shutdown timeout reached with {} tasks still active - forcing exit", active);
            break;
        }

        // Only log every second instead of every 100ms to reduce spam
        if start.elapsed().as_millis() % 1000 < 100 {
            info!("{} active indexing tasks pending.. shutting them down gracefully", active);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        active = active_indexing_count();
    }

    info!("Shutdown complete");
}

pub fn is_running() -> bool {
    IS_RUNNING.load(Ordering::SeqCst)
}
