use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::time::Duration;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Debounce window: after detecting a file change, wait this long before triggering reload.
/// This prevents rapid successive saves from causing multiple reloads.
const DEBOUNCE_DURATION: Duration = Duration::from_millis(500);

/// Watches `rindexer.yaml` for modifications and sends reload signals via an mpsc channel.
///
/// Uses the `notify` crate for cross-platform file watching with custom debouncing
/// to handle rapid successive saves (e.g., editor auto-save).
pub struct ManifestWatcher {
    manifest_path: PathBuf,
    reload_tx: mpsc::Sender<PathBuf>,
}

impl ManifestWatcher {
    pub fn new(manifest_path: PathBuf, reload_tx: mpsc::Sender<PathBuf>) -> Self {
        Self { manifest_path, reload_tx }
    }

    /// Starts watching the manifest file for changes.
    ///
    /// This function runs indefinitely. It spawns a blocking thread for the `notify` watcher
    /// and a tokio task for debouncing. Cancel the parent task to stop watching.
    pub async fn run(self) -> Result<(), ManifestWatchError> {
        let watch_path = self
            .manifest_path
            .parent()
            .ok_or(ManifestWatchError::NoParentDirectory)?
            .to_path_buf();

        let manifest_filename = self
            .manifest_path
            .file_name()
            .ok_or(ManifestWatchError::NoFileName)?
            .to_os_string();

        // Channel from the notify watcher (sync) to our async debounce loop
        let (notify_tx, mut notify_rx) = mpsc::channel::<()>(16);

        // Spawn the notify watcher on a blocking thread since it uses sync callbacks
        let watch_path_clone = watch_path.clone();
        let manifest_filename_clone = manifest_filename.clone();
        let _watcher_handle = tokio::task::spawn_blocking(move || {
            let notify_tx = notify_tx;
            let (std_tx, std_rx) = std_mpsc::channel();

            let watcher = RecommendedWatcher::new(
                move |result: Result<Event, notify::Error>| {
                    let _ = std_tx.send(result);
                },
                notify::Config::default(),
            )
            .map_err(|e| {
                error!("Hot-reload: failed to create file watcher: {}", e);
            });

            let Ok(mut watcher) = watcher else {
                return;
            };

            if let Err(e) = watcher.watch(&watch_path_clone, RecursiveMode::NonRecursive) {
                error!("Hot-reload: failed to watch directory {:?}: {}", watch_path_clone, e);
                return;
            }

            info!("Hot-reload: watching {:?} for changes", watch_path_clone);

            // Forward relevant events to the async channel
            loop {
                match std_rx.recv() {
                    Ok(Ok(event)) => {
                        let is_relevant = matches!(
                            event.kind,
                            EventKind::Modify(_) | EventKind::Create(_)
                        ) && event.paths.iter().any(|p| {
                            p.file_name()
                                .map(|f| f == manifest_filename_clone)
                                .unwrap_or(false)
                        });

                        if is_relevant {
                            if notify_tx.blocking_send(()).is_err() {
                                // Receiver dropped, stop watching
                                break;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!("Hot-reload: file watcher error: {}", e);
                    }
                    Err(_) => {
                        // Sender dropped (watcher stopped)
                        break;
                    }
                }
            }
        });

        // Debounce loop: collect events and only trigger reload after DEBOUNCE_DURATION of quiet
        info!("Hot-reload: debounce loop started for {:?}", self.manifest_path);

        loop {
            // Wait for the first change notification
            if notify_rx.recv().await.is_none() {
                // Watcher stopped
                warn!("Hot-reload: file watcher channel closed");
                return Err(ManifestWatchError::WatcherStopped);
            }

            // Start debounce window: drain all events that arrive within DEBOUNCE_DURATION
            loop {
                match tokio::time::timeout(DEBOUNCE_DURATION, notify_rx.recv()).await {
                    Ok(Some(())) => {
                        // More events during debounce window, reset timer by continuing loop
                        continue;
                    }
                    Ok(None) => {
                        // Channel closed
                        return Err(ManifestWatchError::WatcherStopped);
                    }
                    Err(_) => {
                        // Timeout expired with no new events -- debounce complete
                        break;
                    }
                }
            }

            info!("Hot-reload: detected change to {:?}, triggering reload", self.manifest_path);

            if let Err(e) = self.reload_tx.send(self.manifest_path.clone()).await {
                error!("Hot-reload: failed to send reload signal: {}", e);
                return Err(ManifestWatchError::ReloadChannelClosed);
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ManifestWatchError {
    #[error("Manifest path has no parent directory")]
    NoParentDirectory,

    #[error("Manifest path has no file name")]
    NoFileName,

    #[error("File watcher stopped unexpectedly")]
    WatcherStopped,

    #[error("Reload signal channel closed")]
    ReloadChannelClosed,

    #[error("Notify watcher error: {0}")]
    NotifyError(#[from] notify::Error),
}
