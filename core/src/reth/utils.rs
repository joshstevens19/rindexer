use std::time::{Duration, Instant};

use crate::provider::RetryClientError;

/// Get the default reth IPC path for the current platform
pub fn get_default_reth_ipc_path() -> String {
    #[cfg(unix)]
    {
        "/tmp/reth.ipc".to_string()
    }

    #[cfg(windows)]
    {
        r"\\.\pipe\reth.ipc".to_string()
    }
}

/// Get the IPC path from reth CLI args
pub fn get_reth_ipc_path(cli: &reth::cli::Cli) -> Option<String> {
    use reth::cli::Commands;

    match &cli.command {
        Commands::Node(node_cmd) => {
            if node_cmd.rpc.ipcdisable {
                None
            } else {
                Some(node_cmd.rpc.ipcpath.clone())
            }
        }
        _ => None,
    }
}

/// Wait for IPC to be ready with retry logic
pub async fn wait_for_ipc_ready(ipc_path: &str, timeout: Duration) -> Result<(), RetryClientError> {
    let start = Instant::now();

    while start.elapsed() < timeout {
        #[cfg(unix)]
        if std::path::Path::new(ipc_path).exists() {
            // Socket file exists, assume it's ready
            return Ok(());
        }

        #[cfg(windows)]
        {
            // On Windows, we can't easily check if the named pipe exists
            // Just wait a bit and assume it's ready after some time
            if start.elapsed() > Duration::from_secs(2) {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(RetryClientError::HttpProviderCantBeCreated(
        ipc_path.to_string(),
        "IPC connection timeout".to_string(),
    ))
}
