//! Pruned RPC detection for rindexer.
//!
//! This module provides utilities to detect when an RPC endpoint is pruned
//! (unable to serve historical data) and warn users before indexing begins.

use alloy::primitives::U64;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};

use crate::provider::{ChainProvider, ProviderError};

/// Environment variable to skip prune detection warnings.
pub const IGNORE_PRUNE_WARNINGS_ENV: &str = "RINDEXER_IGNORE_PRUNE_WARNINGS";

/// Result of checking if an RPC node is pruned.
#[derive(Debug, Clone)]
pub enum PruneCheckResult {
    /// RPC can serve historical data from the requested block.
    Available { client_version: Option<String> },
    /// RPC appears to be pruned and cannot serve historical data.
    Pruned { client_version: Option<String>, reason: PruneDetectionReason },
    /// Could not definitively determine if RPC is pruned.
    Inconclusive { client_version: Option<String>, reason: String },
}

/// Reason why an RPC was detected as pruned.
#[derive(Debug, Clone)]
pub enum PruneDetectionReason {
    /// eth_getLogs returned an error indicating pruned state.
    GetLogsError(String),
    /// Transaction receipt was missing for a known transaction.
    ReceiptMissing,
}

impl std::fmt::Display for PruneDetectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PruneDetectionReason::GetLogsError(msg) => write!(f, "eth_getLogs error: {}", msg),
            PruneDetectionReason::ReceiptMissing => {
                write!(f, "Transaction receipt missing for historical transaction")
            }
        }
    }
}

/// Error that can occur during prune detection.
#[derive(Error, Debug)]
pub enum PruneCheckError {
    #[error("Provider error: {0}")]
    ProviderError(#[from] ProviderError),

    #[error("Failed to get client version: {0}")]
    ClientVersionError(String),
}

/// Information about the RPC client.
#[derive(Debug, Clone, Default)]
pub struct ClientInfo {
    pub version: Option<String>,
    pub client_name: Option<String>,
}

impl ClientInfo {
    /// Parse client info from web3_clientVersion response.
    /// Example formats:
    /// - "Geth/v1.10.0-stable/linux-amd64/go1.16"
    /// - "erigon/2.48.0/linux-amd64/go1.20.4"
    /// - "Nethermind/v1.17.0/linux-x64/dotnet7.0.5"
    pub fn from_version_string(version: &str) -> Self {
        let parts: Vec<&str> = version.split('/').collect();
        let client_name = parts.first().map(|s| s.to_string());

        Self { version: Some(version.to_string()), client_name }
    }
}

/// Check if the error message indicates a pruned node.
fn is_pruned_error(error_msg: &str) -> bool {
    let lower = error_msg.to_lowercase();
    lower.contains("missing trie node")
        || lower.contains("pruned")
        || lower.contains("historical state")
        || lower.contains("state not available")
        || lower.contains("required historical state")
        || lower.contains("block not found")
        || lower.contains("header not found")
}

/// Check if an RPC node can serve historical data from the given start block.
///
/// Detection algorithm:
/// 1. Try to fetch the block at `start_block`
///    - If block is returned, check for transactions
///    - If block has transactions, try to get receipt for first tx
///    - If receipt exists -> Available
///    - If receipt is null/missing -> Pruned
/// 2. If block fetch fails with pruning-related error -> Pruned
/// 3. Otherwise -> Inconclusive
pub async fn check_rpc_pruning_status(
    provider: &Arc<dyn ChainProvider>,
    start_block: U64,
    network_name: &str,
) -> Result<PruneCheckResult, PruneCheckError> {
    debug!("Checking RPC pruning status for network '{}' at block {}", network_name, start_block);

    // Try to fetch the block with transactions
    let blocks_result = provider.get_block_by_number_batch(&[start_block], true).await;

    match blocks_result {
        Ok(blocks) => {
            if let Some(block) = blocks.first() {
                let txs: Vec<_> = block.transactions.hashes().collect();

                if txs.is_empty() {
                    // Block exists but has no transactions - can't verify receipts
                    // Try a few more blocks to find one with transactions
                    for offset in 1..=10u64 {
                        let check_block = start_block + U64::from(offset);
                        if let Ok(more_blocks) =
                            provider.get_block_by_number_batch(&[check_block], true).await
                        {
                            if let Some(b) = more_blocks.first() {
                                let more_txs: Vec<_> = b.transactions.hashes().collect();
                                if !more_txs.is_empty() {
                                    // Found a block with transactions
                                    let receipts =
                                        provider.get_tx_receipts_batch(&[more_txs[0]]).await?;
                                    if receipts.is_empty() {
                                        return Ok(PruneCheckResult::Pruned {
                                            client_version: None,
                                            reason: PruneDetectionReason::ReceiptMissing,
                                        });
                                    } else {
                                        return Ok(PruneCheckResult::Available {
                                            client_version: None,
                                        });
                                    }
                                }
                            }
                        }
                    }

                    // No blocks with transactions found in range - assume available
                    // (this is a quiet chain or very old blocks)
                    debug!(
                        "No transactions found in blocks [{}, {}] for network '{}', assuming available",
                        start_block,
                        start_block + U64::from(10),
                        network_name
                    );
                    Ok(PruneCheckResult::Available { client_version: None })
                } else {
                    // Block has transactions, verify we can get receipts
                    let receipts = provider.get_tx_receipts_batch(&[txs[0]]).await?;

                    if receipts.is_empty() {
                        Ok(PruneCheckResult::Pruned {
                            client_version: None,
                            reason: PruneDetectionReason::ReceiptMissing,
                        })
                    } else {
                        Ok(PruneCheckResult::Available { client_version: None })
                    }
                }
            } else {
                // No block returned - node doesn't have this block
                Ok(PruneCheckResult::Pruned {
                    client_version: None,
                    reason: PruneDetectionReason::GetLogsError(
                        "Block not found (node may be pruned)".to_string(),
                    ),
                })
            }
        }
        Err(e) => {
            let error_msg = e.to_string();

            if is_pruned_error(&error_msg) {
                Ok(PruneCheckResult::Pruned {
                    client_version: None,
                    reason: PruneDetectionReason::GetLogsError(error_msg),
                })
            } else {
                // Some other error - inconclusive
                Ok(PruneCheckResult::Inconclusive {
                    client_version: None,
                    reason: format!("Block fetch returned unexpected error: {}", error_msg),
                })
            }
        }
    }
}

/// Check if prune warnings should be ignored based on environment variable.
pub fn should_ignore_prune_warnings() -> bool {
    std::env::var(IGNORE_PRUNE_WARNINGS_ENV).is_ok()
}

/// Format a warning message for a pruned RPC.
pub fn format_prune_warning(
    network_name: &str,
    start_block: U64,
    result: &PruneCheckResult,
) -> String {
    match result {
        PruneCheckResult::Pruned { client_version, reason } => {
            let client_str =
                client_version.as_ref().map(|v| format!(" (client: {})", v)).unwrap_or_default();

            format!(
                r#"
WARNING: Network '{}' RPC{} appears to be PRUNED
and cannot serve historical data from block {}.
Reason: {}

This will cause indexing to fail or miss events. Options:
1. Use an archive node RPC endpoint
2. Adjust start_block in your manifest to a more recent block
3. Use --ignore-prune-warnings to proceed anyway (not recommended)
"#,
                network_name, client_str, start_block, reason
            )
        }
        PruneCheckResult::Inconclusive { client_version, reason } => {
            let client_str =
                client_version.as_ref().map(|v| format!(" (client: {})", v)).unwrap_or_default();

            format!(
                r#"
WARNING: Could not verify if network '{}' RPC{} can serve
historical data from block {}.
Reason: {}

Proceeding, but indexing may fail if the RPC is pruned.
"#,
                network_name, client_str, start_block, reason
            )
        }
        PruneCheckResult::Available { .. } => String::new(),
    }
}

/// Log prune check results with appropriate log level.
pub fn log_prune_result(network_name: &str, start_block: U64, result: &PruneCheckResult) {
    match result {
        PruneCheckResult::Available { client_version } => {
            debug!(
                "Network '{}' RPC (client: {:?}) can serve historical data from block {}",
                network_name, client_version, start_block
            );
        }
        PruneCheckResult::Pruned { .. } | PruneCheckResult::Inconclusive { .. } => {
            let warning = format_prune_warning(network_name, start_block, result);
            warn!("{}", warning.trim());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_info_parsing_geth() {
        let info = ClientInfo::from_version_string("Geth/v1.10.0-stable/linux-amd64/go1.16");
        assert_eq!(info.client_name, Some("Geth".to_string()));
        assert!(info.version.unwrap().contains("Geth"));
    }

    #[test]
    fn test_client_info_parsing_erigon() {
        let info = ClientInfo::from_version_string("erigon/2.48.0/linux-amd64/go1.20.4");
        assert_eq!(info.client_name, Some("erigon".to_string()));
    }

    #[test]
    fn test_client_info_parsing_nethermind() {
        let info = ClientInfo::from_version_string("Nethermind/v1.17.0/linux-x64/dotnet7.0.5");
        assert_eq!(info.client_name, Some("Nethermind".to_string()));
    }

    #[test]
    fn test_is_pruned_error() {
        assert!(is_pruned_error("missing trie node abc123"));
        assert!(is_pruned_error("Error: state pruned"));
        assert!(is_pruned_error("historical state unavailable"));
        assert!(is_pruned_error("state not available for block"));
        assert!(is_pruned_error("required historical state unavailable"));
        assert!(!is_pruned_error("connection timeout"));
        assert!(!is_pruned_error("rate limited"));
    }

    #[test]
    fn test_format_prune_warning_pruned() {
        let result = PruneCheckResult::Pruned {
            client_version: Some("Geth/v1.10.0".to_string()),
            reason: PruneDetectionReason::GetLogsError("missing trie node".to_string()),
        };

        let warning = format_prune_warning("ethereum", U64::from(1000000), &result);
        assert!(warning.contains("ethereum"));
        assert!(warning.contains("PRUNED"));
        assert!(warning.contains("1000000"));
        assert!(warning.contains("Geth/v1.10.0"));
        assert!(warning.contains("missing trie node"));
    }

    #[test]
    fn test_format_prune_warning_available() {
        let result =
            PruneCheckResult::Available { client_version: Some("Geth/v1.10.0".to_string()) };

        let warning = format_prune_warning("ethereum", U64::from(1000000), &result);
        assert!(warning.is_empty());
    }

    #[test]
    fn test_prune_detection_reason_display() {
        let reason = PruneDetectionReason::GetLogsError("missing trie node".to_string());
        assert_eq!(format!("{}", reason), "eth_getLogs error: missing trie node");

        let reason = PruneDetectionReason::ReceiptMissing;
        assert!(format!("{}", reason).contains("receipt missing"));
    }
}
