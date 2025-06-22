use alloy::primitives::{BlockNumber, B256};
use tokio::sync::mpsc;

/// Represents different types of chain state changes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainStateNotification {
    /// New blocks have been committed to the canonical chain
    Committed {
        /// Starting block number of the committed range
        from_block: BlockNumber,
        /// Ending block number of the committed range
        to_block: BlockNumber,
        /// Hash of the new chain tip
        tip_hash: B256,
    },
    /// Chain reorganization occurred
    Reorged {
        /// Starting block number to revert
        revert_from_block: BlockNumber,
        /// Ending block number to revert
        revert_to_block: BlockNumber,
        /// Starting block number of new chain segment
        new_from_block: BlockNumber,
        /// Ending block number of new chain segment
        new_to_block: BlockNumber,
        /// Hash of the new chain tip after reorg
        new_tip_hash: B256,
    },
    /// Blocks have been reverted (chain rollback)
    Reverted {
        /// Starting block number of reverted range
        from_block: BlockNumber,
        /// Ending block number of reverted range
        to_block: BlockNumber,
    },
}

/// Trait for components that can provide chain state notifications
pub trait ChainStateNotifier: Send + Sync {
    /// Subscribe to chain state notifications
    fn subscribe(&self) -> mpsc::UnboundedReceiver<ChainStateNotification>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notification_creation() {
        // Test Committed variant
        let committed = ChainStateNotification::Committed {
            from_block: 100,
            to_block: 200,
            tip_hash: B256::ZERO,
        };

        match committed {
            ChainStateNotification::Committed { from_block, to_block, tip_hash } => {
                assert_eq!(from_block, 100);
                assert_eq!(to_block, 200);
                assert_eq!(tip_hash, B256::ZERO);
            }
            _ => panic!("Expected Committed variant"),
        }

        // Test Reorged variant
        let reorged = ChainStateNotification::Reorged {
            revert_from_block: 150,
            revert_to_block: 200,
            new_from_block: 150,
            new_to_block: 210,
            new_tip_hash: B256::from([1u8; 32]),
        };

        match reorged {
            ChainStateNotification::Reorged {
                revert_from_block,
                revert_to_block,
                new_from_block,
                new_to_block,
                new_tip_hash,
            } => {
                assert_eq!(revert_from_block, 150);
                assert_eq!(revert_to_block, 200);
                assert_eq!(new_from_block, 150);
                assert_eq!(new_to_block, 210);
                assert_eq!(new_tip_hash, B256::from([1u8; 32]));
            }
            _ => panic!("Expected Reorged variant"),
        }

        // Test Reverted variant
        let reverted = ChainStateNotification::Reverted { from_block: 100, to_block: 150 };

        match reverted {
            ChainStateNotification::Reverted { from_block, to_block } => {
                assert_eq!(from_block, 100);
                assert_eq!(to_block, 150);
            }
            _ => panic!("Expected Reverted variant"),
        }
    }

    #[tokio::test]
    async fn test_notification_channel() {
        let (tx, mut rx) = mpsc::unbounded_channel();

        // Send a notification
        let notification =
            ChainStateNotification::Committed { from_block: 1, to_block: 10, tip_hash: B256::ZERO };

        tx.send(notification.clone()).unwrap();

        // Receive and verify
        let received = rx.recv().await.unwrap();
        assert_eq!(received, notification);
    }
}
