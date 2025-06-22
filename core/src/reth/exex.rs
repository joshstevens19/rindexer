use crate::provider::notifications::ChainStateNotification;
use futures::StreamExt;
use reth_exex::{ExExContext, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_tracing::tracing::info;
use tokio::sync::mpsc;

/// Minimal ExEx that only translates ExExNotifications to ChainStateNotifications
pub struct RindexerExEx<Node: FullNodeComponents> {
    /// The context of the ExEx
    ctx: ExExContext<Node>,
    /// Channel to send chain state notifications
    notification_tx: mpsc::UnboundedSender<ChainStateNotification>,
}

impl<Node: FullNodeComponents> RindexerExEx<Node> {
    /// Creates a new instance of the ExEx
    pub fn new(
        ctx: ExExContext<Node>,
        notification_tx: mpsc::UnboundedSender<ChainStateNotification>,
    ) -> Self {
        Self { ctx, notification_tx }
    }

    /// Starts listening for ExEx notifications and converts them to ChainStateNotifications
    pub async fn start(mut self) -> eyre::Result<()> {
        info!("Starting RindexerExEx notification translator");

        while let Some(notification) = self.ctx.notifications.next().await {
            let notification = notification?;

            match &notification {
                ExExNotification::ChainCommitted { new } => {
                    let range = new.range();
                    info!(
                        from = range.start(),
                        to = range.end(),
                        "Received chain committed notification"
                    );

                    let chain_notification = ChainStateNotification::Committed {
                        from_block: *range.start(),
                        to_block: *range.end(),
                        tip_hash: new.tip().hash(),
                    };

                    let _ = self.notification_tx.send(chain_notification);
                }
                ExExNotification::ChainReorged { old, new } => {
                    let old_range = old.range();
                    let new_range = new.range();
                    info!(
                        old_from = old_range.start(),
                        old_to = old_range.end(),
                        new_from = new_range.start(),
                        new_to = new_range.end(),
                        "Received chain reorg notification"
                    );

                    let chain_notification = ChainStateNotification::Reorged {
                        revert_from_block: *old_range.start(),
                        revert_to_block: *old_range.end(),
                        new_from_block: *new_range.start(),
                        new_to_block: *new_range.end(),
                        new_tip_hash: new.tip().hash(),
                    };

                    let _ = self.notification_tx.send(chain_notification);
                }
                ExExNotification::ChainReverted { old } => {
                    let range = old.range();
                    info!(
                        from = range.start(),
                        to = range.end(),
                        "Received chain revert notification"
                    );

                    let chain_notification = ChainStateNotification::Reverted {
                        from_block: *range.start(),
                        to_block: *range.end(),
                    };

                    let _ = self.notification_tx.send(chain_notification);
                }
            }
        }

        Ok(())
    }
}
