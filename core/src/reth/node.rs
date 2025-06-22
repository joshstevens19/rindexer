use std::thread::Builder;

use futures::FutureExt;
use reth::cli::Cli;
use reth_node_ethereum::EthereumNode;
use tokio::sync::mpsc;

use crate::provider::notifications::ChainStateNotification;
use crate::reth::exex::RindexerExEx;

/// The stack size for the Reth node thread.
const STACK_SIZE: usize = 32 * 1024 * 1024; // 32 MB

/// The name of the execution extension.
const EXECUTION_EXTENSION_NAME: &str = "rindexer";

/// Starts a Reth node with the execution extension that forwards chain state notifications to the provided channel.
pub fn start_reth_node_with_exex(
    cli: Cli,
) -> eyre::Result<mpsc::UnboundedReceiver<ChainStateNotification>> {
    // Create a channel for chain state notifications. Sender will go to ExEx, receiver
    // will be returned to the caller.
    let (notification_tx, notification_rx) = mpsc::unbounded_channel::<ChainStateNotification>();

    // Spawn the node with a larger stack size, otherwise it will crash with a stack overflow
    let builder = Builder::new().stack_size(STACK_SIZE);

    let _ = builder.spawn(move || {
        let result = cli.run(|builder, _| async move {
            let handle = builder
                .node(EthereumNode::default())
                .install_exex(EXECUTION_EXTENSION_NAME, move |ctx| {
                    tokio::task::spawn_blocking(move || {
                        tokio::runtime::Handle::current().block_on(async move {
                            let exex = RindexerExEx::new(ctx, notification_tx);
                            eyre::Ok(exex.start())
                        })
                    })
                    .map(|result| result.map_err(Into::into).and_then(|result| result))
                })
                .launch()
                .await?;
            handle.wait_for_node_exit().await
        });
        if let Err(e) = result {
            eprintln!("Node thread error: {:?}", e);
        }
    });

    Ok(notification_rx)
}
