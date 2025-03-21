use std::thread::Builder;

use futures::FutureExt;
use reth::cli::Cli;
use reth_node_ethereum::EthereumNode;
use tokio::sync::mpsc;

use crate::reth::{exex::RindexerExEx, types::ExExRequest};

/// The type of the channel that forwards requests to the Reth node.
type RethTx = mpsc::UnboundedSender<ExExRequest>;

/// The stack size for the Reth node thread.
const STACK_SIZE: usize = 32 * 1024 * 1024; // 32 MB

/// The name of the execution extension.
const EXECUTION_EXTENSION_NAME: &str = "rindexer";

/// The number of concurrent backfills.
const CONCURRENT_BACKFILLS: usize = 10;

/// Starts a Reth node with the execution extension that forwards blocks to the provided channel.
pub fn start_reth_node_with_exex(cli: Cli) -> eyre::Result<RethTx> {
    // Create a channel for backfill requests. Sender will go to rindexer, receiver
    // will be used by the ExEx.
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let request_tx_clone = request_tx.clone();

    // Spawn the node with a larger stack size, otherwise it will crash with a stack overflow
    let builder = Builder::new().stack_size(STACK_SIZE);

    let _ = builder.spawn(move || {
        let result = cli.run(|builder, _| async move {
            let handle = builder
                .node(EthereumNode::default())
                .install_exex(EXECUTION_EXTENSION_NAME, move |ctx| {
                    tokio::task::spawn_blocking(move || {
                        tokio::runtime::Handle::current().block_on(async move {
                            let exex = RindexerExEx::new(ctx, request_tx_clone, request_rx);
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

    Ok(request_tx)
}
