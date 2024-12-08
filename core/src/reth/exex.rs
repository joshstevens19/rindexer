use std::sync::Arc;
use futures::{Future, TryStreamExt};
use reth::builder::components::ExecutorBuilder;
use reth::builder::NodeConfig;
use reth::chainspec::HOLESKY;
use reth::primitives::{Log, SealedBlockWithSenders, TransactionSigned};
use reth_execution_types::Chain;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use tracing::info;
use crate::rindexer_info;

async fn exex<Node: FullNodeComponents>(mut ctx: ExExContext<Node>) -> eyre::Result<()> {
    while let Some(notification) = ctx.notifications.try_next().await? {
        match &notification {
            ExExNotification::ChainCommitted { new } => {
                let events = decode_chain_into_events(new);

                info!("events length - {}", events.count());
                info!(committed_chain = ?new.range(), "Received commit");
            }
            ExExNotification::ChainReorged { old, new } => {
                info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
            }
            ExExNotification::ChainReverted { old } => {
                info!(reverted_chain = ?old.range(), "Received revert");
            }
        };

        if let Some(committed_chain) = notification.committed_chain() {
            ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }
    }

    Ok(())
}

fn decode_chain_into_events(
    chain: &Chain,
) -> impl Iterator<Item = (&SealedBlockWithSenders, &TransactionSigned, &Log)> {
    chain
        // Get all blocks and receipts
        .blocks_and_receipts()
        // Get all receipts
        .flat_map(|(block, receipts)| {
            block
                .body
                .transactions()
                .zip(receipts.iter().flatten())
                .map(move |(tx, receipt)| (block, tx, receipt))
        })
        // Get all logs from expected bridge contracts
        .flat_map(|(block, tx, receipt)| {
            receipt
                .logs
                .iter()
                // .filter(|log| OP_BRIDGES.contains(&log.address))
                .map(move |log| (block, tx, log))
        })
    // Decode and filter bridge events
    // .filter_map(|(block, tx, log)| {
    //     L1StandardBridgeEvents::decode_raw_log(log.topics(), &log.data.data, true)
    //         .ok()
    //         .map(|event| (block, tx, log, event))
    // })
}

pub fn start_reth(args: Vec<String>) -> eyre::Result<()> {
    // Create thread with larger stack size
    let builder = std::thread::Builder::new()
        .name("reth-node".into())
        .stack_size(8 * 1024 * 1024); // 8MB stack size

    builder.spawn(move || {
        let cli = reth::cli::Cli::try_parse_args_from(args.iter())?;

        cli.run(|builder, _| async move {
            let handle = builder
                .node(EthereumNode::default())
                .install_exex("exex", |ctx| async move { Ok(exex(ctx)) })
                .launch()
                .await?;

            rindexer_info!("Launched exex");

            handle.wait_for_node_exit().await
        })
    })?;

    Ok(())
}
