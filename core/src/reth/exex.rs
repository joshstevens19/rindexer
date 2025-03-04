use std::{
    collections::{BTreeMap, HashMap},
    ops::RangeInclusive,
    path::PathBuf,
    sync::Arc,
};

use alloy_primitives::BlockNumber;
use futures::{Stream, StreamExt, TryStreamExt};
use reth_ethereum::{node::api::NodeTypes, provider::BlockNumReader, EthPrimitives};
use reth_execution_types::Chain;
use reth_exex::{BackfillJob, BackfillJobFactory, ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::{error, info};
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};

use crate::reth::{BackfillMessage, BackfillMode, ExexType, RethBlockWithReceipts};

/// The ExEx that consumes new [`ExExNotification`]s and processes new backfill requests.
struct BackfillExEx<Node: FullNodeComponents> {
    /// The context of the ExEx.
    ctx: ExExContext<Node>,
    /// Sender for backfill messages.
    backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
    /// Receiver for backfill messages.
    backfill_rx: mpsc::UnboundedReceiver<BackfillMessage>,
    /// Factory for backfill jobs.
    backfill_job_factory: BackfillJobFactory<Node::Executor, Node::Provider>,
    /// Semaphore to limit the number of concurrent backfills.
    backfill_semaphore: Arc<Semaphore>,
    /// Next backfill job ID.
    next_backfill_job_id: u64,
    /// Mapping of backfill job IDs to backfill jobs.
    backfill_jobs: HashMap<u64, oneshot::Sender<oneshot::Sender<()>>>,
    /// Mapping of live channels so that we can send blocks to the correct channel
    live_channels: HashMap<u64, mpsc::UnboundedSender<RethBlockWithReceipts>>,
}

impl<Node> BackfillExEx<Node>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    /// Creates a new instance of the ExEx.
    fn new(
        ctx: ExExContext<Node>,
        backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
        backfill_rx: mpsc::UnboundedReceiver<BackfillMessage>,
        backfill_limit: usize,
    ) -> Self {
        let backfill_job_factory =
            BackfillJobFactory::new(ctx.block_executor().clone(), ctx.provider().clone());

        Self {
            ctx,
            backfill_tx,
            backfill_rx,
            backfill_job_factory,
            backfill_semaphore: Arc::new(Semaphore::new(backfill_limit)),
            next_backfill_job_id: 0,
            backfill_jobs: HashMap::new(),
            live_channels: HashMap::new(),
        }
    }

    /// Starts listening for notifications and backfill requests.
    async fn start(mut self) -> eyre::Result<()> {
        loop {
            tokio::select! {
                Some(notification) = self.ctx.notifications.next() => {
                    self.handle_notification(notification?).await?;
                }
                Some(message) = self.backfill_rx.recv() => {
                    self.handle_backfill_message(message).await;
                }
            }
        }
    }

    /// Handles the given notification and calls [`process_committed_chain`] for a committed
    /// chain, if any.
    async fn handle_notification(&self, notification: ExExNotification) -> eyre::Result<()> {
        match &notification {
            ExExNotification::ChainCommitted { new } => {
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
            for (_, channel) in self.live_channels.iter() {
                process_committed_chain(&committed_chain, channel.clone(), ExexType::Live)?;
            }

            self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }

        Ok(())
    }

    /// Handles the given backfill message.
    async fn handle_backfill_message(&mut self, message: BackfillMessage) {
        match message {
            BackfillMessage::Start { from_block, to_block, mode, response_tx } => {
                let result = self.start_backfill(from_block, to_block, mode).await;
                let _ = response_tx.send(result);
            }
            BackfillMessage::Cancel { job_id, response_tx } => {
                self.live_channels.remove(&job_id);
                let _ = response_tx.send(self.cancel_backfill(job_id).await);
            }
            BackfillMessage::Finish { job_id } => {
                self.backfill_jobs.remove(&job_id);
                if self.live_channels.contains_key(&job_id) {
                    info!("Job {}: Backfill complete, continuing in live mode", job_id);
                } else {
                    self.live_channels.remove(&job_id);
                }
            }
        }
    }

    /// Backfills the given range of blocks in parallel. Requires acquiring a
    /// semaphore permit that limits the number of concurrent backfills. The backfill job is
    /// spawned in a separate task.
    ///
    /// Returns the backfill job ID or an error if the semaphore permit could not be acquired.
    async fn start_backfill(
        &mut self,
        from_block: BlockNumber,
        to_block: Option<BlockNumber>,
        mode: BackfillMode,
    ) -> eyre::Result<(u64, mpsc::UnboundedReceiver<RethBlockWithReceipts>)> {
        let permit = self
            .backfill_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|err| eyre::eyre!("concurrent backfills limit reached: {err:?}"))?;

        let job_id = self.next_backfill_job_id;
        self.next_backfill_job_id += 1;

        // Create channels for this job
        let (rindexer_tx, rindexer_rx) = mpsc::unbounded_channel();
        let (block_tx, block_rx) = mpsc::unbounded_channel();

        // Determine backfill range
        let backfill_to = match mode {
            BackfillMode::PureBackfill => {
                to_block.ok_or_else(|| eyre::eyre!("to_block required for PureBackfill mode"))?
            }
            BackfillMode::BackfillWithLive => {
                to_block.unwrap_or(self.ctx.provider().best_block_number()?)
            }
        };

        if backfill_to < from_block {
            eyre::bail!("to_block must be >= from_block");
        }

        // Spawn ordering task
        self.ctx.task_executor().spawn(ordering_task(
            block_rx,
            rindexer_tx,
            from_block,
            backfill_to,
            mode,
        ));

        // Spawn backfill job
        let range = from_block..=backfill_to;
        let job = self.backfill_job_factory.backfill(range);
        let backfill_tx = self.backfill_tx.clone();
        let backfill_block_tx = block_tx.clone();
        let (cancel_tx, cancel_rx) = oneshot::channel();
        self.backfill_jobs.insert(job_id, cancel_tx);

        self.ctx.task_executor().spawn(async move {
            Self::backfill(permit, job_id, job, backfill_tx, cancel_rx, backfill_block_tx).await;
        });

        // Register live channel for BackfillWithLive mode
        if matches!(mode, BackfillMode::BackfillWithLive) {
            self.live_channels.insert(job_id, block_tx.clone());
        }

        Ok((job_id, rindexer_rx))
    }

    async fn cancel_backfill(&mut self, job_id: u64) -> eyre::Result<()> {
        let Some(cancel_tx) = self.backfill_jobs.remove(&job_id) else {
            eyre::bail!("backfill job not found");
        };
        let (tx, rx) = oneshot::channel();
        cancel_tx.send(tx).map_err(|_| eyre::eyre!("failed to send cancel signal"))?;
        rx.await.map_err(|_| eyre::eyre!("failed to receive cancel confirmation"))?;
        Ok(())
    }

    /// Calls the [`process_committed_chain`] method for each backfilled block.
    ///
    /// Listens on the `cancel_rx` channel for cancellation requests.
    async fn backfill(
        _permit: OwnedSemaphorePermit,
        job_id: u64,
        job: BackfillJob<Node::Executor, Node::Provider>,
        backfill_tx: mpsc::UnboundedSender<BackfillMessage>,
        cancel_rx: oneshot::Receiver<oneshot::Sender<()>>,
        block_tx: mpsc::UnboundedSender<RethBlockWithReceipts>,
    ) {
        let backfill = backfill_with_job(job.into_stream(), block_tx);

        tokio::select! {
            result = backfill => {
                if let Err(err) = result {
                    error!(%err, "Backfill error occurred");
                }

                let _ = backfill_tx.send(BackfillMessage::Finish { job_id });
            }
            sender = cancel_rx => {
                info!("Backfill job cancelled");

                if let Ok(sender) = sender {
                    let _ = sender.send(());
                }
            }
        }
    }
}

async fn ordering_task(
    mut block_rx: mpsc::UnboundedReceiver<RethBlockWithReceipts>,
    rindexer_tx: mpsc::UnboundedSender<RethBlockWithReceipts>,
    from_block: u64,
    to_block: u64,
    mode: BackfillMode,
) {
    let mut next_block_to_send = from_block;
    let mut blocks = BTreeMap::new();

    while let Some(block) = block_rx.recv().await {
        let block_number = block.block_receipts.block.number;
        if block_number >= next_block_to_send {
            if matches!(mode, BackfillMode::PureBackfill) && block_number > to_block {
                continue; // Ignore blocks beyond to_block in PureBackfill
            }
            blocks.insert(block_number, block);

            while let Some(block) = blocks.remove(&next_block_to_send) {
                if let Err(e) = rindexer_tx.send(block) {
                    error!("Failed to send block: {}", e);
                    return;
                }
                next_block_to_send += 1;
                if matches!(mode, BackfillMode::PureBackfill) && next_block_to_send > to_block {
                    drop(rindexer_tx);
                    return; // Stop after to_block in PureBackfill
                }
            }
        }
    }
}

/// Backfills the given range of blocks in parallel, calling the
/// [`process_committed_chain`] method for each block.
async fn backfill_with_job<S, E>(
    st: S,
    block_tx: mpsc::UnboundedSender<RethBlockWithReceipts>,
) -> eyre::Result<()>
where
    S: Stream<Item = Result<Chain, E>>,
    E: Into<eyre::Error>,
{
    st
        // Covert the block execution error into `eyre`
        .map_err(Into::into)
        // Process each block, returning early if an error occurs
        .try_for_each(|chain| {
            let tx = block_tx.clone();
            async move { process_committed_chain(&chain, tx, ExexType::Backfill) }
        })
        .await
}

fn process_committed_chain(
    chain: &Chain,
    block_tx: mpsc::UnboundedSender<RethBlockWithReceipts>,
    exex_type: ExexType,
) -> eyre::Result<()> {
    let receipts_with_attachment = chain.receipts_with_attachment();

    for block_receipts in receipts_with_attachment {
        let block = block_receipts.block;
        let block_number = block.number;

        // Get the block timestamp
        let block_data = chain.blocks().get(&block_number).expect("Block should exist in chain");
        let block_timestamp = block_data.header().timestamp;

        block_tx.send(RethBlockWithReceipts {
            block_receipts,
            block_timestamp,
            exex_type: exex_type.clone(),
        })?;
    }

    Ok(())
}

/// Starts a Reth node with the execution extension that forwards blocks to the provided channel.
pub async fn start_reth_node_with_exex(
    chain_id: u64,
    data_dir: PathBuf,
    network_name: String,
) -> eyre::Result<mpsc::UnboundedSender<BackfillMessage>> {
    info!("Starting Reth node for network {} with chain ID {}", network_name, chain_id);

    let data_dir = data_dir.to_str().unwrap();
    let chain_id = chain_id.to_string();

    let args = vec!["reth", "node", "--data-dir", data_dir, "--chain-id", &chain_id];
    // Create a channel for backfill requests. Sender will go to the RPC server, receiver
    // will be used by the ExEx.
    let (backfill_tx, backfill_rx) = mpsc::unbounded_channel();
    let rindexer_backfill_tx = backfill_tx.clone();
    let exex_backfill_tx = backfill_tx.clone();

    reth::cli::Cli::try_parse_args_from(args).unwrap().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            // Install the backfill ExEx.
            .install_exex("Backfill", |ctx| async move {
                Ok(BackfillExEx::new(ctx, exex_backfill_tx, backfill_rx, 10).start())
            })
            .launch()
            .await?;
        handle.wait_for_node_exit().await
    })?;

    Ok(rindexer_backfill_tx)
}
