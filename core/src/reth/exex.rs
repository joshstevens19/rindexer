use std::{collections::HashMap, ops::RangeInclusive, sync::Arc};

use alloy_primitives::BlockNumber;
use alloy_rpc_types::{BlockHashOrNumber, Filter, FilteredParams};
use futures::{StreamExt, TryStreamExt};
use reth::providers::{HeaderProvider, ReceiptProvider, TransactionsProvider};
use reth_ethereum::{
    node::api::NodeTypes, primitives::AlloyBlockHeader, provider::BlockNumReader, EthPrimitives,
    TransactionSigned,
};
use reth_execution_types::Chain;
use reth_exex::{BackfillJobFactory, ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_tracing::tracing::{error, info};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

use crate::reth::{
    helpers::extract_block_range,
    types::{
        BlockRangeInclusiveIter, DataSource, ExExDataType, ExExMode, ExExRequest, ExExReturnData,
        LogMetadata,
    },
};
/// The default parallelism for the ExEx backfill jobs.
const DEFAULT_PARALLELISM: usize = 32;
/// The default batch size for the ExEx backfill jobs.
const DEFAULT_BATCH_SIZE: usize = 8;
/// The maximum number of headers we read at once when handling a range filter.
const MAX_HEADERS_RANGE: u64 = 1_000; // with ~530bytes per header this is ~500kb

/// The ExEx that consumes new [`ExExNotification`]s and processes new backfill requests.
pub(crate) struct RindexerExEx<Node: FullNodeComponents> {
    /// The context of the ExEx.
    ctx: ExExContext<Node>,
    /// Sender for exex requests.
    request_tx: mpsc::UnboundedSender<ExExRequest>,
    /// Receiver for exex requests.
    request_rx: mpsc::UnboundedReceiver<ExExRequest>,
    /// Factory for backfill jobs.
    backfill_job_factory: BackfillJobFactory<Node::Executor, Node::Provider>,
    /// Semaphore to limit the number of concurrent backfills.
    backfill_semaphore: Arc<Semaphore>,
    /// Next job ID.
    next_job_id: u64,
    /// Mapping of job IDs to backfill jobs.
    jobs: HashMap<u64, oneshot::Sender<oneshot::Sender<()>>>,
    /// Mapping of live channels so that we can send blocks to the correct channel
    live_channels: HashMap<u64, (ExExDataType, mpsc::UnboundedSender<ExExReturnData>)>,
}

impl<Node> RindexerExEx<Node>
where
    Node: FullNodeComponents<Types: NodeTypes<Primitives = EthPrimitives>>,
{
    /// Creates a new instance of the ExEx.
    pub(crate) fn new(
        ctx: ExExContext<Node>,
        request_tx: mpsc::UnboundedSender<ExExRequest>,
        request_rx: mpsc::UnboundedReceiver<ExExRequest>,
        backfill_limit: usize,
        stream_parallelism: Option<usize>,
    ) -> Self {
        let backfill_job_factory =
            BackfillJobFactory::new(ctx.block_executor().clone(), ctx.provider().clone())
                .with_stream_parallelism(stream_parallelism.unwrap_or(DEFAULT_PARALLELISM));

        Self {
            ctx,
            request_tx,
            request_rx,
            backfill_job_factory,
            backfill_semaphore: Arc::new(Semaphore::new(backfill_limit)),
            next_job_id: 0,
            jobs: HashMap::new(),
            live_channels: HashMap::new(),
        }
    }

    /// Starts listening for notifications and backfill requests.
    pub(crate) async fn start(mut self) -> eyre::Result<()> {
        loop {
            tokio::select! {
                Some(notification) = self.ctx.notifications.next() => {
                    self.handle_notification(notification?).await?;
                }
                Some(message) = self.request_rx.recv() => {
                    self.handle_request(message).await;
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
            for (job_id, (data_type, sender)) in &self.live_channels {
                info!("Processing committed chain for job {}", job_id);
                self.process_committed_chain(&committed_chain, data_type, sender.clone()).await?;
            }
            self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }

        Ok(())
    }

    async fn process_committed_chain(
        &self,
        committed_chain: &Chain,
        data_type: &ExExDataType,
        sender: mpsc::UnboundedSender<ExExReturnData>,
    ) -> eyre::Result<()> {
        match data_type {
            ExExDataType::Chain => {
                sender
                    .send(ExExReturnData::Chain {
                        chain: (*committed_chain).clone(),
                        source: DataSource::Live,
                    })
                    .map_err(|e| eyre::eyre!("failed to send chain to stream consumer: {}", e))?;
            }
            ExExDataType::FilteredLogs { filter } => {
                let range = committed_chain.range();

                process_blocks_with_filter(
                    &self.ctx.provider(),
                    range,
                    filter,
                    &sender,
                    DataSource::Live,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Handles the given exex request.
    async fn handle_request(&mut self, request: ExExRequest) {
        match request {
            ExExRequest::Start { mode, data_type, response_tx } => {
                let result = self.start_request(mode, data_type).await;
                let _ = response_tx.send(result);
            }
            ExExRequest::Cancel { job_id } => {
                self.live_channels.remove(&job_id);
            }
            ExExRequest::Finish { job_id } => {
                self.jobs.remove(&job_id);
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
    async fn start_request(
        &mut self,
        mode: ExExMode,
        data_type: ExExDataType,
    ) -> eyre::Result<(u64, mpsc::UnboundedReceiver<ExExReturnData>)> {
        let permit = self
            .backfill_semaphore
            .clone()
            .try_acquire_owned()
            .map_err(|err| eyre::eyre!("concurrent backfills limit reached: {err:?}"))?;

        let job_id = self.next_job_id;
        self.next_job_id += 1;

        // Create channels for this job
        let (sender, receiver) = mpsc::unbounded_channel();

        match mode {
            ExExMode::HistoricOnly { from, to } => {
                if let Err(e) = self.backfill(permit, from..=to, &data_type, sender).await {
                    error!("Failed to backfill: {}", e);
                }
            }
            ExExMode::HistoricThenLive { from } => {
                let to = self.ctx.provider().best_block_number()?;
                let data_type_clone = data_type.clone();
                let sender_clone = sender.clone();
                if let Err(e) =
                    self.backfill(permit, from..=to, &data_type_clone, sender_clone).await
                {
                    error!("Failed to backfill: {}", e);
                }
                self.live_channels.insert(job_id, (data_type, sender));
            }
            ExExMode::LiveOnly => {
                self.live_channels.insert(job_id, (data_type.clone(), sender.clone()));
            }
        };

        Ok((job_id, receiver))
    }

    /// Calls the [`process_committed_chain`] method for each backfilled block.
    ///
    /// Listens on the `cancel_rx` channel for cancellation requests.
    async fn backfill(
        self: &mut Self,
        _permit: OwnedSemaphorePermit,
        range: RangeInclusive<BlockNumber>,
        data_type: &ExExDataType,
        sender: mpsc::UnboundedSender<ExExReturnData>,
    ) -> eyre::Result<()> {
        match data_type.clone() {
            ExExDataType::Chain => {
                let job = self.backfill_job_factory.backfill(range);
                self.ctx.task_executor().spawn(async move {
                    let stream = job
                        .into_stream()
                        .with_batch_size(DEFAULT_BATCH_SIZE)
                        .with_parallelism(DEFAULT_PARALLELISM);
                    if let Err(e) = stream
                        .map_err(|e| -> eyre::Error { e.into() })
                        .try_for_each(|chain| {
                            let sender = sender.clone();
                            async move {
                                sender.send(ExExReturnData::Chain {
                                    chain,
                                    source: DataSource::Backfill,
                                })?;
                                Ok(())
                            }
                        })
                        .await
                    {
                        error!("Failed to backfill: {}", e);
                    }
                });
            }
            ExExDataType::FilteredLogs { filter } => {
                let provider = self.ctx.provider().clone();
                self.ctx.task_executor().spawn(async move {
                    // Process filter logs with error handling
                    if let Ok(block_range) = extract_block_range(&filter) {
                        if let Err(e) = process_blocks_with_filter(
                            &provider,
                            block_range,
                            &filter,
                            &sender,
                            DataSource::Backfill,
                        )
                        .await
                        {
                            error!("Failed to process blocks with filter: {}", e);
                        }
                    } else {
                        error!("Failed to extract block range from filter");
                    }
                });
            }
        }
        Ok(())
    }
}

/// Process a range of blocks, filtering logs by the provided filter and sending results through
/// the sender
async fn process_blocks_with_filter<
    T: ReceiptProvider<Receipt = reth_ethereum::Receipt>
        + TransactionsProvider<Transaction = TransactionSigned>
        + HeaderProvider,
>(
    provider: &T,
    block_range: RangeInclusive<BlockNumber>,
    filter: &Filter,
    sender: &mpsc::UnboundedSender<ExExReturnData>,
    source: DataSource,
) -> eyre::Result<()> {
    let (from_block, to_block) = (*block_range.start(), *block_range.end());

    // Loop over the range of blocks and check logs if the filter matches the log's bloom filter
    for (from, to) in BlockRangeInclusiveIter::new(from_block..=to_block, MAX_HEADERS_RANGE) {
        let headers = provider.sealed_headers_range(from..=to).unwrap();

        let filter_params = FilteredParams::new(Some(filter.clone()));

        // Derive bloom filters from filter input, so we can check headers for matching logs
        let address_filter = FilteredParams::address_filter(&filter.address);
        let topics_filter = FilteredParams::topics_filter(&filter.topics);

        for header in headers.iter() {
            if FilteredParams::matches_address(header.logs_bloom(), &address_filter) &&
                FilteredParams::matches_topics(header.logs_bloom(), &topics_filter)
            {
                let block_hash = header.hash();
                let block_timestamp = header.timestamp();
                let block_number = header.number();

                let transactions = provider
                    .transactions_by_block(BlockHashOrNumber::Number(block_number))
                    .unwrap()
                    .unwrap();

                let receipts = provider
                    .receipts_by_block(BlockHashOrNumber::Number(block_number))
                    .unwrap()
                    .unwrap();

                for (tx_index, (transaction, receipt)) in
                    transactions.iter().zip(receipts.iter()).enumerate()
                {
                    let tx_hash = transaction.hash();
                    for (log_index, log) in receipt.logs.iter().enumerate() {
                        if filter_params.filter_address(&log.address) &&
                            filter_params.filter_topics(log.topics())
                        {
                            let log_metadata = LogMetadata {
                                block_timestamp,
                                block_hash,
                                block_number,
                                tx_hash: *tx_hash,
                                tx_index: tx_index as u64,
                                log_index,
                                log_type: None,
                                removed: false,
                            };

                            let result = sender.send(ExExReturnData::Log {
                                log: (log.clone(), log_metadata),
                                source: source.clone(),
                            });
                            if let Err(e) = result {
                                error!("Failed to send log to stream consumer: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
