use std::{collections::HashMap, sync::Arc};

use alloy_rpc_types::{BlockNumberOrTag, Filter, FilteredParams, Log};
use futures::StreamExt;
use reth::{
    providers::{HeaderProvider, ReceiptProvider, TransactionsProvider},
    rpc::types::BlockHashOrNumber,
};
use reth_ethereum::{
    node::api::NodeTypes, primitives::AlloyBlockHeader, EthPrimitives, TransactionSigned,
};
use reth_execution_types::Chain;
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_tracing::tracing::{error, info};
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::reth::types::{
    BlockRangeInclusiveIter, ExExMode, ExExRequest, ExExReturnData, LogMetadata,
};
/// The default parallelism for the ExEx backfill jobs.
const DEFAULT_PARALLELISM: usize = 32;
/// The default batch size for the ExEx backfill jobs.
const DEFAULT_BATCH_SIZE: usize = 8;
/// The maximum number of headers we read at once when handling a range filter.
const MAX_HEADERS_RANGE: u64 = 1_000; // with ~530bytes per header this is ~500kb
/// The number of concurrent backfills allowed.
const BACKFILL_CONCURRENCY: usize = 32;

/// State for each job
struct JobState {
    filter: Arc<Filter>,
    tx: mpsc::UnboundedSender<ExExReturnData>,
    backfill_running: bool,
    buffer: Mutex<Chain>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

/// The ExEx that consumes new [`ExExNotification`]s and processes new backfill requests.
pub(crate) struct RindexerExEx<Node: FullNodeComponents> {
    /// The semaphore that limits the number of concurrent backfills.
    backfill_semaphore: Arc<Semaphore>,
    /// The context of the ExEx.
    ctx: ExExContext<Node>,
    /// Sender for exex requests.
    request_tx: mpsc::UnboundedSender<ExExRequest>,
    /// Receiver for exex requests.
    request_rx: mpsc::UnboundedReceiver<ExExRequest>,
    /// jobs
    jobs: HashMap<u64, JobState>,
    /// The next job id
    next_job_id: u64,
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
    ) -> Self {
        Self {
            ctx,
            request_tx,
            request_rx,
            next_job_id: 0,
            jobs: HashMap::new(),
            backfill_semaphore: Arc::new(Semaphore::new(BACKFILL_CONCURRENCY)),
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
            for job in self.jobs.values() {
                let mut buffer = job.buffer.lock().await;
                if buffer.is_empty() {
                    *buffer = (*committed_chain).clone();
                    continue;
                }
                if let Err(e) = buffer.append_chain((*committed_chain).clone()) {
                    error!("Failed to append chain: {:?}", e);
                    continue; // Skip processing if append fails
                }
                if !job.backfill_running {
                    // Clone the necessary data and drop the mutex before awaiting
                    let chain_clone = buffer.clone();
                    *buffer = Chain::default();
                    drop(buffer);

                    self.process_committed_chain(&chain_clone, &job.filter, job.tx.clone()).await?;
                }
            }
            self.ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }

        Ok(())
    }

    async fn process_committed_chain(
        &self,
        committed_chain: &Chain,
        filter: &Filter,
        tx: mpsc::UnboundedSender<ExExReturnData>,
    ) -> eyre::Result<()> {
        let range = committed_chain.range();

        let mut filter = filter.clone();

        filter = filter.from_block(*range.start());
        filter = filter.to_block(*range.end());

        process_blocks_with_filter(&self.ctx.provider(), &filter, tx).await?;

        Ok(())
    }

    /// Handles the given exex request.
    async fn handle_request(&mut self, request: ExExRequest) {
        match request {
            ExExRequest::Start { mode, filter, response_tx } => {
                let filter = Arc::new(filter);
                let result = self.start_request(mode, filter).await;
                let _ = response_tx.send(result);
            }
            ExExRequest::Cancel { job_id } => {
                self.jobs.remove(&job_id);
            }
            ExExRequest::Finish { job_id } => {
                if let Some(job) = self.jobs.get_mut(&job_id) {
                    job.backfill_running = false;
                }
            }
        }
    }

    async fn start_request(
        &mut self,
        mode: ExExMode,
        filter: Arc<Filter>,
    ) -> eyre::Result<(u64, mpsc::UnboundedReceiver<ExExReturnData>)> {
        let job_id = self.next_job_id;
        self.next_job_id += 1;

        // Create channels for this job
        let (tx, rx) = mpsc::unbounded_channel();
        let filter = filter.clone();

        match mode {
            ExExMode::HistoricOnly => {
                info!("Starting historic only backfill for job {}", job_id);
                let handle = self.backfill(filter.clone(), &tx).await?;
                self.jobs.insert(
                    job_id,
                    JobState {
                        filter,
                        tx,
                        backfill_running: true,
                        buffer: Mutex::new(Chain::default()),
                        handle: Some(handle),
                    },
                );
            }
            ExExMode::HistoricThenLive => {
                let mut filter = (*filter).clone();
                filter = filter.to_block(BlockNumberOrTag::Latest);
                let filter = Arc::new(filter);
                let handle = self.backfill(filter.clone(), &tx).await?;
                self.jobs.insert(
                    job_id,
                    JobState {
                        filter,
                        tx,
                        backfill_running: true,
                        buffer: Mutex::new(Chain::default()),
                        handle: Some(handle),
                    },
                );
            }
            ExExMode::LiveOnly => {
                self.jobs.insert(
                    job_id,
                    JobState {
                        filter,
                        tx,
                        backfill_running: false,
                        buffer: Mutex::new(Chain::default()),
                        handle: None,
                    },
                );
            }
        };

        Ok((job_id, rx))
    }

    async fn backfill(
        self: &mut Self,
        filter: Arc<Filter>,
        tx: &mpsc::UnboundedSender<ExExReturnData>,
    ) -> eyre::Result<tokio::task::JoinHandle<()>> {
        let permit = self.backfill_semaphore.acquire().await?;
        let provider = self.ctx.provider().clone();
        let tx = tx.clone();

        let handle = self.ctx.task_executor().spawn(async move {
            // Process filter logs with error handling
            if let Err(e) = process_blocks_with_filter(&provider, &filter, tx).await {
                error!("Failed to process blocks with filter: {}", e);
            }
        });

        drop(permit);
        Ok(handle)
    }
}

async fn process_blocks_with_filter<
    T: ReceiptProvider<Receipt = reth_ethereum::Receipt>
        + TransactionsProvider<Transaction = TransactionSigned>
        + HeaderProvider,
>(
    provider: &T,
    filter: &Filter,
    tx: mpsc::UnboundedSender<ExExReturnData>,
) -> eyre::Result<()> {
    info!("Processing blocks with filter");
    let from_block = filter.get_from_block();
    let to_block = filter.get_to_block();

    if let Some(from_block) = from_block {
        if let Some(to_block) = to_block {
            if from_block > to_block {
                return Err(eyre::eyre!(
                    "Invalid block range: from_block ({}) is greater than to_block ({})",
                    from_block,
                    to_block
                ));
            }
        }
    } else {
        eyre::bail!("No from block found for filter: {:?}", filter);
    }

    let from_block = from_block.unwrap();
    let to_block = to_block.unwrap_or(provider.last_block_number()?);
    let filter_params = FilteredParams::new(Some(filter.clone()));

    // Loop over the range of blocks and check logs if the filter matches the log's bloom filter
    for (from, to) in BlockRangeInclusiveIter::new(from_block..=to_block, MAX_HEADERS_RANGE) {
        let headers = provider
            .sealed_headers_range(from..=to)
            .map_err(|e| eyre::eyre!("Failed to get headers: {}", e))?;

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
                    .map_err(|e| eyre::eyre!("Failed to get transactions: {}", e))?
                    .ok_or_else(|| {
                        eyre::eyre!("No transactions found for block {}", block_number)
                    })?;

                let receipts = provider
                    .receipts_by_block(BlockHashOrNumber::Number(block_number))
                    .map_err(|e| eyre::eyre!("Failed to get receipts: {}", e))?
                    .ok_or_else(|| eyre::eyre!("No receipts found for block {}", block_number))?;

                for (tx_index, (transaction, receipt)) in
                    transactions.iter().zip(receipts.iter()).enumerate()
                {
                    let tx_hash = transaction.hash();
                    for (log_index, log) in receipt.logs.iter().enumerate() {
                        if filter_params.filter_address(&log.address) &&
                            filter_params.filter_topics(log.topics())
                        {
                            let log = Log {
                                inner: log.clone(),
                                block_timestamp: Some(block_timestamp),
                                block_hash: Some(block_hash),
                                block_number: Some(block_number),
                                transaction_hash: Some(*tx_hash),
                                transaction_index: Some(tx_index as u64),
                                log_index: Some(log_index as u64),
                                removed: false,
                            };

                            let result = tx.send(ExExReturnData { log });
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
