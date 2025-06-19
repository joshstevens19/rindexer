use std::{error::Error, sync::Arc};

use alloy::primitives::{B256, U64};
use alloy::rpc::types::Filter;
use futures::stream::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info};

use crate::event::RindexerEventFilter;
use crate::indexer::{FetchLogsResult, IndexingEventProgressStatus};
use crate::reth::types::{ExExMode, ExExRequest, ExExReturnData, ExExTx};

/// Fetch historic logs using ExEx
pub async fn fetch_historic_logs_exex(
    reth_tx: Arc<ExExTx>,
    tx: &mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    topic_id: &B256,
    current_filter: RindexerEventFilter,
    info_log_name: &str,
    network: &str,
) -> Option<()> {
    // Convert RindexerEventFilter to alloy Filter
    let mut filter = Filter::new()
        .from_block(current_filter.from_block())
        .to_block(current_filter.to_block());
    
    // Add contract addresses if available
    if let Some(addresses) = current_filter.contract_addresses().await {
        let addr_vec: Vec<_> = addresses.into_iter().collect();
        if !addr_vec.is_empty() {
            filter = filter.address(addr_vec);
        }
    }
    
    // Add topic filters
    filter = filter.event_signature(*topic_id);

    let (response_tx, response_rx) = oneshot::channel();

    let res = reth_tx.send(ExExRequest::Start {
        mode: ExExMode::HistoricOnly,
        filter: filter.clone(),
        response_tx,
    });
    
    if let Err(e) = res {
        error!(
            "{}::{} - {} - Failed to start ExEx historic fetch: {}",
            info_log_name,
            network,
            IndexingEventProgressStatus::Syncing.log(),
            e
        );
        return None;
    }

    let (job_id, rx) = if let Ok(Ok(res)) = response_rx.await {
        res
    } else {
        error!(
            "{}::{} - {} - Failed to start ExEx historic fetch",
            info_log_name,
            network,
            IndexingEventProgressStatus::Syncing.log(),
        );
        return None;
    };

    info!("ExEx historic fetch started for job {}", job_id);
    
    // Process the logs in batches
    let mut batched_stream = UnboundedReceiverStream::new(rx).chunks(100);
    while let Some(logs) = batched_stream.next().await {
        let mut from_block: u64 = u64::MAX;
        let mut to_block: u64 = u64::MIN;
        let mut wrapped_logs = Vec::new();
        
        for ExExReturnData { log } in logs {
            from_block = from_block.min(log.block_number.unwrap());
            to_block = to_block.max(log.block_number.unwrap());
            wrapped_logs.push(log);
        }
        
        if let Err(e) = tx.send(Ok(FetchLogsResult {
            logs: wrapped_logs,
            from_block: U64::from(from_block),
            to_block: U64::from(to_block),
        })) {
            error!(
                "{}::{} - {} - Failed to send logs to stream consumer: {}",
                info_log_name,
                network,
                IndexingEventProgressStatus::Syncing.log(),
                e
            );
            return None;
        }
    }

    info!("ExEx historic fetch complete for job {}", job_id);
    
    // Send finish request
    let _ = reth_tx.send(ExExRequest::Finish { job_id });
    
    Some(())
}

/// Start live indexing using ExEx
pub async fn start_live_indexing_exex(
    reth_tx: Arc<ExExTx>,
    tx: mpsc::UnboundedSender<Result<FetchLogsResult, Box<dyn Error + Send>>>,
    from_block: U64,
    topic_id: B256,
    current_filter: RindexerEventFilter,
    info_log_name: String,
    network: String,
) -> mpsc::UnboundedReceiver<ExExReturnData> {
    // Convert RindexerEventFilter to alloy Filter
    let mut filter = Filter::new()
        .from_block(from_block);
    
    // Add contract addresses if available
    if let Some(addresses) = current_filter.contract_addresses().await {
        let addr_vec: Vec<_> = addresses.into_iter().collect();
        if !addr_vec.is_empty() {
            filter = filter.address(addr_vec);
        }
    }
    
    // Add topic filters
    filter = filter.event_signature(topic_id);

    let (response_tx, response_rx) = oneshot::channel();
    
    let res = reth_tx.send(ExExRequest::Start {
        mode: ExExMode::LiveOnly,
        filter,
        response_tx,
    });
    
    if let Err(e) = res {
        error!(
            "{}::{} - {} - Failed to start ExEx live indexing: {}",
            info_log_name,
            network,
            IndexingEventProgressStatus::Live.log(),
            e
        );
        return mpsc::unbounded_channel().1; // Return empty receiver
    }

    let (job_id, stream) = if let Ok(Ok(res)) = response_rx.await {
        res
    } else {
        error!(
            "{}::{} - {} - Failed to start ExEx live indexing",
            info_log_name,
            network,
            IndexingEventProgressStatus::Live.log()
        );
        return mpsc::unbounded_channel().1; // Return empty receiver
    };

    info!("ExEx live indexing started for job {}", job_id);
    
    // Spawn task to forward logs
    tokio::spawn(async move {
        let mut stream = UnboundedReceiverStream::new(stream);
        while let Some(ExExReturnData { log }) = stream.next().await {
            if let Err(e) = tx.send(Ok(FetchLogsResult {
                logs: vec![log.clone()],
                from_block: U64::from(log.block_number.unwrap()),
                to_block: U64::from(log.block_number.unwrap()),
            })) {
                error!(
                    "{}::{} - {} - Failed to send logs to stream consumer: {}",
                    info_log_name,
                    network,
                    IndexingEventProgressStatus::Live.log(),
                    e
                );
                break;
            }
        }
    });
    
    // Return a dummy receiver since we're handling forwarding internally
    mpsc::unbounded_channel().1
}