use alloy::primitives::U64;
use colored::{ColoredString, Colorize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::event::callback_registry::{
    EventCallbackRegistryInformation, TraceCallbackRegistryInformation,
};
use crate::events::RindexerEventEmitter;
use crate::RindexerEvent;

#[derive(Clone, Debug, Hash)]
pub enum IndexingEventProgressStatus {
    Syncing { progress: u16, syncing_to_block: U64 },
    Live,
    Completed,
}

impl IndexingEventProgressStatus {
    pub fn syncing_log() -> ColoredString {
        "SYNCING".green()
    }

    pub fn live_log() -> ColoredString {
        "LIVE".green()
    }

    pub fn completed_log() -> ColoredString {
        "COMPLETED".green()
    }
}

#[derive(Clone, Debug, Hash)]
pub struct IndexingEventProgress {
    pub id: String,
    pub contract_name: String,
    pub event_name: String,
    pub starting_block: U64,
    pub last_synced_block: U64,
    pub network: String,
    pub chain_id: u64,
    pub live_indexing: bool,
    pub status: IndexingEventProgressStatus,
    pub info_log: String,
}

impl IndexingEventProgress {
    #[allow(clippy::too_many_arguments)]
    fn running(
        id: String,
        contract_name: String,
        event_name: String,
        starting_block: U64,
        last_synced_block: U64,
        syncing_to_block: U64,
        network: String,
        chain_id: u64,
        live_indexing: bool,
        info_log: String,
    ) -> Self {
        Self {
            id,
            contract_name,
            event_name,
            starting_block,
            last_synced_block,
            network,
            chain_id,
            live_indexing,
            status: IndexingEventProgressStatus::Syncing { progress: 0, syncing_to_block },
            info_log,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SyncError {
    #[error("Event with id {0} not found")]
    EventNotFound(String),

    #[error("Block number conversion error for total blocks: from {0} to {1}")]
    BlockNumberConversionTotalBlocksError(U64, U64),

    #[error("Block number conversion error for synced blocks: from {0} to {1}")]
    BlockNumberConversionSyncedBlocksError(U64, U64),
}

/// Build a composite key for the `events` HashMap so that the same event ID
/// on different chains gets its own entry.
fn progress_key(chain_id: u64, id: &str) -> String {
    format!("{chain_id}::{id}")
}

/// Info needed for block-level progress reporting after releasing the events lock.
struct BlockReport {
    chain_id: u64,
    event_id: String,
    block: U64,
}

struct NetworkBlockProgress {
    events: HashMap<String, U64>,
    last_emitted_min: U64,
}

/// Tracks per-event indexing progress and emits `BlockIndexingCompleted` when the minimum
/// synced block across all events on a chain advances.
pub struct IndexingEventsProgressState {
    events: Mutex<HashMap<String, IndexingEventProgress>>,
    block_networks: Mutex<HashMap<u64, NetworkBlockProgress>>,
    emitter: Option<RindexerEventEmitter>,
}

impl IndexingEventsProgressState {
    pub async fn monitor(
        event_information: &[EventCallbackRegistryInformation],
        trace_information: &[TraceCallbackRegistryInformation],
        emitter: Option<RindexerEventEmitter>,
    ) -> Arc<IndexingEventsProgressState> {
        let mut events: HashMap<String, IndexingEventProgress> = HashMap::new();
        let mut block_networks: HashMap<u64, NetworkBlockProgress> = HashMap::new();
        let mut network_latest_cache: HashMap<String, U64> = HashMap::new();
        let mut seen_trace_networks: HashSet<String> = HashSet::new();

        // Register contract events
        for event_info in event_information {
            for network_contract in &event_info.contract.details {
                let network = network_contract.network.clone();
                let latest_block_cached = network_latest_cache.get(&network);
                let latest_block = match latest_block_cached {
                    Some(b) => {
                        debug!("Got block for {} from cache", &network);
                        Ok(*b)
                    }
                    None => {
                        let block = network_contract.cached_provider.get_block_number().await;
                        if let Ok(b) = block {
                            network_latest_cache.insert(network, b);
                        }
                        block
                    }
                };

                match latest_block {
                    Ok(latest_block) => {
                        let start_block = network_contract.start_block.unwrap_or(latest_block);
                        let end_block = network_contract.end_block.unwrap_or(latest_block);
                        let chain_id = network_contract.cached_provider.chain.id();

                        if emitter.is_some() {
                            let np = block_networks.entry(chain_id).or_insert_with(|| {
                                NetworkBlockProgress {
                                    events: HashMap::new(),
                                    last_emitted_min: U64::ZERO,
                                }
                            });
                            np.events.insert(event_info.id.to_string(), start_block);
                        }

                        events.insert(
                            progress_key(chain_id, &event_info.id),
                            IndexingEventProgress::running(
                                event_info.id.to_string(),
                                event_info.contract.name.clone(),
                                event_info.event_name.to_string(),
                                start_block,
                                start_block,
                                if latest_block > end_block { end_block } else { latest_block },
                                network_contract.network.clone(),
                                chain_id,
                                network_contract.end_block.is_none(),
                                event_info.info_log_name(),
                            ),
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to get latest block for network {}: {}",
                            network_contract.network, e
                        );
                    }
                }
            }
        }

        // Register trace events
        for trace_info in trace_information {
            for network_traces in &trace_info.trace_information.details {
                if !seen_trace_networks.insert(network_traces.network.clone()) {
                    continue;
                }

                let latest_block = network_traces.cached_provider.get_block_number().await;
                match latest_block {
                    Ok(latest_block) => {
                        let start_block = network_traces.start_block.unwrap_or(latest_block);
                        let end_block = network_traces.end_block.unwrap_or(latest_block);
                        let chain_id = network_traces.cached_provider.chain.id();
                        let syncing_to_block =
                            if latest_block > end_block { end_block } else { latest_block };

                        // Trace indexing runs one shared pipeline per network, so progress uses
                        // the first event ID for that network, matching `start_indexing_traces`.
                        if emitter.is_some() {
                            let network_progress =
                                block_networks.entry(chain_id).or_insert_with(|| {
                                    NetworkBlockProgress {
                                        events: HashMap::new(),
                                        last_emitted_min: U64::ZERO,
                                    }
                                });
                            network_progress
                                .events
                                .entry(trace_info.id.clone())
                                .or_insert(start_block);
                        }

                        events.insert(
                            progress_key(chain_id, &trace_info.id),
                            IndexingEventProgress::running(
                                trace_info.id.clone(),
                                trace_info.contract_name.clone(),
                                trace_info.event_name.clone(),
                                start_block,
                                start_block,
                                syncing_to_block,
                                network_traces.network.clone(),
                                chain_id,
                                network_traces.end_block.is_none(),
                                trace_info.info_log_name(),
                            ),
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to get latest block for tracing network {}: {}",
                            network_traces.network, e
                        );
                    }
                }
            }
        }

        Arc::new(Self {
            events: Mutex::new(events),
            block_networks: Mutex::new(block_networks),
            emitter,
        })
    }

    pub async fn update_last_synced_block(
        &self,
        chain_id: u64,
        id: &str,
        new_last_synced_block: U64,
    ) -> Result<(), SyncError> {
        let mut events = self.events.lock().await;

        let report = Self::update_event(&mut events, chain_id, id, new_last_synced_block)?;

        if let Some(ref emitter) = self.emitter {
            let mut networks = self.block_networks.lock().await;

            let Some(network_progress) = networks.get_mut(&report.chain_id) else {
                debug!("BlockProgress: unknown chain_id {}", report.chain_id);
                return Ok(());
            };

            if let Some(block) = network_progress.events.get_mut(&report.event_id) {
                *block = report.block;
            } else {
                debug!(
                    "BlockProgress: unknown event_id {} on chain {}",
                    report.event_id, report.chain_id
                );
                return Ok(());
            }

            let min_block = network_progress
                .events
                .iter()
                .filter(|(id, _)| {
                    // filter completed events, as they don't advance the block
                    let key = progress_key(report.chain_id, id);
                    match events.get(key.as_str()) {
                        Some(e) => !matches!(e.status, IndexingEventProgressStatus::Completed),
                        None => {
                            warn!(
                                "BlockProgress: event {} missing from events map for chain {}",
                                id, report.chain_id
                            );
                            false
                        }
                    }
                })
                .map(|(_, block)| *block)
                .min();

            let Some(min_block) = min_block else {
                // No events left to process
                return Ok(());
            };

            if min_block > network_progress.last_emitted_min {
                network_progress.last_emitted_min = min_block;
                let chain_id = report.chain_id;
                drop(networks);
                drop(events);
                emitter.emit(RindexerEvent::BlockIndexingCompleted {
                    chain_id,
                    block_number: min_block.to::<u64>(),
                });
            }
        }

        Ok(())
    }

    fn update_event(
        events: &mut HashMap<String, IndexingEventProgress>,
        chain_id: u64,
        id: &str,
        new_last_synced_block: U64,
    ) -> Result<BlockReport, SyncError> {
        let key = progress_key(chain_id, id);
        let event =
            events.get_mut(key.as_str()).ok_or_else(|| SyncError::EventNotFound(key.clone()))?;

        if let IndexingEventProgressStatus::Syncing { progress, syncing_to_block } =
            &mut event.status
        {
            let syncing_to_block = *syncing_to_block;
            if *progress < 10_000 {
                if syncing_to_block > event.last_synced_block {
                    let total_blocks: u64 = syncing_to_block
                        .checked_sub(event.starting_block)
                        .ok_or(SyncError::BlockNumberConversionTotalBlocksError(
                            syncing_to_block,
                            event.starting_block,
                        ))?
                        .try_into()
                        .map_err(|_| {
                            SyncError::BlockNumberConversionTotalBlocksError(
                                syncing_to_block,
                                event.starting_block,
                            )
                        })?;

                    let blocks_synced: u64 = new_last_synced_block
                        .checked_sub(event.starting_block)
                        .ok_or(SyncError::BlockNumberConversionSyncedBlocksError(
                            new_last_synced_block,
                            event.starting_block,
                        ))?
                        .try_into()
                        .map_err(|_| {
                            SyncError::BlockNumberConversionSyncedBlocksError(
                                new_last_synced_block,
                                event.starting_block,
                            )
                        })?;

                    *progress =
                        (blocks_synced.saturating_mul(10_000) / total_blocks).min(10_000) as u16;
                }

                if new_last_synced_block >= syncing_to_block {
                    info!("{}::{} - 100.00% progress", event.info_log, event.network,);
                    event.status = if event.live_indexing {
                        IndexingEventProgressStatus::Live
                    } else {
                        IndexingEventProgressStatus::Completed
                    };
                } else {
                    info!(
                        "{}::{} - {:.2}% progress",
                        event.info_log,
                        event.network,
                        *progress as f64 / 100.0
                    );
                }
            }
        }

        event.last_synced_block = new_last_synced_block;

        Ok(BlockReport {
            chain_id: event.chain_id,
            event_id: event.id.clone(),
            block: new_last_synced_block,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::U64;

    use crate::events::{RindexerEventEmitter, RindexerEventStream};

    /// Helper: build an `IndexingEventsProgressState` with both `events` and
    /// `block_networks` populated so that `update_last_synced_block` exercises
    /// the full event-update → block-progress → emission path.
    fn test_state_with_events(
        emitter: RindexerEventEmitter,
        event_defs: Vec<(&str, u64, U64, U64)>, // (id, chain_id, start_block, syncing_to_block)
    ) -> IndexingEventsProgressState {
        let mut events: HashMap<String, IndexingEventProgress> = HashMap::new();
        let mut block_networks: HashMap<u64, NetworkBlockProgress> = HashMap::new();

        for (id, chain_id, start_block, syncing_to_block) in &event_defs {
            events.insert(
                progress_key(*chain_id, id),
                IndexingEventProgress::running(
                    id.to_string(),
                    "Contract".to_string(),
                    id.to_string(),
                    *start_block,
                    *start_block,
                    *syncing_to_block,
                    format!("chain_{chain_id}"),
                    *chain_id,
                    false,
                    format!("Contract::{id}"),
                ),
            );
            let np = block_networks.entry(*chain_id).or_insert_with(|| NetworkBlockProgress {
                events: HashMap::new(),
                last_emitted_min: U64::ZERO,
            });
            np.events.insert(id.to_string(), *start_block);
        }

        IndexingEventsProgressState {
            events: Mutex::new(events),
            block_networks: Mutex::new(block_networks),
            emitter: Some(emitter),
        }
    }

    fn assert_block_event(
        rx: &mut tokio::sync::broadcast::Receiver<RindexerEvent>,
        expected_chain_id: u64,
        expected_block: u64,
    ) {
        let event = rx.try_recv().expect("expected a BlockIndexingCompleted event");
        match event {
            RindexerEvent::BlockIndexingCompleted { chain_id, block_number } => {
                assert_eq!(chain_id, expected_chain_id);
                assert_eq!(block_number, expected_block);
            }
            _ => panic!("Expected BlockIndexingCompleted"),
        }
    }

    #[tokio::test]
    async fn test_emits_only_when_min_advances() {
        let stream = RindexerEventStream::new();
        let mut rx = stream.subscribe();
        let emitter = RindexerEventEmitter::from_stream(stream);

        let state = test_state_with_events(
            emitter,
            vec![
                ("event_a", 1, U64::from(0), U64::from(200)),
                ("event_b", 1, U64::from(0), U64::from(200)),
            ],
        );

        // Event A advances to 100, event B still at 0 -> min is 0, no emission
        state.update_last_synced_block(1, "event_a", U64::from(100)).await.unwrap();
        assert!(rx.try_recv().is_err());

        // Event B advances to 50 -> min advances from 0 to 50
        state.update_last_synced_block(1, "event_b", U64::from(50)).await.unwrap();
        assert_block_event(&mut rx, 1, 50);
    }

    #[tokio::test]
    async fn test_different_networks_are_independent() {
        let stream = RindexerEventStream::new();
        let mut rx = stream.subscribe();
        let emitter = RindexerEventEmitter::from_stream(stream);

        let state = test_state_with_events(
            emitter,
            vec![
                ("eth_event", 1, U64::from(0), U64::from(200)),
                ("arb_event", 42161, U64::from(0), U64::from(1000)),
            ],
        );

        // Eth event advances -> single event on chain 1, so min advances immediately
        state.update_last_synced_block(1, "eth_event", U64::from(100)).await.unwrap();
        assert_block_event(&mut rx, 1, 100);

        // Arb event advances independently
        state.update_last_synced_block(42161, "arb_event", U64::from(500)).await.unwrap();
        assert_block_event(&mut rx, 42161, 500);
    }

    #[tokio::test]
    async fn test_different_start_blocks() {
        let stream = RindexerEventStream::new();
        let mut rx = stream.subscribe();
        let emitter = RindexerEventEmitter::from_stream(stream);

        let state = test_state_with_events(
            emitter,
            vec![
                ("event_a", 1, U64::from(0), U64::from(10000)),
                ("event_b", 1, U64::from(5000), U64::from(10000)),
            ],
        );

        // event_a advances to 1000, event_b still at 5000 -> min is 1000
        state.update_last_synced_block(1, "event_a", U64::from(1000)).await.unwrap();
        assert_block_event(&mut rx, 1, 1000);
    }

    #[tokio::test]
    async fn test_same_network_contract_tracks_events_separately() {
        let stream = RindexerEventStream::new();
        let mut rx = stream.subscribe();
        let emitter = RindexerEventEmitter::from_stream(stream);

        let state = test_state_with_events(
            emitter,
            vec![
                ("event_a", 1, U64::from(0), U64::from(200)),
                ("event_b", 1, U64::from(0), U64::from(200)),
            ],
        );

        // Only event_a at 100, event_b still at 0 -> no emission
        state.update_last_synced_block(1, "event_a", U64::from(100)).await.unwrap();
        assert!(rx.try_recv().is_err());

        // Both at 100 -> min advances to 100
        state.update_last_synced_block(1, "event_b", U64::from(100)).await.unwrap();
        assert_block_event(&mut rx, 1, 100);
    }

    #[test]
    fn test_trace_progress_uses_first_event_id_per_network() {
        let mut events: HashMap<String, IndexingEventProgress> = HashMap::new();
        let mut block_networks: HashMap<u64, NetworkBlockProgress> = HashMap::new();
        let processor_id = "event_a".to_string();
        let chain_id: u64 = 1;
        let key = progress_key(chain_id, &processor_id);

        let network_progress = block_networks.entry(chain_id).or_insert_with(|| {
            NetworkBlockProgress { events: HashMap::new(), last_emitted_min: U64::ZERO }
        });
        network_progress.events.entry(processor_id.clone()).or_insert(U64::from(10));

        events.entry(key.clone()).or_insert_with(|| {
            IndexingEventProgress::running(
                processor_id.clone(),
                "EvmTraces".to_string(),
                "TraceEvents".to_string(),
                U64::from(10),
                U64::from(10),
                U64::from(100),
                "mainnet".to_string(),
                chain_id,
                true,
                "Indexer::TraceEvents".to_string(),
            )
        });

        network_progress.events.entry(processor_id.clone()).or_insert(U64::from(10));

        assert_eq!(events.len(), 1);
        assert_eq!(events.get(&key).unwrap().id, processor_id);
        assert_eq!(block_networks.len(), 1);
        assert_eq!(block_networks.get(&chain_id).unwrap().events.len(), 1);
        assert_eq!(
            block_networks.get(&chain_id).unwrap().events.get(&processor_id),
            Some(&U64::from(10))
        );
    }

    #[tokio::test]
    async fn test_same_event_id_different_chains() {
        let stream = RindexerEventStream::new();
        let mut rx = stream.subscribe();
        let emitter = RindexerEventEmitter::from_stream(stream);

        // Same event ID on two different chains - previously this would cause the
        // second insert to overwrite the first in the events HashMap.
        let state = test_state_with_events(
            emitter,
            vec![
                ("shared_event", 1, U64::from(0), U64::from(200)),
                ("shared_event", 42161, U64::from(0), U64::from(1000)),
            ],
        );

        // Chain 1 update should succeed (not EventNotFound)
        state.update_last_synced_block(1, "shared_event", U64::from(100)).await.unwrap();
        assert_block_event(&mut rx, 1, 100);

        // Chain 42161 update should also succeed independently
        state.update_last_synced_block(42161, "shared_event", U64::from(500)).await.unwrap();
        assert_block_event(&mut rx, 42161, 500);
    }
}
