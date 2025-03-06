use std::{
    hash::{Hash, Hasher},
    sync::Arc,
};

use colored::{ColoredString, Colorize};
use ethers::types::U64;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::event::callback_registry::EventCallbackRegistryInformation;

#[derive(Clone, Debug, Hash)]
pub enum IndexingEventProgressStatus {
    Syncing,
    Live,
    Completed,
    Failed,
}

impl IndexingEventProgressStatus {
    fn as_str(&self) -> &str {
        match self {
            Self::Syncing => "SYNCING",
            Self::Live => "LIVE",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }

    pub fn log(&self) -> ColoredString {
        self.as_str().green()
    }
}

#[derive(Clone, Debug)]
pub struct IndexingEventProgress {
    pub id: String,
    pub contract_name: String,
    pub event_name: String,
    pub starting_block: U64,
    pub last_synced_block: U64,
    pub syncing_to_block: U64,
    pub network: String,
    pub live_indexing: bool,
    pub status: IndexingEventProgressStatus,
    pub progress: f64,
    pub info_log: String,
}

impl Hash for IndexingEventProgress {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.contract_name.hash(state);
        self.event_name.hash(state);
        self.last_synced_block.hash(state);
        self.syncing_to_block.hash(state);
        self.network.hash(state);
        self.live_indexing.hash(state);
        self.status.hash(state);
        let progress_int = (self.progress * 1_000.0) as u64;
        progress_int.hash(state);
    }
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
        live_indexing: bool,
        info_log: String,
    ) -> Self {
        Self {
            id,
            contract_name,
            event_name,
            starting_block,
            last_synced_block,
            syncing_to_block,
            network,
            live_indexing,
            status: IndexingEventProgressStatus::Syncing,
            progress: 0.0,
            info_log,
        }
    }
}

pub struct IndexingEventsProgressState {
    pub events: Vec<IndexingEventProgress>,
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

impl IndexingEventsProgressState {
    pub async fn monitor(
        event_information: &Vec<EventCallbackRegistryInformation>,
    ) -> Arc<Mutex<IndexingEventsProgressState>> {
        let mut events = Vec::new();
        for event_info in event_information {
            for network_contract in &event_info.contract.details {
                let latest_block = network_contract.cached_provider.get_block_number().await;
                match latest_block {
                    Ok(latest_block) => {
                        let start_block = network_contract.start_block.unwrap_or(latest_block);
                        let end_block = network_contract.end_block.unwrap_or(latest_block);

                        events.push(IndexingEventProgress::running(
                            network_contract.id.to_string(),
                            event_info.contract.name.clone(),
                            event_info.event_name.to_string(),
                            start_block,
                            start_block,
                            if latest_block > end_block { end_block } else { latest_block },
                            network_contract.network.clone(),
                            network_contract.end_block.is_none(),
                            event_info.info_log_name(),
                        ));
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

        Arc::new(Mutex::new(Self { events }))
    }

    pub fn update_last_synced_block(
        &mut self,
        id: &str,
        new_last_synced_block: U64,
    ) -> Result<(), SyncError> {
        for event in &mut self.events {
            if event.id == id {
                if event.progress < 1.0 {
                    if event.syncing_to_block > event.last_synced_block {
                        let total_blocks: u64 = event
                            .syncing_to_block
                            .checked_sub(event.starting_block)
                            .ok_or(SyncError::BlockNumberConversionTotalBlocksError(
                                event.syncing_to_block,
                                event.starting_block,
                            ))?
                            .try_into()
                            .map_err(|_| {
                                SyncError::BlockNumberConversionTotalBlocksError(
                                    event.syncing_to_block,
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

                        // Calculate progress based on the proportion of total blocks synced so far
                        event.progress = (blocks_synced as f64) / (total_blocks as f64);
                        event.progress = event.progress.clamp(0.0, 1.0);
                    }

                    if new_last_synced_block >= event.syncing_to_block {
                        event.progress = 1.0;
                        info!(
                            "{} - network {} - {:.2}% progress",
                            event.info_log,
                            event.network,
                            event.progress * 100.0
                        );
                        event.status = if event.live_indexing {
                            IndexingEventProgressStatus::Live
                        } else {
                            IndexingEventProgressStatus::Completed
                        };
                    }

                    if event.progress != 1.0 {
                        info!(
                            "{} - network {} - {:.2}% progress",
                            event.info_log,
                            event.network,
                            event.progress * 100.0
                        );
                    }
                }

                event.last_synced_block = new_last_synced_block;
                return Ok(());
            }
        }

        Err(SyncError::EventNotFound(id.to_string()))
    }
}
