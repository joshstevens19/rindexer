use crate::generator::event_callback_registry::EventInformation;
use colored::{ColoredString, Colorize};
use ethers::providers::Middleware;
use ethers::types::U64;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

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

impl IndexingEventsProgressState {
    pub async fn monitor(
        event_information: Vec<EventInformation>,
    ) -> Arc<Mutex<IndexingEventsProgressState>> {
        let mut events = Vec::new();
        for event_info in &event_information {
            for network_contract in &event_info.contract.details {
                // TODO! LOOK at
                let latest_block = network_contract.provider.get_block_number().await.unwrap();
                let end_block = network_contract.end_block.unwrap_or(latest_block);

                events.push(IndexingEventProgress::running(
                    network_contract.id.to_string(),
                    event_info.contract.name.clone(),
                    event_info.event_name.to_string(),
                    network_contract.start_block.unwrap_or(U64::zero()),
                    if latest_block > end_block {
                        end_block
                    } else {
                        latest_block
                    },
                    network_contract.network.clone(),
                    network_contract.end_block.is_none(),
                    event_info.info_log_name(),
                ));
            }
        }

        Arc::new(Mutex::new(Self { events }))
    }

    pub fn update_last_synced_block(&mut self, id: &str, new_last_synced_block: U64) {
        for event in &mut self.events {
            if event.id == id {
                if event.progress != 1.0 {
                    if event.syncing_to_block > event.last_synced_block {
                        let total_blocks = event.syncing_to_block - event.last_synced_block;
                        let blocks_synced =
                            new_last_synced_block.saturating_sub(event.last_synced_block);

                        let effective_blocks_synced =
                            if new_last_synced_block > event.syncing_to_block {
                                total_blocks
                            } else {
                                blocks_synced
                            };

                        event.progress += (effective_blocks_synced.as_u64() as f64)
                            / (total_blocks.as_u64() as f64);
                        event.progress = event.progress.clamp(0.0, 1.0);
                    }

                    if new_last_synced_block >= event.syncing_to_block {
                        event.progress = 1.0;
                        event.status = if event.live_indexing {
                            IndexingEventProgressStatus::Live
                        } else {
                            IndexingEventProgressStatus::Completed
                        };
                    }

                    info!(
                        "{} - network {} - {:.2}% progress",
                        event.info_log,
                        event.network,
                        event.progress * 100.0
                    );
                }

                event.last_synced_block = new_last_synced_block;
                break;
            }
        }
    }
}
