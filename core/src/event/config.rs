use std::{path::PathBuf, sync::Arc};

use ethers::prelude::U64;
use tokio::sync::{Mutex, Semaphore};

use crate::{
    event::{callback_registry::EventCallbackRegistry, contract_setup::NetworkContract},
    indexer::IndexingEventsProgressState,
    manifest::storage::CsvDetails,
    PostgresClient,
};

pub struct EventProcessingConfig {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub info_log_name: String,
    pub topic_id: String,
    pub event_name: String,
    pub network_contract: Arc<NetworkContract>,
    pub start_block: U64,
    pub end_block: U64,
    pub semaphore: Arc<Semaphore>,
    pub registry: Arc<EventCallbackRegistry>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}
