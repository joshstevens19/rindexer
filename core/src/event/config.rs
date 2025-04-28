use std::{path::PathBuf, sync::Arc};

use alloy::primitives::{B256, U64};
use tokio::sync::{Mutex, Semaphore};

use crate::{
    event::{
        callback_registry::{
            EventCallbackRegistry, EventResult, TraceCallbackRegistry, TraceResult,
        },
        contract_setup::NetworkContract,
        BuildRindexerFilterError, RindexerEventFilter,
    },
    indexer::IndexingEventsProgressState,
    manifest::{native_transfer::TraceProcessingMethod, storage::CsvDetails},
    PostgresClient,
};

pub struct EventProcessingConfig {
    pub id: String,
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub info_log_name: String,
    pub topic_id: B256,
    pub event_name: String,
    pub network_contract: Arc<NetworkContract>,
    pub start_block: U64,
    pub end_block: U64,
    pub semaphore: Arc<Semaphore>,
    pub registry: Arc<EventCallbackRegistry>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub stream_last_synced_block_file_path: Option<String>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}

impl EventProcessingConfig {
    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        RindexerEventFilter::new(
            &self.topic_id,
            &self.event_name,
            &self.network_contract.indexing_contract_setup,
            self.start_block,
            self.end_block,
        )
    }

    pub async fn trigger_event(&self, fn_data: Vec<EventResult>) {
        self.registry.trigger_event(&self.id, fn_data).await;
    }
}

#[derive(Clone)]
pub struct TraceProcessingConfig {
    pub id: String,
    pub project_path: PathBuf,
    pub start_block: U64,
    pub end_block: U64,
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub network: String,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub registry: Arc<TraceCallbackRegistry>,
    pub method: TraceProcessingMethod,
    pub stream_last_synced_block_file_path: Option<String>,
}

impl TraceProcessingConfig {
    pub async fn trigger_event(&self, fn_data: Vec<TraceResult>) {
        self.registry.trigger_event(&self.id, fn_data).await;
    }
}
