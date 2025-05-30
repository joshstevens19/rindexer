use std::{path::PathBuf, sync::Arc};
use alloy::json_abi::Event;
use alloy::primitives::{Address, B256, U64};
use alloy::rpc::types::ValueOrArray;
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
use crate::event::contract_setup::{AddressDetails, IndexingContractSetup};
use crate::event::factory_event_filter_sync::update_known_factory_deployed_addresses;
use crate::event::rindexer_event_filter::FactoryFilter;
use crate::manifest::contract::EventInputIndexedFilters;

pub struct ContractEventProcessingConfig {
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

impl ContractEventProcessingConfig {
    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match &self.network_contract.indexing_contract_setup {
            IndexingContractSetup::Address(details) => {
                RindexerEventFilter::new_address_filter(
                    &self.topic_id,
                    &self.event_name,
                    details,
                    self.start_block,
                    self.end_block,
                )
            }
            IndexingContractSetup::Filter(details) => {
                RindexerEventFilter::new_filter(
                    &self.topic_id,
                    &self.event_name,
                    details,
                    self.start_block,
                    self.end_block,
                )
            }
            IndexingContractSetup::Factory(details) =>
                Ok(RindexerEventFilter::Factory(FactoryFilter {
                    project_path: self.project_path.clone(),
                    factory_contract_name: details.name.clone(),
                    factory_address: details.address.clone(),
                    factory_event_name: details.event_name.clone(),
                    network: self.network_contract.network.clone(),
                    topic_id: self.topic_id.clone(),
                    database: self.database.clone(),
                    csv_details: self.csv_details.clone(),

                    current_block: self.start_block,
                    next_block: self.end_block,
                }))

        }
    }

    pub async fn trigger_event(&self, fn_data: Vec<EventResult>) {
        self.registry.trigger_event(&self.id, fn_data).await;
    }
}

pub struct FactoryEventProcessingConfig {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub contract_name: String,
    pub address: ValueOrArray<Address>,
    pub input_name: String,
    // TODO: Use eventinfo from codebase
    pub event: Event,
    pub network_contract: Arc<NetworkContract>,
    pub start_block: U64,
    pub end_block: U64,
    pub semaphore: Arc<Semaphore>,
    pub progress: Arc<Mutex<IndexingEventsProgressState>>,
    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,
    pub stream_last_synced_block_file_path: Option<String>,
    pub index_event_in_order: bool,
    pub live_indexing: bool,
    pub indexing_distance_from_head: U64,
}

impl FactoryEventProcessingConfig {
    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        let event_name = self.event.name.clone();
        let topic_id = self.event.selector();

        let details = AddressDetails {
            address: self.address.clone(),
            indexed_filters: Some(vec![EventInputIndexedFilters { event_name: event_name.clone(), indexed_1: None, indexed_2: None, indexed_3: None }]),
        };

        RindexerEventFilter::new_address_filter(
            &topic_id,
            &event_name,
            &details,
            self.start_block,
            self.end_block,
        )
    }

    pub async fn trigger_event(&self, events: Vec<EventResult>) {
        update_known_factory_deployed_addresses(self, &events).await;
    }

    pub fn info_log_name(&self) -> String {
        format!("{}::{}", self.contract_name, self.event.name)
    }
}

pub enum EventProcessingConfig {
    ContractEventProcessing(ContractEventProcessingConfig),
    FactoryEventProcessing(FactoryEventProcessingConfig),
}

impl From<ContractEventProcessingConfig> for EventProcessingConfig {
    fn from(config: ContractEventProcessingConfig) -> Self {
        Self::ContractEventProcessing(config)
    }
}

impl From<FactoryEventProcessingConfig> for EventProcessingConfig {
    fn from(config: FactoryEventProcessingConfig) -> Self {
        Self::FactoryEventProcessing(config)
    }
}

impl EventProcessingConfig {
    pub fn topic_id(&self) -> B256 {
        match self {
            Self::ContractEventProcessing(config) => config.topic_id.clone(),
            Self::FactoryEventProcessing(config) => config.event.selector().clone(),
        }
    }

    pub fn info_log_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.info_log_name.clone(),
            Self::FactoryEventProcessing(config) => config.info_log_name(),
        }
    }

    pub fn network_contract(&self) -> Arc<NetworkContract> {
        match self {
            Self::ContractEventProcessing(config) => config.network_contract.clone(),
            Self::FactoryEventProcessing(config) => config.network_contract.clone(),
        }
    }

    pub fn index_event_in_order(&self) -> bool {
        match self {
            Self::ContractEventProcessing(config) => config.index_event_in_order,
            Self::FactoryEventProcessing(config) => config.index_event_in_order,
        }
    }

    pub fn contract_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.contract_name.clone(),
            Self::FactoryEventProcessing(config) =>  config.contract_name.clone(),
        }
    }

    pub fn indexer_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.indexer_name.clone(),
            Self::FactoryEventProcessing(config) => config.indexer_name.clone(),
        }
    }

    pub fn event_name(&self) -> String {
        match self {
            Self::ContractEventProcessing(config) => config.event_name.clone(),
            Self::FactoryEventProcessing(config) => config.event.name.clone(),
        }
    }

    pub fn semaphore(&self) -> Arc<Semaphore> {
        match self {
            Self::ContractEventProcessing(config) => config.semaphore.clone(),
            Self::FactoryEventProcessing(config) => config.semaphore.clone(),
        }
    }

    pub fn live_indexing(&self) -> bool {
        match self {
            Self::ContractEventProcessing(config) => config.live_indexing,
            Self::FactoryEventProcessing(config) => config.live_indexing,
        }
    }

    pub fn indexing_distance_from_head(&self) -> U64 {
        match self {
            Self::ContractEventProcessing(config) => config.indexing_distance_from_head,
            Self::FactoryEventProcessing(config) => config.indexing_distance_from_head,
        }
    }

    pub fn progress(&self) -> Arc<Mutex<IndexingEventsProgressState>> {
        match self {
            Self::ContractEventProcessing(config) => config.progress.clone(),
            Self::FactoryEventProcessing(config) => config.progress.clone(),
        }
    }

    pub fn database(&self) -> Option<Arc<PostgresClient>> {
        match self {
            Self::ContractEventProcessing(config) => config.database.clone(),
            Self::FactoryEventProcessing(config) => config.database.clone(),
        }
    }

    pub fn csv_details(&self) -> Option<CsvDetails> {
        match self {
            Self::ContractEventProcessing(config) => config.csv_details.clone(),
            Self::FactoryEventProcessing(config) => config.csv_details.clone(),
        }
    }

    pub fn stream_last_synced_block_file_path(&self) -> Option<String> {
        match self {
            Self::ContractEventProcessing(config) => config.stream_last_synced_block_file_path.clone(),
            Self::FactoryEventProcessing(config) => config.stream_last_synced_block_file_path.clone(),
        }
    }

    pub fn project_path(&self) -> PathBuf {
        match self {
            Self::ContractEventProcessing(config) => config.project_path.clone(),
            Self::FactoryEventProcessing(config) => config.project_path.clone(),
        }
    }

    pub fn to_event_filter(&self) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match self {
            Self::ContractEventProcessing(config) => config.to_event_filter(),
            Self::FactoryEventProcessing(config) => config.to_event_filter()
        }
    }

    pub async fn trigger_event(&self, fn_data: Vec<EventResult>) {
        match self {
            Self::ContractEventProcessing(config) => config.trigger_event(fn_data).await,
            Self::FactoryEventProcessing(config) => config.trigger_event(fn_data).await,
        }
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
