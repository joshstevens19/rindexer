use ethers::addressbook::Address;
use ethers::contract::LogMeta;
use ethers::prelude::{Filter, RetryClient};
use ethers::types::BigEndianHash;
use ethers::utils::keccak256;
use ethers::{
    providers::{Http, Provider},
    types::{Bytes, Log, H256, U256, U64},
};
use futures::future::BoxFuture;
use log::info;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::{any::Any, sync::Arc};
use tracing::error;

pub type Decoder = Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync>;

/// Details about a factory contract, including its address, event name, parameter name, and ABI.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FactoryDetails {
    pub address: String,

    #[serde(rename = "eventName")]
    pub event_name: String,

    #[serde(rename = "parameterName")]
    pub parameter_name: String,

    pub abi: String,
}

/// Details about a filter, including the event name and optionally indexed parameters.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterDetails {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_1: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_2: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_3: Option<Vec<String>>,
}

/// Parses a topic string into an `H256` hash.
///
/// # Arguments
///
/// * `input` - The input string to parse.
///
/// # Returns
///
/// An `H256` hash parsed from the input string.
fn parse_topic(input: &str) -> H256 {
    match input.to_lowercase().as_str() {
        "true" => H256::from_low_u64_be(1),
        "false" => H256::from_low_u64_be(0),
        _ => {
            if let Ok(address) = Address::from_str(input) {
                H256::from(address)
            } else if let Ok(num) = U256::from_dec_str(input) {
                H256::from_uint(&num)
            } else {
                H256::from(keccak256(input))
            }
        }
    }
}

impl FilterDetails {
    /// Extends a filter with indexed parameters.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter to extend.
    ///
    /// # Returns
    ///
    /// The extended filter.
    pub fn extend_filter_indexed(&self, mut filter: Filter) -> Filter {
        if let Some(indexed_1) = &self.indexed_1 {
            filter = filter.topic1(indexed_1.iter().map(|i| parse_topic(i)).collect::<Vec<_>>());
        }
        if let Some(indexed_2) = &self.indexed_2 {
            filter = filter.topic2(indexed_2.iter().map(|i| parse_topic(i)).collect::<Vec<_>>());
        }
        if let Some(indexed_3) = &self.indexed_3 {
            filter = filter.topic3(indexed_3.iter().map(|i| parse_topic(i)).collect::<Vec<_>>());
        }
        filter
    }
}

#[derive(Clone)]
pub enum IndexingContractSetup {
    Address(String),
    Filter(FilterDetails),
    Factory(FactoryDetails),
}

impl IndexingContractSetup {
    /// Checks if the contract setup is a filter.
    ///
    /// # Returns
    ///
    /// `true` if it is a filter, `false` otherwise.
    pub fn is_filter(&self) -> bool {
        matches!(self, IndexingContractSetup::Filter(_))
    }
}

/// Represents a contract on a specific network with its setup and associated provider.
#[derive(Clone)]
pub struct NetworkContract {
    pub id: String,
    pub network: String,
    pub indexing_contract_setup: IndexingContractSetup,
    pub provider: Arc<Provider<RetryClient<Http>>>,
    pub decoder: Decoder,
    pub start_block: Option<U64>,
    pub end_block: Option<U64>,
    pub polling_every: Option<u64>,
}

impl NetworkContract {
    /// Decodes a log using the contract's decoder.
    ///
    /// # Arguments
    ///
    /// * `log` - The log to decode.
    ///
    /// # Returns
    ///
    /// The decoded log as an `Arc<dyn Any + Send + Sync>`.
    pub fn decode_log(&self, log: Log) -> Arc<dyn Any + Send + Sync> {
        (self.decoder)(log.topics, log.data)
    }
}

/// Information about a contract, including its name, details, ABI, and reorganization safety status.
#[derive(Clone)]
pub struct ContractInformation {
    pub name: String,
    pub details: Vec<NetworkContract>,
    pub abi: String,
    pub reorg_safe_distance: bool,
}

/// Transaction-related information for an event.
#[derive(Debug, Clone)]
pub struct TxInformation {
    pub network: String,
    pub address: Address,
    pub block_hash: H256,
    pub block_number: U64,
    pub transaction_hash: H256,
    pub log_index: U256,
    pub transaction_index: U64,
}

/// Result of an event, including decoded data and transaction information.
#[derive(Debug)]
pub struct EventResult {
    pub log: Log,
    pub decoded_data: Arc<dyn Any + Send + Sync>,
    pub tx_information: TxInformation,
}

impl EventResult {
    /// Creates a new `EventResult` from a network contract and log.
    ///
    /// # Arguments
    ///
    /// * `network_contract` - The network contract associated with the event.
    /// * `log` - The log to process.
    ///
    /// # Returns
    ///
    /// A new `EventResult`.
    pub fn new(network_contract: Arc<NetworkContract>, log: &Log) -> Self {
        let log_meta = LogMeta::from(log);
        Self {
            log: log.clone(),
            decoded_data: network_contract.decode_log(log.clone()),
            tx_information: TxInformation {
                network: network_contract.network.to_string(),
                address: log.address,
                block_hash: log_meta.block_hash,
                block_number: log_meta.block_number,
                transaction_hash: log_meta.transaction_hash,
                transaction_index: log_meta.transaction_index,
                log_index: log_meta.log_index,
            },
        }
    }
}

/// Information about an event, including its indexer, topic ID, event name, contract, and callback.
pub struct EventInformation {
    pub indexer_name: String,
    pub topic_id: String,
    pub event_name: String,
    pub contract: ContractInformation,
    pub callback: Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync>,
}

impl EventInformation {
    pub fn info_log_name(&self) -> String {
        format!(
            "{}::{}",
            self.contract.name, self.event_name
        )
    }
}

impl Clone for EventInformation {
    fn clone(&self) -> Self {
        EventInformation {
            indexer_name: self.indexer_name.clone(),
            topic_id: self.topic_id.clone(),
            event_name: self.event_name.clone(),
            contract: self.contract.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

/// Registry for event callbacks.
#[derive(Clone)]
pub struct EventCallbackRegistry {
    pub events: Vec<EventInformation>,
}

impl Default for EventCallbackRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl EventCallbackRegistry {
    /// Creates a new `EventCallbackRegistry`.
    ///
    /// # Returns
    ///
    /// A new `EventCallbackRegistry`.
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new() }
    }

    /// Finds an event by its topic ID.
    ///
    /// # Arguments
    ///
    /// * `topic_id` - The topic ID of the event.
    ///
    /// # Returns
    ///
    /// An optional reference to the `EventInformation` if found.
    pub fn find_event(&self, topic_id: &str) -> Option<&EventInformation> {
        self.events.iter().find(|e| e.topic_id == topic_id)
    }

    /// Registers a new event.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to register.
    pub fn register_event(&mut self, event: EventInformation) {
        self.events.push(event);
    }

    /// Triggers an event asynchronously.
    ///
    /// # Arguments
    ///
    /// * `topic_id` - The topic ID of the event.
    /// * `data` - The event result data.
    pub async fn trigger_event(&self, topic_id: &str, data: Vec<EventResult>) {
        if let Some(event_information) = self.find_event(topic_id) {
            info!(
                "{} - Pushed {} events",
                data.len(),
                event_information.info_log_name()
            );
            (event_information.callback)(data).await;
        } else {
            error!(
                "EventCallbackRegistry: No event found for topic_id: {}",
                topic_id
            );
        }
    }

    /// Completes the registry and returns an `Arc` reference to it.
    ///
    /// # Returns
    ///
    /// An `Arc<Self>` reference to the registry.
    pub fn complete(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}
