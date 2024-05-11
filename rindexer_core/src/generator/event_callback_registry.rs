use std::str::FromStr;
use std::{any::Any, sync::Arc};

use futures::future::BoxFuture;

use crate::helpers::{parse_hex, u256_to_hex};
use ethers::addressbook::Address;
use ethers::prelude::{Filter, RetryClient};
use ethers::types::BigEndianHash;
use ethers::utils::keccak256;
use ethers::{
    providers::{Http, Provider},
    types::{Bytes, Log, H256, U256, U64},
};
use serde::{Deserialize, Serialize};

type Decoder = Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync>;

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
pub enum AddressOrFilter {
    Address(String),
    Filter(FilterDetails),
}

impl AddressOrFilter {
    pub fn is_filter(&self) -> bool {
        matches!(self, AddressOrFilter::Filter(_))
    }
}

#[derive(Clone)]
pub struct NetworkContract {
    pub network: String,

    pub address_or_filter: AddressOrFilter,

    pub provider: &'static Arc<Provider<RetryClient<Http>>>,

    pub decoder: Decoder,

    pub start_block: Option<u64>,

    pub end_block: Option<u64>,

    pub polling_every: Option<u64>,
}

impl NetworkContract {
    pub fn decode_log(&self, log: Log) -> Arc<dyn Any + Send + Sync> {
        (self.decoder)(log.topics, log.data)
    }
}

#[derive(Clone)]
pub struct ContractInformation {
    pub name: String,
    pub details: Vec<NetworkContract>,
    pub abi: String,
}

#[derive(Debug)]
pub struct TxInformation {
    pub network: String,

    pub address: Address,

    pub block_hash: Option<H256>,

    pub block_number: Option<U64>,

    pub transaction_hash: Option<H256>,

    pub transaction_index: Option<U64>,

    pub log_index: Option<U256>,

    pub transaction_log_index: Option<U256>,

    pub log_type: Option<String>,

    pub removed: Option<bool>,
}

pub struct EventResult {
    pub decoded_data: Arc<dyn Any + Send + Sync>,
    pub tx_information: TxInformation,
}

impl EventResult {
    pub fn new(network_contract: Arc<NetworkContract>, log: &Log) -> Self {
        Self {
            decoded_data: network_contract.decode_log(log.clone()),
            tx_information: TxInformation {
                network: network_contract.network.to_string(),
                address: log.address,
                block_hash: log.block_hash,
                block_number: log.block_number,
                transaction_hash: log.transaction_hash,
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                transaction_log_index: log.transaction_log_index,
                log_type: log.log_type.clone(),
                removed: log.removed,
            },
        }
    }
}

pub struct EventInformation {
    pub topic_id: &'static str,
    pub contract: ContractInformation,
    pub callback: Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync>,
}

impl Clone for EventInformation {
    fn clone(&self) -> Self {
        EventInformation {
            topic_id: self.topic_id,
            contract: self.contract.clone(),
            callback: Arc::clone(&self.callback),
        }
    }
}

#[derive(Clone)]
pub struct EventCallbackRegistry {
    pub events: Vec<EventInformation>,
}

impl EventCallbackRegistry {
    pub fn new() -> Self {
        EventCallbackRegistry { events: Vec::new() }
    }

    pub fn find_event(&self, topic_id: &'static str) -> Option<&EventInformation> {
        self.events.iter().find(|e| e.topic_id == topic_id)
    }

    pub fn register_event(&mut self, event: EventInformation) {
        self.events.push(event);
    }

    pub async fn trigger_event(&self, topic_id: &'static str, data: Vec<EventResult>) {
        if let Some(callback) = self.find_event(topic_id).map(|e| &e.callback) {
            callback(data).await;
        } else {
            println!(
                "EventCallbackRegistry: No event found for topic_id: {}",
                topic_id
            );
        }
    }

    pub fn complete(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}
