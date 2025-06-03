use std::collections::HashSet;
use std::path::{PathBuf};
use std::sync::Arc;
use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::{ValueOrArray},
};
use alloy::rpc::types::{Topic};
use crate::event::contract_setup::{AddressDetails, FilterDetails};
use crate::event::factory_event_filter_sync::{get_known_factory_deployed_addresses, GetKnownFactoryDeployedAddressesParams};
use crate::manifest::storage::CsvDetails;
use crate::PostgresClient;

#[derive(thiserror::Error, Debug)]
pub enum BuildRindexerFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,
}

#[derive(Clone, Debug)]
struct SimpleEventFilter {
    pub address: Option<ValueOrArray<Address>>,
    pub event_signature: B256,
    pub topics: [Topic; 4],
    pub current_block: U64,
    pub next_block: U64,
}

impl SimpleEventFilter {
    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block.into();

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block.into();

        self
    }

    fn contract_address(&self) -> Option<HashSet<Address>> {
        self.address.as_ref().map(|address| match address {
            ValueOrArray::Value(address) => HashSet::from([*address]),
            ValueOrArray::Array(addresses) => addresses.iter().copied().collect()
        })
    }
}

#[derive(Clone)]
pub struct FactoryFilter {
    pub project_path: PathBuf,
    pub factory_address: ValueOrArray<Address>,
    pub factory_contract_name: String,
    pub factory_event_name: String,
    pub factory_input_name: String,
    pub network: String,

    pub event_signature: B256,

    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,

    pub current_block: U64,
    pub next_block: U64,
}

impl FactoryFilter {
    fn get_to_block(&self) -> U64 {
        self.next_block
    }

    fn get_from_block(&self) -> U64 {
        self.current_block
    }

    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block.into();

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block.into();

        self
    }

    async fn contract_address(&self) -> Option<HashSet<Address>> {
         get_known_factory_deployed_addresses(&GetKnownFactoryDeployedAddressesParams {
            project_path: self.project_path.clone(),
            contract_name: self.factory_contract_name.clone(),
            contract_address: self.factory_address.clone(),
            event_name: self.factory_event_name.clone(),
            input_name: self.factory_input_name.clone(),
            network: self.network.clone(),
            database: self.database.clone(),
            csv_details: self.csv_details.clone(),
        }).await.unwrap()
    }
}

#[derive(Clone)]
pub enum RindexerEventFilter {
    Address(SimpleEventFilter),
    Filter(SimpleEventFilter),
    Factory(FactoryFilter),
}

impl RindexerEventFilter {
    pub fn new_address_filter(
        topic_id: &B256,
        event_name: &str,
        address_details: &AddressDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        let index_filter = address_details.indexed_filters.iter().find_map(|indexed_filters| {
            indexed_filters.iter().find(|&n| n.event_name == event_name)
        });

        Ok(RindexerEventFilter::Filter(SimpleEventFilter {
            address: Some(address_details.address.clone()),
            event_signature: *topic_id,
            topics: index_filter.map(|indexed_filter| indexed_filter.clone().into()).unwrap_or_default(),
            current_block,
            next_block,
        }))
    }

    pub fn new_filter(
        topic_id: &B256,
        _: &str,
        filter_details: &FilterDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        Ok(RindexerEventFilter::Filter(SimpleEventFilter {
            address: None,
            event_signature: *topic_id,
            topics: filter_details.clone().indexed_filters.map(|indexed_filter| indexed_filter.clone().into()).unwrap_or_default(),
            current_block,
            next_block,
        }))
    }

    pub fn event_signature(&self) -> B256 {
        match self {
            RindexerEventFilter::Address(filter) => filter.event_signature,
            RindexerEventFilter::Filter(filter) => filter.event_signature,
            RindexerEventFilter::Factory(filter) => filter.event_signature,
        }
    }

    pub fn topic1(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Factory(_) => Default::default(),
        }
    }

    pub fn topic2(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Factory(_) => Default::default(),
        }
    }

    pub fn topic3(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[3].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[3].clone(),
            RindexerEventFilter::Factory(_) => Default::default(),
        }
    }

    pub fn to_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.next_block,
            RindexerEventFilter::Filter(filter) => filter.next_block,
            RindexerEventFilter::Factory(filter) => filter.next_block,
        }
    }

    pub fn from_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.current_block,
            RindexerEventFilter::Filter(filter) => filter.current_block,
            RindexerEventFilter::Factory(filter) => filter.current_block,
        }
    }

    pub fn set_from_block<R: Into<U64>>(mut self, block: R) -> Self {
        let block = block.into();
        match self {
            Self::Address(filter) => Self::Address(filter.set_from_block(block)),
            Self::Filter(filter) => Self::Filter(filter.set_from_block(block)),
            Self::Factory(filter) => Self::Factory(filter.set_from_block(block)),
        }
    }
    pub fn set_to_block<R: Into<U64>>(mut self, block: R) -> Self {
        match self {
            RindexerEventFilter::Address(filter) => RindexerEventFilter::Address(filter.set_to_block(block.into())),
            RindexerEventFilter::Filter(filter) => RindexerEventFilter::Filter(filter.set_to_block(block.into())),
            RindexerEventFilter::Factory(filter) => RindexerEventFilter::Factory(filter.set_to_block(block.into())),
        }
    }

    pub async fn contract_addresses(&self) -> Option<HashSet<Address>> {
        match self {
            RindexerEventFilter::Address(filter) => filter.contract_address(),
            RindexerEventFilter::Filter(filter) =>filter.contract_address(),
            RindexerEventFilter::Factory(filter) => filter.contract_address().await,
        }
    }
}
