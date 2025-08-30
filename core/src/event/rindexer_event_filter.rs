use crate::event::contract_setup::{AddressDetails, FilterDetails};
use crate::event::factory_event_filter_sync::{
    get_known_factory_deployed_addresses, GetKnownFactoryDeployedAddressesParams,
};
use crate::manifest::storage::CsvDetails;
use crate::PostgresClient;
use alloy::rpc::types::Topic;
use alloy::{
    primitives::{Address, B256, U64},
    rpc::types::ValueOrArray,
};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum BuildRindexerFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,
}

#[derive(Clone, Debug)]
pub struct SimpleEventFilter {
    pub address: Option<ValueOrArray<Address>>,
    pub topic_id: B256,
    pub topics: [Topic; 3],
    pub current_block: U64,
    pub next_block: U64,
}

impl SimpleEventFilter {
    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block;

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block;

        self
    }

    fn contract_address(&self) -> Option<HashSet<Address>> {
        self.address.as_ref().map(|address| match address {
            ValueOrArray::Value(address) => HashSet::from([*address]),
            ValueOrArray::Array(addresses) => addresses.iter().copied().collect(),
        })
    }
}

#[derive(Clone)]
pub struct FactoryFilter {
    pub project_path: PathBuf,
    pub indexer_name: String,
    pub factory_address: ValueOrArray<Address>,
    pub factory_contract_name: String,
    pub factory_event_name: String,
    pub factory_input_name: ValueOrArray<String>,
    pub network: String,

    pub topic_id: B256,
    pub topics: [Topic; 3],

    pub database: Option<Arc<PostgresClient>>,
    pub csv_details: Option<CsvDetails>,

    pub current_block: U64,
    pub next_block: U64,
}

impl std::fmt::Debug for FactoryFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FactoryFilter")
            .field("project_path", &self.project_path)
            .field("indexer_name", &self.indexer_name)
            .field("factory_address", &self.factory_address)
            .field("factory_contract_name", &self.factory_contract_name)
            .field("factory_event_name", &self.factory_event_name)
            .field("factory_input_names", &self.factory_input_name)
            .field("network", &self.network)
            .field("topic_id", &self.topic_id)
            .field("current_block", &self.current_block)
            .field("next_block", &self.next_block)
            .finish()
    }
}

impl FactoryFilter {
    fn set_from_block(mut self, block: U64) -> Self {
        self.current_block = block;

        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.next_block = block;

        self
    }

    async fn contract_address(&self) -> Option<HashSet<Address>> {
        let input_names = match &self.factory_input_name {
            ValueOrArray::Value(name) => vec![name.clone()],
            ValueOrArray::Array(names) => names.clone(),
        };

        get_known_factory_deployed_addresses(&GetKnownFactoryDeployedAddressesParams {
            project_path: self.project_path.clone(),
            indexer_name: self.indexer_name.clone(),
            contract_name: self.factory_contract_name.clone(),
            event_name: self.factory_event_name.clone(),
            input_names,
            network: self.network.clone(),
            database: self.database.clone(),
            csv_details: self.csv_details.clone(),
        })
        .await
        .expect("Failed to get known factory deployed addresses")
    }
}

#[derive(Debug, Clone)]
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
            topic_id: *topic_id,
            topics: index_filter
                .map(|indexed_filter| indexed_filter.clone().into())
                .unwrap_or_default(),
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
            topic_id: *topic_id,
            topics: filter_details
                .clone()
                .indexed_filters
                .map(|indexed_filter| indexed_filter.clone().into())
                .unwrap_or_default(),
            current_block,
            next_block,
        }))
    }

    pub fn event_signature(&self) -> B256 {
        match self {
            RindexerEventFilter::Address(filter) => filter.topic_id,
            RindexerEventFilter::Filter(filter) => filter.topic_id,
            RindexerEventFilter::Factory(filter) => filter.topic_id,
        }
    }

    pub fn topic1(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[0].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[0].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[0].clone(),
        }
    }

    pub fn topic2(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[1].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[1].clone(),
        }
    }

    pub fn topic3(&self) -> Topic {
        match self {
            RindexerEventFilter::Address(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Filter(filter) => filter.topics[2].clone(),
            RindexerEventFilter::Factory(filter) => filter.topics[2].clone(),
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

    pub fn set_from_block<R: Into<U64>>(self, block: R) -> Self {
        let block = block.into();
        match self {
            Self::Address(filter) => Self::Address(filter.set_from_block(block)),
            Self::Filter(filter) => Self::Filter(filter.set_from_block(block)),
            Self::Factory(filter) => Self::Factory(filter.set_from_block(block)),
        }
    }
    pub fn set_to_block<R: Into<U64>>(self, block: R) -> Self {
        match self {
            RindexerEventFilter::Address(filter) => {
                RindexerEventFilter::Address(filter.set_to_block(block.into()))
            }
            RindexerEventFilter::Filter(filter) => {
                RindexerEventFilter::Filter(filter.set_to_block(block.into()))
            }
            RindexerEventFilter::Factory(filter) => {
                RindexerEventFilter::Factory(filter.set_to_block(block.into()))
            }
        }
    }

    pub async fn contract_addresses(&self) -> Option<HashSet<Address>> {
        match self {
            RindexerEventFilter::Address(filter) => filter.contract_address(),
            RindexerEventFilter::Filter(filter) => filter.contract_address(),
            RindexerEventFilter::Factory(filter) => filter.contract_address().await,
        }
    }
}
