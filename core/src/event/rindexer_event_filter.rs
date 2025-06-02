use std::path::{PathBuf};
use std::sync::Arc;
use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, B256, U64},
    rpc::types::{Filter, ValueOrArray},
};

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
    filter: Filter,
}

impl SimpleEventFilter {
    pub fn from_filter(filter: Filter) -> Self {
        if filter.get_to_block().is_none() {
            panic!("Filter must have a to block");
        }
        if filter.get_from_block().is_none() {
            panic!("Filter must have a from block");
        }

        Self { filter }
    }

    fn get_to_block(&self) -> U64 {
        U64::from(
            self.filter
                .get_to_block()
                .expect("impossible to not have a to block in RindexerEventFilter"),
        )
    }

    fn get_from_block(&self) -> U64 {
        U64::from(
            self.filter
                .get_from_block()
                .expect("impossible to not have a from block in RindexerEventFilter"),
        )
    }

    fn set_from_block(mut self, block: U64) -> Self {
        self.filter = self.filter.from_block(BlockNumberOrTag::Number(block.as_limbs()[0]));
        self
    }

    fn set_to_block(mut self, block: U64) -> Self {
        self.filter = self.filter.to_block(BlockNumberOrTag::Number(block.as_limbs()[0]));
        self
    }

    async fn contract_address(&self) -> Option<ValueOrArray<Address>> {
        let address_filter = self.filter.address.clone();
        address_filter.to_value_or_array()
    }

    async fn rpc_request_filter(&self) -> Option<Filter> {
        Some(self.filter.clone())
    }
}


#[derive(Clone)]
pub struct FactoryFilter {
    pub project_path: PathBuf,
    pub factory_address: ValueOrArray<Address>,
    pub factory_contract_name: String,
    pub factory_event_name: String,
    pub network: String,

    pub topic_id: B256,

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

    async fn contract_address(&self) -> Option<ValueOrArray<Address>> {
        let result = get_known_factory_deployed_addresses(&GetKnownFactoryDeployedAddressesParams {
            project_path: self.project_path.clone(),
            contract_name: self.factory_contract_name.clone(),
            contract_address: self.factory_address.clone(),
            event_name: self.factory_event_name.clone(),
            network: self.network.clone(),
            database: self.database.clone(),
            csv_details: self.csv_details.clone(),
        }).await.unwrap();

        result.map(Into::into)
    }

    async fn rpc_request_filter(&self) -> Option<Filter> {
        let addresses = self.contract_address().await?;

        Some(Filter::new()
            .address(addresses)
            .event_signature(self.topic_id)
            .from_block(self.current_block)
            .to_block(self.next_block))
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
        match &address_details.indexed_filters {
            Some(indexed_filters) => {
                if let Some(index_filters) =
                    indexed_filters.iter().find(|&n| n.event_name == event_name)
                {
                    return Ok(RindexerEventFilter::Filter(SimpleEventFilter::from_filter(index_filters.extend_filter_indexed(
                            Filter::new()
                                .address(address_details.address.clone())
                                .event_signature(*topic_id)
                                .from_block(current_block)
                                .to_block(next_block))))
                    );
                }


                Ok(RindexerEventFilter::Filter(SimpleEventFilter::from_filter(Filter::new()
                            .address(address_details.address.clone())
                            .event_signature(*topic_id)
                            .from_block(current_block)
                            .to_block(next_block)))
                )
            }
            None => Ok(RindexerEventFilter::Filter(SimpleEventFilter::from_filter(Filter::new()
                    .address(address_details.address.clone())
                    .event_signature(*topic_id)
                    .from_block(current_block)
                    .to_block(next_block)))
            )
        }
    }

    pub fn new_filter(
        topic_id: &B256,
        _: &str,
        filter_details: &FilterDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match &filter_details.indexed_filters {
            Some(indexed_filters) => Ok(RindexerEventFilter::Filter (SimpleEventFilter::from_filter( indexed_filters.extend_filter_indexed(
                       Filter::new()
                       .event_signature(*topic_id)
                       .from_block(current_block)
                       .to_block(next_block))))),
            None => Ok(RindexerEventFilter::Filter( SimpleEventFilter::from_filter(Filter::new()
                    .event_signature(*topic_id)
                    .from_block(current_block)
                    .to_block(next_block))))
        }
    }

    pub fn get_to_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.get_to_block(),
            RindexerEventFilter::Filter(filter) => filter.get_to_block(),
            RindexerEventFilter::Factory(filter) => filter.get_to_block(),
        }
    }

    pub fn get_from_block(&self) -> U64 {
        match self {
            RindexerEventFilter::Address(filter) => filter.get_from_block(),
            RindexerEventFilter::Filter(filter) => filter.get_from_block(),
            RindexerEventFilter::Factory(filter) => filter.get_from_block(),
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

    pub async fn contract_address(&self) -> Option<ValueOrArray<Address>> {
        match self {
            RindexerEventFilter::Address(filter) => filter.contract_address().await,
            RindexerEventFilter::Filter(filter) => filter.contract_address().await,
            RindexerEventFilter::Factory(filter) => filter.contract_address().await,
        }
    }

    pub async fn rpc_request_filter(&self) -> Option<Filter> {
        match self {
            RindexerEventFilter::Address(filter) => filter.rpc_request_filter().await,
            RindexerEventFilter::Filter(filter) => filter.rpc_request_filter().await,
            RindexerEventFilter::Factory(filter) => filter.rpc_request_filter().await,
        }
    }
}
