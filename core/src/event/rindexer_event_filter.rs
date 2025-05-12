use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, B256, U64},
    rpc::types::{Filter, ValueOrArray},
};

use crate::event::contract_setup::{AddressDetails, FactoryDetails, FilterDetails};
use crate::event::factory_event_filter_sync::{get_factory_deployed_addresses};

#[derive(thiserror::Error, Debug)]
pub enum BuildRindexerFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,
}

pub trait EventFilter {
    fn get_to_block(&self) -> U64;
    fn get_from_block(&self) -> U64;
    fn set_from_block(self, block: U64) -> Self;
    fn set_to_block(self, block: U64) -> Self;
    async fn contract_address(&self) -> Option<ValueOrArray<Address>>;
    async fn raw_filter(&self) -> Filter;
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
}

impl EventFilter for SimpleEventFilter {
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

    async fn raw_filter(&self) -> Filter {
        self.filter.clone()
    }
}

#[derive(Clone, Debug)]
struct FactoryFilter {
    // config: FactorySyncConfig<'a>,
    topic_id: B256,
    factory_details: FactoryDetails,
    current_block: U64,
    next_block: U64,
}

impl EventFilter for FactoryFilter {
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
        unimplemented!()
        // let result = get_factory_deployed_addresses(&self.config).await;
        //
        // Some(result.into())
    }

    async fn raw_filter(&self) -> Filter {
        let addresses = self.contract_address().await.expect("Contract addresses should be provided for factory filter");

        Filter::new()
            .address(addresses)
            .event_signature(self.topic_id)
            .from_block(self.current_block)
            .to_block(self.next_block)
    }
}

#[derive(Clone, Debug)]
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

    pub fn new_factory_filter(
        topic_id: &B256,
        _: &str,
        filter_details: &FilterDetails,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
       panic!("Not implemented")
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

    pub async fn raw_filter(&self) -> Filter {
        match self {
            RindexerEventFilter::Address(filter) => filter.raw_filter().await,
            RindexerEventFilter::Filter(filter) => filter.raw_filter().await,
            RindexerEventFilter::Factory(filter) => filter.raw_filter().await,
        }
    }
}
