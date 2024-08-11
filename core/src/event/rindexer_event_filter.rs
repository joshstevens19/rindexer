use alloy::{
    eips::BlockNumberOrTag,
    primitives::{Address, BlockNumber, B256, U64},
    rpc::types::{Filter, ValueOrArray},
};

use crate::event::contract_setup::IndexingContractSetup;

#[derive(thiserror::Error, Debug)]
pub enum BuildRindexerFilterError {
    #[error("Address is valid format")]
    AddressInvalidFormat,
}

#[derive(Clone, Debug)]
pub struct RindexerEventFilter {
    filter: Filter,
}

impl RindexerEventFilter {
    fn from_filter(filter: Filter) -> Self {
        if filter.get_to_block().is_none() {
            panic!("Filter must have a to block");
        }
        if filter.get_from_block().is_none() {
            panic!("Filter must have a from block");
        }

        Self { filter }
    }

    pub fn new(
        topic_id: &B256,
        event_name: &str,
        indexing_contract_setup: &IndexingContractSetup,
        current_block: U64,
        next_block: U64,
    ) -> Result<RindexerEventFilter, BuildRindexerFilterError> {
        match indexing_contract_setup {
            IndexingContractSetup::Address(address_details) => {
                match &address_details.indexed_filters {
                    Some(indexed_filters) => {
                        if let Some(index_filters) =
                            indexed_filters.iter().find(|&n| n.event_name == event_name)
                        {
                            return Ok(RindexerEventFilter::from_filter(
                                index_filters.extend_filter_indexed(
                                    Filter::new()
                                        .address(address_details.address.clone())
                                        .event_signature(*topic_id)
                                        .from_block(current_block)
                                        .to_block(next_block),
                                ),
                            ));
                        }

                        Ok(RindexerEventFilter::from_filter(
                            Filter::new()
                                .address(address_details.address.clone())
                                .event_signature(*topic_id)
                                .from_block(current_block)
                                .to_block(next_block),
                        ))
                    }
                    None => Ok(RindexerEventFilter::from_filter(
                        Filter::new()
                            .address(address_details.address.clone())
                            .event_signature(*topic_id)
                            .from_block(current_block)
                            .to_block(next_block),
                    )),
                }
            }
            IndexingContractSetup::Filter(filter) => match &filter.indexed_filters {
                Some(indexed_filters) => Ok(RindexerEventFilter::from_filter(
                    indexed_filters.extend_filter_indexed(
                        Filter::new()
                            .event_signature(*topic_id)
                            .from_block(current_block)
                            .to_block(next_block),
                    ),
                )),
                None => Ok(RindexerEventFilter::from_filter(
                    Filter::new()
                        .event_signature(*topic_id)
                        .from_block(current_block)
                        .to_block(next_block),
                )),
            },
            IndexingContractSetup::Factory(factory) => {
                let address = factory
                    .address
                    .parse::<Address>()
                    .map_err(|_| BuildRindexerFilterError::AddressInvalidFormat)?;

                Ok(RindexerEventFilter::from_filter(
                    Filter::new()
                        .address(address)
                        .event_signature(*topic_id)
                        .from_block(current_block)
                        .to_block(next_block),
                ))
            }
        }
    }

    pub fn get_to_block(&self) -> BlockNumber {
        self.filter
            .get_to_block()
            .expect("impossible to not have a to block in RindexerEventFilter")
    }

    pub fn get_from_block(&self) -> BlockNumber {
        self.filter
            .get_from_block()
            .expect("impossible to not have a from block in RindexerEventFilter")
    }

    pub fn set_from_block<T: Into<BlockNumberOrTag>>(mut self, block: T) -> Self {
        self.filter = self.filter.from_block(block);
        self
    }

    pub fn set_to_block<T: Into<BlockNumberOrTag>>(mut self, block: T) -> Self {
        self.filter = self.filter.to_block(block);
        self
    }

    pub fn contract_address(&self) -> Option<ValueOrArray<Address>> {
        self.filter.address.clone().to_value_or_array()
    }

    pub fn raw_filter(&self) -> &Filter {
        &self.filter
    }
}
