use alloy_primitives::{map::HashSet, Address as AlloyAddress, B256};
use alloy_rpc_types::{
    BlockNumberOrTag as AlloyBlockNumberOrTag, Filter as AlloyFilter,
    FilterBlockOption as AlloyBlockOption, FilterSet, ValueOrArray as AlloyValueOrArray,
};
use ethers::{
    addressbook::Address,
    prelude::{BlockNumber, Filter, H256, U64},
    types::ValueOrArray,
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
        topic_id: &H256,
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
                                        .topic0(*topic_id)
                                        .from_block(current_block)
                                        .to_block(next_block),
                                ),
                            ));
                        }

                        Ok(RindexerEventFilter::from_filter(
                            Filter::new()
                                .address(address_details.address.clone())
                                .topic0(*topic_id)
                                .from_block(current_block)
                                .to_block(next_block),
                        ))
                    }
                    None => Ok(RindexerEventFilter::from_filter(
                        Filter::new()
                            .address(address_details.address.clone())
                            .topic0(*topic_id)
                            .from_block(current_block)
                            .to_block(next_block),
                    )),
                }
            }
            IndexingContractSetup::Filter(filter) => match &filter.indexed_filters {
                Some(indexed_filters) => Ok(RindexerEventFilter::from_filter(
                    indexed_filters.extend_filter_indexed(
                        Filter::new()
                            .topic0(*topic_id)
                            .from_block(current_block)
                            .to_block(next_block),
                    ),
                )),
                None => Ok(RindexerEventFilter::from_filter(
                    Filter::new().topic0(*topic_id).from_block(current_block).to_block(next_block),
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
                        .topic0(*topic_id)
                        .from_block(current_block)
                        .to_block(next_block),
                ))
            }
        }
    }

    pub fn get_to_block(&self) -> U64 {
        self.filter
            .get_to_block()
            .expect("impossible to not have a to block in RindexerEventFilter")
    }

    pub fn get_from_block(&self) -> U64 {
        self.filter
            .get_from_block()
            .expect("impossible to not have a from block in RindexerEventFilter")
    }

    pub fn set_from_block<T: Into<BlockNumber>>(mut self, block: T) -> Self {
        self.filter = self.filter.from_block(block);
        self
    }

    pub fn set_to_block<T: Into<BlockNumber>>(mut self, block: T) -> Self {
        self.filter = self.filter.to_block(block);
        self
    }

    pub fn contract_address(&self) -> Option<ValueOrArray<Address>> {
        self.filter.address.clone()
    }

    pub fn raw_filter(&self) -> &Filter {
        &self.filter
    }

    pub fn to_alloy_filter(&self) -> AlloyFilter {
        let ethers_filter = self.raw_filter();

        // Convert block options
        let block_option = {
            let from_block = self.get_from_block();
            let to_block = self.get_to_block();

            AlloyBlockOption::Range {
                from_block: Some(AlloyBlockNumberOrTag::Number(from_block.as_u64())),
                to_block: Some(AlloyBlockNumberOrTag::Number(to_block.as_u64())),
            }
        };

        // Convert ValueOrArray<EthersAddress> to AlloyValueOrArray<AlloyAddress>
        let address = match &ethers_filter.address {
            Some(ValueOrArray::Value(addr)) => {
                let address_bytes: [u8; 20] = addr.0;
                AlloyValueOrArray::Value(AlloyAddress::from(address_bytes))
            }
            Some(ValueOrArray::Array(addrs)) => AlloyValueOrArray::Array(
                addrs
                    .iter()
                    .map(|addr| {
                        let address_bytes: [u8; 20] = addr.0;
                        AlloyAddress::from(address_bytes)
                    })
                    .collect(),
            ),
            None => AlloyValueOrArray::Value(AlloyAddress::default()),
        };

        let address = FilterSet::from(address);

        // Convert topics: [Option<ValueOrArray<Option<EthersH256>>; 4] to [ValueOrArray<B256>; 4]
        let topics_value_or_array: [AlloyValueOrArray<B256>; 4] =
            ethers_filter.topics.clone().map(|topic| match topic {
                Some(ValueOrArray::Value(Some(h256))) => {
                    AlloyValueOrArray::Value(B256::from(h256.0))
                }
                Some(ValueOrArray::Value(None)) => AlloyValueOrArray::Value(Default::default()),
                Some(ValueOrArray::Array(arr)) => AlloyValueOrArray::Array(
                    arr.iter()
                        .filter_map(|opt_h256| opt_h256.as_ref().map(|h256| B256::from(h256.0)))
                        .collect(),
                ),
                None => AlloyValueOrArray::Value(Default::default()),
            });

        let topics = topics_value_or_array.map(|topic| FilterSet::from(topic));

        AlloyFilter { block_option, address, topics }
    }
}
