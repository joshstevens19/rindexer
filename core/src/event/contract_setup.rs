use std::{any::Any, sync::Arc};

use ethers::{
    addressbook::Address,
    prelude::{Log, ValueOrArray, U64},
};
use serde::{Deserialize, Serialize};

use crate::{
    event::callback_registry::Decoder,
    generate_random_id,
    manifest::contract::{Contract, EventInputIndexedFilters},
    provider::{CreateNetworkProvider, JsonRpcCachedProvider},
    types::single_or_array::StringOrArray,
};

#[derive(Clone)]
pub struct NetworkContract {
    pub id: String,
    pub network: String,
    pub indexing_contract_setup: IndexingContractSetup,
    pub cached_provider: Arc<JsonRpcCachedProvider>,
    pub decoder: Decoder,
    pub start_block: Option<U64>,
    pub end_block: Option<U64>,
    pub disable_logs_bloom_checks: bool,
}

impl NetworkContract {
    pub fn decode_log(&self, log: Log) -> Arc<dyn Any + Send + Sync> {
        (self.decoder)(log.topics, log.data)
    }

    pub fn is_live_indexing(&self) -> bool {
        self.end_block.is_none()
    }
}

#[derive(Clone)]
pub struct ContractInformation {
    pub name: String,
    pub details: Vec<NetworkContract>,
    pub abi: StringOrArray,
    pub reorg_safe_distance: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateContractInformationError {
    #[error("Can not find network {0} from providers")]
    CanNotFindNetworkFromProviders(String),
}

impl ContractInformation {
    pub fn create(
        contract: &Contract,
        network_providers: &[CreateNetworkProvider],
        decoder: Decoder,
    ) -> Result<ContractInformation, CreateContractInformationError> {
        let mut details = vec![];
        for c in &contract.details {
            let provider = network_providers.iter().find(|item| item.network_name == *c.network);

            match provider {
                None => {
                    return Err(CreateContractInformationError::CanNotFindNetworkFromProviders(
                        c.network.clone(),
                    ));
                }
                Some(provider) => {
                    details.push(NetworkContract {
                        id: generate_random_id(10),
                        network: c.network.clone(),
                        cached_provider: Arc::clone(&provider.client),
                        decoder: Arc::clone(&decoder),
                        indexing_contract_setup: c.indexing_contract_setup(),
                        start_block: c.start_block,
                        end_block: c.end_block,
                        disable_logs_bloom_checks: provider.disable_logs_bloom_checks,
                    });
                }
            }
        }

        Ok(ContractInformation {
            name: contract.name.clone(),
            details,
            abi: contract.abi.clone(),
            reorg_safe_distance: contract.reorg_safe_distance.unwrap_or_default(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ContractEventMapping {
    pub contract_name: String,
    pub event_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AddressDetails {
    pub address: ValueOrArray<Address>,

    pub indexed_filters: Option<Vec<EventInputIndexedFilters>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FactoryDetails {
    pub address: String,

    #[serde(rename = "eventName")]
    pub event_name: String,

    #[serde(rename = "parameterName")]
    pub parameter_name: String,

    pub abi: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterDetails {
    pub events: ValueOrArray<String>,

    pub indexed_filters: Option<EventInputIndexedFilters>,
}

#[derive(Clone)]
pub enum IndexingContractSetup {
    Address(AddressDetails),
    Filter(FilterDetails),
    Factory(FactoryDetails),
}

impl IndexingContractSetup {
    pub fn is_filter(&self) -> bool {
        matches!(self, IndexingContractSetup::Filter(_))
    }
}
