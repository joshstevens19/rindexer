use crate::helpers::get_full_path;
use crate::{
    event::callback_registry::Decoder,
    generate_random_id,
    indexer::native_transfer::EVENT_NAME,
    manifest::{
        contract::{Contract, EventInputIndexedFilters},
        native_transfer::{NativeTransfers, TraceProcessingMethod},
    },
    provider::{
        get_network_provider, get_network_provider_with_notifications, CreateNetworkProvider,
        JsonRpcCachedProvider,
    },
    types::single_or_array::StringOrArray,
};
use alloy::json_abi::{Event, JsonAbi};
use alloy::{
    primitives::{Address, Log, U64},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};
use serde_json::Error;
use std::path::Path;
use std::{any::Any, fs, sync::Arc};

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
        (self.decoder)(log.topics().to_vec(), log.data.data)
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
        project_path: &Path,
        contract: &Contract,
        network_providers: &mut [CreateNetworkProvider],
        decoder: Decoder,
    ) -> Result<ContractInformation, CreateContractInformationError> {
        let mut details = vec![];
        for c in &contract.details {
            let provider_info =
                get_network_provider_with_notifications(&c.network, network_providers);

            match provider_info {
                None => {
                    return Err(CreateContractInformationError::CanNotFindNetworkFromProviders(
                        c.network.clone(),
                    ));
                }
                Some((client, disable_logs_bloom_checks, _state_notifications)) => {
                    details.push(NetworkContract {
                        id: generate_random_id(10),
                        network: c.network.clone(),
                        cached_provider: client,
                        decoder: Arc::clone(&decoder),
                        indexing_contract_setup: c.indexing_contract_setup(project_path),
                        start_block: c.start_block,
                        end_block: c.end_block,
                        disable_logs_bloom_checks,
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

#[derive(Clone)]
pub struct NetworkTrace {
    pub id: String,
    pub network: String,
    pub cached_provider: Arc<JsonRpcCachedProvider>,
    pub start_block: Option<U64>,
    pub end_block: Option<U64>,
    pub method: TraceProcessingMethod,
}

impl NetworkTrace {
    pub fn is_live_indexing(&self) -> bool {
        self.end_block.is_none()
    }
}

#[derive(Clone)]
pub struct TraceInformation {
    pub name: String,
    pub details: Vec<NetworkTrace>,
    pub reorg_safe_distance: bool,
}

impl TraceInformation {
    pub fn create(
        native_transfers: NativeTransfers,
        network_providers: &[CreateNetworkProvider],
    ) -> Result<TraceInformation, CreateContractInformationError> {
        let mut details = vec![];
        let trace_networks = native_transfers.networks.unwrap_or_default();

        for n in &trace_networks {
            let name = n.network.clone();
            let provider = get_network_provider(&name, network_providers);

            match provider {
                None => {
                    return Err(CreateContractInformationError::CanNotFindNetworkFromProviders(
                        name,
                    ));
                }
                Some(provider) => {
                    details.push(NetworkTrace {
                        id: generate_random_id(10),
                        network: name,
                        cached_provider: Arc::clone(&provider.client),
                        start_block: n.start_block,
                        end_block: n.end_block,
                        method: n.method,
                    });
                }
            }
        }

        Ok(TraceInformation {
            name: EVENT_NAME.to_string(),
            details,
            reorg_safe_distance: native_transfers.reorg_safe_distance.unwrap_or_default(),
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

#[derive(thiserror::Error, Debug)]
pub enum FactoryDetailsFromAbiError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),

    #[error(transparent)]
    ABIParsingError(#[from] Error),

    #[error("Can not find event {0}")]
    EventNotFoundError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactoryDetails {
    pub contract_name: String,
    pub address: ValueOrArray<Address>,
    pub input_name: String,
    pub event: Event,
    pub indexed_filters: Option<Vec<EventInputIndexedFilters>>,
}

impl FactoryDetails {
    pub fn from_abi(
        project_path: &Path,
        abi: String,
        contract_name: String,
        address: ValueOrArray<Address>,
        event_name: String,
        input_name: String,
        indexed_filters: Option<Vec<EventInputIndexedFilters>>,
    ) -> Result<FactoryDetails, FactoryDetailsFromAbiError> {
        let full_path = get_full_path(project_path, &abi)?;
        let abi_str = fs::read_to_string(full_path)?;
        let abi: JsonAbi = serde_json::from_str(&abi_str)?;

        let event = abi
            .event(&event_name)
            .and_then(|v| v.first())
            .ok_or(FactoryDetailsFromAbiError::EventNotFoundError(event_name.clone()))?
            .clone();

        Ok(FactoryDetails { contract_name, address, input_name, event, indexed_filters })
    }
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

    pub fn factory_details(&self) -> Option<FactoryDetails> {
        match self {
            IndexingContractSetup::Factory(details) => Some(details.clone()),
            _ => None,
        }
    }
}
