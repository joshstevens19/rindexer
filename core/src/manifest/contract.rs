use std::{borrow::Cow, collections::HashSet, fs, path::Path};

use alloy::rpc::types::Topic;
use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};
use crate::event::contract_setup::FactoryDetails;
use crate::helpers::parse_topic;
use crate::{
    event::contract_setup::{
        AddressDetails, ContractEventMapping, FilterDetails, IndexingContractSetup,
    },
    helpers::get_full_path,
    manifest::{chat::ChatConfig, stream::StreamsConfig},
    types::single_or_array::StringOrArray,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventInputIndexedFilters {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_1: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_2: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_3: Option<Vec<String>>,
}

impl From<EventInputIndexedFilters> for [Topic; 3] {
    fn from(input: EventInputIndexedFilters) -> Self {
        let mut topics: [Topic; 3] = Default::default();

        if let Some(indexed_1) = &input.indexed_1 {
            topics[0] = indexed_1.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }
        if let Some(indexed_2) = &input.indexed_2 {
            topics[1] = indexed_2.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }
        if let Some(indexed_3) = &input.indexed_3 {
            topics[2] = indexed_3.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }

        topics
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterDetailsYaml {
    pub event_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FactoryDetailsYaml {
    pub name: String,

    pub address: ValueOrArray<Address>,

    pub event_name: String,

    pub input_name: ValueOrArray<String>,

    pub abi: String,
}

impl FactoryDetailsYaml {
    pub fn input_names(&self) -> Vec<String> {
        match &self.input_name {
            ValueOrArray::Value(name) => vec![name.clone()],
            ValueOrArray::Array(names) => names.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractDetails {
    pub network: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<ValueOrArray<Address>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<ValueOrArray<FilterDetailsYaml>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_filters: Option<Vec<EventInputIndexedFilters>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factory: Option<FactoryDetailsYaml>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub start_block: Option<U64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub end_block: Option<U64>,
}

impl ContractDetails {
    pub fn indexing_contract_setup(&self, project_path: &Path) -> IndexingContractSetup {
        if let Some(address) = &self.address {
            IndexingContractSetup::Address(AddressDetails {
                address: address.clone(),
                indexed_filters: self.indexed_filters.clone(),
            })
        } else if let Some(factory) = &self.factory {
            IndexingContractSetup::Factory(
                FactoryDetails::from_abi(
                    project_path,
                    factory.abi.clone(),
                    factory.name.clone(),
                    factory.address.clone(),
                    factory.event_name.clone(),
                    factory.input_name.clone(),
                    self.indexed_filters.clone(),
                )
                .unwrap_or_else(|_| panic!("Could not parse ABI from path: {}", factory.abi)),
            )
        } else if let Some(filter) = &self.filter {
            match filter {
                ValueOrArray::Value(filter) => IndexingContractSetup::Filter(FilterDetails {
                    events: ValueOrArray::Value(filter.event_name.clone()),
                    indexed_filters: self.indexed_filters.as_ref().and_then(|f| f.first().cloned()),
                }),
                ValueOrArray::Array(filters) => IndexingContractSetup::Filter(FilterDetails {
                    events: ValueOrArray::Array(
                        filters.iter().map(|f| f.event_name.clone()).collect(),
                    ),
                    indexed_filters: self.indexed_filters.as_ref().and_then(|f| f.first().cloned()),
                }),
            }
        } else {
            panic!("Contract details must have an address, factory or filter");
        }
    }

    pub fn address(&self) -> Option<&ValueOrArray<Address>> {
        if let Some(address) = &self.address {
            return Some(address);
        }
        None
    }

    pub fn new_with_address(
        network: String,
        address: ValueOrArray<Address>,
        indexed_filters: Option<Vec<EventInputIndexedFilters>>,
        start_block: Option<U64>,
        end_block: Option<U64>,
    ) -> Self {
        Self {
            network,
            address: Some(address),
            filter: None,
            indexed_filters,
            factory: None,
            start_block,
            end_block,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SimpleEventOrContractEvent {
    SimpleEvent(String),
    ContractEvent(ContractEventMapping),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyEventTreeYaml {
    pub events: Vec<SimpleEventOrContractEvent>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub then: Option<Box<DependencyEventTreeYaml>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyEventTree {
    pub contract_events: Vec<ContractEventMapping>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub then: Option<Box<DependencyEventTree>>,
}

impl DependencyEventTree {
    pub fn collect_dependency_events(&self) -> Vec<ContractEventMapping> {
        let mut dependencies = Vec::new();

        dependencies.extend(self.contract_events.clone());

        if let Some(children) = &self.then {
            dependencies.extend(children.collect_dependency_events());
        }

        dependencies
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ContractEventDeserializer {
    String(String),
    Struct(ContractEvent),
}

fn deserialize_events<'de, D>(deserializer: D) -> Result<Option<Vec<ContractEvent>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let defs = Vec::<ContractEventDeserializer>::deserialize(deserializer)?;
    Ok(Some(
        defs.into_iter()
            .map(|def| match def {
                ContractEventDeserializer::String(s) => ContractEvent { name: s, timestamps: None },
                ContractEventDeserializer::Struct(ev) => ev,
            })
            .collect(),
    ))
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ContractEvent {
    /// The name of the event.
    pub name: String,
    /// Enable or disable timestamps for the event. This will override the global timestamp
    /// setting with either the true or false state if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamps: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,

    pub details: Vec<ContractDetails>,

    pub abi: StringOrArray,

    #[serde(
        default,
        deserialize_with = "deserialize_events",
        skip_serializing_if = "Option::is_none"
    )]
    pub include_events: Option<Vec<ContractEvent>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_event_in_order: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_events: Option<DependencyEventTreeYaml>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorg_safe_distance: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<StreamsConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat: Option<ChatConfig>,
}

#[derive(thiserror::Error, Debug)]
pub enum ParseAbiError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(String),

    #[error("Could not get full path: {0}")]
    CouldNotGetFullPath(#[from] std::io::Error),

    #[error("Invalid ABI format: {0}")]
    InvalidAbiFormat(String),

    #[error("Could not merge ABI: {0}")]
    CouldNotMergeAbis(#[from] serde_json::Error),
}

impl Contract {
    pub fn override_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn parse_abi(&self, project_path: &Path) -> Result<String, ParseAbiError> {
        match &self.abi {
            StringOrArray::Single(abi_path) => {
                let full_path = get_full_path(project_path, abi_path)?;
                let abi_str = fs::read_to_string(full_path)?;
                Ok(abi_str)
            }
            StringOrArray::Multiple(abis) => {
                let mut unique_entries = HashSet::new();
                let mut merged_abi_value = Vec::new();

                for abi_path in abis {
                    let full_path = get_full_path(project_path, abi_path)?;
                    let abi_str = fs::read_to_string(full_path)?;
                    let abi_value: Value = serde_json::from_str(&abi_str)?;

                    if let Value::Array(abi_arr) = abi_value {
                        for entry in abi_arr {
                            let entry_str = serde_json::to_string(&entry)?;
                            if unique_entries.insert(entry_str) {
                                merged_abi_value.push(entry);
                            }
                        }
                    } else {
                        return Err(ParseAbiError::InvalidAbiFormat(format!(
                            "Expected an array but got a single value: {abi_value}"
                        )));
                    }
                }

                let merged_abi_str = serde_json::to_string(&json!(merged_abi_value))?;
                Ok(merged_abi_str)
            }
        }
    }

    pub fn convert_dependency_event_tree_yaml(
        &self,
        yaml: DependencyEventTreeYaml,
    ) -> DependencyEventTree {
        DependencyEventTree {
            contract_events: yaml
                .events
                .into_iter()
                .map(|event| match event {
                    SimpleEventOrContractEvent::ContractEvent(contract_event) => contract_event,
                    SimpleEventOrContractEvent::SimpleEvent(event_name) => {
                        ContractEventMapping { contract_name: self.name.clone(), event_name }
                    }
                })
                .collect(),
            then: yaml
                .then
                .map(|then_event| Box::new(self.convert_dependency_event_tree_yaml(*then_event))),
        }
    }

    pub fn is_filter(&self) -> bool {
        let filter_count = self.details.iter().filter(|details| details.filter.is_some()).count();

        if filter_count > 0 && filter_count != self.details.len() {
            // panic as this should never happen as validation has already happened
            panic!("Cannot mix and match address and filter for the same contract definition.");
        }

        filter_count > 0
    }

    fn contract_name_to_filter_name(&self) -> String {
        format!("{}Filter", self.name)
    }

    pub fn raw_name(&self) -> String {
        if self.is_filter() {
            self.name.split("Filter").collect::<Vec<&str>>()[0].to_string()
        } else {
            self.name.clone()
        }
    }

    pub fn before_modify_name_if_filter_readonly(&'_ self) -> Cow<'_, str> {
        if self.is_filter() {
            Cow::Owned(self.contract_name_to_filter_name())
        } else {
            Cow::Borrowed(&self.name)
        }
    }

    pub fn identify_and_modify_filter(&mut self) -> bool {
        if self.is_filter() {
            self.override_name(self.contract_name_to_filter_name());
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_contract_include_events_simple() {
        let yaml = r#"
            name: ERC20
            abi: ./abis/ERC20.abi.json
            details:
              - network: ethereum
                start_block: 20090000
                filter:
                  - event_name: Transfer
                  - event_name: Approval
            include_events:
              - Transfer
              - Approval
        "#;

        let contract: Contract = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            contract.include_events,
            Some(vec![
                ContractEvent { name: "Transfer".to_string(), timestamps: None },
                ContractEvent { name: "Approval".to_string(), timestamps: None }
            ])
        );
    }

    #[test]
    fn test_contract_include_events_complex() {
        let yaml = r#"
            name: ERC20
            abi: ./abis/ERC20.abi.json
            details:
              - network: ethereum
                start_block: 20090000
                filter:
                  - event_name: Transfer
                  - event_name: Approval
            include_events:
              - name: Transfer
                timestamps: true
              - name: Approval
                timestamps: false
        "#;

        let contract: Contract = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            contract.include_events,
            Some(vec![
                ContractEvent { name: "Transfer".to_string(), timestamps: Some(true) },
                ContractEvent { name: "Approval".to_string(), timestamps: Some(false) }
            ])
        );
    }
}
