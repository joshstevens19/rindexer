use ethers::addressbook::Address;
use ethers::prelude::{Filter, ValueOrArray};
use ethers::types::U64;
use regex::{Captures, Regex};
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::generator::event_callback_registry::{
    AddressDetails, FilterDetails, IndexingContractSetup,
};
use crate::generator::read_abi_items;
use crate::helpers::replace_env_variable_to_raw_name;
use crate::indexer::{parse_topic, ContractEventMapping, Indexer};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_yaml::Value;

pub const YAML_CONFIG_NAME: &str = "rindexer.yaml";

fn default_storage() -> Storage {
    Storage::default()
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum ProjectType {
    Rust,
    NoCode,
}

fn deserialize_project_type<'de, D>(deserializer: D) -> Result<ProjectType, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Value = Deserialize::deserialize(deserializer)?;
    match value {
        Value::String(s) => match s.as_str() {
            "rust" => Ok(ProjectType::Rust),
            "no-code" => Ok(ProjectType::NoCode),
            _ => Err(serde::de::Error::custom(format!(
                "Unknown project type: {}",
                s
            ))),
        },
        _ => Err(serde::de::Error::custom("Invalid project type format")),
    }
}

fn serialize_project_type<S>(value: &ProjectType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let string_value = match value {
        ProjectType::Rust => "rust",
        ProjectType::NoCode => "no-code",
    };
    serializer.serialize_str(string_value)
}

fn deserialize_option_u64_from_string<'de, D>(deserializer: D) -> Result<Option<U64>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(string) => U64::from_dec_str(&string)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

fn serialize_option_u64_as_string<S>(value: &Option<U64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match *value {
        Some(ref u64_value) => serializer.serialize_some(&u64_value.as_u64().to_string()),
        None => serializer.serialize_none(),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,

    #[serde(deserialize_with = "deserialize_project_type")]
    #[serde(serialize_with = "serialize_project_type")]
    pub project_type: ProjectType,

    pub networks: Vec<Network>,

    #[serde(default = "default_storage")]
    pub storage: Storage,

    pub contracts: Vec<Contract>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<Global>,
}

impl Manifest {
    pub fn to_indexer(&self) -> Indexer {
        Indexer {
            name: self.name.clone(),
            contracts: self.contracts.clone(),
        }
    }
}

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

impl EventInputIndexedFilters {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterDetailsYaml {
    pub event_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractDetails {
    pub network: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<ValueOrArray<Address>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    filter: Option<FilterDetailsYaml>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    indexed_filters: Option<Vec<EventInputIndexedFilters>>,

    // #[serde(default, skip_serializing_if = "Option::is_none")]
    // factory: Option<FactoryDetails>,
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
    pub fn indexing_contract_setup(&self) -> IndexingContractSetup {
        if let Some(address) = &self.address {
            IndexingContractSetup::Address(AddressDetails {
                address: address.clone(),
                indexed_filters: self.indexed_filters.clone(),
            })
        // } else if let Some(factory) = &self.factory {
        //     IndexingContractSetup::Factory(factory.clone())
        } else if let Some(filter) = &self.filter {
            IndexingContractSetup::Filter(FilterDetails {
                event_name: filter.event_name.clone(),
                indexed_filters: self
                    .indexed_filters
                    .as_ref()
                    .and_then(|f| f.first().cloned()),
            })
        } else {
            panic!("Contract details must have an address, factory or filter");
        }
    }

    pub fn address(&self) -> Option<&ValueOrArray<Address>> {
        if let Some(address) = &self.address {
            return Some(address);
        }
        // } else if let Some(factory) = &self.factory {
        //     Some(&factory.address.parse::<Address>().into())
        // } else {
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
            //factory: None,
            start_block,
            end_block,
        }
    }

    pub fn new_with_filter(
        network: String,
        filter: FilterDetailsYaml,
        indexed_filters: Option<Vec<EventInputIndexedFilters>>,
        start_block: Option<U64>,
        end_block: Option<U64>,
    ) -> Self {
        Self {
            network,
            address: None,
            filter: Some(filter),
            indexed_filters,
            //factory: None,
            start_block,
            end_block,
        }
    }

    // pub fn new_with_factory(
    //     network: String,
    //     factory: FactoryDetails,
    //     start_block: Option<U64>,
    //     end_block: Option<U64>,
    // ) -> Self {
    //     Self {
    //         network,
    //         address: None,
    //         filter: None,
    //         factory: Some(factory),
    //         start_block,
    //         end_block,
    //     }
    // }
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,

    pub details: Vec<ContractDetails>,

    pub abi: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_events: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_event_in_order: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_events: Option<DependencyEventTreeYaml>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorg_safe_distance: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,
}

impl Contract {
    pub fn override_name(&mut self, name: String) {
        self.name = name;
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
                    SimpleEventOrContractEvent::SimpleEvent(event_name) => ContractEventMapping {
                        contract_name: self.name.clone(),
                        event_name: event_name.clone(),
                    },
                })
                .collect(),
            then: yaml
                .then
                .map(|then_event| Box::new(self.convert_dependency_event_tree_yaml(*then_event))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u32,

    pub rpc: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub contract_name: String,

    pub event_name: String,

    pub event_input_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKeys {
    pub contract_name: String,

    pub event_name: String,

    pub event_input_name: String,

    #[serde(rename = "linked_to")]
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresDetails {
    pub enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationships: Option<Vec<ForeignKeys>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_create_tables: Option<bool>,
}

fn default_csv_path() -> String {
    "./generated_csv".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CsvDetails {
    pub enabled: bool,

    #[serde(default = "default_csv_path")]
    pub path: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_create_headers: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Storage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresDetails>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv: Option<CsvDetails>,
}

impl Storage {
    pub fn postgres_enabled(&self) -> bool {
        match &self.postgres {
            Some(details) => details.enabled,
            None => false,
        }
    }

    pub fn postgres_disable_create_tables(&self) -> bool {
        let enabled = self.postgres_enabled();
        if !enabled {
            return true;
        }

        self.postgres.as_ref().map_or(false, |details| {
            details.disable_create_tables.unwrap_or_default()
        })
    }

    pub fn csv_enabled(&self) -> bool {
        match &self.csv {
            Some(details) => details.enabled,
            None => false,
        }
    }

    pub fn csv_disable_create_headers(&self) -> bool {
        let enabled = self.csv_enabled();
        if !enabled {
            return true;
        }

        self.csv.as_ref().map_or(false, |details| {
            details.disable_create_headers.unwrap_or_default()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Global {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<Contract>>,
}

fn substitute_env_variables(contents: &str) -> Result<String, regex::Error> {
    let re = Regex::new(r"\$<([^>]+)>")?;
    let result = re.replace_all(contents, |caps: &Captures| {
        let var_name = &caps[1];
        match env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                panic!("Environment variable {} not found", var_name)
            }
        }
    });
    Ok(result.into_owned())
}

#[derive(thiserror::Error, Debug)]
pub enum ValidateManifestError {
    #[error("Invalid network mapped to contract: network - {0} contract - {1}")]
    InvalidNetworkMappedToContract(String, String),

    #[error("Invalid filter event name {0} for contract {1} does not exist in ABI")]
    InvalidFilterEventNameDoesntExistInABI(String, String),

    #[error("Could not read or parse ABI for contract {0} with path {1}")]
    InvalidABI(String, String),

    #[error("Event {0} included in include_events for contract {1} not found in ABI")]
    EventIncludedNotFoundInABI(String, String),

    #[error("Event {0} not found in ABI for contract {1}")]
    IndexedFilterEventNotFoundInABI(String, String),

    #[error("Indexed filter defined more than allowed for event {0} for contract {1} - indexed expected: {2} defined: {3}")]
    IndexedFilterDefinedMoreThanAllowed(String, String, usize, usize),

    #[error("Relationship contract {0} not found")]
    RelationshipContractNotFound(String),

    #[error("Relationship foreign key contract {0} not found")]
    RelationshipForeignKeyContractNotFound(String),
}

fn validate_manifest(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<(), ValidateManifestError> {
    for contract in &manifest.contracts {
        let events = read_abi_items(project_path, contract)
            .map_err(|e| ValidateManifestError::InvalidABI(contract.name.clone(), e.to_string()))?;

        for detail in &contract.details {
            let has_network = manifest.networks.iter().any(|n| n.name == detail.network);
            if !has_network {
                return Err(ValidateManifestError::InvalidNetworkMappedToContract(
                    detail.network.clone(),
                    contract.name.clone(),
                ));
            }

            if let Some(address) = &detail.filter {
                if !events.iter().any(|e| e.name == *address.event_name) {
                    return Err(
                        ValidateManifestError::InvalidFilterEventNameDoesntExistInABI(
                            address.event_name.clone(),
                            contract.name.clone(),
                        ),
                    );
                }
            }

            if let Some(indexed_filters) = &detail.indexed_filters {
                for indexed_filter in indexed_filters.iter() {
                    let event = events.iter().find(|e| e.name == indexed_filter.event_name);
                    if let Some(event) = event {
                        let indexed_allowed_length = event
                            .inputs
                            .iter()
                            .filter(|i| i.indexed.unwrap_or(false))
                            .count();
                        let indexed_filter_defined =
                            indexed_filter.indexed_1.as_ref().map_or(0, |_| 1)
                                + indexed_filter.indexed_2.as_ref().map_or(0, |_| 1)
                                + indexed_filter.indexed_3.as_ref().map_or(0, |_| 1);

                        if indexed_filter_defined > indexed_allowed_length {
                            return Err(
                                ValidateManifestError::IndexedFilterDefinedMoreThanAllowed(
                                    indexed_filter.event_name.clone(),
                                    contract.name.clone(),
                                    indexed_allowed_length,
                                    indexed_filter_defined,
                                ),
                            );
                        }
                    } else {
                        return Err(ValidateManifestError::IndexedFilterEventNotFoundInABI(
                            indexed_filter.event_name.clone(),
                            contract.name.clone(),
                        ));
                    }
                }
            }
        }

        if let Some(include_events) = &contract.include_events {
            for event in include_events {
                if !events.iter().any(|e| e.name == *event) {
                    return Err(ValidateManifestError::EventIncludedNotFoundInABI(
                        event.clone(),
                        contract.name.clone(),
                    ));
                }
            }
        }

        if let Some(_dependency_events) = &contract.dependency_events {
            // TODO - validate the events all exist in the contract ABIs
        }
    }

    if let Some(postgres) = &manifest.storage.postgres {
        if let Some(relationships) = &postgres.relationships {
            for relationship in relationships {
                if !manifest
                    .contracts
                    .iter()
                    .any(|c| c.name == relationship.contract_name)
                {
                    return Err(ValidateManifestError::RelationshipContractNotFound(
                        relationship.contract_name.clone(),
                    ));
                }

                if !relationship
                    .foreign_keys
                    .iter()
                    .any(|fk| fk.contract_name == relationship.contract_name)
                {
                    return Err(
                        ValidateManifestError::RelationshipForeignKeyContractNotFound(
                            relationship.contract_name.clone(),
                        ),
                    );
                }

                // TODO - Add validation for the event names and event inputs match the ABIs
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ReadManifestError {
    #[error("Could not open file: {0}")]
    CouldNotOpenFile(std::io::Error),

    #[error("Could not read file: {0}")]
    CouldNotReadFile(std::io::Error),

    #[error("Could not parse manifest: {0}")]
    CouldNotParseManifest(serde_yaml::Error),

    #[error("Could not substitute env variables: {0}")]
    CouldNotSubstituteEnvVariables(regex::Error),

    #[error("Could not validate manifest: {0}")]
    CouldNotValidateManifest(ValidateManifestError),
}

pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, ReadManifestError> {
    let mut file = File::open(file_path).map_err(ReadManifestError::CouldNotOpenFile)?;
    let mut contents = String::new();

    file.read_to_string(&mut contents)
        .map_err(ReadManifestError::CouldNotReadFile)?;

    let manifest_before_transform: Manifest =
        serde_yaml::from_str(&contents).map_err(ReadManifestError::CouldNotParseManifest)?;

    contents = substitute_env_variables(&contents)
        .map_err(ReadManifestError::CouldNotSubstituteEnvVariables)?;

    let mut manifest_after_transform: Manifest =
        serde_yaml::from_str(&contents).map_err(ReadManifestError::CouldNotParseManifest)?;

    // as we don't want to inject the RPC URL in rust projects in clear text we should change
    // the networks.rpc back to what it was before and the generated code will handle it
    if manifest_after_transform.project_type == ProjectType::Rust {
        for network in &mut manifest_after_transform.networks {
            network.rpc = manifest_before_transform
                .networks
                .iter()
                .find(|n| n.name == network.name)
                .map_or_else(
                    || replace_env_variable_to_raw_name(&network.rpc),
                    |n| replace_env_variable_to_raw_name(&n.rpc),
                );
        }
    }

    validate_manifest(file_path.parent().unwrap(), &manifest_after_transform)
        .map_err(ReadManifestError::CouldNotValidateManifest)?;

    Ok(manifest_after_transform)
}

#[derive(thiserror::Error, Debug)]
pub enum WriteManifestError {
    #[error("Could not open file: {0}")]
    CouldNotOpenFile(std::io::Error),

    #[error("Could not parse manifest to string: {0}")]
    CouldNotTurnManifestToString(serde_yaml::Error),

    #[error("Could not create file: {0}")]
    CouldNotCreateFile(std::io::Error),

    #[error("Could not write to file: {0}")]
    CouldNotWriteToFile(std::io::Error),
}

pub fn write_manifest(data: &Manifest, file_path: &PathBuf) -> Result<(), WriteManifestError> {
    let yaml_string =
        serde_yaml::to_string(data).map_err(WriteManifestError::CouldNotTurnManifestToString)?;

    let mut file = File::create(file_path).map_err(WriteManifestError::CouldNotCreateFile)?;
    file.write_all(yaml_string.as_bytes())
        .map_err(WriteManifestError::CouldNotWriteToFile)?;
    Ok(())
}
