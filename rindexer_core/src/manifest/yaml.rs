use ethers::types::U64;
use regex::{Captures, Regex};
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::generator::event_callback_registry::{
    FactoryDetails, FilterDetails, IndexingContractSetup,
};
use crate::indexer::Indexer;
use serde::{Deserialize, Deserializer, Serialize};
use serde_yaml::Value;

pub const YAML_CONFIG_NAME: &str = "rindexer.yaml";

fn default_global() -> Global {
    Global::default()
}

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

    #[serde(default = "default_global")]
    pub global: Global,
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
pub struct ContractDetails {
    pub network: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    filter: Option<FilterDetails>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    factory: Option<FactoryDetails>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string"
    )]
    pub start_block: Option<U64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string"
    )]
    pub end_block: Option<U64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub polling_every: Option<u64>,
}

impl ContractDetails {
    pub fn indexing_contract_setup(&self) -> IndexingContractSetup {
        if let Some(address) = &self.address {
            IndexingContractSetup::Address(address.clone())
        } else if let Some(factory) = &self.factory {
            IndexingContractSetup::Factory(factory.clone())
        } else if let Some(filter) = &self.filter {
            IndexingContractSetup::Filter(filter.clone())
        } else {
            panic!("Contract details must have an address, factory or filter");
        }
    }

    pub fn address(&self) -> Option<&str> {
        if let Some(address) = &self.address {
            Some(address)
        } else if let Some(factory) = &self.factory {
            Some(&factory.address)
        } else {
            None
        }
    }

    pub fn new_with_address(
        network: String,
        address: String,
        start_block: Option<U64>,
        end_block: Option<U64>,
        polling_every: Option<u64>,
    ) -> Self {
        Self {
            network,
            address: Some(address),
            filter: None,
            factory: None,
            start_block,
            end_block,
            polling_every,
        }
    }

    pub fn new_with_filter(
        network: String,
        filter: FilterDetails,
        start_block: Option<U64>,
        end_block: Option<U64>,
        polling_every: Option<u64>,
    ) -> Self {
        Self {
            network,
            address: None,
            filter: Some(filter),
            factory: None,
            start_block,
            end_block,
            polling_every,
        }
    }

    pub fn new_with_factory(
        network: String,
        factory: FactoryDetails,
        start_block: Option<U64>,
        end_block: Option<U64>,
        polling_every: Option<u64>,
    ) -> Self {
        Self {
            network,
            address: None,
            filter: None,
            factory: Some(factory),
            start_block,
            end_block,
            polling_every,
        }
    }
}

fn default_reorg_safe_distance() -> bool {
    false
}

fn default_generate_csv() -> bool {
    true
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyEventTree {
    pub events: Vec<String>,
    pub then: Option<Vec<DependencyEventTree>>,
}

impl DependencyEventTree {
    pub fn collect_dependency_events(&self) -> Vec<String> {
        let mut dependencies = Vec::new();

        dependencies.extend(self.events.clone());

        if let Some(children) = &self.then {
            for child in children {
                dependencies.extend(child.collect_dependency_events());
            }
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
    pub dependency_events: Option<DependencyEventTree>,

    #[serde(default = "default_reorg_safe_distance")]
    pub reorg_safe_distance: bool,

    #[serde(default = "default_generate_csv")]
    pub generate_csv: bool,
}

impl Contract {
    pub fn override_name(&mut self, name: String) {
        self.name = name;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Network {
    pub name: String,

    pub chain_id: u32,

    pub url: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_block_range: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compute_units_per_second: Option<u64>,
}

fn default_disable_create_tables() -> bool {
    false
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresConnectionDetails {
    pub enabled: bool,
    #[serde(default = "default_disable_create_tables")]
    pub disable_create_tables: bool,
}

fn default_disable_create_headers() -> bool {
    false
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CsvDetails {
    pub path: String,
    #[serde(default = "default_disable_create_headers")]
    pub disable_create_headers: bool,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Storage {
    pub postgres: Option<PostgresConnectionDetails>,
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

        self.postgres
            .as_ref()
            .map_or(false, |details| details.disable_create_tables)
    }

    pub fn csv_enabled(&self) -> bool {
        self.csv.is_some()
    }

    pub fn csv_disable_create_headers(&self) -> bool {
        let enabled = self.csv_enabled();
        if !enabled {
            return true;
        }

        self.csv
            .as_ref()
            .map_or(false, |details| details.disable_create_headers)
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
pub enum ReadManifestError {
    #[error("Could not open file: {0}")]
    CouldNotOpenFile(std::io::Error),

    #[error("Could not read file: {0}")]
    CouldNotReadFile(std::io::Error),

    #[error("Could not parse manifest: {0}")]
    CouldNotParseManifest(serde_yaml::Error),

    #[error("Could not substitute env variables: {0}")]
    CouldNotSubstituteEnvVariables(regex::Error),
}

pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, ReadManifestError> {
    let mut file = File::open(file_path).map_err(ReadManifestError::CouldNotOpenFile)?;
    let mut contents = String::new();
    
    file.read_to_string(&mut contents)
        .map_err(ReadManifestError::CouldNotReadFile)?;

    contents = substitute_env_variables(&contents)
        .map_err(ReadManifestError::CouldNotSubstituteEnvVariables)?;

    let manifest: Manifest =
        serde_yaml::from_str(&contents).map_err(ReadManifestError::CouldNotParseManifest)?;

    Ok(manifest)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_details_address() {
        let contract_details = ContractDetails {
            network: "testnet".to_string(),
            address: Some("0x123".to_string()),
            filter: None,
            factory: None,
            start_block: None,
            end_block: None,
            polling_every: None,
        };

        assert_eq!(contract_details.address(), Some("0x123"));

        let factory_details = FactoryDetails {
            address: "0xabc".to_string(),
            event_name: "TestEvent".to_string(),
            parameter_name: "param".to_string(),
            abi: "[]".to_string(),
        };

        let contract_details = ContractDetails {
            network: "testnet".to_string(),
            address: None,
            filter: None,
            factory: Some(factory_details),
            start_block: None,
            end_block: None,
            polling_every: None,
        };

        assert_eq!(contract_details.address(), Some("0xabc"));
    }

    #[test]
    fn test_contract_details_indexing_contract_setup() {
        let filter_details = FilterDetails {
            event_name: "TestEvent".to_string(),
            indexed_1: None,
            indexed_2: None,
            indexed_3: None,
        };

        let contract_details = ContractDetails {
            network: "testnet".to_string(),
            address: None,
            filter: Some(filter_details.clone()),
            factory: None,
            start_block: None,
            end_block: None,
            polling_every: None,
        };

        match contract_details.indexing_contract_setup() {
            IndexingContractSetup::Filter(filter) => {
                assert_eq!(filter.event_name, "TestEvent");
            }
            _ => panic!("Expected filter setup"),
        }
    }
}
