use ethers::types::U64;
use regex::Regex;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::generator::event_callback_registry::{
    FactoryDetails, FilterDetails, IndexingContractSetup,
};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;

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
    D: serde::Deserializer<'de>,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,

    #[serde(deserialize_with = "deserialize_project_type")]
    #[serde(serialize_with = "serialize_project_type")]
    pub project_type: ProjectType,

    pub networks: Vec<Network>,

    #[serde(default = "default_storage")]
    pub storage: Storage,

    pub indexers: Vec<Indexer>,

    #[serde(default = "default_global")]
    pub global: Global,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Indexer {
    pub name: String,

    pub contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractDetails {
    pub network: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    address: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<FilterDetails>,

    #[serde(skip_serializing_if = "Option::is_none")]
    factory: Option<FactoryDetails>,

    #[serde(rename = "startBlock", skip_serializing_if = "Option::is_none")]
    pub start_block: Option<U64>,

    #[serde(rename = "endBlock", skip_serializing_if = "Option::is_none")]
    pub end_block: Option<U64>,

    #[serde(rename = "pollingEvery", skip_serializing_if = "Option::is_none")]
    pub polling_every: Option<u64>,
}

impl ContractDetails {
    pub fn indexing_contract_setup(&self) -> IndexingContractSetup {
        if let Some(address) = &self.address {
            IndexingContractSetup::Address(address.clone())
        } else if let Some(factory) = &self.factory {
            IndexingContractSetup::Factory(factory.clone())
        } else {
            IndexingContractSetup::Filter(self.filter.clone().unwrap())
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
    false
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,
    pub details: Vec<ContractDetails>,
    pub abi: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_events: Option<Vec<String>>,

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

    #[serde(rename = "chainId")]
    pub chain_id: u32,

    pub url: String,

    #[serde(rename = "maxBlockRange", skip_serializing_if = "Option::is_none")]
    pub max_block_range: Option<u64>,

    #[serde(rename = "maxConcurrency", skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u32>,
}

fn default_disable_create_tables() -> bool {
    false
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresConnectionDetails {
    pub name: String,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
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
        self.postgres.is_some()
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

/// Substitutes environment variables in a string with their values.
///
/// # Arguments
///
/// * `contents` - The string containing environment variables to substitute.
///
/// # Returns
///
/// A `Result` containing the string with substituted environment variables or an error message.
fn substitute_env_variables(contents: &str) -> Result<String, String> {
    let re = Regex::new(r"\$\{([^}]+)}").unwrap();
    let result = re.replace_all(contents, |caps: &regex::Captures| {
        let var_name = &caps[1];
        env::var(var_name).unwrap_or_else(|_| var_name.to_string())
    });
    Ok(result.to_string())
}

/// Reads a manifest file and returns a `Manifest` struct.
///
/// # Arguments
///
/// * `file_path` - A reference to the path of the manifest file.
///
/// # Returns
///
/// A `Result` containing the `Manifest` struct or an error.
pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    // rewrite the env variables
    // let mut substituted_contents =
    //     substitute_env_variables(&contents)?;
    file.read_to_string(&mut contents)?;

    let manifest: Manifest = serde_yaml::from_str(&contents)?;
    Ok(manifest)
}

/// Writes a `Manifest` struct to a YAML file.
///
/// # Arguments
///
/// * `data` - A reference to the `Manifest` struct to write.
/// * `file_path` - A reference to the path of the output file.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub fn write_manifest(data: &Manifest, file_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let yaml_string = serde_yaml::to_string(data)?;

    let mut file = File::create(file_path)?;
    file.write_all(yaml_string.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_substitute_env_variables() {
        env::set_var("TEST_ENV_VAR", "test_value");
        let input = "Value: ${TEST_ENV_VAR}";
        let result = substitute_env_variables(input).unwrap();
        assert_eq!(result, "Value: test_value");
    }

    #[test]
    fn test_read_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("manifest.yaml");
        let content = r#"
        name: "Test Manifest"
        indexers: []
        networks: []
        "#;
        fs::write(&file_path, content).unwrap();

        let manifest = read_manifest(&file_path).unwrap();
        assert_eq!(manifest.name, "Test Manifest");
    }

    #[test]
    fn test_write_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("manifest.yaml");

        let manifest = Manifest {
            name: "Test Manifest".to_string(),
            description: None,
            repository: None,
            project_type: ProjectType::Rust,
            indexers: vec![],
            networks: vec![],
            global: Global::default(),
            storage: Storage::default(),
        };

        write_manifest(&manifest, &file_path).unwrap();

        let written_content = fs::read_to_string(&file_path).unwrap();
        assert!(written_content.contains("name: \"Test Manifest\""));
    }

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
