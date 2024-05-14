use ethers::types::U64;
use regex::Regex;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::generator::event_callback_registry::{AddressOrFilter, FactoryDetails, FilterDetails};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<String>,

    pub indexers: Vec<Indexer>,

    pub networks: Vec<Network>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<Global>,
}

#[derive(Debug, Serialize, Deserialize)]
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

    // #[serde(skip_serializing_if = "Option::is_none")]
    // factory: Option<FactoryDetails>,
    #[serde(rename = "startBlock", skip_serializing_if = "Option::is_none")]
    pub start_block: Option<U64>,

    #[serde(rename = "endBlock", skip_serializing_if = "Option::is_none")]
    pub end_block: Option<U64>,

    #[serde(rename = "pollingEvery", skip_serializing_if = "Option::is_none")]
    pub polling_every: Option<u64>,
}

impl ContractDetails {
    pub fn address_or_filter(&self) -> AddressOrFilter {
        if let Some(address) = &self.address {
            AddressOrFilter::Address(address.clone())
        } else {
            AddressOrFilter::Filter(self.filter.clone().unwrap())
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
            // factory: None,
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
            // factory: None,
            start_block,
            end_block,
            polling_every,
        }
    }
}

fn default_reorg_safe_distance() -> bool {
    false
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,
    pub details: Vec<ContractDetails>,
    pub abi: String,

    #[serde(default = "default_reorg_safe_distance")]
    pub reorg_safe_distance: bool,
}

impl Contract {
    pub fn override_name(&mut self, name: String) {
        self.name = name;
    }
}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PostgresClient {
    pub name: String,
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Databases {
    pub postgres: Option<PostgresClient>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Global {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<Contract>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub databases: Option<Databases>,
}

fn substitute_env_variables(contents: &str) -> Result<String, String> {
    // safe unwrap here, because the regex is hardcoded
    let re = Regex::new(r"\$\{([^}]+)}").unwrap();
    let result = re.replace_all(contents, |caps: &regex::Captures| {
        let var_name = &caps[1];
        env::var(var_name).unwrap_or_else(|_| var_name.to_string())
    });
    Ok(result.to_string())
}

pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    // rewrite the env variables
    // let mut substituted_contents =
    //     substitute_env_variables(&contents)?;

    file.read_to_string(&mut contents)?;

    println!("before manifest {:?}", contents);

    let manifest: Manifest = serde_yaml::from_str(&contents)?;
    println!("after manifest {:?}", manifest);
    Ok(manifest)
}

pub fn write_manifest(data: &Manifest, file_path: &PathBuf) -> Result<(), Box<dyn Error>> {
    let yaml_string = serde_yaml::to_string(data)?;

    let mut file = File::create(file_path)?;
    file.write_all(yaml_string.as_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    #[test]
    fn read_works() {
        let manifest = super::read_manifest(&PathBuf::from("/Users/joshstevens/code/rindexer/rindexer_core/external-examples/manifest-example.yaml")).unwrap();

        println!("{:?}", manifest);
    }
}
