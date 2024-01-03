use std::error::Error;
use std::fs::File;
use std::io::Read;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    description: String,
    repository: String,
    indexers: Vec<Indexer>,
    mapping: Mapping,
    networks: Vec<Network>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Indexer {
    name: String,
    networks: Vec<String>,
    source: Vec<Source>,
    context: Context,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Source {
    address: Option<String>,
    network: Option<String>,
    #[serde(rename = "startBlock")]
    start_block: Option<u64>,
    #[serde(rename = "endBlock")]
    end_block: Option<u64>,
    #[serde(rename = "pollingEvery")]
    polling_every: Option<u64>,
    abi: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Context {
    contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Contract {
    abi: String,
    name: String,
    network: String,
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Mapping {
    abis: Vec<Abi>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Abi {
    name: String,
    file: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Network {
    name: String,
    #[serde(rename = "chainId")]
    chain_id: u32,
    url: String,
    #[serde(rename = "maxBlockRange")]
    max_block_range: Option<u64>,
    #[serde(rename = "maxConcurrency")]
    max_concurrency: Option<u32>,
}

pub fn read_manifest(file_path: &str) -> Result<Manifest, Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let manifest: Manifest = serde_yaml::from_str(&contents)?;
    Ok(manifest)
}
