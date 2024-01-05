use std::error::Error;
use std::fs::File;
use std::io::Read;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub description: Option<String>,
    pub repository: Option<String>,
    pub indexers: Vec<Indexer>,
    pub networks: Vec<Network>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Indexer {
    pub name: String,
    pub networks: Vec<String>,
    pub sources: Vec<Source>,
    pub context: Option<Context>,
    pub mappings: Mappings,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Source {
    pub name: String,
    pub address: String,
    pub network: String,
    #[serde(rename = "startBlock")]
    pub start_block: Option<u64>,
    #[serde(rename = "endBlock")]
    pub end_block: Option<u64>,
    #[serde(rename = "pollingEvery")]
    pub polling_every: Option<u64>,
    pub abi: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Context {
    pub contracts: Vec<Contract>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub abi: String,
    pub name: String,
    pub network: String,
    pub address: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Mappings {
    pub abis: Vec<ABI>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ABI {
    pub name: String,
    pub file: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Network {
    pub name: String,
    #[serde(rename = "chainId")]
    pub chain_id: u32,
    pub url: String,
    #[serde(rename = "maxBlockRange")]
    pub max_block_range: Option<u64>,
    #[serde(rename = "maxConcurrency")]
    pub max_concurrency: Option<u32>,
}

pub fn read_manifest(file_path: &str) -> Result<Manifest, Box<dyn Error>> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let manifest: Manifest = serde_yaml::from_str(&contents)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    const MANIFEST: &str =
        "/Users/joshstevens/code/rindexer/rindexer_core/external-examples/manifest-example.yaml";

    #[test]
    fn read_works() {
        let manifest = super::read_manifest(MANIFEST).unwrap();

        println!("{:?}", manifest);
    }
}
