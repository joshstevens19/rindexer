use std::{error::Error, fs::File, io::Read, path::Path};

use ethers::abi::Abi;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct CloneMeta {
    pub path: String,

    #[serde(rename = "targetContract")]
    pub target_contract: String,

    pub address: String,

    #[serde(rename = "constructorArguments")]
    pub constructor_arguments: String,
}

impl CloneMeta {
    fn get_out_contract_sol_from_path(&self) -> String {
        self.path.split('/').last().unwrap_or_default().to_string()
    }
}

pub fn read_contract_clone_metadata(contract_path: &Path) -> Result<CloneMeta, Box<dyn Error>> {
    let meta_file_path = contract_path.join(".clone.meta");

    let mut file = File::open(meta_file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let clone_meta: CloneMeta = serde_json::from_str(&contents)?;

    Ok(clone_meta)
}

#[derive(Deserialize, Debug)]
pub struct Bytecode {
    pub object: String,
}

#[derive(Deserialize, Debug)]
pub struct CompiledContract {
    pub abi: Abi,

    pub bytecode: Bytecode,
}

pub fn read_compiled_contract(
    contract_path: &Path,
    clone_meta: &CloneMeta,
) -> Result<CompiledContract, Box<dyn Error>> {
    let compiled_file_path = contract_path.join("out").join(format!(
        "{}/{}.json",
        clone_meta.get_out_contract_sol_from_path(),
        clone_meta.target_contract
    ));

    let mut file = File::open(compiled_file_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    let compiled_contract: CompiledContract = serde_json::from_str(&contents)?;

    Ok(compiled_contract)
}
