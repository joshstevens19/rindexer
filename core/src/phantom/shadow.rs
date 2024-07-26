use std::{collections::BTreeMap, path::Path, process::Command};

use ethers_solc::{
    artifacts::{Contract, Contracts, Error, SourceFile},
    CompilerOutput,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{manifest::phantom::PhantomShadow, phantom::common::CloneMeta};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ShadowSourceFile {
    id: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ShadowCompilerOutput {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<ethers_solc::artifacts::Error>,

    #[serde(default)]
    pub sources: BTreeMap<String, ShadowSourceFile>,

    #[serde(default)]
    pub contracts: Contracts,
}

impl ShadowCompilerOutput {
    pub fn from_compile_output(output: CompilerOutput) -> Self {
        let sources = output
            .sources
            .into_iter()
            .map(|(key, value)| {
                let new_key = key.split("lib/").last().unwrap_or_default().to_string();
                let new_value = ShadowSourceFile { id: value.id };
                (new_key, new_value)
            })
            .collect();

        let contracts = output
            .contracts
            .into_iter()
            .map(|(file, contracts)| {
                let new_file = file.split("lib/").last().unwrap_or_default().to_string();
                let new_contracts = contracts.into_iter().collect();
                (new_file, new_contracts)
            })
            .collect();

        ShadowCompilerOutput { errors: output.errors, sources, contracts }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeployShadowError {
    #[error("Could not run forge build")]
    CouldNotCompileContract,

    #[error("Failed to read format json from forge build")]
    CouldNotReadFormatJson,

    #[error("Invalid compiler output from format json")]
    InvalidCompilerOutputFromFormatJson,

    #[error("Failed to deploy contract: {0}")]
    FailedToDeployContract(String, String),

    #[error("dyRPC response is not json: {0}")]
    ResponseNotJson(reqwest::Error),

    #[error("dyRPC api failed: {0}")]
    ApiFailed(reqwest::Error),
}

pub async fn deploy_shadow_contract(
    api_key: &str,
    deploy_in: &Path,
    clone_meta: &CloneMeta,
    shadow_details: &PhantomShadow,
) -> Result<String, DeployShadowError> {
    let output = Command::new("forge")
        .arg("build")
        .arg("--format-json")
        .arg("--force")
        .current_dir(deploy_in)
        .output()
        .map_err(|_| DeployShadowError::CouldNotCompileContract)?;

    if output.status.success() {
        let stdout_str = std::str::from_utf8(&output.stdout)
            .map_err(|_| DeployShadowError::CouldNotReadFormatJson)?;

        let compiler_output: CompilerOutput = forge_to_solc(stdout_str)
            .map_err(|_| DeployShadowError::InvalidCompilerOutputFromFormatJson)?;

        let shadow_compiler_output = ShadowCompilerOutput::from_compile_output(compiler_output);

        deploy_shadow(api_key, clone_meta, shadow_details, shadow_compiler_output).await
    } else {
        Err(DeployShadowError::CouldNotReadFormatJson)
    }
}

fn forge_to_solc(stdout_str: &str) -> Result<CompilerOutput, serde_json::Error> {
    let val: Value = serde_json::from_str(stdout_str)?;
    let errors_arr = val["errors"].as_array().unwrap();
    let contract_objs_val = val["contracts"].as_object().unwrap();
    let sources_obj_val = val["sources"].as_object().unwrap();

    let mut contracts = Contracts::new();
    let mut sources: BTreeMap<String, SourceFile> = BTreeMap::new();

    let errors = errors_arr
        .iter()
        .map(|e| serde_json::from_value::<Error>(e.clone()).unwrap())
        .collect::<Vec<Error>>();

    for (file, value) in contract_objs_val.into_iter() {
        let obj = value.as_object().unwrap();
        let modules = obj.into_iter().collect::<Vec<_>>();
        let mut contracts_map: BTreeMap<String, Contract> = BTreeMap::new();
        for (module_name, contract_objs_wrapper) in modules {
            // Note: Forge output has an array at this level, but solc output
            // seems to be an object. Is there a guarantee to only be one object?
            let contract = &contract_objs_wrapper[0]["contract"];
            let parsed_contract: Contract = serde_json::from_value(contract.clone())?;

            contracts_map.insert(module_name.clone(), parsed_contract);
        }
        contracts.insert(file.clone(), contracts_map);
    }

    for (file, value) in sources_obj_val.into_iter() {
        let arr = value.as_array().unwrap();
        // Note: Forge output has an array at this level, but solc output
        // seems to be an object. Is there a guarantee to only be one object?
        let source_file: SourceFile = serde_json::from_value(arr[0]["source_file"].clone())?;
        sources.insert(file.clone(), source_file);
    }

    Ok(CompilerOutput { errors, sources, contracts })
}

#[derive(Serialize, Deserialize, Debug)]
struct ShadowBodyContract {
    address: String,

    #[serde(rename = "compilerOutput")]
    compiler_output: ShadowCompilerOutput,
}

#[derive(Serialize, Deserialize, Debug)]
struct DeployShadowBody {
    #[serde(rename = "shadowedContracts")]
    shadowed_contracts: Vec<ShadowBodyContract>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DeployShadowResponse {
    pub fork_id: String,
    pub fork_version: u64,
    pub rpc_url: String,
}

async fn deploy_shadow(
    api_key: &str,
    clone_meta: &CloneMeta,
    shadow_details: &PhantomShadow,
    shadow_compiler_output: ShadowCompilerOutput,
) -> Result<String, DeployShadowError> {
    let client = Client::new();

    let response = client
        // https://api.staging.shadow.xyz
        // https://api.shadow.xyz
        .post(format!("https://api.shadow.xyz/v1/{}/deploy", shadow_details.fork_id))
        .header("X-SHADOW-API-KEY", api_key)
        .json(&DeployShadowBody {
            shadowed_contracts: vec![ShadowBodyContract {
                address: clone_meta.address.clone(),
                compiler_output: shadow_compiler_output,
            }],
        })
        .send()
        .await
        .map_err(DeployShadowError::ApiFailed)?;

    if response.status().is_success() {
        let response: DeployShadowResponse =
            response.json().await.map_err(DeployShadowError::ResponseNotJson)?;

        Ok(response.rpc_url)
    } else {
        Err(DeployShadowError::FailedToDeployContract(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}
