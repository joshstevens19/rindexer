use std::{collections::BTreeMap, path::Path, process::Command};

use ethers_solc::{artifacts::Contracts, CompilerOutput};
use ethers_solc::artifacts::{Error, SourceFile};
use reqwest::Client;
use serde::{Deserialize, Serialize};

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

    #[error("Failed to create overlay: {0}")]
    FailedToDeployContract(String, String),

    #[error("overlay response is not json: {0}")]
    ResponseNotJson(reqwest::Error),

    #[error("overlay api failed: {0}")]
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
        // println!("{}", stdout_str);

        let compiler_output: CompilerOutput = serde_json::from_str(stdout_str)
            .map_err(|e| {
                println!("{}", e);
                DeployShadowError::InvalidCompilerOutputFromFormatJson
            })?;

        let shadow_compiler_output = ShadowCompilerOutput::from_compile_output(compiler_output);
        println!("{:?}", shadow_compiler_output);

        deploy_shadow(api_key, clone_meta, shadow_details, shadow_compiler_output).await
    } else {
        Err(DeployShadowError::CouldNotReadFormatJson)
    }
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
        .post(format!("https://api.staging.shadow.xyz/v1/{}/deploy", shadow_details.fork_id))
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
        println!("{:?}", response);
        Ok(response.rpc_url)
    } else {
        Err(DeployShadowError::FailedToDeployContract(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}
