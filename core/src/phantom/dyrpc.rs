use std::error::Error;

use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::phantom::common::{CloneMeta, CompiledContract};

#[derive(Serialize, Debug)]
struct DeployDyrpcRequest<'a> {
    overlays: std::collections::HashMap<&'a str, DeployDyrpcDetails<'a>>,
}

#[derive(Serialize, Debug)]
struct DeployDyrpcDetails<'a> {
    #[serde(rename = "creationCode")]
    creation_code: &'a str,

    #[serde(rename = "constructorArgs")]
    constructor_args: &'a str,
}

#[derive(Deserialize)]
pub struct DeployDyrpcContractResponse {
    // don't care for now about these
    // pub message: String,
    //
    // #[serde(rename = "overlayHash")]
    // pub overlay_hash: String,
    //
    // pub addresses: Vec<String>,
    #[serde(rename = "overlayRpcUrl")]
    pub rpc_url: String,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateDyrpcError {
    #[error("Failed to deploy dyRPC: {0}")]
    FailedToDeployContract(String, String),

    #[error("dyRPC response is not json: {0}")]
    ResponseNotJson(reqwest::Error),

    #[error("dyRPC api failed: {0}")]
    ApiFailed(reqwest::Error),
}

pub async fn create_dyrpc_api_key() -> Result<String, CreateDyrpcError> {
    let client = Client::new();
    let response = client
        .post("https://api.dyrpc.network/generate")
        .send()
        .await
        .map_err(CreateDyrpcError::ApiFailed)?;

    if response.status().is_success() {
        let api_key = response.text().await.map_err(CreateDyrpcError::ResponseNotJson)?;
        Ok(api_key)
    } else {
        Err(CreateDyrpcError::FailedToDeployContract(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}

pub async fn deploy_dyrpc_contract(
    api_key: &str,
    clone_meta: &CloneMeta,
    compiled_contract: &CompiledContract,
) -> Result<String, Box<dyn Error>> {
    let result = deploy_contract(
        &clone_meta.address,
        api_key,
        &compiled_contract.bytecode.object,
        &clone_meta.constructor_arguments,
    )
    .await?;

    let re = Regex::new(r"/eth/([a-fA-F0-9]{64})/").unwrap();
    let rpc_url = re
        .replace(&result.rpc_url, "/eth/{RINDEXER_PHANTOM_API_KEY}/")
        .to_string()
        .replace("{RINDEXER_PHANTOM_API_KEY}", "${RINDEXER_PHANTOM_API_KEY}");

    Ok(rpc_url)
}

async fn deploy_contract(
    address: &str,
    api_key: &str,
    new_bytecode: &str,
    constructor_args_bytecode: &str,
) -> Result<DeployDyrpcContractResponse, CreateDyrpcError> {
    let url = format!("https://node.dyrpc.network/eth/{}/overlay/put", api_key);

    let mut overlays = std::collections::HashMap::new();
    overlays.insert(
        address,
        DeployDyrpcDetails {
            creation_code: new_bytecode,
            constructor_args: constructor_args_bytecode,
        },
    );

    let request_body = DeployDyrpcRequest { overlays };

    let client = Client::new();
    let response = client
        .put(&url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(CreateDyrpcError::ApiFailed)?;

    if response.status().is_success() {
        let overlay_response: DeployDyrpcContractResponse =
            response.json().await.map_err(CreateDyrpcError::ResponseNotJson)?;
        Ok(overlay_response)
    } else {
        Err(CreateDyrpcError::FailedToDeployContract(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}
