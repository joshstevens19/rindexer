use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug)]
struct OverlayRequest<'a> {
    overlays: std::collections::HashMap<&'a str, OverlayDetails<'a>>,
}

#[derive(Serialize, Debug)]
struct OverlayDetails<'a> {
    #[serde(rename = "creationCode")]
    creation_code: &'a str,

    #[serde(rename = "constructorArgs")]
    constructor_args: &'a str,
}

#[derive(Deserialize)]
pub struct OverlayResponse {
    pub message: String,

    #[serde(rename = "overlayHash")]
    pub overlay_hash: String,

    pub addresses: Vec<String>,

    #[serde(rename = "overlayRpcUrl")]
    pub overlay_rpc_url: String,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateOverlayError {
    #[error("Failed to create overlay: {0}")]
    FailedToCreateOverlay(String, String),

    #[error("overlay response is not json: {0}")]
    ResponseNotJson(reqwest::Error),

    #[error("overlay api failed: {0}")]
    ApiFailed(reqwest::Error),
}

pub async fn create_overlay_api_key() -> Result<String, CreateOverlayError> {
    let client = Client::new();
    let response = client
        .get("https://api.dyrpc.network/generate")
        .send()
        .await
        .map_err(CreateOverlayError::ApiFailed)?;

    if response.status().is_success() {
        let api_key = response.text().await.map_err(CreateOverlayError::ResponseNotJson)?;
        Ok(api_key)
    } else {
        Err(CreateOverlayError::FailedToCreateOverlay(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}

pub async fn create_overlay(
    address: &str,
    api_key: &str,
    new_bytecode: &str,
    constructor_args_bytecode: &str,
) -> Result<OverlayResponse, CreateOverlayError> {
    let url = format!("https://node.dyrpc.network/eth/{}/overlay/put", api_key);

    let mut overlays = std::collections::HashMap::new();
    overlays.insert(
        address,
        OverlayDetails { creation_code: new_bytecode, constructor_args: constructor_args_bytecode },
    );

    let request_body = OverlayRequest { overlays };

    let client = Client::new();
    let response = client
        .put(&url)
        .header("Content-Type", "application/json")
        .json(&request_body)
        .send()
        .await
        .map_err(CreateOverlayError::ApiFailed)?;

    if response.status().is_success() {
        let overlay_response: OverlayResponse =
            response.json().await.map_err(CreateOverlayError::ResponseNotJson)?;
        Ok(overlay_response)
    } else {
        Err(CreateOverlayError::FailedToCreateOverlay(
            response.status().to_string(),
            response.text().await.unwrap_or_default(),
        ))
    }
}
