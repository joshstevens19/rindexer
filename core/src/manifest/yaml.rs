use std::{
    collections::HashSet,
    env,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use ethers::types::ValueOrArray;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{
    abi::ABIItem,
    helpers::{load_env_from_full_path, replace_env_variable_to_raw_name},
    manifest::{
        core::{Manifest, ProjectType},
        network::Network,
    },
    StringOrArray,
};

pub const YAML_CONFIG_NAME: &str = "rindexer.yaml";

fn substitute_env_variables(contents: &str) -> Result<String, regex::Error> {
    let re = Regex::new(r"\$\{([^}]+)\}")?;
    let result = re.replace_all(contents, |caps: &Captures| {
        let var_name = &caps[1];
        match env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                error!("Environment variable {} not found", var_name);
                panic!("Environment variable {} not found", var_name)
            }
        }
    });
    Ok(result.into_owned())
}

#[derive(thiserror::Error, Debug)]
pub enum ValidateManifestError {
    #[error("Contract names {0} must be unique")]
    ContractNameMustBeUnique(String),

    #[error("Contract name {0} can not include 'Filter' in the name as it is a reserved word")]
    ContractNameCanNotIncludeFilter(String),

    #[error("Invalid network mapped to contract: network - {0} contract - {1}")]
    InvalidNetworkMappedToContract(String, String),

    #[error("Invalid filter event name {0} for contract {1} does not exist in ABI")]
    InvalidFilterEventNameDoesntExistInABI(String, String),

    #[error("Could not read or parse ABI for contract {0} with path {1}")]
    InvalidABI(String, String),

    #[error("Event {0} included in include_events for contract {1} but not found in ABI - it must be an event type and match the name exactly")]
    EventIncludedNotFoundInABI(String, String),

    #[error("Event {0} not found in ABI for contract {1}")]
    IndexedFilterEventNotFoundInABI(String, String),

    #[error("Indexed filter defined more than allowed for event {0} for contract {1} - indexed expected: {2} defined: {3}")]
    IndexedFilterDefinedMoreThanAllowed(String, String, usize, usize),

    #[error("Relationship contract {0} not found")]
    RelationshipContractNotFound(String),

    #[error("Relationship foreign key contract {0} not found")]
    RelationshipForeignKeyContractNotFound(String),

    #[error("Streams config is invalid: {0}")]
    StreamsConfigValidationError(String),

    #[error("Global ABI can only be a single string")]
    GlobalAbiCanOnlyBeASingleString(String),
}

fn validate_manifest(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<(), ValidateManifestError> {
    let mut seen = HashSet::new();
    let duplicates_contract_names: Vec<String> = manifest
        .contracts
        .iter()
        .filter_map(|c| if seen.insert(&c.name) { None } else { Some(c.name.clone()) })
        .collect();

    if !duplicates_contract_names.is_empty() {
        return Err(ValidateManifestError::ContractNameMustBeUnique(
            duplicates_contract_names.join(", "),
        ));
    }

    for contract in &manifest.contracts {
        if contract.name.to_lowercase().contains("filter") {
            return Err(ValidateManifestError::ContractNameCanNotIncludeFilter(
                contract.name.clone(),
            ));
        }

        let events = ABIItem::read_abi_items(project_path, contract)
            .map_err(|e| ValidateManifestError::InvalidABI(contract.name.clone(), e.to_string()))?;

        for detail in &contract.details {
            let has_network = manifest.networks.iter().any(|n| n.name == detail.network);
            if !has_network {
                return Err(ValidateManifestError::InvalidNetworkMappedToContract(
                    detail.network.clone(),
                    contract.name.clone(),
                ));
            }

            if let Some(filter_details) = &detail.filter {
                match filter_details {
                    ValueOrArray::Value(filter_details) => {
                        if !events.iter().any(|e| e.name == *filter_details.event_name) {
                            return Err(
                                ValidateManifestError::InvalidFilterEventNameDoesntExistInABI(
                                    filter_details.event_name.clone(),
                                    contract.name.clone(),
                                ),
                            );
                        }
                    }
                    ValueOrArray::Array(filters) => {
                        for filter_details in filters {
                            if !events.iter().any(|e| e.name == *filter_details.event_name) {
                                return Err(
                                    ValidateManifestError::InvalidFilterEventNameDoesntExistInABI(
                                        filter_details.event_name.clone(),
                                        contract.name.clone(),
                                    ),
                                );
                            }
                        }
                    }
                }
            }

            if let Some(indexed_filters) = &detail.indexed_filters {
                for indexed_filter in indexed_filters.iter() {
                    let event = events.iter().find(|e| e.name == indexed_filter.event_name);
                    if let Some(event) = event {
                        let indexed_allowed_length =
                            event.inputs.iter().filter(|i| i.indexed.unwrap_or(false)).count();
                        let indexed_filter_defined =
                            indexed_filter.indexed_1.as_ref().map_or(0, |_| 1) +
                                indexed_filter.indexed_2.as_ref().map_or(0, |_| 1) +
                                indexed_filter.indexed_3.as_ref().map_or(0, |_| 1);

                        if indexed_filter_defined > indexed_allowed_length {
                            return Err(ValidateManifestError::IndexedFilterDefinedMoreThanAllowed(
                                indexed_filter.event_name.clone(),
                                contract.name.clone(),
                                indexed_allowed_length,
                                indexed_filter_defined,
                            ));
                        }
                    } else {
                        return Err(ValidateManifestError::IndexedFilterEventNotFoundInABI(
                            indexed_filter.event_name.clone(),
                            contract.name.clone(),
                        ));
                    }
                }
            }
        }

        if let Some(include_events) = &contract.include_events {
            for event in include_events {
                if !events.iter().any(|e| e.name == *event && e.type_ == "event") {
                    return Err(ValidateManifestError::EventIncludedNotFoundInABI(
                        event.clone(),
                        contract.name.clone(),
                    ));
                }
            }
        }

        if let Some(_dependency_events) = &contract.dependency_events {
            // TODO - validate the events all exist in the contract ABIs
        }

        if let Some(streams) = &contract.streams {
            if let Err(e) = streams.validate() {
                return Err(ValidateManifestError::StreamsConfigValidationError(e));
            }
        }
    }

    if let Some(postgres) = &manifest.storage.postgres {
        if let Some(relationships) = &postgres.relationships {
            for relationship in relationships {
                if !manifest.contracts.iter().any(|c| c.name == relationship.contract_name) {
                    return Err(ValidateManifestError::RelationshipContractNotFound(
                        relationship.contract_name.clone(),
                    ));
                }

                for foreign_key in &relationship.foreign_keys {
                    if !manifest.contracts.iter().any(|c| c.name == foreign_key.contract_name) {
                        return Err(ValidateManifestError::RelationshipForeignKeyContractNotFound(
                            foreign_key.contract_name.clone(),
                        ));
                    }
                }

                // TODO - Add validation for the event names and event inputs match the ABIs
            }
        }
    }

    if let Some(global) = &manifest.global {
        if let Some(contracts) = &global.contracts {
            for contract in contracts {
                match &contract.abi {
                    StringOrArray::Single(_) => {}
                    StringOrArray::Multiple(value) => {
                        return Err(ValidateManifestError::GlobalAbiCanOnlyBeASingleString(
                            format!(
                                "Global ABI can only be a single string but found multiple: {:?}",
                                value
                            ),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum ReadManifestError {
    #[error("Could not open file: {0}")]
    CouldNotOpenFile(#[from] std::io::Error),

    #[error("Could not parse manifest: {0}")]
    CouldNotParseManifest(#[from] serde_yaml::Error),

    #[error("Could not substitute env variables: {0}")]
    CouldNotSubstituteEnvVariables(#[from] regex::Error),

    #[error("Could not validate manifest: {0}")]
    CouldNotValidateManifest(#[from] ValidateManifestError),

    #[error("No project path found using parent of manifest path")]
    NoProjectPathFoundUsingParentOfManifestPath,
}

pub fn read_manifest_raw(file_path: &PathBuf) -> Result<Manifest, ReadManifestError> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();

    file.read_to_string(&mut contents)?;

    let manifest: Manifest = serde_yaml::from_str(&contents)?;

    let project_path = file_path.parent();
    match project_path {
        None => Err(ReadManifestError::NoProjectPathFoundUsingParentOfManifestPath),
        Some(project_path) => {
            validate_manifest(project_path, &manifest)?;
            Ok(manifest)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ManifestNetworksOnly {
    pub networks: Vec<Network>,
}

fn extract_environment_path(contents: &str, file_path: &Path) -> Option<PathBuf> {
    let re = Regex::new(r"(?m)^environment_path:\s*(.+)$").unwrap();
    re.captures(contents).and_then(|cap| cap.get(1)).map(|m| {
        let path_str = m.as_str().trim().replace('\"', ""); // Remove any quotes
        let base_dir = file_path.parent().unwrap_or(Path::new(""));
        let full_path = base_dir.join(path_str);
        full_path.canonicalize().unwrap_or(full_path)
    })
}

pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, ReadManifestError> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();

    file.read_to_string(&mut contents)?;

    let environment_path = extract_environment_path(&contents, file_path);
    if let Some(ref path) = environment_path {
        load_env_from_full_path(path);
    }

    let contents_before_transform = contents.clone();

    contents = substitute_env_variables(&contents)?;

    let mut manifest_after_transform: Manifest = serde_yaml::from_str(&contents)?;

    // as we don't want to inject the RPC URL in rust projects in clear text we should change
    // the networks.rpc back to what it was before and the generated code will handle it
    if manifest_after_transform.project_type == ProjectType::Rust {
        let manifest_networks_only: ManifestNetworksOnly =
            serde_yaml::from_str(&contents_before_transform)?;
        for network in &mut manifest_after_transform.networks {
            network.rpc = manifest_networks_only
                .networks
                .iter()
                .find(|n| n.name == network.name)
                .map_or_else(
                    || replace_env_variable_to_raw_name(&network.rpc),
                    |n| replace_env_variable_to_raw_name(&n.rpc),
                );
        }
    }

    let project_path = file_path.parent();
    match project_path {
        None => Err(ReadManifestError::NoProjectPathFoundUsingParentOfManifestPath),
        Some(project_path) => {
            validate_manifest(project_path, &manifest_after_transform)?;
            Ok(manifest_after_transform)
        }
    }
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
    file.write_all(yaml_string.as_bytes()).map_err(WriteManifestError::CouldNotWriteToFile)?;
    Ok(())
}
