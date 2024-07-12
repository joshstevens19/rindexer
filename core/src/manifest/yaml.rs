use regex::{Captures, Regex};
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::abi::ABIItem;
use crate::helpers::replace_env_variable_to_raw_name;
use crate::manifest::core::{Manifest, ProjectType};

pub const YAML_CONFIG_NAME: &str = "rindexer.yaml";

fn substitute_env_variables(contents: &str) -> Result<String, regex::Error> {
    let re = Regex::new(r"\$<([^>]+)>")?;
    let result = re.replace_all(contents, |caps: &Captures| {
        let var_name = &caps[1];
        match env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                panic!("Environment variable {} not found", var_name)
            }
        }
    });
    Ok(result.into_owned())
}

#[derive(thiserror::Error, Debug)]
pub enum ValidateManifestError {
    #[error("Invalid network mapped to contract: network - {0} contract - {1}")]
    InvalidNetworkMappedToContract(String, String),

    #[error("Invalid filter event name {0} for contract {1} does not exist in ABI")]
    InvalidFilterEventNameDoesntExistInABI(String, String),

    #[error("Could not read or parse ABI for contract {0} with path {1}")]
    InvalidABI(String, String),

    #[error("Event {0} included in include_events for contract {1} not found in ABI")]
    EventIncludedNotFoundInABI(String, String),

    #[error("Event {0} not found in ABI for contract {1}")]
    IndexedFilterEventNotFoundInABI(String, String),

    #[error("Indexed filter defined more than allowed for event {0} for contract {1} - indexed expected: {2} defined: {3}")]
    IndexedFilterDefinedMoreThanAllowed(String, String, usize, usize),

    #[error("Relationship contract {0} not found")]
    RelationshipContractNotFound(String),

    #[error("Relationship foreign key contract {0} not found")]
    RelationshipForeignKeyContractNotFound(String),
}

fn validate_manifest(
    project_path: &Path,
    manifest: &Manifest,
) -> Result<(), ValidateManifestError> {
    for contract in &manifest.contracts {
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

            if let Some(address) = &detail.filter {
                if !events.iter().any(|e| e.name == *address.event_name) {
                    return Err(
                        ValidateManifestError::InvalidFilterEventNameDoesntExistInABI(
                            address.event_name.clone(),
                            contract.name.clone(),
                        ),
                    );
                }
            }

            if let Some(indexed_filters) = &detail.indexed_filters {
                for indexed_filter in indexed_filters.iter() {
                    let event = events.iter().find(|e| e.name == indexed_filter.event_name);
                    if let Some(event) = event {
                        let indexed_allowed_length = event
                            .inputs
                            .iter()
                            .filter(|i| i.indexed.unwrap_or(false))
                            .count();
                        let indexed_filter_defined =
                            indexed_filter.indexed_1.as_ref().map_or(0, |_| 1)
                                + indexed_filter.indexed_2.as_ref().map_or(0, |_| 1)
                                + indexed_filter.indexed_3.as_ref().map_or(0, |_| 1);

                        if indexed_filter_defined > indexed_allowed_length {
                            return Err(
                                ValidateManifestError::IndexedFilterDefinedMoreThanAllowed(
                                    indexed_filter.event_name.clone(),
                                    contract.name.clone(),
                                    indexed_allowed_length,
                                    indexed_filter_defined,
                                ),
                            );
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
                if !events.iter().any(|e| e.name == *event) {
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
    }

    if let Some(postgres) = &manifest.storage.postgres {
        if let Some(relationships) = &postgres.relationships {
            for relationship in relationships {
                if !manifest
                    .contracts
                    .iter()
                    .any(|c| c.name == relationship.contract_name)
                {
                    return Err(ValidateManifestError::RelationshipContractNotFound(
                        relationship.contract_name.clone(),
                    ));
                }

                for foreign_key in &relationship.foreign_keys {
                    if !manifest
                        .contracts
                        .iter()
                        .any(|c| c.name == foreign_key.contract_name)
                    {
                        return Err(
                            ValidateManifestError::RelationshipForeignKeyContractNotFound(
                                foreign_key.contract_name.clone(),
                            ),
                        );
                    }
                }

                // TODO - Add validation for the event names and event inputs match the ABIs
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

pub fn read_manifest(file_path: &PathBuf) -> Result<Manifest, ReadManifestError> {
    let mut file = File::open(file_path)?;
    let mut contents = String::new();

    file.read_to_string(&mut contents)?;

    let manifest_before_transform: Manifest = serde_yaml::from_str(&contents)?;

    contents = substitute_env_variables(&contents)?;

    let mut manifest_after_transform: Manifest = serde_yaml::from_str(&contents)?;

    // as we don't want to inject the RPC URL in rust projects in clear text we should change
    // the networks.rpc back to what it was before and the generated code will handle it
    if manifest_after_transform.project_type == ProjectType::Rust {
        for network in &mut manifest_after_transform.networks {
            network.rpc = manifest_before_transform
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
    file.write_all(yaml_string.as_bytes())
        .map_err(WriteManifestError::CouldNotWriteToFile)?;
    Ok(())
}
