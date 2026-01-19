use std::{
    collections::HashSet,
    env,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use alloy::rpc::types::ValueOrArray;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{
    abi::ABIItem,
    event::{parse_arithmetic_expression, parse_filter_expression},
    helpers::{load_env_from_full_path, replace_env_variable_to_raw_name},
    manifest::{
        core::{Manifest, ProjectType},
        network::Network,
    },
    StringOrArray,
};

pub const YAML_CONFIG_NAME: &str = "rindexer.yaml";

/// Checks if a value string contains arithmetic operators indicating it's a computed expression.
fn is_arithmetic_expression(value: &str) -> bool {
    // Must contain at least one arithmetic operator
    let has_operator = value
        .chars()
        .enumerate()
        .any(|(i, c)| {
            if c == '*' || c == '/' {
                true
            } else if c == '+' || c == '-' {
                // Check it's not a unary operator at the start
                i > 0
            } else {
                false
            }
        });

    has_operator && value.contains('$')
}

/// Extracts variable names from a filter/condition expression string.
/// For example, "from != to && value > 100" would return ["from", "to", "value"].
/// Also handles $ prefix: "$from != $to" returns ["from", "to"].
fn extract_filter_variables(expr: &str) -> Vec<String> {
    let mut variables = Vec::new();
    let mut chars = expr.chars().peekable();
    let mut current_word = String::new();
    let mut has_dollar_prefix = false;

    // Keywords and operators to skip
    let keywords = ["true", "false", "null", "and", "or", "AND", "OR"];

    while let Some(c) = chars.next() {
        if c == '$' && current_word.is_empty() {
            // Start of a $-prefixed variable
            has_dollar_prefix = true;
        } else if c.is_alphanumeric() || c == '_' || c == '.' {
            current_word.push(c);
        } else {
            if !current_word.is_empty() {
                // Check if it's a variable (starts with letter or underscore, not a keyword or number)
                let first_char = current_word.chars().next().unwrap();
                if (first_char.is_alphabetic() || first_char == '_' || has_dollar_prefix)
                    && !keywords.contains(&current_word.as_str())
                {
                    // Get root variable name (before any dot)
                    let root_name = current_word.split('.').next().unwrap_or(&current_word);
                    // Avoid duplicates
                    if !variables.contains(&root_name.to_string()) {
                        variables.push(root_name.to_string());
                    }
                }
                current_word.clear();
                has_dollar_prefix = false;
            }
        }
    }

    // Handle last word
    if !current_word.is_empty() {
        let first_char = current_word.chars().next().unwrap();
        if (first_char.is_alphabetic() || first_char == '_' || has_dollar_prefix)
            && !keywords.contains(&current_word.as_str())
        {
            let root_name = current_word.split('.').next().unwrap_or(&current_word);
            if !variables.contains(&root_name.to_string()) {
                variables.push(root_name.to_string());
            }
        }
    }

    variables
}

/// Extracts variable names from an arithmetic expression string.
/// For example, "$amount + $fee * 2" would return ["amount", "fee"].
fn extract_arithmetic_variables(expr: &str) -> Vec<String> {
    let mut variables = Vec::new();
    let mut chars = expr.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let mut var_name = String::new();
            while let Some(&next_c) = chars.peek() {
                if next_c.is_alphanumeric() || next_c == '_' || next_c == '.' {
                    var_name.push(chars.next().unwrap());
                } else {
                    break;
                }
            }
            if !var_name.is_empty() {
                // Get root variable name (before any dot)
                let root_name = var_name.split('.').next().unwrap_or(&var_name);
                variables.push(root_name.to_string());
            }
        }
    }

    variables
}

fn substitute_env_variables(contents: &str) -> Result<String, regex::Error> {
    let re = Regex::new(r"\$\{([^}]+)\}")?;
    let result = re.replace_all(contents, |caps: &Captures| {
        let var_name = &caps[1];
        match env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                error!("Environment variable {} not found", var_name);
                panic!("Environment variable {var_name} not found")
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

    // Custom indexing validation errors
    #[error("Table validation error in contract '{1}': {0}")]
    CustomIndexingValidationError(String, String),

    #[error("Custom indexing event '{0}' in table '{1}' for contract '{2}' not found in ABI")]
    CustomIndexingEventNotFoundInABI(String, String, String),

    #[error("Custom indexing field '{0}' referenced in operation for event '{1}' in table '{2}' for contract '{3}' not found in table fields")]
    CustomIndexingFieldNotFound(String, String, String, String),

    #[error("Custom indexing event field '${0}' referenced in operation for event '{1}' in table '{2}' for contract '{3}' not found in event ABI")]
    CustomIndexingEventFieldNotFound(String, String, String, String),

    #[error("Invalid arithmetic expression '{0}' in operation for event '{1}' in table '{2}' for contract '{3}': {4}")]
    CustomIndexingInvalidArithmeticExpression(String, String, String, String, String),

    #[error("Invalid condition expression '{0}' in operation for event '{1}' in table '{2}' for contract '{3}': {4}")]
    CustomIndexingInvalidConditionExpression(String, String, String, String, String),

    #[error("Iterate field '${0}' in event '{1}' for table '{2}' in contract '{3}' not found in event ABI")]
    CustomIndexingIterateFieldNotFound(String, String, String, String),
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

    for contract in &manifest.all_contracts() {
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
                if !events.iter().any(|e| e.name == *event.name && e.type_ == "event") {
                    return Err(ValidateManifestError::EventIncludedNotFoundInABI(
                        event.name.clone(),
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

        // Validate tables (custom aggregation tables)
        if let Some(tables) = &contract.tables {
            for table in tables {
                // Validate that all operations have consistent where columns
                // (which become the primary key)
                if let Err(e) = table.validate_where_columns() {
                    return Err(ValidateManifestError::CustomIndexingValidationError(
                        e,
                        contract.name.clone(),
                    ));
                }

                // Collect table column names for validation
                let table_column_names: std::collections::HashSet<&str> =
                    table.columns.iter().map(|c| c.name.as_str()).collect();

                for event_mapping in &table.events {
                    // Check event exists in ABI
                    let abi_event =
                        events.iter().find(|e| e.name == event_mapping.event && e.type_ == "event");
                    if abi_event.is_none() {
                        return Err(ValidateManifestError::CustomIndexingEventNotFoundInABI(
                            event_mapping.event.clone(),
                            table.name.clone(),
                            contract.name.clone(),
                        ));
                    }
                    let abi_event = abi_event.unwrap();

                    // Collect event input names for validation
                    let event_input_names: std::collections::HashSet<&str> =
                        abi_event.inputs.iter().map(|i| i.name.as_str()).collect();

                    // Built-in transaction metadata fields that are always available
                    // All prefixed with rindexer_ to avoid conflicts with event fields
                    const BUILTIN_METADATA_FIELDS: &[&str] = &[
                        "rindexer_block_number",
                        "rindexer_block_timestamp",
                        "rindexer_tx_hash",
                        "rindexer_block_hash",
                        "rindexer_contract_address",
                        "rindexer_log_index",
                        "rindexer_tx_index",
                    ];

                    // Validate iterate bindings and collect aliases for later validation
                    let mut iterate_aliases: std::collections::HashSet<String> =
                        std::collections::HashSet::new();
                    for binding in &event_mapping.iterate {
                        // Check that the array field exists in the event
                        // Strip any nested path to get the root field
                        let root_field = binding
                            .array_field
                            .split('.')
                            .next()
                            .unwrap_or(&binding.array_field);
                        if !event_input_names.contains(root_field) {
                            return Err(ValidateManifestError::CustomIndexingIterateFieldNotFound(
                                binding.array_field.clone(),
                                event_mapping.event.clone(),
                                table.name.clone(),
                                contract.name.clone(),
                            ));
                        }
                        iterate_aliases.insert(binding.alias.clone());
                    }

                    for operation in &event_mapping.operations {
                        // Validate where clause columns
                        for (table_column, value) in &operation.where_clause {
                            // Check table column exists
                            if !table_column_names.contains(table_column.as_str()) {
                                return Err(ValidateManifestError::CustomIndexingFieldNotFound(
                                    table_column.clone(),
                                    event_mapping.event.clone(),
                                    table.name.clone(),
                                    contract.name.clone(),
                                ));
                            }

                            // Check event field reference if starts with $
                            if value.starts_with('$') {
                                // Skip validation for view calls ($call(...))
                                if value.starts_with("$call(") {
                                    continue;
                                }
                                let event_field = &value[1..];
                                // For nested fields like $data.amount, validate the root field
                                // Also strip array indices like ids[0] -> ids
                                let root_field = event_field
                                    .split(&['.', '['][..])
                                    .next()
                                    .unwrap_or(event_field);
                                // Skip validation for built-in metadata fields
                                if BUILTIN_METADATA_FIELDS.contains(&root_field) {
                                    continue;
                                }
                                // Also accept iterate aliases
                                if iterate_aliases.contains(root_field) {
                                    continue;
                                }
                                if !event_input_names.contains(root_field) {
                                    return Err(
                                        ValidateManifestError::CustomIndexingEventFieldNotFound(
                                            event_field.to_string(),
                                            event_mapping.event.clone(),
                                            table.name.clone(),
                                            contract.name.clone(),
                                        ),
                                    );
                                }
                            }
                        }

                        // Validate condition expression (from `if` or `filter` field)
                        if let Some(condition_expr) = operation.condition() {
                            // Validate the expression parses correctly
                            if let Err(e) = parse_filter_expression(condition_expr) {
                                return Err(
                                    ValidateManifestError::CustomIndexingInvalidConditionExpression(
                                        condition_expr.to_string(),
                                        event_mapping.event.clone(),
                                        table.name.clone(),
                                        contract.name.clone(),
                                        e.to_string(),
                                    ),
                                );
                            }

                            // Extract and validate variable references in the condition
                            // Variables in filter expressions don't use $ prefix
                            let variables = extract_filter_variables(condition_expr);
                            for var_name in variables {
                                // Strip array indices like ids[0] -> ids
                                let root_field = var_name
                                    .split(&['.', '['][..])
                                    .next()
                                    .unwrap_or(&var_name);
                                // Skip validation for built-in metadata fields
                                if BUILTIN_METADATA_FIELDS.contains(&root_field) {
                                    continue;
                                }
                                // Also accept iterate aliases
                                if iterate_aliases.contains(root_field) {
                                    continue;
                                }
                                if !event_input_names.contains(root_field) {
                                    return Err(
                                        ValidateManifestError::CustomIndexingEventFieldNotFound(
                                            var_name,
                                            event_mapping.event.clone(),
                                            table.name.clone(),
                                            contract.name.clone(),
                                        ),
                                    );
                                }
                            }
                        }

                        // Validate set columns
                        for set_col in &operation.set {
                            // Check table column exists
                            if !table_column_names.contains(set_col.column.as_str()) {
                                return Err(ValidateManifestError::CustomIndexingFieldNotFound(
                                    set_col.column.clone(),
                                    event_mapping.event.clone(),
                                    table.name.clone(),
                                    contract.name.clone(),
                                ));
                            }

                            // Get the effective value (handles increment/decrement defaults)
                            let effective_value = set_col.effective_value();

                            // Check for arithmetic expression (e.g., "$value * 2", "$amount + $fee")
                            if is_arithmetic_expression(effective_value) {
                                // Validate the expression parses correctly
                                if let Err(e) = parse_arithmetic_expression(effective_value) {
                                    return Err(
                                        ValidateManifestError::CustomIndexingInvalidArithmeticExpression(
                                            effective_value.to_string(),
                                            event_mapping.event.clone(),
                                            table.name.clone(),
                                            contract.name.clone(),
                                            e.to_string(),
                                        ),
                                    );
                                }

                                // Extract and validate all variable references
                                let variables = extract_arithmetic_variables(effective_value);
                                for var_name in variables {
                                    // Strip array indices like ids[0] -> ids
                                    let root_field = var_name
                                        .split(&['.', '['][..])
                                        .next()
                                        .unwrap_or(&var_name);
                                    // Skip validation for built-in metadata fields
                                    if BUILTIN_METADATA_FIELDS.contains(&root_field) {
                                        continue;
                                    }
                                    // Also accept iterate aliases
                                    if iterate_aliases.contains(root_field) {
                                        continue;
                                    }
                                    if !event_input_names.contains(root_field) {
                                        return Err(
                                            ValidateManifestError::CustomIndexingEventFieldNotFound(
                                                var_name,
                                                event_mapping.event.clone(),
                                                table.name.clone(),
                                                contract.name.clone(),
                                            ),
                                        );
                                    }
                                }
                            } else if effective_value.starts_with('$') {
                                // Skip validation for view calls ($call(...))
                                if effective_value.starts_with("$call(") {
                                    continue;
                                }
                                // Simple event field reference
                                let event_field = &effective_value[1..];
                                // For nested fields like $data.amount, validate the root field
                                // Also strip array indices like ids[0] -> ids
                                let root_field = event_field
                                    .split(&['.', '['][..])
                                    .next()
                                    .unwrap_or(event_field);
                                // Skip validation for built-in metadata fields
                                if BUILTIN_METADATA_FIELDS.contains(&root_field) {
                                    continue;
                                }
                                // Also accept iterate aliases
                                if iterate_aliases.contains(root_field) {
                                    continue;
                                }
                                if !event_input_names.contains(root_field) {
                                    return Err(
                                        ValidateManifestError::CustomIndexingEventFieldNotFound(
                                            event_field.to_string(),
                                            event_mapping.event.clone(),
                                            table.name.clone(),
                                            contract.name.clone(),
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(postgres) = &manifest.storage.postgres {
        if let Some(relationships) = &postgres.relationships {
            for relationship in relationships {
                if !manifest.all_contracts().iter().any(|c| c.name == relationship.contract_name) {
                    return Err(ValidateManifestError::RelationshipContractNotFound(
                        relationship.contract_name.clone(),
                    ));
                }

                for foreign_key in &relationship.foreign_keys {
                    if !manifest.all_contracts().iter().any(|c| c.name == foreign_key.contract_name)
                    {
                        return Err(ValidateManifestError::RelationshipForeignKeyContractNotFound(
                            foreign_key.contract_name.clone(),
                        ));
                    }
                }

                // TODO - Add validation for the event names and event inputs match the ABIs
            }
        }
    }

    if let Some(contracts) = &manifest.global.contracts {
        for contract in contracts {
            match &contract.abi {
                StringOrArray::Single(_) => {}
                StringOrArray::Multiple(value) => {
                    return Err(ValidateManifestError::GlobalAbiCanOnlyBeASingleString(format!(
                        "Global ABI can only be a single string but found multiple: {value:?}"
                    )));
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

    // Assign networks to the Native Transfer if opted into without defining networks.
    // We treat None as "All available".
    manifest_after_transform.set_native_transfer_networks();

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
