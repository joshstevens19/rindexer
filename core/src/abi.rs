use std::{collections::HashSet, fs, path::Path};

use alloy::{
    primitives::{keccak256, B256},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};

use crate::database::clickhouse::generate::solidity_type_to_clickhouse_type;
use crate::database::sql_type_wrapper::{
    solidity_type_to_ethereum_sql_type_wrapper, EthereumSqlTypeWrapper,
};
use crate::{
    database::postgres::generate::solidity_type_to_db_type,
    helpers::camel_to_snake,
    manifest::contract::{Contract, ParseAbiError},
};

/// Columns rindexer writes into every event table itself.
///
/// An ABI input whose snake-cased column name matches one of these would be
/// emitted twice in the same `CREATE TABLE`, which the database rejects. Keep
/// in sync with the event-table DDL in `database/generate.rs` (Postgres) and
/// `database/clickhouse/generate.rs`; the list is the union of both so the two
/// backends stay column-for-column identical for the same ABI.
pub const RESERVED_EVENT_COLUMN_NAMES: [&str; 9] = [
    "rindexer_id",
    "contract_address",
    "tx_hash",
    "block_number",
    "block_timestamp",
    "block_hash",
    "network",
    "tx_index",
    "log_index",
];

/// Suffix appended to an ABI-derived column that would collide with one of
/// [`RESERVED_EVENT_COLUMN_NAMES`].
pub const RESERVED_COLUMN_COLLISION_SUFFIX: &str = "_abi";

/// Returns the column name to store `column_name` under, renaming it when it
/// would collide with a column rindexer adds itself.
///
/// Applied to the fully-qualified name, so a flattened tuple field such as
/// `block.number` -> `block_number` is caught as well as a top-level input.
pub fn db_safe_column_name(column_name: &str) -> String {
    if RESERVED_EVENT_COLUMN_NAMES.contains(&column_name) {
        format!("{column_name}{RESERVED_COLUMN_COLLISION_SUFFIX}")
    } else {
        column_name.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed: Option<bool>,

    pub name: String,

    #[serde(rename = "type")]
    pub type_: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub components: Option<Vec<ABIInput>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ParamTypeError {
    #[error("tuple type specified but no components found")]
    MissingComponents,
}

#[derive(PartialEq)]
pub enum GenerateAbiPropertiesType {
    PostgresWithDataTypes,
    PostgresColumnsNamesOnly,
    CsvHeaderNames,
    Object,
    ClickhouseWithDataTypes,
}

#[derive(Debug, Clone)]
pub struct AbiNamePropertiesPath {
    pub abi_type: String,
    pub abi_name: String,
}

impl AbiNamePropertiesPath {
    pub fn new(abi_name: &str, abi_type: &str) -> Self {
        Self { abi_type: abi_type.to_string(), abi_name: abi_name.to_string() }
    }
}

#[derive(Debug)]
pub struct AbiProperty {
    pub value: String,
    pub abi_type: String,
    pub abi_name: String,
    /// The path to the property, none if the property is at the root level.
    pub path: Option<Vec<AbiNamePropertiesPath>>,
    pub ethereum_sql_type_wrapper: Option<EthereumSqlTypeWrapper>,
}

impl AbiProperty {
    pub fn new(
        value: String,
        name: &str,
        abi_type: &str,
        path: Option<Vec<AbiNamePropertiesPath>>,
    ) -> Self {
        let has_array_in_path =
            path.as_ref().is_some_and(|p| p.iter().any(|pp| pp.abi_type.ends_with("[]")));
        let adjusted_abi_type =
            if has_array_in_path { format!("{abi_type}[]") } else { abi_type.to_string() };

        Self {
            value,
            ethereum_sql_type_wrapper: solidity_type_to_ethereum_sql_type_wrapper(
                &adjusted_abi_type,
            ),
            abi_type: abi_type.to_string(),
            abi_name: name.to_string(),
            path,
        }
    }
}

impl ABIInput {
    pub fn format_param_type(&self) -> Result<String, ParamTypeError> {
        match self.type_.as_str() {
            "tuple" => {
                let components =
                    self.components.as_ref().ok_or(ParamTypeError::MissingComponents)?;
                let formatted_components = components
                    .iter()
                    .map(|component| component.format_param_type())
                    .collect::<Result<Vec<_>, ParamTypeError>>()?
                    .join(",");
                Ok(format!("({formatted_components})"))
            }
            _ => Ok(self.type_.to_string()),
        }
    }

    pub fn generate_abi_name_properties(
        inputs: &[ABIInput],
        properties_type: &GenerateAbiPropertiesType,
        path: Option<Vec<AbiNamePropertiesPath>>,
    ) -> Vec<AbiProperty> {
        inputs
            .iter()
            .flat_map(|input| {
                // For tuple arrays (both dynamic tuple[] and fixed-size tuple[N]),
                // don't flatten - store as JSONB since array length varies
                let is_tuple_array = input.type_.starts_with("tuple[");

                // Regular tuples with components get flattened into columns via recursion
                // Tuple arrays skip this and are treated as leaf nodes for JSONB storage
                if let Some(components) = &input.components {
                    if !is_tuple_array {
                        let new_path = path.clone().map_or_else(
                            || vec![AbiNamePropertiesPath::new(&input.name, &input.type_)],
                            |mut p| {
                                p.push(AbiNamePropertiesPath::new(&input.name, &input.type_));
                                p
                            },
                        );

                        return ABIInput::generate_abi_name_properties(
                            components,
                            properties_type,
                            Some(new_path),
                        );
                    }
                }

                // Leaf node (primitive type, or tuple[] which is stored as JSONB)
                {
                    let prefix = path.as_ref().map(|p| {
                        p.iter()
                            .map(|pp| camel_to_snake(&pp.abi_name))
                            .collect::<Vec<_>>()
                            .join("_")
                    });

                    let snake_name = camel_to_snake(&input.name);

                    // Postgres, ClickHouse and CSV all share this name so the
                    // three stay column-for-column identical for the same ABI.
                    let column_name = db_safe_column_name(&match &prefix {
                        Some(p) => format!("{p}_{snake_name}"),
                        None => snake_name.clone(),
                    });

                    match properties_type {
                        GenerateAbiPropertiesType::PostgresWithDataTypes => {
                            let value = format!(
                                "\"{}\" {}",
                                column_name,
                                solidity_type_to_db_type(&input.type_)
                            );

                            vec![AbiProperty::new(value, &input.name, &input.type_, path.clone())]
                        }
                        GenerateAbiPropertiesType::PostgresColumnsNamesOnly
                        | GenerateAbiPropertiesType::CsvHeaderNames => {
                            vec![AbiProperty::new(
                                column_name,
                                &input.name,
                                &input.type_,
                                path.clone(),
                            )]
                        }
                        // Not renamed: this builds a Rust field-access path
                        // against the alloy-generated event struct, whose
                        // fields carry the original ABI names.
                        GenerateAbiPropertiesType::Object => {
                            let value = format!(
                                "{}{}",
                                prefix.as_ref().map_or_else(|| "".to_string(), |p| format!("{p}.")),
                                snake_name,
                            );

                            vec![AbiProperty::new(value, &input.name, &input.type_, path.clone())]
                        }
                        GenerateAbiPropertiesType::ClickhouseWithDataTypes => {
                            let value = format!(
                                "\"{}\" {}",
                                column_name,
                                solidity_type_to_clickhouse_type(&input.type_)
                            );

                            vec![AbiProperty::new(value, &input.name, &input.type_, path.clone())]
                        }
                    }
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIItem {
    #[serde(default)]
    pub inputs: Vec<ABIInput>,

    #[serde(default)]
    pub name: String,

    #[serde(rename = "type", default)]
    pub type_: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ReadAbiError {
    #[error("Could not find ABI path: {0}")]
    AbiPathDoesNotExist(String),

    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("{0}")]
    ParseAbiError(#[from] ParseAbiError),
}

impl ABIItem {
    pub fn format_event_signature(&self) -> Result<String, ParamTypeError> {
        let name = &self.name;
        let params = self
            .inputs
            .iter()
            .map(Self::format_param_type)
            .collect::<Result<Vec<_>, _>>()?
            .join(",");

        Ok(format!("{name}({params})"))
    }

    fn format_param_type(input: &ABIInput) -> Result<String, ParamTypeError> {
        let base_type = input.type_.split('[').next().unwrap_or(&input.type_);
        let array_suffix = input.type_.strip_prefix(base_type).unwrap_or("");

        let type_str = match base_type {
            "tuple" => {
                let inner = input
                    .components
                    .as_ref()
                    .ok_or(ParamTypeError::MissingComponents)?
                    .iter()
                    .map(Self::format_param_type)
                    .collect::<Result<Vec<_>, _>>()?
                    .join(",");
                format!("({inner})")
            }
            _ => base_type.to_string(),
        };

        Ok(format!("{type_str}{array_suffix}"))
    }

    pub fn extract_event_names_and_signatures_from_abi(
        abi_json: Vec<ABIItem>,
    ) -> Result<Vec<EventInfo>, ParamTypeError> {
        let mut events = Vec::new();
        for item in abi_json.into_iter() {
            if item.type_ == "event" {
                events.push(EventInfo::new(item)?);
            }
        }
        Ok(events)
    }

    pub fn read_abi_items(
        project_path: &Path,
        contract: &Contract,
    ) -> Result<Vec<ABIItem>, ReadAbiError> {
        let abi_str = contract.parse_abi(project_path)?;
        let abi_items: Vec<ABIItem> = serde_json::from_str(&abi_str)?;

        // Get events from include_events
        let include_event_names: Vec<String> = contract
            .include_events
            .as_ref()
            .map(|events| events.iter().map(|a| a.name.clone()).collect())
            .unwrap_or_default();

        // Get events from tables
        let table_event_names = contract.get_table_event_names();

        // If both are empty, return all events (backward compatible behavior)
        if include_event_names.is_empty() && table_event_names.is_empty() {
            return Ok(abi_items);
        }

        // Combine both sets of event names
        let all_needed_events: HashSet<String> =
            include_event_names.into_iter().chain(table_event_names).collect();

        // Filter to only include needed events (keep all non-event items)
        let filtered_abi_items = abi_items
            .into_iter()
            .filter(|item| item.type_ != "event" || all_needed_events.contains(&item.name))
            .collect();

        Ok(filtered_abi_items)
    }

    pub fn get_abi_items(
        project_path: &Path,
        contract: &Contract,
        is_filter: bool,
    ) -> Result<Vec<ABIItem>, ReadAbiError> {
        let mut abi_items = ABIItem::read_abi_items(project_path, contract)?;

        if is_filter {
            let filter_event_names: HashSet<String> = contract
                .details
                .iter()
                .filter_map(|detail| detail.filter.clone())
                .flat_map(|events| match events {
                    ValueOrArray::Value(event) => vec![event.event_name],
                    ValueOrArray::Array(event_array) => {
                        event_array.into_iter().map(|e| e.event_name).collect()
                    }
                })
                .collect();

            abi_items = abi_items
                .iter()
                .filter(|item| item.type_ == "event" && filter_event_names.contains(&item.name))
                .cloned()
                .collect();
        }

        Ok(abi_items)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventInfo {
    pub name: String,
    pub inputs: Vec<ABIInput>,
    signature: String,
    struct_result: String,
    struct_data: String,
}

#[derive(thiserror::Error, Debug)]
pub enum CreateCsvFileForEvent {
    #[error("Could not create the dir {0}")]
    CreateDirFailed(std::io::Error),
}

impl EventInfo {
    pub fn new(item: ABIItem) -> Result<Self, ParamTypeError> {
        let struct_result = format!("{}Result", item.name);
        let struct_data = format!("{}Data", item.name);
        let signature = item.format_event_signature()?;
        Ok(EventInfo {
            name: item.name,
            inputs: item.inputs,
            signature,
            struct_result,
            struct_data,
        })
    }

    pub fn topic_id(&self) -> B256 {
        let event_signature = self.signature.clone();

        keccak256(event_signature)
    }

    pub fn topic_id_as_hex_string(&self) -> String {
        format!("{:x}", self.topic_id())
    }

    pub fn struct_result(&self) -> &str {
        &self.struct_result
    }

    pub fn struct_data(&self) -> &str {
        &self.struct_data
    }

    pub fn csv_headers_for_event(&self) -> Vec<String> {
        let mut headers: Vec<String> = ABIInput::generate_abi_name_properties(
            &self.inputs,
            &GenerateAbiPropertiesType::CsvHeaderNames,
            None,
        )
        .into_iter()
        .map(|m| m.value)
        .collect();

        headers.insert(0, r#"contract_address"#.to_string());
        headers.push(r#"tx_hash"#.to_string());
        headers.push(r#"block_number"#.to_string());
        headers.push(r#"block_hash"#.to_string());
        headers.push(r#"network"#.to_string());
        headers.push(r#"tx_index"#.to_string());
        headers.push(r#"log_index"#.to_string());

        headers
    }

    pub fn create_csv_file_for_event(
        &self,
        project_path: &Path,
        contract_name: &str,
        csv_path: &str,
    ) -> Result<String, CreateCsvFileForEvent> {
        let csv_file_name = format!("{}-{}.csv", contract_name, self.name).to_lowercase();
        let csv_folder = project_path.join(csv_path).join(contract_name);

        // Create directory if it does not exist.
        if let Err(e) = fs::create_dir_all(&csv_folder) {
            return Err(CreateCsvFileForEvent::CreateDirFailed(e));
        }

        // Create last-synced-blocks if it does not exist.
        if let Err(e) = fs::create_dir_all(csv_folder.join("last-synced-blocks")) {
            return Err(CreateCsvFileForEvent::CreateDirFailed(e));
        }

        Ok(csv_folder.join(csv_file_name).display().to_string())
    }
}

pub struct GetAbiItemWithDbMap {
    pub abi_item: ABIInput,
    pub db_column_name: String,
}

#[derive(thiserror::Error, Debug)]
pub enum GetAbiItemWithDbMapError {
    #[error("Parameter not found: {0}")]
    ParameterNotFound(String),
}

pub fn get_abi_item_with_db_map(
    abi_items: &[ABIItem],
    event_name: &str,
    parameter_mapping: &[&str],
) -> Result<GetAbiItemWithDbMap, GetAbiItemWithDbMapError> {
    let event_item = abi_items.iter().find(|item| item.name == event_name && item.type_ == "event");

    match event_item {
        Some(item) => {
            let mut current_inputs = &item.inputs;
            let mut db_column_name = String::new();

            for param in parameter_mapping {
                match current_inputs.iter().find(|input| input.name == *param) {
                    Some(input) => {
                        if !db_column_name.is_empty() {
                            db_column_name.push('_');
                        }
                        db_column_name.push_str(&camel_to_snake(&input.name));

                        if param
                            == parameter_mapping
                                .last()
                                .expect("Parameter mapping should have at least one element")
                        {
                            // Must match the column naming in
                            // `generate_abi_name_properties`, otherwise the
                            // index/relationship SQL built from this name
                            // would reference a column that does not exist.
                            return Ok(GetAbiItemWithDbMap {
                                abi_item: input.clone(),
                                db_column_name: db_safe_column_name(&db_column_name),
                            });
                        } else {
                            current_inputs = match input.type_.as_str() {
                                "tuple" => {
                                    if let Some(ref components) = input.components {
                                        components
                                    } else {
                                        return Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
                                            "Parameter {param} is not a nested structure in event {event_name} of contract"
                                        )));
                                    }
                                },
                                _ => return Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
                                    "Parameter {param} is not a nested structure in event {event_name} of contract"
                                ))),
                            };
                        }
                    }
                    None => {
                        return Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
                            "Parameter {param} not found in event {event_name} of contract"
                        )));
                    }
                }
            }

            Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
                "Parameter {} not found in event {} of contract",
                parameter_mapping.join("."),
                event_name
            )))
        }
        None => Err(GetAbiItemWithDbMapError::ParameterNotFound(format!(
            "Event {event_name} not found in contract ABI"
        ))),
    }
}

#[cfg(test)]
mod reserved_column_tests {
    use super::*;

    fn input(name: &str, type_: &str) -> ABIInput {
        ABIInput {
            indexed: None,
            name: name.to_string(),
            type_: type_.to_string(),
            components: None,
        }
    }

    fn tuple(name: &str, components: Vec<ABIInput>) -> ABIInput {
        ABIInput {
            indexed: None,
            name: name.to_string(),
            type_: "tuple".to_string(),
            components: Some(components),
        }
    }

    fn names(inputs: &[ABIInput], kind: GenerateAbiPropertiesType) -> Vec<String> {
        ABIInput::generate_abi_name_properties(inputs, &kind, None)
            .into_iter()
            .map(|p| p.value)
            .collect()
    }

    #[test]
    fn leaves_non_reserved_names_untouched() {
        assert_eq!(db_safe_column_name("amount"), "amount");
        assert_eq!(db_safe_column_name("from_address"), "from_address");
    }

    #[test]
    fn renames_every_reserved_column() {
        for reserved in RESERVED_EVENT_COLUMN_NAMES {
            assert_eq!(db_safe_column_name(reserved), format!("{reserved}_abi"));
        }
    }

    /// An ABI input named after one of rindexer's own columns used to emit that
    /// column twice in the same `CREATE TABLE`, which the database rejects.
    #[test]
    fn reserved_abi_input_does_not_duplicate_a_rindexer_column() {
        let inputs = vec![input("network", "string"), input("blockNumber", "uint256")];

        let columns = names(&inputs, GenerateAbiPropertiesType::PostgresColumnsNamesOnly);
        assert_eq!(columns, vec!["network_abi", "block_number_abi"]);

        for column in &columns {
            assert!(
                !RESERVED_EVENT_COLUMN_NAMES.contains(&column.as_str()),
                "`{column}` still collides with a rindexer column"
            );
        }
    }

    /// Flattened tuple fields are renamed on their full path, so a tuple named
    /// `block` with a field `number` cannot collide with `block_number` either.
    #[test]
    fn flattened_tuple_path_is_checked_not_just_the_leaf_name() {
        let inputs = vec![tuple("block", vec![input("number", "uint256")])];

        assert_eq!(
            names(&inputs, GenerateAbiPropertiesType::PostgresColumnsNamesOnly),
            vec!["block_number_abi"]
        );
    }

    #[test]
    fn nested_leaf_named_after_a_reserved_column_is_safe_when_prefixed() {
        let inputs = vec![tuple("meta", vec![input("network", "string")])];

        assert_eq!(
            names(&inputs, GenerateAbiPropertiesType::PostgresColumnsNamesOnly),
            vec!["meta_network"]
        );
    }

    #[test]
    fn postgres_clickhouse_and_csv_agree_on_the_column_name() {
        let inputs = vec![input("network", "string")];

        assert_eq!(
            names(&inputs, GenerateAbiPropertiesType::CsvHeaderNames),
            names(&inputs, GenerateAbiPropertiesType::PostgresColumnsNamesOnly)
        );
        assert!(names(&inputs, GenerateAbiPropertiesType::PostgresWithDataTypes)[0]
            .starts_with("\"network_abi\" "));
        assert!(names(&inputs, GenerateAbiPropertiesType::ClickhouseWithDataTypes)[0]
            .starts_with("\"network_abi\" "));
    }

    /// Index and relationship SQL is built from `get_abi_item_with_db_map`, so
    /// it has to resolve to the same renamed column as the `CREATE TABLE`.
    #[test]
    fn db_map_resolves_to_the_renamed_column() {
        let abi_items = vec![ABIItem {
            type_: "event".to_string(),
            name: "Transfer".to_string(),
            inputs: vec![input("network", "string"), input("amount", "uint256")],
        }];

        let mapped = get_abi_item_with_db_map(&abi_items, "Transfer", &["network"]).unwrap();
        assert_eq!(mapped.db_column_name, "network_abi");

        let untouched = get_abi_item_with_db_map(&abi_items, "Transfer", &["amount"]).unwrap();
        assert_eq!(untouched.db_column_name, "amount");
    }

    /// `Object` builds a Rust field-access path against the alloy-generated
    /// event struct, whose fields keep the original ABI names, so it must never
    /// pick up the database rename.
    #[test]
    fn object_paths_keep_the_original_abi_name() {
        let inputs =
            vec![input("network", "string"), tuple("meta", vec![input("txHash", "bytes32")])];

        assert_eq!(
            names(&inputs, GenerateAbiPropertiesType::Object),
            vec!["network", "meta.tx_hash"]
        );
    }
}
