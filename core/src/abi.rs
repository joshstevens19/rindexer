use std::{collections::HashSet, fs, path::Path};

use alloy::{
    primitives::{keccak256, B256},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};

use crate::{
    database::postgres::{
        generate::solidity_type_to_db_type,
        sql_type_wrapper::{solidity_type_to_ethereum_sql_type_wrapper, EthereumSqlTypeWrapper},
    },
    helpers::camel_to_snake,
    manifest::contract::{Contract, ParseAbiError},
};

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
                if let Some(components) = &input.components {
                    let new_path = path.clone().map_or_else(
                        || vec![AbiNamePropertiesPath::new(&input.name, &input.type_)],
                        |mut p| {
                            p.push(AbiNamePropertiesPath::new(&input.name, &input.type_));
                            p
                        },
                    );

                    ABIInput::generate_abi_name_properties(
                        components,
                        properties_type,
                        Some(new_path),
                    )
                } else {
                    let prefix = path.as_ref().map(|p| {
                        p.iter()
                            .map(|pp| camel_to_snake(&pp.abi_name))
                            .collect::<Vec<_>>()
                            .join("_")
                    });

                    match properties_type {
                        GenerateAbiPropertiesType::PostgresWithDataTypes => {
                            let value = format!(
                                "\"{}{}\" {}",
                                prefix.map_or_else(|| "".to_string(), |p| format!("{p}_")),
                                camel_to_snake(&input.name),
                                solidity_type_to_db_type(&input.type_)
                            );

                            vec![AbiProperty::new(value, &input.name, &input.type_, path.clone())]
                        }
                        GenerateAbiPropertiesType::PostgresColumnsNamesOnly
                        | GenerateAbiPropertiesType::CsvHeaderNames => {
                            let value = format!(
                                "{}{}",
                                prefix.map_or_else(|| "".to_string(), |p| format!("{p}_")),
                                camel_to_snake(&input.name),
                            );

                            vec![AbiProperty::new(value, &input.name, &input.type_, path.clone())]
                        }
                        GenerateAbiPropertiesType::Object => {
                            let value = format!(
                                "{}{}",
                                prefix.map_or_else(|| "".to_string(), |p| format!("{p}.")),
                                camel_to_snake(&input.name),
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

        let filtered_abi_items = match &contract.include_events {
            Some(events) => abi_items
                .into_iter()
                .filter(|item| {
                    item.type_ != "event"
                        || events
                            .iter()
                            .map(|a| a.name.clone())
                            .collect::<Vec<_>>()
                            .contains(&item.name)
                })
                .collect(),
            None => abi_items,
        };

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
                            return Ok(GetAbiItemWithDbMap {
                                abi_item: input.clone(),
                                db_column_name,
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
