use crate::database::postgres::{
    generate_insert_query_for_event, solidity_type_to_db_type,
    solidity_type_to_ethereum_sql_type_wrapper,
};
use crate::generator::event_callback_registry::IndexingContractSetup;
use crate::EthereumSqlTypeWrapper;
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::iter::Map;
use std::path::Path;

use crate::helpers::camel_to_snake;
use crate::manifest::yaml::{Contract, ContractDetails, CsvDetails, Storage};
use crate::types::code::Code;

use super::networks_bindings::network_provider_fn_name_by_name;

/// Struct representing an ABI item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIItem {
    #[serde(default)]
    inputs: Vec<ABIInput>,
    #[serde(default)]
    name: String,
    #[serde(rename = "type", default)]
    type_: String,
}

#[derive(thiserror::Error, Debug)]
pub enum ReadAbiError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),
}

/// Reads and filters ABI items from the contract's ABI file.
///
/// This function reads the ABI JSON string from the file specified in the `contract.abi` path,
/// deserializes it into a vector of `ABIItem`, and filters the items based on the `include_events`
/// option in the contract. If `include_events` is `Some`, only the events listed in `include_events`
/// will be included along with all non-event items. If `include_events` is `None`, all items will
/// be included.
///
/// # Arguments
///
/// * `contract` - A reference to a `Contract` struct containing the ABI file path and an optional
///                list of event names to include.
///
pub fn read_abi_items(contract: &Contract) -> Result<Vec<ABIItem>, ReadAbiError> {
    // Read the ABI JSON string from the file
    let abi_str = fs::read_to_string(&contract.abi).map_err(ReadAbiError::CouldNotReadAbiString)?;
    // Deserialize the JSON string to a vector of ABI items
    let abi_items: Vec<ABIItem> =
        serde_json::from_str(&abi_str).map_err(ReadAbiError::CouldNotReadAbiJson)?;

    // Filter the ABI items
    let filtered_abi_items = match &contract.include_events {
        Some(events) => abi_items
            .into_iter()
            .filter(|item| item.type_ != "event" || events.contains(&item.name))
            .collect(),
        None => abi_items,
    };

    // Return the filtered ABI items
    Ok(filtered_abi_items)
}

/// Struct representing an ABI input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIInput {
    pub indexed: Option<bool>,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub components: Option<Vec<ABIInput>>,
}

/// Struct representing information about an event.
#[derive(Debug)]
pub struct EventInfo {
    pub name: String,
    pub inputs: Vec<ABIInput>,
    signature: String,
    struct_result: String,
    struct_data: String,
}

impl EventInfo {
    /// Creates a new `EventInfo`.
    ///
    /// # Arguments
    ///
    /// * `item` - The ABI item.
    /// * `signature` - The event signature.
    ///
    /// # Returns
    ///
    /// A new `EventInfo`.
    pub fn new(item: &ABIItem, signature: String) -> Self {
        EventInfo {
            name: item.name.clone(),
            inputs: item.inputs.clone(),
            signature,
            struct_result: format!("{}Result", item.name),
            struct_data: format!("{}Data", item.name),
        }
    }

    pub fn topic_id(&self) -> String {
        let event_signature = format!("{}({})", self.name, self.signature);
        compute_topic_id(&event_signature)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParamTypeError {
    #[error("tuple type specified but no components found")]
    MissingComponents,
}

fn format_param_type(input: &ABIInput) -> Result<String, ParamTypeError> {
    match input.type_.as_str() {
        "tuple" => {
            let components = input
                .components
                .as_ref()
                .ok_or(ParamTypeError::MissingComponents)?;
            let formatted_components = components
                .iter()
                .map(format_param_type)
                .collect::<Result<Vec<_>, ParamTypeError>>()?
                .join(",");
            Ok(format!("({})", formatted_components))
        }
        _ => Ok(input.type_.to_string()),
    }
}

/// Computes the topic ID for an event signature.
///
/// # Arguments
///
/// * `event_signature` - The event signature.
///
/// # Returns
///
/// A string representing the topic ID.
fn compute_topic_id(event_signature: &str) -> String {
    Map::collect(
        keccak256(event_signature)
            .iter()
            .map(|byte| format!("{:02x}", byte)),
    )
}

/// Formats the event signature.
///
/// # Arguments
///
/// * `item` - The ABI item.
///
/// # Returns
///
/// A formatted string representing the event signature.
fn format_event_signature(item: &ABIItem) -> Result<String, ParamTypeError> {
    let formatted_inputs = item
        .inputs
        .iter()
        .map(format_param_type)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(formatted_inputs.join(","))
}
/// Generates the contract name for ABI generation.
///
/// # Arguments
///
/// * `contract` - The contract.
///
/// # Returns
///
/// A string representing the contract name for ABI generation.
pub fn abigen_contract_name(contract: &Contract) -> String {
    format!("Rindexer{}Gen", contract.name)
}

/// Generates the module name for the contract.
///
/// # Arguments
///
/// * `contract` - The contract.
///
/// # Returns
///
/// A string representing the module name for the contract.
fn abigen_contract_mod_name(contract: &Contract) -> String {
    camel_to_snake(&abigen_contract_name(contract))
}

/// Generates the file name for the contract ABI.
///
/// # Arguments
///
/// * `contract` - The contract.
///
/// # Returns
///
/// A string representing the file name for the contract ABI.
pub fn abigen_contract_file_name(contract: &Contract) -> String {
    format!("{}_abi_gen", camel_to_snake(&contract.name))
}

pub fn extract_event_names_and_signatures_from_abi(
    abi_json: &[ABIItem],
) -> Result<Vec<EventInfo>, ParamTypeError> {
    let mut events = Vec::new();
    for item in abi_json.iter() {
        if item.type_ == "event" {
            let signature = format_event_signature(item)?;
            events.push(EventInfo::new(item, signature));
        }
    }
    Ok(events)
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateStructsError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),

    #[error("Invalid ABI JSON format")]
    InvalidAbiJsonFormat,
}

/// Generates Rust structs for the events in a contract.
///
/// # Arguments
///
/// * `contract` - The contract.
///
fn generate_structs(contract: &Contract) -> Result<Code, GenerateStructsError> {
    let abi_str =
        fs::read_to_string(&contract.abi).map_err(GenerateStructsError::CouldNotReadAbiString)?;
    let abi_json: Value =
        serde_json::from_str(&abi_str).map_err(GenerateStructsError::CouldNotReadAbiJson)?;

    let mut structs = Code::blank();

    for item in abi_json
        .as_array()
        .ok_or(GenerateStructsError::InvalidAbiJsonFormat)?
        .iter()
    {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_result = format!("{}Result", event_name);
            let struct_data = format!("{}Data", event_name);

            structs.push_str(&Code::new(format!(
                r#"
                    pub type {struct_data} = {abigen_mod_name}::{event_name}Filter;

                    #[derive(Debug)]
                    pub struct {struct_result} {{
                        pub event_data: {struct_data},
                        pub tx_information: TxInformation
                    }}
                "#,
                struct_result = struct_result,
                struct_data = struct_data,
                abigen_mod_name = abigen_contract_mod_name(contract),
                event_name = event_name
            )));
        }
    }

    Ok(structs)
}

/// Generates Rust enum variants for the events.
///
/// # Arguments
///
/// * `event_info` - The event information.
///
fn generate_event_enums_code(event_info: &[EventInfo]) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| format!("{}({}Event<TExtensions>),", info.name, info.name))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Generates the event type name.
///
/// # Arguments
///
/// * `name` - The name of the event.
///
fn generate_event_type_name(name: &str) -> String {
    format!("{}EventType", name)
}

/// Generates match arms for topic IDs.
///
/// # Arguments
///
/// * `event_type_name` - The event type name.
/// * `event_info` - The event information.
///
fn generate_topic_ids_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| {
                format!(
                    "{}::{}(_) => \"0x{}\",",
                    event_type_name,
                    info.name,
                    info.topic_id()
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Generates match arms for event names.
///
/// # Arguments
///
/// * `event_type_name` - The event type name.
/// * `event_info` - The event information.
///
fn generate_event_names_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| {
                format!(
                    "{}::{}(_) => \"{}\",",
                    event_type_name, info.name, info.name
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Generates match arms for event registration.
///
/// # Arguments
///
/// * `event_type_name` - The event type name.
/// * `event_info` - The event information.
///
fn generate_register_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| {
                format!(
                    r#"
                    {}::{}(event) => {{
                        let event = Arc::new(event);
                        Arc::new(move |result| {{
                            let event = event.clone();
                            async move {{ event.call(result).await }}.boxed()
                        }})
                    }},
                "#,
                    event_type_name, info.name
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Generates match arms for event decoders.
///
/// # Arguments
///
/// * `event_type_name` - The event type name.
/// * `event_info` - The event information.
///
fn generate_decoder_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    Code::new(event_info
        .iter()
        .map(|info| {
            format!(
                r#"
                    {event_type_name}::{event_info_name}(_) => {{
                        Arc::new(move |topics: Vec<H256>, data: Bytes| {{
                            match contract.decode_event::<{event_info_name}Data>("{event_info_name}", topics, data) {{
                                Ok(filter) => Arc::new(filter) as Arc<dyn Any + Send + Sync>,
                                Err(error) => Arc::new(error) as Arc<dyn Any + Send + Sync>,
                            }}
                        }})
                    }}
                "#,
                event_type_name = event_type_name,
                event_info_name = info.name
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Generates a string representation of an optional vector of strings.
///
/// # Arguments
///
/// * `indexed` - The optional vector of strings.
///
/// # Returns
///
/// A string representation of the vector.
fn generate_indexed_vec_string(indexed: &Option<Vec<String>>) -> Code {
    match indexed {
        Some(values) => Code::new(format!(
            "Some(vec![{}])",
            values
                .iter()
                .map(|s| format!("\"{}\".to_string()", s))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        None => Code::new("None".to_string()),
    }
}

fn generate_contract_type_fn_code(contract: &Contract) -> Code {
    let mut details = String::new();
    details.push_str("vec![");

    for detail in &contract.details {
        let start_block = match detail.start_block {
            Some(start_block) => format!("Some({}.into())", start_block.as_u64()),
            None => "None".to_string(),
        };
        let end_block = match detail.end_block {
            Some(end_block) => format!("Some({}.into())", end_block.as_u64()),
            None => "None".to_string(),
        };
        let polling_every = detail.polling_every.unwrap_or(1000);

        let item = match detail.indexing_contract_setup() {
            IndexingContractSetup::Address(address) => format!(
                r#"
                ContractDetails::new_with_address(
                    "{network}".to_string(),
                    "{address}".to_string(),
                    {start_block},
                    {end_block},
                    Some({polling_every}),
                ),
                "#,
                network = detail.network,
                address = address,
                start_block = start_block,
                end_block = end_block,
                polling_every = polling_every
            ),
            IndexingContractSetup::Filter(filter) => {
                let indexed_1 = generate_indexed_vec_string(&filter.indexed_1);
                let indexed_2 = generate_indexed_vec_string(&filter.indexed_2);
                let indexed_3 = generate_indexed_vec_string(&filter.indexed_3);

                format!(
                    r#"
                    ContractDetails::new_with_filter(
                        "{network}".to_string(),
                        FilterDetails {{
                            event_name: "{event_name}".to_string(),
                            indexed_1: {indexed_1},
                            indexed_2: {indexed_2},
                            indexed_3: {indexed_3},
                        }},
                        {start_block},
                        {end_block},
                        Some({polling_every}),
                    ),
                    "#,
                    network = detail.network,
                    event_name = filter.event_name,
                    indexed_1 = indexed_1,
                    indexed_2 = indexed_2,
                    indexed_3 = indexed_3,
                    start_block = start_block,
                    end_block = end_block,
                    polling_every = polling_every
                )
            }
            IndexingContractSetup::Factory(factory) => format!(
                r#"
                ContractDetails::new_with_factory(
                    "{network}".to_string(),
                    FactoryDetails {{
                        address: "{address}".to_string(),
                        event_name: "{event_name}".to_string(),
                        parameter_name: "{parameter_name}".to_string(),
                        abi: "{abi}".to_string(),
                    }},
                    {start_block},
                    {end_block},
                    Some({polling_every}),
                ),
                "#,
                network = detail.network,
                address = factory.address,
                event_name = factory.event_name,
                parameter_name = factory.parameter_name,
                abi = factory.abi,
                start_block = start_block,
                end_block = end_block,
                polling_every = polling_every
            ),
        };

        details.push_str(&item);
    }

    details.push(']');

    let include_events_code = if let Some(include_events) = &contract.include_events {
        format!(
            "Some(vec![{}])",
            include_events
                .iter()
                .map(|s| format!("\"{}\".to_string()", s))
                .collect::<Vec<_>>()
                .join(", ")
        )
    } else {
        "None".to_string()
    };

    Code::new(format!(
        r#"
        pub fn contract_information(&self) -> Contract {{
            Contract {{
                name: "{}".to_string(),
                details: {},
                abi: "{}".to_string(),
                include_events: {},
                reorg_safe_distance: {},
                generate_csv: {},
            }}
        }}
        "#,
        contract.name,
        details,
        contract.abi,
        include_events_code,
        contract.reorg_safe_distance,
        contract.generate_csv
    ))
}

#[derive(thiserror::Error, Debug)]
pub enum CreateCsvFileForEvent {
    #[error("Could not create the dir {0}")]
    CreateDirFailed(std::io::Error),
}

pub fn create_csv_file_for_event(
    project_path: &Path,
    contract: &Contract,
    event_info: &EventInfo,
    csv_path: &str,
) -> Result<String, CreateCsvFileForEvent> {
    let csv_file_name = format!("{}-{}.csv", contract.name, event_info.name).to_lowercase();
    let csv_folder = project_path.join(format!("{}/{}", csv_path, contract.name));

    // Create directory if it does not exist.
    if let Err(e) = fs::create_dir_all(&csv_folder) {
        return Err(CreateCsvFileForEvent::CreateDirFailed(e));
    }

    Ok(format!("{}/{}", csv_folder.display(), csv_file_name))
}

pub fn csv_headers_for_event(event_info: &EventInfo) -> Vec<String> {
    let mut headers: Vec<String> = generate_abi_name_properties(
        &event_info.inputs,
        &GenerateAbiPropertiesType::CsvHeaderNames,
        None,
    )
    .iter()
    .map(|m| m.value.clone())
    .collect();

    // Add additional headers.
    headers.insert(0, r#""contract_address""#.to_string());
    headers.push(r#""tx_hash""#.to_string());
    headers.push(r#""block_number""#.to_string());
    headers.push(r#""block_hash""#.to_string());

    headers
}

fn generate_csv_instance(
    project_path: &Path,
    contract: &Contract,
    event_info: &EventInfo,
    csv: &Option<CsvDetails>,
) -> Result<Code, CreateCsvFileForEvent> {
    let csv_path = csv.as_ref().map_or("./generated_csv", |c| &c.path);

    if !contract.generate_csv {
        return Ok(Code::new(format!(
            r#"let csv = AsyncCsvAppender::new("{csv_path}".to_string());"#,
            csv_path = csv_path,
        )));
    }

    let csv_path = create_csv_file_for_event(project_path, contract, event_info, csv_path)?;
    let headers: Vec<String> = csv_headers_for_event(event_info);

    Ok(Code::new(format!(
        r#"
        let csv = AsyncCsvAppender::new("{csv_path}".to_string());
        if !Path::new("{csv_path}").exists() {{
            csv.append_header(vec![{headers}.into()])
                .await
                .unwrap();
        }}
    "#,
        csv_path = csv_path,
        headers = headers.join(".into(), ")
    )))
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateEventCallbackStructsError {
    #[error("{0}")]
    CreateCsvFileForEvent(CreateCsvFileForEvent),
}

fn generate_event_callback_structs_code(
    project_path: &Path,
    event_info: &[EventInfo],
    contract: &Contract,
    storage: &Storage,
) -> Result<Code, GenerateEventCallbackStructsError> {
    let databases_enabled = storage.postgres_enabled();

    let mut parts = Vec::new();

    for info in event_info {
        let csv_generator = generate_csv_instance(project_path, contract, info, &storage.csv)
            .map_err(GenerateEventCallbackStructsError::CreateCsvFileForEvent)?;

        let part = format!(
            r#"
            type {name}EventCallbackType<TExtensions> = Arc<dyn Fn(&Vec<{struct_result}>, Arc<EventContext<TExtensions>>) -> BoxFuture<'_, ()> + Send + Sync>;

            pub struct {name}Event<TExtensions> where TExtensions: Send + Sync {{
                callback: {name}EventCallbackType<TExtensions>,
                context: Arc<EventContext<TExtensions>>,
            }}

            impl<TExtensions> {name}Event<TExtensions> where TExtensions: Send + Sync {{
                pub async fn new(
                    callback: {name}EventCallbackType<TExtensions>,
                    extensions: TExtensions,
                ) -> Self {{
                    {csv_generator}

                    Self {{
                        callback,
                        context: Arc::new(EventContext {{
                            {database}
                            csv: Arc::new(csv),
                            extensions: Arc::new(extensions),
                        }}),
                    }}
                }}
            }}

            #[async_trait]
            impl<TExtensions> EventCallback for {name}Event<TExtensions> where TExtensions: Send + Sync {{
                async fn call(&self, events: Vec<EventResult>) {{
                    let events_len = events.len();

                    let result: Vec<{struct_result}> = events.into_iter()
                        .filter_map(|item| {{
                            item.decoded_data.downcast::<{struct_data}>()
                                .ok()
                                .map(|arc| {struct_result} {{
                                    event_data: (*arc).clone(),
                                    tx_information: item.tx_information
                                }})
                        }})
                        .collect();

                    if result.len() == events_len {{
                        (self.callback)(&result, self.context.clone()).await;
                    }} else {{
                        panic!("{name}Event: Unexpected data type - expected: {struct_data}")
                    }}
                }}
            }}
            "#,
            name = info.name,
            struct_result = info.struct_result,
            struct_data = info.struct_data,
            database = if databases_enabled {
                "database: Arc::new(PostgresClient::new().await.unwrap()),"
            } else {
                ""
            },
            csv_generator = csv_generator,
        );

        parts.push(part);
    }

    Ok(Code::new(parts.join("\n")))
}

/// Generates the Rust code for a function that returns a provider for a given network.
///
/// This function constructs a Rust function that returns a provider instance based on the specified network.
/// It handles multiple network names and generates the appropriate conditional branches.
///
/// # Arguments
///
/// * `networks` - A vector of network names.
///
fn build_get_provider_fn(networks: Vec<String>) -> Code {
    let mut function = String::new();
    function
        .push_str("fn get_provider(&self, network: &str) -> Arc<Provider<RetryClient<Http>>> {\n");

    // Iterate through the networks and generate conditional branches
    for (index, network) in networks.iter().enumerate() {
        if index > 0 {
            function.push_str(" else ");
        }

        function.push_str(&format!(
            r#"
            if network == "{network}" {{
                super::super::super::networks::{network_provider_fn_name}()
            }}"#,
            network = network,
            network_provider_fn_name = network_provider_fn_name_by_name(network)
        ));
    }

    function.push_str(
        r#"
        else {
            panic!("Network not supported")
        }
    }"#,
    );

    Code::new(function)
}

/// Generates the Rust code for a function that returns a contract instance for a given network.
///
/// This function constructs a Rust function that returns a contract instance based on the specified network.
/// It handles multiple contract details and generates the appropriate conditional branches.
///
/// # Arguments
///
/// * `contracts_details` - A vector of references to `ContractDetails`.
/// * `abi_gen_name` - The name of the ABI generation struct.
///
fn build_contract_fn(contracts_details: Vec<&ContractDetails>, abi_gen_name: &str) -> Code {
    let mut function = String::new();
    function.push_str(&format!(
        r#"fn contract(&self, network: &str) -> {abi_gen_name}<Arc<Provider<RetryClient<Http>>>> {{"#,
        abi_gen_name = abi_gen_name
    ));

    // Handling each contract detail with an `if` or `else if`
    for (index, contract_detail) in contracts_details.iter().enumerate() {
        let address = if let IndexingContractSetup::Address(address) =
            contract_detail.indexing_contract_setup()
        {
            address
        } else {
            "0x0000000000000000000000000000000000000000".to_string()
        };

        if index == 0 {
            function.push_str("    if ");
        } else {
            function.push_str("    else if ");
        }

        function.push_str(&format!(
            r#"network == "{network}" {{
                let address: Address = "{address}"
                    .parse()
                    .unwrap();
                {abi_gen_name}::new(address, Arc::new(self.get_provider(network).clone()))
            }}"#,
            network = contract_detail.network,
            address = address,
            abi_gen_name = abi_gen_name
        ));
    }

    // Add a fallback else statement to handle unsupported networks
    function.push_str(
        r#"
        else {
            panic!("Network not supported");
        }
    }"#,
    );

    Code::new(function)
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateEventBindingCodeError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),

    #[error("{0}")]
    GenerateStructsError(GenerateStructsError),

    #[error("{0}")]
    GenerateEventCallbackStructsError(GenerateEventCallbackStructsError),
}

/// Generates the Rust code for event bindings.
///
/// This function constructs Rust code for event bindings, including the event callback structs, context,
/// provider function, and contract-related functions. It handles the initialization of CSV, storage setup,
/// and the setup of event callbacks.
///
/// # Arguments
///
/// * `project_path` - The project path.
/// * `indexer_name` - The name of the indexer.
/// * `contract` - The contract information.
/// * `storage` - An `storage` configuration.
/// * `event_info` - A vector of `EventInfo` containing details about the events.
///
fn generate_event_bindings_code(
    project_path: &Path,
    indexer_name: &str,
    contract: &Contract,
    storage: &Storage,
    event_info: Vec<EventInfo>,
) -> Result<Code, GenerateEventBindingCodeError> {
    let event_type_name = generate_event_type_name(&contract.name);
    let code = Code::new(format!(
        r#"
        /// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY.
        ///
        /// This file was auto generated by rindexer - https://github.com/joshstevens19/rindexer.
        /// Any manual changes to this file will be overwritten.
        
        use super::{abigen_file_name}::{abigen_mod_name}::{{self, {abigen_name}}};
        use std::{{any::Any, sync::Arc}};
        use std::future::Future;
        use std::pin::Pin;
        use std::path::Path;
        use ethers::{{providers::{{Http, Provider, RetryClient}}, abi::Address, types::{{Bytes, H256}}}};
        use rindexer_core::{{
            async_trait,
            AsyncCsvAppender,
            generate_random_id,
            FutureExt,
            generator::event_callback_registry::{{EventCallbackRegistry, EventInformation, ContractInformation, NetworkContract, EventResult, TxInformation, FilterDetails, FactoryDetails}},
            manifest::yaml::{{Contract, ContractDetails}},
            {client_import}
        }};

        {structs}

        type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

        #[async_trait]
        trait EventCallback {{
            async fn call(&self, events: Vec<EventResult>);
        }}

        pub struct EventContext<TExtensions> where TExtensions: Send + Sync {{
            {event_context_database}
            pub csv: Arc<AsyncCsvAppender>,
            pub extensions: Arc<TExtensions>,
        }}

        // didn't want to use option or none made harder DX
        // so a blank struct makes interface nice
        pub struct NoExtensions {{}}
        pub fn no_extensions() -> NoExtensions {{
            NoExtensions {{}}
        }}

        {event_callback_structs}

        pub enum {event_type_name}<TExtensions> where TExtensions: 'static + Send + Sync {{
            {event_enums}
        }}

        impl<TExtensions> {event_type_name}<TExtensions> where TExtensions: 'static + Send + Sync {{
            pub fn topic_id(&self) -> &'static str {{
                match self {{
                    {topic_ids_match_arms}
                }}
            }}

            pub fn event_name(&self) -> &'static str {{
                match self {{
                    {event_names_match_arms}
                }}
            }}

            {contract_type_fn}

            {build_get_provider_fn}

            {build_contract_fn}

            fn decoder(&self, network: &str) -> Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync> {{
                let contract = self.contract(network);

                match self {{
                    {decoder_match_arms}
                }}
            }}

            pub fn register(self, registry: &mut EventCallbackRegistry) {{
                let topic_id = self.topic_id();
                let event_name = self.event_name();
                let contract_information = self.contract_information();
                let contract = ContractInformation {{
                    name: contract_information.name,
                    details: contract_information
                        .details
                        .iter()
                        .map(|c| NetworkContract {{
                            id: generate_random_id(10),
                            network: c.network.clone(),
                            provider: self.get_provider(&c.network),
                            decoder: self.decoder(&c.network),
                            indexing_contract_setup: c.indexing_contract_setup(),
                            start_block: c.start_block,
                            end_block: c.end_block,
                            polling_every: c.polling_every,
                        }})
                        .collect(),
                    abi: contract_information.abi,
                    reorg_safe_distance: contract_information.reorg_safe_distance,
                }};

                let callback: Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync> = match self {{
                    {register_match_arms}
                }};

               registry.register_event(EventInformation {{
                    indexer_name: "{indexer_name}".to_string(),
                    event_name: event_name.to_string(),
                    topic_id: topic_id.to_string(),
                    contract,
                    callback,
                }});
            }}
        }}
        "#,
        client_import = if storage.postgres_enabled() {
            "PostgresClient,"
        } else {
            ""
        },
        abigen_mod_name = abigen_contract_mod_name(contract),
        abigen_file_name = abigen_contract_file_name(contract),
        abigen_name = abigen_contract_name(contract),
        structs = generate_structs(contract)
            .map_err(GenerateEventBindingCodeError::GenerateStructsError)?,
        event_type_name = &event_type_name,
        event_context_database = if storage.postgres_enabled() {
            "pub database: Arc<PostgresClient>,"
        } else {
            ""
        },
        event_callback_structs =
            generate_event_callback_structs_code(project_path, &event_info, contract, storage)
                .map_err(GenerateEventBindingCodeError::GenerateEventCallbackStructsError,)?,
        event_enums = generate_event_enums_code(&event_info),
        topic_ids_match_arms = generate_topic_ids_match_arms_code(&event_type_name, &event_info),
        event_names_match_arms =
            generate_event_names_match_arms_code(&event_type_name, &event_info),
        contract_type_fn = generate_contract_type_fn_code(contract),
        build_get_provider_fn =
            build_get_provider_fn(contract.details.iter().map(|c| c.network.clone()).collect()),
        build_contract_fn = build_contract_fn(
            contract.details.iter().collect(),
            &abigen_contract_name(contract)
        ),
        decoder_match_arms = generate_decoder_match_arms_code(&event_type_name, &event_info),
        register_match_arms = generate_register_match_arms_code(&event_type_name, &event_info)
    ));

    Ok(code)
}

/// Enumeration to specify the type of ABI properties to generate.
#[derive(PartialEq)]
pub enum GenerateAbiPropertiesType {
    PostgresWithDataTypes,
    PostgresColumnsNamesOnly,
    CsvHeaderNames,
    Object,
}

/// Represents the result of generating ABI name properties.
#[derive(Debug)]
pub struct GenerateAbiNamePropertiesResult {
    pub value: String,
    pub abi_type: String,
    pub abi_name: String,
    pub ethereum_sql_type_wrapper: Option<EthereumSqlTypeWrapper>,
}

impl GenerateAbiNamePropertiesResult {
    pub fn new(value: String, name: &str, abi_type: &str) -> Self {
        Self {
            value,
            ethereum_sql_type_wrapper: solidity_type_to_ethereum_sql_type_wrapper(abi_type),
            abi_type: abi_type.to_string(),
            abi_name: name.to_string(),
        }
    }
}

/// Generates ABI name properties based on the inputs and specified properties type.
///
/// # Arguments
///
/// * `inputs` - A slice of `ABIInput` containing ABI inputs.
/// * `properties_type` - The type of ABI properties to generate.
/// * `prefix` - An optional prefix for the property names.
///
/// # Returns
///
/// A vector of `GenerateAbiNamePropertiesResult` containing the generated ABI name properties.
pub fn generate_abi_name_properties(
    inputs: &[ABIInput],
    properties_type: &GenerateAbiPropertiesType,
    prefix: Option<&str>,
) -> Vec<GenerateAbiNamePropertiesResult> {
    inputs
        .iter()
        .flat_map(|input| {
            if let Some(components) = &input.components {
                generate_abi_name_properties(
                    components,
                    properties_type,
                    Some(&camel_to_snake(&input.name)),
                )
            } else {
                match properties_type {
                    GenerateAbiPropertiesType::PostgresWithDataTypes => {
                        let value = format!(
                            "\"{}{}\" {}",
                            prefix.map_or_else(|| "".to_string(), |p| format!("{}_", p)),
                            camel_to_snake(&input.name),
                            solidity_type_to_db_type(&input.type_)
                        );

                        vec![GenerateAbiNamePropertiesResult::new(
                            value,
                            &input.name,
                            &input.type_,
                        )]
                    }
                    GenerateAbiPropertiesType::PostgresColumnsNamesOnly
                    | GenerateAbiPropertiesType::CsvHeaderNames => {
                        let value = format!(
                            "\"{}{}\"",
                            prefix.map_or_else(|| "".to_string(), |p| format!("{}_", p)),
                            camel_to_snake(&input.name),
                        );

                        vec![GenerateAbiNamePropertiesResult::new(
                            value,
                            &input.name,
                            &input.type_,
                        )]
                    }
                    GenerateAbiPropertiesType::Object => {
                        let value = format!(
                            "{}{}",
                            prefix.map_or_else(|| "".to_string(), |p| format!("{}.", p)),
                            camel_to_snake(&input.name),
                        );

                        vec![GenerateAbiNamePropertiesResult::new(
                            value,
                            &input.name,
                            &input.type_,
                        )]
                    }
                }
            }
        })
        .collect()
}

/// Retrieves ABI items from the contract.
///
/// # Arguments
///
/// * `contract` - The contract information.
/// * `is_filter` - A boolean indicating whether the ABI items are for filtering.
///
pub fn get_abi_items(contract: &Contract, is_filter: bool) -> Result<Vec<ABIItem>, ReadAbiError> {
    let mut abi_items = read_abi_items(contract)?;
    if is_filter {
        let filter_event_names: Vec<String> = contract
            .details
            .iter()
            .filter_map(|detail| {
                if let IndexingContractSetup::Filter(filter) = &detail.indexing_contract_setup() {
                    Some(filter.event_name.clone())
                } else {
                    None
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

#[derive(thiserror::Error, Debug)]
pub enum GenerateEventBindingsError {
    #[error("{0}")]
    ReadAbi(ReadAbiError),

    #[error("{0}")]
    GenerateEventBindingCode(GenerateEventBindingCodeError),

    #[error("{0}")]
    ParamType(ParamTypeError),
}

/// Generates event bindings for the specified contract.
///
/// # Arguments
///
/// * `project_path` - The project path.
/// * `indexer_name` - The name of the indexer.
/// * `contract` - The contract information.
/// * `is_filter` - A boolean indicating whether the ABI items are for filtering.
/// * `storage` - The storage configuration.
///
pub fn generate_event_bindings(
    project_path: &Path,
    indexer_name: &str,
    contract: &Contract,
    is_filter: bool,
    storage: &Storage,
) -> Result<Code, GenerateEventBindingsError> {
    let abi_items =
        get_abi_items(contract, is_filter).map_err(GenerateEventBindingsError::ReadAbi)?;
    let event_names = extract_event_names_and_signatures_from_abi(&abi_items)
        .map_err(GenerateEventBindingsError::ParamType)?;

    generate_event_bindings_code(project_path, indexer_name, contract, storage, event_names)
        .map_err(GenerateEventBindingsError::GenerateEventBindingCode)
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateEventHandlersError {
    #[error("{0}")]
    ReadAbiError(ReadAbiError),

    #[error("{0}")]
    ParamTypeError(ParamTypeError),
}

/// Generates event handlers for the specified contract.
///
/// # Arguments
///
/// * `indexer_name` - The name of the indexer.
/// * `is_filter` - A boolean indicating whether the ABI items are for filtering.
/// * `contract` - The contract information.
/// * `storage` - The storage configuration.
///
pub fn generate_event_handlers(
    indexer_name: &str,
    is_filter: bool,
    contract: &Contract,
    storage: &Storage,
) -> Result<Code, GenerateEventHandlersError> {
    let abi_items =
        get_abi_items(contract, is_filter).map_err(GenerateEventHandlersError::ReadAbiError)?;
    let event_names = extract_event_names_and_signatures_from_abi(&abi_items)
        .map_err(GenerateEventHandlersError::ParamTypeError)?;

    let mut imports = String::new();
    imports.push_str(
        r#"
            use rindexer_core::{
                generator::event_callback_registry::{EventCallbackRegistry},
                EthereumSqlTypeWrapper
            };
        "#,
    );
    imports.push_str("use std::sync::Arc;\n");
    imports.push_str(&format!(
        r#"use super::super::super::typings::{indexer_name_formatted}::events::{handler_registry_name}::{{no_extensions, {event_type_name}"#,
        indexer_name_formatted = camel_to_snake(indexer_name),
        handler_registry_name = camel_to_snake(&contract.name),
        event_type_name = generate_event_type_name(&contract.name)
    ));

    let mut handlers = String::new();

    let mut registry_fn = String::new();
    registry_fn.push_str(&format!(
        r#"pub async fn {handler_registry_fn_name}_handlers(registry: &mut EventCallbackRegistry) {{"#,
        handler_registry_fn_name = camel_to_snake(&contract.name),
    ));

    for event in event_names {
        let event_type_name = generate_event_type_name(&contract.name);

        imports.push_str(&format!(
            r#",{handler_name}Event"#,
            handler_name = event.name,
        ));

        let abi_name_properties =
            generate_abi_name_properties(&event.inputs, &GenerateAbiPropertiesType::Object, None);

        let mut csv_write = String::new();
        let mut postgres_write = String::new();

        // this checks storage enabled as well
        if !storage.postgres_disable_create_tables() {
            // TODO look at doing the bulk insert + copy route here as well
            let insert_sql = generate_insert_query_for_event(&event, indexer_name, &contract.name);

            let mut params_sql = String::new();
            params_sql
                .push_str("&[&EthereumSqlTypeWrapper::Address(result.tx_information.address),");

            for item in &abi_name_properties {
                if let Some(wrapper) = &item.ethereum_sql_type_wrapper {
                    params_sql.push_str(&format!(
                        "&EthereumSqlTypeWrapper::{}(result.event_data.{}{}),",
                        wrapper.raw_name(),
                        item.value,
                        if item.abi_type.contains("bytes") {
                            let static_bytes = item.abi_type.replace("bytes", "").replace("[]", "");
                            if !static_bytes.is_empty() {
                                ".into()"
                            } else {
                                ""
                            }
                        } else {
                            ""
                        }
                    ));
                } else {
                    params_sql.push_str(&format!("&result.event_data.{},", item.value));
                }
            }

            params_sql
                .push_str("&EthereumSqlTypeWrapper::H256(result.tx_information.transaction_hash),");
            params_sql
                .push_str("&EthereumSqlTypeWrapper::U64(result.tx_information.block_number),");
            params_sql.push_str("&EthereumSqlTypeWrapper::H256(result.tx_information.block_hash)");
            params_sql.push(']');

            postgres_write = format!(
                r#"context.database.execute("{insert_sql}",{params_sql}).await.unwrap();"#,
                insert_sql = insert_sql.replace('"', "\\\""),
                params_sql = params_sql
            );
        }

        // this checks storage enabled as well
        if !storage.csv_disable_create_headers() {
            let mut csv_data = String::new();
            csv_data.push_str(r#"format!("{:?}", result.tx_information.address),"#);

            for item in &abi_name_properties {
                if item.abi_type == "address" {
                    let key = format!("result.event_data.{},", item.value);
                    csv_data.push_str(&format!(r#"format!("{{:?}}", {}),"#, key));
                } else if item.abi_type.contains("bytes") {
                    csv_data.push_str(&format!(
                        r#"result.event_data.{}.iter().map(|byte| format!("{{:02x}}", byte)).collect::<Vec<_>>().join(""),"#,
                        item.value
                    ));
                } else if item.abi_type.contains("[]") {
                    csv_data.push_str(&format!(
                        r#"result.event_data.{}.iter().map(ToString::to_string).collect::<Vec<_>>().join(","),"#,
                        item.value
                    ));
                } else {
                    csv_data.push_str(&format!("result.event_data.{}.to_string(),", item.value));
                }
            }

            csv_data.push_str(r#"format!("{:?}", result.tx_information.transaction_hash),"#);
            csv_data.push_str(r#"result.tx_information.block_number.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.block_hash.to_string()"#);

            csv_write = format!(
                r#"context.csv.append(vec![{csv_data}]).await.unwrap();"#,
                csv_data = csv_data
            );
        }

        let handler = format!(
            r#"
            async fn {handler_fn_name}_handler(registry: &mut EventCallbackRegistry) {{
                {event_type_name}::{handler_name}(
                    {handler_name}Event::new(
                        Arc::new(|results, context| {{
                            Box::pin(async move {{
                                for result in results {{
                                    {csv_write}
                                    {postgres_write}
                                }}
                           }})
                        }}),
                        no_extensions()
                    )
                    .await,
                )
                .register(registry);
            }}
        "#,
            handler_fn_name = camel_to_snake(&event.name),
            handler_name = event.name,
            event_type_name = event_type_name,
            csv_write = csv_write,
            postgres_write = postgres_write,
        );

        handlers.push_str(&handler);

        registry_fn.push_str(&format!(
            r#"
                {handler_fn_name}_handler(registry).await;
            "#,
            handler_fn_name = camel_to_snake(&event.name)
        ));
    }

    imports.push_str("};\n");

    registry_fn.push('}');

    let mut code = String::new();
    code.push_str(&imports);
    code.push_str(&handlers);
    code.push_str(&registry_fn);

    Ok(Code::new(code))
}
