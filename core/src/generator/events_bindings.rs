use crate::database::postgres::{
    event_table_full_name, generate_column_names_only_with_base_properties,
    solidity_type_to_db_type, solidity_type_to_ethereum_sql_type_wrapper,
};
use crate::generator::build::is_filter;
use crate::generator::event_callback_registry::IndexingContractSetup;
use crate::EthereumSqlTypeWrapper;
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::iter::Map;
use std::path::Path;

use crate::helpers::camel_to_snake;
use crate::manifest::yaml::{Contract, ContractDetails, CsvDetails, DependencyEventTree, Storage};
use crate::types::code::Code;

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

pub fn read_abi_items(contract: &Contract) -> Result<Vec<ABIItem>, ReadAbiError> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIInput {
    pub indexed: Option<bool>,
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub components: Option<Vec<ABIInput>>,
}

#[derive(Debug)]
pub struct EventInfo {
    pub name: String,
    pub inputs: Vec<ABIInput>,
    signature: String,
    struct_result: String,
    struct_data: String,
}

impl EventInfo {
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

fn compute_topic_id(event_signature: &str) -> String {
    Map::collect(
        keccak256(event_signature)
            .iter()
            .map(|byte| format!("{:02x}", byte)),
    )
}

fn format_event_signature(item: &ABIItem) -> Result<String, ParamTypeError> {
    let formatted_inputs = item
        .inputs
        .iter()
        .map(format_param_type)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(formatted_inputs.join(","))
}

pub fn abigen_contract_name(contract: &Contract) -> String {
    format!("Rindexer{}Gen", contract.name)
}

fn abigen_contract_mod_name(contract: &Contract) -> String {
    camel_to_snake(&abigen_contract_name(contract))
}

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

                    #[derive(Debug, Clone)]
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

fn generate_event_enums_code(event_info: &[EventInfo]) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| format!("{}({}Event<TExtensions>),", info.name, info.name))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn generate_event_type_name(name: &str) -> String {
    format!("{}EventType", name)
}

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

fn generate_index_event_in_order_arms_code(
    event_type_name: &str,
    event_info: &[EventInfo],
    index_event_in_order: &Option<Vec<String>>,
) -> Code {
    Code::new(
        event_info
            .iter()
            .map(|info| {
                format!(
                    "{}::{}(_) => {},",
                    event_type_name,
                    info.name,
                    index_event_in_order
                        .as_ref()
                        .map_or(false, |vec| vec.contains(&info.name)),
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

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
    let index_event_in_order = if let Some(include_events) = &contract.index_event_in_order {
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

    fn format_tree(tree: &DependencyEventTree) -> String {
        let events_str = tree
            .events
            .iter()
            .map(|s| format!("\"{}\".to_string()", s))
            .collect::<Vec<_>>()
            .join(", ");

        let then_str = match &tree.then {
            Some(children) => children
                .iter()
                .map(format_tree)
                .collect::<Vec<_>>()
                .join(", "),
            None => String::new(),
        };

        if then_str.is_empty() {
            format!(
                "DependencyEventTree {{ events: vec![{}], then: None }}",
                events_str
            )
        } else {
            format!(
                "DependencyEventTree {{ events: vec![{}], then: Some(vec![{}]) }}",
                events_str, then_str
            )
        }
    }

    let dependency_events = if let Some(event_tree) = &contract.dependency_events {
        format!("Some({})", format_tree(event_tree))
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
                index_event_in_order: {},
                dependency_events: {},
                reorg_safe_distance: Some({}),
                generate_csv: Some({}),
            }}
        }}
        "#,
        contract.name,
        details,
        contract.abi,
        include_events_code,
        index_event_in_order,
        dependency_events,
        contract.reorg_safe_distance.unwrap_or_default(),
        contract.generate_csv.unwrap_or(true)
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

    headers.insert(0, r#""contract_address""#.to_string());
    headers.push(r#""tx_hash""#.to_string());
    headers.push(r#""block_number""#.to_string());
    headers.push(r#""block_hash""#.to_string());
    headers.push(r#""network""#.to_string());

    headers
}

fn generate_csv_instance(
    project_path: &Path,
    contract: &Contract,
    event_info: &EventInfo,
    csv: &Option<CsvDetails>,
) -> Result<Code, CreateCsvFileForEvent> {
    let csv_path = csv.as_ref().map_or("./generated_csv", |c| &c.path);

    if !contract.generate_csv.unwrap_or_default() {
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
    let is_filter = is_filter(contract);

    let mut parts = Vec::new();

    for info in event_info {
        let csv_generator = generate_csv_instance(project_path, contract, info, &storage.csv)
            .map_err(GenerateEventCallbackStructsError::CreateCsvFileForEvent)?;

        let part = format!(
            r#"
            pub fn {lower_name}_handler<TExtensions, F, Fut>(
                custom_logic: F,
            ) -> Arc<
                dyn for<'a> Fn(&'a Vec<{struct_result}>, Arc<EventContext<TExtensions>>) -> BoxFuture<'a, ()>
                    + Send
                    + Sync,
            >
            where
                TransferResult: Clone + 'static,
                F: for<'a> Fn(Vec<{struct_result}>, Arc<EventContext<TExtensions>>) -> Fut
                    + Send
                    + Sync
                    + 'static
                    + Clone,
                Fut: Future<Output = ()> + Send + 'static,
                TExtensions: Send + Sync + 'static,
            {{
                Arc::new(move |results, context| {{
                    let custom_logic = custom_logic.clone();
                    let results = results.clone();
                    let context = Arc::clone(&context);
                    async move {{ (custom_logic)(results, context).await }}.boxed()
                }})
            }}
            
            type {name}EventCallbackType<TExtensions> = Arc<
                dyn for<'a> Fn(&'a Vec<{struct_result}>, Arc<EventContext<TExtensions>>) -> BoxFuture<'a, ()>
                    + Send
                    + Sync,
                >;

            pub struct {name}Event<TExtensions> where TExtensions: Send + Sync + 'static {{
                callback: {name}EventCallbackType<TExtensions>,
                context: Arc<EventContext<TExtensions>>,
            }}

            impl<TExtensions> {name}Event<TExtensions> where TExtensions: Send + Sync + 'static {{
                pub async fn handler<F, Fut>(closure: F, extensions: TExtensions) -> Self
                where
                    {struct_result}: Clone + 'static,
                    F: for<'a> Fn(Vec<{struct_result}>, Arc<EventContext<TExtensions>>) -> Fut
                        + Send
                        + Sync
                        + 'static
                        + Clone,
                    Fut: Future<Output = ()> + Send + 'static,
                {{
                    {csv_generator}
            
                    Self {{
                        callback: {lower_name}_handler(closure),
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
                    {event_callback_events_len}

                    // note some can not downcast because it cant decode
                    // this happens on events which failed decoding due to
                    // not having the right abi for example
                    // transfer events with 2 indexed topics cant decode
                    // transfer events with 3 indexed topics
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

                    {event_callback_return}
                }}
            }}
            "#,
            name = info.name,
            lower_name = info.name.to_lowercase(),
            struct_result = info.struct_result,
            struct_data = info.struct_data,
            database = if databases_enabled {
                "database: Arc::new(PostgresClient::new().await.unwrap()),"
            } else {
                ""
            },
            csv_generator = csv_generator,
            event_callback_events_len = if !is_filter {
                "let events_len = events.len();"
            } else {
                ""
            },
            event_callback_return = if !is_filter {
                format!(
                    r#"
                    if result.len() == events_len {{
                        (self.callback)(&result, self.context.clone()).await;
                    }} else {{
                        panic!("{name}Event: Unexpected data type - expected: {struct_data}")
                    }}
                    "#,
                    name = info.name,
                    struct_data = info.struct_data
                )
            } else {
                "(self.callback)(&result, self.context.clone()).await;".to_string()
            }
        );

        parts.push(part);
    }

    Ok(Code::new(parts.join("\n")))
}

fn build_pub_contract_fn(
    contract_name: &str,
    contracts_details: Vec<&ContractDetails>,
    abi_gen_name: &str,
) -> Code {
    let contract_name = camel_to_snake(contract_name);
    let mut function = String::new();
    function.push_str(&format!(
        r#"pub fn {contract_name}_contract(network: &str) -> {abi_gen_name}<Arc<Provider<RetryClient<Http>>>> {{"#,
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
                {abi_gen_name}::new(
                    address,
                    Arc::new(get_provider_cache_for_network(network).get_inner_provider()),
                 )
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

fn build_self_contract_fn(contract_name: &str, abi_gen_name: &str) -> Code {
    let contract_name = camel_to_snake(contract_name);

    Code::new(format!(
        r#"pub fn contract(&self, network: &str) -> {abi_gen_name}<Arc<Provider<RetryClient<Http>>>> 
        {{
            {contract_name}_contract(network)
        }}"#,
        abi_gen_name = abi_gen_name
    ))
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
            provider::JsonRpcCachedProvider
        }};
        use super::super::super::super::typings::networks::get_provider_cache_for_network;

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
        
        {build_pub_contract_fn}

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

            pub fn index_event_in_order(&self) -> bool {{
                 match self {{
                    {index_event_in_order_match_arms}
                }}
            }}

            {contract_type_fn}

            fn get_provider(&self, network: &str) -> Arc<JsonRpcCachedProvider> {{
                get_provider_cache_for_network(network)
            }}

            {build_self_contract_fn}

            fn decoder(&self, network: &str) -> Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync> {{
                let contract = self.contract(network);

                match self {{
                    {decoder_match_arms}
                }}
            }}

            pub fn register(self, registry: &mut EventCallbackRegistry) {{
                let topic_id = self.topic_id();
                let event_name = self.event_name();
                let index_event_in_order = self.index_event_in_order();
                let contract_information = self.contract_information();
                let contract = ContractInformation {{
                    name: contract_information.name,
                    details: contract_information
                        .details
                        .iter()
                        .map(|c| NetworkContract {{
                            id: generate_random_id(10),
                            network: c.network.clone(),
                            cached_provider: self.get_provider(&c.network),
                            decoder: self.decoder(&c.network),
                            indexing_contract_setup: c.indexing_contract_setup(),
                            start_block: c.start_block,
                            end_block: c.end_block,
                            polling_every: c.polling_every,
                        }})
                        .collect(),
                    abi: contract_information.abi,
                    reorg_safe_distance: contract_information.reorg_safe_distance.unwrap_or_default(),
                }};

                let callback: Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync> = match self {{
                    {register_match_arms}
                }};

               registry.register_event(EventInformation {{
                    indexer_name: "{indexer_name}".to_string(),
                    event_name: event_name.to_string(),
                    index_event_in_order,
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
        index_event_in_order_match_arms = generate_index_event_in_order_arms_code(
            &event_type_name,
            &event_info,
            &contract.index_event_in_order
        ),
        contract_type_fn = generate_contract_type_fn_code(contract),
        build_pub_contract_fn = build_pub_contract_fn(
            &contract.name,
            contract.details.iter().collect(),
            &abigen_contract_name(contract)
        ),
        build_self_contract_fn =
            build_self_contract_fn(&contract.name, &abigen_contract_name(contract)),
        decoder_match_arms = generate_decoder_match_arms_code(&event_type_name, &event_info),
        register_match_arms = generate_register_match_arms_code(&event_type_name, &event_info)
    ));

    Ok(code)
}

#[derive(PartialEq)]
pub enum GenerateAbiPropertiesType {
    PostgresWithDataTypes,
    PostgresColumnsNamesOnly,
    CsvHeaderNames,
    Object,
}

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
                            "{}{}",
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
                EthereumSqlTypeWrapper, PgType, RindexerColorize, rindexer_error, rindexer_info
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
            csv_data.push_str(r#"result.tx_information.block_hash.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.network.to_string()"#);

            csv_write = format!(
                r#"let csv_result = context.csv.append(vec![{csv_data}]).await;
                
                   if let Err(e) = csv_result {{ 
                        rindexer_error!("{event_type_name}::{handler_name} inserting csv data: {{:?}}", e);
                   }}
                "#,
                csv_data = csv_data,
                handler_name = event.name,
                event_type_name = event_type_name,
            );

            if storage.postgres_disable_create_tables() {
                csv_write = format!(
                    r#"for result in results {{
                        {inner_csv_write}
                    }}"#,
                    inner_csv_write = csv_write
                );
            }
        }

        let mut postgres_write = String::new();

        // this checks storage enabled as well
        if !storage.postgres_disable_create_tables() {
            let mut data =
                "vec![EthereumSqlTypeWrapper::Address(result.tx_information.address),".to_string();

            for item in &abi_name_properties {
                if let Some(wrapper) = &item.ethereum_sql_type_wrapper {
                    data.push_str(&format!(
                        "EthereumSqlTypeWrapper::{}(result.event_data.{}{}),",
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
                    // data.push_str(&format!("result.event_data.{},", item.value));
                    panic!("No EthereumSqlTypeWrapper found for: {:?}", item.abi_type);
                }
            }

            data.push_str("EthereumSqlTypeWrapper::H256(result.tx_information.transaction_hash),");
            data.push_str("EthereumSqlTypeWrapper::U64(result.tx_information.block_number),");
            data.push_str("EthereumSqlTypeWrapper::H256(result.tx_information.block_hash),");
            data.push_str(
                "EthereumSqlTypeWrapper::String(result.tx_information.network.to_string())",
            );
            data.push_str("];");

            postgres_write = format!(
                r#"
                    let mut bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
                    for result in results.iter() {{   
                        {csv_write}   
                                      
                        let data = {data};
                        bulk_data.push(data);
                    }}
                    
                    if bulk_data.is_empty() {{
                        return;
                    }}
                    
                     if bulk_data.len() > 100 {{
                        let result = context
                            .database
                            .bulk_insert_via_copy(
                                "{table_name}",
                                &[{columns_names}],
                                &bulk_data
                                    .first()
                                    .unwrap()
                                    .iter()
                                    .map(|param| param.to_type())
                                    .collect::<Vec<PgType>>(),
                                &bulk_data,
                            )
                            .await;
                        
                        if let Err(e) = result {{
                            rindexer_error!("{event_type_name}::{handler_name} inserting bulk data: {{:?}}", e);
                        }}
                        }} else {{
                            let result = context
                                .database
                                .bulk_insert(
                                    "{table_name}",
                                    &[{columns_names}],
                                    &bulk_data,
                                )
                                .await;
                            
                            if let Err(e) = result {{
                                rindexer_error!("{event_type_name}::{handler_name} inserting bulk data: {{:?}}", e);
                            }}
                    }}
                "#,
                table_name = event_table_full_name(indexer_name, &contract.name, &event.name),
                handler_name = event.name,
                event_type_name = event_type_name,
                columns_names = generate_column_names_only_with_base_properties(&event.inputs)
                    .iter()
                    .map(|item| format!("\"{}\".to_string()", item))
                    .collect::<Vec<String>>()
                    .join(", "),
                data = data,
                csv_write = csv_write
            );
        }

        let handler = format!(
            r#"
            async fn {handler_fn_name}_handler(registry: &mut EventCallbackRegistry) {{
                {event_type_name}::{handler_name}(
                    {handler_name}Event::handler(|results, context| async move {{
                            if results.is_empty() {{
                                return;
                            }}

                            {csv_write}
                            {postgres_write}
                            
                            rindexer_info!(
                                "{contract_name}::{handler_name} - {{}} - {{}} events",
                                "INDEXED".green(),
                                results.len(),
                            );
                        }},
                        no_extensions(),
                      )
                      .await,
                )
                .register(registry);
            }}
        "#,
            handler_fn_name = camel_to_snake(&event.name),
            handler_name = event.name,
            event_type_name = event_type_name,
            contract_name = contract.name,
            csv_write = if !postgres_write.is_empty() {
                String::new()
            } else {
                csv_write
            },
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
