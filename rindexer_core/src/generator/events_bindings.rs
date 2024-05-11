use crate::database::postgres::{
    generate_columns_names_only, generate_injected_param, solidity_type_to_db_type,
    solidity_type_to_ethereum_sql_type,
};
use crate::generator::event_callback_registry::AddressOrFilter;
use ethers::utils::keccak256;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fs;

use crate::helpers::{camel_to_snake, generate_random_id};
use crate::manifest::yaml::{Contract, ContractDetails, Databases};

use super::networks_bindings::network_provider_fn_name_by_name;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ABIItem {
    #[serde(default)]
    inputs: Vec<ABIInput>,

    #[serde(default)]
    name: String,

    #[serde(rename = "type", default)]
    type_: String,
}

pub fn read_abi_file(file_path: &str) -> Result<Vec<ABIItem>, Box<dyn Error>> {
    let abi_str = fs::read_to_string(file_path)?;
    println!("abi_str {:?}", abi_str);
    let abi_json: Vec<ABIItem> = serde_json::from_str(&abi_str)?;
    Ok(abi_json)
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
}

fn format_param_type(input: &ABIInput) -> String {
    match input.type_.as_str() {
        "tuple" => {
            let formatted_components = input
                .components
                .as_ref()
                .unwrap()
                .iter()
                .map(format_param_type)
                .collect::<Vec<_>>()
                .join(",");
            format!("({})", formatted_components)
        }
        _ => input.type_.to_string(),
    }
}

fn compute_topic_id(event_signature: &str) -> String {
    keccak256(event_signature)
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect()
}

fn format_event_signature(item: &ABIItem) -> String {
    item.inputs
        .iter()
        .map(format_param_type)
        .collect::<Vec<_>>()
        .join(",")
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
    abi_json: &Vec<ABIItem>,
) -> Result<Vec<EventInfo>, Box<dyn Error>> {
    let result = abi_json
        .iter()
        .filter_map(|item| {
            if item.type_ == "event" {
                let signature = format_event_signature(item);

                Some(EventInfo::new(item, signature))
            } else {
                None
            }
        })
        .collect();

    Ok(result)
}

fn generate_structs(contract: &Contract) -> Result<String, Box<dyn Error>> {
    let abi_str = fs::read_to_string(&contract.abi)?;
    let abi_json: Value = serde_json::from_str(&abi_str)?;

    let mut structs = String::new();

    for item in abi_json.as_array().ok_or("Invalid ABI JSON format")?.iter() {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_result = format!("{}Result", event_name);
            let struct_data = format!("{}Data", event_name);

            structs.push_str(&format!(
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
            ));
        }
    }

    Ok(structs)
}

fn generate_event_enums_code(event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| format!("{}({}Event<TExtensions>),", info.name, info.name))
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_event_type_name(name: &str) -> String {
    format!("{}EventType", name)
}

fn generate_topic_ids_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| {
            let event_signature = format!("{}({})", info.name, info.signature);
            let topic_id = compute_topic_id(&event_signature);
            format!(
                "{}::{}(_) => \"0x{}\",",
                event_type_name, info.name, topic_id
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_event_names_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| {
            format!(
                "{}::{}(_) => \"{}\",",
                event_type_name, info.name, info.name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_register_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> String {
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
        .join("\n")
}

fn generate_decoder_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| {
            format!(
                "{}::{}(event) => Arc::new(move |data| event.call(data)),",
                event_type_name, info.name
            );

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
        .join("\n")
}

fn generate_indexed_vec_string(indexed: &Option<Vec<String>>) -> String {
    match indexed {
        Some(values) => {
            format!(
                "Some(vec![{}])",
                values
                    .iter()
                    .map(|s| format!("\"{}\".to_string()", s))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
        None => "None".to_string(),
    }
}

fn generate_contract_type_fn_code(contract: &Contract) -> String {
    let mut details = String::new();
    details.push_str("vec![");
    for contract in contract.details.iter() {
        match contract.address_or_filter() {
            AddressOrFilter::Address(address) => {
                let item = format!(
                    r#"
                        ContractDetails::new_with_address(
                            "{network}".to_string(),
                            "{address}".to_string(),
                            Some({start_block}),
                            Some({end_block}),
                            Some({polling_every}),
                        ),
                    "#,
                    network = contract.network,
                    address = address,
                    // TODO! FIX
                    start_block = contract.start_block.unwrap(),
                    // TODO! FIX
                    end_block = contract.end_block.unwrap_or(99424866),
                    // TODO! FIX
                    polling_every = contract.polling_every.unwrap_or(1000)
                );
                details.push_str(&item);
            }
            AddressOrFilter::Filter(filter_details) => {
                let indexed_1 = generate_indexed_vec_string(&filter_details.indexed_1);
                let indexed_2 = generate_indexed_vec_string(&filter_details.indexed_2);
                let indexed_3 = generate_indexed_vec_string(&filter_details.indexed_3);

                let item = format!(
                    r#"
                        ContractDetails::new_with_filter(
                            "{network}".to_string(),
                            FilterDetails {{
                                event_name: "{event_name}".to_string(),
                                indexed_1: {indexed_1},
                                indexed_2: {indexed_2},
                                indexed_3: {indexed_3},
                            }},
                            Some({start_block}),
                            Some({end_block}),
                            Some({polling_every}),
                        ),
                    "#,
                    network = contract.network,
                    event_name = filter_details.event_name,
                    indexed_1 = indexed_1,
                    indexed_2 = indexed_2,
                    indexed_3 = indexed_3,
                    // TODO! FIX
                    start_block = contract.start_block.unwrap(),
                    // TODO! FIX
                    end_block = contract.end_block.unwrap_or(99424866),
                    // TODO! FIX
                    polling_every = contract.polling_every.unwrap_or(1000)
                );
                details.push_str(&item);
            }
        };
    }
    details.push(']');
    format!(
        r#"
            fn contract_information(&self) -> Contract {{
                Contract {{
                    name: "{name}".to_string(),
                    details: {details},
                    abi: "{abi}".to_string(),
                }}
            }}
            "#,
        name = contract.name,
        details = details,
        abi = contract.abi
    )
}

fn generate_event_callback_structs_code(
    event_info: &[EventInfo],
    databases: &Option<Databases>,
) -> String {
    let databases_enabled = databases.is_some();
    event_info
        .iter()
        .map(|info| {
            let csv_file_name = format!("{}-{}.csv", &info.struct_result, &info.signature).to_lowercase();
            format!(
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
                            options: NewEventOptions,
                        ) -> Self {{
                            // let mut csv = None;
                            // if options.enabled_csv {{
                            //     let csv_appender = Arc::new(AsyncCsvAppender::new("events.csv".to_string()));
                            //     csv_appender.ap
                            //     csv = Some(Arc::new(AsyncCsvAppender::new("events.csv".to_string())));
                            // }}

                            Self {{
                                callback,
                                context: Arc::new(EventContext {{
                                    {database}
                                    csv: Arc::new(AsyncCsvAppender::new("{csv_file_name}".to_string())),
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
                                println!("{name}Event: Unexpected data type - expected: {struct_data}")
                            }}
                        }}
                    }}
                "#,
                name = info.name,
                struct_result = info.struct_result,
                struct_data = info.struct_data,
                database = if databases_enabled { "database: Arc::new(PostgresClient::new().await.unwrap())," } else { "" },
                csv_file_name = csv_file_name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn build_get_provider_fn(networks: Vec<String>) -> String {
    let mut function = String::new();
    function.push_str(
        "fn get_provider(&self, network: &str) -> &'static Arc<Provider<RetryClient<Http>>> {\n",
    );

    // Iterate through the networks and generate conditional branches
    for (index, network) in networks.iter().enumerate() {
        if index > 0 {
            function.push_str(" else ");
        }

        function.push_str(&format!(
            r#"
            if network == "{network}" {{
                return crate::rindexer::networks::{network_provider_fn_name}();
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

    function
}

fn build_contract_fn(contracts_details: Vec<&ContractDetails>, abi_gen_name: &str) -> String {
    let mut function = String::new();
    function.push_str(&format!(
        r#"fn contract(&self, network: &str) -> {abi_gen_name}<Arc<Provider<RetryClient<Http>>>> {{"#,
        abi_gen_name = abi_gen_name
    ));

    // Handling each contract detail with an `if` or `else if`
    for (index, contract_detail) in contracts_details.iter().enumerate() {
        let address = if let AddressOrFilter::Address(address) = contract_detail.address_or_filter()
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

    function
}

fn generate_event_bindings_code(
    contract: &Contract,
    clients: &Option<Databases>,
    event_info: Vec<EventInfo>,
) -> Result<String, Box<dyn Error>> {
    let event_type_name = generate_event_type_name(&contract.name);
    let code = format!(
        r#"
            use std::{{any::Any, sync::Arc}};

            use std::future::Future;
            use std::pin::Pin;

            use ethers::{{providers::{{Http, Provider, RetryClient}}, abi::Address, types::{{Bytes, H256}}}};
            
            use rindexer_core::{{
                async_trait,
                AsyncCsvAppender,
                generate_random_id,
                FutureExt,
                generator::event_callback_registry::{{EventCallbackRegistry, EventInformation, ContractInformation, NetworkContract, EventResult, TxInformation, FilterDetails}},
                manifest::yaml::{{Contract, ContractDetails}},
                {client_import}
            }};

            use super::{abigen_file_name}::{abigen_mod_name}::{{self, {abigen_name}}};

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

            // TODO: NEED TO SPEC OUT OPTIONS
            pub struct NewEventOptions {{
                pub enabled_csv: bool,
            }}

            impl NewEventOptions {{
                pub fn default() -> Self {{
                    Self {{ enabled_csv: false }}
                }}
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
                                address_or_filter: c.address_or_filter(),
                                start_block: c.start_block,
                                end_block: c.end_block,
                                polling_every: c.polling_every,
                            }})
                            .collect(),
                        abi: contract_information.abi,
                    }};
                    
                    let callback: Arc<dyn Fn(Vec<EventResult>) -> BoxFuture<'static, ()> + Send + Sync> = match self {{
                        {register_match_arms}
                    }};
                
                   registry.register_event({{
                        EventInformation {{
                            event_name,
                            topic_id,
                            contract,
                            callback,
                        }}
                    }});
                }}
            }}
        "#,
        client_import = if clients.is_some() {
            "PostgresClient,"
        } else {
            ""
        },
        abigen_mod_name = abigen_contract_mod_name(contract),
        abigen_file_name = abigen_contract_file_name(contract),
        abigen_name = abigen_contract_name(contract),
        structs = generate_structs(contract)?,
        event_type_name = &event_type_name,
        event_context_database = if clients.is_some() {
            "pub database: Arc<PostgresClient>,"
        } else {
            ""
        },
        event_callback_structs = generate_event_callback_structs_code(&event_info, clients),
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
    );

    Ok(code)
}

#[derive(PartialEq)]
pub enum GenerateAbiPropertiesType {
    PostgresWithDataTypes,
    PostgresColumnsNamesOnly,
    Object,
}

#[derive(Debug)]
pub struct GenerateAbiNamePropertiesResult {
    pub value: String,
    pub abi_type: String,
    pub ethereum_sql_type_wrapper: Option<String>,
}

impl GenerateAbiNamePropertiesResult {
    pub fn new(value: String, abi_type: &str) -> Self {
        Self {
            value,
            ethereum_sql_type_wrapper: solidity_type_to_ethereum_sql_type(abi_type),
            abi_type: abi_type.to_string(),
        }
    }
}

pub fn generate_abi_name_properties(
    inputs: &[ABIInput],
    properties_type: &GenerateAbiPropertiesType,
    prefix: Option<&str>,
) -> Vec<GenerateAbiNamePropertiesResult> {
    fn generate_name_format(name: &str) -> String {
        camel_to_snake(name)
    }

    inputs
        .iter()
        .flat_map(|input| {
            if let Some(components) = &input.components {
                generate_abi_name_properties(
                    components,
                    properties_type,
                    Some(&generate_name_format(&input.name)),
                )
            } else {
                match properties_type {
                    GenerateAbiPropertiesType::PostgresWithDataTypes => {
                        let value = format!(
                            "\"{}{}\" {}",
                            if prefix.is_some() {
                                format!("{}_", prefix.as_ref().unwrap())
                            } else {
                                "".to_string()
                            },
                            generate_name_format(&input.name),
                            solidity_type_to_db_type(&input.type_)
                        );

                        vec![GenerateAbiNamePropertiesResult::new(value, &input.type_)]
                    }
                    GenerateAbiPropertiesType::PostgresColumnsNamesOnly => {
                        let value = format!(
                            "\"{}{}\"",
                            if prefix.is_some() {
                                format!("{}_", prefix.as_ref().unwrap())
                            } else {
                                "".to_string()
                            },
                            generate_name_format(&input.name),
                        );

                        vec![GenerateAbiNamePropertiesResult::new(value, &input.type_)]
                    }
                    GenerateAbiPropertiesType::Object => {
                        let value = format!(
                            "{}{}",
                            if prefix.is_some() {
                                format!("{}.", prefix.as_ref().unwrap())
                            } else {
                                "".to_string()
                            },
                            generate_name_format(&input.name),
                        );

                        vec![GenerateAbiNamePropertiesResult::new(value, &input.type_)]
                    }
                }
            }
        })
        .collect()
}

fn get_abi_items(contract: &Contract, is_filter: bool) -> Result<Vec<ABIItem>, Box<dyn Error>> {
    let mut abi_items = read_abi_file(&contract.abi)?;
    if is_filter {
        let filter_event_names: Vec<String> = contract
            .details
            .iter()
            .filter_map(|detail| {
                if let AddressOrFilter::Filter(filter) = &detail.address_or_filter() {
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

pub fn generate_event_bindings(
    contract: &Contract,
    is_filter: bool,
    databases: &Option<Databases>,
) -> Result<String, Box<dyn Error>> {
    let abi_items = get_abi_items(contract, is_filter)?;
    let event_names = extract_event_names_and_signatures_from_abi(&abi_items)?;

    generate_event_bindings_code(contract, databases, event_names)
}

pub fn generate_event_handlers(
    indexer_name: &str,
    is_filter: bool,
    contract: &Contract,
) -> Result<String, Box<dyn Error>> {
    let abi_items = get_abi_items(contract, is_filter)?;
    let event_names = extract_event_names_and_signatures_from_abi(&abi_items)?;

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
        r#"use crate::rindexer::{indexer_name_formatted}::events::{handler_registry_name}::{{no_extensions, NewEventOptions, {event_type_name}"#,
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

    let mut code = String::new();

    for event in event_names {
        let event_type_name = generate_event_type_name(&contract.name);

        imports.push_str(&format!(
            r#",{handler_name}Event"#,
            handler_name = event.name,
        ));

        let columns = generate_columns_names_only(&event.inputs);

        let insert_sql = format!(
            // NOTE IF YOU CHANGE PARAMETERS BELOW MAIN ONES CHANGE THE COUNT 4 ON generate_injected_param
            "INSERT INTO {}.{} (contract_address, {}, \"tx_hash\", \"block_number\", \"block_hash\") {}",
            camel_to_snake(indexer_name),
            camel_to_snake(&event.name),
            &columns.join(", "),
            // NOTE IF YOU CHANGE PARAMETERS ABOVE MAIN ONES CHANGE THE COUNT 4 HERE
            generate_injected_param(4 + columns.len())
        );

        let postgres_params =
            generate_abi_name_properties(&event.inputs, &GenerateAbiPropertiesType::Object, None);

        let mut params_sql = String::new();
        params_sql.push_str("&[&EthereumSqlTypeWrapper::Address(&result.tx_information.address),");
        for postgres_param in postgres_params {
            if postgres_param.ethereum_sql_type_wrapper.is_some() {
                params_sql.push_str(&format!(
                    "&{}(&result.event_data.{}{}),",
                    postgres_param.ethereum_sql_type_wrapper.unwrap(),
                    postgres_param.value,
                    if postgres_param.abi_type.contains("bytes") {
                        let static_bytes = postgres_param
                            .abi_type
                            .replace("bytes", "")
                            .replace("[]", "");
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
                params_sql.push_str(&format!("&result.event_data.{},", postgres_param.value))
            }
        }

        params_sql.push_str(
            "&EthereumSqlTypeWrapper::H256(&result.tx_information.transaction_hash.unwrap()),",
        );
        params_sql.push_str(
            "&EthereumSqlTypeWrapper::U64(&result.tx_information.block_number.unwrap()),",
        );
        params_sql
            .push_str("&EthereumSqlTypeWrapper::H256(&result.tx_information.block_hash.unwrap())");
        params_sql.push(']');

        let postgres = format!(
            r#"for result in results {{
                    context
                        .database
                        .execute("{insert_sql}",
                        {params_sql})
                        .await.unwrap();
                }}
        "#,
            insert_sql = insert_sql.replace("\"", "\\\""),
            params_sql = params_sql
        );

        let handler = format!(
            r#"
            async fn {handler_fn_name}_handler(registry: &mut EventCallbackRegistry) {{
                {event_type_name}::{handler_name}(
                    {handler_name}Event::new(
                        Arc::new(|results, context| {{
                            Box::pin(async move {{
                                println!("{handler_name} event: {{:?}}", results);
                                {postgres}}})
                        }}),
                        no_extensions(),
                        NewEventOptions::default(),
                    )
                    .await,
                )
                .register(registry);
            }}
        "#,
            handler_fn_name = camel_to_snake(&event.name),
            handler_name = event.name,
            event_type_name = event_type_name,
            postgres = postgres
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

    code.push_str(&imports);
    code.push_str(&handlers);
    code.push_str(&registry_fn);

    Ok(code)
}
