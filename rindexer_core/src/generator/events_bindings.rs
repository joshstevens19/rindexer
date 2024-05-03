use ethers::utils::keccak256;
use serde_json::Value;
use std::error::Error;
use std::fs;

use crate::helpers::camel_to_snake;
use crate::manifest::yaml::{Contract, ContractDetails, Databases};

use super::networks_bindings::network_provider_fn_name_by_name;

struct EventInfo {
    name: String,
    signature: String,
    struct_name: String,
}

impl EventInfo {
    pub fn new(name: String, signature: String) -> Self {
        let struct_name = format!("{}Data", name);
        EventInfo {
            name,
            signature,
            struct_name,
        }
    }
}

fn format_param_type(param: &Value) -> String {
    match param["type"].as_str() {
        Some("tuple") => {
            let components = param["components"].as_array().unwrap();
            let formatted_components = components
                .iter()
                .map(format_param_type)
                .collect::<Vec<_>>()
                .join(",");
            format!("({})", formatted_components)
        }
        _ => param["type"].as_str().unwrap_or_default().to_string(),
    }
}

fn compute_topic_id(event_signature: &str) -> String {
    keccak256(event_signature)
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect()
}

fn format_event_signature(item: &Value) -> String {
    item["inputs"]
        .as_array()
        .map(|params| {
            params
                .iter()
                .map(format_param_type)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
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

fn extract_event_names_and_signatures_from_abi(
    abi_path: &str,
) -> Result<Vec<EventInfo>, Box<dyn Error>> {
    let abi_str = fs::read_to_string(abi_path)?;
    let abi_json: Value = serde_json::from_str(&abi_str)?;

    abi_json
        .as_array()
        .ok_or("Invalid ABI JSON format".into())
        .map(|events| {
            events
                .iter()
                .filter_map(|item| {
                    if item["type"] == "event" {
                        let name = item["name"].as_str()?.to_owned();
                        let signature = format_event_signature(item);

                        Some(EventInfo::new(name, signature))
                    } else {
                        None
                    }
                })
                .collect()
        })
}

fn generate_structs(contract: &Contract) -> Result<String, Box<dyn Error>> {
    let abi_str = fs::read_to_string(&contract.abi)?;
    let abi_json: Value = serde_json::from_str(&abi_str)?;

    let mut structs = String::new();

    for item in abi_json.as_array().ok_or("Invalid ABI JSON format")?.iter() {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_name = format!("{}Data", event_name);

            structs.push_str(&format!(
                "pub type {struct_name} = {abigen_mod_name}::{event_name}Filter;\n",
                struct_name = struct_name,
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
            println!("event_signature: {}", event_signature);
            let topic_id = compute_topic_id(&event_signature);
            format!(
                "{}::{}(_) => \"0x{}\",",
                event_type_name, info.name, topic_id
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
                        Arc::new(move |data| {{
                            let event = event.clone();
                            async move {{ event.call(data).await }}.boxed()
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

fn generate_contract_type_fn_code(contract: &Contract) -> String {
    let mut details = String::new();
    details.push_str("vec![");
    for contract in contract.details.iter() {
        let item = format!(
            r#"
            ContractDetails {{
                network: "{network}".to_string(),
                address: "{address}".to_string(),
                start_block: Some({start_block}),
                end_block: Some({end_block}),
                polling_every: Some({polling_every}),
            }},
        "#,
            network = contract.network,
            address = contract.address,
            // TODO! FIX
            start_block = contract.start_block.unwrap(),
            // TODO! FIX
            end_block = contract.end_block.unwrap_or(99424866),
            // TODO! FIX
            polling_every = contract.polling_every.unwrap_or(1000)
        );
        details.push_str(&item);
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
    clients: &Option<Databases>,
) -> String {
    let clients_enabled = clients.is_some();
    event_info
        .iter()
        .map(|info| {
            let csv_file_name = format!("{}-{}.csv", &info.struct_name, &info.signature).to_lowercase();
            format!(
                r#"
                    type {name}EventCallbackType<TExtensions> = Arc<dyn Fn(&Vec<{struct_name}>, Arc<EventContext<TExtensions>>) -> BoxFuture<'_, ()> + Send + Sync>;

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
                                    {client}
                                    csv: Arc::new(AsyncCsvAppender::new("{csv_file_name}".to_string())),
                                    extensions: Arc::new(extensions),
                                }}),
                            }}
                        }}
                    }}

                    #[async_trait]
                    impl<TExtensions> EventCallback for {name}Event<TExtensions> where TExtensions: Send + Sync {{
                        async fn call(&self, data: Vec<Arc<dyn Any + Send + Sync>>) {{
                            let data_len = data.len();

                            let specific_data: Vec<{struct_name}> = data.into_iter()
                                .filter_map(|item| {{
                                    item.downcast::<{struct_name}>().ok().map(|arc| (*arc).clone())
                                }})
                                .collect();

                            if specific_data.len() == data_len {{
                                (self.callback)(&specific_data, self.context.clone()).await;
                            }} else {{
                                println!("{name}Event: Unexpected data type - expected: {struct_name}")
                            }}
                        }}
                    }}
                "#,
                name = info.name,
                struct_name = info.struct_name,
                client = if clients_enabled { "client: Arc::new(PostgresClient::new().await.unwrap())," } else { "" },
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
    function.push_str("fn contract(&self, network: &str) -> RindexerLensRegistryGen<Arc<Provider<RetryClient<Http>>>> {\n");

    // Handling each contract detail with an `if` or `else if`
    for (index, contract_detail) in contracts_details.iter().enumerate() {
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
            address = contract_detail.address,
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
) -> Result<String, Box<dyn std::error::Error>> {
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
                FutureExt,
                generator::event_callback_registry::{{EventCallbackRegistry, EventInformation, ContractInformation, NetworkContract}},
                manifest::yaml::{{Contract, ContractDetails}},
                {client_import}
            }};

            use super::{abigen_file_name}::{abigen_mod_name}::{{self, {abigen_name}}};

            {structs}

            type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

            #[async_trait]
            trait EventCallback {{
                async fn call(&self, data: Vec<Arc<dyn Any + Send + Sync>>);
            }}

            pub struct EventContext<TExtensions> where TExtensions: Send + Sync {{
                {event_context_client}
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
                    let contract_information = self.contract_information();
                    let contract = ContractInformation {{
                        name: contract_information.name,
                        details: contract_information
                            .details
                            .iter()
                            .map(|c| NetworkContract {{
                                network: c.network.clone(),
                                provider: self.get_provider(&c.network),
                                decoder: self.decoder(&c.network),
                                address: c.address.clone(),
                                start_block: c.start_block,
                                end_block: c.end_block,
                                polling_every: c.polling_every,
                            }})
                            .collect(),
                        abi: contract_information.abi,
                    }};
                    
                    let callback: Arc<dyn Fn(Vec<Arc<dyn Any + Send + Sync>>) -> BoxFuture<'static, ()> + Send + Sync> = match self {{
                        {register_match_arms}
                    }};
                
                   registry.register_event({{
                        EventInformation {{
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
        event_context_client = if clients.is_some() {
            "pub client: Arc<PostgresClient>,"
        } else {
            ""
        },
        event_callback_structs = generate_event_callback_structs_code(&event_info, clients),
        event_enums = generate_event_enums_code(&event_info),
        topic_ids_match_arms = generate_topic_ids_match_arms_code(&event_type_name, &event_info),
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

pub fn generate_event_bindings(
    contract: &Contract,
    databases: &Option<Databases>,
) -> Result<String, Box<dyn Error>> {
    let event_names = extract_event_names_and_signatures_from_abi(&contract.abi)?;
    generate_event_bindings_code(contract, databases, event_names)
}
