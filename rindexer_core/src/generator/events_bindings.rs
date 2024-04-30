use ethers::utils::keccak256;
use serde_json::Value;
use std::error::Error;
use std::fs;

use crate::{helpers::camel_to_snake, manifest::yaml::Source};

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

pub fn abigen_source_name(source: &Source) -> String {
    format!("Rindexer{}Gen", source.name)
}

fn abigen_source_mod_name(source: &Source) -> String {
    camel_to_snake(&abigen_source_name(source))
}

pub fn abigen_source_file_name(source: &Source) -> String {
    format!("{}_abi_gen", camel_to_snake(&source.name))
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

fn generate_structs(abi_path: &str, source: &Source) -> Result<String, Box<dyn std::error::Error>> {
    let abi_str = fs::read_to_string(abi_path)?;
    let abi_json: Value = serde_json::from_str(&abi_str)?;

    let mut structs = String::new();

    for item in abi_json.as_array().ok_or("Invalid ABI JSON format")?.iter() {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_name = format!("{}Data", event_name);

            structs.push_str(&format!(
                "pub type {struct_name} = {abigen_mod_name}::{event_name}Filter;\n",
                struct_name = struct_name,
                abigen_mod_name = abigen_source_mod_name(source),
                event_name = event_name
            ));
        }
    }

    Ok(structs)
}

fn generate_event_enums_code(event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| format!("{}({}Event),", info.name, info.name))
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
                            match contract.decode_event::<{event_info_name}Data>(&"{event_info_name}".to_string(), topics, data.into()) {{
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

fn generate_source_type_fn_code(source: &Source) -> String {
    format!(
        r#"
            fn source(&self) -> Source {{
                Source {{
                    name: "{name}".to_string(),
                    address: "{address}".to_string(),
                    network: "{network}".to_string(),
                    start_block: Some({start_block}),
                    end_block: Some({end_block}),
                    polling_every: Some({polling_every}),
                    abi: "{abi}".to_string(),
                }}
            }}
            "#,
        name = source.name,
        address = source.address,
        network = source.network,
        start_block = source.start_block.unwrap(),
        // TODO! FIX
        end_block = source.end_block.unwrap_or(99424866),
        polling_every = source.polling_every.unwrap_or(1000),
        abi = source.abi
    )
}

fn generate_event_callback_structs_code(event_info: &[EventInfo]) -> String {
    event_info
        .iter()
        .map(|info| {
            format!(
                r#"
                    pub struct {name}Event {{
                        pub callback: Arc<dyn Fn(&{struct_name}) -> BoxFuture<'_, ()> + Send + Sync>,
                    }}

                    #[async_trait]
                    impl EventCallback for {name}Event {{
                        async fn call(&self, data: Arc<dyn Any + Send + Sync>) {{
                             if let Ok(specific_data) = Arc::downcast::<{struct_name}>(data) {{
                                (self.callback)(&specific_data).await;
                            }} else {{
                                println!("{name}Event: Unexpected data type - expected: {struct_name}")
                            }}
                        }}
                    }}
                "#,
                name = info.name,
                struct_name = info.struct_name
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_event_bindings_code(
    source: &Source,
    event_info: Vec<EventInfo>,
    abi_path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let event_type_name = generate_event_type_name(&source.name);
    let code = format!(
        r#"
            use std::{{any::Any, sync::Arc}};

            use std::future::Future;
            use std::pin::Pin;

            use futures::FutureExt;

            use ethers::{{providers::{{Http, Provider}}, abi::Address, types::{{Bytes, H256}}}};
            
            use rindexer_core::{{
                async_trait,
                FutureExt,
                generator::event_callback_registry::{{EventCallbackRegistry, EventInformation}},
                manifest::yaml::Source,
            }};

            use super::{abigen_file_name}::{abigen_mod_name}::{{self, {abigen_name}}};

            {structs}

            type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

            #[async_trait]
            trait EventCallback {{
                async fn call(&self, data: Arc<dyn Any + Send + Sync>);
            }}

            {event_callback_structs}

            pub enum {event_type_name} {{
                {event_enums}
            }}

            impl {event_type_name} {{
                pub fn topic_id(&self) -> &'static str {{
                    match self {{
                        {topic_ids_match_arms}
                    }}
                }}

                {source_type_fn}

                fn get_provider(&self) -> &'static Provider<Http> {{
                    &crate::rindexer::networks::{network_provider_fn_name}()
                }}

                pub fn contract(&self) -> {abigen_name}<Provider<Http>> {{
                    let address: Address = "{contract_address}"
                        .parse()
                        .unwrap();

                    {abigen_name}::new(address, Arc::new(self.get_provider().clone()))
                }}

                pub fn decoder(&self) -> Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync> {{
                    let contract = self.contract();

                    match self {{
                        {decoder_match_arms}
                    }}
                }}
                
                pub fn register(self, registry: &mut EventCallbackRegistry) {{
                    let topic_id = self.topic_id();
                    let source = self.source();
                    let provider = self.get_provider();
                    let decoder = self.decoder();
                    
                    let callback: Arc<dyn Fn(Arc<dyn Any + Send + Sync>) -> BoxFuture<'static, ()> + Send + Sync> = match self {{
                        {register_match_arms}
                    }};
                
                   registry.register_event({{
                        EventInformation {{
                            topic_id,
                            source,
                            provider,
                            callback,
                            decoder
                        }}
                    }});
                }}
            }}
        "#,
        abigen_mod_name = abigen_source_mod_name(&source),
        abigen_file_name = abigen_source_file_name(&source),
        abigen_name = abigen_source_name(&source),
        structs = generate_structs(abi_path, &source)?,
        event_type_name = &event_type_name,
        event_callback_structs = generate_event_callback_structs_code(&event_info),
        event_enums = generate_event_enums_code(&event_info),
        topic_ids_match_arms = generate_topic_ids_match_arms_code(&event_type_name, &event_info),
        source_type_fn = generate_source_type_fn_code(&source),
        network_provider_fn_name = network_provider_fn_name_by_name(&source.network),
        contract_address = &source.address,
        decoder_match_arms = generate_decoder_match_arms_code(&event_type_name, &event_info),
        register_match_arms = generate_register_match_arms_code(&event_type_name, &event_info)
    );

    Ok(code)
}

pub fn generate_event_bindings_from_abi(
    source: &Source,
    abi_path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let event_names = extract_event_names_and_signatures_from_abi(abi_path)?;
    generate_event_bindings_code(source, event_names, abi_path)
}
