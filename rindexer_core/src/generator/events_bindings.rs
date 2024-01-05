use ethers::utils::keccak256;
use serde_json::Value;
use std::error::Error;
use std::fs;

use crate::{
    helpers::{camel_to_snake, capitalize_first_letter},
    manifest::yaml::Source,
};

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
            format!("tuple({})", formatted_components)
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

fn map_base_solidity_type_to_rust(solidity_type: &str) -> String {
    match solidity_type {
        "uint256" | "uint" => "U256".to_string(),
        "int256" | "int" => "U256".to_string(),
        "uint128" | "uint64" | "uint32" | "uint16" | "uint8" => "u64".to_string(),
        "int128" | "int64" | "int32" | "int16" | "int8" => "i64".to_string(),
        "address" => "Address".to_string(),
        "bool" => "bool".to_string(),
        "string" => "String".to_string(),
        // Dynamic byte array
        "bytes" => "Vec<u8>".to_string(),
        // Fixed size bytes (bytes1, bytes2, ..., bytes32)
        typ if typ.starts_with("bytes") && typ.len() > 5 => {
            let size: usize = typ[5..].parse().unwrap_or(0);
            format!("[u8; {}]", size).to_string()
        }
        // Arrays
        typ if typ.ends_with("[]") => {
            let inner_type = &typ[..typ.len() - 2];
            format!("Vec<{}>", map_base_solidity_type_to_rust(inner_type)).to_string()
        }
        // Nested Arrays (handling two-dimensional arrays)
        typ if typ.ends_with("[][]") => {
            let inner_type = &typ[..typ.len() - 4];
            format!("Vec<Vec<{}>>", map_base_solidity_type_to_rust(inner_type)).to_string()
        }
        // Custom Types (Enums and Structs)
        // "enum" => "YourCustomEnum".to_string(),
        // Fallback for unsupported or unrecognized types
        _ => "String".to_string(),
    }
}

fn map_solidity_type_to_rust(
    param: &Value,
    parent_struct_name: &str,
    structs: &mut String,
) -> String {
    match param["type"].as_str() {
        Some("tuple") => {
            let tuple_struct_name = format!(
                "{}{}",
                parent_struct_name,
                capitalize_first_letter(param["name"].as_str().unwrap_or("Tuple"))
            );
            let mut fields = String::new();
            for component in param["components"].as_array().unwrap_or(&Vec::new()) {
                let component_name = component["name"].as_str().unwrap_or("field");
                let component_type =
                    map_solidity_type_to_rust(component, &tuple_struct_name, structs);
                fields.push_str(&format!(
                    "    pub {}: {},\n",
                    component_name, component_type
                ));
            }
            structs.push_str(&format!(
                "#[derive(Debug, serde::Serialize, serde::Deserialize)]\npub struct {} {{\n{}\n}}\n\n",
                tuple_struct_name, fields
            ));
            tuple_struct_name
        }
        _ => map_base_solidity_type_to_rust(param["type"].as_str().unwrap_or_default()),
    }
}

fn generate_structs_from_abi(abi_path: &str) -> Result<String, Box<dyn std::error::Error>> {
    let abi_str = fs::read_to_string(abi_path)?;
    let abi_json: Value = serde_json::from_str(&abi_str)?;

    let mut structs = String::new();

    for item in abi_json.as_array().ok_or("Invalid ABI JSON format")?.iter() {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_name = format!("{}Data", event_name);

            let mut fields = String::new();
            for param in item["inputs"].as_array().unwrap_or(&Vec::new()) {
                let param_name = param["name"].as_str().unwrap_or("");
                let snake_case_name = camel_to_snake(param_name);
                let param_type = map_solidity_type_to_rust(param, &struct_name, &mut structs);
                fields.push_str(&format!(
                    "    #[serde(rename = \"{}\")]\n    pub {}: {},\n",
                    param_name, snake_case_name, param_type
                ));
            }

            structs.push_str(&format!(
                "#[derive(Debug, serde::Serialize, serde::Deserialize)]\npub struct {} {{\n{}\n}}\n\n",
                struct_name, fields
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
                "{}::{}(event) => Box::new(move |data| event.call(data)),",
                event_type_name, info.name
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
                    end_block: None,
                    polling_every: Some({polling_every}),
                    abi: "{abi}".to_string(),
                }}
            }}
            "#,
        name = source.name,
        address = source.address,
        network = source.network,
        start_block = source.start_block.unwrap_or(0),
        // end_block = source.end_block.unwrap_or(0),
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
                        pub callback: Box<dyn Fn(&{struct_name})>,
                    }}

                    impl EventCallback for {name}Event {{
                        fn call(&self, data: &dyn Any) {{
                            if let Some(specific_data) = data.downcast_ref::<{struct_name}>() {{
                                (self.callback)(specific_data);
                            }} else {{
                                println!("{name}Event: Unexpected data type - expected: {struct_name} - received: {{:?}}", data)
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
            use ethers::{{types::{{Address, U256}}, providers::{{Provider, Http}}}};
            use std::any::Any;

            use rindexer_core::{{
                generator::event_callback_registry::{{EventCallbackRegistry, EventInformation}},
                manifest::yaml::Source,
            }};

            {structs}

            trait EventCallback {{
                fn call(&self, data: &dyn Any);
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
                
                pub fn register(self, registry: &mut EventCallbackRegistry) {{
                    let topic_id = self.topic_id();
                    let source = self.source();
                    let provider = self.get_provider();
                    let callback: Box<dyn Fn(&dyn Any) + 'static> = match self {{
                        {register_match_arms}
                    }};
                
                   registry.register_event({{
                        EventInformation {{
                            topic_id,
                            source,
                            provider,
                            callback,
                        }}
                    }});
                }}
            }}
        "#,
        structs = generate_structs_from_abi(abi_path)?,
        event_type_name = &event_type_name,
        event_callback_structs = generate_event_callback_structs_code(&event_info),
        event_enums = generate_event_enums_code(&event_info),
        topic_ids_match_arms = generate_topic_ids_match_arms_code(&event_type_name, &event_info),
        source_type_fn = generate_source_type_fn_code(&source),
        network_provider_fn_name = network_provider_fn_name_by_name(&source.network),
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
