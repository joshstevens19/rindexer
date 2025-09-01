use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::abi::AbiProperty;
use crate::helpers::{is_irregular_width_solidity_integer_type, is_solidity_static_bytes_type};
use crate::{
    abi::{
        ABIInput, ABIItem, CreateCsvFileForEvent, EventInfo, GenerateAbiPropertiesType,
        ParamTypeError, ReadAbiError,
    },
    database::postgres::generate::{
        generate_column_names_only_with_base_properties, generate_event_table_full_name,
    },
    helpers::camel_to_snake,
    indexer::native_transfer::{NATIVE_TRANSFER_ABI, NATIVE_TRANSFER_CONTRACT_NAME},
    manifest::{
        contract::ParseAbiError,
        native_transfer::{NativeTransferDetails, NativeTransfers},
        storage::{CsvDetails, Storage},
    },
    types::code::Code,
    EthereumSqlTypeWrapper,
};

pub fn trace_abigen_contract_name(contract_name: &str) -> String {
    format!("Rindexer{contract_name}Gen")
}

pub fn trace_abigen_contract_file_name(contract_name: &str) -> String {
    format!("{}_abi_gen", camel_to_snake(contract_name))
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateStructsError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("Invalid ABI JSON format")]
    InvalidAbiJsonFormat,

    #[error("{0}")]
    ParseAbiError(#[from] ParseAbiError),
}

fn trace_generate_structs(contract_name: &str) -> Result<Code, GenerateStructsError> {
    let abi_str = NATIVE_TRANSFER_ABI;
    let abi_json: Value = serde_json::from_str(abi_str)?;

    let mut structs = Code::blank();

    for item in abi_json.as_array().ok_or(GenerateStructsError::InvalidAbiJsonFormat)?.iter() {
        if item["type"] == "event" {
            let event_name = item["name"].as_str().unwrap_or_default();
            let struct_result = format!("{event_name}Result");
            let struct_data = format!("{event_name}Data");

            structs.push_str(&Code::new(format!(
                r#"
                    pub type {struct_data} = {abigen_mod_name}::{event_name};

                    #[derive(Debug, Clone)]
                    pub struct {struct_result} {{
                        pub event_data: {struct_data},
                        pub tx_information: TxInformation
                    }}

                    impl HasTxInformation for {struct_result} {{
                        fn tx_information(&self) -> &TxInformation {{
                            &self.tx_information
                        }}
                    }}
                "#,
                struct_result = struct_result,
                struct_data = struct_data,
                abigen_mod_name = trace_abigen_contract_name(contract_name),
            )));
        }
    }

    // Add Block event support
    structs.push_str(&Code::new(
        r#"
        pub type BlockData = alloy::network::AnyRpcBlock;
        
        #[derive(Debug, Clone)]
        pub struct BlockResult {
            pub block: Box<BlockData>,
            pub tx_information: TxInformation
        }

        impl HasTxInformation for BlockResult {
            fn tx_information(&self) -> &TxInformation {
                &self.tx_information
            }
        }
        "#
        .to_string(),
    ));

    Ok(structs)
}

fn generate_event_enums_code(event_info: &[EventInfo]) -> Code {
    let mut events = event_info
        .iter()
        .map(|info| format!("{}({}Event<TExtensions>),", info.name, info.name))
        .collect::<Vec<_>>();

    // Add Block event to the enum
    events.push("Block(BlockEvent<TExtensions>),".to_string());

    Code::new(events.join("\n"))
}

fn generate_event_type_name(name: &str) -> String {
    format!("{name}EventType")
}

fn generate_event_names_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    let mut arms = event_info
        .iter()
        .map(|info| format!("{}::{}(_) => \"{}\",", event_type_name, info.name, info.name))
        .collect::<Vec<_>>();

    // Add Block event match arm
    arms.push(format!("{}::Block(_) => \"Block\",", event_type_name));

    Code::new(arms.join("\n"))
}

fn generate_register_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    let mut arms = event_info
        .iter()
        .map(|info| {
            format!(
                r#"
                {}::{}(event) => {{
                    let event = Arc::new(event);
                    Arc::new(move |result| {{
                        let event = Arc::clone(&event);
                        async move {{ event.call(result).await }}.boxed()
                    }})
                }},
            "#,
                event_type_name, info.name
            )
        })
        .collect::<Vec<_>>();

    // Add Block event register match arm
    arms.push(format!(
        r#"
        {}::Block(event) => {{
            let event = Arc::new(event);
            Arc::new(move |result| {{
                let event = Arc::clone(&event);
                async move {{ event.call(result).await }}.boxed()
            }})
        }},
    "#,
        event_type_name
    ));

    Code::new(arms.join("\n"))
}

fn generate_decoder_match_arms_code(event_type_name: &str, event_info: &[EventInfo]) -> Code {
    let mut arms = event_info
        .iter()
        .map(|info| {
            format!(
                r#"
                {event_type_name}::{event_info_name}(_) => {{
                    Arc::new(move |topics: Vec<B256>, data: Bytes| {{
                        match {event_info_name}Data::decode_raw_log(topics, &data[0..]) {{
                            Ok(event) => {{
                                let result: {event_info_name}Data = event;
                                Arc::new(result) as Arc<dyn Any + Send + Sync>
                            }}
                            Err(error) => Arc::new(error) as Arc<dyn Any + Send + Sync>,
                        }}
                    }})
                }}
            "#,
                event_type_name = event_type_name,
                event_info_name = info.name
            )
        })
        .collect::<Vec<_>>();

    // Add Block event decoder match arm
    arms.push(format!(
        r#"
        {event_type_name}::Block(_) => {{
            Arc::new(move |_topics: Vec<B256>, _data: Bytes| {{
                // Block events don't need decoding, they're passed as-is
                Arc::new(()) as Arc<dyn Any + Send + Sync>
            }})
        }}
    "#,
        event_type_name = event_type_name
    ));

    Code::new(arms.join("\n"))
}

fn generate_csv_instance(
    project_path: &Path,
    contract: &NativeTransfers,
    event_info: &EventInfo,
    csv: &Option<CsvDetails>,
) -> Result<Code, CreateCsvFileForEvent> {
    let mut csv_path = csv.as_ref().map_or(PathBuf::from("generated_csv"), |c| {
        PathBuf::from(c.path.strip_prefix("./").unwrap())
    });

    csv_path = project_path.join(csv_path);

    if !contract.generate_csv.unwrap_or(true) {
        return Ok(Code::new(format!(
            r#"let csv = AsyncCsvAppender::new(r"{}");"#,
            csv_path.display(),
        )));
    }

    let csv_path_str = csv_path.to_str().expect("Failed to convert csv path to string");
    let csv_path = event_info.create_csv_file_for_event(
        project_path,
        NATIVE_TRANSFER_CONTRACT_NAME,
        csv_path_str,
    )?;
    let headers: Vec<String> =
        event_info.csv_headers_for_event().iter().map(|h| format!("\"{h}\"")).collect();

    let headers_with_into: Vec<String> = headers.iter().map(|h| format!("{h}.into()")).collect();

    Ok(Code::new(format!(
        r#"
        let csv = AsyncCsvAppender::new(r"{}");
        if !Path::new(r"{}").exists() {{
            csv.append_header(vec![{}].into())
                .await
                .expect("Failed to write CSV header");
        }}
    "#,
        csv_path,
        csv_path,
        headers_with_into.join(", ")
    )))
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateTraceCallbackStructsError {
    #[error("{0}")]
    CreateCsvFileForEvent(#[from] CreateCsvFileForEvent),
}

fn generate_trace_callback_structs_code(
    project_path: &Path,
    event_info: &[EventInfo],
    contract: &NativeTransfers,
    storage: &Storage,
) -> Result<Code, GenerateTraceCallbackStructsError> {
    let databases_enabled = storage.postgres_enabled();
    let csv_enabled = storage.csv_enabled();
    let is_filter = false; // TODO: Hardcoded for now

    let mut parts = Vec::new();

    for info in event_info {
        let csv_generator = if csv_enabled {
            generate_csv_instance(project_path, contract, info, &storage.csv)?
        } else {
            Code::blank()
        };

        let part = format!(
            r#"
            pub fn {lower_name}_handler<TExtensions, F, Fut>(
                custom_logic: F,
            ) -> {name}TraceCallbackType<TExtensions>
            where
                {struct_result}: Clone + 'static,
                F: for<'a> Fn(Vec<{struct_result}>, Arc<TraceContext<TExtensions>>) -> Fut
                    + Send
                    + Sync
                    + 'static
                    + Clone,
                Fut: Future<Output = TraceCallbackResult<()>> + Send + 'static,
                TExtensions: Send + Sync + 'static,
            {{
                Arc::new(move |results, context| {{
                    let custom_logic = custom_logic.clone();
                    let results = results.clone();
                    let context = Arc::clone(&context);
                    async move {{ (custom_logic)(results, context).await }}.boxed()
                }})
            }}

            type {name}TraceCallbackType<TExtensions> = Arc<
                dyn for<'a> Fn(&'a Vec<{struct_result}>, Arc<TraceContext<TExtensions>>) -> BoxFuture<'a, TraceCallbackResult<()>>
                    + Send
                    + Sync,
                >;

            pub struct {name}Event<TExtensions> where TExtensions: Send + Sync + 'static {{
                callback: {name}TraceCallbackType<TExtensions>,
                context: Arc<TraceContext<TExtensions>>,
            }}

            impl<TExtensions> {name}Event<TExtensions> where TExtensions: Send + Sync + 'static {{
                pub async fn handler<F, Fut>(closure: F, extensions: TExtensions) -> Self
                where
                    {struct_result}: Clone + 'static,
                    F: for<'a> Fn(Vec<{struct_result}>, Arc<TraceContext<TExtensions>>) -> Fut
                        + Send
                        + Sync
                        + 'static
                        + Clone,
                    Fut: Future<Output = TraceCallbackResult<()>> + Send + 'static,
                {{
                    {csv_generator}

                    Self {{
                        callback: {lower_name}_handler(closure),
                        context: Arc::new(TraceContext {{
                            {database}
                            {csv}
                            extensions: Arc::new(extensions),
                        }}),
                    }}
                }}
            }}

            #[async_trait]
            impl<TExtensions> TraceCallback for {name}Event<TExtensions> where TExtensions: Send + Sync {{
                async fn call(&self, events: Vec<TraceResult>) -> TraceCallbackResult<()> {{
                    {event_callback_events_len}

                    // note some can not downcast because it cant decode
                    // this happens on events which failed decoding due to
                    // not having the right abi for example
                    // transfer events with 2 indexed topics cant decode
                    // transfer events with 3 indexed topics
                    let result: Vec<{name}Result> = events
                        .into_iter()
                        .filter_map(|item| {{
                            match item {{
                                TraceResult::NativeTransfer {{ from, to, value, tx_information, .. }} => {{
                                    Some({name}Result {{
                                        event_data: {name}Data {{
                                            from,
                                            to,
                                            value,
                                        }},
                                        tx_information,
                                    }})
                                }}
                                _ => None,
                            }}
                        }})
                        .collect();

                    {event_callback_return}
                }}
            }}
            "#,
            name = info.name,
            lower_name = info.name.to_lowercase(),
            struct_result = info.struct_result(),
            database = if databases_enabled {
                "database: get_or_init_postgres_client().await,"
            } else {
                ""
            },
            csv = if csv_enabled { r#"csv: Arc::new(csv),"# } else { "" },
            csv_generator = csv_generator,
            event_callback_events_len =
                if !is_filter { "let _events_len = events.len();" } else { "" },
            event_callback_return = if !is_filter {
                r#"
                        (self.callback)(&result, Arc::clone(&self.context)).await
                    "#
                .to_string()
            } else {
                "(self.callback)(&result, Arc::clone(&self.context)).await".to_string()
            }
        );

        parts.push(part);
    }

    // Add Block event callback struct
    let block_part = format!(
        r#"
        pub fn block_handler<TExtensions, F, Fut>(
            custom_logic: F,
        ) -> BlockTraceCallbackType<TExtensions>
        where
            BlockResult: Clone + 'static,
            F: for<'a> Fn(Vec<BlockResult>, Arc<TraceContext<TExtensions>>) -> Fut
                + Send
                + Sync
                + 'static
                + Clone,
            Fut: Future<Output = TraceCallbackResult<()>> + Send + 'static,
            TExtensions: Send + Sync + 'static,
        {{
            Arc::new(move |results, context| {{
                let custom_logic = custom_logic.clone();
                let results = results.clone();
                let context = Arc::clone(&context);
                async move {{ (custom_logic)(results, context).await }}.boxed()
            }})
        }}

        type BlockTraceCallbackType<TExtensions> = Arc<
            dyn for<'a> Fn(&'a Vec<BlockResult>, Arc<TraceContext<TExtensions>>) -> BoxFuture<'a, TraceCallbackResult<()>>
                + Send
                + Sync,
            >;

        pub struct BlockEvent<TExtensions> where TExtensions: Send + Sync + 'static {{
            callback: BlockTraceCallbackType<TExtensions>,
            context: Arc<TraceContext<TExtensions>>,
        }}

        impl<TExtensions> BlockEvent<TExtensions> where TExtensions: Send + Sync + 'static {{
            pub async fn handler<F, Fut>(closure: F, extensions: TExtensions) -> Self
            where
                BlockResult: Clone + 'static,
                F: for<'a> Fn(Vec<BlockResult>, Arc<TraceContext<TExtensions>>) -> Fut
                    + Send
                    + Sync
                    + 'static
                    + Clone,
                Fut: Future<Output = TraceCallbackResult<()>> + Send + 'static,
            {{
                Self {{
                    callback: block_handler(closure),
                    context: Arc::new(TraceContext {{
                        {database}
                        extensions: Arc::new(extensions),
                    }}),
                }}
            }}
        }}

        #[async_trait]
        impl<TExtensions> TraceCallback for BlockEvent<TExtensions> where TExtensions: Send + Sync {{
            async fn call(&self, events: Vec<TraceResult>) -> TraceCallbackResult<()> {{
                // Block events are passed as-is, no decoding needed
                let result: Vec<BlockResult> = events
                    .into_iter()
                    .filter_map(|item| {{
                        match item {{
                            TraceResult::Block {{ block, tx_information, .. }} => {{
                                Some(BlockResult {{ block, tx_information }})
                            }}
                            _ => None,
                        }}
                    }})
                    .collect();

                if !result.is_empty() {{
                    (self.callback)(&result, Arc::clone(&self.context)).await
                }} else {{
                    Ok(())
                }}
            }}
        }}
        "#,
        database =
            if databases_enabled { "database: get_or_init_postgres_client().await," } else { "" },
    );

    parts.push(block_part);

    Ok(Code::new(parts.join("\n")))
}

fn decoder_contract_fn(contracts_details: Vec<NativeTransferDetails>, abi_gen_name: &str) -> Code {
    let mut function = String::new();
    function.push_str(&format!(
        r#"pub async fn decoder_contract(network: &str) -> {abi_gen_name}Instance<Arc<RindexerProvider>, AnyNetwork> {{"#
    ));

    let networks: Vec<&String> = contracts_details.iter().map(|c| &c.network).collect();
    for (index, network) in networks.iter().enumerate() {
        if index == 0 {
            function.push_str("    if ");
        } else {
            function.push_str("    else if ");
        }

        function.push_str(&format!(
            r#"network == "{network}" {{
                {abi_gen_name}::new(
                    // do not care about address here its decoding makes it easier to handle ValueOrArray
                    Address::ZERO,
                    get_provider_cache_for_network(network).await.get_inner_provider(),
                 )
            }}"#
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

fn build_pub_contract_fn(
    contract_name: &str,
    _contracts_details: Vec<NativeTransferDetails>,
    abi_gen_name: &str,
) -> Code {
    let contract_name = camel_to_snake(contract_name);

    Code::new(format!(
        r#"pub async fn {contract_name}_contract(network: &str, address: Address) -> {abi_gen_name}Instance<Arc<RindexerProvider>, AnyNetwork> {{
                {abi_gen_name}::new(
                    address,
                    get_provider_cache_for_network(network).await.get_inner_provider(),
                 )
               }}
            "#
    ))
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateTraceBindingCodeError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("{0}")]
    GenerateStructsError(#[from] GenerateStructsError),

    #[error("{0}")]
    GenerateTraceCallbackStructsError(#[from] GenerateTraceCallbackStructsError),
}

fn generate_trace_bindings_code(
    project_path: &Path,
    indexer_name: &str,
    contract_name: &str,
    contract: &NativeTransfers,
    storage: &Storage,
    event_info: Vec<EventInfo>,
) -> Result<Code, GenerateTraceBindingCodeError> {
    let event_type_name = generate_event_type_name(contract_name);

    let code = Code::new(format!(
        r#"#![allow(non_camel_case_types, clippy::enum_variant_names, clippy::too_many_arguments, clippy::upper_case_acronyms, clippy::type_complexity, dead_code)]
        /// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY.
        ///
        /// This file was auto generated by rindexer - https://github.com/joshstevens19/rindexer.
        /// Any manual changes to this file will be overwritten.

        use super::{abigen_file_name}::{{
            {abigen_name}::{{self, {abigen_name}Instance, {abigen_name}Events}}
        }};
        use std::{{any::Any, sync::Arc}};
        use std::error::Error;
        use std::future::Future;
        use std::collections::HashMap;
        use std::pin::Pin;
        use std::path::{{Path, PathBuf}};
        use alloy::network::AnyNetwork;
        use alloy::primitives::{{Address, Bytes, B256}};
        use alloy::sol_types::{{SolEvent, SolEventInterface, SolType}};
        use rindexer::{{
            async_trait,
            {csv_import}
            generate_random_id,
            FutureExt,
            event::{{
                callback_registry::{{
                    TraceCallbackRegistry, TraceCallbackRegistryInformation, TraceCallbackResult,
                    TraceResult, TxInformation, HasTxInformation
                }},
                contract_setup::{{TraceInformation, NetworkTrace}},
            }},
            manifest::{{
                contract::{{Contract, ContractDetails}},
                yaml::read_manifest,
            }},
            provider::{{JsonRpcCachedProvider, RindexerProvider}},
            {postgres_client_import}
        }};
        use super::super::super::super::typings::networks::get_provider_cache_for_network;
        {postgres_import}

        {structs}

        type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

        #[async_trait]
        trait TraceCallback {{
            async fn call(&self, events: Vec<TraceResult>) -> TraceCallbackResult<()>;
        }}

        pub struct TraceContext<TExtensions> where TExtensions: Send + Sync {{
            {event_context_database}
            {event_context_csv}
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

        {decoder_contract_fn}

        impl<TExtensions> {event_type_name}<TExtensions> where TExtensions: 'static + Send + Sync {{
            pub fn event_name(&self) -> &'static str {{
                match self {{
                    {event_names_match_arms}
                }}
            }}

            pub fn contract_name(&self) -> String {{
                "{raw_contract_name}".to_string()
            }}

            async fn get_provider(&self, network: &str) -> Arc<JsonRpcCachedProvider> {{
                get_provider_cache_for_network(network).await
            }}

            fn decoder(&self, network: &str) -> Arc<dyn Fn(Vec<B256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync> {{
                let decoder_contract = decoder_contract(network);

                match self {{
                    {decoder_match_arms}
                }}
            }}

            pub async fn register(self, manifest_path: &PathBuf, registry: &mut TraceCallbackRegistry) {{
                 let rindexer_yaml = read_manifest(manifest_path).expect("Failed to read rindexer.yaml");
                 let contract_name = self.contract_name();
                 let event_name = self.event_name();

                 let contract_details = rindexer_yaml
                    .native_transfers.networks.unwrap_or_default();

                // Expect providers to have been initialized, but it's an async init so this should
                // be fast but for correctness we must await each future.
                let mut providers = HashMap::new();
                for n in contract_details.iter() {{
                    let provider = self.get_provider(&n.network).await;
                    providers.insert(n.network.clone(), provider);
                }}

                let trace_information = TraceInformation {{
                    name: event_name.to_string(),
                    details: contract_details
                        .iter()
                        .map(|c| NetworkTrace {{
                            id: generate_random_id(10),
                            network: c.network.clone(),
                            cached_provider: providers
                                .get(&c.network)
                                .expect("must have a provider")
                                .clone(),
                            start_block: c.start_block,
                            end_block: c.end_block,
                            method: c.method,
                        }})
                        .collect(),
                    reorg_safe_distance: rindexer_yaml
                        .native_transfers.reorg_safe_distance.unwrap_or_default(),
                }};

                let callback: Arc<dyn Fn(Vec<TraceResult>) -> BoxFuture<'static, TraceCallbackResult<()>> + Send + Sync> = match self {{
                    {register_match_arms}
                }};


               registry.register_event(TraceCallbackRegistryInformation {{
                    id: generate_random_id(10),
                    indexer_name: "{indexer_name}".to_string(),
                    event_name: event_name.to_string(),
                    contract_name: contract_name.to_string(),
                    trace_information: trace_information,
                    callback,
                }});
            }}
        }}
        "#,
        postgres_import = if storage.postgres_enabled() {
            "use super::super::super::super::typings::database::get_or_init_postgres_client;"
        } else {
            ""
        },
        postgres_client_import = if storage.postgres_enabled() { "PostgresClient," } else { "" },
        csv_import = if storage.csv_enabled() { "AsyncCsvAppender," } else { "" },
        abigen_file_name = trace_abigen_contract_file_name(contract_name),
        abigen_name = trace_abigen_contract_name(contract_name),
        structs = trace_generate_structs(contract_name)?,
        event_type_name = &event_type_name,
        event_context_database =
            if storage.postgres_enabled() { "pub database: Arc<PostgresClient>," } else { "" },
        event_context_csv =
            if storage.csv_enabled() { "pub csv: Arc<AsyncCsvAppender>," } else { "" },
        event_callback_structs =
            generate_trace_callback_structs_code(project_path, &event_info, contract, storage)?,
        event_enums = generate_event_enums_code(&event_info),
        event_names_match_arms =
            generate_event_names_match_arms_code(&event_type_name, &event_info),
        raw_contract_name = contract_name,
        decoder_contract_fn = decoder_contract_fn(
            contract.networks.clone().unwrap_or_default(),
            &trace_abigen_contract_name(contract_name)
        ),
        build_pub_contract_fn = build_pub_contract_fn(
            contract_name,
            contract.networks.clone().unwrap_or_default(),
            &trace_abigen_contract_name(contract_name)
        ),
        decoder_match_arms = generate_decoder_match_arms_code(&event_type_name, &event_info),
        register_match_arms = generate_register_match_arms_code(&event_type_name, &event_info)
    ));

    Ok(code)
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateTraceBindingsError {
    #[error("{0}")]
    ReadAbi(#[from] ReadAbiError),

    #[error("{0}")]
    GenerateEventBindingCode(#[from] GenerateTraceBindingCodeError),

    #[error("{0}")]
    ParamType(#[from] ParamTypeError),
}

/// Minimal changes by hardcoding the "mocked" native transfer Event abi as per erc20 standard.
fn get_native_transfer_abi_items() -> Vec<ABIItem> {
    vec![ABIItem {
        inputs: vec![
            ABIInput {
                indexed: Some(true),
                name: "from".to_string(),
                type_: "address".to_string(),
                components: None,
            },
            ABIInput {
                indexed: Some(true),
                name: "to".to_string(),
                type_: "address".to_string(),
                components: None,
            },
            ABIInput {
                indexed: Some(false),
                name: "value".to_string(),
                type_: "uint256".to_string(),
                components: None,
            },
        ],
        name: "NativeTransfer".to_string(),
        type_: "event".to_string(),
    }]
}

pub fn generate_trace_bindings(
    project_path: &Path,
    indexer_name: &str,
    contract_name: &str,
    contract: &NativeTransfers,
    _is_filter: bool,
    storage: &Storage,
) -> Result<Code, GenerateTraceBindingsError> {
    let abi_items = get_native_transfer_abi_items();
    let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;

    generate_trace_bindings_code(
        project_path,
        indexer_name,
        contract_name,
        contract,
        storage,
        event_names,
    )
    .map_err(GenerateTraceBindingsError::GenerateEventBindingCode)
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateTraceHandlersError {
    #[error("{0}")]
    ReadAbiError(#[from] ReadAbiError),

    #[error("{0}")]
    ParamTypeError(#[from] ParamTypeError),
}

pub fn generate_trace_handlers(
    indexer_name: &str,
    contract_name: &str,
    storage: &Storage,
) -> Result<Code, GenerateTraceHandlersError> {
    let abi_items = get_native_transfer_abi_items();
    let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;

    let mut imports = String::new();
    imports.push_str(
        r#"#![allow(non_snake_case)]
            use rindexer::{
                event::callback_registry::TraceCallbackRegistry,
                EthereumSqlTypeWrapper, PgType, RindexerColorize, rindexer_error, rindexer_info
            };
        "#,
    );
    imports.push_str("use std::sync::Arc;\n");
    imports.push_str(&format!(
        r#"use std::path::PathBuf;
        use super::super::super::typings::{indexer_name_formatted}::events::{handler_registry_name}::{{no_extensions, {event_type_name}"#,
        indexer_name_formatted = camel_to_snake(indexer_name),
        handler_registry_name = camel_to_snake(contract_name),
        event_type_name = generate_event_type_name(contract_name)
    ));

    let mut handlers = String::new();

    let mut registry_fn = String::new();
    registry_fn.push_str(&format!(
        r#"pub async fn {handler_registry_fn_name}_handlers(manifest_path: &PathBuf, registry: &mut TraceCallbackRegistry) {{"#,
        handler_registry_fn_name = camel_to_snake(contract_name),
    ));

    for event in event_names {
        let event_type_name = generate_event_type_name(contract_name);

        imports.push_str(&format!(r#",{handler_name}Event"#, handler_name = event.name,));

        let abi_name_properties = ABIInput::generate_abi_name_properties(
            &event.inputs,
            &GenerateAbiPropertiesType::Object,
            None,
        );

        let mut csv_write = String::new();
        // this checks storage enabled as well
        if !storage.csv_disable_create_headers() {
            let mut csv_data = String::new();
            csv_data.push_str(r#"format!("{:?}", result.tx_information.address),"#);

            for item in &abi_name_properties {
                // note: tracing uses a custom NativeTransfer event
                // so only address and uint256 are used
                csv_data.push_str(&format!("result.event_data.{}.to_string(),", item.value));
            }

            csv_data.push_str(r#"format!("{:?}", result.tx_information.transaction_hash),"#);
            csv_data.push_str(r#"result.tx_information.block_number.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.block_hash.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.network.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.transaction_index.to_string(),"#);
            csv_data.push_str(r#"result.tx_information.log_index.to_string()"#);

            csv_write = format!(r#"csv_bulk_data.push(vec![{csv_data}]);"#,);

            if storage.postgres_disable_create_tables() {
                csv_write = format!(
                    r#"
                      let mut csv_bulk_data: Vec<Vec<String>> = vec![];
                      for result in &results {{
                        {inner_csv_write}
                      }}

                      if !csv_bulk_data.is_empty() {{
                        let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                        if let Err(e) = csv_result {{
                            rindexer_error!("{event_type_name}::{handler_name} inserting csv data: {{:?}}", e);
                            return Err(e.to_string());
                        }}
                      }}
                    "#,
                    inner_csv_write = csv_write,
                    event_type_name = event_type_name,
                    handler_name = event.name,
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
                        "EthereumSqlTypeWrapper::{}(result.event_data.{}),",
                        wrapper.raw_name(),
                        item.value,
                    ));
                } else {
                    // data.push_str(&format!("result.event_data.{},", item.value));
                    panic!("No EthereumSqlTypeWrapper found for: {:?}", item.abi_type);
                }
            }

            data.push_str(
                "\nEthereumSqlTypeWrapper::B256(result.tx_information.transaction_hash),",
            );
            data.push_str("\nEthereumSqlTypeWrapper::U64(result.tx_information.block_number),");
            data.push_str("\nEthereumSqlTypeWrapper::DateTimeNullable(result.tx_information.block_timestamp_to_datetime()),");
            data.push_str("\nEthereumSqlTypeWrapper::B256(result.tx_information.block_hash),");
            data.push_str(
                "EthereumSqlTypeWrapper::String(result.tx_information.network.to_string()),",
            );
            data.push_str(
                "\nEthereumSqlTypeWrapper::U64(result.tx_information.transaction_index),",
            );
            data.push_str("\nEthereumSqlTypeWrapper::U256(result.tx_information.log_index)");
            data.push(']');

            postgres_write = format!(
                r#"
                    let mut postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = vec![];
                    {csv_bulk_data}
                    for result in results.iter() {{
                        {csv_write}
                        let data = {data};
                        postgres_bulk_data.push(data);
                    }}

                    {csv_bulk_insert}

                    if postgres_bulk_data.is_empty() {{
                        return Ok(());
                    }}

                    let result = context
                        .database
                        .insert_bulk(
                            "{table_name}",
                            &[{columns_names}],
                            &postgres_bulk_data,
                        )
                        .await;

                    if let Err(e) = result {{
                        rindexer_error!("{event_type_name}::{handler_name} inserting bulk data: {{:?}}", e);
                        return Err(e.to_string());
                    }}
                "#,
                table_name =
                    generate_event_table_full_name(indexer_name, contract_name, &event.name),
                handler_name = event.name,
                event_type_name = event_type_name,
                columns_names = generate_column_names_only_with_base_properties(&event.inputs)
                    .iter()
                    .map(|item| format!("\"{item}\".to_string()"))
                    .collect::<Vec<String>>()
                    .join(", "),
                data = data,
                csv_write = csv_write,
                csv_bulk_data = if storage.csv_enabled() {
                    "let mut csv_bulk_data: Vec<Vec<String>> = vec![];"
                } else {
                    ""
                },
                csv_bulk_insert = if storage.csv_enabled() {
                    format!(
                        r#"if !csv_bulk_data.is_empty() {{
                        let csv_result = context.csv.append_bulk(csv_bulk_data).await;
                        if let Err(e) = csv_result {{
                            rindexer_error!("{event_type_name}::{handler_name} inserting csv data: {{:?}}", e);
                            return Err(e.to_string());
                        }}
                    }}"#,
                        event_type_name = event_type_name,
                        handler_name = event.name
                    )
                } else {
                    "".to_string()
                }
            );
        }

        let handler = format!(
            r#"
            async fn {handler_fn_name}_handler(manifest_path: &PathBuf, registry: &mut TraceCallbackRegistry) {{
                {event_type_name}::{handler_name}(
                    {handler_name}Event::handler(|results, context| async move {{
                            if results.is_empty() {{
                                return Ok(());
                            }}

                            {csv_write}
                            {postgres_write}

                            rindexer_info!(
                                "{contract_name}::{handler_name} - {{}} - {{}} events",
                                "INDEXED".green(),
                                results.len(),
                            );

                            Ok(())
                        }},
                        no_extensions(),
                      )
                      .await,
                )
                .register(manifest_path, registry).await;
            }}
        "#,
            handler_fn_name = camel_to_snake(&event.name),
            handler_name = event.name,
            event_type_name = event_type_name,
            contract_name = contract_name,
            csv_write = if !postgres_write.is_empty() { String::new() } else { csv_write },
            postgres_write = postgres_write,
        );

        handlers.push_str(&handler);

        registry_fn.push_str(&format!(
            r#"
                {handler_fn_name}_handler(manifest_path, registry).await;
            "#,
            handler_fn_name = camel_to_snake(&event.name)
        ));
    }

    // Add Block event handler
    imports.push_str(",BlockEvent};\n");

    let block_handler = format!(
        r#"
        async fn block_handler(manifest_path: &PathBuf, registry: &mut TraceCallbackRegistry) {{
            BlockEvent::handler(|results, context| async move {{
                if results.is_empty() {{
                    return Ok(());
                }}

                rindexer_info!(
                    "{contract_name}::Block - {{}} - {{}} blocks",
                    "INDEXED".green(),
                    results.len(),
                );

                Ok(())
            }},
            no_extensions(),
          )
          .await,
        }}
        "#,
        contract_name = contract_name,
    );

    handlers.push_str(&block_handler);

    registry_fn.push_str("block_handler(manifest_path, registry).await;");

    registry_fn.push('}');

    let mut code = String::new();
    code.push_str(&imports);
    code.push_str(&handlers);
    code.push_str(&registry_fn);

    Ok(Code::new(code))
}
