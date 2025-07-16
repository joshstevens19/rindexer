use crate::{manifest::network::Network, types::code::Code};

fn network_provider_name(network: &Network) -> String {
    network_provider_name_from_name(&network.name)
}

fn network_provider_name_from_name(network_name: &str) -> String {
    format!("{network_name}_PROVIDER", network_name = network_name.to_uppercase())
}

pub fn network_provider_fn_name(network: &Network) -> String {
    format!("get_{fn_name}", fn_name = network_provider_name(network).to_lowercase())
}

#[cfg(not(feature = "reth"))]
fn generate_reth_init_fn(_network: &Network) -> Code {
    Code::new(
        r#"
            let chain_state_notification = None;
            "#
        .to_string(),
    )
}

#[cfg(feature = "reth")]
fn generate_reth_init_fn(network: &Network) -> Code {
    if network.is_reth_enabled() {
        let reth_cli_args = network.reth.as_ref().unwrap().to_cli_args();
        Code::new(format!(
            r#"
            use rindexer::reth::node::start_reth_node_with_exex;
            use rindexer::reth::Cli;
            let cli = Cli::try_parse_args_from({reth_cli_args:?}).unwrap();
            let chain_state_notification = start_reth_node_with_exex(cli).unwrap();
            let chain_state_notification = Some(chain_state_notification);
            "#
        ))
    } else {
        Code::new(
            r#"
            let chain_state_notification = None;
            "#
            .to_string(),
        )
    }
}

fn get_network_url(network: &Network) -> String {
    #[cfg(feature = "reth")]
    if network.is_reth_enabled() {
        network.get_reth_ipc_path().unwrap()
    } else {
        network.rpc.clone()
    }
    #[cfg(not(feature = "reth"))]
    network.rpc.clone()
}

fn generate_network_lazy_provider_code(network: &Network) -> Code {
    Code::new(format!(
        r#"
        {network_name}
            .get_or_init(|| async {{
                {reth_init_fn}
                {client_fn}(&public_read_env_value("{network_url}").unwrap_or("{network_url}".to_string()), {chain_id}, {compute_units_per_second}, {max_block_range}, {block_poll_frq} {placeholder_headers}, {get_logs_settings}, {chain_state_notification})
                .await
                .expect("Error creating provider")
            }})
            .await
            .clone()
        "#,
        network_name = network_provider_name(network),
        network_url = get_network_url(network),
        chain_id = network.chain_id,
        compute_units_per_second =
            if let Some(compute_units_per_second) = network.compute_units_per_second {
                format!("Some({compute_units_per_second})")
            } else {
                "None".to_string()
            },
        max_block_range = if let Some(max_block_range) = network.max_block_range {
            format!("Some(U64::from({max_block_range}))")
        } else {
            "None".to_string()
        },
        block_poll_frq = if let Some(block_frq) = network.block_poll_frequency {
            format!("Some(BlockPollFrequency::{block_frq:?})")
        } else {
            "None".to_string()
        },
        get_logs_settings = if let Some(settings) = &network.get_logs_settings {
            format!("Some(AddressFiltering::{:?})", settings.address_filtering)
        } else {
            "None".to_string()
        },
        client_fn =
            if network.rpc.contains("shadow") { "create_shadow_client" } else { "create_client" },
        placeholder_headers =
            if network.rpc.contains("shadow") { "" } else { ", HeaderMap::new()" },
        chain_state_notification = "chain_state_notification",
        reth_init_fn = generate_reth_init_fn(network),
    ))
}

fn generate_network_provider_code(network: &Network) -> Code {
    Code::new(format!(
        r#"
            pub async fn {fn_name}_cache() -> Arc<JsonRpcCachedProvider> {{
                {provider_init_fn}
            }}

            pub async fn {fn_name}() -> Arc<RindexerProvider> {{
                {fn_name}_cache().await.get_inner_provider()
            }}
        "#,
        fn_name = network_provider_fn_name(network),
        provider_init_fn = generate_network_lazy_provider_code(network),
    ))
}

fn generate_provider_cache_for_network_fn(networks: &[Network]) -> Code {
    let mut if_code = Code::blank();
    for network in networks {
        let network_if = format!(
            r#"
            if network == "{network_name}" {{
                return get_{network_name}_provider_cache().await;
            }}
        "#,
            network_name = network.name
        );

        if_code.push_str(&Code::new(network_if));
    }

    if_code.push_str(&Code::new(r#"panic!("Network not supported")"#.to_string()));

    let provider_cache_for_network_fn = format!(
        r#"
        pub async fn get_provider_cache_for_network(network: &str) -> Arc<JsonRpcCachedProvider>  {{
            {if_code}
        }}
    "#
    );

    Code::new(provider_cache_for_network_fn)
}

pub fn generate_networks_code(networks: &[Network]) -> Code {
    let mut output = Code::new(
        r#"
    /// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY.
    ///
    /// This file was auto generated by rindexer - https://github.com/joshstevens19/rindexer.
    /// Any manual changes to this file will be overwritten.

    use alloy::{primitives::U64, transports::http::reqwest::header::HeaderMap};
    use rindexer::{
        lazy_static,
        manifest::network::{AddressFiltering, BlockPollFrequency},
        provider::{RindexerProvider, create_client, JsonRpcCachedProvider, RetryClientError},
        notifications::ChainStateNotification,
        public_read_env_value
    };
    use std::sync::Arc;
    use tokio::sync::OnceCell;
    use tokio::sync::broadcast::Sender;

    #[allow(dead_code)]
    async fn create_shadow_client(
        rpc_url: &str,
        chain_id: u64,
        compute_units_per_second: Option<u64>,
        block_poll_frequency: Option<BlockPollFrequency>,
        max_block_range: Option<U64>,
        address_filtering: Option<AddressFiltering>,
        chain_state_notification: Option<Sender<ChainStateNotification>>,
    ) -> Result<Arc<JsonRpcCachedProvider>, RetryClientError> {
        let mut header = HeaderMap::new();
        header.insert(
            "X-SHADOW-API-KEY",
            public_read_env_value("RINDEXER_PHANTOM_API_KEY").unwrap().parse().unwrap(),
        );
        create_client(rpc_url, chain_id, compute_units_per_second, max_block_range, block_poll_frequency, header, address_filtering, chain_state_notification).await
    }
        "#
        .to_string(),
    );

    for network in networks {
        output.push_str(&Code::new(format!(
            r#"
            static {network_name}: OnceCell<Arc<JsonRpcCachedProvider>> = OnceCell::const_new();
            "#,
            network_name = network_provider_name(network),
        )));
    }

    for network in networks {
        output.push_str(&generate_network_provider_code(network));
    }

    output.push_str(&generate_provider_cache_for_network_fn(networks));

    output
}
