use crate::manifest::yaml::Network;

fn network_provider_name(network: &Network) -> String {
    network_provider_name_from_name(&network.name)
}

fn network_provider_name_from_name(network_name: &str) -> String {
    format!(
        "{network_name}_PROVIDER",
        network_name = network_name.to_uppercase()
    )
}

pub fn network_provider_fn_name(network: &Network) -> String {
    format!(
        "get_{fn_name}",
        fn_name = network_provider_name(&network).to_lowercase()
    )
}

pub fn network_provider_fn_name_by_name(network_name: &str) -> String {
    format!(
        "get_{fn_name}",
        fn_name = network_provider_name_from_name(&network_name).to_lowercase()
    )
}

fn generate_network_lazy_provider_code(
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
            static ref {network_name}: Provider<Http> = Provider::<Http>::try_from("{network_url}").expect("Error creating provider");
        "#,
        network_name = network_provider_name(&network),
        network_url = network.url
    );

    Ok(code)
}

fn generate_network_provider_code(network: &Network) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
            pub fn {fn_name}() -> &'static Provider<Http> {{
                &{provider_lazy_name}
            }}
        "#,
        fn_name = network_provider_fn_name(&network),
        provider_lazy_name = network_provider_name(&network)
    );

    Ok(code)
}

pub fn generate_networks_code(
    networks: &Vec<Network>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = r#"
            use rindexer_core::lazy_static;
            use ethers::providers::{Provider, Http};

            lazy_static! {
        "#
    .to_string();

    for network in networks {
        output.push_str(&generate_network_lazy_provider_code(network)?);
    }

    output.push_str("}");

    for network in networks {
        output.push_str(&generate_network_provider_code(network)?);
    }

    Ok(output)
}
