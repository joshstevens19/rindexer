use crate::manifest::yaml::Network;

fn network_provider_name(network: &Network) -> String {
    format!(
        "{network_name}_PROVIDER",
        network_name = network.name.to_uppercase()
    )
}

fn generate_network_lazy_provider_code(
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
        static ref {network_name}: Mutex<Provider<Http>> = Mutex::new(
            Provider::<Http>::try_from("{network_url}")
                .expect("Error creating provider")
        );
    "#,
        network_name = network_provider_name(&network),
        network_url = network.url
    );

    Ok(code)
}

pub fn network_provider_fn_name(network: &Network) -> String {
    format!(
        "get_{fn_name}",
        fn_name = network_provider_name(&network).to_lowercase()
    )
}

fn generate_network_provider_code(network: &Network) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
        pub fn {fn_name}() -> &'static Mutex<Provider<Http>> {{
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
    use lazy_static::lazy_static;
    use ethers::providers::{Provider, Http};
    use std::sync::Mutex;

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
