use crate::manifest::yaml::ContractDetails;
use crate::{
    helpers::camel_to_snake,
    manifest::yaml::{Contract, Network},
};

use super::networks_bindings::network_provider_fn_name;

/// Generates the contract code for a specific contract and network.
///
/// # Arguments
///
/// * `contract_name` - The name of the contract.
/// * `contract_details` - The details of the contract.
/// * `abi_location` - The location of the ABI file.
/// * `network` - The network configuration.
///
/// # Returns
///
/// A `Result` containing the generated contract code as a `String`, or an error if something goes wrong.
fn generate_contract_code(
    contract_name: &str,
    contract_details: &ContractDetails,
    abi_location: &str,
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(address) = contract_details.address() {
        let code = format!(
            r#"
            abigen!({contract_name}, "{contract_path}");

            pub fn get_{contract_fn_name}() -> {contract_name}<Arc<Provider<RetryClient<Http>>>> {{
                let address: Address = "{contract_address}"
                .parse()
                .unwrap();

                {contract_name}::new(address, Arc::new({network_fn_name}().clone()))
            }}
        "#,
            contract_name = contract_name,
            contract_fn_name = camel_to_snake(contract_name),
            contract_address = address,
            network_fn_name = network_provider_fn_name(network),
            contract_path = abi_location
        );
        Ok(code)
    } else {
        Ok(String::new())
    }
}

/// Generates the code for all contracts across multiple networks.
///
/// # Arguments
///
/// * `contracts` - A reference to a vector of `Contract` configurations.
/// * `networks` - A reference to a slice of `Network` configurations.
///
/// # Returns
///
/// A `Result` containing the generated contracts code as a `String`, or an error if something goes wrong.
fn generate_contracts_code(
    contracts: &[Contract],
    networks: &[Network],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = r#"
        use std::sync::Arc;
        use ethers::{contract::abigen, abi::Address, providers::{Provider, Http, RetryClient}};
    "#
    .to_string();

    let mut code = String::new();

    for contract in contracts {
        for details in &contract.details {
            if let Some(network) = networks.iter().find(|&n| n.name == details.network) {
                code.push_str(&generate_contract_code(
                    &contract.name,
                    details,
                    &contract.abi,
                    network,
                )?);
            }
        }
    }

    let network_imports: Vec<String> = networks.iter().map(network_provider_fn_name).collect();
    output.push_str(&format!(
        "use super::networks::{{{}}};",
        network_imports.join(", ")
    ));
    output.push_str(&code);

    Ok(output)
}

/// Generates the context code for the given contracts and networks.
///
/// # Arguments
///
/// * `contracts` - An optional reference to a vector of `Contract` configurations.
/// * `networks` - A reference to a slice of `Network` configurations.
///
/// # Returns
///
/// A `Result` containing the generated context code as a `String`, or an error if something goes wrong.
pub fn generate_context_code(
    contracts: &Option<Vec<Contract>>,
    networks: &[Network],
) -> Result<String, Box<dyn std::error::Error>> {
    if let Some(contracts) = contracts {
        generate_contracts_code(contracts, networks)
    } else {
        Ok(String::new())
    }
}
