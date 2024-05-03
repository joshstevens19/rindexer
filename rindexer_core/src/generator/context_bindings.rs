use crate::manifest::yaml::ContractDetails;
use crate::{
    helpers::camel_to_snake,
    manifest::yaml::{Contract, Network},
};

use super::networks_bindings::network_provider_fn_name;

fn generate_contract_code(
    contract_name: &str,
    contract_details: &ContractDetails,
    abi_location: &str,
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
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
        contract_fn_name = camel_to_snake(&contract_name),
        contract_address = contract_details.address,
        network_fn_name = network_provider_fn_name(network),
        contract_path = abi_location
    );

    Ok(code)
}

fn generate_contracts_code(
    contracts: &Vec<Contract>,
    networks: &[Network],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = r#"
        use std::sync::Arc;
        use ethers::{contract::abigen, abi::Address, providers::{Provider, Http, RetryClient}};
    "#
    .to_string();

    let mut network_import = String::new();
    let mut code = String::new();

    for contract in contracts {
        for details in &contract.details {
            let network = networks
                .iter()
                .find(|&obj| obj.name == details.network)
                .unwrap();

            network_import.push_str(network_provider_fn_name(network).as_str());

            code.push_str(&generate_contract_code(
                &contract.name,
                &details,
                &contract.abi,
                network,
            )?);
        }
    }

    output.push_str("use super::networks::get_polygon_provider;");
    output.push_str(&code);

    Ok(output)
}

pub fn generate_context_code(
    contracts: &Option<Vec<Contract>>,
    networks: &[Network],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = String::new();

    if let Some(contracts) = contracts {
        output.push_str(&generate_contracts_code(contracts, networks)?);
    }

    Ok(output)
}
