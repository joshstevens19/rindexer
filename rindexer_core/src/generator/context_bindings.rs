use crate::{
    helpers::camel_to_snake,
    manifest::yaml::{Context, Contract, Mappings, Network, ABI},
};

use super::networks_bindings::network_provider_fn_name;

fn generate_contract_code(
    contract: &Contract,
    abi: &ABI,
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
            abigen!({contract_name}, "{contract_path}");

            pub fn get_{contract_fn_name}() -> {contract_name}<Provider<Http>> {{
                let address: Address = "{contract_address}"
                .parse()
                .unwrap();

                {contract_name}::new(address, Arc::new({network_fn_name}().clone()))
            }}
        "#,
        contract_name = contract.name,
        contract_fn_name = camel_to_snake(&contract.name),
        contract_address = contract.address,
        network_fn_name = network_provider_fn_name(&network),
        contract_path = abi.file
    );

    Ok(code)
}

fn generate_contracts_code(
    contracts: &Vec<Contract>,
    mappings: &Mappings,
    networks: &Vec<Network>,
    for_global: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = r#"
        use std::sync::Arc;
        use ethers::{contract::abigen, abi::Address, providers::{Provider, Http}};
    "#
    .to_string();

    let mut network_import = String::new();
    let mut code = String::new();

    for contract in contracts {
        let network = networks
            .iter()
            .find(|&obj| obj.name == contract.network)
            .unwrap();

        network_import.push_str(network_provider_fn_name(&network).as_str());

        code.push_str(&generate_contract_code(
            &contract,
            mappings
                .abis
                .iter()
                .find(|&obj| obj.name == contract.abi)
                .unwrap(),
            network,
        )?);
    }

    if for_global {
        output.push_str("use super::networks::get_polygon_provider;");
    } else {
        output.push_str(format!("use super::super::networks::{{{}}};", network_import).as_str());
    }
    output.push_str(&code);

    Ok(output)
}

pub fn generate_context_code(
    context: &Option<Context>,
    mappings: &Mappings,
    networks: &Vec<Network>,
    for_global: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = String::new();

    if let Some(context) = context {
        output.push_str(&generate_contracts_code(
            &context.contracts,
            mappings,
            networks,
            for_global,
        )?);
    }

    Ok(output)
}
