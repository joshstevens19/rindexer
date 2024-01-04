use crate::manifest::yaml::{Context, Contract, Mappings, Network, ABI};

use super::networks_bindings::network_provider_fn_name;

fn generate_contract_code(
    contract: &Contract,
    abi: &ABI,
    network: &Network,
) -> Result<String, Box<dyn std::error::Error>> {
    let code = format!(
        r#"
        abigen!({contract_name}, "{contract_path}");

        let contract = {contract_name}::new("{contract_address}.parse().unwrap()", Arc::new({network_fn_name}()));

    "#,
        contract_name = contract.name,
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
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = r#"use ethers::contract::abigen;"#.to_string();

    for contract in contracts {
        output.push_str(&generate_contract_code(
            &contract,
            mappings
                .abis
                .iter()
                .find(|&obj| obj.name == contract.abi)
                .unwrap(),
            networks
                .iter()
                .find(|&obj| obj.name == contract.network)
                .unwrap(),
        )?);
    }

    Ok(output)
}

pub fn generate_context_code(
    context: &Option<Context>,
    mappings: &Mappings,
    networks: &Vec<Network>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut output = String::new();

    if let Some(context) = context {
        output.push_str(&generate_contracts_code(
            &context.contracts,
            mappings,
            networks,
        )?);
    }

    Ok(output)
}
