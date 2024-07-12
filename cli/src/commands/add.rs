use std::{borrow::Cow, fs, path::PathBuf};

use ethers::{
    addressbook::{Address, Chain},
    prelude::ValueOrArray,
};
use ethers_etherscan::Client;
use rindexer::{
    manifest::{
        contract::{Contract, ContractDetails},
        yaml::{read_manifest, write_manifest, YAML_CONFIG_NAME},
    },
    write_file,
};

use crate::{
    console::{
        print_error_message, print_success_message, prompt_for_input, prompt_for_input_list,
    },
    rindexer_yaml::validate_rindexer_yaml_exist,
};

pub async fn handle_add_contract_command(
    project_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_rindexer_yaml_exist();

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let mut manifest = read_manifest(&rindexer_yaml_path).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e));
        e
    })?;

    let rindexer_abis_folder = project_path.join("abis");

    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return Err(err.into());
    }

    let networks: Vec<(&str, u32)> =
        manifest.networks.iter().map(|network| (network.name.as_str(), network.chain_id)).collect();

    if networks.is_empty() {
        print_error_message("No networks found in rindexer.yaml. Please add a network first before downloading ABIs.");
        return Err("No networks found in rindexer.yaml.".into());
    }

    let network_choices: Vec<String> = networks.iter().map(|(name, _)| name.to_string()).collect();

    let network = if network_choices.len() > 1 {
        Cow::Owned(prompt_for_input_list("Enter Network Name", &network_choices, None))
    } else {
        Cow::Borrowed(&network_choices[0])
    };

    let chain_id = networks
        .iter()
        .find(|(name, _)| *name == network.as_ref())
        .expect("Unreachable: Network not found in networks")
        .1;

    let chain_network = Chain::try_from(chain_id).map_err(|e| {
        print_error_message("Network is not supported by etherscan API.");
        e
    })?;
    let contract_address =
        prompt_for_input(&format!("Enter {} Contract Address", network), None, None, None);

    let client = Client::builder()
        .chain(chain_network)
        .map_err(|e| {
            print_error_message(&format!("Invalid chain id {}", e));
            e
        })?
        .build()
        .map_err(|e| {
            print_error_message(&format!("Failed to create etherscan client: {}", e));
            e
        })?;

    let address = contract_address.parse().map_err(|e| {
        print_error_message(&format!("Invalid contract address: {}", e));
        e
    })?;

    loop {
        let metadata = client.contract_source_code(address).await.map_err(|e| {
            print_error_message(&format!("Failed to fetch contract metadata: {}", e));
            e
        })?;

        if metadata.items.is_empty() {
            print_error_message(&format!(
                "No contract found on network {} with address {}.",
                network, contract_address
            ));
            break;
        }

        let item = &metadata.items[0];
        if item.proxy == 1 && item.implementation.is_some() {
            println!("This contract is a proxy contract. Loading the implementation contract...");
            continue;
        }

        let contract_name = manifest.contracts.iter().find(|c| c.name == item.contract_name);
        let contract_name = if contract_name.is_some() {
            Cow::Owned(prompt_for_input(
                &format!("Enter a name for the contract as it is clashing with another registered contract name in the yaml: {}", item.contract_name),
                None,
                None,
                None,
            ))
        } else {
            Cow::Borrowed(&item.contract_name)
        };

        let abi_file_name = format!("{}.abi.json", contract_name);

        let abi_path = rindexer_abis_folder.join(&abi_file_name);
        write_file(&abi_path, &item.abi).map_err(|e| {
            print_error_message(&format!("Failed to write ABI file: {}", e));
            e
        })?;

        let abi_path_relative = format!("./abis/{}", abi_file_name);

        print_success_message(&format!(
            "Downloaded ABI for: {} in {}",
            contract_name, &abi_path_relative
        ));

        let success_message = format!(
            "Updated rindexer.yaml with contract: {} and ABI path: {}",
            contract_name, abi_path_relative
        );

        manifest.contracts.push(Contract {
            name: contract_name.into_owned(),
            details: vec![ContractDetails::new_with_address(
                network.to_string(),
                ValueOrArray::<Address>::Value(address),
                None,
                None,
                None,
            )],
            abi: abi_path_relative,
            include_events: None,
            index_event_in_order: None,
            dependency_events: None,
            reorg_safe_distance: None,
            generate_csv: None,
        });

        write_manifest(&manifest, &rindexer_yaml_path).map_err(|e| {
            print_error_message(&format!("Failed to write rindexer.yaml file: {}", e));
            e
        })?;

        print_success_message(&success_message);

        break;
    }

    Ok(())
}
