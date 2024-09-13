use std::{borrow::Cow, fs, path::PathBuf, time::Duration};

use ethers::{
    addressbook::{Address, Chain},
    prelude::ValueOrArray,
};
use ethers_etherscan::Client;
use rindexer::{
    manifest::{
        contract::{Contract, ContractDetails},
        yaml::{read_manifest_raw, write_manifest, YAML_CONFIG_NAME},
    },
    public_read_env_value, write_file, StringOrArray,
};

use crate::{
    commands::BACKUP_ETHERSCAN_API_KEY,
    console::{
        print_error_message, print_success_message, prompt_for_input, prompt_for_input_list,
    },
    rindexer_yaml::validate_rindexer_yaml_exist,
};

pub async fn handle_add_contract_command(
    project_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_rindexer_yaml_exist(&project_path);

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let mut manifest = read_manifest_raw(&rindexer_yaml_path).inspect_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e))
    })?;

    let rindexer_abis_folder = project_path.join("abis");

    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return Err(err.into());
    }

    let networks: Vec<(&str, u64)> =
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

    let chain_network = Chain::try_from(chain_id)
        .inspect_err(|_| print_error_message("Network is not supported by etherscan API, please add the contract manually in the rindexer.yaml file"))?;
    let contract_address =
        prompt_for_input(&format!("Enter {} Contract Address", network), None, None, None);

    let etherscan_api_key =
        manifest.global.as_ref().and_then(|global| global.etherscan_api_key.as_ref()).map_or_else(
            || BACKUP_ETHERSCAN_API_KEY.to_string(),
            |key| public_read_env_value(key).unwrap_or_else(|_| key.to_string()),
        );

    let client = Client::builder()
        .with_api_key(etherscan_api_key)
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

    let address = contract_address
        .parse()
        .inspect_err(|e| print_error_message(&format!("Invalid contract address: {}", e)))?;

    let mut abi_lookup_address = address;
    let mut timeout = 1000;
    let mut retry_attempts = 0;
    let max_retries = 3;

    loop {
        let metadata = match client.contract_source_code(abi_lookup_address).await {
            Ok(data) => data,
            Err(e) => {
                if retry_attempts >= max_retries {
                    print_error_message(&format!(
                        "Failed to fetch contract metadata: {}, retries: {}",
                        e, retry_attempts
                    ));
                    return Err(Box::new(e));
                }
                // Different verifiers have different rate limits which leads to
                // rate limit errors when adding a contract.
                // Etherscan has good rate limits whereas Arbiscan is not as good
                // Sleeping here avoids APIs hitting rate limit
                tokio::time::sleep(Duration::from_millis(timeout)).await;
                retry_attempts += 1;
                timeout *= retry_attempts * 2;
                continue;
            }
        };

        if metadata.items.is_empty() {
            print_error_message(&format!(
                "No contract found on network {} with address {}.",
                network, contract_address
            ));
            break;
        }

        let item = &metadata.items[0];
        if item.proxy == 1 && item.implementation.is_some() {
            abi_lookup_address = item.implementation.unwrap();
            println!(
                "This contract is a proxy contract. Loading the implementation contract {}",
                abi_lookup_address
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
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
            abi: StringOrArray::Single(abi_path_relative),
            include_events: None,
            index_event_in_order: None,
            dependency_events: None,
            reorg_safe_distance: None,
            generate_csv: None,
            streams: None,
            chat: None,
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
