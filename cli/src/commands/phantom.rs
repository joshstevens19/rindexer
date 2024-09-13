use std::{
    env,
    error::Error,
    fs,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use ethers::types::{Address, ValueOrArray, U64};
use rindexer::{
    manifest::{
        network::Network,
        phantom::{Phantom, PhantomDyrpc, PhantomShadow},
        yaml::{read_manifest, read_manifest_raw, write_manifest, YAML_CONFIG_NAME},
    },
    phantom::{
        common::{read_compiled_contract, read_contract_clone_metadata},
        create_dyrpc_api_key, deploy_dyrpc_contract,
        shadow::deploy_shadow_contract,
    },
    public_read_env_value, write_file, StringOrArray,
};

use crate::{
    cli_interface::{PhantomBaseArgs, PhantomSubcommands},
    commands::BACKUP_ETHERSCAN_API_KEY,
    console::{
        print_error_message, print_success_message, print_warn_message, prompt_for_input,
        prompt_for_input_list,
    },
    rindexer_yaml::validate_rindexer_yaml_exist,
};

const RINDEXER_PHANTOM_API_ENV_KEY: &str = "RINDEXER_PHANTOM_API_KEY";

pub async fn handle_phantom_commands(
    project_path: PathBuf,
    command: &PhantomSubcommands,
) -> Result<(), Box<dyn Error>> {
    validate_rindexer_yaml_exist(&project_path);

    match command {
        PhantomSubcommands::Init => handle_phantom_init(&project_path).await,
        PhantomSubcommands::Clone { contract_name, network } => handle_phantom_clone(
            &project_path,
            &PhantomBaseArgs {
                contract_name: contract_name.to_owned(),
                network: network.to_owned(),
            },
        ),
        PhantomSubcommands::Compile { contract_name, network } => handle_phantom_compile(
            &project_path,
            &PhantomBaseArgs {
                contract_name: contract_name.to_owned(),
                network: network.to_owned(),
            },
        ),
        PhantomSubcommands::Deploy { contract_name, network } => {
            handle_phantom_deploy(
                &project_path,
                &PhantomBaseArgs {
                    contract_name: contract_name.to_owned(),
                    network: network.to_owned(),
                },
            )
            .await
        }
    }
}

fn install_foundry() -> Result<(), Box<dyn Error>> {
    let foundry_check =
        Command::new("which").arg("foundryup").output().expect("Failed to execute command");

    if foundry_check.status.success() {
        Ok(())
    } else {
        println!("Foundry is not installed. Installing Foundry...");

        let install_command = Command::new("sh")
            .arg("-c")
            .arg("curl -L https://foundry.paradigm.xyz | bash")
            .status()
            .map_err(|e| e.to_string())?;

        if install_command.success() {
            Ok(())
        } else {
            Err("Failed to install Foundry.".into())
        }
    }
}

async fn handle_phantom_init(project_path: &Path) -> Result<(), Box<dyn Error>> {
    let env_file = project_path.join(".env");
    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let mut manifest = read_manifest_raw(&rindexer_yaml_path).inspect_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e))
    })?;

    if manifest.phantom.is_some() {
        let error_message = "phantom already setup in rindexer.yaml";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    print_success_message("setting up phantom events on rindexer...");

    install_foundry()?;

    let phantom_provider_choice = prompt_for_input_list(
        "Which provider are you using?",
        &["shadow".to_string(), "dyrpc".to_string()],
        None,
    );

    let mut api_key_value = prompt_for_input(
        if phantom_provider_choice == "dyrpc" {
            "Enter your dyRPC API key (enter to new to generate a new key)"
        } else {
            "Enter your Shadow API key"
        },
        None,
        None,
        None,
    );

    match phantom_provider_choice.as_str() {
        "dyrpc" => {
            if api_key_value == "new" {
                api_key_value = create_dyrpc_api_key().await?;
                println!(
                    "Your API has been created and key is {} - it has also been written to your .env file.",
                    api_key_value
                );
            }

            manifest.phantom = Some(Phantom {
                dyrpc: Some(PhantomDyrpc {
                    api_key: format!("${{{}}}", RINDEXER_PHANTOM_API_ENV_KEY),
                }),
                shadow: None,
            });

            write_manifest(&manifest, &rindexer_yaml_path)?;
        }
        "shadow" => {
            let fork_id = prompt_for_input("Enter the fork ID", None, None, None);

            manifest.phantom = Some(Phantom {
                shadow: Some(PhantomShadow {
                    api_key: format!("${{{}}}", RINDEXER_PHANTOM_API_ENV_KEY),
                    fork_id,
                }),
                dyrpc: None,
            });

            write_manifest(&manifest, &rindexer_yaml_path)?;
        }
        value => panic!("Unknown phantom provider: {}", value),
    }

    let env_content = fs::read_to_string(&env_file).unwrap_or_default();

    let value = api_key_value;

    let mut lines: Vec<String> = env_content.lines().map(|line| line.to_string()).collect();
    let mut key_found = false;
    for line in &mut lines {
        if line.starts_with(&format!("{}=", RINDEXER_PHANTOM_API_ENV_KEY)) {
            *line = format!("{}={}", RINDEXER_PHANTOM_API_ENV_KEY, value);
            key_found = true;
            break;
        }
    }

    if !key_found {
        lines.push(format!("{}={}", RINDEXER_PHANTOM_API_ENV_KEY, value));
    }

    let new_env_content = lines.join("\n");

    let mut file = OpenOptions::new().write(true).truncate(true).create(true).open(&env_file)?;

    writeln!(file, "{}", new_env_content)?;

    print_success_message("rindexer Phantom events are now setup.\nYou can now use `rindexer phantom clone --contract-name <contract> --network <network>` to start adding your own custom events.");

    Ok(())
}

fn forge_clone_contract(
    clone_in: &Path,
    network: &Network,
    address: &Address,
    contract_name: &str,
    etherscan_api_key: &str,
) -> Result<(), Box<dyn Error>> {
    print_success_message(&format!(
        "Cloning contract {} on network {} at address {:?} this may take a little moment...",
        contract_name, network.name, address
    ));
    let output = Command::new("forge")
        .arg("clone")
        .arg("--no-commit")
        .arg(format!("{:?}", address))
        //.arg(format!("--chain {}", network.chain_id))
        .arg("--etherscan-api-key")
        .arg(etherscan_api_key)
        .arg(contract_name)
        .current_dir(clone_in)
        .output()?;

    if output.status.success() {
        Ok(())
    } else {
        print_error_message(&format!(
            "Failed to clone contract: {} at address: {:?}",
            contract_name, address
        ));
        print_error_message(&format!("Error: {}", String::from_utf8_lossy(&output.stderr)));
        Err("Failed to clone contract".into())
    }
}

fn handle_phantom_clone(project_path: &Path, args: &PhantomBaseArgs) -> Result<(), Box<dyn Error>> {
    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let manifest = read_manifest(&rindexer_yaml_path).inspect_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e))
    })?;

    if manifest.phantom.is_none() {
        let error_message =
            "phantom not setup in rindexer.yaml. Please run `rindexer phantom init` first.";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    let cloning_location =
        project_path.join("phantom").join(&args.network).join(args.contract_name.as_str());
    if cloning_location.exists() {
        let error_message = format!("Phantom contract {} on network {} already cloned in {}. If you want to clone it again please delete the folder first.", args.contract_name, args.network, cloning_location.display());
        print_error_message(&error_message);
        return Err(error_message.into());
    }

    let contract = manifest.contracts.iter().find(|c| c.name == args.contract_name);
    match contract {
        Some(contract) => {
            let network = manifest.networks.iter().find(|n| n.name == args.network);
            if network.is_none() {
                let error_message = format!("Network {} not found in rindexer.yaml", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            if network.unwrap().chain_id != 1 {
                let error_message = format!("Network {} is not supported", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            let contract_network = contract.details.iter().find(|c| c.network == args.network);
            if let Some(contract_network) = contract_network {
                if let Some(address) = contract_network.address() {
                    // pick the first one as the ABI has to match so assume all contracts do
                    let address = match address {
                        ValueOrArray::Value(address) => address,
                        ValueOrArray::Array(addresses) => {
                            print_warn_message(&format!("Multiple addresses found for contract {} on network {} rindexer.yaml, using first one", args.contract_name.as_str(), args.network.as_str()));
                            addresses.first().unwrap()
                        }
                    };

                    if !project_path.join("phantom").exists() {
                        fs::create_dir(project_path.join("phantom"))?;
                    }

                    let clone_in = project_path.join("phantom").join(&args.network);
                    if !clone_in.exists() {
                        fs::create_dir(&clone_in)?;
                    }

                    let etherscan_api_key = manifest
                        .global
                        .as_ref()
                        .and_then(|global| global.etherscan_api_key.as_ref())
                        .map_or_else(
                            || BACKUP_ETHERSCAN_API_KEY.to_string(),
                            |key| public_read_env_value(key).unwrap_or_else(|_| key.to_string()),
                        );

                    forge_clone_contract(
                        &clone_in,
                        network.unwrap(),
                        address,
                        contract.name.as_str(),
                        &etherscan_api_key,
                    )
                    .map_err(|e| format!("Failed to clone contract: {}", e))?;

                    print_success_message(format!("\ncloned {} in {} you can start adding your custom events.\nYou can now use `rindexer phantom compile -contract-name {} --network {}` to compile the phantom contract anytime.", contract.name.as_str(), clone_in.display(), contract.name.as_str(), args.network).as_str());

                    Ok(())
                } else {
                    let error_message = format!(
                        "Contract {} in network {} does not have an address in rindexer.yaml",
                        args.contract_name, args.network
                    );
                    print_error_message(&error_message);
                    Err(error_message.into())
                }
            } else {
                let error_message = format!(
                    "Network {} not found in contract {} in rindexer.yaml",
                    args.network, args.contract_name
                );
                print_error_message(&error_message);
                Err(error_message.into())
            }
        }
        None => {
            let error_message =
                format!("Contract {} not found in rindexer.yaml", args.contract_name);
            print_error_message(&error_message);
            Err(error_message.into())
        }
    }
}

fn forge_compile_contract(
    compile_in: &Path,
    network: &Network,
    contract_name: &str,
) -> Result<(), Box<dyn Error>> {
    print_success_message(&format!(
        "Compiling contract {} on network {}...",
        contract_name, network.name
    ));
    let output = Command::new("forge").arg("build").current_dir(compile_in).output()?;

    if output.status.success() {
        Ok(())
    } else {
        print_error_message(&format!(
            "Failed to compile contract: {} for network: {}",
            contract_name, network.name
        ));
        print_error_message(&format!("Error: {}", String::from_utf8_lossy(&output.stderr)));
        Err("Failed to compile contract".into())
    }
}

fn get_phantom_network_name(args: &PhantomBaseArgs) -> String {
    format!("phantom_{}_{}", args.network, args.contract_name)
}

fn handle_phantom_compile(
    project_path: &Path,
    args: &PhantomBaseArgs,
) -> Result<(), Box<dyn Error>> {
    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let manifest = read_manifest(&rindexer_yaml_path).inspect_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e))
    })?;

    if manifest.phantom.is_none() {
        let error_message =
            "phantom not setup in rindexer.yaml. Please run `rindexer phantom init` first.";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    if !project_path.join("phantom").exists() {
        let error_message =
            "phantom folder not found in the project. Please run `rindexer phantom init` first.";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    let network_path = project_path.join("phantom").join(&args.network);
    if !network_path.exists() {
        let error_message = format!("phantom network {} folder not found in the project. Please run `rindexer phantom clone` first.", args.network);
        print_error_message(&error_message);
        return Err(error_message.into());
    }

    let compile_in = network_path.join(args.contract_name.as_str());
    if !compile_in.exists() {
        let error_message = format!("phantom contract {} folder not found in the project. Please run `rindexer phantom clone` first.", args.contract_name);
        print_error_message(&error_message);
        return Err(error_message.into());
    }

    let contract = manifest.contracts.iter().find(|c| c.name == args.contract_name);
    match contract {
        Some(contract) => {
            let name = get_phantom_network_name(args);
            let network =
                manifest.networks.iter().find(|n| n.name == args.network || n.name == name);
            if network.is_none() {
                let error_message = format!("Network {} not found in rindexer.yaml", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            if network.unwrap().chain_id != 1 {
                let error_message = format!("Network {} is not supported", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            let contract_network =
                contract.details.iter().find(|c| c.network == args.network || c.network == name);
            if contract_network.is_some() {
                forge_compile_contract(&compile_in, network.unwrap(), &args.contract_name)
                    .map_err(|e| format!("Failed to compile contract: {}", e))?;

                print_success_message(format!("\ncompiled contract {} for network {} successful.\nYou can use `rindexer phantom deploy --contract-name {} --network {}` to deploy the phantom contract and start indexing your custom events.", args.contract_name, args.network, args.contract_name, args.network).as_str());
                Ok(())
            } else {
                let error_message = format!(
                    "Network {} not found in contract {} in rindexer.yaml",
                    args.network, args.contract_name
                );
                print_error_message(&error_message);
                Err(error_message.into())
            }
        }
        None => {
            let error_message =
                format!("Contract {} not found in rindexer.yaml", args.contract_name);
            print_error_message(&error_message);
            Err(error_message.into())
        }
    }
}

async fn handle_phantom_deploy(
    project_path: &Path,
    args: &PhantomBaseArgs,
) -> Result<(), Box<dyn Error>> {
    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let mut manifest = read_manifest_raw(&rindexer_yaml_path).inspect_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e))
    })?;

    if manifest.phantom.is_none() {
        let error_message =
            "phantom not setup in rindexer.yaml. Please run `rindexer phantom init` first.";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    if !project_path.join("phantom").exists() {
        let error_message =
            "phantom folder not found in the project. Please run `rindexer phantom init` first.";
        print_error_message(error_message);
        return Err(error_message.into());
    }

    let network_path = project_path.join("phantom").join(&args.network);
    if !network_path.exists() {
        let error_message = format!("phantom network {} folder not found in the project. Please run `rindexer phantom clone` first.", args.network);
        print_error_message(&error_message);
        return Err(error_message.into());
    }

    let deploy_in = network_path.join(args.contract_name.as_str());
    if !deploy_in.exists() {
        let error_message = format!("phantom contract {} folder not found in the project. Please run `rindexer phantom clone` first.", args.contract_name);
        print_error_message(&error_message);
        return Err(error_message.into());
    }

    let contract = manifest.contracts.iter_mut().find(|c| c.name == args.contract_name);
    match contract {
        Some(contract) => {
            let name = get_phantom_network_name(args);
            let network =
                manifest.networks.iter().find(|n| n.name == args.network || n.name == name);
            if network.is_none() {
                let error_message = format!("Network {} not found in rindexer.yaml", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            if network.unwrap().chain_id != 1 {
                let error_message = format!("Network {} is not supported", args.network);
                print_error_message(&error_message);
                return Err(error_message.into());
            }

            let contract_network = contract
                .details
                .iter_mut()
                .find(|c| c.network == args.network || c.network == name);
            if contract_network.is_some() {
                let clone_meta = read_contract_clone_metadata(&deploy_in)?;

                let phantom = manifest.phantom.as_ref().expect("Failed to get phantom");
                let rpc_url = if phantom.dyrpc_enabled() {
                    // only compile here as shadow has to do its own compiling to deploy
                    forge_compile_contract(&deploy_in, network.unwrap(), &args.contract_name)
                        .map_err(|e| format!("Failed to compile contract: {}", e))?;

                    deploy_dyrpc_contract(
                        &env::var(RINDEXER_PHANTOM_API_ENV_KEY)
                            .expect("Failed to get phantom api key"),
                        &clone_meta,
                        &read_compiled_contract(&deploy_in, &clone_meta)?,
                    )
                    .await
                    .map_err(|e| format!("Failed to deploy contract: {}", e))?
                } else {
                    println!("deploying shadow contracts, this may take a while....");
                    deploy_shadow_contract(
                        &env::var(RINDEXER_PHANTOM_API_ENV_KEY)
                            .expect("Failed to get phantom api key"),
                        &deploy_in,
                        &clone_meta,
                        phantom.shadow.as_ref().expect("Failed to get phantom shadow"),
                    )
                    .await
                    .map_err(|e| format!("Failed to deploy contract: {}", e))?
                };

                let network_index = manifest.networks.iter().position(|net| net.name == name);

                if let Some(index) = network_index {
                    let net = &mut manifest.networks[index];
                    net.rpc = rpc_url.to_string();
                } else {
                    manifest.networks.push(Network {
                        name: name.to_string(),
                        chain_id: network.unwrap().chain_id,
                        rpc: rpc_url.to_string(),
                        compute_units_per_second: None,
                        max_block_range: if phantom.dyrpc_enabled() {
                            Some(U64::from(20_000))
                        } else {
                            Some(U64::from(2_000))
                        },
                        disable_logs_bloom_checks: None,
                    });
                }

                let compiled_contract = read_compiled_contract(&deploy_in, &clone_meta)?;
                let abi_path = project_path.join("abis").join(format!("{}.abi.json", name));
                write_file(
                    &abi_path,
                    serde_json::to_string_pretty(&compiled_contract.abi).unwrap().as_str(),
                )?;

                contract.abi = StringOrArray::Single(format!("./abis/{}.abi.json", name));
                contract_network.unwrap().network = name;

                write_manifest(&manifest, &rindexer_yaml_path)?;

                print_success_message(format!("\ndeployed contract {} for network {} successful.\nYou can use `rindexer start all` to start indexing the phantom contract", args.contract_name, args.network).as_str());
                Ok(())
            } else {
                let error_message = format!(
                    "Network {} not found in contract {} in rindexer.yaml",
                    args.network, args.contract_name
                );
                print_error_message(&error_message);
                Err(error_message.into())
            }
        }
        None => {
            let error_message =
                format!("Contract {} not found in rindexer.yaml", args.contract_name);
            print_error_message(&error_message);
            Err(error_message.into())
        }
    }
}
