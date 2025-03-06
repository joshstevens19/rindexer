use std::{
    fs,
    path::{Path, PathBuf},
};

use ethers::{
    addressbook::Address,
    prelude::{ValueOrArray, U64},
};
use rindexer::{
    generator::{build::generate_rust_project, generate_docker_file},
    manifest::{
        contract::{Contract, ContractDetails},
        core::{Manifest, ProjectType},
        network::Network,
        storage::{CsvDetails, PostgresDetails, Storage},
        yaml::{write_manifest, YAML_CONFIG_NAME},
    },
    write_file, StringOrArray, WriteFileError,
};

use crate::console::{
    print_error_message, print_success_message, prompt_for_input, prompt_for_input_list,
    prompt_for_optional_input,
};

fn generate_rindexer_rust_project(project_path: &Path) {
    let generated = generate_rust_project(project_path);
    match generated {
        Ok(_) => {
            print_success_message("Successfully generated rindexer rust project.");
        }
        Err(err) => {
            print_error_message(&format!("Failed to generate rindexer rust project: {}", err));
        }
    }
}

fn write_example_abi(rindexer_abis_folder: &Path) -> Result<PathBuf, WriteFileError> {
    let abi = r#"[{"inputs":[{"internalType":"contract RocketStorageInterface","name":"_rocketStorageAddress","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"EtherDeposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"TokensBurned","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"TokensMinted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_rethAmount","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"depositExcess","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"depositExcessCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getCollateralRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_rethAmount","type":"uint256"}],"name":"getEthValue","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getExchangeRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_ethAmount","type":"uint256"}],"name":"getRethValue","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getTotalCollateral","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_ethAmount","type":"uint256"},{"internalType":"address","name":"_to","type":"address"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]"#;

    let path = rindexer_abis_folder.join("RocketTokenRETH.abi.json");

    write_file(&path, abi)?;

    let relative_path = Path::new("./abis/RocketTokenRETH.abi.json").to_path_buf();

    Ok(relative_path)
}

fn write_docker_compose(path: &Path) -> Result<(), WriteFileError> {
    write_file(&path.join("docker-compose.yml"), generate_docker_file())
}

fn write_gitignore(path: &Path) -> Result<(), WriteFileError> {
    write_file(
        &path.join(".gitignore"),
        r#".rindexer
    generated_csv/**/*.txt
    "#,
    )
}

pub fn handle_new_command(
    project_path: PathBuf,
    project_type: ProjectType,
) -> Result<(), Box<dyn std::error::Error>> {
    print_success_message("Initializing new rindexer project...");

    let project_name = prompt_for_input(
        "Project Name",
        Some(r"^[a-zA-Z][a-zA-Z0-9]*$"),
        Some("No spaces, special characters are allowed, and the first letter cannot be a number"),
        None,
    );

    let project_path = project_path.join(&project_name);
    if project_path.exists() {
        print_error_message("Directory already exists. Please choose a different project name.");
        return Err("Directory already exists.".into());
    }
    let project_description = prompt_for_optional_input::<String>("Project Description", None);
    let repository = prompt_for_optional_input::<String>("Repository", None);
    let storage_choice = prompt_for_input_list(
        "What Storages To Enable? (graphql can only be supported if postgres is enabled)",
        &["postgres".to_string(), "csv".to_string(), "both".to_string(), "none".to_string()],
        None,
    );
    let mut postgres_docker_enable = false;
    if storage_choice == "postgres" || storage_choice == "both" {
        let postgres_docker = prompt_for_input_list(
            "Postgres Docker Support Out The Box?",
            &["yes".to_string(), "no".to_string()],
            None,
        );
        postgres_docker_enable = postgres_docker == "yes";
    }

    let postgres_enabled = storage_choice == "postgres" || storage_choice == "both";
    let csv_enabled = storage_choice == "csv" || storage_choice == "both";

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);
    let rindexer_abis_folder = project_path.join("abis");

    // Create the project directory
    if let Err(err) = fs::create_dir_all(&project_path) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return Err(err.into());
    }

    // Create the ABIs directory
    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return Err(err.into());
    }

    let abi_example_path = write_example_abi(&rindexer_abis_folder).map_err(|e| {
        print_error_message(&format!("Failed to write example ABI file: {}", e));
        e
    })?;

    // for later to avoid cloning
    let success_message = if project_type == ProjectType::Rust {
        format!("rindexer rust project created with a rETH transfer events YAML template.\n cd ./{} \n- use rindexer codegen commands to regenerate the code\n- run `rindexer start all` to start rindexer\n- run `rindexer add contract` to add new contracts to your project", &project_name)
    } else {
        format!("rindexer no-code project created with a rETH transfer events YAML template.\n cd ./{} \n- run `rindexer start all` to start rindexer\n- run `rindexer add contract` to add new contracts to your project", &project_name)
    };

    // for later to avoid cloning
    let is_rust_project = project_type == ProjectType::Rust;

    let manifest = Manifest {
        name: project_name,
        description: project_description,
        repository,
        project_type,
        networks: vec![Network {
            name: "ethereum".to_string(),
            chain_id: 1,
            rpc: "https://mainnet.gateway.tenderly.co".to_string(),
            compute_units_per_second: None,
            max_block_range: None,
            disable_logs_bloom_checks: None,
        }],
        contracts: vec![Contract {
            name: "RocketPoolETH".to_string(),
            details: vec![ContractDetails::new_with_address(
                "ethereum".to_string(),
                ValueOrArray::<Address>::Value(
                    "0xae78736cd615f374d3085123a210448e74fc6393"
                        .parse::<Address>()
                        .expect("Invalid address"),
                ),
                None,
                Some(U64::from(18900000)),
                Some(U64::from(19000000)),
            )],
            abi: StringOrArray::Single(abi_example_path.display().to_string()),
            include_events: Some(vec!["Transfer".to_string(), "Approval".to_string()]),
            index_event_in_order: None,
            dependency_events: None,
            reorg_safe_distance: None,
            generate_csv: None,
            streams: None,
            chat: None,
        }],
        phantom: None,
        global: None,
        storage: Storage {
            postgres: if postgres_enabled {
                Some(PostgresDetails {
                    enabled: true,
                    drop_each_run: None,
                    relationships: None,
                    indexes: None,
                    disable_create_tables: None,
                })
            } else {
                None
            },
            csv: if csv_enabled {
                Some(CsvDetails {
                    enabled: true,
                    path: "./generated_csv".to_string(),
                    disable_create_headers: None,
                })
            } else {
                None
            },
        },
        graphql: None,
    };

    // Write the rindexer.yaml file
    write_manifest(&manifest, &rindexer_yaml_path)?;

    // Write .env if required
    if postgres_enabled {
        if postgres_docker_enable {
            let env = r#"DATABASE_URL=postgresql://postgres:rindexer@localhost:5440/postgres
POSTGRES_PASSWORD=rindexer"#;

            write_file(&project_path.join(".env"), env).map_err(|e| {
                print_error_message(&format!("Failed to write .env file: {}", e));
                e
            })?;

            write_docker_compose(&project_path).map_err(|e| {
                print_error_message(&format!("Failed to write docker compose file: {}", e));
                e
            })?;
        } else {
            let env = r#"DATABASE_URL=postgresql://[user[:password]@][host][:port][/dbname]"#;

            write_file(&project_path.join(".env"), env).map_err(|e| {
                print_error_message(&format!("Failed to write .env file: {}", e));
                e
            })?;
        }
    }

    if is_rust_project {
        generate_rindexer_rust_project(&project_path);
    }

    write_gitignore(&project_path)?;

    print_success_message(&success_message);

    Ok(())
}
