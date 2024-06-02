use clap::{Parser, Subcommand};
use colored::Colorize;
use ethers::types::{Chain, U64};
use ethers_etherscan::Client;
use regex::Regex;
use rindexer_core::generator::build::{
    generate, generate_rindexer_handlers, generate_rindexer_typings,
};
use rindexer_core::manifest::yaml::{
    read_manifest, write_manifest, Contract, ContractDetails, CsvDetails, Global, Indexer,
    Manifest, Network, PostgresConnectionDetails, Storage,
};
use rindexer_core::{write_file, PostgresClient};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::{fs, io};

#[allow(clippy::upper_case_acronyms)]
#[derive(Parser, Debug)]
#[clap(name = "rindexer", about, version, author = "Your Name")]
struct CLI {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
struct NewDetails {
    #[clap(short, long)]
    name: Option<String>,

    #[clap(short, long)]
    project_description: Option<String>,

    #[clap(short, long)]
    repository: Option<String>,

    #[clap(short, long)]
    database: Option<bool>,
}

#[derive(Subcommand, Debug)]
enum CodegenSubcommands {
    Typings,
    Indexer,
    All,
}

#[derive(Subcommand, Debug)]
enum StartSubcommands {
    Indexer,
    Graphql {
        #[clap(short, long)]
        port: Option<String>,
    },
    All {
        #[clap(short, long)]
        port: Option<String>,
    },
}

/// Define the subcommands for the CLI
#[derive(Subcommand, Debug)]
enum Commands {
    /// Initializes a new project
    New,
    #[clap(name = "dev")]
    Dev,
    #[clap(name = "start")]
    Start {
        #[clap(subcommand)]
        subcommand: StartSubcommands,
    },
    #[clap(name = "download_abi")]
    DownloadAbi,
    #[clap(name = "codegen")]
    Codegen {
        #[clap(subcommand)]
        subcommand: CodegenSubcommands,
    },
}

const VALID_URL: &str = r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})(:[0-9]+)?(\/[\w \.-]*)*\/?(\\?[\w=&.+-]*)?(#[\w.-]*)?$";

const YAML_NAME: &str = "rindexer.yaml";

fn print_error_message(error_message: &str) {
    println!("{}", error_message.red());
}

fn print_success_message(success_message: &str) {
    println!("{}", success_message.green());
}

fn rindexer_yaml_exists() -> bool {
    fs::metadata(YAML_NAME).is_ok()
}

fn rindexer_yaml_does_not_exist() -> bool {
    !rindexer_yaml_exists()
}

fn validate_rindexer_yaml_exist() {
    if rindexer_yaml_does_not_exist() {
        print_error_message("rindexer.yaml does not exist in the current directory. Please use rindexer new to create a new project.");
        std::process::exit(1);
    }
}

fn read_rindexer_yaml(rindexer_yaml_path: &PathBuf) -> Manifest {
    read_manifest(rindexer_yaml_path).unwrap()
}

fn write_rindexer_yaml(manifest: &Manifest, rindexer_yaml_path: &PathBuf) {
    write_manifest(manifest, rindexer_yaml_path).unwrap();
}

fn generate_rindexer_rust_project(path: PathBuf, rindexer_yaml_path: &PathBuf) {
    let manifest = read_rindexer_yaml(rindexer_yaml_path);

    if let Err(err) = fs::create_dir_all(path.join("abis")) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    // TODO! max rindexer_core to github
    let cargo = format!(
        r#"
[package]
name = "{project_name}"
version = "0.1.0"
edition = "2021"

[dependencies]
rindexer_core = {{ path = "../../rindexer_core" }}
tokio = {{ version = "1", features = ["full"] }}
ethers = {{ version = "2.0", features = ["rustls", "openssl"] }}
serde = {{ version = "1.0.194", features = ["derive"] }}
"#,
        project_name = manifest.name,
    );

    write_file(path.join("Cargo.toml").to_str().unwrap(), &cargo).unwrap();

    if let Err(err) = fs::create_dir_all(path.join("src")) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    let main_code = r#"
            use std::env;
            use std::path::PathBuf;
            use std::str::FromStr;

            use self::rindexer::indexers::all_handlers::register_all_handlers;
            use rindexer_core::{
                start_rindexer, GraphQLServerDetails, GraphQLServerSettings, IndexingDetails, StartDetails,
            };

            mod rindexer;

            #[tokio::main]
            async fn main() {
                let args: Vec<String> = env::args().collect();

                let mut enable_graphql = false;
                let mut enable_indexer = false;
                
                let mut port: Option<usize> = None;

                for arg in args.iter() {
                    match arg.as_str() {
                        "--graphql" => enable_graphql = true,
                        "--indexer" => enable_indexer = true,
                        _ if arg.starts_with("--port=") || arg.starts_with("--p") => {
                            if let Some(value) = arg.split('=').nth(1) {
                                let overridden_port = value.parse::<usize>();
                                match overridden_port {
                                    Ok(overridden_port) => port = Some(overridden_port),
                                    Err(_) => {
                                        println!("Invalid port number");
                                        return;
                                    }
                                }
                            }
                        },
                        _ => {
                            // default run both
                            enable_graphql = true;
                            enable_indexer = true;
                        }
                    }
                }

                let _ = start_rindexer(StartDetails {
                    manifest_path: env::current_dir().unwrap().join("rindexer.yaml"),
                    indexing_details: if enable_indexer {
                        Some(IndexingDetails {
                            registry: register_all_handlers().await,
                            settings: Default::default(),
                        })
                    } else {
                        None
                    },
                    graphql_server: if enable_graphql {
                        Some(GraphQLServerDetails {
                            settings: if port.is_some() {
                                GraphQLServerSettings::port(port.unwrap())
                            } else {
                                Default::default()
                            },
                        })
                    } else {
                        None
                    },
                })
                .await;
            }
          "#;

    write_file(
        path.join("src").join("main.rs").to_str().unwrap(),
        main_code,
    )
    .unwrap();

    generate(rindexer_yaml_path).unwrap();
}

fn handle_new_command(project_path: PathBuf) {
    print_success_message("Initializing new rindexer project...");

    let project_name = prompt_for_input("Project Name", None, None);
    if project_path.exists() {
        print_error_message("Directory already exists. Please choose a different project name.");
        return;
    }
    let project_description = prompt_for_optional_input::<String>("Project Description", None);
    let repository = prompt_for_optional_input::<String>("Repository", None);
    let storage_choice = prompt_for_input_list(
        "What Storages To Enable?",
        &["postgres", "csv", "both", "none"],
        None,
    );
    let mut postgres_docker_enable = false;
    if storage_choice == "postgres" || storage_choice == "both" {
        let postgres_docker =
            prompt_for_input_list("Postgres Docker Support Out The Box?", &["yes", "no"], None);
        postgres_docker_enable = postgres_docker == "yes";
    }

    let postgres_enabled = storage_choice == "postgres" || storage_choice == "both";
    let csv_enabled = storage_choice == "csv" || storage_choice == "both";

    let rindexer_yaml_path = project_path.join(YAML_NAME);
    let rindexer_abis_folder = project_path.join("abis");

    // Create the project directory
    if let Err(err) = fs::create_dir_all(&project_path) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    // Create the ABIs directory
    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    let abi_example_path = write_example_abi(&rindexer_abis_folder);

    let manifest = Manifest {
        name: project_name.clone(),
        description: project_description,
        repository,
        networks: vec![Network {
            name: "ethereum".to_string(),
            chain_id: 1,
            url: "https://eth.rpc.blxrbdn.com".to_string(),
            max_block_range: None,
            max_concurrency: None,
        }],
        indexers: vec![Indexer {
            name: "MyFirstIndexerExample".to_string(),
            contracts: vec![Contract {
                name: "RocketPoolETH".to_string(),
                details: vec![ContractDetails::new_with_address(
                    "ethereum".to_string(),
                    "0xae78736cd615f374d3085123a210448e74fc6393".to_string(),
                    Some(U64::from(18900000)),
                    Some(U64::from(19000000)),
                    None,
                )],
                abi: abi_example_path.to_str().unwrap().to_string(),
                include_events: Some(vec!["Transfer".to_string()]),
                generate_csv: csv_enabled,
                reorg_safe_distance: false,
            }],
        }],
        global: Global { contracts: None },
        storage: Storage {
            postgres: if postgres_enabled {
                Some(PostgresConnectionDetails {
                    name: "${DATABASE_NAME}".to_string(),
                    user: "${DATABASE_USER}".to_string(),
                    password: "${DATABASE_PASSWORD}".to_string(),
                    host: "${DATABASE_HOST}".to_string(),
                    port: "${DATABASE_PORT}".to_string(),
                })
            } else {
                None
            },
            csv: if csv_enabled {
                Some(CsvDetails {
                    path: "./generated_csv".to_string(),
                })
            } else {
                None
            },
        },
    };

    // Write the rindexer.yaml file
    write_rindexer_yaml(&manifest, &rindexer_yaml_path);

    // Write .env if required
    if postgres_enabled {
        if postgres_docker_enable {
            let env = r#"
DATABASE_NAME=postgres
DATABASE_USER=rindexer_user
DATABASE_PASSWORD=U3uaAFmEbv9dnxjKOo9SbUFwc9wMU5ADBHW+HUT/7+DpQaDeUYV/
DATABASE_HOST=localhost
DATABASE_PORT=5440
"#;

            write_file(project_path.join(".env").to_str().unwrap(), env).unwrap();

            write_docker_compose(&project_path);
        } else {
            let env = r#"
DATABASE_NAME=INSERT_HERE
DATABASE_USER=INSERT_HERE
DATABASE_PASSWORD=INSERT_HERE
DATABASE_HOST=INSERT_HERE
DATABASE_PORT=INSERT_HERE
"#;

            write_file(project_path.join(".env").to_str().unwrap(), env).unwrap();
        }
    }

    generate_rindexer_rust_project(project_path, &rindexer_yaml_path);

    print_success_message(
        "rindexer project created.\n - run rindexer codegen both to generate the code\n - run rindexer dev to start rindexer\n - run rindexer download-abi to download new ABIs",
    );
}

async fn handle_download_abi_command(project_path: PathBuf) {
    validate_rindexer_yaml_exist();

    let rindexer_abis_folder = project_path.join("abis");

    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    let network = prompt_for_input_list(
        "Enter Network",
        &["ethereum", "polygon", "base", "bsc"],
        None,
    );
    let contract_address = prompt_for_input("Enter Contract Address", None, None);

    let client = Client::builder()
        .chain(Chain::Mainnet)
        .unwrap()
        .build()
        .unwrap();

    let address = contract_address.parse().unwrap();

    loop {
        let metadata = client.contract_source_code(address).await.unwrap();

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

        let abi_path = rindexer_abis_folder.join(format!("{}.abi.json", item.contract_name));
        write_file(abi_path.to_str().unwrap(), &item.abi).unwrap();
        print_success_message(&format!("Downloaded ABI for: {}", item.contract_name));

        break;
    }
}

fn handle_codegen_command(project_path: PathBuf, subcommand: &CodegenSubcommands) {
    validate_rindexer_yaml_exist();

    let rindexer_yaml_path = project_path.join(YAML_NAME);

    match subcommand {
        CodegenSubcommands::Typings => {
            generate_rindexer_typings(&rindexer_yaml_path).unwrap();
            print_success_message("Generated rindexer typings.");
        }
        CodegenSubcommands::Indexer => {
            generate_rindexer_handlers(&rindexer_yaml_path).unwrap();
            print_success_message("Generated rindexer indexer handlers.");
        }
        CodegenSubcommands::All => {
            generate(&rindexer_yaml_path).unwrap();
            print_success_message("Generated rindexer typings and indexer handlers");
        }
    }
}

async fn start(project_path: PathBuf, command: &StartSubcommands) {
    validate_rindexer_yaml_exist();

    let manifest = read_rindexer_yaml(&project_path.join(YAML_NAME));
    if manifest.storage.postgres_enabled() {
        let client = PostgresClient::new().await;
        if client.is_err() {
            print_error_message("Failed to connect to the postgres database.\nMake sure the database is running and the connection details are correct in the .env file and yaml file.\nIf you are trying to run locally do not forget to run docker-compose up before you run the indexer.");
            return;
        }
    }

    let manifest_path = project_path.join("Cargo.toml");
    let status = Command::new("cargo")
        .arg("run")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg(match command {
            StartSubcommands::Indexer => "-- --indexer".to_string(),
            StartSubcommands::Graphql { port } => match port {
                Some(port) => format!("-- --graphql --port={}", port),
                None => "-- --graphql".to_string(),
            },
            StartSubcommands::All { port } => match port {
                Some(port) => format!("-- --port={}", port),
                None => "".to_string(),
            },
        })
        .status()
        .expect("Failed to execute cargo run.");

    if !status.success() {
        panic!("cargo run failed with status: {:?}", status);
    }
}

// async fn start_indexer() {
//     validate_rindexer_yaml_exist();
//
//     // TODO! fix this
//     let path = PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo_cli").unwrap();
//     let rindexer_yaml_path = path.join(YAML_NAME);
//
//     let manifest = read_rindexer_yaml(&rindexer_yaml_path);
//
//     let client = PostgresClient::new().await.unwrap();
//
//     for indexer in &manifest.indexers {
//         let sql = create_tables_for_indexer_sql(indexer);
//         println!("{}", sql);
//         client.batch_execute(&sql).await.unwrap();
//     }
//
//     let result = start_graphql_server(&manifest.indexers, Default::default()).unwrap();
//     thread::sleep(Duration::from_secs(5000000000000000000));
// }
//
// async fn start_graphql() {
//     validate_rindexer_yaml_exist();
//
//     // TODO! fix this
//     let path = PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo_cli").unwrap();
//     let rindexer_yaml_path = path.join(YAML_NAME);
//
//     let manifest = read_rindexer_yaml(&rindexer_yaml_path);
//
//     let client = PostgresClient::new().await.unwrap();
//
//     for indexer in &manifest.indexers {
//         let sql = create_tables_for_indexer_sql(indexer);
//         println!("{}", sql);
//         client.batch_execute(&sql).await.unwrap();
//     }
//
//     let result = start_graphql_server(&manifest.indexers, Default::default()).unwrap();
//     thread::sleep(Duration::from_secs(5000000000000000000));
// }

#[tokio::main]
async fn main() {
    let cli = CLI::parse();

    // TODO: sort this to inherit the path from execution
    let path =
        PathBuf::from_str("/Users/joshstevens/code/rindexer/examples/rindexer_demo_cli").unwrap();

    match &cli.command {
        Commands::New => handle_new_command(path),
        Commands::DownloadAbi => handle_download_abi_command(path).await,
        Commands::Codegen { subcommand } => handle_codegen_command(path, subcommand),
        Commands::Start { subcommand } => start(path, subcommand).await,
        _ => panic!("Command not implemented"),
    }
}

fn prompt_for_input(
    field_name: &str,
    pattern: Option<&str>,
    current_value: Option<&str>,
) -> String {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    match current_value {
        Some(value) => value.to_string(),
        None => loop {
            print!("{} {}: ", "Please enter the".green(), field_name.yellow());
            io::stdout().flush().unwrap();

            let mut input = String::new();
            io::stdin()
                .read_line(&mut input)
                .expect("Failed to read line");
            let trimmed = input.trim();

            if let Some(ref regex) = regex {
                if regex.is_match(trimmed) {
                    return trimmed.to_string();
                } else {
                    println!(
                        "{}",
                        "Invalid input according to regex. Please try again.".red()
                    );
                }
            } else if !trimmed.is_empty() {
                return trimmed.to_string();
            } else {
                println!("{}", "Input cannot be empty. Please try again.".red());
            }
        },
    }
}

fn prompt_for_optional_input<T: FromStr>(prompt: &str, pattern: Option<&str>) -> Option<T> {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    loop {
        print!("{} (skip by pressing Enter): ", prompt.blue());
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let trimmed = input.trim();

        if trimmed.is_empty() {
            return None;
        }

        if let Some(ref regex) = regex {
            if regex.is_match(trimmed) {
                match trimmed.parse::<T>() {
                    Ok(value) => return Some(value),
                    Err(_) => println!(
                        "{}",
                        "Invalid format. Please try again or press Enter to skip.".red()
                    ),
                }
            } else {
                println!(
                    "{}",
                    "Invalid input according to regex. Please try again or press Enter to skip."
                        .red()
                );
            }
        } else {
            match trimmed.parse::<T>() {
                Ok(value) => return Some(value),
                Err(_) => println!("{}", "Invalid format. Please try again.".red()),
            }
        }
    }
}

fn prompt_for_input_list(
    field_name: &str,
    options: &[&str],
    current_value: Option<&str>,
) -> String {
    let options_str = options.join(", ");

    if let Some(value) = current_value {
        return value.to_string();
    }

    loop {
        print!(
            "{} [{}]: ",
            format!("Please enter the {}", field_name).green(),
            options_str.yellow()
        );
        io::stdout().flush().unwrap();

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let trimmed = input.trim().to_lowercase();

        if options.contains(&trimmed.as_str()) {
            return trimmed;
        } else {
            println!(
                "{}",
                format!(
                    "Invalid option. Please choose one of the following: {}",
                    options_str
                )
                .red()
            );
        }
    }
}

fn write_example_abi(rindexer_abis_folder: &Path) -> PathBuf {
    let abi = r#"[{"inputs":[{"internalType":"contract RocketStorageInterface","name":"_rocketStorageAddress","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"EtherDeposited","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"TokensBurned","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"ethAmount","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"time","type":"uint256"}],"name":"TokensMinted","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_rethAmount","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"depositExcess","outputs":[],"stateMutability":"payable","type":"function"},{"inputs":[],"name":"depositExcessCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getCollateralRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_rethAmount","type":"uint256"}],"name":"getEthValue","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getExchangeRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_ethAmount","type":"uint256"}],"name":"getRethValue","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getTotalCollateral","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_ethAmount","type":"uint256"},{"internalType":"address","name":"_to","type":"address"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"version","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"stateMutability":"payable","type":"receive"}]"#;

    let path = rindexer_abis_folder.join("RocketTokenRETH.abi.json");

    write_file(path.to_str().unwrap(), abi).unwrap();

    path
}

fn write_docker_compose(path: &Path) {
    let yml = r#"version: '3.8'
volumes:
  postgres_data:
    driver: local

services:
  postgresql:
    image: postgres:16
    shm_size: 1g
    restart: always
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - 5440:5432
    env_file:
      - ./.env
    healthcheck:
      test:
        ['CMD-SHELL', 'pg_isready -U $${DATABASE_USER} -d $${DATABASE_NAME} -q']
      interval: 5s
      timeout: 10s
      retries: 10
 "#;

    write_file(path.join("docker-compose.yml").to_str().unwrap(), yml).unwrap();
}
