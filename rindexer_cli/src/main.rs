use clap::{Parser, Subcommand};
use colored::Colorize;
use ethers::types::{Chain, U64};
use ethers_etherscan::Client;
use regex::Regex;
use rindexer_core::generator::build::{
    generate, generate_rindexer_handlers, generate_rindexer_typings, generate_rust_project,
};
use rindexer_core::generator::generate_docker_file;
use rindexer_core::manifest::yaml::{
    read_manifest, write_manifest, Contract, ContractDetails, CsvDetails, Global, Indexer,
    Manifest, Network, PostgresConnectionDetails, ProjectType, Storage, YAML_CONFIG_NAME,
};
use rindexer_core::{
    start_rindexer_no_code, write_file, GraphQLServerDetails, GraphQLServerSettings,
    PostgresClient, StartNoCodeDetails,
};
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

#[derive(Parser, Debug)]
#[clap(author = "Josh Stevens", version = "1.0", about = "Blazing fast EVM indexing tool built in rust", long_about = None)]
enum Commands {
    /// Creates a new rust rindexer project or a rindexer no-code project
    ///
    /// This command initialises a new workspace project with rindexer
    /// with everything populated to start using rindexer.
    ///
    /// Example:
    /// `rindexer new`
    New,

    /// Start various services like indexers, GraphQL APIs or both together
    #[clap(name = "start")]
    Start {
        #[clap(subcommand)]
        subcommand: StartSubcommands,
    },

    /// Downloads ABIs from etherscan to build up your rindexer.yaml mappings.
    ///
    /// This command helps in fetching ABI files necessary for indexing.
    /// It will add them to the abis folder any mappings required will need
    /// to be done in your rindexer.yaml file manually.
    ///
    /// Example:
    /// `rindexer download_abi`
    #[clap(name = "download_abi")]
    DownloadAbi,

    /// Generates rust code based on rindexer.yaml - if you are using no-code projects
    /// you will not need to use this.
    #[clap(name = "codegen")]
    Codegen {
        #[clap(subcommand)]
        subcommand: CodegenSubcommands,
    },
}

#[derive(Subcommand, Debug)]
enum StartSubcommands {
    /// Starts the indexing service based on the rindexer.yaml file.
    ///
    /// Starts an indexer based on the rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer start indexer`
    Indexer,

    /// Starts the GraphQL server based on the rindexer.yaml file.
    ///
    /// Optionally specify a port to override the default.
    ///
    /// Example:
    /// `rindexer start graphql --port 4000`
    Graphql {
        #[clap(short, long, help = "Specify the port number for the GraphQL server")]
        port: Option<String>,
    },

    /// Starts the indexers and the GraphQL together based on the rindexer.yaml file.
    ///
    /// You can specify a port which will be used by all services that require one.
    ///
    /// Example:
    /// `rindexer start all --port 3000`
    All {
        #[clap(short, long, help = "Specify the port number for all services")]
        port: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum CodegenSubcommands {
    /// Generates the rindexer rust typings based on the rindexer.yaml file.
    ///
    /// This should not be edited manually and always generated.
    ///
    /// Example:
    /// `rindexer codegen typings`
    Typings,

    /// Generates the rindexer rust indexers handlers based on the rindexer.yaml file.
    ///
    /// You can use these as the foundations to build your advanced indexers.
    ///
    /// Example:
    /// `rindexer codegen indexer`
    Indexer,

    /// Generates both typings and indexers handlers based on the rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer codegen all`
    All,
}

const VALID_URL: &str = r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})(:[0-9]+)?(\/[\w \.-]*)*\/?(\\?[\w=&.+-]*)?(#[\w.-]*)?$";

fn print_error_message(error_message: &str) {
    println!("{}", error_message.red());
}

fn print_success_message(success_message: &str) {
    println!("{}", success_message.green());
}

fn rindexer_yaml_exists() -> bool {
    fs::metadata(YAML_CONFIG_NAME).is_ok()
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

fn generate_rindexer_rust_project(project_path: &PathBuf) {
    let generated = generate_rust_project(project_path);
    match generated {
        Ok(_) => {
            print_success_message("Successfully generated rindexer rust project.");
        }
        Err(err) => {
            print_error_message(&format!(
                "Failed to generate rindexer rust project: {}",
                err
            ));
        }
    }
}

fn handle_new_command(project_path: PathBuf) {
    print_success_message("Initializing new rindexer project...");

    let rindexer_type = prompt_for_input_list(
        "indexer type you wish to create:\n- No code: best choice when starting, no extra code required\n- Project: customise advanced indexer as you see by writing rust code\n",
        &["no-code".to_string(),"project".to_string()],
        None,
    );

    let project_name = prompt_for_input("Project Name", None, None, None);
    if project_path.exists() {
        print_error_message("Directory already exists. Please choose a different project name.");
        return;
    }
    let project_description = prompt_for_optional_input::<String>("Project Description", None);
    let repository = prompt_for_optional_input::<String>("Repository", None);
    let storage_choice = prompt_for_input_list(
        "What Storages To Enable?",
        &[
            "postgres".to_string(),
            "csv".to_string(),
            "both".to_string(),
            "none".to_string(),
        ],
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
        return;
    }

    // Create the ABIs directory
    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    let abi_example_path = write_example_abi(&rindexer_abis_folder);

    let project_type = if rindexer_type == "no-code" {
        ProjectType::NoCode
    } else {
        ProjectType::Rust
    };

    let manifest = Manifest {
        name: project_name.clone(),
        description: project_description,
        repository,
        project_type: project_type.clone(),
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
                    database_url: "${DATABASE_URL}".to_string(),
                    disable_create_tables: false,
                })
            } else {
                None
            },
            csv: if csv_enabled {
                Some(CsvDetails {
                    path: "./generated_csv".to_string(),
                    disable_create_headers: false,
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
            let env = r#"DATABASE_URL=postgresql://postgres:U3uaAFmEbv9dnxjKOo9SbUFwc9wMU5ADBHW+HUT/7+DpQaDeUYV/@localhost:5440/postgres"#;

            write_file(project_path.join(".env").to_str().unwrap(), env).unwrap();

            write_docker_compose(&project_path);
        } else {
            let env = r#"DATABASE_URL=INSERT_HERE"#;

            write_file(project_path.join(".env").to_str().unwrap(), env).unwrap();
        }
    }

    if project_type == ProjectType::Rust {
        generate_rindexer_rust_project(&project_path);
        print_success_message(
            "rindexer project created created with a starter manifest.\n- run rindexer codegen both to regenerate the code\n- run rindexer dev to start rindexer\n - run rindexer download-abi to download new ABIs",
        );
    } else {
        print_success_message(
            "rindexer no-code project created with a starter manifest.\n- run rindexer start to start rindexer\n- run rindexer download-abi to download new ABIs",
        );
    }
}

async fn handle_download_abi_command(project_path: PathBuf) {
    validate_rindexer_yaml_exist();

    let rindexer_abis_folder = project_path.join("abis");

    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    let network = prompt_for_input(
        "Enter Network Chain Id",
        Some(r"^\d+$"),
        Some("Invalid network chain id. Please enter a valid chain id."),
        None,
    );
    let network = U64::from_dec_str(&network)
        .map_err(|_| {
            print_error_message("Invalid network chain id. Please enter a valid chain id.");
        })
        .unwrap();

    let network = Chain::try_from(network)
        .map_err(|_| {
            print_error_message("Chain id is not supported by etherscan API.");
        })
        .unwrap();
    let contract_address = prompt_for_input("Enter Contract Address", None, None, None);

    let client = Client::builder().chain(network).unwrap().build().unwrap();

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

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let manifest = read_manifest(&rindexer_yaml_path).unwrap();
    if manifest.project_type == ProjectType::NoCode {
        print_error_message("This command is not supported for no-code projects, please migrate to a project to use this.");
        return;
    }

    match subcommand {
        CodegenSubcommands::Typings => {
            generate_rindexer_typings(manifest.clone(), &rindexer_yaml_path).unwrap();
            print_success_message("Generated rindexer typings.");
        }
        CodegenSubcommands::Indexer => {
            generate_rindexer_handlers(manifest.clone(), &rindexer_yaml_path).unwrap();
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

    let manifest = read_rindexer_yaml(&project_path.join(YAML_CONFIG_NAME));
    if manifest.storage.postgres_enabled() {
        let client = PostgresClient::new().await;
        if client.is_err() {
            print_error_message("Failed to connect to the postgres database.\nMake sure the database is running and the connection details are correct in the .env file and yaml file.\nIf you are running locally and using docker do not forget to run docker-compose up before you run the indexer.");
            return;
        }
    }

    match manifest.project_type {
        ProjectType::Rust => {
            let project_cargo_manifest_path = project_path.join("Cargo.toml");
            let status = Command::new("cargo")
                .arg("run")
                .arg("--manifest-path")
                .arg(project_cargo_manifest_path)
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
        ProjectType::NoCode => match command {
            StartSubcommands::Indexer => {
                let details = StartNoCodeDetails {
                    manifest_path: project_path.join(YAML_CONFIG_NAME),
                    indexing_settings: None,
                    graphql_server: None,
                };

                start_rindexer_no_code(details).await.unwrap();
            }
            StartSubcommands::Graphql { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: project_path.join(YAML_CONFIG_NAME),
                    indexing_settings: None,
                    graphql_server: Some(GraphQLServerDetails {
                        settings: match port {
                            Some(port) => GraphQLServerSettings::port(port.parse().unwrap()),
                            None => Default::default(),
                        },
                    }),
                };

                start_rindexer_no_code(details).await.unwrap();
            }
            StartSubcommands::All { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: project_path.join(YAML_CONFIG_NAME),
                    indexing_settings: None,
                    graphql_server: Some(GraphQLServerDetails {
                        settings: match port {
                            Some(port) => GraphQLServerSettings::port(port.parse().unwrap()),
                            None => Default::default(),
                        },
                    }),
                };

                let _ = start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the server: {}", e));
                });
            }
        },
    }
}

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
    }
}

fn prompt_for_input(
    field_name: &str,
    pattern: Option<&str>,
    pattern_failure_message: Option<&str>,
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
                    let message = pattern_failure_message
                        .unwrap_or("Invalid input according to regex. Please try again.");
                    println!("{}", message.red());
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
    options: &[String],
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

        if options.contains(&trimmed) {
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
    write_file(
        path.join("docker-compose.yml").to_str().unwrap(),
        generate_docker_file(),
    )
    .unwrap();
}
