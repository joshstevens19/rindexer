use clap::{Parser, Subcommand};
use colored::Colorize;
use regex::Regex;
use rindexer_core::generator::build::generate_rindexer_code;
use rindexer_core::manifest::yaml::{
    read_manifest, write_manifest, Databases, Global, Manifest,
    Network, PostgresClient,
};
use rindexer_core::provider::get_chain_id;
use rindexer_core::write_file;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;
use std::{fs, io};

/// Main structure for the CLI application
#[derive(Parser, Debug)]
#[clap(name = "rindexer", about, version, author = "Your Name")]
struct CLI {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
struct NetworkDetails {
    /// Name of the network
    #[clap(short, long)]
    name: Option<String>,

    /// RPC URL of the network
    #[clap(short, long)]
    rpc_url: Option<String>,

    /// Maximum block range (optional)
    #[clap(short = 'm', long)]
    max_block_range: Option<u64>,

    /// Maximum concurrency (optional)
    #[clap(short = 'c', long)]
    max_concurrency: Option<u32>,
}

/// Subcategories for the `ls` command
#[derive(Subcommand, Debug)]
enum ListCategory {
    /// List all indexers
    Indexers,
    /// List all networks
    Networks,
    /// List global settings
    Global,
}

/// Indexer details for adding an indexer
#[derive(Parser, Debug)]
struct IndexerDetails {
    #[clap(long)]
    name: Option<String>,

    #[clap(long)]
    network: Option<String>,

    #[clap(long)]
    contract_address: Option<String>,

    #[clap(long)]
    contract_name: Option<String>,

    #[clap(long)]
    start_block: Option<u64>,

    #[clap(long)]
    end_block: Option<u64>,

    #[clap(long)]
    abi_location: Option<String>,
}

#[derive(Parser, Debug)]
struct InitDetails {
    /// Name of the network
    #[clap(short, long)]
    name: Option<String>,

    /// Name of the network
    #[clap(short, long)]
    project_description: Option<String>,

    /// RPC URL of the network
    #[clap(short, long)]
    repository: Option<String>,

    #[clap(short, long)]
    database: Option<bool>,
}

/// Define the subcommands for the CLI
#[derive(Subcommand, Debug)]
enum Commands {
    /// Lists all indexers
    Ls {
        #[clap(subcommand)]
        category: ListCategory,
    },
    /// Initializes a new project
    Init {
        #[clap(flatten)]
        details: InitDetails,
    },
    #[clap(name = "generate")]
    /// Generate the project from the rindexer.yaml
    Generate,
    /// Adds a new network
    #[clap(name = "network-add")]
    AddNetwork {
        #[clap(flatten)]
        details: NetworkDetails,
    },
    #[clap(name = "network-remove")]
    /// Removes an existing network
    RemoveNetwork {
        #[clap(name = "network")]
        network_name: String,
    },
    /// Adds a new indexer
    AddIndexer {
        #[clap(flatten)]
        details: IndexerDetails,
    },
    /// Removes an existing indexer
    RemoveIndexer {
        #[clap(name = "indexer_name")]
        indexer_name: String,
    },
    /// Adds a contract to an indexer
    AddContract {
        #[clap(name = "indexer_name")]
        indexer_name: String,
    },
    /// Removes a contract from an indexer
    RemoveContract {
        #[clap(name = "indexer_name")]
        indexer_name: String,
        #[clap(name = "contract_name")]
        contract_name: String,
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

fn validate_rindexer_yaml_does_not_exist() {
    if rindexer_yaml_does_not_exist() {
        print_error_message("rindexer.yaml does not exist in the current directory. Please use rindexer init to create a new project.");
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

    let cargo = format!(
        r#"
            [package]
            name = "{project_name}"
            version = "0.1.0"
            edition = "2021"

            [dependencies]
            rindexer_core = {{ path = "../rindexer_core" }}
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
            mod rindexer;

            #[tokio::main]
            async fn main() {{
                println!("Hello, world!");
            }}
        "#;

    write_file(
        path.join("src").join("main.rs").to_str().unwrap(),
        main_code,
    )
    .unwrap();

    let rindexer_path = path.join("src").join("rindexer");
    generate_rindexer_code(rindexer_yaml_path, rindexer_path.to_str().unwrap()).unwrap();
}

fn handle_init_command(details: &InitDetails) {
    print_success_message("Initializing new rindexer project...");

    let project_name = prompt_for_input("Project Name", None, &details.name);
    let path = PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo_cli").unwrap();
    if path.exists() {
        print_error_message("Directory already exists. Please choose a different project name.");
        return;
    }
    let project_description = prompt_for_optional_input::<String>("Project description", None);
    let repository = prompt_for_optional_input::<String>("Repository", None);
    let database = prompt_for_input("Enable Postgres? (yes/no)", None, &None);

    let manifest = Manifest {
        name: project_name.clone(),
        description: project_description,
        repository,
        networks: vec![Network {
            name: "INSERT HERE".to_string(),
            chain_id: 404,
            url: "INSERT HERE".to_string(),
            max_block_range: None,
            max_concurrency: None,
        }],
        // indexers: vec![Indexer {
        //     name: "INSERT HERE".to_string(),
        //     contracts: vec![Contract {
        //         name: "INSERT HERE".to_string(),
        //         details: vec![ContractDetails {
        //             network: "INSERT HERE".to_string(),
        //             address: "INSERT HERE".to_string(),
        //             start_block: None,
        //             end_block: None,
        //             polling_every: None,
        //         }],
        //         abi: "INSERT HERE".to_string(),
        //     }],
        // }],
        indexers: vec![],
        global: if database == "yes" {
            Some(Global {
                contracts: None,
                databases: Some(Databases {
                    postgres: Some(PostgresClient {
                        name: "${DATABASE_NAME}".to_string(),
                        user: "${DATABASE_USER}".to_string(),
                        password: "${DATABASE_PASSWORD}".to_string(),
                        host: "${DATABASE_HOST}".to_string(),
                        port: "${DATABASE_PORT}".to_string(),
                    }),
                }),
            })
        } else {
            None
        },
    };

    let rindexer_yaml_path = path.join(YAML_NAME);
    let rindexer_abis_folder = path.join("ABIs");

    if let Err(err) = fs::create_dir_all(path) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    if let Err(err) = fs::create_dir_all(rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return;
    }

    write_rindexer_yaml(&manifest, &rindexer_yaml_path);

    print_success_message(
        "Project initialized successfully. Add a network next - rindexer add-network",
    );
}

fn render_network(network: &Network, include_end_space: bool) {
    println!("Network Name: {}", network.name);
    println!("Chain Id: {}", network.chain_id);
    println!("RPC URL: {}", network.url);
    println!("Max Block Range: {}", network.max_block_range.unwrap_or(0));
    println!("Max Concurrency: {}", network.max_concurrency.unwrap_or(0));
    if include_end_space {
        println!(" ");
    }
}

fn handle_ls_networks_command(rindexer_yaml_path: &PathBuf) {
    let manifest = read_rindexer_yaml(rindexer_yaml_path);

    println!("All Networks:");
    println!(" ");
    for network in manifest.networks {
        render_network(&network, true);
    }
}

async fn handle_add_network_command(rindexer_yaml_path: &PathBuf, details: &NetworkDetails) {
    validate_rindexer_yaml_does_not_exist();

    // TODO validate that network name does not already exist
    let network_name = prompt_for_input("Network name", None, &details.name);
    let rpc_url = prompt_for_input("RPC URL", Some(VALID_URL), &details.rpc_url);

    let chain_id = get_chain_id(rpc_url.as_str()).await;
    match chain_id {
        Ok(chain_id) => {
            let max_block_range = prompt_for_optional_input::<u64>("Max block range", None);
            let max_concurrency = prompt_for_optional_input::<u32>("Max concurrency:", None);

            let mut manifest = read_rindexer_yaml(rindexer_yaml_path);

            manifest.networks.push(Network {
                name: network_name,
                chain_id: chain_id
                    .to_string()
                    .parse()
                    .expect("Failed to parse chain ID"),
                url: rpc_url,
                max_block_range,
                max_concurrency,
            });

            write_rindexer_yaml(&manifest, rindexer_yaml_path);
            print_success_message("Network added successfully");
        }
        Err(_) => {
            print_error_message("Failed to fetch chain ID from the provided RPC URL.");
        }
    }
}

// async fn handle_add_new_index(rindexer_yaml_path: &PathBuf, details: IndexerDetails) {
//     validate_rindexer_yaml_does_not_exist();
//
//     let indexer_name = prompt_for_input("Indexer name", None, &details.name);
//     let network_name = prompt_for_input("Network name", None, &details.network);
// }

#[tokio::main]
async fn main() {
    let cli = CLI::parse();

    let path = PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo_cli").unwrap();
    let rindexer_yaml_path = path.join(YAML_NAME);

    match &cli.command {
        Commands::Init { details } => handle_init_command(details),
        Commands::Generate => generate_rindexer_rust_project(path, &rindexer_yaml_path),
        Commands::Ls { category } => match category {
            ListCategory::Indexers => println!("Listing indexers..."),
            ListCategory::Networks => handle_ls_networks_command(&rindexer_yaml_path),
            ListCategory::Global => println!("Listing global settings..."),
        },
        Commands::AddNetwork { details } => {
            handle_add_network_command(&rindexer_yaml_path, details).await
        }
        Commands::RemoveNetwork { network_name } => println!("Removing network: {}", network_name),
        Commands::AddIndexer { details } => println!("Adding indexer"),
        Commands::RemoveIndexer { indexer_name } => println!("Removing indexer: {}", indexer_name),
        Commands::AddContract { indexer_name } => {
            println!("Adding contract to indexer: {}", indexer_name)
        }
        Commands::RemoveContract {
            indexer_name,
            contract_name,
        } => println!(
            "Removing contract: {} from indexer: {}",
            contract_name, indexer_name
        ),
    }
}

fn prompt_for_input(
    field_name: &str,
    pattern: Option<&str>,
    current_value: &Option<String>,
) -> String {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    match current_value {
        Some(value) => value.clone(),
        None => loop {
            print!("{} {}: ", "Please enter the".green(), field_name.yellow());
            io::stdout().flush().unwrap(); // Ensure the prompt is displayed before blocking on input

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

fn prompt_for_optional_input<T: std::str::FromStr>(
    prompt: &str,
    pattern: Option<&str>,
) -> Option<T> {
    let regex = pattern.map(|p| Regex::new(p).unwrap());
    loop {
        print!("{} (skip by pressing Enter): ", prompt.blue());
        io::stdout().flush().unwrap(); // Ensure the prompt is displayed before blocking on input

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
