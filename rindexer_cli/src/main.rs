use clap::{Parser, Subcommand};
use colored::Colorize;
use regex::Regex;
use rindexer_core::manifest::yaml::{read_manifest, write_manifest, Manifest, Network};
use rindexer_core::provider::get_chain_id;
use std::io::Write;
use std::path::PathBuf;
use std::{env, fs, io};

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
    name: String,

    #[clap(short, long)]
    network: Option<String>,
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

fn handle_init_command(rindexer_yaml_path: &PathBuf, details: &InitDetails) {
    if rindexer_yaml_exists() {
        print_error_message("rindexer.yaml already exists in the current directory.");
    }

    println!("{}", "Initializing new rindexer project...".green());

    let project_name = prompt_for_input("Project Name", None, &details.name);

    let project_description = prompt_for_optional_input::<String>("Project description", None);
    let repository = prompt_for_optional_input::<String>("Repository", None);

    let manifest = Manifest {
        name: project_name,
        description: project_description,
        repository,
        networks: vec![],
        indexers: vec![],
        global: None,
    };

    write_rindexer_yaml(&manifest, rindexer_yaml_path);
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
        }
        Err(_) => {
            print_error_message("Failed to fetch chain ID from the provided RPC URL.");
        }
    }
}

#[tokio::main]
async fn main() {
    let cli = CLI::parse();

    let path = env::current_dir().unwrap();
    let rindexer_yaml_path = path.join(YAML_NAME);

    match &cli.command {
        Commands::Init { details } => handle_init_command(&rindexer_yaml_path, details),
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
