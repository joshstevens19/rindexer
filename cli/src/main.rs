#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

use clap::{Parser, Subcommand};
use colored::Colorize;
use dotenv::{dotenv, from_path};
use ethers::types::{Chain, U64};
use ethers_etherscan::Client;
use regex::Regex;
use rindexer_core::generator::build::{
    generate_rindexer_handlers, generate_rindexer_typings, generate_rindexer_typings_and_handlers,
    generate_rust_project,
};
use rindexer_core::generator::generate_docker_file;
use rindexer_core::manifest::yaml::{
    read_manifest, write_manifest, Contract, ContractDetails, CsvDetails, Manifest, Network,
    PostgresConnectionDetails, ProjectType, Storage, YAML_CONFIG_NAME,
};
use rindexer_core::{
    drop_tables_for_indexer_sql, generate_graphql_queries, start_rindexer_no_code, write_file,
    GraphQLServerDetails, GraphQLServerSettings, GraphqlNoCodeDetails, IndexerNoCodeDetails,
    PostgresClient, StartNoCodeDetails, WriteFileError,
};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::str::FromStr;
use std::{fs, io};
use tokio::fs::remove_dir_all;

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
    /// Creates a new rindexer no-code project or rust project.
    ///
    /// no-code = Best choice when starting, no extra code required.
    /// rust = Customise advanced indexer by writing rust code.
    ///
    /// This command initialises a new workspace project with rindexer
    /// with everything populated to start using rindexer.
    ///
    /// Example:
    /// `rindexer new no-code` or `rindexer new rust`
    #[clap(name = "new")]
    New {
        #[clap(subcommand)]
        subcommand: NewSubcommands,

        /// optional - The path to create the project in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
    /// Start various services like indexers, GraphQL APIs or both together
    ///
    /// `rindexer start indexer` or `rindexer start graphql` or `rindexer start all`
    #[clap(name = "start")]
    Start {
        #[clap(subcommand)]
        subcommand: StartSubcommands,

        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },

    /// Downloads ABIs from etherscan to build up your rindexer.yaml mappings.
    ///
    /// This command helps in fetching ABI files necessary for indexing.
    /// It will add them to the abis folder any mappings required will need
    /// to be done in your rindexer.yaml file manually.
    ///
    /// Example:
    /// `rindexer download-abi`
    #[clap(name = "download-abi")]
    DownloadAbi {
        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },

    /// Generates rust code based on rindexer.yaml or graphql queries
    ///
    /// Example:
    /// `rindexer codegen typings` or `rindexer codegen handlers` or `rindexer codegen graphql --endpoint=graphql_api` or `rindexer codegen rust-all`
    #[clap(name = "codegen")]
    Codegen {
        #[clap(subcommand)]
        subcommand: CodegenSubcommands,

        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
    /// Delete data from the postgres database or csv files.
    ///
    /// This command deletes rindexer project data from the postgres database or csv files.
    ///
    /// Example:
    /// `rindexer delete`
    Delete {
        /// optional - The path to run the command in, default will be where the command is run.
        #[clap(long, short)]
        path: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum NewSubcommands {
    /// Creates a new no-code project
    ///
    /// Best choice when starting, no extra code required.
    /// Example:
    /// `rindexer new no-code`
    #[clap(name = "no-code")]
    NoCode,

    /// Creates a new rust project
    ///
    /// Customise advanced indexer by writing rust code
    /// Example:
    /// `rindexer new rust`
    #[clap(name = "rust")]
    Rust,
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
    /// This is not relevant for no-code projects.
    ///
    /// Example:
    /// `rindexer codegen typings`
    Typings,

    /// Generates the rindexer rust indexers handlers based on the rindexer.yaml file.
    ///
    /// You can use these as the foundations to build your advanced indexers.
    ///
    /// This is not relevant for no-code projects.
    ///
    /// Example:
    /// `rindexer codegen indexer`
    Indexer,

    /// Generates the GraphQL queries from a GraphQL schema
    ///
    /// You can then use this in your dApp instantly to interact with the GraphQL API
    ///
    /// Example:
    /// `rindexer codegen graphql`
    #[clap(name = "graphql")]
    GraphQL {
        #[clap(long, help = "The graphql endpoint")]
        endpoint: String,
    },

    /// Generates both typings and indexers handlers based on the rindexer.yaml file.
    ///
    /// Example:
    /// `rindexer codegen rust-all`
    All,
}

// const VALID_URL: &str = r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})(:[0-9]+)?(\/[\w \.-]*)*\/?(\\?[\w=&.+-]*)?(#[\w.-]*)?$";

fn print_error_message(error_message: &str) {
    println!("{}", error_message.red());
}

fn print_warn_message(error_message: &str) {
    println!("{}", error_message.yellow());
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

fn generate_rindexer_rust_project(project_path: &Path) {
    let generated = generate_rust_project(project_path);
    match generated {
        Ok(_) => {
            print_success_message("Successfully generated rindexer rust project.");
        }
        Err(err) => {
            println!("{:?}", err);
            print_error_message(&format!(
                "Failed to generate rindexer rust project: {}",
                err
            ));
        }
    }
}

fn handle_new_command(
    project_path: PathBuf,
    project_type: ProjectType,
) -> Result<(), Box<dyn std::error::Error>> {
    print_success_message("Initializing new rindexer project...");

    let project_name = prompt_for_input(
        "Project Name",
        Some(r"^\S+$"),
        Some("No spaces are allowed in the project name"),
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

    let manifest = Manifest {
        name: project_name.clone(),
        description: project_description,
        repository,
        project_type: project_type.clone(),
        networks: vec![Network {
            name: "ethereum".to_string(),
            chain_id: 1,
            rpc: "https://mainnet.gateway.tenderly.co".to_string(),
            compute_units_per_second: None,
        }],
        contracts: vec![Contract {
            name: "RocketPoolETH".to_string(),
            details: vec![ContractDetails::new_with_address(
                "ethereum".to_string(),
                "0xae78736cd615f374d3085123a210448e74fc6393".to_string(),
                Some(U64::from(18900000)),
                Some(U64::from(19000000)),
            )],
            abi: abi_example_path.display().to_string(),
            include_events: Some(vec!["Transfer".to_string(), "Approval".to_string()]),
            index_event_in_order: None,
            dependency_events: None,
            reorg_safe_distance: None,
            generate_csv: None,
        }],
        global: None,
        storage: Storage {
            postgres: if postgres_enabled {
                Some(PostgresConnectionDetails {
                    enabled: true,
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
                print_error_message(&format!("Failed to write docker-compose file: {}", e));
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

    if project_type == ProjectType::Rust {
        generate_rindexer_rust_project(&project_path);
        print_success_message(
            &format!("rindexer project created with a starter manifest.\n cd ./{} \n- run rindexer codegen both to regenerate the code\n- run rindexer dev to start rindexer\n - run rindexer download-abi to download new ABIs", &project_name),
        );
    } else {
        print_success_message(
            &format!("rindexer no-code project created with a starter manifest.\n cd ./{} \n- run rindexer start to start rindexer\n- run rindexer download-abi to download new ABIs", &project_name),
        );
    }

    Ok(())
}

async fn handle_download_abi_command(
    project_path: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_rindexer_yaml_exist();

    let rindexer_abis_folder = project_path.join("abis");

    if let Err(err) = fs::create_dir_all(&rindexer_abis_folder) {
        print_error_message(&format!("Failed to create directory: {}", err));
        return Err(err.into());
    }

    let network = prompt_for_input(
        "Enter Network Chain Id",
        Some(r"^\d+$"),
        Some("Invalid network chain id. Please enter a valid chain id."),
        None,
    );
    let network = U64::from_dec_str(&network).map_err(|e| {
        print_error_message("Invalid network chain id. Please enter a valid chain id.");
        e
    })?;

    let network = Chain::try_from(network).map_err(|e| {
        print_error_message("Chain id is not supported by etherscan API.");
        e
    })?;
    let contract_address = prompt_for_input("Enter Contract Address", None, None, None);

    let client = Client::builder()
        .chain(network)
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

        let abi_path = rindexer_abis_folder.join(format!("{}.abi.json", item.contract_name));
        write_file(&abi_path, &item.abi).map_err(|e| {
            print_error_message(&format!("Failed to write ABI file: {}", e));
            e
        })?;
        print_success_message(&format!(
            "Downloaded ABI for: {} in {}",
            item.contract_name,
            abi_path.display()
        ));

        break;
    }

    Ok(())
}

async fn handle_codegen_command(
    project_path: PathBuf,
    subcommand: &CodegenSubcommands,
) -> Result<(), Box<dyn std::error::Error>> {
    if let CodegenSubcommands::GraphQL { endpoint } = subcommand {
        generate_graphql_queries(endpoint, &project_path)
            .await
            .map_err(|e| {
                print_error_message(&format!("Failed to generate graphql queries: {}", e));
                e
            })?;

        print_success_message("Generated graphql queries.");

        return Ok(());
    }

    validate_rindexer_yaml_exist();

    let rindexer_yaml_path = project_path.join(YAML_CONFIG_NAME);

    let manifest = read_manifest(&rindexer_yaml_path).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e));
        e
    })?;
    if manifest.project_type == ProjectType::NoCode {
        let error = "This command is not supported for no-code projects, please migrate to a project to use this.";
        print_error_message(error);
        return Err(error.into());
    }

    match subcommand {
        CodegenSubcommands::Typings => {
            generate_rindexer_typings(manifest, &rindexer_yaml_path).map_err(|e| {
                print_error_message(&format!("Failed to generate rindexer typings: {}", e));
                e
            })?;
            print_success_message("Generated rindexer typings.");
        }
        CodegenSubcommands::Indexer => {
            generate_rindexer_handlers(manifest, &rindexer_yaml_path).map_err(|e| {
                print_error_message(&format!(
                    "Failed to generate rindexer indexer handlers: {}",
                    e
                ));
                e
            })?;
            print_success_message("Generated rindexer indexer handlers.");
        }
        CodegenSubcommands::GraphQL {
            endpoint: _endpoint,
        } => {
            unreachable!("This should not be reachable");
        }
        CodegenSubcommands::All => {
            generate_rindexer_typings_and_handlers(&rindexer_yaml_path).map_err(|e| {
                print_error_message(&format!("Failed to generate rindexer code: {}", e));
                e
            })?;
            print_success_message("Generated rindexer typings and indexer handlers");
        }
    }

    Ok(())
}

async fn start(
    project_path: PathBuf,
    command: &StartSubcommands,
) -> Result<(), Box<dyn std::error::Error>> {
    validate_rindexer_yaml_exist();

    let manifest = read_manifest(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could not read the rindexer.yaml file: {}", e));
        e
    })?;

    if manifest.storage.postgres_enabled() {
        let client = PostgresClient::new().await;
        if client.is_err() {
            let error = "Failed to connect to the postgres database.\nMake sure the database is running and the connection details are correct in the .env file and yaml file.\nIf you are running locally and using docker do not forget to run docker-compose up before you run the indexer.";
            print_error_message(error);
            return Err(error.into());
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
                    indexing_details: IndexerNoCodeDetails { enabled: true },
                    graphql_details: GraphqlNoCodeDetails {
                        enabled: false,
                        settings: None,
                    },
                };

                start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the server: {}", e));
                    e
                })?;
            }
            StartSubcommands::Graphql { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: project_path.join(YAML_CONFIG_NAME),
                    indexing_details: IndexerNoCodeDetails { enabled: false },
                    graphql_details: GraphqlNoCodeDetails {
                        enabled: true,
                        settings: Some(GraphQLServerDetails {
                            settings: match port {
                                Some(port) => GraphQLServerSettings::port(port.parse().unwrap()),
                                None => Default::default(),
                            },
                        }),
                    },
                };

                start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the indexer: {}", e));
                    e
                })?;
            }
            StartSubcommands::All { port } => {
                let details = StartNoCodeDetails {
                    manifest_path: project_path.join(YAML_CONFIG_NAME),
                    indexing_details: IndexerNoCodeDetails { enabled: true },
                    graphql_details: GraphqlNoCodeDetails {
                        enabled: false,
                        settings: Some(GraphQLServerDetails {
                            settings: match port {
                                Some(port) => GraphQLServerSettings::port(port.parse().unwrap()),
                                None => Default::default(),
                            },
                        }),
                    },
                };

                let _ = start_rindexer_no_code(details).await.map_err(|e| {
                    print_error_message(&format!("Error starting the server: {}", e));
                });
            }
        },
    }

    Ok(())
}

async fn handle_delete_command(project_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    print_warn_message(&format!(
        "This will delete all data in the postgres database and csv files for the project at: {}",
        project_path.display()
    ));
    print_warn_message(
        "This operation can not be reverted. Make sure you know what you are doing.",
    );
    let manifest = read_manifest(&project_path.join(YAML_CONFIG_NAME)).map_err(|e| {
        print_error_message(&format!("Could read the rindexer.yaml please make sure you are running the command with rindexer.yaml in root: trace: {}", e));
        e
    })?;

    let postgres_enabled = manifest.storage.postgres_enabled();
    let csv_enabled = manifest.storage.csv_enabled();

    if !postgres_enabled && !csv_enabled {
        print_success_message("No storage enabled. Nothing to delete.");
        return Ok(());
    }

    if postgres_enabled {
        let postgres_delete = prompt_for_input_list(
            "Are you sure you wish to delete the database data (it can not be reverted)?",
            &["yes".to_string(), "no".to_string()],
            None,
        );

        if postgres_delete == "yes" {
            let postgres_client = PostgresClient::new().await.map_err(|e| {
                print_error_message(&format!("Could not connect to Postgres, make sure your connection string is mapping in the .env correctly: trace: {}", e));
                e
            })?;
            let sql = drop_tables_for_indexer_sql(&manifest.to_indexer());

            postgres_client.batch_execute(sql.as_str()).await.map_err(|e| {
                print_error_message(&format!("Could not delete tables from Postgres make sure your connection string is mapping in the .env correctly: trace: {}", e));
                e
            })?;

            print_success_message(
                "\n\nSuccessfully deleted all data from the postgres database.\n\n",
            );
        }
    }

    if csv_enabled {
        let csv_delete = prompt_for_input_list(
            "Are you sure you wish to delete the csv data (it can not be reverted)?",
            &["yes".to_string(), "no".to_string()],
            None,
        );

        if csv_delete == "yes" {
            let path = &project_path.join(manifest.storage.csv.unwrap().path);
            // if no csv exist we will just look like it cleared it
            if path.exists() {
                remove_dir_all(&project_path.join(path))
                    .await
                    .map_err(|e| {
                        print_error_message(&format!("Could not delete csv files: trace: {}", e));
                        e
                    })?;
            }

            print_success_message("\n\nSuccessfully deleted all csv files.\n\n");
        }
    }

    Ok(())
}

fn load_env_from_path(project_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if from_path(project_path).is_err() {
        dotenv().ok();
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CLI::parse();

    match &cli.command {
        Commands::New { subcommand, path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;

            let project_type = match subcommand {
                NewSubcommands::NoCode => ProjectType::NoCode,
                NewSubcommands::Rust => ProjectType::Rust,
            };

            handle_new_command(resolved_path, project_type)
        }
        Commands::DownloadAbi { path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;
            handle_download_abi_command(resolved_path).await
        }
        Commands::Codegen { subcommand, path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;
            handle_codegen_command(resolved_path, subcommand).await
        }
        Commands::Start { subcommand, path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;
            start(resolved_path, subcommand).await
        }
        Commands::Delete { path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;
            handle_delete_command(resolved_path).await
        }
    }
}

fn resolve_path(override_path: &Option<String>) -> Result<PathBuf, String> {
    match override_path {
        Some(path) => {
            let path = PathBuf::from_str(path).map_err(|_| "Invalid path provided.".to_string())?;
            Ok(path)
        }
        None => {
            Ok(std::env::current_dir()
                .map_err(|_| "Failed to get current directory.".to_string())?)
        }
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
            print!("{}: ", field_name.yellow());
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
            field_name.to_string().green(),
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
