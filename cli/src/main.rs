use std::env;

#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod cli_interface;
mod commands;
mod console;
mod rindexer_yaml;

use std::{path::PathBuf, str::FromStr};

use clap::Parser;
use rindexer::{load_env_from_project_path, manifest::core::ProjectType};

#[cfg(feature = "reth")]
use rindexer::manifest::reth::RethConfig;

use crate::{
    cli_interface::{AddSubcommands, Commands, NewSubcommands, CLI},
    commands::{
        add::handle_add_contract_command, codegen::handle_codegen_command,
        delete::handle_delete_command, new::handle_new_command, phantom::handle_phantom_commands,
        start::start,
    },
    console::print_error_message,
};

fn resolve_path(override_path: &Option<String>) -> Result<PathBuf, String> {
    match override_path {
        Some(path) => {
            let path = PathBuf::from_str(path).map_err(|_| "Invalid path provided.".to_string())?;
            Ok(path)
        }
        None => Ok(env::current_dir().map_err(|_| "Failed to get current directory.".to_string())?),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = CLI::parse();

    match &cli.command {
        Commands::New { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);

            #[allow(unused_variables)]
            let (project_type, reth_args) = match subcommand {
                NewSubcommands::NoCode { reth } => (ProjectType::NoCode, reth),
                NewSubcommands::Rust { reth } => (ProjectType::Rust, reth),
            };

            #[cfg(feature = "reth")]
            let reth_config = if reth_args.reth {
                match RethConfig::from_cli_args(reth_args.reth_args.clone()) {
                    Ok(config) => Some(config),
                    Err(e) => {
                        print_error_message(&format!("Invalid reth arguments: {e}"));
                        return Err(e.into());
                    }
                }
            } else {
                None
            };

            #[cfg(not(feature = "reth"))]
            let reth_config = None;

            handle_new_command(resolved_path, project_type, reth_config)
        }
        Commands::Add { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);

            match subcommand {
                AddSubcommands::Contract => handle_add_contract_command(resolved_path).await,
            }
        }
        Commands::Codegen { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);
            handle_codegen_command(resolved_path, subcommand).await
        }
        Commands::Start { subcommand, path, yes } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);
            start(resolved_path, subcommand, *yes).await
        }
        Commands::Delete { path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);
            handle_delete_command(resolved_path).await
        }
        Commands::Phantom { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_project_path(&resolved_path);
            handle_phantom_commands(resolved_path, subcommand).await
        }
    }
}
