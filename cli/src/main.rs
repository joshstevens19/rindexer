#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod cli_interface;
mod commands;
mod console;
mod rindexer_yaml;

use crate::cli_interface::{AddSubcommands, Commands, NewSubcommands, CLI};
use crate::commands::{
    add::handle_add_contract_command, codegen::handle_codegen_command,
    delete::handle_delete_command, new::handle_new_command, start::start,
};
use crate::console::print_error_message;
use clap::Parser;
use dotenv::{dotenv, from_path};
use rindexer::manifest::yaml::ProjectType;
use std::path::PathBuf;
use std::str::FromStr;

fn load_env_from_path(project_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if from_path(project_path).is_err() {
        dotenv().ok();
    }
    Ok(())
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
        Commands::Add { subcommand, path } => {
            let resolved_path = resolve_path(path).map_err(|e| {
                print_error_message(&e);
                e
            })?;
            load_env_from_path(&resolved_path)?;

            match subcommand {
                AddSubcommands::Contract => handle_add_contract_command(resolved_path).await,
            }
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
