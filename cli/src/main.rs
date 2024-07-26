use std::{backtrace::Backtrace, env, panic};

#[cfg(feature = "jemalloc")]
use jemallocator::Jemalloc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

mod cli_interface;
mod commands;
mod console;
mod rindexer_yaml;

use std::{path::PathBuf, str::FromStr, sync::Once};

use clap::Parser;
use rindexer::{load_env_from_path, manifest::core::ProjectType};

use crate::{
    cli_interface::{AddSubcommands, Commands, NewSubcommands, CLI},
    commands::{
        add::handle_add_contract_command, codegen::handle_codegen_command,
        delete::handle_delete_command, new::handle_new_command, phantom::handle_phantom_commands,
        start::start,
    },
    console::print_error_message,
};

static INIT: Once = Once::new();

fn set_panic_hook() {
    INIT.call_once(|| {
        panic::set_hook(Box::new(|info| {
            eprintln!("=== Start Of Custom rindexer unhandled panic hook - please supply this information on the github issue if it happens ===");

            if let Some(location) = info.location() {
                eprintln!(
                    "Panic occurred in file '{}' at line {}",
                    location.file(),
                    location.line()
                );
            } else {
                eprintln!("Panic occurred but can't get location information...");
            }

            if let Some(s) = info.payload().downcast_ref::<&str>() {
                eprintln!("Panic message: {}", s);
            } else {
                eprintln!("Panic occurred but can't get the panic message...");
            }

            let backtrace = Backtrace::capture();
            eprintln!("{:?}", backtrace);
            eprintln!("=== End Of Custom rindexer unhandled panic hook - please supply this information on the github issue if it happens ===");
        }));
    });
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
    env::set_var("RUST_BACKTRACE", "full");
    set_panic_hook();
    let cli = CLI::parse();

    match &cli.command {
        Commands::New { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);

            let project_type = match subcommand {
                NewSubcommands::NoCode => ProjectType::NoCode,
                NewSubcommands::Rust => ProjectType::Rust,
            };

            handle_new_command(resolved_path, project_type)
        }
        Commands::Add { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);

            match subcommand {
                AddSubcommands::Contract => handle_add_contract_command(resolved_path).await,
            }
        }
        Commands::Codegen { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);
            handle_codegen_command(resolved_path, subcommand).await
        }
        Commands::Start { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);
            start(resolved_path, subcommand).await
        }
        Commands::Delete { path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);
            handle_delete_command(resolved_path).await
        }
        Commands::Phantom { subcommand, path } => {
            let resolved_path = resolve_path(path).inspect_err(|e| print_error_message(e))?;
            load_env_from_path(&resolved_path);
            handle_phantom_commands(resolved_path, subcommand).await
        }
    }
}
