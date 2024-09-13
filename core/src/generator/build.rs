use std::{
    fs,
    path::{Path, PathBuf},
};

use ethers::contract::Abigen;

use super::{
    context_bindings::generate_context_code,
    events_bindings::{
        abigen_contract_file_name, abigen_contract_name, generate_event_bindings,
        generate_event_handlers, GenerateEventBindingsError, GenerateEventHandlersError,
    },
    networks_bindings::generate_networks_code,
};
use crate::{
    generator::database_bindings::generate_database_code,
    helpers::{
        camel_to_snake, create_mod_file, format_all_files_for_project, write_file,
        CreateModFileError, WriteFileError,
    },
    indexer::Indexer,
    manifest::{
        contract::ParseAbiError,
        core::Manifest,
        global::Global,
        network::Network,
        storage::Storage,
        yaml::{read_manifest, ReadManifestError, YAML_CONFIG_NAME},
    },
};

fn generate_file_location(output: &Path, location: &str) -> PathBuf {
    let mut path = PathBuf::from(output);
    path.push(format!("{}.rs", location));
    path
}

#[derive(thiserror::Error, Debug)]
pub enum WriteNetworksError {
    #[error("{0}")]
    CanNotWriteNetworksCode(#[from] WriteFileError),
}

fn write_networks(output: &Path, networks: &[Network]) -> Result<(), WriteNetworksError> {
    let networks_code = generate_networks_code(networks);
    write_file(&generate_file_location(output, "networks"), networks_code.as_str())?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum WriteGlobalError {
    #[error("{0}")]
    CanNotWriteGlobalCode(#[from] WriteFileError),
}

fn write_global(
    output: &Path,
    global: &Global,
    networks: &[Network],
) -> Result<(), WriteGlobalError> {
    let context_code = generate_context_code(&global.contracts, networks);
    write_file(&generate_file_location(output, "global_contracts"), context_code.as_str())?;

    Ok(())
}

fn write_database(output: &Path) -> Result<(), WriteGlobalError> {
    let database_code = generate_database_code();
    write_file(&generate_file_location(output, "database"), database_code.as_str())?;

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum WriteIndexerEvents {
    #[error("Could not write events code: {0}")]
    CouldNotWriteEventsCode(#[from] WriteFileError),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("Could not generate Abigen instance")]
    CouldNotCreateAbigenInstance,

    #[error("Could not generate ABI")]
    CouldNotGenerateAbi,

    #[error("Could not write abigen code: {0}")]
    CouldNotWriteAbigenCodeCode(WriteFileError),

    #[error("{0}")]
    GenerateEventBindingCodeError(#[from] GenerateEventBindingsError),

    #[error("Could not parse ABI: {0}")]
    CouldNotParseAbi(#[from] ParseAbiError),
}

fn write_indexer_events(
    project_path: &Path,
    output: &Path,
    indexer: Indexer,
    storage: &Storage,
) -> Result<(), WriteIndexerEvents> {
    for mut contract in indexer.contracts {
        let is_filter = contract.identify_and_modify_filter();
        let events_code =
            generate_event_bindings(project_path, &indexer.name, &contract, is_filter, storage)?;

        let event_path =
            format!("{}/events/{}", camel_to_snake(&indexer.name), camel_to_snake(&contract.name));
        write_file(&generate_file_location(output, &event_path), events_code.as_str())?;

        let abi_string = contract.parse_abi(project_path)?;

        let abi_gen = Abigen::new(abigen_contract_name(&contract), abi_string)
            .map_err(|_| WriteIndexerEvents::CouldNotCreateAbigenInstance)?
            .generate()
            .map_err(|_| WriteIndexerEvents::CouldNotGenerateAbi)?;

        write_file(
            &generate_file_location(
                output,
                &format!(
                    "{}/events/{}",
                    camel_to_snake(&indexer.name),
                    abigen_contract_file_name(&contract)
                ),
            ),
            &abi_gen.to_string(),
        )
        .map_err(WriteIndexerEvents::CouldNotWriteAbigenCodeCode)?;
    }
    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateRindexerTypingsError {
    #[error("Manifest location does not have a parent - {0}")]
    ManifestLocationDoesNotHaveAParent(String),

    #[error("Manifest location can not be resolved")]
    ManifestLocationCanNotBeResolved,

    #[error("{0}")]
    WriteNetworksError(#[from] WriteNetworksError),

    #[error("{0}")]
    WriteGlobalError(#[from] WriteGlobalError),

    #[error("{0}")]
    WriteIndexerEventsError(#[from] WriteIndexerEvents),

    #[error("{0}")]
    CreateModFileError(#[from] CreateModFileError),
}

pub fn generate_rindexer_typings(
    manifest: &Manifest,
    manifest_location: &Path,
    format_after_generation: bool,
) -> Result<(), GenerateRindexerTypingsError> {
    let project_path = manifest_location.parent();
    match project_path {
        Some(project_path) => {
            let output = project_path.join("./src/rindexer_lib/typings");

            write_networks(&output, &manifest.networks)?;
            if let Some(global) = &manifest.global {
                write_global(&output, global, &manifest.networks)?;
            }

            if manifest.storage.postgres_enabled() {
                write_database(&output)?;
            }

            write_indexer_events(project_path, &output, manifest.to_indexer(), &manifest.storage)?;

            create_mod_file(output.as_path(), true)?;

            if format_after_generation {
                format_all_files_for_project(project_path);
            }

            Ok(())
        }
        None => {
            let manifest_location = manifest_location.to_str();
            match manifest_location {
                Some(manifest_location) => {
                    Err(GenerateRindexerTypingsError::ManifestLocationDoesNotHaveAParent(
                        manifest_location.to_string(),
                    ))
                }
                None => Err(GenerateRindexerTypingsError::ManifestLocationCanNotBeResolved),
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateRindexerHandlersError {
    #[error("Manifest location does not have a parent")]
    ManifestLocationDoesNotHaveAParent,

    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(#[from] std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(#[from] serde_json::Error),

    #[error("{0}")]
    GenerateEventBindingCodeError(#[from] GenerateEventHandlersError),

    #[error("Could not write event handler code: {0}")]
    CouldNotWriteEventHandlerCode(#[from] WriteFileError),

    #[error("Could not write event handlers code: {0}")]
    CouldNotWriteEventHandlersCode(WriteFileError),

    #[error("{0}")]
    CreateModFileError(#[from] CreateModFileError),
}

pub fn generate_rindexer_handlers(
    manifest: Manifest,
    manifest_location: &Path,
    format_after_generation: bool,
) -> Result<(), GenerateRindexerHandlersError> {
    let project_path = manifest_location.parent();
    match project_path {
        None => Err(GenerateRindexerHandlersError::ManifestLocationDoesNotHaveAParent),
        Some(project_path) => {
            let output = project_path.join("./src/rindexer_lib");

            let mut handlers = String::new();
            handlers.push_str(
                r#"
        use std::path::PathBuf;
        use rindexer::event::callback_registry::EventCallbackRegistry;
        
        pub async fn register_all_handlers(manifest_path: &PathBuf) -> EventCallbackRegistry {
             let mut registry = EventCallbackRegistry::new();
        "#,
            );

            for mut contract in manifest.contracts {
                let is_filter = contract.identify_and_modify_filter();

                let indexer_name = camel_to_snake(&manifest.name);
                let contract_name = camel_to_snake(&contract.name);
                let handler_fn_name = format!("{}_handlers", contract_name);

                handlers.insert_str(
                    0,
                    &format!(r#"use super::{indexer_name}::{contract_name}::{handler_fn_name};"#,),
                );

                handlers.push_str(&format!(
                    r#"{handler_fn_name}(manifest_path, &mut registry).await;"#
                ));

                let handler_path = format!("indexers/{}/{}", indexer_name, contract_name);

                write_file(
                    &generate_file_location(&output, &handler_path),
                    generate_event_handlers(
                        project_path,
                        &manifest.name,
                        is_filter,
                        &contract,
                        &manifest.storage,
                    )?
                    .as_str(),
                )?;
            }

            handlers.push_str("registry");
            handlers.push('}');
            write_file(&generate_file_location(&output, "indexers/all_handlers"), &handlers)
                .map_err(GenerateRindexerHandlersError::CouldNotWriteEventHandlersCode)?;

            create_mod_file(output.as_path(), false)?;

            if format_after_generation {
                format_all_files_for_project(project_path);
            }

            Ok(())
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateError {
    #[error("{0}")]
    ReadManifestError(#[from] ReadManifestError),

    #[error("{0}")]
    GenerateRindexerTypingsError(#[from] GenerateRindexerTypingsError),

    #[error("{0}")]
    GenerateRindexerHandlersError(#[from] GenerateRindexerHandlersError),

    #[error("Manifest location does not have a parent - {0}")]
    ManifestLocationDoesNotHaveAParent(String),

    #[error("Manifest location can not be resolved")]
    ManifestLocationCanNotBeResolved,
}

/// Generates all the rindexer project typings and handlers
pub fn generate_rindexer_typings_and_handlers(
    manifest_location: &PathBuf,
) -> Result<(), GenerateError> {
    let manifest = read_manifest(manifest_location)?;

    generate_rindexer_typings(&manifest, manifest_location, false)?;
    generate_rindexer_handlers(manifest, manifest_location, false)?;

    let parent = manifest_location.parent();
    match parent {
        Some(parent) => {
            format_all_files_for_project(parent);
            Ok(())
        }
        None => {
            let manifest_location = manifest_location.to_str();
            match manifest_location {
                Some(manifest_location) => Err(GenerateError::ManifestLocationDoesNotHaveAParent(
                    manifest_location.to_string(),
                )),
                None => Err(GenerateError::ManifestLocationCanNotBeResolved),
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateRustProjectError {
    #[error("{0}")]
    ReadManifestError(#[from] ReadManifestError),

    #[error("Could not create the dir :{0}")]
    CouldNotCreateDir(#[from] std::io::Error),

    #[error("Could not write the file: {0}")]
    WriteFileError(#[from] WriteFileError),

    #[error("{0}")]
    GenerateError(#[from] GenerateError),
}

pub fn generate_rust_project(project_path: &Path) -> Result<(), GenerateRustProjectError> {
    let manifest_location = project_path.join(YAML_CONFIG_NAME);
    let manifest = read_manifest(&project_path.join(&manifest_location))?;

    let abi_path = project_path.join("abis");

    fs::create_dir_all(abi_path)?;

    let cargo = format!(
        r#"
[package]
name = "{project_name}"
version = "0.1.0"
edition = "2021"

[dependencies]
rindexer = {{ git = "https://github.com/joshstevens19/rindexer", branch = "master" }}
tokio = {{ version = "1", features = ["full"] }}
ethers = {{ version = "2.0", features = ["rustls", "openssl"] }}
serde = {{ version = "1.0.194", features = ["derive"] }}
"#,
        project_name = manifest.name,
    );

    let cargo_path = project_path.join("Cargo.toml");
    write_file(&cargo_path, &cargo)?;

    fs::create_dir_all(project_path.join("src"))?;

    let main_code = r#"
            use std::env;

            use self::rindexer_lib::indexers::all_handlers::register_all_handlers;
            use rindexer::{
                start_rindexer, GraphqlOverrideSettings, IndexingDetails, StartDetails,
            };

            mod rindexer_lib;

            #[tokio::main]
            async fn main() {
                let args: Vec<String> = env::args().collect();

                let mut enable_graphql = false;
                let mut enable_indexer = false;
                
                let mut port: Option<u16> = None;

                let args = args.iter();
                if args.len() == 0 {
                    enable_graphql = true;
                    enable_indexer = true;
                }

                for arg in args {
                    match arg.as_str() {
                        "--graphql" => enable_graphql = true,
                        "--indexer" => enable_indexer = true,
                        _ if arg.starts_with("--port=") || arg.starts_with("--p") => {
                            if let Some(value) = arg.split('=').nth(1) {
                                let overridden_port = value.parse::<u16>();
                                match overridden_port {
                                    Ok(overridden_port) => port = Some(overridden_port),
                                    Err(_) => {
                                        println!("Invalid port number");
                                        return;
                                    }
                                }
                            }
                        },
                        _ => {}
                    }
                }

                let path = env::current_dir();
                match path {
                    Ok(path) => {
                        let manifest_path = path.join("rindexer.yaml");
                        let result = start_rindexer(StartDetails {
                            manifest_path: &manifest_path,
                            indexing_details: if enable_indexer {
                                Some(IndexingDetails {
                                    registry: register_all_handlers(&manifest_path).await,
                                })
                            } else {
                                None
                            },
                            graphql_details: GraphqlOverrideSettings {
                                enabled: enable_graphql,
                                override_port: port,
                            }
                        })
                        .await;
                        
                        match result {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Error starting rindexer: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("Error getting current directory: {:?}", e);
                    }
                }
            }
          "#;

    let main_path = project_path.join("src").join("main.rs");
    write_file(&main_path, main_code)?;

    generate_rindexer_typings_and_handlers(&manifest_location)
        .map_err(GenerateRustProjectError::GenerateError)
}
