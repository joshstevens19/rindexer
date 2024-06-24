use std::path::Path;
use std::{fs, path::PathBuf};

use ethers::contract::Abigen;

use crate::helpers::{
    camel_to_snake, create_mod_file, format_all_files_for_project, write_file, CreateModFileError,
    WriteFileError,
};
use crate::indexer::Indexer;
use crate::manifest::yaml::{
    read_manifest, Contract, Global, Manifest, Network, ReadManifestError, Storage,
    YAML_CONFIG_NAME,
};

use super::events_bindings::{
    abigen_contract_file_name, abigen_contract_name, generate_event_bindings,
    generate_event_handlers, GenerateEventBindingsError, GenerateEventHandlersError,
};
use super::{context_bindings::generate_context_code, networks_bindings::generate_networks_code};

fn generate_file_location(output: &Path, location: &str) -> PathBuf {
    let mut path = PathBuf::from(output);
    path.push(format!("{}.rs", location));
    path
}

#[derive(thiserror::Error, Debug)]
pub enum WriteNetworksError {
    #[error("{0}")]
    CanNotWriteNetworksCode(WriteFileError),
}

fn write_networks(output: &Path, networks: &[Network]) -> Result<(), WriteNetworksError> {
    let networks_code = generate_networks_code(networks);
    write_file(
        &generate_file_location(output, "networks"),
        networks_code.as_str(),
    )
    .map_err(WriteNetworksError::CanNotWriteNetworksCode)
}

#[derive(thiserror::Error, Debug)]
pub enum WriteGlobalError {
    #[error("{0}")]
    CanNotWriteGlobalCode(WriteFileError),
}

fn write_global(
    output: &Path,
    global: &Global,
    networks: &[Network],
) -> Result<(), WriteGlobalError> {
    let context_code = generate_context_code(&global.contracts, networks);
    write_file(
        &generate_file_location(output, "global_contracts"),
        context_code.as_str(),
    )
    .map_err(WriteGlobalError::CanNotWriteGlobalCode)?;

    Ok(())
}

/// Identifies if the contract uses a filter
pub fn is_filter(contract: &Contract) -> bool {
    let filter_count = contract
        .details
        .iter()
        .filter(|details| details.indexing_contract_setup().is_filter())
        .count();

    if filter_count > 0 && filter_count != contract.details.len() {
        // panic as this should never happen as validation has already happened
        panic!("Cannot mix and match address and filter for the same contract definition.");
    }

    filter_count > 0
}

pub fn contract_name_to_filter_name(contract_name: &str) -> String {
    format!("{}Filter", contract_name)
}

/// Identifies if the contract uses a filter and updates its name if necessary.
pub fn identify_and_modify_filter(contract: &mut Contract) -> bool {
    if is_filter(contract) {
        contract.override_name(contract_name_to_filter_name(&contract.name));
        true
    } else {
        false
    }
}

#[derive(thiserror::Error, Debug)]
pub enum WriteIndexerEvents {
    #[error("Could not write events code: {0}")]
    CouldNotWriteEventsCode(WriteFileError),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),

    #[error("Could not generate Abigen instance")]
    CouldNotCreateAbigenInstance,

    #[error("Could not generate ABI")]
    CouldNotGenerateAbi,

    #[error("Could not write abigen code: {0}")]
    CouldNotWriteAbigenCodeCode(WriteFileError),

    #[error("{0}")]
    GenerateEventBindingCodeError(GenerateEventBindingsError),
}

fn write_indexer_events(
    project_path: &Path,
    output: &Path,
    indexer: Indexer,
    storage: &Storage,
) -> Result<(), WriteIndexerEvents> {
    for mut contract in indexer.contracts {
        let is_filter = identify_and_modify_filter(&mut contract);
        let events_code =
            generate_event_bindings(project_path, &indexer.name, &contract, is_filter, storage)
                .map_err(WriteIndexerEvents::GenerateEventBindingCodeError)?;

        let event_path = format!(
            "{}/events/{}",
            camel_to_snake(&indexer.name),
            camel_to_snake(&contract.name)
        );
        write_file(
            &generate_file_location(output, &event_path),
            events_code.as_str(),
        )
        .map_err(WriteIndexerEvents::CouldNotWriteEventsCode)?;

        let abi_gen = Abigen::new(abigen_contract_name(&contract), &contract.abi)
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
    ManifestLocationCanNotBeResolved(),

    #[error("{0}")]
    WriteNetworksError(WriteNetworksError),

    #[error("{0}")]
    WriteGlobalError(WriteGlobalError),

    #[error("{0}")]
    WriteIndexerEventsError(WriteIndexerEvents),

    #[error("{0}")]
    CreateModFileError(CreateModFileError),
}

pub fn generate_rindexer_typings(
    manifest: Manifest,
    manifest_location: &Path,
) -> Result<(), GenerateRindexerTypingsError> {
    let project_path = manifest_location.parent();
    match project_path {
        Some(project_path) => {
            let output = project_path.join("./src/rindexer/typings");

            write_networks(&output, &manifest.networks)
                .map_err(GenerateRindexerTypingsError::WriteNetworksError)?;
            write_global(&output, &manifest.global, &manifest.networks)
                .map_err(GenerateRindexerTypingsError::WriteGlobalError)?;

            write_indexer_events(
                project_path,
                &output,
                manifest.to_indexer(),
                &manifest.storage,
            )
            .map_err(GenerateRindexerTypingsError::WriteIndexerEventsError)?;

            create_mod_file(output.as_path(), true)
                .map_err(GenerateRindexerTypingsError::CreateModFileError)?;

            Ok(())
        }
        None => {
            let manifest_location = manifest_location.to_str();
            match manifest_location {
                Some(manifest_location) => Err(
                    GenerateRindexerTypingsError::ManifestLocationDoesNotHaveAParent(
                        manifest_location.to_string(),
                    ),
                ),
                None => Err(GenerateRindexerTypingsError::ManifestLocationCanNotBeResolved()),
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateRindexerHandlersError {
    #[error("Manifest location does not have a parent")]
    ManifestLocationDoesNotHaveAParent,

    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(std::io::Error),

    #[error("Could not read ABI JSON: {0}")]
    CouldNotReadAbiJson(serde_json::Error),

    #[error("{0}")]
    GenerateEventBindingCodeError(GenerateEventHandlersError),

    #[error("Could not write event handler code: {0}")]
    CouldNotWriteEventHandlerCode(WriteFileError),

    #[error("Could not write event handlers code: {0}")]
    CouldNotWriteEventHandlersCode(WriteFileError),

    #[error("{0}")]
    CreateModFileError(CreateModFileError),
}

pub fn generate_rindexer_handlers(
    manifest: Manifest,
    manifest_location: &Path,
) -> Result<(), GenerateRindexerHandlersError> {
    let output_parent = manifest_location.parent();
    match output_parent {
        None => Err(GenerateRindexerHandlersError::ManifestLocationDoesNotHaveAParent),
        Some(output_parent) => {
            let output = output_parent.join("./src/rindexer");

            let mut handlers = String::new();
            handlers.push_str(
                r#"
        use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;
        
        pub async fn register_all_handlers() -> EventCallbackRegistry {
             let mut registry = EventCallbackRegistry::new();
        "#,
            );

            for mut contract in manifest.contracts {
                let is_filter = identify_and_modify_filter(&mut contract);

                let indexer_name = camel_to_snake(&manifest.name);
                let contract_name = camel_to_snake(&contract.name);
                let handler_fn_name = format!("{}_handlers", contract_name);

                handlers.insert_str(
                    0,
                    &format!(r#"use super::{indexer_name}::{contract_name}::{handler_fn_name};"#,),
                );

                handlers.push_str(&format!(r#"{handler_fn_name}(&mut registry).await;"#));

                let handler_path = format!("indexers/{}/{}", indexer_name, contract_name);

                write_file(
                    &generate_file_location(&output, &handler_path),
                    generate_event_handlers(
                        &manifest.name,
                        is_filter,
                        &contract,
                        &manifest.storage,
                    )
                    .map_err(GenerateRindexerHandlersError::GenerateEventBindingCodeError)?
                    .as_str(),
                )
                .map_err(GenerateRindexerHandlersError::CouldNotWriteEventHandlerCode)?;
            }

            handlers.push_str("registry");
            handlers.push('}');
            write_file(
                &generate_file_location(&output, "indexers/all_handlers"),
                &handlers,
            )
            .map_err(GenerateRindexerHandlersError::CouldNotWriteEventHandlersCode)?;

            create_mod_file(output.as_path(), false)
                .map_err(GenerateRindexerHandlersError::CreateModFileError)?;

            Ok(())
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateError {
    #[error("{0}")]
    ReadManifestError(ReadManifestError),

    #[error("{0}")]
    GenerateRindexerTypingsError(GenerateRindexerTypingsError),

    #[error("{0}")]
    GenerateRindexerHandlersError(GenerateRindexerHandlersError),

    #[error("Manifest location does not have a parent - {0}")]
    ManifestLocationDoesNotHaveAParent(String),

    #[error("Manifest location can not be resolved")]
    ManifestLocationCanNotBeResolved(),
}

/// Generates all the rindexer project typings and handlers
pub fn generate_rindexer_typings_and_handlers(
    manifest_location: &PathBuf,
) -> Result<(), GenerateError> {
    let manifest = read_manifest(manifest_location).map_err(GenerateError::ReadManifestError)?;

    generate_rindexer_typings(manifest.clone(), manifest_location)
        .map_err(GenerateError::GenerateRindexerTypingsError)?;
    generate_rindexer_handlers(manifest.clone(), manifest_location)
        .map_err(GenerateError::GenerateRindexerHandlersError)?;

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
                None => Err(GenerateError::ManifestLocationCanNotBeResolved()),
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateRustProjectError {
    #[error("{0}")]
    ReadManifestError(ReadManifestError),

    #[error("Could not create the dir :{0}")]
    CouldNotCreateDir(std::io::Error),

    #[error("Could not write the file: {0}")]
    WriteFileError(WriteFileError),

    #[error("{0}")]
    GenerateError(GenerateError),
}

pub fn generate_rust_project(project_path: &Path) -> Result<(), GenerateRustProjectError> {
    let manifest_location = project_path.join(YAML_CONFIG_NAME);
    let manifest = read_manifest(&project_path.join(&manifest_location))
        .map_err(GenerateRustProjectError::ReadManifestError)?;

    let abi_path = project_path.join("abis");

    fs::create_dir_all(abi_path).map_err(GenerateRustProjectError::CouldNotCreateDir)?;

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

    let cargo_path = project_path.join("Cargo.toml");
    write_file(&cargo_path, &cargo).map_err(GenerateRustProjectError::WriteFileError)?;

    fs::create_dir_all(project_path.join("src"))
        .map_err(GenerateRustProjectError::CouldNotCreateDir)?;

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

                let path = env::current_dir();
                match path {
                    Ok(path) => {
                        let result = start_rindexer(StartDetails {
                            manifest_path: path.join("rindexer.yaml"),
                            indexing_details: if enable_indexer {
                                Some(IndexingDetails {
                                    registry: register_all_handlers().await,
                                })
                            } else {
                                None
                            },
                            graphql_server: if enable_graphql {
                                Some(GraphQLServerDetails {
                                    settings: port.map_or_else(Default::default, GraphQLServerSettings::port),
                                })
                            } else {
                                None
                            },
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
    write_file(&main_path, main_code).map_err(GenerateRustProjectError::WriteFileError)?;

    generate_rindexer_typings_and_handlers(&manifest_location)
        .map_err(GenerateRustProjectError::GenerateError)
}
