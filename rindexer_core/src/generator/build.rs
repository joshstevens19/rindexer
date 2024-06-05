use std::path::Path;
use std::{error::Error, path::PathBuf};

use ethers::contract::Abigen;

use crate::helpers::{camel_to_snake, create_mod_file, format_all_files_for_project, write_file};
use crate::manifest::yaml::{read_manifest, Contract, Global, Indexer, Network, Storage, Manifest};

use super::events_bindings::{
    abigen_contract_file_name, abigen_contract_name, generate_event_bindings,
    generate_event_handlers,
};
use super::{context_bindings::generate_context_code, networks_bindings::generate_networks_code};

/// Generates the file location path for a given output directory and location.
///
/// # Arguments
///
/// * `output` - The output directory.
/// * `location` - The location within the output directory.
///
/// # Returns
///
/// A `String` representing the full path to the file.
fn generate_file_location(output: &str, location: &str) -> String {
    format!("{}/{}.rs", output, location)
}

/// Writes the networks configuration to a file.
///
/// # Arguments
///
/// * `output` - The output directory.
/// * `networks` - A reference to a vector of `Network` configurations.
///
/// # Returns
///
/// A `Result` indicating success or failure.
fn write_networks(output: &str, networks: &[Network]) -> Result<(), Box<dyn Error>> {
    let networks_code = generate_networks_code(networks)?;
    write_file(&generate_file_location(output, "networks"), &networks_code)
}

/// Writes the global configuration to a file if it exists.
///
/// # Arguments
///
/// * `output` - The output directory.
/// * `global` - An optional reference to a `Global` configuration.
/// * `networks` - A reference to a slice of `Network` configurations.
///
/// # Returns
///
/// A `Result` indicating success or failure.
fn write_global(output: &str, global: &Global, networks: &[Network]) -> Result<(), Box<dyn Error>> {
    let context_code = generate_context_code(&global.contracts, networks)?;
    write_file(
        &generate_file_location(output, "global_contracts"),
        &context_code,
    )?;

    Ok(())
}

/// Identifies if the contract uses a filter
///
/// # Arguments
///
/// * `contract` - A reference to a `Contract`.
///
/// # Returns
///
/// A `bool` indicating whether the contract uses a filter.
pub fn is_filter(contract: &Contract) -> bool {
    let filter_count = contract
        .details
        .iter()
        .filter(|details| details.indexing_contract_setup().is_filter())
        .count();

    if filter_count > 0 && filter_count != contract.details.len() {
        panic!("Cannot mix and match address and filter for the same contract definition.");
    }

    filter_count > 0
}

/// Converts a contract name to a filter name.
pub fn contract_name_to_filter_name(contract_name: &str) -> String {
    format!("{}Filter", contract_name)
}

/// Identifies if the contract uses a filter and updates its name if necessary.
///
/// # Arguments
///
/// * `contract` - A mutable reference to a `Contract`.
///
/// # Returns
///
/// A `bool` indicating whether the contract uses a filter.
pub fn identify_and_modify_filter(contract: &mut Contract) -> bool {
    if is_filter(contract) {
        contract.override_name(contract_name_to_filter_name(&contract.name));
        true
    } else {
        false
    }
}

/// Writes event bindings and ABI generation for the given indexer and its contracts.
///
/// # Arguments
///
/// * `project_path` - A reference to the project path.
/// * `output` - The output directory.
/// * `indexer` - A reference to an `Indexer`.
/// * `global` - An optional reference to a `Global` configuration.
///
/// # Returns
///
/// A `Result` indicating success or failure.
fn write_indexer_events(
    project_path: &Path,
    output: &str,
    indexer: Indexer,
    storage: &Storage,
) -> Result<(), Box<dyn Error>> {
    for mut contract in indexer.contracts {
        let is_filter = identify_and_modify_filter(&mut contract);
        let events_code =
            generate_event_bindings(project_path, &indexer.name, &contract, is_filter, storage)?;

        let event_path = format!(
            "{}/events/{}",
            camel_to_snake(&indexer.name),
            camel_to_snake(&contract.name)
        );
        write_file(&generate_file_location(output, &event_path), &events_code)?;

        // Write ABI gen
        let abi_gen = Abigen::new(abigen_contract_name(&contract), &contract.abi)?.generate()?;

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
        )?;
    }
    Ok(())
}

/// Generates typings for the rindexer based on the manifest file.
///
/// # Arguments
///
/// * `manifest_location` - A reference to the path of the manifest file.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub fn generate_rindexer_typings(manifest: Manifest, manifest_location: &PathBuf) -> Result<(), Box<dyn Error>> {
    let project_path = manifest_location.parent().unwrap();
    let output = project_path.join("./src/rindexer/typings");

    let output_path = output.to_str().unwrap();

    write_networks(output_path, &manifest.networks)?;
    write_global(output_path, &manifest.global, &manifest.networks)?;

    for indexer in manifest.indexers {
        write_indexer_events(project_path, output_path, indexer, &manifest.storage)?;
    }

    create_mod_file(output.as_path(), true)?;

    Ok(())
}

/// Generates code for indexer handlers based on the manifest file.
///
/// # Arguments
///
/// * `manifest_location` - A reference to the path of the manifest file.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub fn generate_rindexer_handlers(manifest: Manifest, manifest_location: &PathBuf) -> Result<(), Box<dyn Error>> {
    let output = manifest_location.parent().unwrap().join("./src/rindexer");

    let mut all_indexers = String::new();
    all_indexers.push_str(
        r#"
        use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;
        
        pub async fn register_all_handlers() -> EventCallbackRegistry {
             let mut registry = EventCallbackRegistry::new();
        "#,
    );

    for indexer in manifest.indexers {
        for mut contract in indexer.contracts {
            let is_filter = identify_and_modify_filter(&mut contract);

            let indexer_name = camel_to_snake(&indexer.name);
            let contract_name = camel_to_snake(&contract.name);
            let handler_fn_name = format!("{}_handlers", contract_name);

            all_indexers.insert_str(
                0,
                &format!(r#"use super::{indexer_name}::{contract_name}::{handler_fn_name};"#,),
            );

            all_indexers.push_str(&format!(r#"{handler_fn_name}(&mut registry).await;"#));

            let handler_path = format!("indexers/{}/{}", indexer_name, contract_name);

            write_file(
                &generate_file_location(output.to_str().unwrap(), &handler_path),
                &generate_event_handlers(&indexer.name, is_filter, &contract, &manifest.storage)?,
            )?;
        }
    }

    all_indexers.push_str("registry");
    all_indexers.push('}');
    write_file(
        &generate_file_location(output.to_str().unwrap(), "indexers/all_handlers"),
        &all_indexers,
    )?;

    create_mod_file(output.as_path(), false)?;

    Ok(())
}

/// Generates all the rindexer project typings and handlers
///
/// # Arguments
///
/// * `manifest_location` - A reference to the path of the manifest file.
///
/// # Returns
///
/// A `Result` indicating success or failure.
pub fn generate(manifest_location: &PathBuf) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(manifest_location)?;
    
    generate_rindexer_typings(manifest.clone(), manifest_location)?;
    generate_rindexer_handlers(manifest.clone(), manifest_location)?;

    format_all_files_for_project(manifest_location.parent().unwrap());

    Ok(())
}
