use std::path::PathBuf;
use std::{error::Error, path::Path};

use ethers::contract::Abigen;

use crate::helpers::create_mod_file;
use crate::manifest::yaml::{Contract, Global};
use crate::{
    helpers::{camel_to_snake, write_file},
    manifest::yaml::{read_manifest, Indexer, Network},
};

use super::events_bindings::{
    abigen_contract_file_name, abigen_contract_name, generate_event_handlers,
};
use super::{
    context_bindings::generate_context_code, events_bindings::generate_event_bindings,
    networks_bindings::generate_networks_code,
};

fn generate_file_location(output: &str, location: &str) -> String {
    format!("{}/{}.rs", output, location)
}

fn write_networks(output: &str, networks: &Vec<Network>) -> Result<(), Box<dyn Error>> {
    let networks_code = generate_networks_code(networks)?;

    write_file(&generate_file_location(output, "networks"), &networks_code)?;

    Ok(())
}

fn write_global(
    output: &str,
    global: &Option<Global>,
    networks: &[Network],
) -> Result<(), Box<dyn Error>> {
    if let Some(global) = global {
        let context_code = generate_context_code(&global.contracts, networks)?;
        write_file(
            &generate_file_location(output, "global_contracts"),
            &context_code,
        )?;
    }

    Ok(())
}

fn identify_filter(contract: &mut Contract) -> bool {
    // TODO look into how we can mix and match
    let filter_count = contract
        .details
        .iter()
        .filter(|details| details.address_or_filter().is_filter())
        .count();

    if filter_count != contract.details.len() {
        panic!("Cannot mix and match address and filter for the same contract definition.");
    }

    let is_filter = filter_count > 0;
    if is_filter {
        contract.override_name(format!("{}Filter", contract.name));
    }

    is_filter
}

fn write_indexer_events(
    output: &str,
    indexer: Indexer,
    global: &Option<Global>,
) -> Result<(), Box<dyn Error>> {
    for mut contract in indexer.contracts {
        let databases = if let Some(global) = global {
            &global.databases
        } else {
            &None
        };

        let is_filter = identify_filter(&mut contract);
        let events_code = generate_event_bindings(&contract, is_filter, databases)?;

        write_file(
            &generate_file_location(
                output,
                &format!(
                    "{}/events/{}",
                    camel_to_snake(&indexer.name),
                    camel_to_snake(&contract.name)
                ),
            ),
            &events_code,
        )?;

        // write ABI gen
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

pub fn generate_rindexer_code(
    manifest_location: &PathBuf,
    output: &str,
) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(manifest_location)?;

    write_networks(output, &manifest.networks)?;
    write_global(output, &manifest.global, &manifest.networks)?;

    for indexer in manifest.indexers {
        write_indexer_events(output, indexer, &manifest.global)?;
    }

    create_mod_file(Path::new(output))?;

    Ok(())
}

pub fn generate_indexers_handlers_code(
    manifest_location: &PathBuf,
    output: &str,
) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(manifest_location)?;

    for indexer in manifest.indexers {
        for mut contract in indexer.contracts {
            let is_filter = identify_filter(&mut contract);
            let result = generate_event_handlers(&indexer.name, is_filter, &contract).unwrap();
            write_file(
                &generate_file_location(
                    output,
                    &format!(
                        "indexers/{}/{}",
                        camel_to_snake(&indexer.name),
                        camel_to_snake(&contract.name)
                    ),
                ),
                &result,
            )?;
        }
    }

    create_mod_file(Path::new(output))?;

    Ok(())
}
