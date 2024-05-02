use std::{error::Error, path::Path};
use std::path::PathBuf;

use ethers::contract::Abigen;

use crate::helpers::create_mod_file;
use crate::manifest::yaml::Global;
use crate::{
    helpers::{camel_to_snake, write_file},
    manifest::yaml::{read_manifest, Indexer, Network},
};

use super::events_bindings::{abigen_source_file_name, abigen_source_name};
use super::{
    context_bindings::generate_context_code, events_bindings::generate_event_bindings_from_abi,
    networks_bindings::generate_networks_code,
};

fn generate_file_location(output: &str, location: &str) -> String {
    format!("{}/{}.rs", output, location)
}

fn write_networks(output: &str, networks: &Vec<Network>) -> Result<(), Box<dyn Error>> {
    let networks_code = generate_networks_code(&networks)?;

    write_file(&generate_file_location(output, "networks"), &networks_code)?;

    Ok(())
}

fn write_global(
    output: &str,
    global: &Option<Global>,
    networks: &[Network],
) -> Result<(), Box<dyn Error>> {
    if let Some(global) = global {
        if let Some(mappings) = &global.mappings {
            let context_code = generate_context_code(&global.context, &mappings, networks)?;
            write_file(
                &generate_file_location(output, "global_context"),
                &context_code,
            )?;
        } else {
            Err("Mappings not found in global, if global contracts is defined mappings must")?
        }
    }

    Ok(())
}

fn write_indexer_events(
    output: &str,
    indexer: &Indexer,
    global: &Option<Global>,
) -> Result<(), Box<dyn Error>> {
    for source in &indexer.contracts {
        let abi = &indexer
            .mappings
            .abis
            .iter()
            .find(|&obj| obj.name == source.abi)
            .unwrap();

        let clients = if let Some(global) = global {
            &global.clients
        } else {
            &None
        };

        let events_code = generate_event_bindings_from_abi(source, clients, &abi.file)?;

        write_file(
            &generate_file_location(
                output,
                &format!(
                    "{}/events/{}",
                    camel_to_snake(&indexer.name),
                    camel_to_snake(&source.name)
                ),
            ),
            &events_code,
        )?;

        // write ABI gen
        let abi_gen = Abigen::new(abigen_source_name(&source), &abi.file)?.generate()?;

        write_file(
            &generate_file_location(
                output,
                &format!(
                    "{}/events/{}",
                    camel_to_snake(&indexer.name),
                    abigen_source_file_name(&source)
                ),
            ),
            &abi_gen.to_string(),
        )?;
    }

    Ok(())
}

pub fn build(manifest_location: &PathBuf, output: &str) -> Result<(), Box<dyn Error>> {
    let manifest = read_manifest(manifest_location)?;

    write_networks(output, &manifest.networks)?;
    write_global(output, &manifest.global, &manifest.networks)?;

    for indexer in manifest.indexers {
        write_indexer_events(output, &indexer, &manifest.global)?;
    }

    create_mod_file(Path::new(output))?;

    Ok(())
}
