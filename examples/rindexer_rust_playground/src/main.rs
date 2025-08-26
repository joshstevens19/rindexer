use std::{env, path::PathBuf};

use rindexer::{
    GraphqlOverrideSettings, IndexingDetails, StartDetails,
    event::callback_registry::TraceCallbackRegistry, manifest::yaml::read_manifest, start_rindexer,
};

use self::rindexer_lib::indexers::all_handlers::register_all_handlers;

#[allow(clippy::all)]
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
            }
            _ => {}
        }
    }

    println!("Starting rindexer rust project - graphql {enable_graphql} indexer {enable_indexer}");

    let path = env::current_dir();
    match path {
        Ok(path) => {
            let manifest_path = path.join("rindexer.yaml");
            let result = start_rindexer(StartDetails {
                manifest_path: &manifest_path,
                indexing_details: if enable_indexer {
                    Some(IndexingDetails {
                        registry: register_all_handlers(&manifest_path).await,
                        trace_registry: TraceCallbackRegistry { events: vec![] },
                    })
                } else {
                    None
                },
                graphql_details: GraphqlOverrideSettings {
                    enabled: enable_graphql,
                    override_port: port,
                },
            })
            .await;

            match result {
                Ok(_) => {}
                Err(e) => {
                    println!("Error starting rindexer: {e:?}");
                }
            }
        }
        Err(e) => {
            println!("Error getting current directory: {e:?}");
        }
    }
}

#[allow(dead_code)]
fn generate() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let path = PathBuf::from(manifest_dir).join("rindexer.yaml");
    let manifest = read_manifest(&path).expect("Failed to read manifest");
    rindexer::generator::build::generate_rindexer_typings(&manifest, &path, true)
        .expect("Failed to generate typings");
}

#[allow(dead_code)]
fn generate_code_test() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let path = PathBuf::from(manifest_dir).join("rindexer.yaml");
    let manifest = read_manifest(&path).expect("Failed to read manifest");

    rindexer::generator::build::generate_rindexer_handlers(manifest, &path, true)
        .expect("Failed to generate handlers");
}

#[allow(dead_code)]
fn generate_all() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let path = PathBuf::from(manifest_dir).join("rindexer.yaml");
    rindexer::generator::build::generate_rindexer_typings_and_handlers(&path)
        .expect("Failed to generate typings and handlers");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        generate();
    }

    #[test]
    fn test_code_generate() {
        generate_code_test();
    }

    #[test]
    fn test_generate_all() {
        generate_all();
    }
}
