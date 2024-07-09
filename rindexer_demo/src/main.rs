use std::env;
// use std::path::PathBuf;
// use std::str::FromStr;

use self::rindexer::indexers::all_handlers::register_all_handlers;
// use rindexer_core::manifest::yaml::read_manifest;
use rindexer_core::{start_rindexer, GraphqlOverrideSettings, IndexingDetails, StartDetails};

mod rindexer;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut enable_graphql = false;
    let mut enable_indexer = false;

    let mut port: Option<u16> = None;

    for arg in args.iter() {
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
                graphql_details: GraphqlOverrideSettings {
                    enabled: enable_graphql,
                    override_port: port,
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

// fn generate() {
//     let path =
//         PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/rindexer.yaml").unwrap();
//     let manifest = read_manifest(&path).unwrap();
//     rindexer_core::generator::build::generate_rindexer_typings(manifest, &path).unwrap();
// }
//
// fn generate_code_test() {
//     let path =
//         PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/rindexer.yaml").unwrap();
//     let manifest = read_manifest(&path).unwrap();
//
//     rindexer_core::generator::build::generate_rindexer_handlers(manifest, &path).unwrap();
// }
//
// fn generate_all() {
//     let path =
//         PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/rindexer.yaml").unwrap();
//     rindexer_core::generator::build::generate_rindexer_typings_and_handlers(&path).unwrap();
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_generate() {
//         generate();
//     }

// #[test]
// fn test_code_generate() {
//     generate_code_test();
// }
//
// #[test]
// fn test_generate_all() {
//     generate_all();
// }
// }
