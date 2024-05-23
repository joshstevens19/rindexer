use std::env;
use std::path::PathBuf;
use std::str::FromStr;

use self::rindexer::indexers::all_handlers::register_all_handlers;
use rindexer_core::{
    start_rindexer, GraphQLServerDetails, IndexingDetails, StartDetails,
};

mod rindexer;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mut enable_graphql = false;
    let mut enable_indexer = false;

    for arg in args.iter() {
        match arg.as_str() {
            "--graphql" => enable_graphql = true,
            "--indexer" => enable_indexer = true,
            _ => {}
        }
    }

    let _ = start_rindexer(StartDetails {
        manifest_path: env::current_dir().unwrap().join("rindexer.yaml"),
        indexing_details: if enable_indexer {
            Some(IndexingDetails {
                registry: register_all_handlers().await,
                settings: Default::default(),
            })
        } else {
            None
        },
        graphql_server: if enable_graphql {
            Some(GraphQLServerDetails {
                settings: Default::default(),
            })
        } else {
            None
        },
    })
    .await;
}

fn generate() {
    rindexer_core::generator::build::generate_rindexer_typings(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/rindexer.yaml").unwrap(),
    )
    .unwrap();
}

fn generate_code_test() {
    rindexer_core::generator::build::generate_rindexer_handlers(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/rindexer.yaml").unwrap(),
    )
    .unwrap();
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
}
