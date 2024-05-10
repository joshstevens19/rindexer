mod indexers;
mod rindexer;

// use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
//use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;
//use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
//use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;
use crate::indexers::lens_registry_example::erc20_filter::erc20_filter_handlers;
use rindexer_core::generator::build::generate_indexers_handlers_code;
use rindexer_core::manifest::yaml::read_manifest;
use rindexer_core::{
    create_tables_for_indexer_sql,
    generator::{build::generate_rindexer_code, event_callback_registry::EventCallbackRegistry},
    indexer::start::{start_indexing, StartIndexingSettings},
    PostgresClient,
};
use std::path::PathBuf;
use std::str::FromStr;
//use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;

#[tokio::main]
async fn main() {
    // generate();
    let mut registry = EventCallbackRegistry::new();

    let manifest = read_manifest(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
    )
    .unwrap();

    let client = PostgresClient::new().await.unwrap();

    for indexer in manifest.indexers {
        let sql = create_tables_for_indexer_sql(&indexer);
        println!("{}", sql);
        client.batch_execute(&sql).await.unwrap();
    }

    //lens_registry_handlers(&mut registry).await;
    // lens_hub_handlers(&mut registry).await;
    erc20_filter_handlers(&mut registry).await;

    let _ = start_indexing(registry.complete(), StartIndexingSettings::default()).await;
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

fn generate() {
    generate_rindexer_code(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}

fn generate_code_test() {
    generate_indexers_handlers_code(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        "/Users/joshstevens/code/rindexer/rindexer_demo/src",
    )
    .unwrap();
}
