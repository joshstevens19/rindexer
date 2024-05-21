// mod indexers;
mod rindexer;

// use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
//use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;
//use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
//use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;
// use crate::indexers::lens_registry_example::erc20_filter::erc20_filter_handlers;
// use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
// use crate::indexers::lens_registry_example::lens_registry::lens_registry_handlers;
use rindexer_core::generator::build::generate_indexers_handlers_code;
use rindexer_core::manifest::yaml::read_manifest;
use rindexer_core::{
    create_tables_for_indexer_sql,
    generator::{build::generate_rindexer_code, event_callback_registry::EventCallbackRegistry},
    indexer::start::{start_indexing, StartIndexingSettings},
    start_graphql_server, PostgresClient,
};
use std::path::PathBuf;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

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

    for indexer in &manifest.indexers {
        let sql = create_tables_for_indexer_sql(indexer);
        println!("{}", sql);
        client.batch_execute(&sql).await.unwrap();
    }

    // lens_registry_handlers(&mut registry).await;
    //lens_hub_handlers(&mut registry).await;
    //erc20_filter_handlers(&mut registry).await;

    let result = start_graphql_server(&manifest.indexers, Default::default()).unwrap();
    thread::sleep(Duration::from_secs(5000000000000000000));

    // let _ = start_indexing(registry.complete(), StartIndexingSettings::default()).await;
}

fn generate() {
    generate_rindexer_code(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        None,
    )
    .unwrap();
}

fn generate_code_test() {
    generate_indexers_handlers_code(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        None,
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
