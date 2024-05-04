mod indexers;
mod rindexer;

use crate::indexers::lens_registry_example::lens_hub::lens_hub_handlers;
use rindexer_core::generator::build::generate_code;
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::{start_indexing, StartIndexingSettings},
};
use std::path::PathBuf;
use std::str::FromStr;

#[tokio::main]
async fn main() {
    let mut registry = EventCallbackRegistry::new();

    lens_hub_handlers(&mut registry).await;

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
    build(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}

fn generate_code_test() {
    generate_code(
        &PathBuf::from_str("/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml")
            .unwrap(),
        "/Users/joshstevens/code/rindexer/rindexer_demo/src",
    )
    .unwrap();
}
