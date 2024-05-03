mod rindexer;

use ethers::types::Address;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use rindexer::lens_registry_example::events::lens_registry::{
    HandleLinkedEvent, LensRegistryEventType,
};

use crate::rindexer::global_contracts::get_injected_global;
use crate::rindexer::lens_registry_example::events::lens_registry::{
    no_extensions, EventContext, NewEventOptions,
};
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::{start_indexing, StartIndexingSettings},
    AsyncCsvAppender, PostgresClient,
};

pub struct HeyBaby {
    pub bobby: bool,
}

#[tokio::main]
async fn main() {
    // 1. Create the event callback registry
    let mut registry = EventCallbackRegistry::new();

    // 2. register event you wish to listen to
    LensRegistryEventType::HandleLinked(
        HandleLinkedEvent::new(
            // 4. write your callback it must be in an Arc
            Arc::new(|data, context| {
                println!("HandleLinked event: {:?}", data);
                // needs to wrap as a pin to use async with closure and be safely passed around
                Box::pin(async move {
                    // you can grab any smart contract you mapped in the manifest here
                    let injected_provider = get_injected_global();
                    // let state = injected_provider.get_state().await.unwrap();

                    // you can write data to your postgres
                    for handle_linked_data in data {
                        let handle_id = handle_linked_data.handle.id.to_string();
                        context
                            .client
                            .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
                            .await
                            .unwrap();
                    }

                    // context.csv - you can use this write csvs
                    // context.extensions - you can use this to pass any context you wish over
                })
            }),
            no_extensions(), // HeyBaby { bobby: true },
            NewEventOptions::default(),
        )
        .await,
    )
    .register(&mut registry);

    let _ = start_indexing(registry.complete(), StartIndexingSettings::default()).await;

    // // 2. register event you wish to listen to
    // LensRegistryEventType::HandleLinked(
    //     HandleLinkedEvent::new(
    //         // 4. write your callback it must be in an Arc
    //         Arc::new(|data, context| {
    //             println!("HandleLinked event: {:?}", data);
    //             // needs to wrap as a pin to use async with closure and be safely passed around
    //             Box::pin(async move {
    //                 // you can grab any smart contract you mapped in the manifest here
    //                 let injected_provider = get_injected();
    //                 let state = injected_provider.get_state().await.unwrap();
    //
    //                 // postgres!
    //                 // for handle_linked_data in data {
    //                 //     let handle_id = handle_linked_data.handle.id.to_string();
    //                 //     context
    //                 //         .client
    //                 //         .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
    //                 //         .await
    //                 //         .unwrap();
    //                 // }
    //
    //                 // you can do a SQL query or an write here using execute
    //                 let query = context.client.query("SELECT 1", &[]).await.unwrap();
    //                 println!("HandleLinked postgres hit: {:?}", query);
    //
    //                 // let bob = &context.extensions.bobby;
    //
    //                 // println!("${:?}", bob)
    //
    //                 // csv!
    //                 // for handle_linked_data in data {
    //                 //     context
    //                 //         .csv
    //                 //         .append(vec![
    //                 //             handle_linked_data.handle.id.to_string(),
    //                 //             handle_linked_data.handle.collection.to_string(),
    //                 //             handle_linked_data.token.id.to_string(),
    //                 //         ])
    //                 //         .await
    //                 //         .unwrap();
    //                 // }
    //
    //                 // postgres!
    //                 // for handle_linked_data in data {
    //                 //     let handle_id = handle_linked_data.handle.id.to_string();
    //                 //     context
    //                 //         .client
    //                 //         .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
    //                 //         .await
    //                 //         .unwrap();
    //                 // }
    //             })
    //         }),
    //         no_extensions(), // HeyBaby { bobby: true },
    //         NewEventOptions::default(),
    //     )
    //         .await,
    // )
    //     .register(&mut registry);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate() {
        generate();
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
