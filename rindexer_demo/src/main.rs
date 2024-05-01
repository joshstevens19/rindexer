mod rindexer;

use std::sync::Arc;

use rindexer::lens_registry_example::{
    contexts::get_injected,
    events::lens_registry::{HandleLinkedEvent, LensRegistryEventType},
};

use crate::rindexer::lens_registry_example::events::lens_registry::NewEventOptions;
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::{start_indexing, StartIndexingSettings},
    AsyncCsvAppender, PostgresClient,
};

#[tokio::main]
async fn main() {
    // 1. Create the event callback registry
    let mut registry = EventCallbackRegistry::new();

    // 2. create postgres client
    // let postgres = Arc::new(PostgresClient::new().await.unwrap());
    //
    // let appender = AsyncCsvAppender::new("events.csv".to_string());
    // let header = vec![
    //     "Column1".to_string(),
    //     "Column2".to_string(),
    //     "Column3".to_string(),
    // ];
    // let data = vec![
    //     "Data1".to_string(),
    //     "Data2".to_string(),
    //     "Data3".to_string(),
    // ];

    // appender.append_header(header).await.unwrap();
    // appender.append(data).await.unwrap();

    // // 3. register event you wish to listen to
    // LensRegistryEventType::HandleLinked(HandleLinkedEvent {
    //     // 4. write your callback it must be thread safe
    //     callback: Arc::new(|data, client, csv| {
    //         println!("HandleLinked event: {:?}", data);
    //         // needs to wrap as a pin to use async with closure and be safely passed around
    //         Box::pin(async move {
    //             // you can grab any smart contract you mapped in the manifest here
    //             let injected_provider = get_injected();
    //
    //             // postgres!
    //             // for handle_linked_data in data {
    //             //     let handle_id = handle_linked_data.handle.id.to_string();
    //             //     client
    //             //         .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
    //             //         .await
    //             //         .unwrap();
    //             // }
    //
    //             // csv!
    //             for handle_linked_data in data {
    //                 csv.append(vec![
    //                     handle_linked_data.handle.id.to_string(),
    //                     handle_linked_data.handle.collection.to_string(),
    //                     handle_linked_data.token.id.to_string(),
    //                 ])
    //                 .await
    //                 .unwrap();
    //             }
    //         })
    //     }),
    //     client: postgres.clone(),
    //     csv: Arc::new(appender),
    // })
    // .register(&mut registry);

    // 3. register event you wish to listen to
    LensRegistryEventType::HandleLinked(
        HandleLinkedEvent::new(
            Arc::new(|data, context| {
                println!("HandleLinked event: {:?}", data);
                // needs to wrap as a pin to use async with closure and be safely passed around
                Box::pin(async move {
                    // you can grab any smart contract you mapped in the manifest here
                    let injected_provider = get_injected();

                    // postgres!
                    for handle_linked_data in data {
                        let handle_id = handle_linked_data.handle.id.to_string();
                        context
                            .client
                            .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
                            .await
                            .unwrap();
                    }

                    // csv!
                    // for handle_linked_data in data {
                    //     context
                    //         .csv
                    //         .append(vec![
                    //             handle_linked_data.handle.id.to_string(),
                    //             handle_linked_data.handle.collection.to_string(),
                    //             handle_linked_data.token.id.to_string(),
                    //         ])
                    //         .await
                    //         .unwrap();
                    // }
                })
            }),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(&mut registry);

    let _ = start_indexing(registry, StartIndexingSettings::default()).await;
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
        "/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml",
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}
