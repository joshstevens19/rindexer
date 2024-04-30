mod rindexer;

use std::sync::Arc;

use rindexer::lens_registry_example::{
    contexts::get_injected,
    events::lens_registry::{HandleLinkedEvent, LensRegistryEventType},
};

use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start_indexing,
    PostgresClient,
};

#[tokio::main]
async fn main() {
    // 1. Create the event callback registry
    let mut registry = EventCallbackRegistry::new();

    // 2. create postgres client
    let postgres = Arc::new(PostgresClient::new().await.unwrap());

    // 3. register event you wish to listen to
    LensRegistryEventType::HandleLinked(HandleLinkedEvent {
        // 4. write your callback it must be thread safe
        callback: Arc::new(|data, client| {
            println!("HandleLinked event: {:?}", data);
            // needs to wrap as a pin to use async with closure and be safely passed around
            Box::pin(async move {
                // you can grab any smart contract you mapped in the manifest here
                let injected_provider = get_injected();

                for handle_linked_data in data {
                    let handle_id = handle_linked_data.handle.id.to_string();
                    client
                        .execute("INSERT INTO hello VALUES($1)", &[&handle_id])
                        .await
                        .unwrap();
                }
            })
        }),
        client: postgres.clone(),
    })
    .register(&mut registry);

    let _ = start_indexing(registry, 100).await;
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
