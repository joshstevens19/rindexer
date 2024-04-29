mod rindexer;

use std::sync::Arc;

use rindexer::lens_registry_example::{
    contexts::get_injected,
    events::lens_registry::{HandleLinkedEvent, HandleUnlinkedEvent, LensRegistryEventType},
};

use crate::rindexer::lens_registry_example::events::lens_registry::HandleLinkedData;
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start_indexing,
    PostgresClient,
};

// // Macro to create event handlers and register them
// macro_rules! event_handler {
//     ($handler:ident<$data:ty>, $data:expr, $body:block, $registry:expr) => {{
//         let callback = move |data: &$data| async move $body;
//         let event = Event {
//             callback: Arc::new(callback),
//         };
//         $registry.register(event);
//         event
//     }};
// }

#[tokio::main]
async fn main() {
    // generate();

    let mut registry = EventCallbackRegistry::new();

    // LensRegistryEventType::NonceUpdated(NonceUpdatedEvent {
    //     callback: Arc::new(|data| {
    //         println!("NonceUpdated event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleLinked(HandleLinkedEvent {
    //     callback: Arc::new(|data| {
    //         println!("HandleLinked event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleLinked(HandleLinkedEvent {
    //     callback: Arc::new(|data| {
    //         let fut = async move {
    //             // Your asynchronous callback implementation
    //             println!("HandleLinked event: {:?}", data);
    //         };
    //         Box::pin(fut)
    //     }),
    // })
    // .register(&mut registry);

    // LensRegistryEventType::HandleUnlinked(HandleUnlinkedEvent {
    //     callback: Arc::new(|data| {
    //         println!("HandleUnlinked event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    // Create your HandleLinkedEvent instance with its callback function using the macro
    // let handle_linked_event = event_handler!(handle_linked_event<HandleLinkedData>, {
    //     // Your asynchronous callback implementation
    //     // Simulated async operation (e.g., database insertion)
    //     tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    //     println!("HandleLinked event: {:?}", data);
    // }, &mut registry);

    LensRegistryEventType::HandleLinked(HandleLinkedEvent {
        callback: Arc::new(|data| {
            Box::pin(async move {
                // Your asynchronous callback implementation
                // Simulated async operation (e.g., database insertion)
                // tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                println!("HandleLinked event: {:?}", data);
            })
        }),
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
