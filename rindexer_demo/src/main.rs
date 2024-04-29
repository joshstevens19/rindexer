mod rindexer;

use std::sync::Arc;

use rindexer::lens_registry_example::{
    contexts::get_injected,
    events::lens_registry::{
        HandleLinkedEvent, HandleUnlinkedEvent, LensRegistryEventType, NonceUpdatedEvent,
    },
};

use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start,
};

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

    // LensRegistryEventType::HandleUnlinked(HandleUnlinkedEvent {
    //     callback: Arc::new(|data| {
    //         println!("HandleUnlinked event: {:?}", data);
    //     }),
    // })
    // .register(&mut registry);

    LensRegistryEventType::HandleLinked(HandleLinkedEvent {
        callback: Arc::new(|data| {
            println!("HandleLinked event: {:?}", data);
        }),
    })
    .register(&mut registry);

    let result = start(registry, 100).await;

    println!("{:?}", result);
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
