mod rindexer;

use rindexer::lens_registry_example::events::lens_registry::{
    LensRegistryEventType, NonceUpdatedEvent,
};

use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start,
};

#[tokio::main]
async fn main() {
    //generate();

    let mut registry = EventCallbackRegistry::new();

    let event_type = LensRegistryEventType::NonceUpdated(NonceUpdatedEvent {
        callback: Box::new(|data| {
            // Handle the event using data
            println!("NonceUpdated event: {:?}", data);
        }),
    });

    // Register the event using the RindexerEventType
    event_type.register(&mut registry);

    start(registry).await
}

fn generate() {
    build(
        "/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml",
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}
