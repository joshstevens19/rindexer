mod rindexer;

use ethers::types::Address;
use rindexer::lens_registry_example::events::lens_registry::{
    LensRegistryEventType, NonceUpdatedEvent,
};
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start,
};

use crate::rindexer::lens_registry_example::{events::lens_registry::NonceUpdatedData, contexts::blah};

fn main() {
    
    blah();

    // generate();
    let mut registry = EventCallbackRegistry::new();

    let event_type = LensRegistryEventType::NonceUpdated(NonceUpdatedEvent {
        callback: Box::new(|data| {
            // Handle the event using data
            println!("NonceUpdated event: {:?}", data);
        }),
    });

    // Register the event using the RindexerEventType
    event_type.register(&mut registry);

    // Triggering an event...
    println!("hey {}", registry.events.len());

    let hey = registry
        .find_event("0xc906270cebe7667882104effe64262a73c422ab9176a111e05ea837b021065fc")
        .unwrap();
    println!("hey {:?}", (&hey.source, &hey.topic_id));

    start(registry);
}

fn generate() {
    build(
        "/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml",
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}
