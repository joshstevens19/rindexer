mod rindexer;

use ethers::types::Address;
use rindexer::lens_registry_example::events::lens_registry::{
    LensRegistryEventType, NonceUpdatedEvent,
};
use rindexer_core::{
    generator::{build::build, event_callback_registry::EventCallbackRegistry},
    indexer::start::start,
};

use crate::rindexer::lens_registry_example::events::lens_registry::NonceUpdatedData;

fn main() {
    generate();
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

    let address: Address = "0xD4F2F33680FCCb36748FA9831851643781608844"
        .to_string()
        .parse()
        .unwrap();

    let hey = registry
        .events
        .get("0xc906270cebe7667882104effe64262a73c422ab9176a111e05ea837b021065fc")
        .unwrap();
    hey(&NonceUpdatedData {
        nonce: 1.into(),
        signer: address,
        timestamp: 1.into(),
    });

    start(registry);
}

fn generate() {
    build(
        "/Users/joshstevens/code/rindexer/rindexer_demo/manifest-example.yaml",
        "/Users/joshstevens/code/rindexer/rindexer_demo/src/rindexer",
    )
    .unwrap();
}
