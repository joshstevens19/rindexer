use crate::rindexer::lens_registry_example::events::lens_registry::{
    no_extensions, HandleLinkedEvent, HandleUnlinkedEvent, LensRegistryEventType, NewEventOptions,
    NonceUpdatedEvent,
};
use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;
use std::sync::Arc;

async fn handle_linked_handler(registry: &mut EventCallbackRegistry) {
    LensRegistryEventType::HandleLinked(
        HandleLinkedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("HandleLinked event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn handle_unlinked_handler(registry: &mut EventCallbackRegistry) {
    LensRegistryEventType::HandleUnlinked(
        HandleUnlinkedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("HandleUnlinked event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}

async fn nonce_updated_handler(registry: &mut EventCallbackRegistry) {
    LensRegistryEventType::NonceUpdated(
        NonceUpdatedEvent::new(
            Arc::new(|data, network, context| {
                Box::pin(async move {
                    println!("NonceUpdated event: {:?}", data);
                })
            }),
            no_extensions(),
            NewEventOptions::default(),
        )
        .await,
    )
    .register(registry);
}
pub async fn lens_registry_handlers(registry: &mut EventCallbackRegistry) {
    handle_linked_handler(registry).await;

    handle_unlinked_handler(registry).await;

    nonce_updated_handler(registry).await;
}
