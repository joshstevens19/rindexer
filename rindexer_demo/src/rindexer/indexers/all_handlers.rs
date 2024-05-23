use super::lens_registry_example::erc20_filter::erc20_filter_handlers;
use super::lens_registry_example::lens_registry::lens_registry_handlers;
use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;

pub async fn register_all_handlers() -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    lens_registry_handlers(&mut registry).await;
    erc20_filter_handlers(&mut registry).await;
    registry
}
