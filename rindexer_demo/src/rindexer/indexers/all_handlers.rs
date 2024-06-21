use super::blah_baby::erc20_filter::erc20_filter_handlers;
use rindexer_core::generator::event_callback_registry::EventCallbackRegistry;

pub async fn register_all_handlers() -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    erc20_filter_handlers(&mut registry).await;
    registry
}
