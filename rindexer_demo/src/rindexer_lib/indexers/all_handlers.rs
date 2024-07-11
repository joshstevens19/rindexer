use super::blah_baby::erc20_filter::erc20_filter_handlers;
use rindexer::event::callback_registry::EventCallbackRegistry;
use std::path::PathBuf;

pub async fn register_all_handlers(manifest_path: &PathBuf) -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    erc20_filter_handlers(manifest_path, &mut registry).await;
    registry
}
