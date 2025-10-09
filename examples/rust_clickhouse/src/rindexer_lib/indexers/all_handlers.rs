use super::clickhouse_indexer::rocket_pool::rocket_pool_handlers;
use rindexer::event::callback_registry::EventCallbackRegistry;
use std::path::PathBuf;

pub async fn register_all_handlers(manifest_path: &PathBuf) -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    rocket_pool_handlers(manifest_path, &mut registry).await;
    registry
}
