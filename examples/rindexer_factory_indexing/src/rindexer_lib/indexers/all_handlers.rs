use super::rindexer_factory_contract::uniswap_v3_factory::uniswap_v3_factory_handlers;
use super::rindexer_factory_contract::uniswap_v3_factory_pool_createdpool::uniswap_v3_factory_pool_createdpool_handlers;
use super::rindexer_factory_contract::uniswap_v3_pool::uniswap_v3_pool_handlers;
use rindexer::event::callback_registry::EventCallbackRegistry;
use std::path::PathBuf;

pub async fn register_all_handlers(manifest_path: &PathBuf) -> EventCallbackRegistry {
    let mut registry = EventCallbackRegistry::new();
    uniswap_v3_factory_handlers(manifest_path, &mut registry).await;
    uniswap_v3_factory_pool_createdpool_handlers(manifest_path, &mut registry).await;
    uniswap_v3_pool_handlers(manifest_path, &mut registry).await;
    registry
}
