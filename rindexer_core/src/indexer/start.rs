use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use std::any::Any;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;

use crate::generator::event_callback_registry::EventCallbackRegistry;
use crate::indexer::fetch_logs::fetch_logs_stream;

pub struct ConcurrentSettings {
    /// The max amount of concurrency you want to do side by side for indexing, the higher, the faster
    /// note it will depend on your RPC client - this is based per event
    max_concurrency: usize,
}

impl ConcurrentSettings {
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self { max_concurrency: 100 }
    }
}


pub struct StartIndexingSettings {
    /// If events  rely on other event data you can set this to true,
    /// and it does it in the order its registered
    /// default is false as it opts for the fastest processes
    index_in_event_registry_order: bool,
    concurrent: Option<ConcurrentSettings>,
}

impl StartIndexingSettings {
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self { index_in_event_registry_order: false, concurrent: Some(ConcurrentSettings::default()) }
    }
}

pub async fn start_indexing(
    registry: EventCallbackRegistry,
    settings: StartIndexingSettings
) -> Result<(), Box<dyn std::error::Error>> {
    let max_block_range = 20000000000;

    let semaphore = if let Some(concurrent_settings) = settings.concurrent {
        Arc::new(Semaphore::new(concurrent_settings.max_concurrency))
    } else {
        Arc::new(Semaphore::new(ConcurrentSettings::default().max_concurrency))
    };

    let mut handles = Vec::new();

    for event in &registry.events {
        let latest_block = event.provider.get_block_number().await?.as_u64();
        let live_indexing = event.source.end_block.is_some();
        let start_block = event.source.start_block.unwrap_or(latest_block);
        let mut end_block = event.source.end_block.unwrap_or(latest_block);
        if end_block > latest_block {
            end_block = latest_block;
        }

        println!(
            "Starting event: {} from block: {} to block: {}",
            event.topic_id, start_block, end_block
        );

        for current_block in (start_block..end_block).step_by(max_block_range as usize) {
            let next_block = std::cmp::min(current_block + max_block_range, end_block);
            let filter = Filter::new()
                .address(event.source.address.parse::<Address>()?)
                .topic0(event.topic_id.parse::<H256>()?)
                .from_block(U64::from(current_block))
                .to_block(U64::from(next_block));

            println!("current_block: {:?}", current_block);
            println!("next_block: {:?}", next_block);

            let provider_clone = Arc::new(event.provider.clone());
            let event_clone = event.clone();
            let registry_clone = registry.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            let handle = tokio::spawn(async move {
                let mut logs_stream = fetch_logs_stream(provider_clone, filter, live_indexing);

                while let Some(logs) = logs_stream.next().await {
                    match logs {
                        Ok(logs) => {
                            println!("start_indexing::Fetched logs: {:?}", logs.len());
                            let decoded_logs = logs
                                .iter()
                                .map(|log| event_clone.decode_log(log.clone()))
                                .collect::<Vec<Arc<dyn Any + Send + Sync>>>();

                            registry_clone
                                .trigger_event(event_clone.topic_id, decoded_logs)
                                .await;
                        }
                        Err(e) => {
                            eprintln!("Error fetching logs: {:?}", e);
                            break;
                        }
                    }
                }
                drop(permit);
            });

            handles.push(handle);
        }
    }

    for handle in handles {
        handle.await.expect("Task failed");
    }

    Ok(())
}
