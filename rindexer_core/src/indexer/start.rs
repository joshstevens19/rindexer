use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;

use crate::generator::event_callback_registry::EventCallbackRegistry;
use crate::indexer::fetch_logs::{fetch_logs, fetch_logs_stream};

pub async fn start(
    registry: EventCallbackRegistry,
    max_concurrency: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_block_range = 20000000000;

    let semaphore = Arc::new(Semaphore::new(max_concurrency));

    let mut handles = Vec::new();

    for event in &registry.events {
        let latest_block = event.provider.get_block_number().await?.as_u64();
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
            let event_clone = event.clone(); // Assuming EventInformation implements Clone
            let registry_clone = registry.clone(); // Clone the registry
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            let handle = tokio::spawn(async move {
                // let logs = fetch_logs(provider_clone, filter)
                //     .await
                //     .expect("Failed to fetch logs");
                // drop(permit);
                //
                // for log in logs {
                //     let decoded = event_clone.decode_log(log);
                //     registry_clone.trigger_event(event_clone.topic_id, decoded);
                // }

                let mut logs_stream = fetch_logs_stream(provider_clone, filter);

                while let Some(logs) = logs_stream.next().await {
                    match logs {
                        Ok(logs) => {
                            for log in logs {
                                let decoded = event_clone.decode_log(log);
                                registry_clone.trigger_event(event_clone.topic_id, decoded);
                            }
                        }
                        Err(e) => {
                            eprintln!("Error fetching logs: {:?}", e);
                            break; // Optionally handle errors more gracefully
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
