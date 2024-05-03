use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use std::any::Any;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_stream::StreamExt;

use crate::generator::event_callback_registry::{EventCallbackRegistry, EventInformation};
use crate::indexer::fetch_logs::fetch_logs_stream;

pub struct ConcurrentSettings {
    /// The max amount of concurrency you want to do side by side for indexing, the higher, the faster
    /// note it will depend on your RPC client - this is based per event
    max_concurrency: usize,
}

impl ConcurrentSettings {
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self {
            max_concurrency: 100,
        }
    }
}

pub struct StartIndexingSettings {
    concurrent: Option<ConcurrentSettings>,
    /// If events rely on other event data you can set this to true,
    /// and then it does it in the order its registered
    /// default is false as it opts for the fastest processes
    execute_in_event_order: bool,
    /// If event logs you are indexing care about order you can set this to true,
    /// and then it will process it in the order the logs come in
    /// default is false as it opts for the fastest process
    execute_event_logs_in_order: bool,
}

impl StartIndexingSettings {
    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Self {
        Self {
            concurrent: Some(ConcurrentSettings::default()),
            execute_in_event_order: false,
            execute_event_logs_in_order: false,
        }
    }
}

type BoxedError = Box<dyn Error + Send + Sync>;

struct EventProcessingConfig {
    event: EventInformation,
    start_block: u64,
    end_block: u64,
    max_block_range: u64,
    semaphore: Arc<Semaphore>,
    registry: Arc<EventCallbackRegistry>,
    execute_event_logs_in_order: bool,
    live_indexing: bool,
}

pub async fn start_indexing(
    registry: Arc<EventCallbackRegistry>,
    settings: StartIndexingSettings,
) -> Result<(), BoxedError> {
    let max_block_range = 20000000000;
    let semaphore = Arc::new(Semaphore::new(
        settings
            .concurrent
            .map_or(ConcurrentSettings::default().max_concurrency, |c| {
                c.max_concurrency
            }),
    ));

    let mut handles = Vec::new();

    for event in registry.events.clone() {
        let latest_block = event.provider.get_block_number().await?.as_u64();
        for contract in &event.contract.details {
            let live_indexing = contract.end_block.is_some();
            let start_block = contract.start_block.unwrap_or(latest_block);
            let end_block = std::cmp::min(contract.end_block.unwrap_or(latest_block), latest_block);

            println!(
                "Starting event: {} from block: {} to block: {}",
                event.topic_id, start_block, end_block
            );

            let event_processing_config = EventProcessingConfig {
                event: event.clone(),
                start_block,
                end_block,
                max_block_range,
                semaphore: semaphore.clone(),
                registry: registry.clone(),
                live_indexing,
                execute_event_logs_in_order: settings.execute_event_logs_in_order,
            };

            if settings.execute_in_event_order {
                // Sequential processing of events
                process_event_sequentially(event_processing_config).await?;
            } else {
                // Concurrent processing of events
                let handle = tokio::spawn(process_event_concurrently(event_processing_config));
                handles.push(handle);
            }
        }
    }

    // Await all handles, whether they were started concurrently or sequentially
    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn process_event_sequentially(
    event_processing_config: EventProcessingConfig,
) -> Result<(), BoxedError> {
    for current_block in (event_processing_config.start_block..event_processing_config.end_block)
        .step_by(event_processing_config.max_block_range as usize)
    {
        let next_block = std::cmp::min(
            current_block + event_processing_config.max_block_range,
            event_processing_config.end_block,
        );
        let filter = build_filter(&event_processing_config.event, current_block, next_block);
        let semaphore_client = event_processing_config.semaphore.clone();
        let permit = semaphore_client.acquire_owned().await.unwrap();
        process_logs(
            event_processing_config.event.clone(),
            filter,
            event_processing_config.registry.clone(),
            event_processing_config.execute_event_logs_in_order,
            event_processing_config.live_indexing,
        )
        .await?;
        drop(permit);
    }
    Ok(())
}

async fn process_event_concurrently(
    event_processing_config: EventProcessingConfig,
) -> Result<(), BoxedError> {
    let mut handles = Vec::new();
    for current_block in (event_processing_config.start_block..event_processing_config.end_block)
        .step_by(event_processing_config.max_block_range as usize)
    {
        let next_block = std::cmp::min(
            current_block + event_processing_config.max_block_range,
            event_processing_config.end_block,
        );
        let event_clone = event_processing_config.event.clone();
        let registry_copy = event_processing_config.registry.clone();

        let filter = build_filter(&event_clone, current_block, next_block);
        let permit = event_processing_config
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let handle = tokio::spawn(async move {
            let result = process_logs(
                event_clone,
                filter,
                registry_copy,
                event_processing_config.execute_event_logs_in_order,
                event_processing_config.live_indexing,
            )
            .await;
            drop(permit);
            result
        });
        handles.push(handle);
    }

    // Await all spawned handles within the event
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

async fn process_logs(
    event: EventInformation,
    filter: Filter,
    registry: Arc<EventCallbackRegistry>,
    execute_events_logs_in_order: bool,
    live_indexing: bool,
) -> Result<(), BoxedError> {
    let provider = Arc::new(event.provider.clone());
    let mut logs_stream = fetch_logs_stream(provider, filter, live_indexing);

    if execute_events_logs_in_order {
        // Process logs in the exact order they are fetched
        while let Some(log_result) = logs_stream.next().await {
            match log_result {
                Ok(logs) => {
                    let decoded_logs = logs
                        .iter()
                        .map(|log| event.decode_log(log.clone()))
                        .collect::<Vec<Arc<dyn Any + Send + Sync>>>();

                    registry.trigger_event(event.topic_id, decoded_logs).await;
                }
                Err(e) => {
                    eprintln!("Error fetching logs: {:?}", e);
                    break;
                }
            }
        }
    } else {
        // Process logs concurrently as they arrive, without waiting for the order
        let mut handles = Vec::new();
        while let Some(log_result) = logs_stream.next().await {
            match log_result {
                Ok(logs) => {
                    for log in logs {
                        let decoded_log = event.decode_log(log);
                        let registry_clone = registry.clone();
                        let topic_id = event.topic_id;
                        // Spawn a task for each log to process concurrently
                        let handle = tokio::spawn(async move {
                            registry_clone
                                .trigger_event(topic_id, vec![decoded_log])
                                .await;
                        });
                        handles.push(handle);
                    }
                }
                Err(e) => {
                    eprintln!("Error fetching logs: {:?}", e);
                    break;
                }
            }
        }

        // Await all handles to ensure all logs are processed
        for handle in handles {
            handle.await?;
        }
    }

    Ok(())
}

fn build_filter(event: &EventInformation, current_block: u64, next_block: u64) -> Filter {
    Filter::new()
        // TODO: MULTICHAIN
        .address(
            event
                .contract
                .details
                .first()
                .unwrap()
                .address
                .parse::<Address>()
                .unwrap(),
        )
        .topic0(event.topic_id.parse::<H256>().unwrap())
        .from_block(U64::from(current_block))
        .to_block(U64::from(next_block))
}
