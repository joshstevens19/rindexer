use ethers::providers::ProviderError;
use ethers::{
    providers::Middleware,
    types::{Address, Filter, H256, U64},
};
use rust_decimal::Decimal;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio_stream::StreamExt;

use crate::generator::event_callback_registry::{
    AddressOrFilter, EventCallbackRegistry, EventResult, NetworkContract,
};
use crate::helpers::camel_to_snake;
use crate::indexer::fetch_logs::{fetch_logs_stream, FetchLogsStream, LiveIndexingDetails};
use crate::indexer::progress::IndexingEventsProgressState;
use crate::indexer::reorg::reorg_safe_distance_for_chain;
use crate::{EthereumSqlTypeWrapper, PostgresClient};

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
    indexer_name: &'static str,
    topic_id: &'static str,
    event_name: &'static str,
    network_contract: Arc<NetworkContract>,
    start_block: U64,
    end_block: U64,
    max_block_range: u64,
    semaphore: Arc<Semaphore>,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Arc<PostgresClient>,
    execute_event_logs_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
}

pub async fn start_indexing(
    registry: Arc<EventCallbackRegistry>,
    settings: StartIndexingSettings,
) -> Result<(), BoxedError> {
    let database = Arc::new(PostgresClient::new().await.unwrap());

    let event_progress_state = IndexingEventsProgressState::monitor(registry.events.clone()).await;

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
        for contract in event.contract.details.clone() {
            let latest_block = contract.provider.get_block_number().await?;
            let live_indexing = contract.end_block.is_none();
            let last_known_start_block = get_last_synced_block_number(
                database.clone(),
                event.indexer_name,
                event.event_name,
                &contract.network,
            )
            .await;

            let start_block =
                last_known_start_block.unwrap_or(contract.start_block.unwrap_or(latest_block));
            // println!(
            //     "Starting event: {} from block: {} to block: {}",
            //     event.topic_id, start_block2, latest_block
            // );

            // handle reorg block
            let mut indexing_distance_from_head = U64::zero();
            let mut end_block =
                std::cmp::min(contract.end_block.unwrap_or(latest_block), latest_block);
            if event.contract.reorg_safe_distance {
                let chain_id = contract.provider.get_chainid().await?;
                let reorg_safe_distance = reorg_safe_distance_for_chain(&chain_id);
                let safe_block_number = latest_block - reorg_safe_distance;
                if end_block > safe_block_number {
                    end_block = safe_block_number
                }
                indexing_distance_from_head = reorg_safe_distance;
            }

            // println!(
            //     "Starting event: {} from block: {} to block: {}",
            //     event.topic_id, start_block, end_block
            // );

            let event_processing_config = EventProcessingConfig {
                indexer_name: event.indexer_name,
                topic_id: event.topic_id,
                event_name: event.event_name,
                network_contract: Arc::new(contract),
                start_block,
                end_block,
                max_block_range,
                semaphore: semaphore.clone(),
                registry: registry.clone(),
                progress: event_progress_state.clone(),
                database: database.clone(),
                live_indexing,
                execute_event_logs_in_order: settings.execute_event_logs_in_order,
                indexing_distance_from_head,
            };

            if settings.execute_in_event_order {
                process_event_sequentially(event_processing_config).await?;
            } else {
                let handle = tokio::spawn(process_event_concurrently(event_processing_config));
                handles.push(handle);
            }
        }
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn process_event_sequentially(
    event_processing_config: EventProcessingConfig,
) -> Result<(), BoxedError> {
    for _current_block in (event_processing_config.start_block.as_u64()
        ..event_processing_config.end_block.as_u64())
        .step_by(event_processing_config.max_block_range as usize)
    {
        let current_block = U64::from(_current_block);
        let next_block = std::cmp::min(
            current_block + event_processing_config.max_block_range,
            event_processing_config.end_block,
        );

        let filter = build_filter(
            event_processing_config.topic_id,
            &event_processing_config.network_contract.address_or_filter,
            current_block,
            next_block,
        );
        let semaphore_client = event_processing_config.semaphore.clone();
        let permit = semaphore_client.acquire_owned().await.unwrap();
        process_logs(ProcessLogsParams {
            indexer_name: event_processing_config.indexer_name,
            topic_id: event_processing_config.topic_id,
            event_name: event_processing_config.event_name,
            network_contract: event_processing_config.network_contract.clone(),
            filter,
            registry: event_processing_config.registry.clone(),
            progress: event_processing_config.progress.clone(),
            database: event_processing_config.database.clone(),
            execute_events_logs_in_order: event_processing_config.execute_event_logs_in_order,
            live_indexing: event_processing_config.live_indexing,
            indexing_distance_from_head: event_processing_config.indexing_distance_from_head,
        })
        .await?;
        drop(permit);
    }
    Ok(())
}

async fn process_event_concurrently(
    event_processing_config: EventProcessingConfig,
) -> Result<(), BoxedError> {
    let mut handles = Vec::new();
    for _current_block in (event_processing_config.start_block.as_u64()
        ..event_processing_config.end_block.as_u64())
        .step_by(event_processing_config.max_block_range as usize)
    {
        let current_block = U64::from(_current_block);
        let next_block = std::cmp::min(
            current_block + event_processing_config.max_block_range,
            event_processing_config.end_block,
        );

        let filter = build_filter(
            event_processing_config.topic_id,
            &event_processing_config.network_contract.address_or_filter,
            current_block,
            next_block,
        );

        let registry_copy = event_processing_config.registry.clone();
        let permit = event_processing_config
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let handle = tokio::spawn({
            let network_contract = event_processing_config.network_contract.clone();
            let progress = event_processing_config.progress.clone();
            let database = event_processing_config.database.clone();
            async move {
                let result = process_logs(ProcessLogsParams {
                    indexer_name: event_processing_config.indexer_name,
                    topic_id: event_processing_config.topic_id,
                    event_name: event_processing_config.event_name,
                    network_contract: network_contract.clone(),
                    filter,
                    registry: registry_copy,
                    progress,
                    database,
                    execute_events_logs_in_order: event_processing_config
                        .execute_event_logs_in_order,
                    live_indexing: event_processing_config.live_indexing,
                    indexing_distance_from_head: event_processing_config
                        .indexing_distance_from_head,
                })
                .await;
                drop(permit);
                result
            }
        });
        handles.push(handle);
    }

    // Await all spawned handles within the event
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

#[derive(Clone)]
pub struct ProcessLogsParams {
    indexer_name: &'static str,
    topic_id: &'static str,
    event_name: &'static str,
    network_contract: Arc<NetworkContract>,
    filter: Filter,
    registry: Arc<EventCallbackRegistry>,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    database: Arc<PostgresClient>,
    execute_events_logs_in_order: bool,
    live_indexing: bool,
    indexing_distance_from_head: U64,
}

async fn process_logs(params: ProcessLogsParams) -> Result<(), BoxedError> {
    let provider = Arc::new(params.network_contract.provider.clone());
    let mut logs_stream = fetch_logs_stream(
        provider,
        params.filter,
        if params.live_indexing {
            Some(LiveIndexingDetails {
                indexing_distance_from_head: params.indexing_distance_from_head,
            })
        } else {
            None
        },
    );

    while let Some(result) = logs_stream.next().await {
        handle_logs_result(
            params.indexer_name,
            params.event_name,
            params.topic_id,
            params.execute_events_logs_in_order,
            params.progress.clone(),
            params.network_contract.clone(),
            params.database.clone(),
            params.registry.clone(),
            result,
        )
        .await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_logs_result(
    indexer_name: &'static str,
    event_name: &'static str,
    topic_id: &'static str,
    execute_events_logs_in_order: bool,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Arc<PostgresClient>,
    registry: Arc<EventCallbackRegistry>,
    result: Result<FetchLogsStream, Box<ProviderError>>,
) -> Result<(), BoxedError> {
    match result {
        Ok(result) => {
            let fn_data = result
                .logs
                .iter()
                .map(|log| EventResult::new(network_contract.clone(), log))
                .collect::<Vec<_>>();

            if !fn_data.is_empty() {
                if execute_events_logs_in_order {
                    registry.trigger_event(topic_id, fn_data).await;
                } else {
                    tokio::spawn(async move {
                        registry.trigger_event(topic_id, fn_data).await;
                    });
                }
            }
            update_progress_and_db(
                indexer_name,
                event_name,
                progress,
                network_contract,
                database,
                result.to_block,
            );

            Ok(())
        }
        Err(e) => {
            eprintln!("Error fetching logs: {:?}", e);
            Err(e)
        }
    }
}

async fn get_last_synced_block_number(
    database: Arc<PostgresClient>,
    indexer_name: &'static str,
    event_name: &'static str,
    network: &str,
) -> Option<U64> {
    let query = format!(
        "SELECT last_synced_block FROM rindexer_internal.{}_{} WHERE network = $1",
        camel_to_snake(indexer_name),
        camel_to_snake(event_name)
    );

    let row = database.query_one(&query, &[&network]).await;
    match row {
        Ok(row) => {
            let result: Decimal = row.get("last_synced_block");
            Some(U64::from_dec_str(&result.to_string()).unwrap())
        }
        Err(e) => {
            eprintln!("Error fetching last synced block: {:?}", e);
            None
        }
    }
}

fn update_progress_and_db(
    indexer_name: &'static str,
    event_name: &'static str,
    progress: Arc<Mutex<IndexingEventsProgressState>>,
    network_contract: Arc<NetworkContract>,
    database: Arc<PostgresClient>,
    to_block: U64,
) {
    tokio::spawn(async move {
        progress
            .lock()
            .await
            .update_last_synced_block(&network_contract.id, to_block);

        database
            .execute(
                &format!(
                    "UPDATE rindexer_internal.{}_{} SET last_synced_block = $1 WHERE network = $2 AND $1 > last_synced_block",
                    camel_to_snake(indexer_name),
                    camel_to_snake(event_name)
                ),
                &[
                    &EthereumSqlTypeWrapper::U64(&to_block),
                    &network_contract.network,
                ],
            )
            .await
            .unwrap();
    });
}

fn build_filter(
    topic_id: &'static str,
    address_or_filter: &AddressOrFilter,
    current_block: U64,
    next_block: U64,
) -> Filter {
    match address_or_filter {
        AddressOrFilter::Address(address) => Filter::new()
            .address(address.parse::<Address>().unwrap())
            .topic0(topic_id.parse::<H256>().unwrap())
            .from_block(current_block)
            .to_block(next_block),
        AddressOrFilter::Filter(filter) => filter.extend_filter_indexed(
            Filter::new()
                .topic0(topic_id.parse::<H256>().unwrap())
                .from_block(current_block)
                .to_block(next_block),
        ),
    }
}
