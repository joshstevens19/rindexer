use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex as StdMutex},
    time::Instant,
};

use alloy::primitives::B256;
use aws_sdk_sns::{config::http::HttpResponse, error::SdkError, operation::publish::PublishError};
use futures::future::join_all;
use serde_json::{json, Value};
use thiserror::Error;
use tokio::{
    sync::Mutex as AsyncMutex,
    task,
    task::{JoinError, JoinHandle},
};
use tracing::warn;

use crate::{
    event::{filter_event_data_by_conditions, EventMessage},
    indexer::native_transfer::EVENT_NAME,
    indexer::reorg::AffectedTable,
    manifest::stream::{
        CloudflareQueuesStreamConfig, CloudflareQueuesStreamQueueConfig, RabbitMQStreamConfig,
        RabbitMQStreamQueueConfig, RedisStreamConfig, RedisStreamStreamConfig,
        SNSStreamTopicConfig, StreamEvent, StreamsConfig, WebhookStreamConfig,
    },
    metrics::streams::{self as stream_metrics, stream_type},
    streams::{
        CloudflareQueues, CloudflareQueuesError, RabbitMQ, RabbitMQError, Redis, RedisError,
        Webhook, WebhookError, SNS,
    },
};

#[cfg(feature = "kafka")]
use crate::{
    manifest::stream::{KafkaStreamConfig, KafkaStreamQueueConfig},
    streams::kafka::{Kafka, KafkaError},
};

// we should limit the max chunk size we send over when streaming to 70KB - 100KB is most limits
// we can add this to yaml if people need it
const MAX_CHUNK_SIZE: usize = 75 * 1024; // 75 KB

/// Soft cap on how many events a single `(stream_type, config, network, event)`
/// finalized buffer can hold before we warn + record a metric. Steady-state
/// memory is bounded by the flush rate (one block every ~`reorg_safe_distance`
/// blocks); sustained growth past this cap means something is stuck.
const FINALIZED_BUFFER_SOFT_CAP: usize = 10_000;

type StreamPublishes = Vec<JoinHandle<Result<usize, StreamError>>>;

#[derive(Debug, Clone)]
struct SNSStream {
    config: Vec<SNSStreamTopicConfig>,
    client: Arc<SNS>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum StreamError {
    #[error("SNS could not publish - {0}")]
    SnsCouldNotPublish(#[from] SdkError<PublishError, HttpResponse>),

    #[error("Webhook could not publish: {0}")]
    WebhookCouldNotPublish(#[from] WebhookError),

    #[error("RabbitMQ could not publish: {0}")]
    RabbitMQCouldNotPublish(#[from] RabbitMQError),

    #[cfg(feature = "kafka")]
    #[error("Kafka could not publish: {0}")]
    KafkaCouldNotPublish(#[from] KafkaError),

    #[error("Redis could not publish: {0}")]
    RedisCouldNotPublish(#[from] RedisError),

    #[error("Cloudflare Queues could not publish: {0}")]
    CloudflareQueuesCouldNotPublish(#[from] CloudflareQueuesError),

    #[error("Task failed: {0}")]
    JoinError(JoinError),
}

#[derive(Debug, Clone)]
struct WebhookStream {
    config: Vec<WebhookStreamConfig>,
    client: Arc<Webhook>,
}

#[derive(Debug)]
pub struct RabbitMQStream {
    config: RabbitMQStreamConfig,
    client: Arc<RabbitMQ>,
}

#[cfg(feature = "kafka")]
#[derive(Debug)]
pub struct KafkaStream {
    config: KafkaStreamConfig,
    client: Arc<Kafka>,
}

#[derive(Debug)]
pub struct RedisStream {
    config: RedisStreamConfig,
    client: Arc<Redis>,
}

#[derive(Debug)]
pub struct CloudflareQueuesStream {
    config: CloudflareQueuesStreamConfig,
    client: Arc<CloudflareQueues>,
}

/// Key into the finalized-delivery buffer map. One `FinalizedBuffer` exists per
/// `(stream_type, config_index, network, event_name)` tuple. The event
/// signature hash rides alongside so we can rebuild faithful `EventMessage`s at
/// flush time without re-deriving it from the ABI.
#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(crate) struct BufferKey {
    stream_type: &'static str,
    config_index: usize,
    network: String,
    event_name: String,
    event_signature_hash: B256,
}

#[derive(Debug)]
pub struct StreamsClients {
    sns: Option<SNSStream>,
    webhook: Option<WebhookStream>,
    rabbitmq: Option<RabbitMQStream>,
    #[cfg(feature = "kafka")]
    kafka: Option<KafkaStream>,
    redis: Option<RedisStream>,
    cloudflare_queues: Option<CloudflareQueuesStream>,
    /// Per-(stream-type, config-index, network, event) buffers for
    /// `StreamDeliveryMode::Finalized`. `stream_with_mode` appends here when
    /// `delivery: finalized` is set; the `ReorgCoordinator` drives draining via
    /// `flush_finalized` and invalidation via `discard_finalized`.
    finalized_buffers: AsyncMutex<HashMap<BufferKey, FinalizedBuffer>>,
    /// Per-network `reorg_safe_distance`. Populated at startup by
    /// `register_network_reorg_distance`. `FinalizedBuffer::new` needs this to
    /// know how far behind the chain head a block must be before flushing.
    reorg_safe_distances: StdMutex<HashMap<String, u64>>,
}

type FinalizedDeliveryBuffer = (BufferKey, Vec<(u64, Vec<Value>)>);

impl StreamsClients {
    pub async fn new(stream_config: StreamsConfig) -> Self {
        #[allow(clippy::manual_map)]
        let sns = if let Some(config) = &stream_config.sns {
            Some(SNSStream {
                config: config.topics.clone(),
                client: Arc::new(SNS::new(&config.aws_config).await),
            })
        } else {
            None
        };

        #[allow(clippy::manual_map)]
        let webhook = stream_config.webhooks.as_ref().map(|config| WebhookStream {
            config: config.clone(),
            client: Arc::new(Webhook::new()),
        });

        #[allow(clippy::manual_map)]
        let rabbitmq = if let Some(config) = stream_config.rabbitmq.as_ref() {
            Some(RabbitMQStream {
                config: config.clone(),
                client: Arc::new(RabbitMQ::new(&config.url).await),
            })
        } else {
            None
        };

        #[cfg(feature = "kafka")]
        #[allow(clippy::manual_map)]
        let kafka = if let Some(config) = stream_config.kafka.as_ref() {
            Some(KafkaStream {
                config: config.clone(),
                client: Arc::new(
                    Kafka::new(config)
                        .await
                        .unwrap_or_else(|e| panic!("Failed to create Kafka client: {e:?}")),
                ),
            })
        } else {
            None
        };

        #[allow(clippy::manual_map)]
        let redis = if let Some(config) = stream_config.redis.as_ref() {
            Some(RedisStream {
                config: config.clone(),
                client: Arc::new(
                    Redis::new(config)
                        .await
                        .unwrap_or_else(|e| panic!("Failed to create Redis client: {e:?}")),
                ),
            })
        } else {
            None
        };

        #[allow(clippy::manual_map)]
        let cloudflare_queues = if let Some(config) = stream_config.cloudflare_queues.as_ref() {
            Some(CloudflareQueuesStream {
                config: config.clone(),
                client: Arc::new(CloudflareQueues::new(
                    config.api_token.clone(),
                    config.account_id.clone(),
                )),
            })
        } else {
            None
        };

        Self {
            sns,
            webhook,
            rabbitmq,
            #[cfg(feature = "kafka")]
            kafka,
            redis,
            cloudflare_queues,
            finalized_buffers: AsyncMutex::new(HashMap::new()),
            reorg_safe_distances: StdMutex::new(HashMap::new()),
        }
    }

    /// Register the `reorg_safe_distance` to use for any `Finalized` buffer on
    /// this network. Must be called before the first finalized event on the
    /// network is buffered — otherwise a buffer gets created with a default
    /// distance of 0 (always-safe) and would flush immediately. `start.rs`
    /// calls this for every live-indexed network at coordinator construction.
    pub fn register_network_reorg_distance(&self, network: String, distance: u64) {
        let mut map = self.reorg_safe_distances.lock().expect("reorg_safe_distances poisoned");
        map.insert(network, distance);
    }

    fn reorg_safe_distance_for(&self, network: &str) -> u64 {
        let map = self.reorg_safe_distances.lock().expect("reorg_safe_distances poisoned");
        map.get(network).copied().unwrap_or(0)
    }

    /// Build an empty `StreamsClients` with no publishers configured. Used by
    /// cross-module tests (e.g. `ReorgCoordinator::handle_reorg`) that need a
    /// real `StreamsClients` value to wire up but don't need to actually
    /// publish. Flush paths will panic if the buffer is populated for a
    /// stream type whose publisher is `None`, so seed only via
    /// `test_seed_finalized_buffer` in tests that don't also call
    /// `flush_finalized`.
    #[cfg(test)]
    pub(crate) fn empty_for_test() -> Self {
        Self {
            sns: None,
            webhook: None,
            rabbitmq: None,
            #[cfg(feature = "kafka")]
            kafka: None,
            redis: None,
            cloudflare_queues: None,
            finalized_buffers: AsyncMutex::new(HashMap::new()),
            reorg_safe_distances: StdMutex::new(HashMap::new()),
        }
    }

    /// Directly populate the finalized-delivery buffer for a given
    /// (network, event) pair. Tests only — production code reaches the
    /// buffer via `stream_with_mode` + `StreamDeliveryMode::Finalized`.
    #[cfg(test)]
    pub(crate) async fn test_seed_finalized_buffer(
        &self,
        network: &str,
        event_name: &str,
        blocks: &[u64],
        distance: u64,
    ) {
        use alloy::primitives::B256;
        self.register_network_reorg_distance(network.to_string(), distance);
        let key = BufferKey {
            stream_type: crate::metrics::streams::stream_type::WEBHOOK,
            config_index: 0,
            network: network.to_string(),
            event_name: event_name.to_string(),
            event_signature_hash: B256::ZERO,
        };
        let mut map = self.finalized_buffers.lock().await;
        let buf = map.entry(key).or_insert_with(|| FinalizedBuffer::new(distance));
        for &b in blocks {
            buf.add(b, vec![serde_json::json!({"block": b})]);
        }
    }

    /// Returns the sorted block numbers still present in the buffer for
    /// `network` across all event types. Tests only.
    #[cfg(test)]
    pub(crate) async fn test_buffered_blocks(&self, network: &str) -> Vec<u64> {
        let map = self.finalized_buffers.lock().await;
        let mut out = Vec::new();
        for (key, buf) in map.iter() {
            if key.network == network {
                out.extend(buf.buffer.keys().copied());
            }
        }
        out.sort_unstable();
        out
    }

    /// Redirects the Cloudflare Queues client to a different base URL. Intended
    /// solely for integration tests that mock the Cloudflare REST API with
    /// `mockito` — prefer configuring the real `api_token`/`account_id` in
    /// production code.
    #[doc(hidden)]
    pub fn set_cloudflare_base_url_for_test(&mut self, base_url: &str) {
        if let Some(cf) = self.cloudflare_queues.as_mut() {
            cf.client = Arc::new(
                CloudflareQueues::new(cf.config.api_token.clone(), cf.config.account_id.clone())
                    .with_base_url(base_url.to_string()),
            );
        }
    }

    fn has_any_streams(&self) -> bool {
        self.sns.is_some()
            || self.webhook.is_some()
            || self.rabbitmq.is_some()
            || {
                #[cfg(feature = "kafka")]
                {
                    self.kafka.is_some()
                }
                #[cfg(not(feature = "kafka"))]
                {
                    false
                }
            }
            || self.redis.is_some()
            || self.cloudflare_queues.is_some()
    }

    fn chunk_data(&self, data_array: &Vec<Value>) -> Vec<Vec<Value>> {
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        let mut chunks = Vec::new();
        for item in data_array {
            let item_str = serde_json::to_string(item)
                .expect("serde_json::to_string on Value cannot fail for valid JSON data");
            let item_size = item_str.len();

            if current_size + item_size > MAX_CHUNK_SIZE {
                chunks.push(current_chunk);
                current_chunk = Vec::new();
                current_size = 0;
            }

            current_chunk.push(item.clone());
            current_size += item_size;
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }

        chunks
    }

    /// Gets event name, which may be an optional alias, or the contract's event name.
    fn get_event_name(&self, events: &[StreamEvent], event_message: &EventMessage) -> String {
        let alias_name = events
            .iter()
            .find(|n| n.event_name == event_message.event_name)
            .and_then(|n| n.alias.clone());

        alias_name.unwrap_or_else(|| event_message.event_name.clone())
    }

    fn create_chunk_message_raw(
        &self,
        events: &[StreamEvent],
        event_message: &EventMessage,
        chunk: &[Value],
    ) -> String {
        let chunk_message = EventMessage {
            event_name: self.get_event_name(events, event_message),
            event_data: Value::Array(chunk.to_vec()),
            event_signature_hash: event_message.event_signature_hash,
            network: event_message.network.clone(),
            block_number: event_message.block_number,
        };

        serde_json::to_string(&chunk_message)
            .expect("serde_json::to_string on EventMessage cannot fail for valid JSON data")
    }

    fn create_chunk_message_json(
        &self,
        events: &[StreamEvent],
        event_message: &EventMessage,
        chunk: &[Value],
    ) -> Value {
        let chunk_message = EventMessage {
            event_name: self.get_event_name(events, event_message),
            event_data: Value::Array(chunk.to_vec()),
            event_signature_hash: event_message.event_signature_hash,
            network: event_message.network.clone(),
            block_number: event_message.block_number,
        };

        serde_json::to_value(&chunk_message)
            .expect("serde_json::to_value on EventMessage cannot fail for valid JSON data")
    }

    fn generate_publish_message_id(
        &self,
        id: &str,
        index: usize,
        prefix: &Option<String>,
    ) -> String {
        format!(
            "rindexer_stream__{}-{}-chunk-{}",
            prefix.as_ref().unwrap_or(&"".to_string()),
            id.to_lowercase(),
            index
        )
    }

    fn filter_chunk_event_data_by_conditions(
        &self,
        events: &[StreamEvent],
        event_message: &EventMessage,
        chunk: &[Value],
        force_send_network_wide: bool,
    ) -> Vec<Value> {
        if force_send_network_wide {
            return chunk.to_vec();
        }

        let stream_event = events.iter().find(|e| e.event_name == event_message.event_name);

        // Allow no trace events to be defined, otherwise use the defined event config.
        if event_message.event_name == EVENT_NAME && stream_event.is_none() {
            return chunk.to_vec();
        }

        let stream_event = stream_event
            .expect("Failed to find stream event - should never happen please raise an issue");

        let filtered_chunk: Vec<Value> = chunk
            .iter()
            .filter(|event_data| {
                if let Some(conditions) = &stream_event.conditions {
                    filter_event_data_by_conditions(event_data, conditions)
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        filtered_chunk
    }

    /// Returns `true` when the dispatch should be routed into the
    /// `FinalizedBuffer` instead of publishing immediately. Reorg notifications
    /// (`force_send_network_wide`) and trace events always bypass the buffer;
    /// `block_number == 0` is a sentinel for synthetic messages that likewise
    /// never buffer.
    fn should_buffer(
        delivery: Option<&crate::manifest::stream::StreamDeliveryMode>,
        event_message: &EventMessage,
        is_trace_event: bool,
        force_send_network_wide: bool,
    ) -> bool {
        if force_send_network_wide || is_trace_event {
            return false;
        }
        if event_message.block_number == 0 {
            return false;
        }
        matches!(delivery, Some(crate::manifest::stream::StreamDeliveryMode::Finalized))
    }

    /// Append a per-config event to the finalized-delivery buffer. Idempotently
    /// creates the buffer on first use using the network's registered
    /// `reorg_safe_distance`. Emits a warning + metric if the buffer crosses
    /// the soft cap — events are never dropped; the cap is a health signal.
    async fn buffer_event(
        &self,
        stream_type: &'static str,
        config_index: usize,
        event_message: &EventMessage,
    ) {
        let distance = self.reorg_safe_distance_for(&event_message.network);
        let key = BufferKey {
            stream_type,
            config_index,
            network: event_message.network.clone(),
            event_name: event_message.event_name.clone(),
            event_signature_hash: event_message.event_signature_hash,
        };
        let mut map = self.finalized_buffers.lock().await;
        let buffer = map.entry(key).or_insert_with(|| FinalizedBuffer::new(distance));
        if let Value::Array(data) = &event_message.event_data {
            buffer.add(event_message.block_number, data.clone());
        }
        let len = buffer.len();
        if len > FINALIZED_BUFFER_SOFT_CAP {
            warn!(
                network = %event_message.network,
                stream_type,
                config_index,
                buffered = len,
                "Finalized stream buffer exceeding soft cap"
            );
            stream_metrics::record_finalized_buffer_overflow(stream_type, &event_message.network);
        }
    }

    fn should_send_for_config(
        config_events: &[StreamEvent],
        event_name: &str,
        is_trace_event: bool,
        force_send_network_wide: bool,
    ) -> bool {
        force_send_network_wide
            || is_trace_event
            || config_events.iter().any(|e| e.event_name == event_name)
    }

    fn sns_stream_tasks(
        &self,
        config: &SNSStreamTopicConfig,
        client: Arc<SNS>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id =
                    self.generate_publish_message_id(id, index, &config.prefix_id);
                let client = Arc::clone(&client);
                let topic_arn = config.topic_arn.clone();
                let publish_message =
                    self.create_chunk_message_raw(&config.events, event_message, &filtered_chunk);
                task::spawn(async move {
                    let start = Instant::now();
                    let result =
                        client.publish(&publish_message_id, &topic_arn, &publish_message).await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::SNS,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();

        tasks
    }

    fn webhook_stream_tasks(
        &self,
        config: &WebhookStreamConfig,
        client: Arc<Webhook>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let endpoint = config.endpoint.clone();
                let shared_secret = config.shared_secret.clone();
                let client = Arc::clone(&client);
                let publish_message =
                    self.create_chunk_message_json(&config.events, event_message, &filtered_chunk);
                task::spawn(async move {
                    let start = Instant::now();
                    let result = client
                        .publish(&publish_message_id, &endpoint, &shared_secret, &publish_message)
                        .await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::WEBHOOK,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();

        tasks
    }

    fn rabbitmq_stream_tasks(
        &self,
        config: &RabbitMQStreamQueueConfig,
        client: Arc<RabbitMQ>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let exchange = config.exchange.clone();
                let exchange_type = config.exchange_type.clone();
                let routing_key = config.routing_key.clone();
                let publish_message =
                    self.create_chunk_message_json(&config.events, event_message, &filtered_chunk);

                task::spawn(async move {
                    let start = Instant::now();
                    let result = client
                        .publish(
                            &publish_message_id,
                            &exchange,
                            &exchange_type,
                            &routing_key,
                            &publish_message,
                        )
                        .await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::RABBITMQ,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();
        tasks
    }

    #[cfg(feature = "kafka")]
    fn kafka_stream_tasks(
        &self,
        config: &KafkaStreamQueueConfig,
        client: Arc<Kafka>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let exchange = config.topic.clone();
                let routing_key = config.key.clone();
                let publish_message =
                    self.create_chunk_message_json(&config.events, event_message, &filtered_chunk);
                task::spawn(async move {
                    let start = Instant::now();
                    let result = client
                        .publish(&publish_message_id, &exchange, &routing_key, &publish_message)
                        .await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::KAFKA,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();
        tasks
    }

    fn redis_stream_tasks(
        &self,
        config: &RedisStreamStreamConfig,
        client: Arc<Redis>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let stream_name = config.stream_name.clone();
                let publish_message =
                    self.create_chunk_message_json(&config.events, event_message, &filtered_chunk);

                task::spawn(async move {
                    let start = Instant::now();
                    let result =
                        client.publish(&publish_message_id, &stream_name, &publish_message).await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::REDIS,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();
        tasks
    }

    fn cloudflare_queues_stream_tasks(
        &self,
        config: &CloudflareQueuesStreamQueueConfig,
        client: Arc<CloudflareQueues>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
        force_send_network_wide: bool,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                    force_send_network_wide,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let queue_id = config.queue_id.clone();
                let publish_message =
                    self.create_chunk_message_json(&config.events, event_message, &filtered_chunk);

                task::spawn(async move {
                    let start = Instant::now();
                    let result =
                        client.publish(&publish_message_id, &queue_id, &publish_message).await;
                    let duration = start.elapsed().as_secs_f64();
                    let count = filtered_chunk.len();

                    stream_metrics::record_stream_operation(
                        stream_type::CLOUDFLARE_QUEUES,
                        result.is_ok(),
                        duration,
                        count,
                    );

                    result?;
                    Ok(count)
                })
            })
            .collect();
        tasks
    }

    pub async fn stream(
        &self,
        id: String,
        event_message: &EventMessage,
        index_event_in_order: bool,
        is_trace_event: bool,
    ) -> Result<usize, StreamError> {
        self.stream_with_mode(id, event_message, index_event_in_order, is_trace_event, false).await
    }

    async fn stream_with_mode(
        &self,
        id: String,
        event_message: &EventMessage,
        index_event_in_order: bool,
        is_trace_event: bool,
        force_send_network_wide: bool,
    ) -> Result<usize, StreamError> {
        if !self.has_any_streams() {
            return Ok(0);
        }

        // will always have something even if the event has no parameters due to the tx_information
        if let Value::Array(data_array) = &event_message.event_data {
            let chunks = Arc::new(self.chunk_data(data_array));
            let mut streams: Vec<StreamPublishes> = Vec::new();

            if let Some(sns) = &self.sns {
                for (idx, config) in sns.config.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::SNS, idx, event_message).await;
                            continue;
                        }
                        streams.push(self.sns_stream_tasks(
                            config,
                            Arc::clone(&sns.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            };

            if let Some(webhook) = &self.webhook {
                for (idx, config) in webhook.config.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::WEBHOOK, idx, event_message).await;
                            continue;
                        }
                        streams.push(self.webhook_stream_tasks(
                            config,
                            Arc::clone(&webhook.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            }

            if let Some(rabbitmq) = &self.rabbitmq {
                for (idx, config) in rabbitmq.config.exchanges.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::RABBITMQ, idx, event_message).await;
                            continue;
                        }
                        streams.push(self.rabbitmq_stream_tasks(
                            config,
                            Arc::clone(&rabbitmq.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            }

            #[cfg(feature = "kafka")]
            if let Some(kafka) = &self.kafka {
                for (idx, config) in kafka.config.topics.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::KAFKA, idx, event_message).await;
                            continue;
                        }
                        streams.push(self.kafka_stream_tasks(
                            config,
                            Arc::clone(&kafka.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            }

            if let Some(redis) = &self.redis {
                for (idx, config) in redis.config.streams.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::REDIS, idx, event_message).await;
                            continue;
                        }
                        streams.push(self.redis_stream_tasks(
                            config,
                            Arc::clone(&redis.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            }

            if let Some(cloudflare_queues) = &self.cloudflare_queues {
                for (idx, config) in cloudflare_queues.config.queues.iter().enumerate() {
                    if Self::should_send_for_config(
                        &config.events,
                        &event_message.event_name,
                        is_trace_event,
                        force_send_network_wide,
                    ) && config.networks.contains(&event_message.network)
                    {
                        if Self::should_buffer(
                            config.delivery.as_ref(),
                            event_message,
                            is_trace_event,
                            force_send_network_wide,
                        ) {
                            self.buffer_event(stream_type::CLOUDFLARE_QUEUES, idx, event_message)
                                .await;
                            continue;
                        }
                        streams.push(self.cloudflare_queues_stream_tasks(
                            config,
                            Arc::clone(&cloudflare_queues.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                            force_send_network_wide,
                        ));
                    }
                }
            }

            let mut streamed_total = 0;

            if index_event_in_order {
                for stream in streams {
                    for task in stream {
                        match task.await {
                            Ok(Ok(streamed)) => {
                                streamed_total += streamed;
                            }
                            Ok(Err(e)) => return Err(e),
                            Err(e) => return Err(StreamError::JoinError(e)),
                        }
                    }
                }
            } else {
                let tasks: Vec<_> = streams.into_iter().flatten().collect();
                let results = join_all(tasks).await;
                for result in results {
                    match result {
                        Ok(Ok(streamed)) => {
                            streamed_total += streamed;
                        }
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(StreamError::JoinError(e)),
                    }
                }
            }

            Ok(streamed_total)
        } else {
            unreachable!("Event data should be an array");
        }
    }

    /// Publishes a `__rindexer_reorg` retraction event to all configured streams.
    ///
    /// Routing is delegated to the internal `stream_with_mode` path with
    /// `force_send_network_wide = true`, which bypasses the per-stream
    /// `events` filter: every destination whose `networks` list contains the
    /// affected `network` receives the reorg payload regardless of whether
    /// `__rindexer_reorg` appears in its configured events. Per-stream-type
    /// routing behaviour:
    ///
    /// - **Webhook**: POST to every endpoint whose `networks` matches.
    ///   Body is the JSON `EventMessage` with `event_name = "__rindexer_reorg"`.
    /// - **SNS**: publishes to every topic whose `networks` matches. The
    ///   payload is the JSON-encoded `EventMessage` string; `event_name` is
    ///   carried inside the payload (no SNS message attributes are set).
    /// - **Kafka** *(feature-gated)*: publishes to every configured topic
    ///   whose `networks` matches. The record `key` is the per-topic
    ///   `key` from config (not derived from `event_name`); the
    ///   `x-rindexer-id` header carries the generated message id.
    /// - **RabbitMQ**: publishes to the configured `exchange` with the
    ///   configured `routing_key`. Fanout exchanges ignore the routing
    ///   key. Topic and direct exchanges require a non-`None` routing key
    ///   at manifest-validation time — this is enforced by
    ///   [`RabbitMQStreamConfig::validate`].
    /// - **Redis**: `XADD`s to every configured stream whose `networks`
    ///   matches, under the `payload` field.
    /// - **CloudflareQueues**: enqueues (via the Cloudflare REST API) to
    ///   every queue whose `networks` matches.
    ///
    /// All types reach publish through the shared `force_send_network_wide`
    /// path — no destination is silently dropped because its `events` list
    /// omits `__rindexer_reorg`.
    pub async fn stream_reorg(
        &self,
        network: &str,
        fork_block: u64,
        depth: u64,
        events_deleted: u64,
        affected_tx_hashes: &[B256],
        affected_tables: &[AffectedTable],
    ) -> Result<usize, StreamError> {
        if !self.has_any_streams() {
            return Ok(0);
        }

        let reorg_payload = json!({
            "type": "reorg",
            "network": network,
            "fork_block": fork_block,
            "depth": depth,
            "events_deleted": events_deleted,
            "affected_tx_hashes": affected_tx_hashes.iter().map(|h| format!("{:#x}", h)).collect::<Vec<_>>(),
            "affected_events": affected_tables.iter().map(|t| json!({
                "indexer": t.indexer_name,
                "contract": t.contract_name,
                "event": t.event_name,
                "schema": t.schema,
                "table": t.table_name,
                "rows_deleted": t.rows_deleted,
            })).collect::<Vec<_>>(),
        });

        let event_message = EventMessage {
            event_name: "__rindexer_reorg".to_string(),
            event_data: Value::Array(vec![reorg_payload]),
            event_signature_hash: B256::ZERO,
            network: network.to_string(),
            // Synthetic reorg notifications are never buffered for finalized
            // delivery — `should_buffer` treats `block_number == 0` as a
            // sentinel for "always dispatch immediately".
            block_number: 0,
        };

        self.stream_with_mode(
            format!("reorg_{}_{}", network, fork_block),
            &event_message,
            false,
            false,
            true,
        )
        .await
    }

    /// Drain finalized-delivery buffers for `network` that have become safe
    /// relative to `head_block` (i.e., buried by at least the network's
    /// registered `reorg_safe_distance`). Called by `ReorgCoordinator` on every
    /// validated `on_new_block`. Returns the total number of events dispatched.
    pub async fn flush_finalized(
        &self,
        network: &str,
        head_block: u64,
    ) -> Result<usize, StreamError> {
        let mut ready_by_key: Vec<FinalizedDeliveryBuffer> = Vec::new();
        {
            let mut map = self.finalized_buffers.lock().await;
            for (key, buffer) in map.iter_mut() {
                if key.network != network {
                    continue;
                }
                let ready = buffer.flush(head_block);
                if !ready.is_empty() {
                    ready_by_key.push((key.clone(), ready));
                }
            }
        }

        let mut total_sent = 0;
        for (key, ready) in ready_by_key {
            total_sent += self.dispatch_flushed(&key, ready).await?;
        }
        Ok(total_sent)
    }

    /// Remove buffered events for `network` whose source block falls in the
    /// inclusive range `[fork_point, detection_point]`. Called by
    /// `ReorgCoordinator::handle_reorg` *before* the next `flush_finalized` so
    /// invalidated events never escape the buffer.
    pub async fn discard_finalized(&self, network: &str, fork_point: u64, detection_point: u64) {
        let mut map = self.finalized_buffers.lock().await;
        for (key, buffer) in map.iter_mut() {
            if key.network != network {
                continue;
            }
            buffer.discard_range(fork_point, detection_point);
        }
    }

    /// Re-publish a block's worth of previously-buffered events through the
    /// stream-type-specific `*_stream_tasks` helper. Does NOT re-enter
    /// `stream_with_mode` — that would re-buffer indefinitely.
    ///
    /// Per-block `*_stream_tasks` calls are built sequentially (they only
    /// allocate; they don't await network I/O), and every resulting JoinHandle
    /// from every block is then awaited through a single `join_all`, so a
    /// flush of N buffered blocks runs at ~1 RTT instead of N.
    async fn dispatch_flushed(
        &self,
        key: &BufferKey,
        ready: Vec<(u64, Vec<Value>)>,
    ) -> Result<usize, StreamError> {
        let mut all_tasks: StreamPublishes = Vec::new();
        for (block_number, events) in ready {
            let event_message = EventMessage {
                event_name: key.event_name.clone(),
                event_data: Value::Array(events.clone()),
                event_signature_hash: key.event_signature_hash,
                network: key.network.clone(),
                block_number,
            };
            let chunks = Arc::new(self.chunk_data(&events));
            let id = format!(
                "finalized_{}_{}_{}_blk{}",
                key.stream_type, key.config_index, key.network, block_number
            );

            let tasks: StreamPublishes = match key.stream_type {
                stream_type::SNS => {
                    let sns = self.sns.as_ref().expect("sns buffer without sns config");
                    self.sns_stream_tasks(
                        &sns.config[key.config_index],
                        Arc::clone(&sns.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                stream_type::WEBHOOK => {
                    let webhook =
                        self.webhook.as_ref().expect("webhook buffer without webhook config");
                    self.webhook_stream_tasks(
                        &webhook.config[key.config_index],
                        Arc::clone(&webhook.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                stream_type::RABBITMQ => {
                    let rabbitmq =
                        self.rabbitmq.as_ref().expect("rabbitmq buffer without rabbitmq config");
                    self.rabbitmq_stream_tasks(
                        &rabbitmq.config.exchanges[key.config_index],
                        Arc::clone(&rabbitmq.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                #[cfg(feature = "kafka")]
                stream_type::KAFKA => {
                    let kafka = self.kafka.as_ref().expect("kafka buffer without kafka config");
                    self.kafka_stream_tasks(
                        &kafka.config.topics[key.config_index],
                        Arc::clone(&kafka.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                stream_type::REDIS => {
                    let redis = self.redis.as_ref().expect("redis buffer without redis config");
                    self.redis_stream_tasks(
                        &redis.config.streams[key.config_index],
                        Arc::clone(&redis.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                stream_type::CLOUDFLARE_QUEUES => {
                    let cf = self
                        .cloudflare_queues
                        .as_ref()
                        .expect("cloudflare_queues buffer without cloudflare_queues config");
                    self.cloudflare_queues_stream_tasks(
                        &cf.config.queues[key.config_index],
                        Arc::clone(&cf.client),
                        &id,
                        &event_message,
                        chunks,
                        false,
                    )
                }
                other => unreachable!("unknown stream_type in BufferKey: {other}"),
            };
            all_tasks.extend(tasks);
        }

        let mut total_sent = 0;
        for result in join_all(all_tasks).await {
            match result {
                Ok(Ok(n)) => total_sent += n,
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(StreamError::JoinError(e)),
            }
        }
        Ok(total_sent)
    }
}

/// Per-(stream-type, config-index, network) buffer of JSON events keyed by
/// source block number. Used by `StreamDeliveryMode::Finalized` to delay
/// dispatch until a block is deeper than the chain's `reorg_safe_distance`.
///
/// `flush(current_head)` drains every bucket whose block is `<= current_head -
/// reorg_safe_distance`; `discard_range(fork_point, detection_point)` removes
/// buckets in an invalidated range when a reorg is detected.
#[derive(Debug)]
pub(crate) struct FinalizedBuffer {
    buffer: BTreeMap<u64, Vec<Value>>,
    reorg_safe_distance: u64,
}

impl FinalizedBuffer {
    pub fn new(reorg_safe_distance: u64) -> Self {
        Self { buffer: BTreeMap::new(), reorg_safe_distance }
    }

    pub fn add(&mut self, block_number: u64, events: Vec<Value>) {
        self.buffer.entry(block_number).or_default().extend(events);
    }

    pub fn flush(&mut self, current_head: u64) -> Vec<(u64, Vec<Value>)> {
        let finality_threshold = current_head.saturating_sub(self.reorg_safe_distance);
        let ready_keys: Vec<u64> =
            self.buffer.keys().copied().filter(|&k| k <= finality_threshold).collect();
        ready_keys.into_iter().filter_map(|k| self.buffer.remove(&k).map(|v| (k, v))).collect()
    }

    pub fn discard_range(&mut self, fork_point: u64, detection_point: u64) {
        let keys_to_remove: Vec<u64> = self
            .buffer
            .keys()
            .copied()
            .filter(|&k| k >= fork_point && k <= detection_point)
            .collect();
        for k in keys_to_remove {
            self.buffer.remove(&k);
        }
    }

    pub fn len(&self) -> usize {
        self.buffer.values().map(|v| v.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::stream::{
        CloudflareQueuesStreamConfig, CloudflareQueuesStreamQueueConfig, StreamEvent,
        WebhookStreamConfig,
    };
    use alloy::primitives::B256;
    use serde_json::json;

    // ---- helpers ----

    fn stream_event(name: &str) -> StreamEvent {
        StreamEvent { event_name: name.to_string(), conditions: None, alias: None }
    }

    fn stream_event_with_alias(name: &str, alias: &str) -> StreamEvent {
        StreamEvent {
            event_name: name.to_string(),
            conditions: None,
            alias: Some(alias.to_string()),
        }
    }

    fn empty_clients() -> StreamsClients {
        StreamsClients {
            sns: None,
            webhook: None,
            rabbitmq: None,
            #[cfg(feature = "kafka")]
            kafka: None,
            redis: None,
            cloudflare_queues: None,
            finalized_buffers: AsyncMutex::new(HashMap::new()),
            reorg_safe_distances: StdMutex::new(HashMap::new()),
        }
    }

    fn webhook_clients(config: Vec<WebhookStreamConfig>) -> StreamsClients {
        StreamsClients {
            sns: None,
            webhook: Some(WebhookStream { config, client: Arc::new(Webhook::new()) }),
            rabbitmq: None,
            #[cfg(feature = "kafka")]
            kafka: None,
            redis: None,
            cloudflare_queues: None,
            finalized_buffers: AsyncMutex::new(HashMap::new()),
            reorg_safe_distances: StdMutex::new(HashMap::new()),
        }
    }

    fn cloudflare_clients(
        base_url: &str,
        queues: Vec<CloudflareQueuesStreamQueueConfig>,
    ) -> StreamsClients {
        let config = CloudflareQueuesStreamConfig {
            api_token: "test-token".to_string(),
            account_id: "acc-123".to_string(),
            queues,
        };
        StreamsClients {
            sns: None,
            webhook: None,
            rabbitmq: None,
            #[cfg(feature = "kafka")]
            kafka: None,
            redis: None,
            cloudflare_queues: Some(CloudflareQueuesStream {
                config,
                client: Arc::new(
                    CloudflareQueues::new("test-token".to_string(), "acc-123".to_string())
                        .with_base_url(base_url.to_string()),
                ),
            }),
            finalized_buffers: AsyncMutex::new(HashMap::new()),
            reorg_safe_distances: StdMutex::new(HashMap::new()),
        }
    }

    fn sample_event_message() -> EventMessage {
        EventMessage {
            event_name: "Transfer".to_string(),
            event_data: json!([{"from": "0x1", "to": "0x2", "value": "100"}]),
            event_signature_hash: B256::ZERO,
            network: "ethereum".to_string(),
            block_number: 100,
        }
    }

    // ---- should_send_for_config ----

    #[test]
    fn should_send_for_config_requires_event_without_force_or_trace() {
        let events = vec![stream_event("Transfer")];
        assert!(
            !StreamsClients::should_send_for_config(&events, "__rindexer_reorg", false, false,)
        );
    }

    #[test]
    fn should_send_for_config_force_send_bypasses_event_match() {
        let events = vec![stream_event("Transfer")];
        assert!(StreamsClients::should_send_for_config(&events, "__rindexer_reorg", false, true,));
    }

    #[test]
    fn should_send_for_config_trace_event_bypasses_event_match() {
        let events = vec![stream_event("Transfer")];
        assert!(StreamsClients::should_send_for_config(&events, "NativeTransfer", true, false,));
    }

    #[test]
    fn should_send_for_config_matching_event() {
        let events = vec![stream_event("Transfer")];
        assert!(StreamsClients::should_send_for_config(&events, "Transfer", false, false));
    }

    #[test]
    fn should_send_for_config_empty_events() {
        let events: Vec<StreamEvent> = vec![];
        assert!(!StreamsClients::should_send_for_config(&events, "Transfer", false, false));
    }

    // ---- has_any_streams ----

    #[test]
    fn has_any_streams_false_when_empty() {
        assert!(!empty_clients().has_any_streams());
    }

    #[test]
    fn has_any_streams_true_with_webhook() {
        assert!(webhook_clients(vec![]).has_any_streams());
    }

    // ---- chunk_data ----

    #[test]
    fn chunk_data_empty_input() {
        assert!(empty_clients().chunk_data(&vec![]).is_empty());
    }

    #[test]
    fn chunk_data_single_chunk_for_small_data() {
        let data = vec![json!({"key": "value"})];
        let chunks = empty_clients().chunk_data(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], data);
    }

    #[test]
    fn chunk_data_splits_when_exceeding_max_size() {
        let large = "x".repeat(40 * 1024); // 40KB each, MAX_CHUNK_SIZE is 75KB
        let data = vec![json!({"d": large.clone()}), json!({"d": large})];
        let chunks = empty_clients().chunk_data(&data);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn chunk_data_keeps_small_items_together() {
        let small = "x".repeat(100);
        let data: Vec<Value> = (0..10).map(|i| json!({"i": i, "d": small})).collect();
        let chunks = empty_clients().chunk_data(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 10);
    }

    // ---- get_event_name ----

    #[test]
    fn get_event_name_returns_original_when_no_alias() {
        let events = vec![stream_event("Transfer")];
        let msg = sample_event_message();
        assert_eq!(empty_clients().get_event_name(&events, &msg), "Transfer");
    }

    #[test]
    fn get_event_name_returns_alias_when_set() {
        let events = vec![stream_event_with_alias("Transfer", "TokenTransfer")];
        let msg = sample_event_message();
        assert_eq!(empty_clients().get_event_name(&events, &msg), "TokenTransfer");
    }

    #[test]
    fn get_event_name_falls_back_when_event_not_found() {
        let events = vec![stream_event("Approval")];
        let msg = sample_event_message();
        assert_eq!(empty_clients().get_event_name(&events, &msg), "Transfer");
    }

    // ---- generate_publish_message_id ----

    #[test]
    fn generate_id_without_prefix() {
        let id = empty_clients().generate_publish_message_id("MyEvent", 0, &None);
        assert_eq!(id, "rindexer_stream__-myevent-chunk-0");
    }

    #[test]
    fn generate_id_with_prefix() {
        let id =
            empty_clients().generate_publish_message_id("MyEvent", 3, &Some("pfx".to_string()));
        assert_eq!(id, "rindexer_stream__pfx-myevent-chunk-3");
    }

    // ---- create_chunk_message_raw ----

    #[test]
    fn create_chunk_message_raw_structure() {
        let events = vec![stream_event("Transfer")];
        let msg = sample_event_message();
        let chunk = vec![json!({"from": "0x1"})];
        let raw = empty_clients().create_chunk_message_raw(&events, &msg, &chunk);
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["event_name"], "Transfer");
        assert_eq!(parsed["network"], "ethereum");
        assert!(parsed["event_data"].is_array());
    }

    #[test]
    fn create_chunk_message_raw_applies_alias() {
        let events = vec![stream_event_with_alias("Transfer", "Xfer")];
        let msg = sample_event_message();
        let chunk = vec![json!({"v": 1})];
        let raw = empty_clients().create_chunk_message_raw(&events, &msg, &chunk);
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(parsed["event_name"], "Xfer");
    }

    // ---- create_chunk_message_json ----

    #[test]
    fn create_chunk_message_json_structure() {
        let events = vec![stream_event("Transfer")];
        let msg = sample_event_message();
        let chunk = vec![json!({"a": 1}), json!({"a": 2})];
        let val = empty_clients().create_chunk_message_json(&events, &msg, &chunk);
        assert_eq!(val["event_name"], "Transfer");
        assert_eq!(val["event_data"].as_array().unwrap().len(), 2);
        assert_eq!(val["network"], "ethereum");
    }

    // ---- filter_chunk_event_data_by_conditions ----

    #[test]
    fn filter_chunk_force_send_passes_all() {
        let events = vec![stream_event("Transfer")];
        let msg = sample_event_message();
        let chunk = vec![json!({"v": 1}), json!({"v": 2})];
        let result =
            empty_clients().filter_chunk_event_data_by_conditions(&events, &msg, &chunk, true);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn filter_chunk_no_conditions_passes_all() {
        let events = vec![stream_event("Transfer")];
        let msg = sample_event_message();
        let chunk = vec![json!({"v": 1}), json!({"v": 2})];
        let result =
            empty_clients().filter_chunk_event_data_by_conditions(&events, &msg, &chunk, false);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn filter_chunk_with_conditions_filters() {
        let mut m = serde_json::Map::new();
        m.insert("value".to_string(), json!(">=100"));
        let events = vec![StreamEvent {
            event_name: "Transfer".to_string(),
            conditions: Some(vec![m]),
            alias: None,
        }];
        let msg = sample_event_message();
        let chunk = vec![json!({"value": "200"}), json!({"value": "50"}), json!({"value": "100"})];
        let result =
            empty_clients().filter_chunk_event_data_by_conditions(&events, &msg, &chunk, false);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn filter_chunk_native_transfer_without_config_passes_all() {
        let events = vec![stream_event("Transfer")]; // NativeTransfer not defined
        let msg = EventMessage {
            event_name: EVENT_NAME.to_string(),
            event_data: json!([]),
            event_signature_hash: B256::ZERO,
            network: "ethereum".to_string(),
            block_number: 100,
        };
        let chunk = vec![json!({"v": 1})];
        let result =
            empty_clients().filter_chunk_event_data_by_conditions(&events, &msg, &chunk, false);
        assert_eq!(result.len(), 1);
    }

    // ---- stream (async) ----

    #[tokio::test]
    async fn stream_returns_zero_with_no_streams() {
        let msg = sample_event_message();
        let result = empty_clients().stream("id".to_string(), &msg, false, false).await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn stream_skips_webhook_on_network_mismatch() {
        let config = WebhookStreamConfig {
            endpoint: "http://127.0.0.1:1/hook".to_string(),
            shared_secret: "s".to_string(),
            networks: vec!["polygon".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };
        let msg = sample_event_message(); // network: ethereum
        let result =
            webhook_clients(vec![config]).stream("id".to_string(), &msg, false, false).await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn stream_skips_webhook_on_event_mismatch() {
        let config = WebhookStreamConfig {
            endpoint: "http://127.0.0.1:1/hook".to_string(),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Approval")],
            delivery: None,
        };
        let msg = sample_event_message(); // event: Transfer
        let result =
            webhook_clients(vec![config]).stream("id".to_string(), &msg, false, false).await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn stream_publishes_to_webhook() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/hook")
            .match_header("content-type", "application/json")
            .with_status(200)
            .create_async()
            .await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "secret".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let msg = sample_event_message();
        let result =
            webhook_clients(vec![config]).stream("id".to_string(), &msg, false, false).await;

        assert_eq!(result.unwrap(), 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_webhook_propagates_error() {
        let mut server = mockito::Server::new_async().await;
        // `publish_with_retry` runs up to 3 attempts before surfacing the
        // terminal error, so a persistently-500 endpoint gets hit 3 times.
        let mock = server.mock("POST", "/hook").with_status(500).expect(3).create_async().await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let msg = sample_event_message();
        let result =
            webhook_clients(vec![config]).stream("id".to_string(), &msg, false, false).await;

        assert!(result.is_err());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_trace_event_bypasses_event_match() {
        let mut server = mockito::Server::new_async().await;
        let mock = server.mock("POST", "/hook").with_status(200).create_async().await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")], // does not include NativeTransfer
            delivery: None,
        };

        let msg = EventMessage {
            event_name: "NativeTransfer".to_string(),
            event_data: json!([{"from": "0x1", "to": "0x2", "value": "1000"}]),
            event_signature_hash: B256::ZERO,
            network: "ethereum".to_string(),
            block_number: 100,
        };

        let result = webhook_clients(vec![config])
            .stream("id".to_string(), &msg, false, true) // is_trace_event = true
            .await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_multiple_webhooks() {
        let mut server = mockito::Server::new_async().await;
        let mock = server.mock("POST", "/hook").with_status(200).expect(2).create_async().await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let msg = sample_event_message();
        let result = webhook_clients(vec![config.clone(), config])
            .stream("id".to_string(), &msg, false, false)
            .await;

        assert_eq!(result.unwrap(), 2);
        mock.assert_async().await;
    }

    // ---- stream_reorg ----

    fn affected_table(
        schema: &str,
        table: &str,
        indexer: &str,
        contract: &str,
        event: &str,
    ) -> AffectedTable {
        AffectedTable {
            schema: schema.to_string(),
            table_name: table.to_string(),
            rows_deleted: 0,
            indexer_name: indexer.to_string(),
            contract_name: contract.to_string(),
            event_name: event.to_string(),
        }
    }

    #[tokio::test]
    async fn stream_reorg_returns_zero_without_streams() {
        // No streams configured — empty `affected_tables` slice is fine.
        let result = empty_clients().stream_reorg("ethereum", 100, 2, 0, &[], &[]).await;
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn stream_reorg_publishes_to_webhook() {
        // Capture the webhook body and assert the enriched payload shape:
        // the inner `reorg_payload` carried in `event_data[0]` must contain
        // `affected_events` with the table metadata we pass in.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/hook")
            .match_body(mockito::Matcher::PartialJson(json!({
                "event_name": "__rindexer_reorg",
                "network": "ethereum",
                "event_data": [{
                    "type": "reorg",
                    "network": "ethereum",
                    "fork_block": 100,
                    "depth": 2,
                    "events_deleted": 42,
                    "affected_events": [{
                        "indexer": "my_indexer",
                        "contract": "USDC",
                        "event": "Transfer",
                        "schema": "my_indexer_usdc",
                        "table": "transfer",
                        "rows_deleted": 0,
                    }],
                }],
            })))
            .with_status(200)
            .create_async()
            .await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")], // doesn't matter, force_send
            delivery: None,
        };

        let tables =
            vec![affected_table("my_indexer_usdc", "transfer", "my_indexer", "USDC", "Transfer")];
        let result = webhook_clients(vec![config])
            .stream_reorg("ethereum", 100, 2, 42, &[B256::ZERO], &tables)
            .await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_reorg_payload_includes_native_transfer_rows() {
        // NativeTransfer-sourced rows must appear as a distinct entry in the
        // `affected_events` array so downstream consumers can detect them.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/hook")
            .match_body(mockito::Matcher::PartialJson(json!({
                "event_data": [{
                    "affected_events": [{
                        "indexer": "my_indexer",
                        "contract": "EvmTraces",
                        "event": "NativeTransfer",
                        "schema": "my_indexer_evm_traces",
                        "table": "native_transfer",
                    }],
                }],
            })))
            .with_status(200)
            .create_async()
            .await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let tables = vec![affected_table(
            "my_indexer_evm_traces",
            "native_transfer",
            "my_indexer",
            "EvmTraces",
            "NativeTransfer",
        )];
        let result =
            webhook_clients(vec![config]).stream_reorg("ethereum", 500, 1, 0, &[], &tables).await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_reorg_empty_affected_serializes_empty_array() {
        // Streams configured, but no affected tables and no affected tx hashes.
        // The payload must still serialize with `affected_events: []` and
        // `events_deleted: 0` so downstream consumers can rely on their presence.
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/hook")
            .match_body(mockito::Matcher::PartialJson(json!({
                "event_name": "__rindexer_reorg",
                "network": "ethereum",
                "event_data": [{
                    "type": "reorg",
                    "network": "ethereum",
                    "fork_block": 7,
                    "depth": 1,
                    "events_deleted": 0,
                    "affected_events": [],
                }],
            })))
            .with_status(200)
            .create_async()
            .await;

        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let result =
            webhook_clients(vec![config]).stream_reorg("ethereum", 7, 1, 0, &[], &[]).await;

        assert!(result.is_ok());
        mock.assert_async().await;
    }

    // ---- cloudflare_queues end-to-end ----

    #[tokio::test]
    async fn stream_publishes_to_cloudflare_queues() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/client/v4/accounts/acc-123/queues/q-456/messages")
            .with_status(200)
            .create_async()
            .await;

        let queue_config = CloudflareQueuesStreamQueueConfig {
            queue_id: "q-456".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let msg = sample_event_message();
        let result = cloudflare_clients(&server.url(), vec![queue_config])
            .stream("id".to_string(), &msg, false, false)
            .await;

        assert_eq!(result.unwrap(), 1);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn stream_skips_cloudflare_on_network_mismatch() {
        let queue_config = CloudflareQueuesStreamQueueConfig {
            queue_id: "q-456".to_string(),
            networks: vec!["polygon".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };

        let msg = sample_event_message(); // network: ethereum
        let result = cloudflare_clients("http://127.0.0.1:1", vec![queue_config])
            .stream("id".to_string(), &msg, false, false)
            .await;

        assert_eq!(result.unwrap(), 0);
    }

    // ---- FinalizedBuffer ----

    #[test]
    fn finalized_buffer_flushes_only_below_threshold() {
        let mut buf = FinalizedBuffer::new(10);
        buf.add(100, vec![json!({"event": "a"})]);
        buf.add(105, vec![json!({"event": "b"})]);

        let flushed = buf.flush(112); // threshold = 102
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, 100);

        let flushed2 = buf.flush(120);
        assert_eq!(flushed2.len(), 1);
        assert_eq!(flushed2[0].0, 105);
    }

    #[test]
    fn finalized_buffer_discards_range_inclusive() {
        let mut buf = FinalizedBuffer::new(10);
        buf.add(98, vec![json!({"event": "a"})]);
        buf.add(99, vec![json!({"event": "b"})]);
        buf.add(100, vec![json!({"event": "c"})]);

        buf.discard_range(99, 100);

        let flushed = buf.flush(200);
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].0, 98);
    }

    #[test]
    fn finalized_buffer_discard_on_empty_is_noop() {
        let mut buf = FinalizedBuffer::new(10);
        buf.discard_range(5, 10);
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn finalized_buffer_add_merges_same_block() {
        let mut buf = FinalizedBuffer::new(10);
        buf.add(50, vec![json!({"a": 1})]);
        buf.add(50, vec![json!({"a": 2})]);
        assert_eq!(buf.len(), 2);
    }

    // ---- StreamsClients finalized-buffer integration tests ----
    //
    // These exercise the public `discard_finalized` / `flush_finalized`
    // surface that `ReorgCoordinator::handle_reorg` drives, covering the
    // exact path that silently leaked stale events before the reth-semantic
    // fix (`on_exex_reorg` now passes the correct `fork_point = first
    // reverted, detection_point = last reverted` range).

    async fn seed_buffer(
        clients: &StreamsClients,
        network: &str,
        event_name: &str,
        blocks: &[u64],
        distance: u64,
    ) {
        clients.register_network_reorg_distance(network.to_string(), distance);
        let key = BufferKey {
            stream_type: stream_type::WEBHOOK,
            config_index: 0,
            network: network.to_string(),
            event_name: event_name.to_string(),
            event_signature_hash: B256::ZERO,
        };
        let mut map = clients.finalized_buffers.lock().await;
        let buf = map.entry(key).or_insert_with(|| FinalizedBuffer::new(distance));
        for &b in blocks {
            buf.add(b, vec![json!({"block": b})]);
        }
    }

    async fn buffered_blocks(clients: &StreamsClients, network: &str) -> Vec<u64> {
        let map = clients.finalized_buffers.lock().await;
        let mut out = Vec::new();
        for (key, buf) in map.iter() {
            if key.network == network {
                out.extend(buf.buffer.keys().copied());
            }
        }
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn discard_finalized_removes_exactly_the_reorged_span() {
        // The reth-semantic bug caused fork_point to equal last_reverted, so
        // the discard range collapsed to a single block and the other
        // reverted blocks leaked out on the next flush. This verifies the
        // post-fix path: a reorg of 101..=110 discards exactly those 10
        // blocks, leaving 95..=100 and 111..=115 untouched.
        let clients = empty_clients();
        let all: Vec<u64> = (95..=115).collect();
        seed_buffer(&clients, "ethereum", "Transfer", &all, 10).await;

        clients.discard_finalized("ethereum", 101, 110).await;

        let remaining = buffered_blocks(&clients, "ethereum").await;
        let expected: Vec<u64> = (95..=100).chain(111..=115).collect();
        assert_eq!(remaining, expected);
    }

    #[tokio::test]
    async fn discard_finalized_single_block_reorg_only_drops_that_block() {
        // Minimum-depth reorg: `on_exex_reorg(n, n)` must discard only block
        // n. This guards against an accidental re-introduction of a `+ 1`
        // drift in the inclusive-range discard.
        let clients = empty_clients();
        seed_buffer(&clients, "ethereum", "Transfer", &[99, 100, 101], 10).await;

        clients.discard_finalized("ethereum", 100, 100).await;

        assert_eq!(buffered_blocks(&clients, "ethereum").await, vec![99, 101]);
    }

    #[tokio::test]
    async fn discard_finalized_is_scoped_to_network() {
        // A reorg on Ethereum must not touch Polygon's buffer. The buffer
        // map is keyed on network, but the discard iterates all entries and
        // filters — this pins the filter.
        let clients = empty_clients();
        seed_buffer(&clients, "ethereum", "Transfer", &[100, 101, 102], 10).await;
        seed_buffer(&clients, "polygon", "Transfer", &[100, 101, 102], 10).await;

        clients.discard_finalized("ethereum", 100, 102).await;

        assert!(buffered_blocks(&clients, "ethereum").await.is_empty());
        assert_eq!(buffered_blocks(&clients, "polygon").await, vec![100, 101, 102]);
    }

    #[tokio::test]
    async fn discard_finalized_spans_multiple_event_types() {
        // Reorgs are chain-level, not event-level — if two different events
        // on the same network have buffered entries at reorged heights,
        // both must be cleared.
        let clients = empty_clients();
        seed_buffer(&clients, "ethereum", "Transfer", &[100, 101], 10).await;
        seed_buffer(&clients, "ethereum", "Approval", &[100, 101], 10).await;

        clients.discard_finalized("ethereum", 101, 101).await;

        // Both event buckets for block 101 are gone; block 100 survives in both.
        let map = clients.finalized_buffers.lock().await;
        for (_key, buf) in map.iter() {
            assert!(buf.buffer.get(&100).is_some(), "block 100 must survive");
            assert!(buf.buffer.get(&101).is_none(), "block 101 must be discarded");
        }
    }

    #[tokio::test]
    async fn flush_finalized_emits_blocks_up_to_inclusive_threshold() {
        // Sanity-check that `flush_finalized(head)` uses the same
        // `head - distance` inclusive threshold as `calculate_safe_block_number`.
        // A block at the threshold must flush; a block one above must stay.
        // Runs the flush through a real webhook dispatch so any future change
        // to the threshold condition is observable as a delivered-count diff.
        let mut server = mockito::Server::new_async().await;
        let mock = server.mock("POST", "/hook").with_status(200).expect(1).create_async().await;
        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };
        let clients = webhook_clients(vec![config]);
        seed_buffer(&clients, "ethereum", "Transfer", &[100, 101], 10).await;

        let sent = clients.flush_finalized("ethereum", 110).await.unwrap();

        // Threshold = 110 - 10 = 100, so block 100 flushes and block 101 stays.
        assert_eq!(sent, 1, "exactly one block should flush at the threshold");
        assert_eq!(buffered_blocks(&clients, "ethereum").await, vec![101]);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn flush_then_reorg_clears_only_unflushed_reorged_blocks() {
        // End-to-end: blocks 95..=115 buffered, head=105 flushes only block
        // 95 (threshold = 95), then a reorg hits [100, 105]. Post-fix the
        // buffer must end up as [96..=99] ∪ [106..=115]. Before the reth
        // semantic fix `discard_finalized(100, 100)` (collapsed to a
        // single-block range) would have left 101..=105 in the buffer —
        // they would then flush as canonical on the next tip advance,
        // shipping reorged data.
        let mut server = mockito::Server::new_async().await;
        let mock = server.mock("POST", "/hook").with_status(200).expect(1).create_async().await;
        let config = WebhookStreamConfig {
            endpoint: format!("{}/hook", server.url()),
            shared_secret: "s".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![stream_event("Transfer")],
            delivery: None,
        };
        let clients = webhook_clients(vec![config]);
        let all: Vec<u64> = (95..=115).collect();
        seed_buffer(&clients, "ethereum", "Transfer", &all, 10).await;

        let sent = clients.flush_finalized("ethereum", 105).await.unwrap();
        assert_eq!(sent, 1, "only block 95 is at/below threshold (105-10)");

        clients.discard_finalized("ethereum", 100, 105).await;

        let remaining = buffered_blocks(&clients, "ethereum").await;
        let expected: Vec<u64> = (96..=99).chain(106..=115).collect();
        assert_eq!(remaining, expected);
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn flush_finalized_with_head_below_distance_flushes_nothing() {
        // Early in a chain's life `head < distance`; saturating_sub yields 0
        // and nothing should flush. No webhook setup needed because with
        // `head=5, distance=10` the flush produces an empty ready-set and
        // `dispatch_flushed` is never invoked.
        let clients = empty_clients();
        seed_buffer(&clients, "ethereum", "Transfer", &[1, 2, 5], 10).await;

        let sent = clients.flush_finalized("ethereum", 5).await.unwrap();

        assert_eq!(sent, 0);
        assert_eq!(buffered_blocks(&clients, "ethereum").await, vec![1, 2, 5]);
    }
}
