use std::sync::Arc;

use aws_sdk_sns::{config::http::HttpResponse, error::SdkError, operation::publish::PublishError};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    task,
    task::{JoinError, JoinHandle},
};
use tracing::error;

use crate::{
    event::{filter_event_data_by_conditions, EventMessage},
    manifest::stream::{
        KafkaStreamConfig, KafkaStreamQueueConfig, RabbitMQStreamConfig, RabbitMQStreamQueueConfig,
        SNSStreamConfig, StreamEvent, StreamsConfig, WebhookStreamConfig,
    },
    streams::{
        kafka::{Kafka, KafkaError},
        RabbitMQ, RabbitMQError, Webhook, WebhookError, SNS,
    },
};

// we should limit the max chunk size we send over when streaming to 70KB - 100KB is most limits
// we can add this to yaml if people need it
const MAX_CHUNK_SIZE: usize = 75 * 1024; // 75 KB

type StreamPublishes = Vec<JoinHandle<Result<(), StreamError>>>;

#[derive(Debug, Clone)]
struct SNSStream {
    config: Vec<SNSStreamConfig>,
    client: Arc<SNS>,
}

#[derive(Error, Debug)]
pub enum StreamError {
    #[error("SNS could not publish - {0}")]
    SnsCouldNotPublish(#[from] SdkError<PublishError, HttpResponse>),

    #[error("Webhook could not publish: {0}")]
    WebhookCouldNotPublish(#[from] WebhookError),

    #[error("RabbitMQ could not publish: {0}")]
    RabbitMQCouldNotPublish(#[from] RabbitMQError),

    #[error("Kafka could not publish: {0}")]
    KafkaCouldNotPublish(#[from] KafkaError),

    #[error("Task failed: {0}")]
    JoinError(JoinError),
}

#[derive(Debug, Clone)]
struct WebhookStream {
    config: Vec<WebhookStreamConfig>,
    client: Arc<Webhook>,
}

pub struct RabbitMQStream {
    config: RabbitMQStreamConfig,
    client: Arc<RabbitMQ>,
}

pub struct KafkaStream {
    config: KafkaStreamConfig,
    client: Arc<Kafka>,
}

pub struct StreamsClients {
    sns: Option<SNSStream>,
    webhook: Option<WebhookStream>,
    rabbitmq: Option<RabbitMQStream>,
    kafka: Option<KafkaStream>,
}

impl StreamsClients {
    pub async fn new(stream_config: StreamsConfig) -> Self {
        let sns = if let Some(config) = &stream_config.sns {
            Some(SNSStream { config: config.clone(), client: Arc::new(SNS::new().await) })
        } else {
            None
        };

        let webhook = stream_config.webhooks.as_ref().map(|config| WebhookStream {
            config: config.clone(),
            client: Arc::new(Webhook::new()),
        });

        let rabbitmq = if let Some(config) = stream_config.rabbitmq.as_ref() {
            Some(RabbitMQStream {
                config: config.clone(),
                client: Arc::new(RabbitMQ::new(&config.url).await),
            })
        } else {
            None
        };

        let kafka = if let Some(config) = stream_config.kafka.as_ref() {
            Some(KafkaStream {
                config: config.clone(),
                client: Arc::new(
                    Kafka::new(config)
                        .await
                        .unwrap_or_else(|e| panic!("Failed to create Kafka client: {:?}", e)),
                ),
            })
        } else {
            None
        };

        Self { sns, webhook, rabbitmq, kafka }
    }

    fn has_any_streams(&self) -> bool {
        self.sns.is_some() ||
            self.webhook.is_some() ||
            self.rabbitmq.is_some() ||
            self.kafka.is_some()
    }

    fn chunk_data(&self, data_array: &Vec<Value>) -> Vec<Vec<Value>> {
        let mut current_chunk = Vec::new();
        let mut current_size = 0;

        let mut chunks = Vec::new();
        for item in data_array {
            let item_str = serde_json::to_string(item).unwrap();
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

    fn create_chunk_message_raw(&self, event_message: &EventMessage, chunk: &[Value]) -> String {
        let chunk_message = EventMessage {
            event_name: event_message.event_name.clone(),
            event_data: Value::Array(chunk.to_vec()),
            network: event_message.network.clone(),
        };

        serde_json::to_string(&chunk_message).unwrap()
    }

    fn create_chunk_message_json(&self, event_message: &EventMessage, chunk: &[Value]) -> Value {
        let chunk_message = EventMessage {
            event_name: event_message.event_name.clone(),
            event_data: Value::Array(chunk.to_vec()),
            network: event_message.network.clone(),
        };

        serde_json::to_value(&chunk_message).unwrap()
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
    ) -> Vec<Value> {
        let stream_event = events
            .iter()
            .find(|e| e.event_name == event_message.event_name)
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

    fn sns_stream_tasks(
        &self,
        config: &SNSStreamConfig,
        client: Arc<SNS>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                );

                let publish_message_id =
                    self.generate_publish_message_id(id, index, &config.prefix_id);
                let client = Arc::clone(&client);
                let topic_arn = config.topic_arn.clone();
                let publish_message = self.create_chunk_message_raw(event_message, &filtered_chunk);
                task::spawn(async move {
                    let _ =
                        client.publish(&publish_message_id, &topic_arn, &publish_message).await?;

                    Ok(())
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
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let endpoint = config.endpoint.clone();
                let shared_secret = config.shared_secret.clone();
                let client = Arc::clone(&client);
                let publish_message =
                    self.create_chunk_message_json(event_message, &filtered_chunk);
                task::spawn(async move {
                    client
                        .publish(&publish_message_id, &endpoint, &shared_secret, &publish_message)
                        .await?;

                    Ok(())
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
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let exchange = config.exchange.clone();
                let exchange_type = config.exchange_type.clone();
                let routing_key = config.routing_key.clone();
                let publish_message =
                    self.create_chunk_message_json(event_message, &filtered_chunk);

                task::spawn(async move {
                    client
                        .publish(
                            &publish_message_id,
                            &exchange,
                            &exchange_type,
                            &routing_key,
                            &publish_message,
                        )
                        .await?;
                    Ok(())
                })
            })
            .collect();
        tasks
    }

    fn kafka_stream_tasks(
        &self,
        config: &KafkaStreamQueueConfig,
        client: Arc<Kafka>,
        id: &str,
        event_message: &EventMessage,
        chunks: Arc<Vec<Vec<Value>>>,
    ) -> StreamPublishes {
        let tasks: Vec<_> = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let filtered_chunk: Vec<Value> = self.filter_chunk_event_data_by_conditions(
                    &config.events,
                    event_message,
                    chunk,
                );

                let publish_message_id = self.generate_publish_message_id(id, index, &None);
                let client = Arc::clone(&client);
                let exchange = config.topic.clone();
                let routing_key = config.key.clone();
                let publish_message =
                    self.create_chunk_message_json(event_message, &filtered_chunk);
                task::spawn(async move {
                    client
                        .publish(&publish_message_id, &exchange, &routing_key, &publish_message)
                        .await?;
                    Ok(())
                })
            })
            .collect();
        tasks
    }

    pub async fn stream(
        &self,
        id: String,
        event_message: &EventMessage,
    ) -> Result<usize, StreamError> {
        if !self.has_any_streams() {
            return Ok(0);
        }

        // will always have something even if the event has no parameters due to the tx_information
        if let Value::Array(data_array) = &event_message.event_data {
            let chunks = Arc::new(self.chunk_data(data_array));
            let total_streamed: usize = chunks.iter().map(|chunk| chunk.len()).sum();

            let mut streams: Vec<StreamPublishes> = Vec::new();

            if let Some(sns) = &self.sns {
                for config in &sns.config {
                    if config.events.iter().any(|e| e.event_name == event_message.event_name) &&
                        config.networks.contains(&event_message.network)
                    {
                        streams.push(self.sns_stream_tasks(
                            config,
                            Arc::clone(&sns.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                        ));
                    }
                }
            };

            if let Some(webhook) = &self.webhook {
                for config in &webhook.config {
                    if config.events.iter().any(|e| e.event_name == event_message.event_name) &&
                        config.networks.contains(&event_message.network)
                    {
                        streams.push(self.webhook_stream_tasks(
                            config,
                            Arc::clone(&webhook.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                        ));
                    }
                }
            }

            if let Some(rabbitmq) = &self.rabbitmq {
                for config in &rabbitmq.config.exchanges {
                    if config.events.iter().any(|e| e.event_name == event_message.event_name) &&
                        config.networks.contains(&event_message.network)
                    {
                        streams.push(self.rabbitmq_stream_tasks(
                            config,
                            Arc::clone(&rabbitmq.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                        ));
                    }
                }
            }

            if let Some(kafka) = &self.kafka {
                for config in &kafka.config.topics {
                    if config.events.iter().any(|e| e.event_name == event_message.event_name) &&
                        config.networks.contains(&event_message.network)
                    {
                        streams.push(self.kafka_stream_tasks(
                            config,
                            Arc::clone(&kafka.client),
                            &id,
                            event_message,
                            Arc::clone(&chunks),
                        ));
                    }
                }
            }

            for stream in streams {
                for publish in stream {
                    match publish.await {
                        Ok(Ok(_)) => (),
                        Ok(Err(e)) => return Err(e),
                        Err(e) => return Err(StreamError::JoinError(e)),
                    }
                }
            }

            Ok(total_streamed)
        } else {
            unreachable!("Event data should be an array");
        }
    }
}
