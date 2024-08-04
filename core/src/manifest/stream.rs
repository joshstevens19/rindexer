use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SNSStreamConfig {
    pub prefix_id: Option<String>,
    pub topic_arn: String,
    pub networks: Vec<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WebhookStreamConfig {
    pub endpoint: String,
    pub networks: Vec<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RabbitMQStreamQueueConfig {
    pub exchange: String,
    pub routing_key: String,
    pub networks: Vec<String>,
    pub events: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RabbitMQStreamConfig {
    pub url: String,
    pub exchanges: Vec<RabbitMQStreamQueueConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StreamsConfig {
    pub sns: Option<Vec<SNSStreamConfig>>,
    pub webhook: Option<Vec<WebhookStreamConfig>>,
    pub rabbitmq: Option<RabbitMQStreamConfig>,
}
