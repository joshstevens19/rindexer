use lapin::ExchangeKind;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StreamEvent {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Map<String, Value>>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SNSStreamConfig {
    pub prefix_id: Option<String>,
    pub topic_arn: String,
    pub networks: Vec<String>,
    pub events: Vec<StreamEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WebhookStreamConfig {
    pub endpoint: String,
    pub networks: Vec<String>,
    pub events: Vec<StreamEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ExchangeKindWrapper(pub ExchangeKind);

impl<'de> Deserialize<'de> for ExchangeKindWrapper {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let kind = match s.to_lowercase().as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "headers" => ExchangeKind::Headers,
            "topic" => ExchangeKind::Topic,
            _ => ExchangeKind::Custom(s),
        };
        Ok(ExchangeKindWrapper(kind))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RabbitMQStreamQueueConfig {
    pub exchange: String,
    pub exchange_type: ExchangeKindWrapper,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
    pub networks: Vec<String>,
    pub events: Vec<StreamEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RabbitMQStreamConfig {
    pub url: String,
    pub exchanges: Vec<RabbitMQStreamQueueConfig>,
}

impl RabbitMQStreamConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.exchanges.is_empty() {
            return Err("No exchanges defined in RabbitMQ config".to_string());
        }

        for config in &self.exchanges {
            if config.exchange_type.0 != ExchangeKind::Direct &&
                config.exchange_type.0 != ExchangeKind::Fanout &&
                config.exchange_type.0 != ExchangeKind::Topic
            {
                return Err("Only direct, topic and fanout exchanges are supported".to_string());
            }

            if config.exchange_type.0 == ExchangeKind::Fanout && config.routing_key.is_some() {
                return Err("Fanout exchanges do not support routing keys".to_string());
            }

            if config.exchange_type.0 == ExchangeKind::Topic && config.routing_key.is_none() {
                return Err("Topic exchanges require a routing key".to_string());
            }

            if config.exchange_type.0 == ExchangeKind::Direct && config.routing_key.is_none() {
                return Err("Direct exchanges require a routing keys".to_string());
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KafkaStreamQueueConfig {
    pub topic: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub networks: Vec<String>,
    pub events: Vec<StreamEvent>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KafkaStreamConfig {
    pub brokers: Vec<String>,
    pub security_protocol: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_mechanisms: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_username: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sasl_password: Option<String>,

    pub acks: String,
    pub topics: Vec<KafkaStreamQueueConfig>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StreamsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sns: Option<Vec<SNSStreamConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<Vec<WebhookStreamConfig>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rabbitmq: Option<RabbitMQStreamConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka: Option<KafkaStreamConfig>,
}

impl StreamsConfig {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(rabbitmq) = &self.rabbitmq {
            return rabbitmq.validate();
        }

        Ok(())
    }
}
