use std::time::Duration;

#[cfg(not(windows))]
use rdkafka::{
    config::ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};
use serde_json::Value;
use thiserror::Error;

use crate::{manifest::stream::KafkaStreamConfig, streams::STREAM_MESSAGE_ID_KEY};

#[derive(Error, Debug)]
pub enum KafkaError {
    #[error("Kafka error: {0}")]
    RdkafkaError(String),

    #[error("Could not parse message: {0}")]
    CouldNotParseMessage(#[from] serde_json::Error),
}

#[derive(Clone)]
pub struct Kafka {
    #[cfg(not(windows))]
    producer: FutureProducer,
}

impl Kafka {
    pub async fn new(config: &KafkaStreamConfig) -> Result<Self, KafkaError> {
        #[cfg(not(windows))]
        {
            let servers_list = config.brokers.join(",");
            let mut client_config = ClientConfig::new();

            client_config
                .set("bootstrap.servers", &servers_list)
                .set("security.protocol", &config.security_protocol)
                .set("acks", &config.acks);

            if let Some(ref sasl_mechanisms) = config.sasl_mechanisms {
                client_config.set("sasl.mechanisms", sasl_mechanisms);
            }
            if let Some(ref sasl_username) = config.sasl_username {
                client_config.set("sasl.username", sasl_username);
            }
            if let Some(ref sasl_password) = config.sasl_password {
                client_config.set("sasl.password", sasl_password);
            }

            let producer: FutureProducer =
                client_config.create().map_err(|e| KafkaError::RdkafkaError(e.to_string()))?;

            Ok(Self { producer })
        }

        #[cfg(windows)]
        {
            panic!("Kafka is not supported on Windows")
        }
    }

    pub async fn publish(
        &self,
        id: &str,
        topic: &str,
        key: &Option<String>,
        message: &Value,
    ) -> Result<(), KafkaError> {
        #[cfg(not(windows))]
        {
            let message_body = serde_json::to_vec(message)?;

            let record = if key.is_some() {
                FutureRecord::to(topic).key(key.as_ref().unwrap()).payload(&message_body).headers(
                    OwnedHeaders::new()
                        .insert(Header { key: STREAM_MESSAGE_ID_KEY, value: Some(id) }),
                )
            } else {
                FutureRecord::to(topic).payload(&message_body).headers(
                    OwnedHeaders::new()
                        .insert(Header { key: STREAM_MESSAGE_ID_KEY, value: Some(id) }),
                )
            };

            self.producer
                .send(record, Timeout::After(Duration::from_secs(0)))
                .await
                .map_err(|(e, _)| KafkaError::RdkafkaError(e.to_string()))?;

            Ok(())
        }

        #[cfg(windows)]
        {
            panic!("Kafka is not supported on Windows")
        }
    }
}
