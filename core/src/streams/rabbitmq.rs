use bb8::{Pool, RunError};
use bb8_lapin::LapinConnectionManager;
use lapin::{
    options::*, types::FieldTable, BasicProperties, DefaultConnectionBuilder, ExchangeKind,
};
use serde_json::Value;

use crate::manifest::stream::ExchangeKindWrapper;
use crate::streams::publish_with_retry;

#[derive(thiserror::Error, Debug)]
pub enum RabbitMQError {
    #[error("Request error: {0}")]
    LapinError(#[from] lapin::Error),

    #[error("Could not parse message: {0}")]
    CouldNotParseMessage(#[from] serde_json::Error),

    #[error("Connection pool timed out")]
    PoolTimedOut,
}

impl From<RunError<lapin::ErrorKind>> for RabbitMQError {
    fn from(err: RunError<lapin::ErrorKind>) -> Self {
        match err {
            RunError::User(kind) => Self::LapinError(lapin::Error::from(kind)),
            RunError::TimedOut => Self::PoolTimedOut,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RabbitMQ {
    pool: Pool<LapinConnectionManager<async_rs::Tokio>>,
}

impl RabbitMQ {
    pub async fn new(uri: &str) -> Result<Self, RabbitMQError> {
        let builder = DefaultConnectionBuilder::new()
            .map_err(RabbitMQError::LapinError)?
            .with_uri_str(uri.to_string());
        let manager = LapinConnectionManager::new(builder);
        let pool = Pool::builder()
            .max_size(16)
            .build(manager)
            .await
            .map_err(|kind| RabbitMQError::LapinError(lapin::Error::from(kind)))?;

        Ok(Self { pool })
    }

    pub async fn publish(
        &self,
        id: &str,
        exchange: &str,
        exchange_type: &ExchangeKindWrapper,
        routing_key: &Option<String>,
        message: &Value,
    ) -> Result<(), RabbitMQError> {
        publish_with_retry("rabbitmq", exchange, || {
            self.publish_once(id, exchange, exchange_type, routing_key, message)
        })
        .await
    }

    async fn publish_once(
        &self,
        id: &str,
        exchange: &str,
        exchange_type: &ExchangeKindWrapper,
        routing_key: &Option<String>,
        message: &Value,
    ) -> Result<(), RabbitMQError> {
        let message_body = serde_json::to_vec(message)?;

        let conn = self.pool.get().await?;
        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                exchange.into(),
                exchange_type.0.clone(),
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        let routing_key: &str = match exchange_type.0 {
            ExchangeKind::Fanout => "", // Fanout exchange ignores the routing key
            _ => routing_key.as_deref().expect("Routing key should be defined"),
        };

        channel
            .basic_publish(
                exchange.into(),
                routing_key.into(),
                BasicPublishOptions::default(),
                &message_body,
                BasicProperties::default()
                    .with_message_id(id.into())
                    .with_content_type("application/json".into()),
            )
            .await?;

        Ok(())
    }
}
