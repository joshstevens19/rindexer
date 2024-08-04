use deadpool::managed::PoolError;
use deadpool_lapin::{Manager, Pool};
use lapin::{options::*, types::FieldTable, BasicProperties, ConnectionProperties};
use serde_json::Value;

#[derive(thiserror::Error, Debug)]
pub enum RabbitMQError {
    #[error("Request error: {0}")]
    LapinError(#[from] lapin::Error),

    #[error("Could not parse message: {0}")]
    CouldNotParseMessage(#[from] serde_json::Error),

    #[error("Connection pool error")]
    PoolError(#[from] PoolError<lapin::Error>),
}

#[derive(Debug, Clone)]
pub struct RabbitMQ {
    pool: Pool,
}

impl RabbitMQ {
    pub async fn new(uri: &str) -> Self {
        let manager = Manager::new(uri, ConnectionProperties::default());
        let pool = Pool::builder(manager).max_size(16).build().expect("Failed to create pool");

        Self { pool }
    }

    pub async fn publish(
        &self,
        id: &str,
        exchange: &str,
        routing_key: &str,
        message: &Value,
    ) -> Result<(), RabbitMQError> {
        let message_body = serde_json::to_vec(message)?;

        let conn = self.pool.get().await?;
        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                exchange,
                lapin::ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        channel
            .basic_publish(
                exchange,
                routing_key,
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
