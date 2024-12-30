use log::{error, info};
use thiserror::Error;
use redis::{cmd, AsyncCommands};
use redis::aio::MultiplexedConnection;
use serde_json::Value;

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
}

#[derive(Debug, Clone)]
pub struct Redis {
    client: redis::Client,
}

impl Redis {
    pub fn new(url: &str) -> Self {
        let client = redis::Client::open(url).unwrap();
        match client.get_connection() {
            Ok(mut c) => {
                match cmd("PING").query::<String>(&mut c) {
                    Ok(_) => info!("Successfully connected to Redis."),
                    Err(error) => {
                        error!("Error connecting to Redis: {}", error);
                        panic!("Error connecting to Redis: {}", error);
                    }
                }
            },
            Err(e) => {
                error!("Error connecting to Redis: {}", e);
                panic!("Error connecting to Redis: {}", e);
            }
        };

        Self { client }
    }

    pub async fn publish(&self, message_id: &str, stream_name: &str, message: &Value) -> Result<(), RedisError> {
        // redis stream message ids need to be a timestamp with guaranteed unique identification
        // so instead, we attach the message_id to the message value.
        let mut message_with_id = message.clone();
        if let Value::Object(ref mut map) = message_with_id {
            map.insert("message_id".to_string(), Value::String(message_id.to_string()));
        }

        let json_value = serde_json::to_string(&message_with_id).unwrap();
        let mut con: MultiplexedConnection = self.client.get_multiplexed_async_connection().await?;
        let _: String = con.xadd(stream_name, "*", &[("payload", &json_value)]).await?;

        Ok(())
    }
}