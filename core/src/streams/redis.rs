use std::sync::Arc;
use bb8_redis::bb8::{Pool, PooledConnection};
use bb8_redis::{RedisConnectionManager, redis::{cmd, AsyncCommands}};
use log::{error};
use thiserror::Error;
use serde_json::Value;
use crate::manifest::stream::RedisStreamConfig;

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("Redis error: {0}")]
    RedisError(#[from] bb8_redis::redis::RedisError),

    #[error("Redis pool error: {0}")]
    PoolError(#[from] bb8_redis::bb8::RunError<bb8_redis::redis::RedisError>),

    #[error("Could not serialize message: {0}")]
    CouldNotSerialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct Redis {
    client: Arc<Pool<RedisConnectionManager>>
}

async fn get_pooled_connection(pool: &Arc<Pool<RedisConnectionManager>>) -> Result<PooledConnection<RedisConnectionManager>, RedisError> {
    match pool.get().await {
        Ok(c) => Ok(c),
        Err(err) => {
            Err(RedisError::PoolError(err))
        }
    }
}

impl Redis {
    pub async fn new(config: &RedisStreamConfig) -> Result<Self, RedisError> {
        let connection_manager = RedisConnectionManager::new(config.connection_uri.as_str())?;
        let redis_pool = Arc::new(Pool::builder()
            .max_size(config.max_pool_size)
            .build(connection_manager).await?
        );

        let mut connection = get_pooled_connection(&redis_pool).await?;
        let _ = cmd("PING").query_async::<String>(&mut *connection).await?;

        Ok(Self { client: redis_pool.clone() })
    }

    pub async fn publish(&self, message_id: &str, stream_name: &str, message: &Value) -> Result<(), RedisError> {
        // redis stream message ids need to be a timestamp with guaranteed unique identification
        // so instead, we attach the message_id to the message value.
        let mut message_with_id = message.clone();
        if let Value::Object(ref mut map) = message_with_id {
            map.insert("message_id".to_string(), Value::String(message_id.to_string()));
        }

        let json_value = serde_json::to_string(&message_with_id)?;
        let mut con = get_pooled_connection(&self.client).await?;
        let _: String = con.xadd(stream_name, "*", &[("payload", &json_value)]).await?;

        Ok(())
    }
}