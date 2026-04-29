use std::sync::Arc;

use bb8_redis::{
    bb8::{self, Pool, PooledConnection},
    redis::{cmd, AsyncCommands},
    RedisConnectionManager,
};
use serde_json::Value;
use thiserror::Error;

use crate::manifest::stream::RedisStreamConfig;
use crate::streams::publish_with_retry;

#[derive(Error, Debug)]
pub enum RedisError {
    #[error("Redis error: {0}")]
    RedisCmdError(#[from] redis::RedisError),

    #[error("Redis pool error: {0}")]
    PoolError(#[from] bb8::RunError<redis::RedisError>),

    #[error("Could not serialize message: {0}")]
    CouldNotSerialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub struct Redis {
    client: Arc<Pool<RedisConnectionManager>>,
}

async fn get_pooled_connection(
    pool: &'_ Arc<Pool<RedisConnectionManager>>,
) -> Result<PooledConnection<'_, RedisConnectionManager>, RedisError> {
    match pool.get().await {
        Ok(c) => Ok(c),
        Err(err) => Err(RedisError::PoolError(err)),
    }
}

impl Redis {
    pub async fn new(config: &RedisStreamConfig) -> Result<Self, RedisError> {
        let connection_manager = RedisConnectionManager::new(config.connection_uri.as_str())?;
        let redis_pool = Arc::new(
            Pool::builder().max_size(config.max_pool_size).build(connection_manager).await?,
        );

        let mut connection = get_pooled_connection(&redis_pool).await?;
        let _ = cmd("PING").query_async::<String>(&mut *connection).await?;

        Ok(Self { client: redis_pool.clone() })
    }

    pub async fn publish(
        &self,
        message_id: &str,
        stream_name: &str,
        message: &Value,
    ) -> Result<(), RedisError> {
        publish_with_retry("redis", stream_name, || {
            self.publish_once(message_id, stream_name, message)
        })
        .await
    }

    async fn publish_once(
        &self,
        message_id: &str,
        stream_name: &str,
        message: &Value,
    ) -> Result<(), RedisError> {
        // redis stream message ids need to be a timestamp with guaranteed unique identification
        // so instead, we attach the message_id to the message value.
        let mut message_with_id = message.clone();
        if let Value::Object(ref mut map) = message_with_id {
            map.insert("message_id".to_string(), Value::String(message_id.to_string()));
        }

        let json_value = serde_json::to_string(&message_with_id)?;
        let mut con = get_pooled_connection(&self.client).await?;
        let _: () = con.xadd(stream_name, "*", &[("payload", &json_value)]).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::manifest::stream::{RedisStreamConfig, RedisStreamStreamConfig};

    fn cfg(uri: &str) -> RedisStreamConfig {
        RedisStreamConfig {
            connection_uri: uri.to_string(),
            max_pool_size: 1,
            streams: vec![RedisStreamStreamConfig {
                stream_name: "rindexer_unit".to_string(),
                networks: vec!["ethereum".to_string()],
                events: vec![],
                delivery: None,
            }],
        }
    }

    #[tokio::test]
    async fn new_returns_error_on_malformed_uri() {
        // `RedisConnectionManager::new` parses the URI eagerly via
        // `IntoConnectionInfo`, so a non-redis scheme must fail synchronously
        // before any pool/PING work runs.
        let err = Redis::new(&cfg("not-a-valid-uri")).await.expect_err("expected URI parse error");
        assert!(matches!(err, RedisError::RedisCmdError(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn new_does_not_succeed_against_unreachable_server() {
        // Port 1 is reserved (tcpmux); no redis can bind there. We bound the
        // wait with `tokio::time::timeout` rather than letting bb8's default
        // 30s `connection_timeout` dominate the test runtime — either an
        // outright `Err` from `Redis::new` or a tokio-level elapsed timeout
        // is acceptable; the only forbidden outcome is `Ok`.
        let result =
            tokio::time::timeout(Duration::from_secs(3), Redis::new(&cfg("redis://127.0.0.1:1")))
                .await;
        match result {
            Err(_elapsed) => {} // bb8 still trying — fine, proves no fast Ok.
            Ok(Err(_)) => {}    // Redis::new bubbled an error — also fine.
            Ok(Ok(_)) => panic!("Redis::new must not succeed against an unreachable server"),
        }
    }
}
