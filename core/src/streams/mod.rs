#[cfg(feature = "sns")]
mod sns;
#[cfg(feature = "sns")]
pub use sns::SNS;

mod webhook;
pub use webhook::{Webhook, WebhookError};

#[cfg(feature = "rabbitmq")]
mod rabbitmq;
#[cfg(feature = "rabbitmq")]
pub use rabbitmq::{RabbitMQ, RabbitMQError};

#[cfg(feature = "kafka")]
mod kafka;

mod clients;

mod redis;
pub use clients::StreamsClients;
pub use redis::{Redis, RedisError};

pub const STREAM_MESSAGE_ID_KEY: &str = "x-rindexer-id";
