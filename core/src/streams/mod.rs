mod sns;
pub use sns::SNS;

mod webhook;
pub use webhook::{Webhook, WebhookError};

mod rabbitmq;
pub use rabbitmq::{RabbitMQ, RabbitMQError};

#[cfg(feature = "kafka")]
mod kafka;

mod clients;

mod redis;
pub use clients::StreamsClients;
pub use redis::{Redis, RedisError};

mod cloudflare_queues;
pub use cloudflare_queues::{CloudflareQueues, CloudflareQueuesError};

pub const STREAM_MESSAGE_ID_KEY: &str = "x-rindexer-id";

// Per-publisher retry absorbs transient failures before they reach the
// caller. The user handler in `indexer::no_code` is all-or-nothing: if a
// publish returns Err, the callback-level retry re-runs the whole closure,
// including PG/ClickHouse/CSV inserts that already succeeded above. Event
// tables have no natural UNIQUE constraint, so that retry produces
// duplicate rows. Keeping retries local here turns a momentary socket
// glitch into a warn, not a correctness bug.

use std::future::Future;
use std::time::Duration;

const PUBLISH_RETRY_ATTEMPTS: u32 = 3;
const PUBLISH_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(100);

pub(crate) async fn publish_with_retry<T, E, F, Fut>(
    publisher: &'static str,
    target: &str,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut delay = PUBLISH_RETRY_INITIAL_DELAY;
    for attempt in 1..=PUBLISH_RETRY_ATTEMPTS {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt == PUBLISH_RETRY_ATTEMPTS => return Err(e),
            Err(e) => {
                tracing::warn!(
                    publisher,
                    target,
                    attempt,
                    max_attempts = PUBLISH_RETRY_ATTEMPTS,
                    retry_in_ms = delay.as_millis() as u64,
                    error = %e,
                    "stream publish attempt failed, retrying",
                );
                tokio::time::sleep(delay).await;
                delay *= 2;
            }
        }
    }
    unreachable!("retry loop returns on every path when PUBLISH_RETRY_ATTEMPTS >= 1")
}
