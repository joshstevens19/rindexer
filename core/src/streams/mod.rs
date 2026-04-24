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
            Err(e) if attempt == PUBLISH_RETRY_ATTEMPTS => {
                tracing::error!(
                    publisher,
                    target,
                    attempts = PUBLISH_RETRY_ATTEMPTS,
                    error = %e,
                    "stream publish dropped after exhausting retries",
                );
                crate::metrics::streams::record_publish_dropped(publisher, target);
                return Err(e);
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::definitions::STREAM_PUBLISH_DROPPED_TOTAL;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test(start_paused = true)]
    async fn publish_with_retry_returns_ok_on_first_success() {
        let calls = AtomicU32::new(0);
        let result: Result<(), &'static str> = publish_with_retry("test", "ok", || async {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn publish_with_retry_retries_then_succeeds() {
        let calls = AtomicU32::new(0);
        let result: Result<(), &'static str> = publish_with_retry("test", "transient", || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 3 {
                    Err("nope")
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn publish_with_retry_exhausts_attempts_and_increments_dropped_metric() {
        // Capture the dropped-metric value for THIS specific target before
        // and after — the metric is a process-wide counter so we measure
        // the delta on a unique label rather than the absolute value.
        let target = "drop_metric_unique_target_0xA";
        let before =
            STREAM_PUBLISH_DROPPED_TOTAL.with_label_values(&["test_publisher", target]).get();

        let calls = AtomicU32::new(0);
        let result: Result<(), &'static str> = publish_with_retry("test_publisher", target, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err("persistent") }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), PUBLISH_RETRY_ATTEMPTS);

        let after =
            STREAM_PUBLISH_DROPPED_TOTAL.with_label_values(&["test_publisher", target]).get();
        assert!(
            (after - before - 1.0).abs() < f64::EPSILON,
            "expected exactly one drop to be recorded (before={before}, after={after})"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn publish_with_retry_does_not_record_drop_on_eventual_success() {
        // If a retry ultimately succeeds, the drop counter must NOT fire —
        // that'd create false alerts every time a transient hiccup is
        // recovered within the retry budget.
        let target = "drop_metric_unique_target_0xB";
        let before =
            STREAM_PUBLISH_DROPPED_TOTAL.with_label_values(&["test_publisher", target]).get();

        let calls = AtomicU32::new(0);
        let _: Result<(), &'static str> = publish_with_retry("test_publisher", target, || {
            let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n < 2 {
                    Err("hiccup")
                } else {
                    Ok(())
                }
            }
        })
        .await;

        let after =
            STREAM_PUBLISH_DROPPED_TOTAL.with_label_values(&["test_publisher", target]).get();
        assert!((after - before).abs() < f64::EPSILON, "no drop expected on eventual success");
    }
}
