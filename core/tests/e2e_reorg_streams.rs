//! Integration tests for `StreamsClients::stream_reorg` across each stream type.
//!
//! The goal is to confirm that a `__rindexer_reorg` payload actually reaches
//! each concrete broker/endpoint when the stream is configured, with the
//! expected identifying markers in the body.
//!
//! Coverage:
//! - Redis — testcontainers `redis` image, `XREAD` from the configured stream.
//! - RabbitMQ — testcontainers `rabbitmq` image, consume a queue bound to
//!   the configured exchange.
//! - Kafka — testcontainers `kafka` image (KRaft, no Zookeeper),
//!   `#[cfg(feature = "kafka")]` only.
//! - Cloudflare — mockito server in place of the Cloudflare REST API.
//!
//! SNS is deliberately deferred. `testcontainers-modules 0.15` does ship a
//! `localstack` module, and `AwsConfig.endpoint_url` already lets the SNS
//! client target a non-AWS endpoint — so an integration test is technically
//! feasible. It's held back for two reasons: (1) the plan's original scoping
//! (Task 6 step 2e) explicitly defers it; (2) `SNS::new` performs an eager
//! `list_topics` handshake on construction and panics on failure, which
//! makes a robust localstack bring-up fiddly (requires pre-creating the
//! topic ARN before `StreamsClients::new` is called). Future work: wire up
//! a localstack container, create the topic via the AWS SDK, then construct
//! `StreamsClients` with the matching topic ARN.
//!
//! All tests are `#[ignore]` and require a working Docker daemon. Run with:
//!   cargo test -q -p rindexer --test e2e_reorg_streams -- --ignored
//!   cargo test -q -p rindexer --test e2e_reorg_streams --features kafka -- --ignored

use std::time::Duration;

use alloy::primitives::B256;
use serde_json::{json, Value};

use rindexer::manifest::stream::{
    CloudflareQueuesStreamConfig, CloudflareQueuesStreamQueueConfig, ExchangeKindWrapper,
    RabbitMQStreamConfig, RabbitMQStreamQueueConfig, RedisStreamConfig, RedisStreamStreamConfig,
    StreamsConfig,
};
use rindexer::StreamsClients;

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

/// A hash we pass into `stream_reorg` to assert on round-tripped payloads.
const MARKER_HASH_HEX: &str = "0x0101010101010101010101010101010101010101010101010101010101010101";

fn marker_hash() -> B256 {
    MARKER_HASH_HEX.parse().expect("valid B256")
}

/// Invoke `stream_reorg` with a canonical payload. Returns the count reported
/// by the call.
async fn publish_reorg(clients: &StreamsClients) -> usize {
    clients
        .stream_reorg(
            "ethereum",
            100,
            2,
            7,
            &[marker_hash()],
            &[], // no affected tables — keep payload tight for assertions
        )
        .await
        .expect("stream_reorg should succeed")
}

/// Asserts that a JSON blob representing the outer `EventMessage` carries the
/// expected reorg identifiers:
/// - `event_name == "__rindexer_reorg"`
/// - `network == "ethereum"`
/// - `event_data[0]` contains the marker hash in `affected_tx_hashes`
fn assert_reorg_envelope(payload: &Value) {
    assert_eq!(
        payload["event_name"], "__rindexer_reorg",
        "event_name mismatch, got payload: {payload}"
    );
    assert_eq!(payload["network"], "ethereum", "network mismatch, got payload: {payload}");
    let inner = &payload["event_data"][0];
    assert_eq!(inner["type"], "reorg", "missing reorg marker, got payload: {payload}");
    assert_eq!(inner["fork_block"], 100);
    assert_eq!(inner["depth"], 2);
    assert_eq!(inner["events_deleted"], 7);
    let hashes = inner["affected_tx_hashes"]
        .as_array()
        .unwrap_or_else(|| panic!("affected_tx_hashes should be an array: {payload}"));
    assert!(
        hashes.iter().any(|h| h.as_str() == Some(MARKER_HASH_HEX)),
        "missing marker hash in affected_tx_hashes: {payload}"
    );
}

// ---------------------------------------------------------------------------
// Redis
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn stream_reorg_reaches_redis() {
    use bb8_redis::redis::{cmd, AsyncCommands, Value as RedisValue};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::redis::{Redis as RedisImage, REDIS_PORT};

    let redis = RedisImage::default().start().await.expect("start redis");
    let host = redis.get_host().await.expect("redis host");
    let port = redis.get_host_port_ipv4(REDIS_PORT).await.expect("redis port");
    let url = format!("redis://{host}:{port}");

    let stream_name = "rindexer_reorg_test";
    let redis_config = RedisStreamConfig {
        connection_uri: url.clone(),
        max_pool_size: 4,
        streams: vec![RedisStreamStreamConfig {
            stream_name: stream_name.to_string(),
            networks: vec!["ethereum".to_string()],
            // Intentionally empty — reorg routing must bypass event-name filtering.
            events: vec![],
            delivery: None,
        }],
    };

    let clients = StreamsClients::new(StreamsConfig {
        sns: None,
        webhooks: None,
        rabbitmq: None,
        #[cfg(feature = "kafka")]
        kafka: None,
        redis: Some(redis_config),
        cloudflare_queues: None,
    })
    .await;

    let streamed = publish_reorg(&clients).await;
    assert_eq!(streamed, 1, "should publish exactly one reorg payload");

    // Read it back with a plain redis client so we don't depend on
    // rindexer::streams::Redis internals.
    let client = redis::Client::open(url.as_str()).expect("redis client");
    let mut con = client.get_multiplexed_async_connection().await.expect("redis connection");

    // XLEN — confirm we have a single entry.
    let len: i64 = cmd("XLEN").arg(stream_name).query_async(&mut con).await.expect("XLEN");
    assert_eq!(len, 1, "expected exactly one XADD into the stream");

    // XRANGE to fetch the entry.
    let entries: Vec<(String, Vec<(String, RedisValue)>)> = con
        .xrange_all(stream_name)
        .await
        .expect("xrange");
    assert_eq!(entries.len(), 1);
    let (_id, fields) = &entries[0];

    let payload_str = fields
        .iter()
        .find_map(|(k, v)| {
            if k == "payload" {
                match v {
                    RedisValue::BulkString(b) => String::from_utf8(b.clone()).ok(),
                    RedisValue::SimpleString(s) => Some(s.clone()),
                    _ => None,
                }
            } else {
                None
            }
        })
        .expect("payload field missing");

    let payload: Value = serde_json::from_str(&payload_str).expect("payload JSON");
    assert_reorg_envelope(&payload);
    // The Redis publish path injects `message_id` alongside the EventMessage fields.
    assert!(
        payload["message_id"].as_str().is_some(),
        "expected message_id in redis payload: {payload}"
    );
}

// ---------------------------------------------------------------------------
// RabbitMQ
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires Docker"]
async fn stream_reorg_reaches_rabbitmq() {
    use futures::StreamExt;
    use lapin::{
        options::{BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
        types::FieldTable,
        Connection, ConnectionProperties, ExchangeKind,
    };
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::rabbitmq::RabbitMq as RabbitMqImage;

    let rabbit = RabbitMqImage::default().start().await.expect("start rabbitmq");
    let host = rabbit.get_host().await.expect("rabbitmq host");
    let port = rabbit.get_host_port_ipv4(5672).await.expect("rabbitmq amqp port");
    let url = format!("amqp://{host}:{port}");

    // Pre-declare the exchange and bind a queue BEFORE the rindexer client
    // publishes, so the message isn't dropped.
    let consumer_conn = Connection::connect(&url, ConnectionProperties::default())
        .await
        .expect("consumer connect");
    let channel = consumer_conn.create_channel().await.expect("channel");

    let exchange = "rindexer_reorg_exchange";
    let routing_key = "reorg";
    let queue_name = "rindexer_reorg_queue";

    channel
        .exchange_declare(
            exchange,
            ExchangeKind::Direct,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("exchange_declare");

    channel
        .queue_declare(queue_name, QueueDeclareOptions::default(), FieldTable::default())
        .await
        .expect("queue_declare");

    channel
        .queue_bind(
            queue_name,
            exchange,
            routing_key,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("queue_bind");

    let mut consumer = channel
        .basic_consume(
            queue_name,
            "e2e-reorg-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("basic_consume");

    let rabbit_config = RabbitMQStreamConfig {
        url,
        exchanges: vec![RabbitMQStreamQueueConfig {
            exchange: exchange.to_string(),
            exchange_type: ExchangeKindWrapper(ExchangeKind::Direct),
            routing_key: Some(routing_key.to_string()),
            networks: vec!["ethereum".to_string()],
            events: vec![],
            delivery: None,
        }],
    };

    let clients = StreamsClients::new(StreamsConfig {
        sns: None,
        webhooks: None,
        rabbitmq: Some(rabbit_config),
        #[cfg(feature = "kafka")]
        kafka: None,
        redis: None,
        cloudflare_queues: None,
    })
    .await;

    let streamed = publish_reorg(&clients).await;
    assert_eq!(streamed, 1);

    let delivery = tokio::time::timeout(Duration::from_secs(15), consumer.next())
        .await
        .expect("consumer timed out")
        .expect("no delivery received")
        .expect("delivery error");

    let payload: Value = serde_json::from_slice(&delivery.data).expect("payload JSON");
    assert_reorg_envelope(&payload);
    assert_eq!(delivery.exchange.as_str(), exchange);
    assert_eq!(delivery.routing_key.as_str(), routing_key);
}

// ---------------------------------------------------------------------------
// Kafka (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires Docker"]
async fn stream_reorg_reaches_kafka() {
    use futures::StreamExt;
    use rdkafka::{
        consumer::{Consumer, StreamConsumer},
        ClientConfig, Message,
    };
    use rindexer::manifest::stream::{KafkaStreamConfig, KafkaStreamQueueConfig};
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::kafka::apache::{Kafka as KafkaImage, KAFKA_PORT};

    let kafka = KafkaImage::default().start().await.expect("start kafka");
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.expect("kafka port");
    let bootstrap = format!("127.0.0.1:{port}");

    let topic = "rindexer_reorg_topic";

    // Subscribe BEFORE producing with `earliest` offset reset so we don't miss the message.
    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "e2e-reorg-consumer")
        .set("bootstrap.servers", &bootstrap)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("kafka consumer");
    consumer.subscribe(&[topic]).expect("subscribe");

    let kafka_config = KafkaStreamConfig {
        brokers: vec![bootstrap.clone()],
        security_protocol: "PLAINTEXT".to_string(),
        sasl_mechanisms: None,
        sasl_username: None,
        sasl_password: None,
        acks: "all".to_string(),
        topics: vec![KafkaStreamQueueConfig {
            topic: topic.to_string(),
            key: Some("reorg".to_string()),
            networks: vec!["ethereum".to_string()],
            events: vec![],
            delivery: None,
        }],
    };

    let clients = StreamsClients::new(StreamsConfig {
        sns: None,
        webhooks: None,
        rabbitmq: None,
        kafka: Some(kafka_config),
        redis: None,
        cloudflare_queues: None,
    })
    .await;

    let streamed = publish_reorg(&clients).await;
    assert_eq!(streamed, 1);

    let mut stream = consumer.stream();
    let msg = tokio::time::timeout(Duration::from_secs(30), stream.next())
        .await
        .expect("kafka consumer timed out")
        .expect("no message")
        .expect("kafka delivery error");

    let body = msg.payload().expect("message payload");
    let payload: Value = serde_json::from_slice(body).expect("kafka payload JSON");
    assert_reorg_envelope(&payload);

    // Record key is the configured `key`, not the event name.
    assert_eq!(msg.key().map(|k| std::str::from_utf8(k).unwrap()), Some("reorg"));
}

// ---------------------------------------------------------------------------
// Cloudflare Queues (mockito — no Docker required, but tagged ignore for
// parity and to keep this file runnable in CI under a single command).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_reorg_reaches_cloudflare_queues() {
    let mut server = mockito::Server::new_async().await;

    let mock = server
        .mock("POST", "/client/v4/accounts/acc-123/queues/q-reorg/messages")
        .match_header("authorization", "Bearer test-token")
        .match_body(mockito::Matcher::PartialJson(json!({
            "body": {
                "event_name": "__rindexer_reorg",
                "network": "ethereum",
                "event_data": [{
                    "type": "reorg",
                    "network": "ethereum",
                    "fork_block": 100,
                    "depth": 2,
                    "events_deleted": 7,
                    "affected_tx_hashes": [MARKER_HASH_HEX],
                }],
            }
        })))
        .with_status(200)
        .create_async()
        .await;

    let cloudflare_config = CloudflareQueuesStreamConfig {
        api_token: "test-token".to_string(),
        account_id: "acc-123".to_string(),
        queues: vec![CloudflareQueuesStreamQueueConfig {
            queue_id: "q-reorg".to_string(),
            networks: vec!["ethereum".to_string()],
            events: vec![],
            delivery: None,
        }],
    };

    // Construct via public API so we exercise the full `StreamsClients::new` path.
    let mut clients = StreamsClients::new(StreamsConfig {
        sns: None,
        webhooks: None,
        rabbitmq: None,
        #[cfg(feature = "kafka")]
        kafka: None,
        redis: None,
        cloudflare_queues: Some(cloudflare_config),
    })
    .await;

    // Swap the internal client to a mockito-backed one so the POST hits the
    // test server rather than the real Cloudflare API.
    clients.set_cloudflare_base_url_for_test(&server.url());

    let streamed = publish_reorg(&clients).await;
    assert_eq!(streamed, 1);
    mock.assert_async().await;
}

