//! Integration tests for `StreamDeliveryMode::Finalized`.
//!
//! Covers the buffer-then-flush lifecycle end-to-end via the public
//! `StreamsClients` surface. Uses `mockito` to stand in for a webhook
//! endpoint and asserts on exactly when HTTP requests reach it.

use alloy::primitives::B256;
use serde_json::json;

use rindexer::event::EventMessage;
use rindexer::manifest::stream::{
    StreamDeliveryMode, StreamEvent, StreamsConfig, WebhookStreamConfig,
};
use rindexer::StreamsClients;

const NETWORK: &str = "ethereum";
const REORG_SAFE_DISTANCE: u64 = 5;

fn stream_event(name: &str) -> StreamEvent {
    StreamEvent { event_name: name.to_string(), conditions: None, alias: None }
}

fn webhook_config(endpoint: &str, delivery: Option<StreamDeliveryMode>) -> WebhookStreamConfig {
    WebhookStreamConfig {
        endpoint: endpoint.to_string(),
        shared_secret: "test-secret".to_string(),
        networks: vec![NETWORK.to_string()],
        events: vec![stream_event("Transfer")],
        delivery,
    }
}

async fn streams_clients_for(webhooks: Vec<WebhookStreamConfig>) -> StreamsClients {
    let config = StreamsConfig {
        sns: None,
        webhooks: Some(webhooks),
        rabbitmq: None,
        #[cfg(feature = "kafka")]
        kafka: None,
        redis: None,
        cloudflare_queues: None,
    };
    let clients = StreamsClients::new(config).await;
    clients.register_network_reorg_distance(NETWORK.to_string(), REORG_SAFE_DISTANCE);
    clients
}

fn event_message_at(block_number: u64) -> EventMessage {
    EventMessage {
        event_name: "Transfer".to_string(),
        event_data: json!([{"from": "0x1", "to": "0x2", "value": "100", "block": block_number}]),
        event_signature_hash: B256::ZERO,
        network: NETWORK.to_string(),
        block_number,
    }
}

/// Happy path: `Finalized` events buffer until the block is buried by
/// `reorg_safe_distance`, then flush on the next `flush_finalized`.
#[tokio::test]
async fn finalized_flushes_only_after_reorg_safe_distance() {
    let mut server = mockito::Server::new_async().await;
    let mock = server.mock("POST", "/hook").with_status(200).expect(1).create_async().await;

    let clients = streams_clients_for(vec![webhook_config(
        &format!("{}/hook", server.url()),
        Some(StreamDeliveryMode::Finalized),
    )])
    .await;

    // Stream at block 100.
    let sent = clients.stream("id1".to_string(), &event_message_at(100), false, false).await;
    assert_eq!(sent.unwrap(), 0, "Finalized stream must not publish immediately");

    // Head at 104 → finality threshold = 99 → block 100 still not safe.
    let flushed = clients.flush_finalized(NETWORK, 104).await.unwrap();
    assert_eq!(flushed, 0, "Not yet finalized");

    // Head at 105 → threshold = 100 → block 100 now safe, flushes.
    let flushed = clients.flush_finalized(NETWORK, 105).await.unwrap();
    assert_eq!(flushed, 1, "Should flush exactly the one buffered event");

    // Buffer drained — a subsequent flush is a no-op.
    let flushed = clients.flush_finalized(NETWORK, 110).await.unwrap();
    assert_eq!(flushed, 0, "Buffer should be empty after flush");

    mock.assert_async().await;
}

/// Reorg discard: events in `[fork_point, detection_point]` are purged before
/// any subsequent flush could leak them to the webhook.
#[tokio::test]
async fn finalized_discards_invalidated_events_on_reorg() {
    let mut server = mockito::Server::new_async().await;
    // Only one event (block 100) should ever reach the mock — 101, 102 are
    // discarded before flush can see them.
    let mock = server.mock("POST", "/hook").with_status(200).expect(1).create_async().await;

    let clients = streams_clients_for(vec![webhook_config(
        &format!("{}/hook", server.url()),
        Some(StreamDeliveryMode::Finalized),
    )])
    .await;

    clients.stream("id100".to_string(), &event_message_at(100), false, false).await.unwrap();
    clients.stream("id101".to_string(), &event_message_at(101), false, false).await.unwrap();
    clients.stream("id102".to_string(), &event_message_at(102), false, false).await.unwrap();

    clients.discard_finalized(NETWORK, 101, 102).await;

    let flushed = clients.flush_finalized(NETWORK, 110).await.unwrap();
    assert_eq!(flushed, 1, "Only block 100 should survive the discard + flush");

    mock.assert_async().await;
}

/// Mixed mode: two webhooks on the same `StreamsClients`, one `Instant`, one
/// `Finalized`. Instant fires immediately; Finalized waits for flush.
#[tokio::test]
async fn mixed_instant_and_finalized_on_same_clients() {
    let mut instant_server = mockito::Server::new_async().await;
    let mut finalized_server = mockito::Server::new_async().await;

    let instant_mock =
        instant_server.mock("POST", "/instant").with_status(200).expect(1).create_async().await;
    let finalized_mock =
        finalized_server.mock("POST", "/finalized").with_status(200).expect(1).create_async().await;

    let clients = streams_clients_for(vec![
        WebhookStreamConfig {
            endpoint: format!("{}/instant", instant_server.url()),
            shared_secret: "s".to_string(),
            networks: vec![NETWORK.to_string()],
            events: vec![stream_event("Transfer")],
            delivery: Some(StreamDeliveryMode::Instant),
        },
        WebhookStreamConfig {
            endpoint: format!("{}/finalized", finalized_server.url()),
            shared_secret: "s".to_string(),
            networks: vec![NETWORK.to_string()],
            events: vec![stream_event("Transfer")],
            delivery: Some(StreamDeliveryMode::Finalized),
        },
    ])
    .await;

    // A single stream() call — instant webhook receives it immediately, finalized doesn't.
    let sent =
        clients.stream("id".to_string(), &event_message_at(100), false, false).await.unwrap();
    assert_eq!(sent, 1, "Exactly the instant endpoint publishes immediately");

    instant_mock.assert_async().await;

    // Now head passes the reorg_safe_distance threshold — finalized drains.
    let flushed = clients.flush_finalized(NETWORK, 105).await.unwrap();
    assert_eq!(flushed, 1, "Finalized webhook should receive on flush");

    finalized_mock.assert_async().await;
}

/// Reorg notifications (`stream_reorg`) bypass the finalized buffer entirely —
/// operators need these alerts immediately, not deferred.
#[tokio::test]
async fn reorg_notifications_bypass_finalized_buffer() {
    let mut server = mockito::Server::new_async().await;
    let mock = server.mock("POST", "/hook").with_status(200).expect(1).create_async().await;

    let clients = streams_clients_for(vec![webhook_config(
        &format!("{}/hook", server.url()),
        Some(StreamDeliveryMode::Finalized),
    )])
    .await;

    // Fire a reorg notification — expect immediate dispatch despite Finalized.
    let sent =
        clients.stream_reorg(NETWORK, 100, 2, 0, &[B256::ZERO], &[]).await.expect("stream_reorg");
    assert_eq!(sent, 1, "Reorg notification should publish immediately, not buffer");

    mock.assert_async().await;
}
