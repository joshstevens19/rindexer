use std::time::Duration;

use rindexer::{
    adaptive_concurrency::AdaptiveConcurrency,
    manifest::{
        chat::ChatConfig,
        global::Global,
        graphql::GraphQLSettings,
        storage::Storage,
        stream::{RabbitMQStreamConfig, StreamsConfig},
    },
    StringOrArray,
};

#[test]
fn storage_rejects_postgres_and_clickhouse_together() {
    let yaml = r#"
postgres:
  enabled: true
clickhouse:
  enabled: true
"#;

    let err = serde_yaml::from_str::<Storage>(yaml).expect_err("storage should reject both dbs");

    assert!(err.to_string().contains("cannot specify both `postgres` and `clickhouse`"));
}

#[test]
fn storage_defaults_disabled_sinks_to_safe_create_flags() {
    let storage: Storage = serde_yaml::from_str("{}").expect("empty storage should parse");

    assert!(!storage.postgres_enabled());
    assert!(storage.postgres_disable_create_tables());
    assert!(!storage.postgres_drop_each_run());
    assert!(!storage.clickhouse_enabled());
    assert!(storage.clickhouse_disable_create_tables());
    assert!(!storage.clickhouse_drop_each_run());
    assert!(!storage.csv_enabled());
    assert!(storage.csv_disable_create_headers());
}

#[test]
fn storage_honors_enabled_sink_options_and_csv_default_path() {
    let yaml = r#"
postgres:
  enabled: true
  drop_each_run: true
  disable_create_tables: true
csv:
  enabled: true
"#;

    let storage: Storage = serde_yaml::from_str(yaml).expect("storage should parse");

    assert!(storage.postgres_enabled());
    assert!(storage.postgres_drop_each_run());
    assert!(storage.postgres_disable_create_tables());
    assert!(storage.csv_enabled());
    assert!(!storage.csv_disable_create_headers());
    assert_eq!(storage.csv.as_ref().expect("csv details").path, "./generated_csv");
}

#[test]
fn rabbitmq_validation_covers_required_routing_rules() {
    let empty: RabbitMQStreamConfig =
        serde_yaml::from_str("url: amqp://localhost\nexchanges: []").expect("config parses");
    assert_eq!(
        empty.validate().expect_err("empty exchanges should fail"),
        "No exchanges defined in RabbitMQ config"
    );

    let fanout_with_routing_key: RabbitMQStreamConfig = serde_yaml::from_str(
        r#"
url: amqp://localhost
exchanges:
  - exchange: blocks
    exchange_type: fanout
    routing_key: ignored
    networks: [mainnet]
"#,
    )
    .expect("config parses");
    assert_eq!(
        fanout_with_routing_key.validate().expect_err("fanout routing key should fail"),
        "Fanout exchanges do not support routing keys"
    );

    let topic_without_routing_key: RabbitMQStreamConfig = serde_yaml::from_str(
        r#"
url: amqp://localhost
exchanges:
  - exchange: events
    exchange_type: topic
    networks: [mainnet]
"#,
    )
    .expect("config parses");
    assert_eq!(
        topic_without_routing_key.validate().expect_err("topic without key should fail"),
        "Topic exchanges require a routing key"
    );

    let direct_without_routing_key: RabbitMQStreamConfig = serde_yaml::from_str(
        r#"
url: amqp://localhost
exchanges:
  - exchange: events
    exchange_type: direct
    networks: [mainnet]
"#,
    )
    .expect("config parses");
    assert_eq!(
        direct_without_routing_key.validate().expect_err("direct without key should fail"),
        "Direct exchanges require a routing keys"
    );

    let valid: RabbitMQStreamConfig = serde_yaml::from_str(
        r#"
url: amqp://localhost
exchanges:
  - exchange: direct_events
    exchange_type: direct
    routing_key: transfer
    networks: [mainnet]
  - exchange: topic_events
    exchange_type: topic
    routing_key: transfer.*
    networks: [base]
  - exchange: fanout_events
    exchange_type: fanout
    networks: [optimism]
"#,
    )
    .expect("config parses");
    assert!(valid.validate().is_ok());
}

#[test]
fn streams_last_synced_block_path_uses_configured_sink_priority() {
    let rabbitmq: StreamsConfig = serde_yaml::from_str(
        r#"
rabbitmq:
  url: amqp://localhost
  exchanges:
    - exchange: events
      exchange_type: direct
      routing_key: transfer
      networks: [mainnet]
webhooks:
  - endpoint: http://localhost/hook
    shared_secret: secret
    networks: [mainnet]
"#,
    )
    .expect("stream config parses");
    assert_eq!(rabbitmq.get_streams_last_synced_block_path(), ".rindexer/rabbitmq");

    let webhook: StreamsConfig = serde_yaml::from_str(
        r#"
webhooks:
  - endpoint: http://localhost/hook
    shared_secret: secret
    networks: [mainnet]
"#,
    )
    .expect("stream config parses");
    assert_eq!(webhook.get_streams_last_synced_block_path(), ".rindexer/webhooks");

    let no_streams: StreamsConfig = serde_yaml::from_str("{}").expect("empty streams parse");
    assert_eq!(no_streams.get_streams_last_synced_block_path(), ".rindexer/");
}

#[test]
fn finalized_delivery_targets_include_each_stream_sink() {
    let streams: StreamsConfig = serde_yaml::from_str(
        r#"
sns:
  aws_config:
    region: us-east-1
    access_key: key
    secret_key: secret
  topics:
    - topic_arn: arn:aws:sns:us-east-1:123:events
      networks: [mainnet]
      delivery: finalized
    - topic_arn: arn:aws:sns:us-east-1:123:instant
      networks: [mainnet]
webhooks:
  - endpoint: http://localhost/finalized
    shared_secret: secret
    networks: [base]
    delivery: finalized
  - endpoint: http://localhost/instant
    shared_secret: secret
    networks: [base]
rabbitmq:
  url: amqp://localhost
  exchanges:
    - exchange: finalized_events
      exchange_type: direct
      routing_key: transfer
      networks: [optimism]
      delivery: finalized
redis:
  connection_uri: redis://localhost
  streams:
    - stream_name: finalized-stream
      networks: [arbitrum]
      delivery: finalized
cloudflare_queues:
  api_token: token
  account_id: account
  queues:
    - queue_id: finalized-queue
      networks: [polygon]
      delivery: finalized
"#,
    )
    .expect("stream config parses");

    let targets = streams.finalized_delivery_targets();

    assert_eq!(targets.len(), 5);
    assert!(targets.contains(&(
        "sns",
        "arn:aws:sns:us-east-1:123:events".to_string(),
        vec!["mainnet".to_string()]
    )));
    assert!(targets.contains(&(
        "webhook",
        "http://localhost/finalized".to_string(),
        vec!["base".to_string()]
    )));
    assert!(targets.contains(&(
        "rabbitmq",
        "finalized_events".to_string(),
        vec!["optimism".to_string()]
    )));
    assert!(targets.contains(&(
        "redis",
        "finalized-stream".to_string(),
        vec!["arbitrum".to_string()]
    )));
    assert!(targets.contains(&(
        "cloudflare_queues",
        "finalized-queue".to_string(),
        vec!["polygon".to_string()]
    )));
}

#[cfg(feature = "kafka")]
#[test]
fn finalized_delivery_targets_include_kafka_when_feature_enabled() {
    let streams: StreamsConfig = serde_yaml::from_str(
        r#"
kafka:
  brokers: [localhost:9092]
  security_protocol: PLAINTEXT
  acks: all
  topics:
    - topic: finalized-topic
      networks: [mainnet]
      delivery: finalized
"#,
    )
    .expect("kafka stream config parses");

    assert_eq!(
        streams.finalized_delivery_targets(),
        vec![("kafka", "finalized-topic".to_string(), vec!["mainnet".to_string()])]
    );
}

#[tokio::test]
async fn create_full_streams_last_synced_block_path_creates_contract_directory() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let streams: StreamsConfig = serde_yaml::from_str(
        r#"
redis:
  connection_uri: redis://localhost
  streams:
    - stream_name: events
      networks: [mainnet]
"#,
    )
    .expect("stream config parses");

    streams.create_full_streams_last_synced_block_path(temp_dir.path(), "Token").await;

    assert!(temp_dir.path().join(".rindexer/redis/Token/last-synced-blocks").is_dir());
}

#[test]
fn chat_defaults_and_omits_absent_optional_fields() {
    let yaml = r#"
pagerduty:
  - routing_key: route
    networks: [mainnet]
    messages:
      - event_name: Transfer
        template_inline: moved
opsgenie:
  - api_key: key
    networks: [base]
    messages:
      - event_name: Approval
        template_inline: approved
"#;

    let chat: ChatConfig = serde_yaml::from_str(yaml).expect("chat config parses");

    assert_eq!(chat.pagerduty.as_ref().expect("pagerduty")[0].severity, "critical");
    assert_eq!(chat.opsgenie.as_ref().expect("opsgenie")[0].priority, "P1");

    let serialized = serde_yaml::to_string(&chat).expect("chat serializes");
    assert!(!serialized.contains("conditions:"));
    assert!(!serialized.contains("filter_expression:"));
}

#[test]
fn graphql_settings_default_and_set_port_are_consistent() {
    let mut settings = GraphQLSettings::default();

    assert_eq!(settings.port, 3001);
    assert!(!settings.disable_advanced_filters);
    assert!(!settings.filter_only_on_indexed_columns);

    settings.set_port(4000);

    assert_eq!(settings.port, 4000);
}

#[test]
fn global_default_sets_health_port_and_omits_optional_fields() {
    let global = Global::default();

    assert!(global.contracts.is_none());
    assert!(global.etherscan_api_key.is_none());
    assert_eq!(global.health_port, 8080);

    let serialized = serde_yaml::to_string(&global).expect("global serializes");
    assert!(!serialized.contains("contracts:"));
    assert!(!serialized.contains("etherscan_api_key:"));
    assert!(serialized.contains("health_port: 8080"));
}

#[test]
fn string_or_array_deserializes_single_and_multiple_shapes() {
    let single: StringOrArray = serde_yaml::from_str("mainnet").expect("single string parses");
    let multiple: StringOrArray =
        serde_yaml::from_str("- mainnet\n- base\n").expect("array parses");
    let from_string = StringOrArray::from("optimism".to_string());

    match single {
        StringOrArray::Single(value) => assert_eq!(value, "mainnet"),
        StringOrArray::Multiple(_) => panic!("expected single"),
    }

    match multiple {
        StringOrArray::Multiple(values) => assert_eq!(values, vec!["mainnet", "base"]),
        StringOrArray::Single(_) => panic!("expected multiple"),
    }

    match from_string {
        StringOrArray::Single(value) => assert_eq!(value, "optimism"),
        StringOrArray::Multiple(_) => panic!("expected single"),
    }
}

#[test]
fn adaptive_concurrency_scales_up_down_and_tracks_backoff() {
    let controller = AdaptiveConcurrency::new(10, 2, 12);

    assert_eq!(controller.current(), 10);
    assert_eq!(controller.current_batch_size(), 50);
    assert_eq!(controller.current_backoff_ms(), 0);
    assert_eq!(controller.rate_limit_count(), 0);

    for _ in 0..10 {
        controller.record_success();
    }

    assert_eq!(controller.current(), 12);
    assert_eq!(controller.current_batch_size(), 60);

    controller.record_rate_limit();

    assert_eq!(controller.current(), 6);
    assert_eq!(controller.current_batch_size(), 30);
    assert_eq!(controller.current_backoff_ms(), 500);
    assert_eq!(controller.rate_limit_count(), 1);

    controller.record_error();

    assert_eq!(controller.current(), 5);

    for _ in 0..3 {
        controller.record_rate_limit();
    }

    assert_eq!(controller.current(), 2);
    assert_eq!(controller.current_batch_size(), 5);
    assert_eq!(controller.current_backoff_ms(), 4_000);

    controller.record_success();

    assert_eq!(controller.current_backoff_ms(), 3_000);
}

#[tokio::test(start_paused = true)]
async fn adaptive_concurrency_wait_for_backoff_observes_current_delay() {
    let controller = AdaptiveConcurrency::new(5, 1, 10);
    controller.record_rate_limit();

    let wait = controller.wait_for_backoff();
    tokio::pin!(wait);

    tokio::select! {
        _ = &mut wait => panic!("backoff should not complete before timer advances"),
        _ = tokio::time::sleep(Duration::from_millis(499)) => {}
    }

    tokio::time::advance(Duration::from_millis(1)).await;
    wait.await;
}
