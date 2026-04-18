/// E2E validation tests for ClickHouse type handling and features.
///
/// Unit tests (no CH needed): validate type mapping, serialization, YAML parsing.
/// Integration tests: spin up a ClickHouse container per test via testcontainers
/// and validate full round-trip queries against it.
///
/// Requires Docker. Run via nextest so each test gets its own process:
///   cargo nextest run -p rindexer --test clickhouse_type_e2e
use rindexer::manifest::contract::ColumnType;

// =========================================================================
// Unit tests — type mapping
// =========================================================================

#[test]
fn test_custom_table_types_match_raw_event_types() {
    assert_eq!(ColumnType::Uint256.to_clickhouse_type(), "UInt256");
    assert_eq!(ColumnType::Uint128.to_clickhouse_type(), "UInt128");
    assert_eq!(ColumnType::Int256.to_clickhouse_type(), "Int256");
    assert_eq!(ColumnType::Int128.to_clickhouse_type(), "Int128");
    assert_eq!(ColumnType::Address.to_clickhouse_type(), "FixedString(42)");
    assert_eq!(ColumnType::Uint64.to_clickhouse_type(), "UInt64");
    assert_eq!(ColumnType::Int64.to_clickhouse_type(), "Int64");
    assert_eq!(ColumnType::Uint8.to_clickhouse_type(), "UInt8");
    assert_eq!(ColumnType::Bool.to_clickhouse_type(), "Bool");
    assert_eq!(ColumnType::String.to_clickhouse_type(), "String");
    assert_eq!(ColumnType::Timestamp.to_clickhouse_type(), "DateTime('UTC')");
}

#[test]
fn test_array_types_use_native_inner() {
    let u256_arr = ColumnType::Array(Box::new(ColumnType::Uint256));
    assert_eq!(u256_arr.to_clickhouse_type(), "Array(UInt256)");

    let addr_arr = ColumnType::Array(Box::new(ColumnType::Address));
    assert_eq!(addr_arr.to_clickhouse_type(), "Array(FixedString(42))");
}

#[test]
fn test_pg_types_unchanged() {
    assert_eq!(ColumnType::Uint256.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Uint128.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Int256.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Address.to_postgres_type(), "CHAR(42)");
    assert_eq!(ColumnType::Uint64.to_postgres_type(), "BIGINT");
}

#[test]
fn test_ddl_for_table_with_native_types() {
    let columns: Vec<(&str, ColumnType)> = vec![
        ("id", ColumnType::Uint64),
        ("block_number", ColumnType::Uint64),
        ("sender", ColumnType::Address),
        ("recipient", ColumnType::Address),
        ("amount", ColumnType::Uint256),
        ("fee", ColumnType::Uint256),
        ("event_type", ColumnType::String),
        ("is_deleted", ColumnType::Uint8),
    ];

    let ddl_parts: Vec<String> = columns
        .iter()
        .map(|(name, ct)| format!("  `{}` {}", name, ct.to_clickhouse_type()))
        .collect();
    let ddl = format!(
        "CREATE TABLE test.events (\n{}\n) ENGINE = MergeTree() ORDER BY id",
        ddl_parts.join(",\n")
    );

    assert!(ddl.contains("`amount` UInt256"), "amount should be UInt256");
    assert!(ddl.contains("`fee` UInt256"), "fee should be UInt256");
    assert!(ddl.contains("`sender` FixedString(42)"), "sender should be FixedString(42)");
    assert!(ddl.contains("`id` UInt64"), "id should be UInt64");
    assert!(!ddl.contains("`amount` String"), "amount must NOT be String");
    assert!(!ddl.contains("`sender` String"), "sender must NOT be String");
}

// =========================================================================
// Unit tests — database override YAML field
// =========================================================================

#[test]
fn test_table_database_field_parses() {
    use rindexer::manifest::contract::Table;

    let yaml = r#"
        name: events
        database: shared_db
        columns:
          - name: id
            type: uint64
    "#;
    let table: Table = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(table.database, Some("shared_db".to_string()));
    assert_eq!(table.name, "events");
}

#[test]
fn test_table_database_field_defaults_to_none() {
    use rindexer::manifest::contract::Table;

    let yaml = r#"
        name: events
        columns:
          - name: id
            type: uint64
    "#;
    let table: Table = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(table.database, None);
}

#[test]
fn test_table_database_field_independent_of_other_fields() {
    use rindexer::manifest::contract::Table;

    let yaml = r#"
        name: counters
        database: analytics
        global: true
        cross_chain: false
        columns:
          - name: value
            type: uint256
    "#;
    let table: Table = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(table.database, Some("analytics".to_string()));
    assert!(table.global);
    assert!(!table.cross_chain);
}

// =========================================================================
// E2E tests — spin up ClickHouse via testcontainers
// =========================================================================

#[cfg(test)]
mod e2e {
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::clickhouse::ClickHouse as ClickHouseImage;

    pub(super) struct ChEnv {
        pub url: String,
        // Container handle kept alive for the duration of the test; the testcontainers
        // crate stops + removes it on drop.
        _container: ContainerAsync<ClickHouseImage>,
    }

    pub(super) async fn start() -> ChEnv {
        let container =
            ClickHouseImage::default().start().await.expect("failed to start clickhouse container");
        let port = container.get_host_port_ipv4(8123).await.expect("failed to get clickhouse port");
        ChEnv { url: format!("http://127.0.0.1:{}", port), _container: container }
    }

    pub(super) fn ch_query(url: &str, query: &str) -> Result<String, String> {
        let full_url = format!("{}/?user=default&password=", url);
        let handle = std::process::Command::new("curl")
            .args(["-sf", "--data-binary", query, &full_url])
            .output()
            .map_err(|e| format!("curl failed: {e}"))?;

        if handle.status.success() {
            Ok(String::from_utf8_lossy(&handle.stdout).trim().to_string())
        } else {
            Err(format!("CH error: {}", String::from_utf8_lossy(&handle.stderr).trim()))
        }
    }

    pub(super) fn create_db(url: &str, db: &str) {
        ch_query(url, &format!("CREATE DATABASE {db}")).unwrap();
    }
}

// ── Native types: CREATE + INSERT + Float64 division ──

#[tokio::test]
async fn test_e2e_native_types_roundtrip() {
    let env = e2e::start().await;
    let db = "rindexer_e2e_types";
    e2e::create_db(&env.url, db);

    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {db}.events (
            id UInt64, block_number UInt64,
            sender FixedString(42), recipient FixedString(42),
            amount UInt256, shares UInt256, fee UInt256,
            event_type String, side String, is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();

    e2e::ch_query(
        &env.url,
        &format!(
            "INSERT INTO {db}.events VALUES \
        (1, 100, '0xaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaA', \
        '0xbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbBbB', \
        1500000, 2000000, 30000, 'SWAP', 'BUY', 0)"
        ),
    )
    .unwrap();

    // UInt256 → Float64 division
    let amount =
        e2e::ch_query(&env.url, &format!("SELECT toFloat64(amount) / 1e6 FROM {db}.events"))
            .unwrap();
    assert_eq!(amount, "1.5");

    // UInt256 / UInt256 → ratio
    let ratio = e2e::ch_query(
        &env.url,
        &format!("SELECT toFloat64(amount) / toFloat64(shares) FROM {db}.events"),
    )
    .unwrap();
    assert_eq!(ratio, "0.75");

    // lower() on FixedString(42)
    let addr = e2e::ch_query(&env.url, &format!("SELECT lower(sender) FROM {db}.events")).unwrap();
    assert_eq!(addr, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
}

// ── Streaming MV: raw UInt256 → Float64 transform ──

#[tokio::test]
async fn test_e2e_streaming_mv_transform() {
    let env = e2e::start().await;
    let db = "rindexer_e2e_mv";
    e2e::create_db(&env.url, db);

    // Raw table (UInt256 amounts)
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {db}.raw_events (
            id UInt64, amount UInt256, shares UInt256,
            event_type String, source FixedString(42), fee UInt256, is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();

    // Transformed table (Float64)
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {db}.processed (
            id String, price Float64, amount Float64,
            shares Float64, fee Float64, is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();

    // Streaming MV with table alias to avoid CH alias shadowing
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE MATERIALIZED VIEW {db}.mv_transform TO {db}.processed AS
        SELECT
            toString(r.id) AS id,
            if(r.shares = toUInt256(0), toFloat64(0),
               toFloat64(r.amount) / toFloat64(r.shares)) AS price,
            toFloat64(r.amount) / 1e6 AS amount,
            toFloat64(r.shares) / 1e6 AS shares,
            if(lower(r.source) IN (
                '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
            ), toFloat64(r.fee) / 1e6, 0) AS fee,
            r.is_deleted
        FROM {db}.raw_events AS r
        WHERE r.event_type = 'SWAP'"
        ),
    )
    .unwrap();

    // Insert raw data
    e2e::ch_query(
        &env.url,
        &format!(
            "INSERT INTO {db}.raw_events VALUES \
        (1, 1500000, 2000000, 'SWAP', '0xaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaA', 30000, 0), \
        (2, 500000, 1000000, 'SWAP', '0xcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcCcC', 10000, 0), \
        (3, 750000, 750000, 'DEPOSIT', '0xaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaAaA', 0, 0)"
        ),
    )
    .unwrap();

    // MV captures only SWAP rows
    let count = e2e::ch_query(&env.url, &format!("SELECT count() FROM {db}.processed")).unwrap();
    assert_eq!(count, "2");

    // Row 1: matched source → fee applied
    let r1 = e2e::ch_query(
        &env.url,
        &format!("SELECT amount, shares, price, fee FROM {db}.processed WHERE id = '1'"),
    )
    .unwrap();
    assert_eq!(r1, "1.5\t2\t0.75\t0.03");

    // Row 2: unmatched source → fee = 0
    let r2 = e2e::ch_query(
        &env.url,
        &format!("SELECT amount, shares, price, fee FROM {db}.processed WHERE id = '2'"),
    )
    .unwrap();
    assert_eq!(r2, "0.5\t1\t0.5\t0");
}

// ── UInt256 max value round-trip ──

#[tokio::test]
async fn test_e2e_uint256_max_roundtrip() {
    let env = e2e::start().await;
    let db = "rindexer_e2e_max";
    let max_str = "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    e2e::create_db(&env.url, db);
    e2e::ch_query(
        &env.url,
        &format!("CREATE TABLE {db}.t (val UInt256) ENGINE = MergeTree() ORDER BY val"),
    )
    .unwrap();

    e2e::ch_query(&env.url, &format!("INSERT INTO {db}.t VALUES ({max_str})")).unwrap();
    let result = e2e::ch_query(&env.url, &format!("SELECT val FROM {db}.t")).unwrap();
    assert_eq!(result, max_str, "U256::MAX must survive CH round-trip");
}

// ── Database override: shared table across contracts ──

#[tokio::test]
async fn test_e2e_database_override_shared_table() {
    let env = e2e::start().await;
    let shared = "rindexer_e2e_shared";
    let c1 = "rindexer_e2e_c1";
    let c2 = "rindexer_e2e_c2";

    for db in [shared, c1, c2] {
        e2e::create_db(&env.url, db);
    }

    // Per-contract raw tables (isolated — default rindexer behavior)
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {c1}.raw_events (sender String, amount UInt256) ENGINE = MergeTree() ORDER BY sender"
        ),
    )
    .unwrap();
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {c2}.raw_events (sender String, amount UInt256) ENGINE = MergeTree() ORDER BY sender"
        ),
    )
    .unwrap();

    // Shared custom table (what `database:` override produces)
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {shared}.events (
            id UInt64, source_contract String, user_id String, amount UInt256, event_type String
        ) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();

    // Contract 1 writes raw + shared
    e2e::ch_query(&env.url, &format!("INSERT INTO {c1}.raw_events VALUES ('alice', 1000000)"))
        .unwrap();
    e2e::ch_query(
        &env.url,
        &format!(
            "INSERT INTO {shared}.events VALUES \
        (1, 'contract_a', 'alice', 1000000, 'SWAP'), \
        (2, 'contract_a', 'bob', 1000000, 'SWAP')"
        ),
    )
    .unwrap();

    // Contract 2 writes raw + shared
    e2e::ch_query(&env.url, &format!("INSERT INTO {c2}.raw_events VALUES ('carol', 2000000)"))
        .unwrap();
    e2e::ch_query(
        &env.url,
        &format!(
            "INSERT INTO {shared}.events VALUES \
        (3, 'contract_b', 'carol', 2000000, 'SWAP'), \
        (4, 'contract_b', 'dave', 2000000, 'SWAP')"
        ),
    )
    .unwrap();

    // Raw tables isolated
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT count() FROM {c1}.raw_events")).unwrap(),
        "1"
    );
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT count() FROM {c2}.raw_events")).unwrap(),
        "1"
    );

    // Shared table has ALL rows from ALL contracts
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT count() FROM {shared}.events")).unwrap(),
        "4"
    );

    // Can query across contracts from single table
    let by_source = e2e::ch_query(
        &env.url,
        &format!(
            "SELECT source_contract, count() FROM {shared}.events GROUP BY source_contract ORDER BY source_contract"
        ),
    )
    .unwrap();
    assert_eq!(by_source, "contract_a\t2\ncontract_b\t2");

    // Streaming MV works on shared table
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {shared}.processed (id String, amount Float64) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE MATERIALIZED VIEW {shared}.mv TO {shared}.processed AS
        SELECT toString(id) AS id, toFloat64(amount)/1e6 AS amount
        FROM {shared}.events WHERE event_type = 'SWAP'"
        ),
    )
    .unwrap();

    // New insert triggers MV
    e2e::ch_query(
        &env.url,
        &format!("INSERT INTO {shared}.events VALUES (5, 'contract_a', 'eve', 3000000, 'SWAP')"),
    )
    .unwrap();
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT amount FROM {shared}.processed WHERE id = '5'"))
            .unwrap(),
        "3"
    );
}

// ── Without database override: tables are per-contract ──

#[tokio::test]
async fn test_e2e_default_per_contract_isolation() {
    let env = e2e::start().await;
    let db1 = "rindexer_e2e_iso1";
    let db2 = "rindexer_e2e_iso2";

    for db in [db1, db2] {
        e2e::create_db(&env.url, db);
    }

    // Each contract gets its own table (no override)
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {db1}.events (id UInt64, event_type String) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();
    e2e::ch_query(
        &env.url,
        &format!(
            "CREATE TABLE {db2}.events (id UInt64, event_type String) ENGINE = MergeTree() ORDER BY id"
        ),
    )
    .unwrap();

    e2e::ch_query(&env.url, &format!("INSERT INTO {db1}.events VALUES (1, 'SWAP')")).unwrap();
    e2e::ch_query(&env.url, &format!("INSERT INTO {db2}.events VALUES (2, 'DEPOSIT')")).unwrap();

    // Tables are isolated
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT event_type FROM {db1}.events")).unwrap(),
        "SWAP"
    );
    assert_eq!(
        e2e::ch_query(&env.url, &format!("SELECT event_type FROM {db2}.events")).unwrap(),
        "DEPOSIT"
    );
    assert_eq!(e2e::ch_query(&env.url, &format!("SELECT count() FROM {db1}.events")).unwrap(), "1");
    assert_eq!(e2e::ch_query(&env.url, &format!("SELECT count() FROM {db2}.events")).unwrap(), "1");
}
