/// E2E validation tests for ClickHouse type handling.
///
/// Unit tests (no CH needed): validate type mapping and serialization consistency.
/// Integration tests (#[ignore]): validate full round-trip against a real ClickHouse.
///
/// Run unit tests:  cargo test -p rindexer --test clickhouse_type_e2e
/// Run E2E tests:   cargo test -p rindexer --test clickhouse_type_e2e -- --ignored
///
/// E2E setup:
///   docker run -d --name ch-test -p 8123:8123 clickhouse/clickhouse-server:24.8
use rindexer::manifest::contract::ColumnType;

// =========================================================================
// Unit tests — type mapping consistency
// =========================================================================

#[test]
fn test_custom_table_types_match_raw_event_types() {
    // Custom table path (ColumnType::to_clickhouse_type) must produce the same
    // CH types as raw event path (solidity_type_to_clickhouse_type).
    // This ensures a uint256 column in a YAML table creates the same CH column
    // as a uint256 ABI field in a raw event table.
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
    // Ensure PG mapping wasn't broken by the CH fix
    assert_eq!(ColumnType::Uint256.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Uint128.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Int256.to_postgres_type(), "NUMERIC");
    assert_eq!(ColumnType::Address.to_postgres_type(), "CHAR(42)");
    assert_eq!(ColumnType::Uint64.to_postgres_type(), "BIGINT");
}

#[test]
fn test_ddl_for_activities_raw_table() {
    // Simulate what rindexer would generate for our rindexer-ch.yaml activities_raw table
    let columns: Vec<(&str, ColumnType)> = vec![
        ("id", ColumnType::Uint64),
        ("block_number", ColumnType::Uint64),
        ("block_timestamp", ColumnType::Uint64),
        ("transaction_hash", ColumnType::String),
        ("address", ColumnType::Address),
        ("user_id", ColumnType::Address),
        ("asset", ColumnType::String),
        ("condition_id", ColumnType::String),
        ("neg_risk_market_id", ColumnType::String),
        ("amount_usdc", ColumnType::Uint256),
        ("amount_shares", ColumnType::Uint256),
        ("tx_type", ColumnType::String),
        ("side", ColumnType::String),
        ("order_hash", ColumnType::String),
        ("counterparty_id", ColumnType::Address),
        ("order_type", ColumnType::String),
        ("fee", ColumnType::Uint256),
        ("builder", ColumnType::String),
        ("is_deleted", ColumnType::Uint8),
    ];

    let ddl_parts: Vec<String> = columns
        .iter()
        .map(|(name, ct)| format!("  `{}` {}", name, ct.to_clickhouse_type()))
        .collect();
    let ddl = format!(
        "CREATE TABLE test.activities_raw (\n{}\n) ENGINE = MergeTree() ORDER BY id",
        ddl_parts.join(",\n")
    );

    // Verify key types in DDL
    assert!(ddl.contains("`amount_usdc` UInt256"), "amount_usdc should be UInt256, got:\n{ddl}");
    assert!(ddl.contains("`amount_shares` UInt256"), "amount_shares should be UInt256");
    assert!(ddl.contains("`fee` UInt256"), "fee should be UInt256");
    assert!(ddl.contains("`address` FixedString(42)"), "address should be FixedString(42)");
    assert!(ddl.contains("`user_id` FixedString(42)"), "user_id should be FixedString(42)");
    assert!(ddl.contains("`id` UInt64"), "id should be UInt64");
    assert!(ddl.contains("`is_deleted` UInt8"), "is_deleted should be UInt8");

    // Verify no String fallback for numeric/address types
    assert!(!ddl.contains("`amount_usdc` String"), "amount_usdc must NOT be String");
    assert!(!ddl.contains("`address` String"), "address must NOT be String");
}

// =========================================================================
// E2E tests — require running ClickHouse (run with --ignored)
// =========================================================================

#[cfg(test)]
fn ch_query(query: &str) -> Result<String, String> {
    use std::io::Read;
    let url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".to_string());
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".to_string());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();

    let full_url = format!("{}/?user={}&password={}", url, user, password);
    let mut handle = std::process::Command::new("curl")
        .args(["-sf", "--data-binary", query, &full_url])
        .output()
        .map_err(|e| format!("curl failed: {e}"))?;

    if handle.status.success() {
        Ok(String::from_utf8_lossy(&handle.stdout).trim().to_string())
    } else {
        Err(format!("CH error: {}", String::from_utf8_lossy(&handle.stderr).trim()))
    }
}

#[test]
#[ignore]
fn test_e2e_native_types_create_and_insert() {
    let db = "rindexer_e2e_types";

    ch_query(&format!("DROP DATABASE IF EXISTS {db}")).unwrap();
    ch_query(&format!("CREATE DATABASE {db}")).unwrap();

    // DDL with native types (what rindexer now generates)
    ch_query(&format!(
        "CREATE TABLE {db}.activities_raw (
            id UInt64,
            block_number UInt64,
            address FixedString(42),
            user_id FixedString(42),
            amount_usdc UInt256,
            amount_shares UInt256,
            fee UInt256,
            tx_type String,
            side String,
            is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
    ))
    .unwrap();

    // Insert with decimal string values for UInt256 (same as rindexer serialization)
    ch_query(&format!(
        "INSERT INTO {db}.activities_raw VALUES \
        (1, 85267446, '0xe111180000d2663C0091e4f400237545B87B996B', \
        '0x1234567890123456789012345678901234567890', \
        1500000, 2000000, 30000, 'TRADE', 'BUY', 0)"
    ))
    .unwrap();

    // Verify Float64 division on UInt256 (the core of our transform MV pattern)
    let usdc =
        ch_query(&format!("SELECT toFloat64(amount_usdc) / 1e6 FROM {db}.activities_raw")).unwrap();
    assert_eq!(usdc, "1.5");

    let price = ch_query(&format!(
        "SELECT toFloat64(amount_usdc) / toFloat64(amount_shares) FROM {db}.activities_raw"
    ))
    .unwrap();
    assert_eq!(price, "0.75");

    // Verify lower() on FixedString(42) addresses
    let addr = ch_query(&format!("SELECT lower(address) FROM {db}.activities_raw")).unwrap();
    assert_eq!(addr, "0xe111180000d2663c0091e4f400237545b87b996b");

    ch_query(&format!("DROP DATABASE {db}")).unwrap();
}

#[test]
#[ignore]
fn test_e2e_transform_mv_pipeline() {
    let db = "rindexer_e2e_mv";

    ch_query(&format!("DROP DATABASE IF EXISTS {db}")).unwrap();
    ch_query(&format!("CREATE DATABASE {db}")).unwrap();

    // Raw table
    ch_query(&format!(
        "CREATE TABLE {db}.raw (
            id UInt64, amount_usdc UInt256, amount_shares UInt256,
            tx_type String, address FixedString(42), fee UInt256, is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
    ))
    .unwrap();

    // Transformed table
    ch_query(&format!(
        "CREATE TABLE {db}.trades (
            id String, amount_usdc Float64, shares Float64,
            price_usdc Float64, fee Float64, is_deleted UInt8
        ) ENGINE = MergeTree() ORDER BY id"
    ))
    .unwrap();

    // Streaming MV — use _raw suffixed names to avoid alias shadowing,
    // then rename in the outer expression. This is the pattern our production
    // MVs must use when amount_usdc alias shadows the source column.
    ch_query(&format!(
        "CREATE MATERIALIZED VIEW {db}.mv_transform TO {db}.trades AS
        SELECT
            toString(id) AS id,
            toFloat64(_raw_usdc) / 1e6 AS amount_usdc,
            toFloat64(_raw_shares) / 1e6 AS shares,
            if(_raw_shares = toUInt256(0), toFloat64(0),
               toFloat64(_raw_usdc) / toFloat64(_raw_shares)) AS price_usdc,
            if(lower(address) IN (
                '0xe111180000d2663c0091e4f400237545b87b996b',
                '0xe2222d002000ba0053cef3375333610f64600036'
            ), toFloat64(fee) / 1e6, 0) AS fee,
            is_deleted
        FROM (
            SELECT *, amount_usdc AS _raw_usdc, amount_shares AS _raw_shares
            FROM {db}.raw
            WHERE tx_type = 'TRADE'
        )"
    ))
    .unwrap();

    // Insert raw data
    ch_query(&format!(
        "INSERT INTO {db}.raw VALUES \
        (1, 1500000, 2000000, 'TRADE', '0xe111180000d2663C0091e4f400237545B87B996B', 30000, 0), \
        (2, 500000, 1000000, 'TRADE', '0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E', 10000, 0), \
        (3, 750000, 750000, 'SPLIT', '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045', 0, 0)"
    ))
    .unwrap();

    // MV should only capture TRADE rows
    let count = ch_query(&format!("SELECT count() FROM {db}.trades")).unwrap();
    assert_eq!(count, "2");

    // Row 1: V2 address → fee applied
    let r1 = ch_query(&format!(
        "SELECT amount_usdc, shares, price_usdc, fee FROM {db}.trades WHERE id = '1'"
    ))
    .unwrap();
    assert_eq!(r1, "1.5\t2\t0.75\t0.03");

    // Row 2: V1 address → fee = 0
    let r2 = ch_query(&format!(
        "SELECT amount_usdc, shares, price_usdc, fee FROM {db}.trades WHERE id = '2'"
    ))
    .unwrap();
    assert_eq!(r2, "0.5\t1\t0.5\t0");

    ch_query(&format!("DROP DATABASE {db}")).unwrap();
}

#[test]
#[ignore]
fn test_e2e_uint256_max_roundtrip() {
    let db = "rindexer_e2e_max";
    let max_str = "115792089237316195423570985008687907853269984665640564039457584007913129639935";

    ch_query(&format!("DROP DATABASE IF EXISTS {db}")).unwrap();
    ch_query(&format!("CREATE DATABASE {db}")).unwrap();
    ch_query(&format!("CREATE TABLE {db}.t (val UInt256) ENGINE = MergeTree() ORDER BY val"))
        .unwrap();

    ch_query(&format!("INSERT INTO {db}.t VALUES ({max_str})")).unwrap();

    let result = ch_query(&format!("SELECT val FROM {db}.t")).unwrap();
    assert_eq!(result, max_str, "U256::MAX must survive CH round-trip");

    ch_query(&format!("DROP DATABASE {db}")).unwrap();
}
