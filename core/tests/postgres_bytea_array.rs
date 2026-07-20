//! PostgreSQL round-trip tests for byte array wrappers.
//!
//! Requires Docker:
//!   cargo test -p rindexer --test postgres_bytea_array

use alloy::primitives::{Address, Bytes, B256, I256, U256};
use futures::pin_mut;
use rindexer::{EthereumSqlTypeWrapper, PgType};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql};

#[tokio::test]
async fn vec_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecBytes",
        EthereumSqlTypeWrapper::VecBytes(vec![
            Bytes::copy_from_slice(&[1u8; 32]),
            Bytes::copy_from_slice(&[2u8; 32]),
        ]),
        vec![vec![1u8; 32], vec![2u8; 32]],
    )
    .await;
}

#[tokio::test]
async fn vec_address_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecAddressBytes",
        EthereumSqlTypeWrapper::VecAddressBytes(vec![
            Address::from([3u8; 20]),
            Address::from([4u8; 20]),
        ]),
        vec![vec![3u8; 20], vec![4u8; 20]],
    )
    .await;
}

#[tokio::test]
async fn vec_b256_bytes_insert_and_copy_into_postgres_bytea_array() {
    assert_bytea_array_roundtrip(
        "VecB256Bytes",
        EthereumSqlTypeWrapper::VecB256Bytes(vec![B256::from([5u8; 32]), B256::from([6u8; 32])]),
        vec![vec![5u8; 32], vec![6u8; 32]],
    )
    .await;
}

#[tokio::test]
async fn vec_u256_bytes_insert_and_copy_into_postgres_bytea_array() {
    let values = vec![U256::from(42u64), U256::from(99u64)];
    let expected_ids = values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect();

    assert_bytea_array_roundtrip(
        "VecU256Bytes",
        EthereumSqlTypeWrapper::VecU256Bytes(values),
        expected_ids,
    )
    .await;
}

#[tokio::test]
async fn vec_i256_bytes_insert_and_copy_into_postgres_bytea_array() {
    let values = vec![I256::try_from(42i64).unwrap(), I256::try_from(-99i64).unwrap()];
    let expected_ids = values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect();

    assert_bytea_array_roundtrip(
        "VecI256Bytes",
        EthereumSqlTypeWrapper::VecI256Bytes(values),
        expected_ids,
    )
    .await;
}

async fn assert_bytea_array_roundtrip(
    name: &str,
    wrapper: EthereumSqlTypeWrapper,
    expected_ids: Vec<Vec<u8>>,
) {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let pg_container = Postgres::default().start().await.expect("failed to start postgres");
    let pg_port = pg_container.get_host_port_ipv4(5432).await.expect("failed to get postgres port");
    let conn_str =
        format!("host=127.0.0.1 port={pg_port} user=postgres password=postgres dbname=postgres");

    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to postgres");
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            eprintln!("postgres connection error: {err}");
        }
    });

    assert_eq!(wrapper.to_type(), PgType::BYTEA_ARRAY, "{name}");

    client
        .batch_execute(
            "CREATE TABLE bytea_array_insert_roundtrip (ids BYTEA[] NOT NULL);
             CREATE TABLE bytea_array_copy_roundtrip (ids BYTEA[] NOT NULL);",
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to create test tables: {error}"));

    client
        .execute("INSERT INTO bytea_array_insert_roundtrip (ids) VALUES ($1)", &[&wrapper])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to insert bytea array: {error}"));

    let row = client
        .query_one("SELECT ids FROM bytea_array_insert_roundtrip", &[])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to query bytea array: {error}"));
    let stored_ids: Vec<Vec<u8>> = row.get("ids");

    assert_eq!(stored_ids, expected_ids, "{name}: insert roundtrip");

    let sink = client
        .copy_in("COPY bytea_array_copy_roundtrip (ids) FROM STDIN WITH (FORMAT binary)")
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to start bytea array copy: {error}"));
    let writer = BinaryCopyInWriter::new(sink, &[PgType::BYTEA_ARRAY]);
    pin_mut!(writer);

    let row: [&(dyn ToSql + Sync); 1] = [&wrapper];
    writer
        .as_mut()
        .write(&row)
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to copy bytea array: {error}"));
    writer
        .finish()
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to finish bytea array copy: {error}"));

    let row = client
        .query_one("SELECT ids FROM bytea_array_copy_roundtrip", &[])
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to query copied bytea array: {error}"));
    let stored_ids: Vec<Vec<u8>> = row.get("ids");

    assert_eq!(stored_ids, expected_ids, "{name}: copy roundtrip");
}
