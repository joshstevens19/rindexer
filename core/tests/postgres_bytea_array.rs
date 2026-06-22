//! PostgreSQL round-trip tests for byte array wrappers.
//!
//! Requires Docker:
//!   cargo test -p rindexer --test postgres_bytea_array

use alloy::primitives::{Address, Bytes, B256, I256, U256};
use futures::pin_mut;
use rindexer::{EthereumSqlTypeWrapper, PgType};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql, Client};

#[tokio::test]
async fn bytea_array_wrappers_insert_and_copy_into_postgres_bytea_array() {
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

    client
        .batch_execute(
            "CREATE TABLE bytea_array_insert_roundtrip (ids BYTEA[] NOT NULL);
             CREATE TABLE bytea_array_copy_roundtrip (ids BYTEA[] NOT NULL);",
        )
        .await
        .expect("failed to create test tables");

    let u256_values = vec![U256::from(42u64), U256::from(99u64)];
    let i256_values = vec![I256::try_from(42i64).unwrap(), I256::try_from(-99i64).unwrap()];

    let cases = vec![
        (
            "VecBytes",
            EthereumSqlTypeWrapper::VecBytes(vec![
                Bytes::copy_from_slice(&[1u8; 32]),
                Bytes::copy_from_slice(&[2u8; 32]),
            ]),
            vec![vec![1u8; 32], vec![2u8; 32]],
        ),
        (
            "VecAddressBytes",
            EthereumSqlTypeWrapper::VecAddressBytes(vec![
                Address::from([3u8; 20]),
                Address::from([4u8; 20]),
            ]),
            vec![vec![3u8; 20], vec![4u8; 20]],
        ),
        (
            "VecB256Bytes",
            EthereumSqlTypeWrapper::VecB256Bytes(vec![
                B256::from([5u8; 32]),
                B256::from([6u8; 32]),
            ]),
            vec![vec![5u8; 32], vec![6u8; 32]],
        ),
        (
            "VecU256Bytes",
            EthereumSqlTypeWrapper::VecU256Bytes(u256_values.clone()),
            u256_values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect(),
        ),
        (
            "VecI256Bytes",
            EthereumSqlTypeWrapper::VecI256Bytes(i256_values.clone()),
            i256_values.iter().map(|value| value.to_be_bytes::<32>().to_vec()).collect(),
        ),
    ];

    for (name, wrapper, expected_ids) in cases {
        assert_bytea_array_roundtrip(&client, name, &wrapper, expected_ids).await;
    }
}

async fn assert_bytea_array_roundtrip(
    client: &Client,
    name: &str,
    wrapper: &EthereumSqlTypeWrapper,
    expected_ids: Vec<Vec<u8>>,
) {
    assert_eq!(wrapper.to_type(), PgType::BYTEA_ARRAY, "{name}");

    client
        .batch_execute(
            "TRUNCATE bytea_array_insert_roundtrip;
             TRUNCATE bytea_array_copy_roundtrip;",
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: failed to truncate test tables: {error}"));

    client
        .execute("INSERT INTO bytea_array_insert_roundtrip (ids) VALUES ($1)", &[wrapper])
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

    let row: [&(dyn ToSql + Sync); 1] = [wrapper];
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
