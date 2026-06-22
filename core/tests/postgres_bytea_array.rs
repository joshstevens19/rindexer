//! PostgreSQL round-trip tests for byte array wrappers.
//!
//! Requires Docker:
//!   cargo test -p rindexer --test postgres_bytea_array

use alloy::primitives::Bytes;
use rindexer::{EthereumSqlTypeWrapper, PgType, solidity_type_to_ethereum_sql_type_wrapper};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

#[tokio::test]
async fn vec_bytes_inserts_into_postgres_bytea_array() {
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
        .batch_execute("CREATE TABLE bytea_array_roundtrip (ids BYTEA[] NOT NULL)")
        .await
        .expect("failed to create test table");

    let first = vec![1u8; 32];
    let second = vec![2u8; 32];
    let mut mapped_ids = match solidity_type_to_ethereum_sql_type_wrapper("bytes32[]")
        .expect("bytes32[] should map to a SQL wrapper")
    {
        EthereumSqlTypeWrapper::VecBytes(values) => values,
        other => panic!("bytes32[] should map to VecBytes, got {other:?}"),
    };
    mapped_ids.push(Bytes::copy_from_slice(&first));
    mapped_ids.push(Bytes::copy_from_slice(&second));

    let ids = EthereumSqlTypeWrapper::VecBytes(mapped_ids);
    assert_eq!(ids.to_type(), PgType::BYTEA_ARRAY);

    client
        .execute("INSERT INTO bytea_array_roundtrip (ids) VALUES ($1)", &[&ids])
        .await
        .expect("failed to insert bytea array");

    let row = client
        .query_one("SELECT ids FROM bytea_array_roundtrip", &[])
        .await
        .expect("failed to query bytea array");
    let stored_ids: Vec<Vec<u8>> = row.get("ids");

    assert_eq!(stored_ids, vec![first, second]);
}
