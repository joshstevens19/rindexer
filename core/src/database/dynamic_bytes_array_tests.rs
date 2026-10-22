//! Regressions for ABI-decoded bytes arrays, not preconstructed SQL wrappers.
//!
//! The PostgreSQL cases require Docker and exercise both INSERT and binary COPY.

use alloy::{json_abi::Event, primitives::Address, rpc::types::Log};
use futures::pin_mut;
use serde_json::json;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::{binary_copy::BinaryCopyInWriter, types::ToSql};

use super::postgres::generate::solidity_type_to_db_type;
use super::sql_type_wrapper::{map_log_params_to_ethereum_wrapper, EthereumSqlTypeWrapper};
use crate::abi::ABIInput;
use crate::helpers::parse_log;

// Literal ABI payload for Values(bytes[]): [0x01, 0x, 0x0200ff, 33 bytes of 0xa5].
// The last element crosses a 32-byte ABI word boundary. Expected values below
// are independent of the decoder and converter under test.
const DYNAMIC_BYTES_DATA: &str = concat!(
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000004",
    "0000000000000000000000000000000000000000000000000000000000000080",
    "00000000000000000000000000000000000000000000000000000000000000c0",
    "00000000000000000000000000000000000000000000000000000000000000e0",
    "0000000000000000000000000000000000000000000000000000000000000120",
    "0000000000000000000000000000000000000000000000000000000000000001",
    "0100000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000003",
    "0200ff0000000000000000000000000000000000000000000000000000000000",
    "0000000000000000000000000000000000000000000000000000000000000021",
    "a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5",
    "a500000000000000000000000000000000000000000000000000000000000000",
);

const SINGLE_EMPTY_BYTES_DATA: &str = concat!(
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000001",
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

const EMPTY_ARRAY_DATA: &str = concat!(
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000000",
);

const FIXED_BYTES_DATA: &str = concat!(
    "0000000000000000000000000000000000000000000000000000000000000020",
    "0000000000000000000000000000000000000000000000000000000000000002",
    "1111111111111111111111111111111111111111111111111111111111111111",
    "2222222222222222222222222222222222222222222222222222222222222222",
);

fn expected_values() -> Vec<Vec<u8>> {
    vec![vec![0x01], vec![], vec![0x02, 0x00, 0xff], vec![0xa5; 33]]
}

fn decode_array_wrapper(abi_type: &str, data: &str) -> EthereumSqlTypeWrapper {
    let event_json = json!({
        "type": "event",
        "name": "Values",
        "anonymous": false,
        "inputs": [{"name": "payload", "type": abi_type, "indexed": false}]
    });
    let inputs: Vec<ABIInput> =
        serde_json::from_value(event_json["inputs"].clone()).expect("valid ABI inputs");
    let event: Event = serde_json::from_value(event_json).expect("valid event ABI");
    let log = Log {
        inner: alloy::primitives::Log::new_unchecked(
            Address::ZERO,
            vec![event.selector()],
            hex::decode(data).expect("valid ABI payload hex").into(),
        ),
        ..Default::default()
    };
    let parsed = parse_log(&event, &log).expect("fixture must decode before conversion");
    let mut wrappers = map_log_params_to_ethereum_wrapper(&inputs, &parsed.params);
    assert_eq!(wrappers.len(), 1, "one array field must map to one column");
    wrappers.pop().expect("one wrapper")
}

fn converted_values(abi_type: &str, data: &str) -> Vec<Vec<u8>> {
    match decode_array_wrapper(abi_type, data) {
        EthereumSqlTypeWrapper::VecBytes(values) => {
            values.into_iter().map(|value| value.to_vec()).collect()
        }
        other => panic!("expected a bytes-array wrapper, got {other:?}"),
    }
}

#[test]
fn abi_dynamic_bytes_array_preserves_elements() {
    assert_eq!(
        converted_values("bytes[]", DYNAMIC_BYTES_DATA),
        expected_values(),
        "dynamic bytes[] conversion must preserve element data and order"
    );
}

#[test]
fn abi_dynamic_bytes_array_preserves_one_empty_element() {
    assert_eq!(
        converted_values("bytes[]", SINGLE_EMPTY_BYTES_DATA),
        vec![Vec::<u8>::new()],
        "one empty bytes element must not become an empty array"
    );
}

#[test]
fn abi_dynamic_bytes_array_keeps_empty_array_behavior() {
    assert!(converted_values("bytes[]", EMPTY_ARRAY_DATA).is_empty());
}

#[test]
fn abi_fixed_bytes_array_preserves_existing_values() {
    assert_eq!(
        converted_values("bytes32[]", FIXED_BYTES_DATA),
        vec![vec![0x11; 32], vec![0x22; 32]]
    );
}

#[tokio::test]
async fn abi_dynamic_bytes_array_insert_preserves_elements() {
    assert_postgres_roundtrip(false).await;
}

#[tokio::test]
async fn abi_dynamic_bytes_array_copy_preserves_elements() {
    assert_postgres_roundtrip(true).await;
}

async fn assert_postgres_roundtrip(via_copy: bool) {
    let pg = Postgres::default().start().await.expect("failed to start postgres");
    let port = pg.get_host_port_ipv4(5432).await.expect("failed to get postgres port");
    let connection_string =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
    let (client, connection) = tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
        .await
        .expect("failed to connect to postgres");
    let connection_task = tokio::spawn(async move {
        connection.await.expect("postgres connection failed");
    });

    let column_type = solidity_type_to_db_type("bytes[]");
    assert_eq!(column_type, "BYTEA[]");
    client
        .batch_execute(&format!("CREATE TABLE array_roundtrip (payload {column_type})"))
        .await
        .expect("failed to create table");

    // Decode the same event payload as production; constructing VecBytes here
    // would bypass the defective ABI-to-wrapper conversion entirely.
    let wrapper = decode_array_wrapper("bytes[]", DYNAMIC_BYTES_DATA);
    if via_copy {
        let sink = client
            .copy_in("COPY array_roundtrip (payload) FROM STDIN WITH (FORMAT binary)")
            .await
            .expect("failed to start COPY");
        let writer = BinaryCopyInWriter::new(sink, &[wrapper.to_type()]);
        pin_mut!(writer);
        let row: [&(dyn ToSql + Sync); 1] = [&wrapper];
        writer.as_mut().write(&row).await.expect("failed to write COPY row");
        writer.finish().await.expect("failed to finish COPY");
    } else {
        client
            .execute("INSERT INTO array_roundtrip (payload) VALUES ($1)", &[&wrapper])
            .await
            .expect("failed to INSERT row");
    }

    let row = client
        .query_one("SELECT payload FROM array_roundtrip", &[])
        .await
        .expect("failed to query stored array");
    let stored: Option<Vec<Vec<u8>>> = row.get("payload");
    let operation = if via_copy { "COPY" } else { "INSERT" };
    assert_eq!(
        stored,
        Some(expected_values()),
        "ABI-decoded dynamic bytes[] must survive {operation} unchanged"
    );

    drop(client);
    connection_task.await.expect("postgres connection task failed");
}
