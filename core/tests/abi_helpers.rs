use rindexer::ABIItem;

fn abi_item(json: serde_json::Value) -> ABIItem {
    serde_json::from_value(json).expect("abi item should deserialize")
}

#[test]
fn abi_event_signature_formats_nested_tuples_and_arrays() {
    let item = abi_item(serde_json::json!({
        "type": "event",
        "name": "Complex",
        "inputs": [
            {
                "name": "ownerStats",
                "type": "tuple",
                "components": [
                    { "name": "owner", "type": "address" },
                    { "name": "amount", "type": "uint256" }
                ]
            },
            { "name": "proof", "type": "bytes32[2]" },
            {
                "name": "receivers",
                "type": "tuple[]",
                "components": [
                    { "name": "share", "type": "uint8" },
                    { "name": "wallet", "type": "address" }
                ]
            }
        ]
    }));

    assert_eq!(
        item.format_event_signature().expect("signature formats"),
        "Complex((address,uint256),bytes32[2],(uint8,address)[])"
    );
}

#[test]
fn abi_event_signature_errors_when_tuple_components_are_missing() {
    let item = abi_item(serde_json::json!({
        "type": "event",
        "name": "Broken",
        "inputs": [
            { "name": "details", "type": "tuple" }
        ]
    }));

    assert_eq!(
        item.format_event_signature()
            .expect_err("missing tuple components should fail")
            .to_string(),
        "tuple type specified but no components found"
    );
}

#[test]
fn abi_extract_event_names_ignores_non_events_and_builds_event_helpers() {
    let items: Vec<ABIItem> = serde_json::from_value(serde_json::json!([
        {
            "type": "function",
            "name": "transfer",
            "inputs": [
                { "name": "to", "type": "address" },
                { "name": "amount", "type": "uint256" }
            ]
        },
        {
            "type": "event",
            "name": "Transfer",
            "inputs": [
                { "indexed": true, "name": "from", "type": "address" },
                { "indexed": true, "name": "to", "type": "address" },
                { "name": "value", "type": "uint256" }
            ]
        }
    ]))
    .expect("abi items parse");

    let events =
        ABIItem::extract_event_names_and_signatures_from_abi(items).expect("events extract");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name, "Transfer");
    assert_eq!(events[0].struct_result(), "TransferResult");
    assert_eq!(events[0].struct_data(), "TransferData");
    assert_eq!(
        events[0].topic_id_as_hex_string(),
        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    );
}

#[test]
fn csv_headers_flatten_tuple_fields_and_append_rindexer_metadata() {
    let items: Vec<ABIItem> = serde_json::from_value(serde_json::json!([
        {
            "type": "event",
            "name": "PositionChanged",
            "inputs": [
                { "name": "account", "type": "address" },
                {
                    "name": "position",
                    "type": "tuple",
                    "components": [
                        { "name": "tokenId", "type": "uint256" },
                        { "name": "ownerAddress", "type": "address" }
                    ]
                }
            ]
        }
    ]))
    .expect("abi items parse");
    let event = ABIItem::extract_event_names_and_signatures_from_abi(items)
        .expect("events extract")
        .remove(0);

    assert_eq!(
        event.csv_headers_for_event(),
        vec![
            "contract_address",
            "account",
            "position_token_id",
            "position_owner_address",
            "tx_hash",
            "block_number",
            "block_hash",
            "network",
            "tx_index",
            "log_index",
        ]
    );
}

#[test]
fn create_csv_file_for_event_creates_output_and_checkpoint_directories() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let items: Vec<ABIItem> = serde_json::from_value(serde_json::json!([
        {
            "type": "event",
            "name": "Transfer",
            "inputs": [
                { "name": "from", "type": "address" },
                { "name": "to", "type": "address" }
            ]
        }
    ]))
    .expect("abi items parse");
    let event = ABIItem::extract_event_names_and_signatures_from_abi(items)
        .expect("events extract")
        .remove(0);

    let csv_file =
        event.create_csv_file_for_event(temp_dir.path(), "Token", "csv").expect("path builds");

    assert!(csv_file.ends_with("csv/Token/token-transfer.csv"));
    assert!(temp_dir.path().join("csv/Token").is_dir());
    assert!(temp_dir.path().join("csv/Token/last-synced-blocks").is_dir());
}
