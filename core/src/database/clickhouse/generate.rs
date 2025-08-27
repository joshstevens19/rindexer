use async_std::prelude::StreamExt;
use std::path::Path;
use tracing::{error, info};

use crate::{
    abi::{ABIInput, ABIItem, EventInfo, GenerateAbiPropertiesType, ParamTypeError, ReadAbiError},
    helpers::camel_to_snake,
    indexer::Indexer,
    manifest::contract::Contract,
    types::code::Code,
};

use crate::database::generate::{
    generate_indexer_contract_schema_name, GenerateTablesForIndexerSqlError,
};

pub fn generate_tables_for_indexer_clickhouse(
    project_path: &Path,
    indexer: &Indexer,
    disable_event_tables: bool,
) -> Result<Code, GenerateTablesForIndexerSqlError> {
    let mut sql = "CREATE DATABASE IF NOT EXISTS rindexer_internal;".to_string();

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let abi_items = ABIItem::read_abi_items(project_path, contract)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        let networks: Vec<&str> = contract.details.iter().map(|d| d.network.as_str()).collect();

        if !disable_event_tables {
            sql.push_str(format!("CREATE DATABASE IF NOT EXISTS {};", schema_name).as_str());

            info!("Creating database if not exists: {}", schema_name);

            let event_matching_name_on_other = find_clashing_event_names(
                project_path,
                contract,
                &indexer.contracts,
                &event_names,
            )?;

            sql.push_str(&generate_event_table_clickhouse(
                &event_names,
                &contract.name,
                &schema_name,
                event_matching_name_on_other,
            ));
        }
        // we still need to create the internal tables for the contract
        sql.push_str(&generate_internal_event_table_clickhouse(
            &event_names,
            &schema_name,
            networks,
        ));
    }

    Ok(Code::new(sql))
}

fn find_clashing_event_names(
    project_path: &Path,
    current_contract: &Contract,
    other_contracts: &[Contract],
    current_event_names: &[EventInfo],
) -> Result<Vec<String>, GenerateTablesForIndexerSqlError> {
    let mut clashing_events = Vec::new();

    for other_contract in other_contracts {
        if other_contract.name == current_contract.name {
            continue;
        }

        let other_abi_items = ABIItem::read_abi_items(project_path, other_contract)?;
        let other_event_names =
            ABIItem::extract_event_names_and_signatures_from_abi(other_abi_items)?;

        for event_name in current_event_names {
            if other_event_names.iter().any(|e| e.name == event_name.name)
                && !clashing_events.contains(&event_name.name)
            {
                clashing_events.push(event_name.name.clone());
            }
        }
    }

    Ok(clashing_events)
}

fn generate_event_table_clickhouse(
    abi_inputs: &[EventInfo],
    contract_name: &str,
    schema_name: &str,
    apply_full_name_comment_for_events: Vec<String>,
) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            let table_name = format!("{}.{}", schema_name, camel_to_snake(&event_info.name));
            info!("Creating table if not exists: {}", table_name);
            let event_columns = if event_info.inputs.is_empty() {
                "".to_string()
            } else {
                generate_columns_with_data_types(&event_info.inputs).join(", ") + ","
            };

            //  TODO: Nullable isn't recommended in OLAP
            let create_table_sql = format!(
                r#"CREATE TABLE IF NOT EXISTS {} (
                    contract_address FixedString(42),
                    {}
                    tx_hash FixedString(66),
                    block_number UInt64,
                    block_timestamp Nullable(DateTime('UTC')),
                    block_hash FixedString(66),
                    network String,
                    tx_index UInt64,
                    log_index UInt64,

                    index idx_block_num (block_number) type minmax granularity 1,
                    index idx_timestamp (block_timestamp) type minmax granularity 1,
                    index idx_network (network) type bloom_filter granularity 1,
                    index idx_tx_hash (tx_hash) type bloom_filter granularity 1
                )
                ENGINE = ReplacingMergeTree
                ORDER BY (network, block_number, tx_hash, log_index);"#,
                table_name, event_columns
            );

            return create_table_sql;
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_internal_event_table_clickhouse(
    abi_inputs: &[EventInfo],
    schema_name: &str,
    networks: Vec<&str>,
) -> String {
    abi_inputs.iter().map(|event_info| {
        let table_name = format!(
            "rindexer_internal.{}_{}",
            schema_name,
            camel_to_snake(&event_info.name)
        );

        let create_table_query = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {} (
                    "network" String,
                    "last_synced_block" UInt64
                )
                    ENGINE = ReplacingMergeTree(last_synced_block)
                    ORDER BY network;"#,
            table_name
        );

        let insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT INTO {} ("network", "last_synced_block") VALUES ('{}', 0);"#,
                table_name,
                network
            )
        }).collect::<Vec<_>>().join("\n");

        let create_latest_block_query = r#"
            CREATE TABLE IF NOT EXISTS rindexer_internal.latest_block (
                "network" String,
                "block" UInt256
              )
              ENGINE = ReplacingMergeTree(block)
                ORDER BY network;
        "#.to_string();

        let latest_block_insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT INTO rindexer_internal.latest_block ("network", "block") VALUES ('{network}', 0);"#
            )
        }).collect::<Vec<_>>().join("\n");


        format!("{} {} {} {}", create_table_query, insert_queries, create_latest_block_query, latest_block_insert_queries)
    }).collect::<Vec<_>>().join("\n")
}

fn generate_columns(inputs: &[ABIInput], property_type: &GenerateAbiPropertiesType) -> Vec<String> {
    ABIInput::generate_abi_name_properties(inputs, property_type, None)
        .into_iter()
        .map(|m| m.value)
        .collect()
}

pub fn generate_columns_with_data_types(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::ClickhouseWithDataTypes)
}
#[allow(clippy::manual_strip)]
pub fn solidity_type_to_clickhouse_type(abi_type: &str) -> String {
    let is_array = abi_type.ends_with("[]");
    let base_type = abi_type.trim_end_matches("[]");

    let sql_type = match base_type {
        "address" => "FixedString(42)", // Use FixedString for fixed-size strings
        "bool" => "Bool",               // Use UInt8 to represent booleans (0 or 1)
        "string" => "String",           // Use String for variable-length text
        t if t.starts_with("bytes") => "String", // Use String for binary data
        t if t.starts_with("int") || t.starts_with("uint") => {
            // Handling fixed-size integers (intN and uintN where N can be 8 to 256 in steps of 8)
            let (prefix, size): (&str, usize) = if t.starts_with("int") {
                ("int", t[3..].parse().expect("Invalid intN type"))
            } else {
                ("uint", t[4..].parse().expect("Invalid uintN type"))
            };

            let int = match size {
                8 => "Int8",
                16 => "Int16",
                32 => "Int32",
                64 => "Int64",
                128 => "Int128",
                256 => "Int256",
                // Use String for very large integers as ClickHouse
                // doesn't support greater than UInt256.
                512 => "String",
                _ => panic!("Unsupported {}N size: {}", prefix, size),
            };

            if prefix == "uint" {
                &format!("U{}", int)
            } else {
                int
            }
        }
        _ => panic!("Unsupported type: {}", base_type),
    };

    // Return the SQL type, appending array brackets if necessary
    if is_array {
        // ClickHouse does not have native array types with specific sizes like PostgreSQL
        // Represent arrays as Array(T) where T is the base type
        format!("Array({})", sql_type)
    } else {
        sql_type.to_string()
    }
}
