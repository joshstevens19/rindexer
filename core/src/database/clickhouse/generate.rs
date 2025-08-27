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

#[derive(thiserror::Error, Debug)]
pub enum GenerateTablesForIndexerClickhouseError {
    #[error("{0}")]
    ReadAbiError(#[from] ReadAbiError),

    #[error("{0}")]
    ParamTypeError(#[from] ParamTypeError),
}
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
            info!("Creating schema if not exists: {}", schema_name);

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

    sql.push_str(&format!(
        r#"
            CREATE TABLE IF NOT EXISTS rindexer_internal.{indexer_name}_last_known_indexes_dropping_sql
            (
                key Int32,
                value String
            )
            ENGINE = MergeTree
            ORDER BY key;
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal.{indexer_name}_last_known_indexes_dropping_sql
         (
            key Int32,
            value String
         )
        ENGINE = MergeTree
        ORDER BY key;
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

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

            let create_table_sql = format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                rindexer_id UInt64 NOT NULL, \
                contract_address FixedString(66) NOT NULL, \
                {} \
                tx_hash FixedString(66) NOT NULL, \
                block_number Float64 NOT NULL, \
                block_hash FixedString(66) NOT NULL, \
                network String NOT NULL, \
                tx_index Float64 NOT NULL, \
                log_index String NOT NULL\
                )\
                ENGINE = MergeTree
                ORDER BY rindexer_id;",
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
            r#"CREATE TABLE IF NOT EXISTS {} ("network" String NOT NULL, "last_synced_block" Float64 NOT NULL) ENGINE = MergeTree ORDER BY network;;"#,
            table_name
        );

        let insert_queries = networks.iter().map(|network| {

            format!(
                r#"INSERT INTO {} ("network", "last_synced_block") VALUES ('{}', 0);"#,
                table_name,
                network
            )
        }).collect::<Vec<_>>().join("\n");

        format!("{}", create_table_query)
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
        "bool" => "UInt8",              // Use UInt8 to represent booleans (0 or 1)
        "string" => "String",           // Use String for variable-length text
        t if t.starts_with("bytes") => "String", // Use String for binary data
        t if t.starts_with("int") || t.starts_with("uint") => {
            // Handling fixed-size integers (intN and uintN where N can be 8 to 256 in steps of 8)
            let (prefix, size): (&str, usize) = if t.starts_with("int") {
                ("int", t[3..].parse().expect("Invalid intN type"))
            } else {
                ("uint", t[4..].parse().expect("Invalid uintN type"))
            };

            match size {
                8 => "Int8",
                16 => "Int16",
                32 => "Int32",
                64 => "Int64",
                128 | 256 => "String", // Use String for very large integers as ClickHouse doesn't support 128/256-bit integers
                _ => panic!("Unsupported {}N size: {}", prefix, size),
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
