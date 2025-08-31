use std::path::Path;
use tracing::{error, info};

use crate::{
    abi::{ABIInput, ABIItem, EventInfo, GenerateAbiPropertiesType},
    helpers::camel_to_snake,
    indexer::Indexer,
    types::code::Code,
};

use crate::database::generate::{
    generate_indexer_contract_schema_name, generate_internal_factory_event_table_name,
    GenerateTablesForIndexerSqlError,
};
use crate::database::postgres::generate::{
    generate_internal_event_table_name, GenerateInternalFactoryEventTableNameParams,
};
use crate::manifest::contract::FactoryDetailsYaml;

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
        let factories = contract.details.iter().flat_map(|d| d.factory.clone()).collect::<Vec<_>>();

        if !disable_event_tables {
            sql.push_str(format!("CREATE DATABASE IF NOT EXISTS {};", schema_name).as_str());
            info!("Creating database if not exists: {}", schema_name);

            sql.push_str(&generate_event_table_clickhouse(&event_names, &schema_name));
        }

        sql.push_str(&generate_internal_event_table_clickhouse(
            &event_names,
            &schema_name,
            networks,
        ));

        sql.push_str(&generate_internal_factory_event_table_sql(&indexer.name, &factories));
    }

    Ok(Code::new(sql))
}

fn generate_event_table_clickhouse(abi_inputs: &[EventInfo], schema_name: &str) -> String {
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

            create_table_sql
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
                "block" UInt64
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

fn generate_internal_factory_event_table_sql(
    indexer_name: &str,
    factories: &[FactoryDetailsYaml],
) -> String {
    factories
        .iter()
        .map(|factory| {
            let params = GenerateInternalFactoryEventTableNameParams {
                indexer_name: indexer_name.to_string(),
                contract_name: factory.name.to_string(),
                event_name: factory.event_name.to_string(),
                input_names: factory.input_names(),
            };
            let table_name = generate_internal_factory_event_table_name(&params);

            let create_table_query = format!(
                r#"
            CREATE TABLE IF NOT EXISTS rindexer_internal.{table_name} (
                "factory_address" FixedString(42),
                "factory_deployed_address" FixedString(42),
                "network" TEXT
            )
            ENGINE = ReplacingMergeTree()
                ORDER BY ("factory_address", "factory_deployed_address", "network")"#
            );

            create_table_query
        })
        .collect::<Vec<_>>()
        .join("\n")
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

pub fn drop_tables_for_indexer_clickhouse(project_path: &Path, indexer: &Indexer) -> Code {
    let mut sql = String::new();

    sql.push_str("DROP TABLE IF EXISTS rindexer_internal.latest_block;");

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("DROP DATABASE IF EXISTS {schema_name};").as_str());

        // drop last synced blocks for contracts
        let abi_items = ABIItem::read_abi_items(project_path, contract);
        if let Ok(abi_items) = abi_items {
            for abi_item in abi_items.iter() {
                let table_name = generate_internal_event_table_name(&schema_name, &abi_item.name);
                sql.push_str(
                    format!("DROP TABLE IF EXISTS rindexer_internal.{table_name};").as_str(),
                );
            }
        } else {
            error!(
                "Could not read ABI items for contract moving on clearing the other data up: {}",
                contract.name
            );
        }

        // drop factory indexing tables
        for factory in contract.details.iter().flat_map(|d| d.factory.as_ref()) {
            let params = GenerateInternalFactoryEventTableNameParams {
                indexer_name: indexer.name.clone(),
                contract_name: factory.name.clone(),
                event_name: factory.event_name.clone(),
                input_names: factory.input_names(),
            };
            let table_name = generate_internal_factory_event_table_name(&params);
            sql.push_str(format!("DROP TABLE IF EXISTS rindexer_internal.{table_name};").as_str())
        }
    }

    Code::new(sql)
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
