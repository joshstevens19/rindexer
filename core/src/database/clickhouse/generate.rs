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
    generate_internal_factory_event_table_name_no_shorten, GenerateTablesForIndexerSqlError,
};
use crate::database::postgres::generate::{
    generate_internal_event_table_name_no_shorten, GenerateInternalFactoryEventTableNameParams,
};
use crate::manifest::contract::FactoryDetailsYaml;

pub fn generate_tables_for_indexer_clickhouse(
    project_path: &Path,
    indexer: &Indexer,
    database_name: &str,
    disable_event_tables: bool,
) -> Result<Code, GenerateTablesForIndexerSqlError> {
    let mut sql = String::new();

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let abi_items = ABIItem::read_abi_items(project_path, contract)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        let networks: Vec<&str> = contract.details.iter().map(|d| d.network.as_str()).collect();
        let factories = contract.details.iter().flat_map(|d| d.factory.clone()).collect::<Vec<_>>();

        if !disable_event_tables {
            info!("Creating event tables in database: {}", database_name);

            sql.push_str(&generate_event_table_clickhouse(
                &event_names,
                database_name,
                &schema_name,
            ));
        }

        sql.push_str(&generate_internal_event_table_clickhouse(
            &event_names,
            database_name,
            &schema_name,
            networks,
        ));

        sql.push_str(&generate_internal_factory_event_table_sql(
            database_name,
            &indexer.name,
            &factories,
        ));
    }

    Ok(Code::new(sql))
}

fn generate_event_table_clickhouse(
    abi_inputs: &[EventInfo],
    database_name: &str,
    schema_name: &str,
) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            let table_name =
                format!("{}.{}_{}", database_name, schema_name, camel_to_snake(&event_info.name));
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
    database_name: &str,
    schema_name: &str,
    networks: Vec<&str>,
) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            let table_name = format!(
                "{}.rindexer_internal_{}_{}",
                database_name,
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

            let insert_queries = networks
                .iter()
                .map(|network| {
                    format!(
                        r#"INSERT INTO {} ("network", "last_synced_block") VALUES ('{}', 0);"#,
                        table_name, network
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            let latest_block_table_name =
                format!("{}.rindexer_internal_latest_block", database_name);
            let create_latest_block_query = format!(
                r#"
            CREATE TABLE IF NOT EXISTS {} (
                "network" String,
                "block" UInt64
              )
              ENGINE = ReplacingMergeTree(block)
                ORDER BY network;
        "#,
                latest_block_table_name
            );

            let latest_block_insert_queries = networks
                .iter()
                .map(|network| {
                    format!(
                        r#"INSERT INTO {} ("network", "block") VALUES ('{network}', 0);"#,
                        latest_block_table_name
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            format!(
                "{} {} {} {}",
                create_table_query,
                insert_queries,
                create_latest_block_query,
                latest_block_insert_queries
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_internal_factory_event_table_sql(
    database_name: &str,
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
            let table_name = generate_internal_factory_event_table_name_no_shorten(&params);

            let create_table_query = format!(
                r#"
                CREATE TABLE IF NOT EXISTS {}.rindexer_internal_{} (
                    "factory_address" FixedString(42),
                    "factory_deployed_address" FixedString(42),
                    "network" String
                )
                ENGINE = ReplacingMergeTree()
                    ORDER BY ("network", "factory_address", "factory_deployed_address");
                "#,
                database_name, table_name
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

pub fn drop_tables_for_indexer_clickhouse(
    project_path: &Path,
    indexer: &Indexer,
    database_name: &str,
) -> Code {
    let mut sql = String::new();

    sql.push_str(&format!(
        "DROP TABLE IF EXISTS {}.rindexer_internal_latest_block;",
        database_name
    ));

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);

        // drop event tables
        let abi_items = ABIItem::read_abi_items(project_path, contract);
        if let Ok(abi_items) = abi_items {
            for abi_item in abi_items.iter() {
                // Drop event table
                let event_table_name =
                    format!("{}_{}", schema_name, camel_to_snake(&abi_item.name));
                sql.push_str(&format!(
                    "DROP TABLE IF EXISTS {}.{};",
                    database_name, event_table_name
                ));

                // Drop internal tracking table
                let internal_table_name =
                    generate_internal_event_table_name_no_shorten(&schema_name, &abi_item.name);
                sql.push_str(&format!(
                    "DROP TABLE IF EXISTS {}.rindexer_internal_{};",
                    database_name, internal_table_name
                ));
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
            sql.push_str(&format!(
                "DROP TABLE IF EXISTS {}.rindexer_internal_{};",
                database_name, table_name
            ))
        }
    }

    Code::new(sql)
}

#[allow(clippy::manual_strip)]
pub fn solidity_type_to_clickhouse_type(abi_type: &str) -> String {
    let is_array = abi_type.ends_with("[]");
    let base_type = abi_type.trim_end_matches("[]");

    let sql_type = match base_type {
        "address" => "FixedString(42)",
        "bool" => "Bool",
        "string" => "String",
        t if t.starts_with("bytes") => "String",
        t if t.starts_with("int") || t.starts_with("uint") => {
            let (prefix, size): (&str, usize) = if t.starts_with("int") {
                ("int", t[3..].parse().unwrap_or(256))
            } else {
                ("uint", t[4..].parse().unwrap_or(256))
            };

            let rounded_size = match size {
                0..=8 => 8,
                9..=16 => 16,
                17..=32 => 32,
                33..=64 => 64,
                65..=128 => 128,
                129..=256 => 256,
                _ => 512, // fallback to String
            };

            let int = match rounded_size {
                8 => "Int8",
                16 => "Int16",
                32 => "Int32",
                64 => "Int64",
                128 => "Int128",
                256 => "Int256",
                512 => "String",
                _ => unreachable!(),
            };

            if prefix == "uint" && rounded_size <= 256 {
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
