use std::path::Path;

use tracing::{error, info};

use crate::{
    abi::{ABIInput, ABIItem, EventInfo, GenerateAbiPropertiesType, ParamTypeError, ReadAbiError},
    helpers::camel_to_snake,
    indexer::Indexer,
    types::code::Code,
};

fn generate_columns(inputs: &[ABIInput], property_type: &GenerateAbiPropertiesType) -> Vec<String> {
    ABIInput::generate_abi_name_properties(inputs, property_type, None)
        .into_iter()
        .map(|m| m.value)
        .collect()
}

pub fn generate_columns_with_data_types(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresWithDataTypes)
}

fn generate_columns_names_only(inputs: &[ABIInput]) -> Vec<String> {
    generate_columns(inputs, &GenerateAbiPropertiesType::PostgresColumnsNamesOnly)
}

pub fn generate_column_names_only_with_base_properties(inputs: &[ABIInput]) -> Vec<String> {
    let mut column_names: Vec<String> = vec!["contract_address".to_string()];
    column_names.extend(generate_columns_names_only(inputs));
    column_names.extend(vec![
        "tx_hash".to_string(),
        "block_number".to_string(),
        "block_hash".to_string(),
        "network".to_string(),
        "tx_index".to_string(),
        "log_index".to_string(),
    ]);
    column_names
}

fn generate_event_table_sql(abi_inputs: &[EventInfo], schema_name: &str) -> String {
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

            format!(
                "CREATE TABLE IF NOT EXISTS {} (\
                rindexer_id SERIAL PRIMARY KEY NOT NULL, \
                contract_address CHAR(66) NOT NULL, \
                {} \
                tx_hash CHAR(66) NOT NULL, \
                block_number NUMERIC NOT NULL, \
                block_hash CHAR(66) NOT NULL, \
                network VARCHAR(50) NOT NULL, \
                tx_index NUMERIC NOT NULL, \
                log_index VARCHAR(78) NOT NULL\
            );",
                table_name, event_columns
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_internal_event_table_sql(
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
            r#"CREATE TABLE IF NOT EXISTS {} ("network" TEXT PRIMARY KEY, "last_synced_block" NUMERIC);"#,
            table_name
        );

        let insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT INTO {} ("network", "last_synced_block") VALUES ('{}', 0) ON CONFLICT ("network") DO NOTHING;"#,
                table_name,
                network
            )
        }).collect::<Vec<_>>().join("\n");

        format!("{}\n{}", create_table_query, insert_queries)
    }).collect::<Vec<_>>().join("\n")
}

#[derive(thiserror::Error, Debug)]
pub enum GenerateTablesForIndexerSqlError {
    #[error("{0}")]
    ReadAbiError(#[from] ReadAbiError),

    #[error("{0}")]
    ParamTypeError(#[from] ParamTypeError),
}

pub fn generate_tables_for_indexer_sql(
    project_path: &Path,
    indexer: &Indexer,
) -> Result<Code, GenerateTablesForIndexerSqlError> {
    let mut sql = "CREATE SCHEMA IF NOT EXISTS rindexer_internal;".to_string();

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let abi_items = ABIItem::read_abi_items(project_path, contract)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("CREATE SCHEMA IF NOT EXISTS {};", schema_name).as_str());
        info!("Creating schema if not exists: {}", schema_name);

        let networks: Vec<&str> = contract.details.iter().map(|d| d.network.as_str()).collect();

        sql.push_str(&generate_event_table_sql(&event_names, &schema_name));
        sql.push_str(&generate_internal_event_table_sql(&event_names, &schema_name, networks));
    }

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal.{indexer_name}_last_known_relationship_dropping_sql (
            key INT PRIMARY KEY,
            value TEXT NOT NULL
        );
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal.{indexer_name}_last_known_indexes_dropping_sql (
            key INT PRIMARY KEY,
            value TEXT NOT NULL
        );
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    Ok(Code::new(sql))
}

pub fn generate_event_table_full_name(
    indexer_name: &str,
    contract_name: &str,
    event_name: &str,
) -> String {
    let schema_name = generate_indexer_contract_schema_name(indexer_name, contract_name);
    format!("{}.{}", schema_name, camel_to_snake(event_name))
}

pub fn generate_event_table_columns_names_sql(column_names: &[String]) -> String {
    column_names.iter().map(|name| format!("\"{}\"", name)).collect::<Vec<String>>().join(", ")
}

pub fn generate_indexer_contract_schema_name(indexer_name: &str, contract_name: &str) -> String {
    format!("{}_{}", camel_to_snake(indexer_name), camel_to_snake(contract_name))
}

pub fn drop_tables_for_indexer_sql(project_path: &Path, indexer: &Indexer) -> Code {
    let mut sql = format!(
        "DROP TABLE IF EXISTS rindexer_internal.{}_last_known_indexes_dropping_sql CASCADE;",
        camel_to_snake(&indexer.name)
    );
    sql.push_str(format!("DROP TABLE IF EXISTS rindexer_internal.{}_last_known_relationship_dropping_sql CASCADE;", camel_to_snake(&indexer.name)).as_str());

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("DROP SCHEMA IF EXISTS {} CASCADE;", schema_name).as_str());

        // drop last synced blocks for contracts
        let abi_items = ABIItem::read_abi_items(project_path, contract);
        if let Ok(abi_items) = abi_items {
            for abi_item in abi_items.iter() {
                let table_name = format!("{}_{}", schema_name, camel_to_snake(&abi_item.name));
                sql.push_str(
                    format!("DROP TABLE IF EXISTS rindexer_internal.{} CASCADE;", table_name)
                        .as_str(),
                );
            }
        } else {
            error!(
                "Could not read ABI items for contract moving on clearing the other data up: {}",
                contract.name
            );
        }
    }

    Code::new(sql)
}

#[allow(clippy::manual_strip)]
pub fn solidity_type_to_db_type(abi_type: &str) -> String {
    let is_array = abi_type.ends_with("[]");
    let base_type = abi_type.trim_end_matches("[]");

    let sql_type = match base_type {
        "address" => "CHAR(42)",
        "bool" => "BOOLEAN",
        "string" => "TEXT",
        t if t.starts_with("bytes") => "BYTEA",
        t if t.starts_with("int") || t.starts_with("uint") => {
            // Handling fixed-size integers (intN and uintN where N can be 8 to 256 in steps of 8)
            let (prefix, size): (&str, usize) = if t.starts_with("int") {
                ("int", t[3..].parse().expect("Invalid intN type"))
            } else {
                ("uint", t[4..].parse().expect("Invalid uintN type"))
            };

            match size {
                8 | 16 => "SMALLINT",
                24 | 32 => "INTEGER",
                40 | 48 | 56 | 64 | 72 | 80 | 88 | 96 | 104 | 112 | 120 | 128 => "NUMERIC",
                136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232 |
                240 | 248 | 256 => "VARCHAR(78)",
                _ => panic!("Unsupported {}N size: {}", prefix, size),
            }
        }
        _ => panic!("Unsupported type: {}", base_type),
    };

    // Return the SQL type, appending array brackets if necessary
    if is_array {
        // CHAR(42)[] does not work nicely with parsers so using
        // TEXT[] works out the box and CHAR(42) doesnt protect much anyway
        // as its already in type Address
        if base_type == "address" {
            return "TEXT[]".to_string();
        }
        format!("{}[]", sql_type)
    } else {
        sql_type.to_string()
    }
}
