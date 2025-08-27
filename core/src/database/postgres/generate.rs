use alloy::primitives::keccak256;
use std::path::Path;
use tracing::error;

use crate::database::generate::{
    compact_table_name_if_needed, generate_indexer_contract_schema_name,
    generate_internal_factory_event_table_name,
};
use crate::helpers::parse_solidity_integer_type;
use crate::{
    abi::{ABIInput, ABIItem, GenerateAbiPropertiesType, ParamTypeError, ReadAbiError},
    helpers::camel_to_snake,
    indexer::{
        native_transfer::{NATIVE_TRANSFER_ABI, NATIVE_TRANSFER_CONTRACT_NAME},
        Indexer,
    },
    manifest::contract::Contract,
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
        "block_timestamp".to_string(),
        "block_hash".to_string(),
        "network".to_string(),
        "tx_index".to_string(),
        "log_index".to_string(),
    ]);
    column_names
}

pub fn generate_internal_event_table_name(schema_name: &str, event_name: &str) -> String {
    let table_name = format!("{}_{}", schema_name, camel_to_snake(event_name));

    compact_table_name_if_needed(table_name)
}

pub struct GenerateInternalFactoryEventTableNameParams {
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub input_names: Vec<String>,
}

pub fn drop_tables_for_indexer_sql(project_path: &Path, indexer: &Indexer) -> Code {
    let mut sql = format!(
        "DROP TABLE IF EXISTS rindexer_internal.{}_last_known_indexes_dropping_sql CASCADE;",
        camel_to_snake(&indexer.name)
    );
    sql.push_str(format!("DROP TABLE IF EXISTS rindexer_internal.{}_last_known_relationship_dropping_sql CASCADE;", camel_to_snake(&indexer.name)).as_str());

    sql.push_str("DROP TABLE IF EXISTS rindexer_internal.latest_block;");

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        sql.push_str(format!("DROP SCHEMA IF EXISTS {schema_name} CASCADE;").as_str());

        // drop last synced blocks for contracts
        let abi_items = ABIItem::read_abi_items(project_path, contract);
        if let Ok(abi_items) = abi_items {
            for abi_item in abi_items.iter() {
                let table_name = generate_internal_event_table_name(&schema_name, &abi_item.name);
                sql.push_str(
                    format!("DROP TABLE IF EXISTS rindexer_internal.{table_name} CASCADE;")
                        .as_str(),
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
            sql.push_str(
                format!("DROP TABLE IF EXISTS rindexer_internal.{table_name} CASCADE;").as_str(),
            )
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
            let (prefix, size) = parse_solidity_integer_type(t);

            match size {
                8 | 16 => "SMALLINT",
                24 | 32 => "INTEGER",
                40 | 48 | 56 | 64 | 72 | 80 | 88 | 96 | 104 | 112 | 120 | 128 => "NUMERIC",
                136 | 144 | 152 | 160 | 168 | 176 | 184 | 192 | 200 | 208 | 216 | 224 | 232
                | 240 | 248 | 256 => "VARCHAR(78)",
                _ => panic!("Unsupported {prefix}N size: {size}"),
            }
        }
        _ => panic!("Unsupported type: {base_type}"),
    };

    // Return the SQL type, appending array brackets if necessary
    if is_array {
        // CHAR(42)[] does not work nicely with parsers so using
        // TEXT[] works out the box and CHAR(42) doesnt protect much anyway
        // as its already in type Address
        if base_type == "address" {
            return "TEXT[]".to_string();
        }
        format!("{sql_type}[]")
    } else {
        sql_type.to_string()
    }
}
