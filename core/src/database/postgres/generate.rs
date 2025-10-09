use crate::database::generate::compact_table_name_if_needed;
use crate::helpers::parse_solidity_integer_type;
use crate::{
    abi::{ABIInput, GenerateAbiPropertiesType},
    helpers::camel_to_snake,
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

pub fn generate_internal_event_table_name_no_shorten(
    schema_name: &str,
    event_name: &str,
) -> String {
    format!("{}_{}", schema_name, camel_to_snake(event_name))
}

pub struct GenerateInternalFactoryEventTableNameParams {
    pub indexer_name: String,
    pub contract_name: String,
    pub event_name: String,
    pub input_names: Vec<String>,
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
