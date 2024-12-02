use crate::helpers::camel_to_snake;

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
