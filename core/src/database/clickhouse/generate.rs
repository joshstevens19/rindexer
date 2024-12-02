use std::path::Path;

use tracing::{error, info};

use crate::{
    abi::{ABIInput, ABIItem, EventInfo, GenerateAbiPropertiesType, ParamTypeError, ReadAbiError},
    helpers::camel_to_snake,
    indexer::Indexer,
    manifest::contract::Contract,
    types::code::Code,
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
    indexer: &Indexer
) -> Result<Code, GenerateTablesForIndexerClickhouseError> {
    /*
    let mut sql = "CREATE SCHEMA IF NOT EXISTS rindexer_internal;".to_string();

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let abi_items = ABIItem::read_abi_items(project_path, contract)?;
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);
        let networks: Vec<&str> = contract.details.iter().map(|d| d.network.as_str()).collect();

        if !disable_event_tables {
            sql.push_str(format!("CREATE SCHEMA IF NOT EXISTS {};", schema_name).as_str());
            info!("Creating schema if not exists: {}", schema_name);

            let event_matching_name_on_other = find_clashing_event_names(
                project_path,
                contract,
                &indexer.contracts,
                &event_names,
            )?;

            sql.push_str(&generate_event_table_sql_with_comments(
                &event_names,
                &contract.name,
                &schema_name,
                event_matching_name_on_other,
            ));
        }
        // we still need to create the internal tables for the contract
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
    */
    Ok(Code::new("".parse().unwrap()))

}