use crate::abi::{EventInfo, ParamTypeError, ReadAbiError};
use crate::database::sqlite::client::SqliteError;
use crate::helpers::camel_to_snake;
use crate::indexer::native_transfer::{NATIVE_TRANSFER_ABI, NATIVE_TRANSFER_CONTRACT_NAME};
use crate::indexer::Indexer;
use crate::manifest::contract::FactoryDetailsYaml;
use crate::types::code::Code;
use crate::ABIItem;
use std::path::Path;
use tracing::{error, info};

#[derive(thiserror::Error, Debug)]
pub enum GenerateTablesForIndexerSqlError {
    #[error("{0}")]
    ReadAbiError(#[from] ReadAbiError),

    #[error("{0}")]
    ParamTypeError(#[from] ParamTypeError),

    #[error("failed to execute {0}")]
    Sqlite(#[from] SqliteError),
}

fn sqlite_type_for_solidity(solidity_type: &str) -> &'static str {
    // SQLite has only 5 storage classes: NULL, INTEGER, REAL, TEXT, BLOB
    // We'll use TEXT for most things for simplicity
    if solidity_type.starts_with("uint") || solidity_type.starts_with("int") {
        "TEXT" // Store large numbers as text to avoid overflow
    } else if solidity_type == "bool" {
        "INTEGER" // 0 or 1
    } else {
        // includes bytes, address and anything else
        "TEXT"
    }
}

fn generate_event_table_sql(abi_inputs: &[EventInfo], table_prefix: &str) -> String {
    abi_inputs
        .iter()
        .map(|event_info| {
            let table_name = format!("{}_{}", table_prefix, camel_to_snake(&event_info.name));
            info!("Creating table if not exists: {}", table_name);

            let event_columns = if event_info.inputs.is_empty() {
                String::new()
            } else {
                event_info
                    .inputs
                    .iter()
                    .map(|input| {
                        let col_name = camel_to_snake(&input.name);
                        let col_type = sqlite_type_for_solidity(&input.type_);
                        format!("{} {}", col_name, col_type)
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
                    + ","
            };

            format!(
                "CREATE TABLE IF NOT EXISTS {table_name} (\
                    rindexer_id INTEGER PRIMARY KEY AUTOINCREMENT, \
                    contract_address TEXT NOT NULL, \
                    {event_columns} \
                    tx_hash TEXT NOT NULL, \
                    block_number INTEGER NOT NULL, \
                    block_timestamp TEXT, \
                    block_hash TEXT NOT NULL, \
                    network TEXT NOT NULL, \
                    tx_index INTEGER NOT NULL, \
                    log_index TEXT NOT NULL\
                );"
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn generate_internal_event_table_sql(
    abi_inputs: &[EventInfo],
    table_prefix: &str,
    networks: Vec<&str>,
) -> String {
    abi_inputs.iter().map(|event_info| {
        let table_name = format!("rindexer_internal_{}_{}", table_prefix, camel_to_snake(&event_info.name));

        let create_table_query = format!(
            r#"CREATE TABLE IF NOT EXISTS {table_name} (network TEXT PRIMARY KEY, last_synced_block INTEGER);"#
        );

        let insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT OR IGNORE INTO {table_name} (network, last_synced_block) VALUES ('{network}', 0);"#
            )
        }).collect::<Vec<_>>().join("\n");

        let create_latest_block_query = r#"CREATE TABLE IF NOT EXISTS rindexer_internal_latest_block (network TEXT PRIMARY KEY, block INTEGER);"#.to_string();

        let latest_block_insert_queries = networks.iter().map(|network| {
            format!(
                r#"INSERT OR IGNORE INTO rindexer_internal_latest_block (network, block) VALUES ('{network}', 0);"#
            )
        }).collect::<Vec<_>>().join("\n");

        format!("{create_table_query}\n{insert_queries}\n{create_latest_block_query}\n{latest_block_insert_queries}")
    }).collect::<Vec<_>>().join("\n")
}

fn generate_internal_factory_event_table_sql(
    indexer_name: &str,
    factories: &[FactoryDetailsYaml],
) -> String {
    factories.iter().map(|factory| {
        let table_name = format!(
            "rindexer_internal_{}_{}_{}_{}",
            camel_to_snake(indexer_name),
            camel_to_snake(&factory.name),
            camel_to_snake(&factory.event_name),
            factory.input_names().iter().map(|v| camel_to_snake(v)).collect::<Vec<String>>().join("_")
        );

        format!(
            r#"CREATE TABLE IF NOT EXISTS {table_name} (factory_address TEXT, factory_deployed_address TEXT, network TEXT, PRIMARY KEY (factory_address, factory_deployed_address, network));"#
        )
    }).collect::<Vec<_>>().join("\n")
}

pub fn generate_tables_for_indexer_sql(
    project_path: &Path,
    indexer: &Indexer,
    _disable_event_tables: bool,
) -> Result<Code, GenerateTablesForIndexerSqlError> {
    let mut sql = String::new();

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let abi_items = ABIItem::read_abi_items(project_path, contract)?;
        let events = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let table_prefix =
            format!("{}_{}", camel_to_snake(&indexer.name), camel_to_snake(&contract_name));
        let networks: Vec<&str> = contract.details.iter().map(|d| d.network.as_str()).collect();
        let factories = contract.details.iter().flat_map(|d| d.factory.clone()).collect::<Vec<_>>();

        // Create event tables
        sql.push_str(&generate_event_table_sql(&events, &table_prefix));

        // Create internal tracking tables
        sql.push_str(&generate_internal_event_table_sql(&events, &table_prefix, networks));

        // Create factory tables if needed
        if !factories.is_empty() {
            sql.push_str(&generate_internal_factory_event_table_sql(&indexer.name, &factories));
        }
    }

    if indexer.native_transfers.enabled {
        let contract_name = NATIVE_TRANSFER_CONTRACT_NAME.to_string();
        let abi_str = NATIVE_TRANSFER_ABI;
        let abi_items: Vec<ABIItem> =
            serde_json::from_str(abi_str).expect("JSON was not well-formatted");
        let event_names = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
        let table_prefix =
            format!("{}_{}", camel_to_snake(&indexer.name), camel_to_snake(&contract_name));
        let networks = indexer.clone().native_transfers.networks.unwrap();
        let networks: Vec<&str> = networks.iter().map(|d| d.network.as_str()).collect();

        sql.push_str(&generate_event_table_sql(&event_names, &table_prefix));
        sql.push_str(&generate_internal_event_table_sql(&event_names, &table_prefix, networks));
    }

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal_{indexer_name}_last_known_relationship_dropping_sql (
            key INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        );
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal_{indexer_name}_last_known_indexes_dropping_sql (
            key INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        );
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    sql.push_str(&format!(
        r#"
        CREATE TABLE IF NOT EXISTS rindexer_internal_{indexer_name}_last_run_migrations_sql (
            version INTEGER PRIMARY KEY,
            migration_applied INTEGER NOT NULL
        );
    "#,
        indexer_name = camel_to_snake(&indexer.name)
    ));

    Ok(Code::new(sql))
}

pub fn drop_tables_for_indexer_sql(project_path: &Path, indexer: &Indexer) -> Code {
    let mut sql = format!(
        "DROP TABLE IF EXISTS rindexer_internal_{}_last_known_indexes_dropping_sql;",
        camel_to_snake(&indexer.name)
    );
    sql.push_str(&format!(
        "DROP TABLE IF EXISTS rindexer_internal_{}_last_known_relationship_dropping_sql;",
        camel_to_snake(&indexer.name)
    ));
    sql.push_str("DROP TABLE IF EXISTS rindexer_internal_latest_block;");

    for contract in &indexer.contracts {
        let contract_name = contract.before_modify_name_if_filter_readonly();
        let table_prefix =
            format!("{}_{}", camel_to_snake(&indexer.name), camel_to_snake(&contract_name));

        let abi_items = ABIItem::read_abi_items(project_path, contract);
        if let Ok(abi_items) = abi_items {
            for abi_item in abi_items.iter() {
                let table_name = format!("{}_{}", table_prefix, camel_to_snake(&abi_item.name));
                sql.push_str(&format!("DROP TABLE IF EXISTS {table_name};"));

                let internal_table_name = format!(
                    "rindexer_internal_{}_{}",
                    table_prefix,
                    camel_to_snake(&abi_item.name)
                );
                sql.push_str(&format!("DROP TABLE IF EXISTS {internal_table_name};"));
            }
        } else {
            error!(
                "Could not read ABI items for contract moving on clearing the other data up: {}",
                contract.name
            );
        }

        // Drop factory indexing tables
        for factory in contract.details.iter().flat_map(|d| d.factory.as_ref()) {
            let factory_table_name = format!(
                "rindexer_internal_{}_{}_{}_{}",
                camel_to_snake(&indexer.name),
                camel_to_snake(&factory.name),
                camel_to_snake(&factory.event_name),
                factory
                    .input_names()
                    .iter()
                    .map(|v| camel_to_snake(v))
                    .collect::<Vec<String>>()
                    .join("_")
            );
            sql.push_str(&format!("DROP TABLE IF EXISTS {factory_table_name};"));
        }
    }

    Code::new(sql)
}

// Note: generate_event_table_full_name is defined in database/generate.rs and used by no_code.rs
