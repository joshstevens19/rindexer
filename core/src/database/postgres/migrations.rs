use crate::database::generate::{
    generate_indexer_contract_schema_name, GenerateTablesForIndexerSqlError,
};
use crate::helpers::camel_to_snake;
use crate::indexer::Indexer;
use crate::{ABIItem, PostgresClient};
use std::path::Path;

/// Atomically incrementing version number for migrations.
///
/// Note: if all migrations are idempotent on re-run we can avoid tracking last-known migration
/// entirely, however the cost associated with checking already run migrations is low enough that
/// we can design it to be future-proof at least until are clear it's not worth it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Migration {
    /// Add `block_timestamp` column to all tables, nullable by default.
    V1,
}

impl Migration {
    pub const ALL: &'static [Migration] = &[Migration::V1];

    pub fn as_u32(&self) -> u32 {
        match self {
            Migration::V1 => 1,
        }
    }

    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            1 => Some(Migration::V1),
            _ => None,
        }
    }

    pub fn generate_table_migration_sql(&self, table_name: &str) -> String {
        match self {
            Migration::V1 => {
                format!("ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS block_timestamp TIMESTAMPTZ;")
            }
        }
    }
}

/// Execute any unapplied migrations for rindexer project.
///
/// ## Migration flow
///
/// 1. Check any already applied migrations
/// 2. Get the migrations that have not yet been applied
/// 3. Generate the migration SQL and apply them
/// 4. Insert new rows reflecting that these migrations have been applied
pub async fn execute_migrations_for_indexer_sql(
    client: &PostgresClient,
    project_path: &Path,
    indexer: &Indexer,
    disable_event_tables: bool,
) -> Result<Vec<Migration>, GenerateTablesForIndexerSqlError> {
    if disable_event_tables {
        return Ok(vec![]);
    }

    let applied = client
        .query(
            &format!(
                r#"
                 SELECT version FROM rindexer_internal.{indexer_name}_last_run_migrations_sql
                 WHERE migration_applied IS TRUE
                "#,
                indexer_name = camel_to_snake(&indexer.name)
            ),
            &[],
        )
        .await?;

    let applied = applied
        .iter()
        .filter_map(|row| {
            let id: i32 = row.get(0);
            Migration::from_u32(id as u32)
        })
        .collect::<Vec<_>>();
    let unapplied =
        Migration::ALL.iter().cloned().filter(|v| !applied.contains(v)).collect::<Vec<_>>();

    let mut statements = String::new();

    for version in &unapplied {
        for contract in &indexer.contracts {
            let contract_name = contract.before_modify_name_if_filter_readonly();
            let abi_items = ABIItem::read_abi_items(project_path, contract)?;
            let events = ABIItem::extract_event_names_and_signatures_from_abi(abi_items)?;
            let schema_name = generate_indexer_contract_schema_name(&indexer.name, &contract_name);

            for event_info in events {
                let table_name = format!("{}.{}", schema_name, camel_to_snake(&event_info.name));
                statements.push_str(&version.generate_table_migration_sql(&table_name));
            }
        }
    }

    client.batch_execute(&statements).await?;

    client
        .query(
            &format!(
                r#"
                 INSERT INTO rindexer_internal.{indexer_name}_last_run_migrations_sql (version, migration_applied)
                 SELECT UNNEST($1::INT[]), TRUE
                 ON CONFLICT (version) DO NOTHING
                "#,
                indexer_name = camel_to_snake(&indexer.name),
            ),
            &[&unapplied.iter().map(|v| v.as_u32() as i32).collect::<Vec<_>>()],
        )
        .await?;

    Ok(unapplied)
}
