use crate::abi::{get_abi_item_with_db_map, ABIItem, GetAbiItemWithDbMapError, ReadAbiError};
use crate::database::postgres::client::{PostgresClient, PostgresConnectionError, PostgresError};
use crate::helpers::camel_to_snake;
use crate::manifest::contract::Contract;
use crate::manifest::storage::PostgresIndexes;
use crate::types::code::Code;
use futures::future::join_all;
use std::path::Path;
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Clone)]
pub struct PostgresIndexResult {
    db_table_name: String,
    db_table_columns: Vec<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum ApplyPostgresIndexesError {
    #[error("{0}")]
    PostgresConnectionError(PostgresConnectionError),

    #[error("Could not apply indexes: {0}")]
    ApplyIndexesError(PostgresError),
}

impl PostgresIndexResult {
    pub fn apply_index_sql(&self) -> Code {
        info!(
            "Applying index after historic resync complete: table - {} constraint - {}",
            self.db_table_name,
            self.index_name()
        );

        // CONCURRENTLY is used to avoid locking the table for writes
        Code::new(format!(
            r#"
                CREATE INDEX CONCURRENTLY {index_name}
                ON {db_table_name} ({db_table_columns});
            "#,
            index_name = self.index_name(),
            db_table_name = self.db_table_name,
            db_table_columns = self.db_table_columns.join(", "),
        ))
    }

    fn drop_index_sql(&self) -> Code {
        info!(
            "Dropping index for historic resync: table - {} index - {}",
            self.db_table_name,
            self.index_name()
        );

        Code::new(format!(
            // CONCURRENTLY is used to avoid locking the table for writes
            "DROP INDEX CONCURRENTLY IF EXISTS {}.{};",
            // get schema else drop won't work
            self.db_table_name.split('.').next().unwrap(),
            self.index_name(),
        ))
    }

    pub fn index_name(&self) -> String {
        format!(
            "idx_{db_table_name}_{db_table_columns}",
            db_table_name = self.db_table_name.split('.').last().unwrap(),
            db_table_columns = self.db_table_columns.join("_"),
        )
    }

    pub async fn apply_indexes(
        indexes: Vec<PostgresIndexResult>,
    ) -> Result<(), ApplyPostgresIndexesError> {
        if indexes.is_empty() {
            return Ok(());
        }

        let client = PostgresClient::new()
            .await
            .map_err(ApplyPostgresIndexesError::PostgresConnectionError)?;

        // do a loop due to deadlocks on concurrent execution
        for postgres_index in indexes {
            let sql = postgres_index.apply_index_sql();
            client
                .execute(sql.as_str(), &[])
                .await
                .map_err(ApplyPostgresIndexesError::ApplyIndexesError)?;
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastKnownIndexesDroppingSqlError {
    #[error("Could not read last known indexes: {0}")]
    CouldNotReadLastKnownIndexes(PostgresError),

    #[error("Could not serialize indexes: {0}")]
    CouldNotParseIndexesToJson(serde_json::Error),
}

async fn get_last_known_indexes_dropping_sql(
    client: &PostgresClient,
    manifest_name: &str,
) -> Result<Vec<Code>, GetLastKnownIndexesDroppingSqlError> {
    let row_opt = client
        .query_one_or_none(
            &format!(
                r#"
                    SELECT value FROM rindexer_internal.{}_last_known_indexes_dropping_sql WHERE key = 1
                "#,
                camel_to_snake(manifest_name)
            ),
            &[],
        )
        .await
        .map_err(GetLastKnownIndexesDroppingSqlError::CouldNotReadLastKnownIndexes)?;

    if let Some(row) = row_opt {
        let value: &str = row.get(0);
        let foreign_keys: Vec<String> = serde_json::from_str(value)
            .map_err(GetLastKnownIndexesDroppingSqlError::CouldNotParseIndexesToJson)?;
        Ok(foreign_keys
            .iter()
            .map(|foreign_key| Code::new(foreign_key.to_string()))
            .collect())
    } else {
        Ok(Vec::new())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DropLastKnownIndexesError {
    #[error("Could not connect to Postgres: {0}")]
    PostgresConnection(PostgresConnectionError),

    #[error("{0}")]
    GetLastKnownIndexesDroppingSql(GetLastKnownIndexesDroppingSqlError),

    #[error("Could not execute dropping sql: {0}")]
    PostgresError(PostgresError),

    #[error("Could not drop indexes: {0}")]
    CouldNotDropIndexes(PostgresError),
}

pub async fn drop_last_known_indexes(manifest_name: &str) -> Result<(), DropLastKnownIndexesError> {
    let client = Arc::new(
        PostgresClient::new()
            .await
            .map_err(DropLastKnownIndexesError::PostgresConnection)?,
    );

    // people can edit the indexes, so we have to drop old stuff
    // we save all drops in the database, so we can drop them all at once
    // even if old stuff has been changed
    let last_known_indexes_dropping_sql =
        get_last_known_indexes_dropping_sql(&client, manifest_name)
            .await
            .map_err(DropLastKnownIndexesError::GetLastKnownIndexesDroppingSql)?;

    let futures = last_known_indexes_dropping_sql.into_iter().map(|sql| {
        let client = Arc::clone(&client);
        async move {
            client
                .execute(sql.as_str(), &[])
                .await
                .map_err(DropLastKnownIndexesError::CouldNotDropIndexes)
        }
    });

    let results = join_all(futures).await;
    for result in results {
        result?;
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum PrepareIndexesError {
    #[error("{0}")]
    PostgresConnectionError(PostgresConnectionError),

    #[error("{0}")]
    GetAbiParameterError(GetAbiItemWithDbMapError),

    #[error("Contract {0} not found in `contracts` make sure it is defined")]
    ContractMissing(String),

    #[error("{0}")]
    ReadAbiError(ReadAbiError),

    #[error("Could not serialize foreign keys: {0}")]
    CouldNotParseIndexToJson(serde_json::Error),

    #[error("Could not save indexes to postgres: {0}")]
    SaveIndexesError(PostgresError),
}

pub async fn prepare_indexes(
    project_path: &Path,
    manifest_name: &str,
    postgres_indexes: &PostgresIndexes,
    contracts: &[Contract],
) -> Result<Vec<PostgresIndexResult>, PrepareIndexesError> {
    let mut index_results: Vec<PostgresIndexResult> = vec![];
    let mut dropping_sql: Vec<Code> = vec![];
    let client = Arc::new(
        PostgresClient::new()
            .await
            .map_err(PrepareIndexesError::PostgresConnectionError)?,
    );

    // global first
    if let Some(global_injected_parameters) = &postgres_indexes.global_injected_parameters {
        for contract in contracts {
            let abi_items = ABIItem::read_abi_items(project_path, contract)
                .map_err(PrepareIndexesError::ReadAbiError)?;

            for abi_item in abi_items {
                let db_table_name = format!(
                    "{}_{}.{}",
                    camel_to_snake(manifest_name),
                    camel_to_snake(&contract.name),
                    camel_to_snake(&abi_item.name)
                );

                for global_parameter_column_name in global_injected_parameters {
                    let index_result = PostgresIndexResult {
                        db_table_name: db_table_name.clone(),
                        db_table_columns: vec![global_parameter_column_name.clone()],
                    };
                    dropping_sql.push(index_result.drop_index_sql());
                    index_results.push(index_result);
                }
            }
        }
    }

    // then contracts
    if let Some(contract_events_indexes) = &postgres_indexes.contracts {
        for contract_event_indexes in contract_events_indexes.iter() {
            let contract = contracts
                .iter()
                .find(|c| c.name == contract_event_indexes.name);

            match contract {
                None => {
                    return Err(PrepareIndexesError::ContractMissing(
                        contract_event_indexes.name.clone(),
                    ));
                }
                Some(contract) => {
                    let abi_items = ABIItem::read_abi_items(project_path, contract)
                        .map_err(PrepareIndexesError::ReadAbiError)?;

                    if let Some(injected_parameters) = &contract_event_indexes.injected_parameters {
                        for abi_item in &abi_items {
                            let db_table_name = format!(
                                "{}_{}.{}",
                                camel_to_snake(manifest_name),
                                camel_to_snake(&contract.name),
                                camel_to_snake(&abi_item.name)
                            );

                            for injected_parameter in injected_parameters {
                                let index_result = PostgresIndexResult {
                                    db_table_name: db_table_name.clone(),
                                    db_table_columns: vec![injected_parameter.clone()],
                                };
                                dropping_sql.push(index_result.drop_index_sql());
                                index_results.push(index_result);
                            }
                        }
                    }

                    for event_indexes in &contract_event_indexes.events {
                        let db_table_name = format!(
                            "{}_{}.{}",
                            camel_to_snake(manifest_name),
                            camel_to_snake(&contract.name),
                            camel_to_snake(&event_indexes.name)
                        );

                        if let Some(injected_parameters) = &event_indexes.injected_parameters {
                            for injected_parameter in injected_parameters {
                                let index_result = PostgresIndexResult {
                                    db_table_name: db_table_name.clone(),
                                    db_table_columns: vec![injected_parameter.clone()],
                                };
                                dropping_sql.push(index_result.drop_index_sql());
                                index_results.push(index_result);
                            }
                        }

                        for index in &event_indexes.indexes {
                            let mut db_table_columns = vec![];
                            for parameter in &index.event_input_names {
                                let abi_parameter = get_abi_item_with_db_map(
                                    &abi_items,
                                    &event_indexes.name,
                                    &parameter.split('.').collect::<Vec<&str>>(),
                                )
                                .map_err(PrepareIndexesError::GetAbiParameterError)?;
                                db_table_columns.push(abi_parameter.db_column_name);
                            }

                            let index_result = PostgresIndexResult {
                                db_table_name: db_table_name.clone(),
                                db_table_columns,
                            };
                            dropping_sql.push(index_result.drop_index_sql());
                            index_results.push(index_result);
                        }
                    }
                }
            }
        }
    }

    let indexes_dropping_sql_json = serde_json::to_string(
        &dropping_sql
            .iter()
            .map(|code| code.as_str())
            .collect::<Vec<&str>>(),
    )
    .map_err(PrepareIndexesError::CouldNotParseIndexToJson)?;

    client
        .execute(
            &format!(r#"
                INSERT INTO rindexer_internal.{manifest_name}_last_known_indexes_dropping_sql (key, value) VALUES (1, $1)
                ON CONFLICT (key) DO UPDATE SET value = $1;
            "#,
                     manifest_name = camel_to_snake(manifest_name)
            ),
            &[&indexes_dropping_sql_json],
        )
        .await
        .map_err(PrepareIndexesError::SaveIndexesError)?;

    Ok(index_results)
}
