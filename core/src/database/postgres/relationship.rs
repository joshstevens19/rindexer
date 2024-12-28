use std::path::Path;

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::{
    abi::{get_abi_item_with_db_map, ABIInput, ABIItem, GetAbiItemWithDbMapError, ReadAbiError},
    database::postgres::client::{PostgresClient, PostgresConnectionError, PostgresError},
    helpers::camel_to_snake,
    manifest::{contract::Contract, storage::ForeignKeys},
    types::code::Code,
};

#[derive(thiserror::Error, Debug)]
pub enum CreateRelationshipError {
    #[error("{0}")]
    PostgresConnectionError(#[from] PostgresConnectionError),

    #[error("Contract missing: {0}")]
    ContractMissing(String),

    #[error("{0}")]
    ReadAbiError(#[from] ReadAbiError),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("{0}")]
    GetAbiParameterError(#[from] GetAbiItemWithDbMapError),

    #[error("Dropping relationship failed: {0}")]
    DropRelationshipError(#[from] PostgresError),

    #[error("Could not save relationships to postgres: {0}")]
    SaveRelationshipsError(PostgresError),

    #[error("Could not serialize foreign keys: {0}")]
    CouldNotParseRelationshipToJson(#[from] serde_json::Error),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LinkTo {
    pub contract_name: String,

    pub event: String,

    pub abi_input: ABIInput,

    pub db_table_name: String,

    pub db_table_column: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Relationship {
    pub contract_name: String,

    pub event: String,

    pub abi_input: ABIInput,

    pub db_table_name: String,

    pub db_table_column: String,

    pub linked_to: LinkTo,
}

#[derive(thiserror::Error, Debug)]
pub enum ApplyAllRelationships {
    #[error("{0}")]
    PostgresConnectionError(#[from] PostgresConnectionError),

    #[error("Could not apply relationship - {0}")]
    ApplyRelationshipError(#[from] PostgresError),
}

impl Relationship {
    pub fn has_cross_contract_dependency(relationships: &[Relationship]) -> bool {
        for relationship in relationships {
            if relationship.linked_to.contract_name != relationship.contract_name {
                return true;
            }
        }
        false
    }

    fn apply_foreign_key_construct_sql(&self) -> Code {
        Code::new(format!(
            r#"
                ALTER TABLE {db_table_name}
                ADD CONSTRAINT {foreign_key_construct_name}
                FOREIGN KEY ({db_table_column}) REFERENCES {linked_db_table_name}({linked_db_table_column});
            "#,
            foreign_key_construct_name = self.foreign_key_construct_name(),
            db_table_name = self.db_table_name,
            db_table_column = self.db_table_column,
            linked_db_table_name = self.linked_to.db_table_name,
            linked_db_table_column = self.linked_to.db_table_column
        ))
    }

    fn drop_foreign_key_construct_sql(&self) -> Code {
        Code::new(format!(
            r#"
                ALTER TABLE {db_table_name}
                DROP CONSTRAINT IF EXISTS {foreign_key_construct_name};
            "#,
            foreign_key_construct_name = self.foreign_key_construct_name(),
            db_table_name = self.db_table_name,
        ))
    }

    fn foreign_key_construct_name(&self) -> String {
        format!(
            "fk_{linked_db_table_name}_{linked_db_table_column}",
            linked_db_table_name =
                self.linked_to.db_table_name.split('.').last().unwrap_or_else(|| panic!(
                    "Failed to split and then get schema for table: {}",
                    self.linked_to.db_table_column
                )),
            linked_db_table_column = self.linked_to.db_table_column
        )
    }

    // IF NOT EXISTS does not work on unique constraints, so we only want to
    // apply if it's not already applied
    fn apply_unique_construct_sql(&self) -> Code {
        Code::new(format!(
            r#"
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = '{unique_construct_name}'
                    AND conrelid = '{linked_db_table_name}'::regclass
                ) THEN
                    ALTER TABLE {linked_db_table_name}
                    ADD CONSTRAINT {unique_construct_name}
                    UNIQUE ({linked_db_table_column});
                END IF;
            END $$;
        "#,
            unique_construct_name = self.unique_construct_name(),
            linked_db_table_name = self.linked_to.db_table_name,
            linked_db_table_column = self.linked_to.db_table_column
        ))
    }

    fn drop_unique_construct_sql(&self) -> Code {
        Code::new(format!(
            r#"
                ALTER TABLE {linked_db_table_name}
                DROP CONSTRAINT IF EXISTS {unique_construct_name};
            "#,
            unique_construct_name = self.unique_construct_name(),
            linked_db_table_name = self.linked_to.db_table_name,
        ))
    }

    fn unique_construct_name(&self) -> String {
        format!(
            "unique_{linked_db_table_name}_{linked_db_table_column}",
            linked_db_table_name =
                self.linked_to.db_table_name.split('.').last().unwrap_or_else(|| panic!(
                    "Failed to split and then get schema for table: {}",
                    self.linked_to.db_table_column
                )),
            linked_db_table_column = self.linked_to.db_table_column
        )
    }

    fn apply_index_sql(&self) -> Code {
        // CONCURRENTLY is used to avoid locking the table for writes
        Code::new(format!(
            r#"
                CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                ON {db_table_name} ({db_table_column});
            "#,
            index_name = self.index_name(),
            db_table_name = self.db_table_name,
            db_table_column = self.db_table_column,
        ))
    }

    fn drop_index_sql(&self) -> Code {
        Code::new(format!(
            // CONCURRENTLY is used to avoid locking the table for writes
            "DROP INDEX CONCURRENTLY IF EXISTS {}.{};",
            // get schema else drop won't work
            self.db_table_name.split('.').next().unwrap_or_else(|| panic!(
                "Failed to split and then get schema for table: {}",
                self.db_table_column
            )),
            self.index_name(),
        ))
    }

    pub async fn drop_sql(&self) -> Result<Vec<Code>, PostgresError> {
        let mut codes = vec![];
        let sql = format!(
            r#"
            {}
            {}
          "#,
            self.drop_foreign_key_construct_sql(),
            self.drop_unique_construct_sql()
        );

        codes.push(Code::new(sql));

        info!(
            "Dropped foreign key for relationship for historic resync: table - {} constraint - {}",
            self.db_table_name,
            self.foreign_key_construct_name()
        );

        info!(
            "Dropped unique constraint key for relationship for historic resync: table - {} constraint - {}",
            self.linked_to.db_table_name,
            self.unique_construct_name()
        );

        let drop_index_sql = self.drop_index_sql();

        codes.push(drop_index_sql);

        info!(
            "Dropped index for relationship for historic resync: table - {} index - {}",
            self.db_table_name,
            self.index_name()
        );

        Ok(codes)
    }

    pub fn index_name(&self) -> String {
        format!(
            "idx_{db_table_name}_{db_table_column}",
            db_table_name = self.db_table_name.split('.').last().unwrap_or_else(|| panic!(
                "Failed to split and then get schema for table: {}",
                self.db_table_column
            )),
            db_table_column = self.db_table_column,
        )
    }

    pub async fn apply(&self, client: &PostgresClient) -> Result<(), PostgresError> {
        // apply on its own as it's in a DO block
        client.execute(self.apply_unique_construct_sql().as_str(), &[]).await?;
        info!(
            "Applied unique constraint key for relationship after historic resync complete: table - {} constraint - {}",
            self.linked_to.db_table_name,
            self.unique_construct_name()
        );

        client.execute(self.apply_foreign_key_construct_sql().as_str(), &[]).await?;

        info!(
            "Applied foreign key for relationship after historic resync complete: table - {} constraint - {}",
            self.db_table_name,
            self.foreign_key_construct_name()
        );

        // CONCURRENTLY is used to avoid locking the table for writes
        client.execute(&self.apply_index_sql().to_string(), &[]).await?;

        info!(
            "Applied index for relationship after historic resync complete: table - {} index - {}",
            self.db_table_name,
            self.index_name()
        );

        Ok(())
    }

    pub async fn apply_all(relationships: &Vec<Relationship>) -> Result<(), ApplyAllRelationships> {
        if relationships.is_empty() {
            return Ok(());
        }

        let client = PostgresClient::new().await?;

        for relationship in relationships {
            relationship.apply(&client).await?;
        }

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum GetLastKnownRelationshipsDroppingSqlError {
    #[error("Could not read last known relationship: {0}")]
    CouldNotReadLastKnownRelationship(#[from] PostgresError),

    #[error("Could not serialize foreign keys: {0}")]
    CouldNotParseRelationshipToJson(#[from] serde_json::Error),
}

async fn get_last_known_relationships_dropping_sql(
    client: &PostgresClient,
    manifest_name: &str,
) -> Result<Vec<Code>, GetLastKnownRelationshipsDroppingSqlError> {
    let row_opt = client
        .query_one_or_none(
            &format!(
                r#"
                    SELECT value FROM rindexer_internal.{}_last_known_relationship_dropping_sql WHERE key = 1
                "#,
                camel_to_snake(manifest_name)
            ),
            &[],
        )
        .await?;

    if let Some(row) = row_opt {
        let value: &str = row.get(0);
        let foreign_keys: Vec<String> = serde_json::from_str(value)?;
        Ok(foreign_keys.iter().map(|foreign_key| Code::new(foreign_key.to_string())).collect())
    } else {
        Ok(Vec::new())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DropLastKnownRelationshipsError {
    #[error("Could not connect to Postgres: {0}")]
    PostgresConnection(#[from] PostgresConnectionError),

    #[error("{0}")]
    GetLastKnownRelationshipsDroppingSql(#[from] GetLastKnownRelationshipsDroppingSqlError),

    #[error("Could not execute dropping sql: {0}")]
    PostgresError(#[from] PostgresError),
}

pub async fn drop_last_known_relationships(
    manifest_name: &str,
) -> Result<(), DropLastKnownRelationshipsError> {
    let client = PostgresClient::new().await?;

    // people can edit the relationships, so we have to drop old stuff
    // we save all drops in the database, so we can drop them all at once
    // even if old stuff has been changed
    let last_known_relationships_dropping_sql =
        get_last_known_relationships_dropping_sql(&client, manifest_name).await?;
    for drop_sql in last_known_relationships_dropping_sql {
        client.batch_execute(drop_sql.as_str()).await?;
    }

    Ok(())
}

pub async fn create_relationships(
    project_path: &Path,
    manifest_name: &str,
    contracts: &[Contract],
    foreign_keys: &[ForeignKeys],
) -> Result<Vec<Relationship>, CreateRelationshipError> {
    let mut relationships = vec![];
    let mut dropping_sql: Vec<Code> = vec![];
    for foreign_key in foreign_keys {
        let contract = contracts.iter().find(|c| c.name == foreign_key.contract_name);

        match contract {
            None => {
                return Err(CreateRelationshipError::ContractMissing(format!(
                    "Contract {} not found in `contracts` make sure it is defined",
                    foreign_key.contract_name
                )));
            }
            Some(contract) => {
                let abi_items = ABIItem::read_abi_items(project_path, contract)?;

                for linked_key in &foreign_key.foreign_keys {
                    let parameter_mapping =
                        foreign_key.event_input_name.split('.').collect::<Vec<&str>>();
                    let abi_parameter = get_abi_item_with_db_map(
                        &abi_items,
                        &foreign_key.event_name,
                        &parameter_mapping,
                    )?;

                    let linked_key_contract = contracts
                        .iter()
                        .find(|c| c.name == linked_key.contract_name)
                        .ok_or_else(|| {
                            CreateRelationshipError::ContractMissing(format!(
                                "Contract {} not found in `contracts` and linked in relationships. Make sure it is defined.",
                                linked_key.contract_name
                            ))
                        })?;

                    let linked_abi_items =
                        ABIItem::read_abi_items(project_path, linked_key_contract)?;
                    let linked_parameter_mapping =
                        linked_key.event_input_name.split('.').collect::<Vec<&str>>();
                    let linked_abi_parameter = get_abi_item_with_db_map(
                        &linked_abi_items,
                        &linked_key.event_name,
                        &linked_parameter_mapping,
                    )?;

                    if abi_parameter.abi_item.type_ != linked_abi_parameter.abi_item.type_ {
                        return Err(CreateRelationshipError::TypeMismatch(format!(
                            "Type mismatch between {}.{} ({}) and {}.{} ({})",
                            &foreign_key.contract_name,
                            &foreign_key.event_input_name,
                            &abi_parameter.abi_item.type_,
                            &linked_key.contract_name,
                            &linked_key.event_input_name,
                            &linked_abi_parameter.abi_item.type_
                        )));
                    }

                    let relationship = Relationship {
                        contract_name: foreign_key.contract_name.clone(),
                        event: foreign_key.event_name.clone(),
                        db_table_column: camel_to_snake(&abi_parameter.db_column_name),
                        db_table_name: format!(
                            "{}_{}.{}",
                            camel_to_snake(manifest_name),
                            camel_to_snake(&contract.name),
                            camel_to_snake(&foreign_key.event_name)
                        ),
                        abi_input: abi_parameter.abi_item,
                        linked_to: LinkTo {
                            contract_name: linked_key.contract_name.clone(),
                            event: linked_key.event_name.clone(),
                            db_table_column: camel_to_snake(&linked_abi_parameter.db_column_name),
                            db_table_name: format!(
                                "{}_{}.{}",
                                camel_to_snake(manifest_name),
                                camel_to_snake(&linked_key_contract.name),
                                camel_to_snake(&linked_key.event_name)
                            ),
                            abi_input: linked_abi_parameter.abi_item,
                        },
                    };

                    let sql = relationship.drop_sql().await?;
                    dropping_sql.extend(sql);
                    relationships.push(relationship);
                }
            }
        }
    }

    let relationships_dropping_sql_json = serde_json::to_string(
        &dropping_sql.iter().map(|code| code.as_str()).collect::<Vec<&str>>(),
    )?;

    // save relationships in postgres
    let client = PostgresClient::new().await?;

    client
        .execute(
            &format!(r#"
                INSERT INTO rindexer_internal.{manifest_name}_last_known_relationship_dropping_sql (key, value) VALUES (1, $1)
                ON CONFLICT (key) DO UPDATE SET value = $1;
            "#,
                     manifest_name = camel_to_snake(manifest_name)
            ),
            &[&relationships_dropping_sql_json],
        )
        .await
        .map_err(CreateRelationshipError::SaveRelationshipsError)?;

    Ok(relationships)
}
