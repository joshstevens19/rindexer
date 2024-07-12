use crate::database::postgres::indexes::{
    drop_last_known_indexes, prepare_indexes, DropLastKnownIndexesError, PostgresIndexResult,
    PrepareIndexesError,
};
use crate::database::postgres::relationship::{
    create_relationships, drop_last_known_relationships, CreateRelationshipError,
    DropLastKnownRelationshipsError, Relationship,
};
use crate::manifest::contract::Contract;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKey {
    pub contract_name: String,

    pub event_name: String,

    pub event_input_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ForeignKeys {
    pub contract_name: String,

    pub event_name: String,

    pub event_input_name: String,

    #[serde(rename = "linked_to")]
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventIndex {
    pub event_input_names: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractEventsIndexes {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injected_parameters: Option<Vec<String>>,

    pub events: Vec<EventIndexes>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventIndexes {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub injected_parameters: Option<Vec<String>>,

    pub indexes: Vec<EventIndex>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresIndexes {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_injected_parameters: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<ContractEventsIndexes>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PostgresDetails {
    pub enabled: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationships: Option<Vec<ForeignKeys>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexes: Option<PostgresIndexes>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_create_tables: Option<bool>,
}

fn default_csv_path() -> String {
    "./generated_csv".to_string()
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CsvDetails {
    pub enabled: bool,

    #[serde(default = "default_csv_path")]
    pub path: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_create_headers: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Storage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres: Option<PostgresDetails>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv: Option<CsvDetails>,
}

#[derive(thiserror::Error, Debug)]
pub enum RelationshipsAndIndexersError {
    #[error("{0}")]
    DropLastKnownRelationshipsError(#[from] DropLastKnownRelationshipsError),

    #[error("Yaml relationship error: {0}")]
    RelationshipError(#[from] CreateRelationshipError),

    #[error("{0}")]
    DropLastKnownIndexesError(#[from] DropLastKnownIndexesError),

    #[error("Could not prepare and drop indexes: {0}")]
    FailedToPrepareAndDropIndexes(#[from] PrepareIndexesError),
}

impl Storage {
    pub fn postgres_enabled(&self) -> bool {
        match &self.postgres {
            Some(details) => details.enabled,
            None => false,
        }
    }

    pub fn postgres_disable_create_tables(&self) -> bool {
        let enabled = self.postgres_enabled();
        if !enabled {
            return true;
        }

        self.postgres.as_ref().map_or(false, |details| {
            details.disable_create_tables.unwrap_or_default()
        })
    }

    pub fn csv_enabled(&self) -> bool {
        match &self.csv {
            Some(details) => details.enabled,
            None => false,
        }
    }

    pub fn csv_disable_create_headers(&self) -> bool {
        let enabled = self.csv_enabled();
        if !enabled {
            return true;
        }

        self.csv.as_ref().map_or(false, |details| {
            details.disable_create_headers.unwrap_or_default()
        })
    }

    pub async fn create_relationships_and_indexes(
        &self,
        project_path: &Path,
        manifest_name: &str,
        contracts: &[Contract],
    ) -> Result<(Vec<Relationship>, Vec<PostgresIndexResult>), RelationshipsAndIndexersError> {
        if self.postgres_enabled() && !self.postgres_disable_create_tables() {
            if let Some(storage) = &self.postgres {
                // setup relationships
                let mut relationships: Vec<Relationship> = vec![];
                // setup postgres indexes
                let mut postgres_indexes: Vec<PostgresIndexResult> = vec![];

                info!("Temp dropping constraints relationships from the database for historic indexing for speed reasons");
                drop_last_known_relationships(manifest_name).await?;

                let mapped_relationships = &storage.relationships;
                if let Some(mapped_relationships) = mapped_relationships {
                    let relationships_result = create_relationships(
                        project_path,
                        manifest_name,
                        contracts,
                        mapped_relationships,
                    )
                    .await;
                    match relationships_result {
                        Ok(result) => {
                            relationships = result;
                        }
                        Err(e) => {
                            return Err(RelationshipsAndIndexersError::RelationshipError(e));
                        }
                    }
                }

                info!("Temp dropping indexes from the database for historic indexing for speed reasons");
                drop_last_known_indexes(manifest_name).await?;

                if let Some(indexes) = &storage.indexes {
                    let indexes_result =
                        prepare_indexes(project_path, manifest_name, indexes, contracts).await;

                    match indexes_result {
                        Ok(result) => {
                            postgres_indexes = result;
                        }
                        Err(e) => {
                            return Err(
                                RelationshipsAndIndexersError::FailedToPrepareAndDropIndexes(e),
                            );
                        }
                    }
                }

                return Ok((relationships, postgres_indexes));
            }
        }

        Ok((vec![], vec![]))
    }
}
