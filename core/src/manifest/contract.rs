use std::{borrow::Cow, collections::HashMap, collections::HashSet, fs, path::Path};

use alloy::rpc::types::Topic;
use alloy::{
    primitives::{Address, U64},
    rpc::types::ValueOrArray,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::core::{deserialize_option_u64_from_string, serialize_option_u64_as_string};
use crate::event::contract_setup::FactoryDetails;
use crate::helpers::parse_topic;
use crate::{
    event::contract_setup::{
        AddressDetails, ContractEventMapping, FilterDetails, IndexingContractSetup,
    },
    helpers::get_full_path,
    manifest::{chat::ChatConfig, stream::StreamsConfig},
    types::single_or_array::StringOrArray,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventInputIndexedFilters {
    pub event_name: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_1: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_2: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_3: Option<Vec<String>>,
}

impl From<EventInputIndexedFilters> for [Topic; 4] {
    fn from(input: EventInputIndexedFilters) -> Self {
        let mut topics: [Topic; 4] = Default::default();

        if let Some(indexed_1) = &input.indexed_1 {
            topics[1] = indexed_1.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }
        if let Some(indexed_2) = &input.indexed_2 {
            topics[2] = indexed_2.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }
        if let Some(indexed_3) = &input.indexed_3 {
            topics[3] = indexed_3.iter().map(|i| parse_topic(i)).collect::<Vec<_>>().into();
        }

        topics
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FilterDetailsYaml {
    pub event_name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FactoryDetailsYaml {
    pub name: String,

    pub address: ValueOrArray<Address>,

    pub event_name: String,

    pub input_name: ValueOrArray<String>,

    pub abi: String,
}

impl FactoryDetailsYaml {
    pub fn input_names(&self) -> Vec<String> {
        match &self.input_name {
            ValueOrArray::Value(name) => vec![name.clone()],
            ValueOrArray::Array(names) => names.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ContractDetails {
    pub network: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<ValueOrArray<Address>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<ValueOrArray<FilterDetailsYaml>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_filters: Option<Vec<EventInputIndexedFilters>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factory: Option<FactoryDetailsYaml>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub start_block: Option<U64>,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub end_block: Option<U64>,
}

impl ContractDetails {
    pub fn indexing_contract_setup(&self, project_path: &Path) -> IndexingContractSetup {
        if let Some(address) = &self.address {
            IndexingContractSetup::Address(AddressDetails {
                address: address.clone(),
                indexed_filters: self.indexed_filters.clone(),
            })
        } else if let Some(factory) = &self.factory {
            IndexingContractSetup::Factory(
                FactoryDetails::from_abi(
                    project_path,
                    factory.abi.clone(),
                    factory.name.clone(),
                    factory.address.clone(),
                    factory.event_name.clone(),
                    factory.input_name.clone(),
                    self.indexed_filters.clone(),
                )
                .unwrap_or_else(|_| panic!("Could not parse ABI from path: {}", factory.abi)),
            )
        } else if let Some(filter) = &self.filter {
            match filter {
                ValueOrArray::Value(filter) => IndexingContractSetup::Filter(FilterDetails {
                    events: ValueOrArray::Value(filter.event_name.clone()),
                    indexed_filters: self.indexed_filters.as_ref().and_then(|f| f.first().cloned()),
                }),
                ValueOrArray::Array(filters) => IndexingContractSetup::Filter(FilterDetails {
                    events: ValueOrArray::Array(
                        filters.iter().map(|f| f.event_name.clone()).collect(),
                    ),
                    indexed_filters: self.indexed_filters.as_ref().and_then(|f| f.first().cloned()),
                }),
            }
        } else {
            panic!("Contract details must have an address, factory or filter");
        }
    }

    pub fn address(&self) -> Option<&ValueOrArray<Address>> {
        if let Some(address) = &self.address {
            return Some(address);
        }
        None
    }

    pub fn new_with_address(
        network: String,
        address: ValueOrArray<Address>,
        indexed_filters: Option<Vec<EventInputIndexedFilters>>,
        start_block: Option<U64>,
        end_block: Option<U64>,
    ) -> Self {
        Self {
            network,
            address: Some(address),
            filter: None,
            indexed_filters,
            factory: None,
            start_block,
            end_block,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SimpleEventOrContractEvent {
    SimpleEvent(String),
    ContractEvent(ContractEventMapping),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyEventTreeYaml {
    pub events: Vec<SimpleEventOrContractEvent>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub then: Option<Box<DependencyEventTreeYaml>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DependencyEventTree {
    pub contract_events: Vec<ContractEventMapping>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub then: Option<Box<DependencyEventTree>>,
}

impl DependencyEventTree {
    pub fn collect_dependency_events(&self) -> Vec<ContractEventMapping> {
        let mut dependencies = Vec::new();

        dependencies.extend(self.contract_events.clone());

        if let Some(children) = &self.then {
            dependencies.extend(children.collect_dependency_events());
        }

        dependencies
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ContractEventDeserializer {
    String(String),
    Struct(ContractEvent),
}

fn deserialize_events<'de, D>(deserializer: D) -> Result<Option<Vec<ContractEvent>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let defs = Vec::<ContractEventDeserializer>::deserialize(deserializer)?;
    Ok(Some(
        defs.into_iter()
            .map(|def| match def {
                ContractEventDeserializer::String(s) => ContractEvent { name: s, timestamps: None },
                ContractEventDeserializer::Struct(ev) => ev,
            })
            .collect(),
    ))
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ContractEvent {
    /// The name of the event.
    pub name: String,
    /// Enable or disable timestamps for the event. This will override the global timestamp
    /// setting with either the true or false state if set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamps: Option<bool>,
}

// ============================================================================
// Tables (Custom Aggregation Tables)
// ============================================================================

/// Auto-injected column names for custom tables.
/// These columns are automatically added to all custom tables and managed by rindexer.
pub mod injected_columns {
    /// Unique sequence ID computed from block_number, tx_index, and log_index.
    /// Formula: block_number * 100_000_000 + tx_index * 100_000 + log_index
    /// Used for deterministic ordering and deduplication in ReplacingMergeTree.
    pub const RINDEXER_SEQUENCE_ID: &str = "rindexer_sequence_id";

    /// Auto-incrementing ID for insert-only tables (BIGSERIAL in PostgreSQL).
    /// Used as primary key for tables that only use INSERT operations.
    pub const RINDEXER_ID: &str = "rindexer_id";

    /// The block number of the event that last updated this row.
    pub const BLOCK_NUMBER: &str = "rindexer_block_number";

    /// The block timestamp of the event that last updated this row.
    pub const BLOCK_TIMESTAMP: &str = "rindexer_block_timestamp";

    /// The transaction hash of the event that last updated this row.
    pub const TX_HASH: &str = "rindexer_tx_hash";

    /// The block hash of the event that last updated this row.
    pub const BLOCK_HASH: &str = "rindexer_block_hash";

    /// The contract address that emitted the event.
    pub const CONTRACT_ADDRESS: &str = "rindexer_contract_address";
}

/// Computes a unique sequence ID from block number, transaction index, and log index.
/// This provides deterministic ordering for events within and across blocks.
///
/// Formula: block_number * 100_000_000 + tx_index * 100_000 + log_index
///
/// This allows for:
/// - Up to 100,000 transactions per block
/// - Up to 100,000 logs per transaction
pub fn compute_sequence_id(block_number: u64, tx_index: u64, log_index: u64) -> u128 {
    (block_number as u128) * 100_000_000 + (tx_index as u128) * 100_000 + log_index as u128
}

/// Defines a custom table for aggregated indexing.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Table {
    /// The table name (will be prefixed with {indexer}_{contract}_)
    pub name: String,

    /// If true, this is a global/aggregate table with a single row per network.
    /// Global tables don't require a `where` clause in operations - the primary key
    /// is just `network`, giving one row per network for aggregate counters.
    #[serde(default)]
    pub global: bool,

    /// If true, this table aggregates data across all networks.
    /// The `network` column will NOT be created, and data from all chains
    /// contributes to the same rows. Use this for cross-chain aggregation
    /// like total token balance across Ethereum, Arbitrum, Optimism, etc.
    #[serde(default)]
    pub cross_chain: bool,

    /// Column definitions for the table
    pub columns: Vec<TableColumn>,

    /// Event-to-table mappings. Optional if using cron triggers only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub events: Vec<TableEventMapping>,

    /// Cron-triggered operations. Optional if using event triggers only.
    /// Tables can have events, cron, or both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<Vec<TableCronMapping>>,

    /// Whether to include the `rindexer_block_timestamp` column.
    /// Default is false - the column will not be created.
    /// When true, the column is created as `TIMESTAMPTZ NOT NULL` and rindexer will
    /// fetch the block timestamp from the RPC if not available in the event metadata.
    /// Note: This may impact indexing performance as it requires additional RPC calls.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub timestamp: bool,
}

impl Table {
    /// Returns true if this table has at least one trigger (event or cron).
    pub fn has_triggers(&self) -> bool {
        !self.events.is_empty() || self.has_cron()
    }

    /// Returns true if this table has cron triggers.
    pub fn has_cron(&self) -> bool {
        self.cron.as_ref().is_some_and(|c| !c.is_empty())
    }

    /// Returns true if all operations in this table are Insert type.
    /// Insert-only tables use rindexer_sequence_id as primary key.
    pub fn is_insert_only(&self) -> bool {
        let ops: Vec<_> = self.all_operations().collect();
        !ops.is_empty() && ops.iter().all(|op| op.operation_type == OperationType::Insert)
    }

    /// Get all operations (from both events and cron).
    pub fn all_operations(&self) -> impl Iterator<Item = &TableOperation> {
        let event_ops = self.events.iter().flat_map(|e| e.operations.iter());
        let cron_ops =
            self.cron.iter().flat_map(|crons| crons.iter().flat_map(|c| c.operations.iter()));
        event_ops.chain(cron_ops)
    }

    /// Get all primary key column names derived from `where` clauses.
    /// Primary key = all unique column names used in `where` across all operations.
    pub fn primary_key_columns(&self) -> Vec<&str> {
        let mut pk_columns: Vec<&str> = Vec::new();
        for operation in self.all_operations() {
            for column_name in operation.where_clause.keys() {
                if !pk_columns.contains(&column_name.as_str()) {
                    pk_columns.push(column_name.as_str());
                }
            }
        }
        pk_columns
    }

    /// Check if a column is part of the primary key (used in any where clause)
    pub fn is_primary_key_column(&self, column_name: &str) -> bool {
        self.all_operations().any(|op| op.where_clause.contains_key(column_name))
    }

    /// Validate that all operations use the same where clause columns.
    /// Returns an error message if inconsistent.
    /// For global tables, where clause is optional (primary key is just network).
    /// For Insert operations, where clause is optional (uses rindexer_sequence_id).
    pub fn validate_where_columns(&self) -> Result<(), String> {
        // Global tables don't need where clauses - they have one row per network
        if self.global {
            // Verify all operations have empty where clauses for global tables
            for operation in self.all_operations() {
                if !operation.where_clause.is_empty() {
                    return Err(format!(
                        "Global table '{}' should not have 'where' clauses. \
                         Global tables have a single row per network.",
                        self.name
                    ));
                }
                // Global tables should not use insert - they're single-row aggregates
                if operation.operation_type == OperationType::Insert {
                    return Err(format!(
                        "Global table '{}' cannot use 'insert' operation type. \
                         Global tables maintain a single row per network - use 'upsert' instead. \
                         For time-series data that inserts new rows, remove 'global: true'.",
                        self.name
                    ));
                }
            }
            return Ok(());
        }

        let mut expected_columns: Option<Vec<String>> = None;
        let mut has_non_insert_operations = false;

        for operation in self.all_operations() {
            // Insert operations don't require where clauses - each row is unique via sequence_id
            if operation.operation_type == OperationType::Insert {
                // Insert operations should NOT have where clauses
                if !operation.where_clause.is_empty() {
                    return Err(format!(
                        "Insert operation in table '{}' should not have a 'where' clause. \
                         Insert always creates new rows (identified by rindexer_sequence_id).",
                        self.name
                    ));
                }
                continue;
            }

            has_non_insert_operations = true;
            let mut op_columns: Vec<String> = operation.where_clause.keys().cloned().collect();
            op_columns.sort();

            match &expected_columns {
                None => expected_columns = Some(op_columns),
                Some(expected) => {
                    if &op_columns != expected {
                        return Err(format!(
                            "Inconsistent 'where' columns in table '{}'. \
                             Expected {:?} but found {:?}. \
                             All operations must use the same where columns.",
                            self.name, expected, op_columns
                        ));
                    }
                }
            }
        }

        // Only require where columns if there are non-insert operations
        if has_non_insert_operations
            && (expected_columns.is_none() || expected_columns.as_ref().unwrap().is_empty())
        {
            return Err(format!(
                "Table '{}' has no 'where' clause in upsert/update/delete operations. \
                 At least one column must be specified in 'where' to identify rows. \
                 For single-row aggregate tables, use 'global: true' instead. \
                 For time-series data, use 'type: insert' which doesn't require 'where'.",
                self.name
            ));
        }

        Ok(())
    }

    /// Validates that `$null` values are only used on columns marked as `nullable: true`.
    ///
    /// # Returns
    /// Ok(()) if all `$null` usages are valid, Err with description otherwise.
    pub fn validate_null_values(&self) -> Result<(), String> {
        // Build a set of nullable column names for quick lookup
        let nullable_columns: std::collections::HashSet<&str> =
            self.columns.iter().filter(|c| c.nullable).map(|c| c.name.as_str()).collect();

        // Check all operations for $null values
        for operation in self.all_operations() {
            // Check where clause values
            for (column_name, value) in &operation.where_clause {
                if value == "$null" && !nullable_columns.contains(column_name.as_str()) {
                    return Err(format!(
                        "Cannot use '$null' for column '{}' in table '{}' because it is not nullable. \
                         Add 'nullable: true' to the column definition to allow NULL values.",
                        column_name, self.name
                    ));
                }
            }

            // Check set clause values
            for set_col in &operation.set {
                let effective_value = set_col.effective_value();
                if effective_value == "$null" && !nullable_columns.contains(set_col.column.as_str())
                {
                    return Err(format!(
                        "Cannot use '$null' for column '{}' in table '{}' because it is not nullable. \
                         Add 'nullable: true' to the column definition to allow NULL values.",
                        set_col.column, self.name
                    ));
                }
            }
        }

        Ok(())
    }

    /// Resolve column types from the event ABI.
    ///
    /// This method looks at all operations to find value sources for each column and
    /// infers the type from:
    /// - Event field references ($fieldname or $tuple.nested.field) -> ABI type
    /// - Transaction metadata ($block_number, $tx_hash, etc.) -> known types
    /// - Literal values -> requires explicit type (error if missing)
    ///
    /// # Arguments
    /// * `event_abi_types` - Map of event_name -> field_name -> solidity_type
    ///
    /// # Returns
    /// Ok(()) if all types could be resolved, Err with description otherwise.
    pub fn resolve_column_types(
        &mut self,
        event_abi_types: &HashMap<String, HashMap<String, String>>,
    ) -> Result<(), String> {
        // Build a map of column_name -> inferred_type for each column
        let mut inferred_types: HashMap<String, ColumnType> = HashMap::new();

        // Iterate through all events and operations to find value sources
        for event_mapping in &self.events {
            let event_types = event_abi_types.get(&event_mapping.event);

            for operation in &event_mapping.operations {
                // Check where clause columns
                for (column_name, value_ref) in &operation.where_clause {
                    if let Some(inferred) = Self::infer_type_from_value(value_ref, event_types) {
                        inferred_types.entry(column_name.clone()).or_insert(inferred);
                    }
                }

                // Check set columns
                for set_col in &operation.set {
                    let effective_value = set_col.effective_value();
                    if !effective_value.is_empty() {
                        if let Some(inferred) =
                            Self::infer_type_from_value(effective_value, event_types)
                        {
                            inferred_types.entry(set_col.column.clone()).or_insert(inferred);
                        }
                    }
                }
            }
        }

        // Now apply the inferred types to columns that don't have explicit types
        for column in &mut self.columns {
            if column.column_type.is_none() {
                if let Some(inferred) = inferred_types.get(&column.name) {
                    column.column_type = Some(inferred.clone());
                } else {
                    // Check if column has a default value - we can try to infer from that
                    if let Some(default) = &column.default {
                        // Try to infer from the default value pattern
                        if default == "0" || default.parse::<u64>().is_ok() {
                            column.column_type = Some(ColumnType::Uint256);
                        } else if default == "true" || default == "false" {
                            column.column_type = Some(ColumnType::Bool);
                        } else if default.starts_with("0x") && default.len() == 42 {
                            column.column_type = Some(ColumnType::Address);
                        } else {
                            column.column_type = Some(ColumnType::String);
                        }
                    } else {
                        return Err(format!(
                            "Cannot infer type for column '{}' in table '{}'. \
                             Please specify the type explicitly.",
                            column.name, self.name
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Infer the column type from a value reference.
    fn infer_type_from_value(
        value_ref: &str,
        event_types: Option<&HashMap<String, String>>,
    ) -> Option<ColumnType> {
        if let Some(field_name) = value_ref.strip_prefix('$') {
            // Check for tx metadata fields first
            if let Some(column_type) = ColumnType::from_tx_metadata_field(field_name) {
                return Some(column_type);
            }

            // Check event ABI types
            if let Some(event_types) = event_types {
                // Handle nested fields like $data.amount - use the root field's type
                // (for tuples, we'd need the full ABI to resolve nested types)
                let root_field = field_name.split('.').next().unwrap_or(field_name);
                if let Some(solidity_type) = event_types.get(root_field) {
                    return ColumnType::from_solidity_type(solidity_type);
                }
            }
        }

        // Literal values - can't infer without explicit type
        None
    }
}

/// A single column in a table.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableColumn {
    /// Column name
    pub name: String,

    /// Data type. Optional - if not specified, will be inferred from the event ABI
    /// when the column value references an event field ($fieldname) or tx metadata.
    /// Required for literal values.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub column_type: Option<ColumnType>,

    /// Whether this column allows NULL values.
    /// Default is false (NOT NULL) for data integrity.
    /// Set to true to allow NULL values.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nullable: bool,

    /// Default value for new rows (as a string)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

impl TableColumn {
    /// Get the resolved column type.
    ///
    /// # Panics
    /// Panics if the column type has not been resolved. Call `Table::resolve_column_types`
    /// before using this method.
    pub fn resolved_type(&self) -> &ColumnType {
        self.column_type
            .as_ref()
            .expect("Column type not resolved. Call resolve_column_types() first.")
    }
}

/// Supported column types for tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    // Integer types
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Uint256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,

    // Address
    Address,

    // Bytes types
    Bytes,
    Bytes32,

    // Other types
    String,
    Bool,
    Timestamp,

    // Array type (wraps any base type)
    Array(Box<ColumnType>),
}

// Custom serde implementation to handle array syntax like "address[]"
impl<'de> serde::Deserialize<'de> for ColumnType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = std::string::String::deserialize(deserializer)?;
        ColumnType::from_type_string(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("Unknown column type: {}", s)))
    }
}

impl serde::Serialize for ColumnType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_type_string())
    }
}

impl ColumnType {
    /// Convert to PostgreSQL type
    pub fn to_postgres_type(&self) -> String {
        match self {
            ColumnType::Uint8 | ColumnType::Int8 => "SMALLINT".to_string(),
            ColumnType::Uint16 | ColumnType::Int16 => "SMALLINT".to_string(),
            ColumnType::Uint32 | ColumnType::Int32 => "INTEGER".to_string(),
            ColumnType::Uint64 | ColumnType::Int64 => "BIGINT".to_string(),
            ColumnType::Uint128 | ColumnType::Int128 | ColumnType::Uint256 | ColumnType::Int256 => {
                "NUMERIC".to_string()
            }
            ColumnType::Address => "CHAR(42)".to_string(),
            ColumnType::Bytes | ColumnType::Bytes32 => "BYTEA".to_string(),
            ColumnType::String => "TEXT".to_string(),
            ColumnType::Bool => "BOOLEAN".to_string(),
            ColumnType::Timestamp => "TIMESTAMPTZ".to_string(),
            ColumnType::Array(inner) => {
                // Use TEXT[] for address arrays (CHAR(42)[] doesn't work well with parsers)
                if **inner == ColumnType::Address {
                    "TEXT[]".to_string()
                } else {
                    format!("{}[]", inner.to_postgres_type())
                }
            }
        }
    }

    /// Convert to ClickHouse type
    pub fn to_clickhouse_type(&self) -> String {
        match self {
            ColumnType::Uint8 => "UInt8".to_string(),
            ColumnType::Uint16 => "UInt16".to_string(),
            ColumnType::Uint32 => "UInt32".to_string(),
            ColumnType::Uint64 => "UInt64".to_string(),
            ColumnType::Uint128 | ColumnType::Uint256 => "String".to_string(),
            ColumnType::Int8 => "Int8".to_string(),
            ColumnType::Int16 => "Int16".to_string(),
            ColumnType::Int32 => "Int32".to_string(),
            ColumnType::Int64 => "Int64".to_string(),
            ColumnType::Int128 | ColumnType::Int256 => "String".to_string(),
            ColumnType::Address => "String".to_string(),
            ColumnType::Bytes | ColumnType::Bytes32 => "String".to_string(),
            ColumnType::String => "String".to_string(),
            ColumnType::Bool => "Bool".to_string(),
            ColumnType::Timestamp => "DateTime('UTC')".to_string(),
            ColumnType::Array(inner) => format!("Array({})", inner.to_clickhouse_type()),
        }
    }

    /// Parse from a type string (used for YAML deserialization)
    pub fn from_type_string(s: &str) -> Option<Self> {
        let is_array = s.ends_with("[]");
        let base_type = s.trim_end_matches("[]");

        let base = match base_type {
            "uint8" => ColumnType::Uint8,
            "uint16" => ColumnType::Uint16,
            "uint32" => ColumnType::Uint32,
            "uint64" => ColumnType::Uint64,
            "uint128" => ColumnType::Uint128,
            "uint256" => ColumnType::Uint256,
            "int8" => ColumnType::Int8,
            "int16" => ColumnType::Int16,
            "int32" => ColumnType::Int32,
            "int64" => ColumnType::Int64,
            "int128" => ColumnType::Int128,
            "int256" => ColumnType::Int256,
            "address" => ColumnType::Address,
            "bytes" => ColumnType::Bytes,
            "bytes32" => ColumnType::Bytes32,
            "string" => ColumnType::String,
            "bool" => ColumnType::Bool,
            "timestamp" => ColumnType::Timestamp,
            _ => return None,
        };

        if is_array {
            Some(ColumnType::Array(Box::new(base)))
        } else {
            Some(base)
        }
    }

    /// Convert to type string (used for YAML serialization)
    pub fn to_type_string(&self) -> String {
        match self {
            ColumnType::Uint8 => "uint8".to_string(),
            ColumnType::Uint16 => "uint16".to_string(),
            ColumnType::Uint32 => "uint32".to_string(),
            ColumnType::Uint64 => "uint64".to_string(),
            ColumnType::Uint128 => "uint128".to_string(),
            ColumnType::Uint256 => "uint256".to_string(),
            ColumnType::Int8 => "int8".to_string(),
            ColumnType::Int16 => "int16".to_string(),
            ColumnType::Int32 => "int32".to_string(),
            ColumnType::Int64 => "int64".to_string(),
            ColumnType::Int128 => "int128".to_string(),
            ColumnType::Int256 => "int256".to_string(),
            ColumnType::Address => "address".to_string(),
            ColumnType::Bytes => "bytes".to_string(),
            ColumnType::Bytes32 => "bytes32".to_string(),
            ColumnType::String => "string".to_string(),
            ColumnType::Bool => "bool".to_string(),
            ColumnType::Timestamp => "timestamp".to_string(),
            ColumnType::Array(inner) => format!("{}[]", inner.to_type_string()),
        }
    }

    /// Infer column type from a Solidity ABI type string (e.g., "uint256", "address", "address[]").
    pub fn from_solidity_type(solidity_type: &str) -> Option<Self> {
        let is_array = solidity_type.ends_with("[]");
        let base_type = solidity_type.trim_end_matches("[]");

        let base = match base_type {
            "address" => ColumnType::Address,
            "bool" => ColumnType::Bool,
            "string" => ColumnType::String,
            "bytes" => ColumnType::Bytes,
            "bytes32" => ColumnType::Bytes32,
            t if t.starts_with("bytes") => ColumnType::Bytes,
            "uint8" => ColumnType::Uint8,
            "uint16" => ColumnType::Uint16,
            "uint24" | "uint32" => ColumnType::Uint32,
            "uint40" | "uint48" | "uint56" | "uint64" => ColumnType::Uint64,
            "uint72" | "uint80" | "uint88" | "uint96" | "uint104" | "uint112" | "uint120"
            | "uint128" => ColumnType::Uint128,
            t if t.starts_with("uint") => ColumnType::Uint256,
            "int8" => ColumnType::Int8,
            "int16" => ColumnType::Int16,
            "int24" | "int32" => ColumnType::Int32,
            "int40" | "int48" | "int56" | "int64" => ColumnType::Int64,
            "int72" | "int80" | "int88" | "int96" | "int104" | "int112" | "int120" | "int128" => {
                ColumnType::Int128
            }
            t if t.starts_with("int") => ColumnType::Int256,
            _ => return None,
        };

        if is_array {
            Some(ColumnType::Array(Box::new(base)))
        } else {
            Some(base)
        }
    }

    /// Get the column type for transaction metadata fields.
    /// All metadata fields are prefixed with rindexer_ to avoid conflicts with event fields.
    pub fn from_tx_metadata_field(field_name: &str) -> Option<Self> {
        Some(match field_name {
            "rindexer_block_number" => ColumnType::Uint64,
            "rindexer_block_timestamp" => ColumnType::Timestamp,
            // tx_hash and block_hash are stored as hex strings for readability
            "rindexer_tx_hash" => ColumnType::String,
            "rindexer_block_hash" => ColumnType::String,
            "rindexer_contract_address" => ColumnType::Address,
            "rindexer_log_index" => ColumnType::Uint256,
            "rindexer_tx_index" => ColumnType::Uint64,
            _ => return None,
        })
    }
}

/// Specifies an array to iterate over and the alias to use for each element.
/// Format in YAML: "$arrayField as alias" (e.g., "$ids as id")
#[derive(Debug, Clone)]
pub struct IterateBinding {
    /// The event field containing the array (e.g., "ids" from "$ids")
    pub array_field: String,
    /// The alias to use for each element (e.g., "id")
    pub alias: String,
}

impl IterateBinding {
    /// Parse a binding from string format: "$field as alias"
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        let parts: Vec<&str> = s.split(" as ").collect();
        if parts.len() != 2 {
            return None;
        }
        let array_field = parts[0].trim();
        let alias = parts[1].trim();

        // Array field must start with $
        if !array_field.starts_with('$') {
            return None;
        }

        Some(Self {
            array_field: array_field[1..].to_string(), // Remove the $ prefix
            alias: alias.to_string(),
        })
    }
}

impl Serialize for IterateBinding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("${} as {}", self.array_field, self.alias))
    }
}

impl<'de> Deserialize<'de> for IterateBinding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "Invalid iterate binding '{}'. Expected format: '$field as alias'",
                s
            ))
        })
    }
}

/// Maps an event to operations on a table.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableEventMapping {
    /// The event name (must exist in contract ABI)
    pub event: String,

    /// Optional array iteration - execute operations once per array element.
    /// Specify one or more parallel arrays to iterate over.
    /// Format: ["$arrayField as alias", "$anotherArray as anotherAlias"]
    ///
    /// Example for ERC1155 TransferBatch:
    /// ```yaml
    /// iterate:
    ///   - "$ids as token_id"
    ///   - "$values as amount"
    /// ```
    /// This will execute operations for each (token_id, amount) pair.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub iterate: Vec<IterateBinding>,

    /// Operations to perform when this event is received
    pub operations: Vec<TableOperation>,
}

/// Cron-triggered operations for a table.
/// Allows operations to run on a time-based schedule instead of (or in addition to) events.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableCronMapping {
    /// Simple interval like "5m", "1h", "30s", "1d".
    /// Either `interval` or `schedule` must be specified, but not both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<String>,

    /// Cron expression like "*/5 * * * *".
    /// Either `interval` or `schedule` must be specified, but not both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schedule: Option<String>,

    /// Optional network filter - if omitted, runs on all networks defined in contract details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<String>,

    /// Starting block for historical sync.
    /// If specified, the cron will replay operations from this block forward
    /// before running live. Similar to event indexing start_block.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub start_block: Option<U64>,

    /// Ending block for historical sync.
    /// If specified, the cron will stop after reaching this block and will NOT
    /// continue with live cron execution. Use "latest" or a block number.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub end_block: Option<U64>,

    /// Block interval for historical sync.
    /// How many blocks between each cron execution during historical sync.
    /// If not specified, the cron will run at every block.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_u64_from_string",
        serialize_with = "serialize_option_u64_as_string"
    )]
    pub block_interval: Option<U64>,

    /// Operations to perform on each cron tick.
    /// Note: Event fields ($from, $to, etc.) are NOT available in cron operations.
    /// Available: $call(...), $contract, $rindexer_block_number, $rindexer_timestamp, literals.
    pub operations: Vec<TableOperation>,
}

/// Parse interval string like "5m", "1h", "30s", "1d" into Duration.
///
/// Supported units:
/// - `s` - seconds (e.g., "30s" = 30 seconds)
/// - `m` - minutes (e.g., "5m" = 5 minutes)
/// - `h` - hours (e.g., "1h" = 1 hour)
/// - `d` - days (e.g., "1d" = 1 day)
pub fn parse_interval(interval: &str) -> Result<std::time::Duration, String> {
    let interval = interval.trim();
    if interval.is_empty() {
        return Err("Interval cannot be empty".to_string());
    }

    // Find where the numeric part ends
    let split_idx = interval.chars().position(|c| !c.is_ascii_digit()).ok_or_else(|| {
        format!("Invalid interval '{}': no unit specified. Use s/m/h/d", interval)
    })?;

    let (num_str, unit) = interval.split_at(split_idx);
    let num: u64 = num_str.parse().map_err(|_| format!("Invalid interval number: {}", num_str))?;

    if num == 0 {
        return Err("Interval must be greater than 0".to_string());
    }

    match unit {
        "s" => Ok(std::time::Duration::from_secs(num)),
        "m" => Ok(std::time::Duration::from_secs(num * 60)),
        "h" => Ok(std::time::Duration::from_secs(num * 60 * 60)),
        "d" => Ok(std::time::Duration::from_secs(num * 60 * 60 * 24)),
        _ => Err(format!(
            "Invalid interval unit: '{}'. Use s (seconds), m (minutes), h (hours), or d (days)",
            unit
        )),
    }
}

/// A single operation to perform when an event is received.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TableOperation {
    /// The type of operation
    #[serde(rename = "type")]
    pub operation_type: OperationType,

    /// Columns to match for finding/creating the row (column_name -> event_field or literal).
    /// Required for regular tables, must be empty for global tables.
    #[serde(rename = "where", default, skip_serializing_if = "HashMap::is_empty")]
    pub where_clause: HashMap<String, String>,

    /// Conditional expression - only execute this operation if the condition evaluates to true.
    /// Use `if` for clearer intent (recommended) or `filter` (legacy alias).
    ///
    /// Supports complex expressions with && (and), || (or), comparisons (>, <, >=, <=, ==, !=),
    /// arithmetic (+, -, *, /), nested field access (data.amount), and more.
    ///
    /// Example: "from != to && value > 0"
    /// Example: "value > balance * 2"
    #[serde(rename = "if", skip_serializing_if = "Option::is_none")]
    pub if_condition: Option<String>,

    /// Legacy alias for `if`. Use `if` for new configurations.
    /// If both `if` and `filter` are specified, `if` takes precedence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    /// Columns to set/update (optional for delete operations)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set: Vec<SetColumn>,
}

impl TableOperation {
    /// Returns the condition expression (from `if` or `filter` field).
    /// The `if` field takes precedence if both are specified.
    pub fn condition(&self) -> Option<&str> {
        self.if_condition.as_deref().or(self.filter.as_deref())
    }
}

/// The type of database operation.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OperationType {
    /// Insert or update row (creates if not exists)
    Upsert,
    /// Insert a new row (for time-series/history data)
    Insert,
    /// Update existing row only
    Update,
    /// Delete row
    Delete,
}

/// A column to set/update in an operation.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SetColumn {
    /// The column name to update
    pub column: String,

    /// The action to perform
    pub action: SetAction,

    /// The value (event field reference with $ prefix, or literal).
    /// Optional for increment/decrement actions (defaults to 1).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

impl SetColumn {
    /// Get the effective value for this set column.
    /// Returns "1" for increment/decrement if no value specified.
    pub fn effective_value(&self) -> &str {
        match &self.value {
            Some(v) => v.as_str(),
            None if self.action.is_counter_action() => "1",
            None => "",
        }
    }

    /// Check if value is an event field reference (starts with $)
    pub fn is_event_field_reference(&self) -> bool {
        self.effective_value().starts_with('$')
    }

    /// Get the event field name (without $ prefix)
    pub fn event_field_name(&self) -> Option<&str> {
        let value = self.effective_value();
        value.strip_prefix('$')
    }
}

/// Action to perform when setting a column value.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SetAction {
    /// Replace the existing value
    Set,
    /// Add to the existing value
    Add,
    /// Subtract from the existing value
    Subtract,
    /// Keep the maximum (higher) value
    Max,
    /// Keep the minimum (lower) value
    Min,
    /// Increment by 1 (shorthand for add with value "1")
    Increment,
    /// Decrement by 1 (shorthand for subtract with value "1")
    Decrement,
}

impl SetAction {
    /// Returns true if this action doesn't require a value (increment/decrement)
    pub fn is_counter_action(&self) -> bool {
        matches!(self, SetAction::Increment | SetAction::Decrement)
    }
}

// ============================================================================
// Contract Struct
// ============================================================================

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Contract {
    pub name: String,

    pub details: Vec<ContractDetails>,

    pub abi: StringOrArray,

    #[serde(
        default,
        deserialize_with = "deserialize_events",
        skip_serializing_if = "Option::is_none"
    )]
    pub include_events: Option<Vec<ContractEvent>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_event_in_order: Option<Vec<String>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dependency_events: Option<DependencyEventTreeYaml>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reorg_safe_distance: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generate_csv: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub streams: Option<StreamsConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat: Option<ChatConfig>,

    /// Custom indexing tables for aggregations (upsert, update, delete operations)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tables: Option<Vec<Table>>,
}

#[derive(thiserror::Error, Debug)]
pub enum ParseAbiError {
    #[error("Could not read ABI string: {0}")]
    CouldNotReadAbiString(String),

    #[error("Could not get full path: {0}")]
    CouldNotGetFullPath(#[from] std::io::Error),

    #[error("Invalid ABI format: {0}")]
    InvalidAbiFormat(String),

    #[error("Could not merge ABI: {0}")]
    CouldNotMergeAbis(#[from] serde_json::Error),
}

impl Contract {
    pub fn override_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn parse_abi(&self, project_path: &Path) -> Result<String, ParseAbiError> {
        match &self.abi {
            StringOrArray::Single(abi_path) => {
                let full_path = get_full_path(project_path, abi_path)?;
                let abi_str = fs::read_to_string(full_path)?;
                Ok(abi_str)
            }
            StringOrArray::Multiple(abis) => {
                let mut unique_entries = HashSet::new();
                let mut merged_abi_value = Vec::new();

                for abi_path in abis {
                    let full_path = get_full_path(project_path, abi_path)?;
                    let abi_str = fs::read_to_string(full_path)?;
                    let abi_value: Value = serde_json::from_str(&abi_str)?;

                    if let Value::Array(abi_arr) = abi_value {
                        for entry in abi_arr {
                            let entry_str = serde_json::to_string(&entry)?;
                            if unique_entries.insert(entry_str) {
                                merged_abi_value.push(entry);
                            }
                        }
                    } else {
                        return Err(ParseAbiError::InvalidAbiFormat(format!(
                            "Expected an array but got a single value: {abi_value}"
                        )));
                    }
                }

                let merged_abi_str = serde_json::to_string(&json!(merged_abi_value))?;
                Ok(merged_abi_str)
            }
        }
    }

    pub fn convert_dependency_event_tree_yaml(
        &self,
        yaml: DependencyEventTreeYaml,
    ) -> DependencyEventTree {
        DependencyEventTree {
            contract_events: yaml
                .events
                .into_iter()
                .map(|event| match event {
                    SimpleEventOrContractEvent::ContractEvent(contract_event) => contract_event,
                    SimpleEventOrContractEvent::SimpleEvent(event_name) => {
                        ContractEventMapping { contract_name: self.name.clone(), event_name }
                    }
                })
                .collect(),
            then: yaml
                .then
                .map(|then_event| Box::new(self.convert_dependency_event_tree_yaml(*then_event))),
        }
    }

    pub fn is_filter(&self) -> bool {
        let filter_count = self.details.iter().filter(|details| details.filter.is_some()).count();

        if filter_count > 0 && filter_count != self.details.len() {
            // panic as this should never happen as validation has already happened
            panic!("Cannot mix and match address and filter for the same contract definition.");
        }

        filter_count > 0
    }

    fn contract_name_to_filter_name(&self) -> String {
        format!("{}Filter", self.name)
    }

    pub fn raw_name(&self) -> String {
        if self.is_filter() {
            self.name.split("Filter").collect::<Vec<&str>>()[0].to_string()
        } else {
            self.name.clone()
        }
    }

    pub fn before_modify_name_if_filter_readonly(&'_ self) -> Cow<'_, str> {
        if self.is_filter() {
            Cow::Owned(self.contract_name_to_filter_name())
        } else {
            Cow::Borrowed(&self.name)
        }
    }

    pub fn identify_and_modify_filter(&mut self) -> bool {
        if self.is_filter() {
            self.override_name(self.contract_name_to_filter_name());
            true
        } else {
            false
        }
    }

    /// Get all unique event names referenced in custom tables.
    pub fn get_table_event_names(&self) -> Vec<String> {
        self.tables
            .as_ref()
            .map(|tables| {
                tables
                    .iter()
                    .flat_map(|table| table.events.iter().map(|e| e.event.clone()))
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if any table has cron triggers.
    pub fn has_any_table_cron(&self) -> bool {
        self.tables.as_ref().is_some_and(|tables| tables.iter().any(|table| table.has_cron()))
    }

    /// Check if an event name is in include_events.
    /// Returns true if include_events is None (meaning all events are included for raw storage).
    pub fn is_event_in_include_events(&self, event_name: &str) -> bool {
        match &self.include_events {
            Some(events) => events.iter().any(|e| e.name == event_name),
            None => false, // No include_events means no raw event storage
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_yaml;

    use super::*;

    #[test]
    fn test_contract_include_events_simple() {
        let yaml = r#"
            name: ERC20
            abi: ./abis/ERC20.abi.json
            details:
              - network: ethereum
                start_block: 20090000
                filter:
                  - event_name: Transfer
                  - event_name: Approval
            include_events:
              - Transfer
              - Approval
        "#;

        let contract: Contract = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            contract.include_events,
            Some(vec![
                ContractEvent { name: "Transfer".to_string(), timestamps: None },
                ContractEvent { name: "Approval".to_string(), timestamps: None }
            ])
        );
    }

    #[test]
    fn test_contract_include_events_complex() {
        let yaml = r#"
            name: ERC20
            abi: ./abis/ERC20.abi.json
            details:
              - network: ethereum
                start_block: 20090000
                filter:
                  - event_name: Transfer
                  - event_name: Approval
            include_events:
              - name: Transfer
                timestamps: true
              - name: Approval
                timestamps: false
        "#;

        let contract: Contract = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(
            contract.include_events,
            Some(vec![
                ContractEvent { name: "Transfer".to_string(), timestamps: Some(true) },
                ContractEvent { name: "Approval".to_string(), timestamps: Some(false) }
            ])
        );
    }
}
