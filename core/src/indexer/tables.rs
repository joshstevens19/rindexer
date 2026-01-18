//! Tables support for no-code aggregation tables.
//!
//! This module provides runtime processing of table operations
//! defined in the rindexer.yaml configuration, allowing upsert, update,
//! and delete operations on custom tables without writing Rust code.
//!
//! ## Auto-Injected Columns
//!
//! Every custom table automatically gets these metadata columns:
//!
//! - `rindexer_sequence_id` (NUMERIC) - Unique ID for deterministic ordering
//!   computed as: `block_number * 100_000_000 + tx_index * 100_000 + log_index`
//! - `last_updated_block` (BIGINT) - The block number when the row was last updated
//! - `last_updated_at` (TIMESTAMPTZ) - The timestamp when the row was last updated
//! - `tx_hash` (CHAR(66)) - The transaction hash of the event that last updated this row
//! - `block_hash` (CHAR(66)) - The block hash of the event that last updated this row
//! - `contract_address` (CHAR(42)) - The contract address that emitted the event
//!
//! These columns are automatically set by rindexer and do NOT need to be defined
//! in your YAML configuration.
//!
//! ## Supported Value References
//!
//! In the YAML config, you can reference values using the `$` prefix:
//!
//! - **Event fields**: `$from`, `$to`, `$value`, etc. (any field from the event)
//! - **Nested tuple fields**: `$data.amount`, `$info.token.address` (for events with tuples/structs)
//! - **Array indexing**: `$ids[0]`, `$data.tokens[1]` (access specific array elements)
//! - **Post-array field access**: `$transfers[0].amount`, `$orders[1].maker` (array element then named field)
//! - **String templates**: `"Pool: $token0/$token1"`, `"$from-$to"` (embed fields in strings)
//! - **Transaction metadata**:
//!   - `$block_number` - The block number
//!   - `$block_timestamp` - The block timestamp (as TIMESTAMPTZ)
//!   - `$tx_hash` - The transaction hash (as hex string)
//!   - `$block_hash` - The block hash (as hex string)
//!   - `$contract_address` - The contract address that emitted the event
//!   - `$log_index` - The log index within the transaction
//!   - `$tx_index` - The transaction index within the block
//!
//! ## Filter Expressions
//!
//! The `filter` field supports powerful expressions for filtering events:
//!
//! - **Comparison operators**: `==`, `!=`, `>`, `<`, `>=`, `<=`
//! - **Logical operators**: `&&` (and), `||` (or)
//! - **Nested field access**: `data.amount`, `info.token.address`
//!
//! Examples:
//! ```yaml
//! # Simple comparison
//! filter: "to != 0x0000000000000000000000000000000000000000"
//!
//! # Multiple conditions with AND
//! filter: "value > 0 && from != 0x0000000000000000000000000000000000000000"
//!
//! # Complex expression with OR
//! filter: "value >= 1000000 || (from == 0x1234... && to != 0x5678...)"
//!
//! # Nested field access
//! filter: "data.amount > 0 && data.recipient != 0x0000..."
//! ```
//!
//! ## Global Tables
//!
//! For aggregate/counter tables that need only one row per network, use `global: true`.
//! Global tables don't require a `where` clause - the primary key is just `network`.
//!
//! ## Array Iteration
//!
//! For events with parallel arrays (like ERC1155 `TransferBatch`), use `iterate` to process
//! each array element as a separate operation:
//!
//! ```yaml
//! events:
//!   - event: TransferBatch
//!     iterate:
//!       - "$ids as token_id"       # First array to iterate
//!       - "$values as amount"      # Second array (must be same length)
//!     operations:
//!       - type: upsert
//!         where:
//!           holder: $to
//!           token_id: $token_id    # Use the aliased value
//!         set:
//!           - column: balance
//!             action: add
//!             value: $amount       # Use the aliased value
//! ```
//!
//! This creates one operation per array element, with `$token_id` and `$amount` bound to
//! the corresponding elements at each index.
//!
//! ## Example YAML
//!
//! ```yaml
//! tables:
//!   # Regular table with per-address rows
//!   - name: token_balances
//!     columns:
//!       - name: holder         # Will be primary key (used in 'where')
//!       - name: balance
//!         default: "0"
//!     events:
//!       - event: Transfer
//!         operations:
//!           - type: upsert
//!             where:
//!               holder: $to    # This makes 'holder' the primary key
//!             filter: "to != 0x0000000000000000000000000000000000000000"
//!             set:
//!               - column: balance
//!                 action: add
//!                 value: $value
//!
//!   # Global table for aggregate counters (one row per network)
//!   - name: token_supply
//!     global: true             # No 'where' clause needed
//!     columns:
//!       - name: total_supply
//!         default: "0"
//!     events:
//!       - event: Transfer
//!         operations:
//!           - type: upsert
//!             filter: "from == 0x0000..."  # Mint events
//!             set:
//!               - column: total_supply
//!                 action: add
//!                 value: $value
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use alloy::dyn_abi::DynSolValue;
use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use tracing::{debug, info};

use crate::database::batch_operations::{
    BatchOperationAction, BatchOperationColumnBehavior, BatchOperationSqlType, BatchOperationType,
    DynamicColumnDefinition,
};
use crate::database::clickhouse::batch_operations::execute_dynamic_batch_operation as execute_clickhouse_dynamic_batch_operation;
use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::generate::generate_table_full_name;
use crate::database::postgres::batch_operations::execute_dynamic_batch_operation;
use crate::database::postgres::client::PostgresClient;
use crate::database::sql_type_wrapper::EthereumSqlTypeWrapper;
use crate::event::{
    evaluate_arithmetic, filter_by_expression, parse_filter_expression, ComputedValue,
};
use crate::manifest::contract::{
    compute_sequence_id, injected_columns, ColumnType, IterateBinding, OperationType, SetAction,
    Table, TableOperation,
};
use crate::types::core::LogParam;

/// Transaction metadata available for table value references.
#[derive(Clone, Debug)]
pub struct TxMetadata {
    pub block_number: u64,
    pub block_timestamp: Option<U256>,
    pub tx_hash: B256,
    pub block_hash: B256,
    pub contract_address: Address,
    pub log_index: U256,
    pub tx_index: u64,
}

/// Runtime representation of a table with resolved table name.
#[derive(Clone, Debug)]
pub struct TableRuntime {
    pub table: Table,
    pub full_table_name: String,
    pub indexer_name: String,
    pub contract_name: String,
}

impl TableRuntime {
    pub fn new(table: Table, indexer_name: &str, contract_name: &str) -> Self {
        let full_table_name =
            generate_table_full_name(indexer_name, contract_name, &table.name);
        Self {
            table,
            full_table_name,
            indexer_name: indexer_name.to_string(),
            contract_name: contract_name.to_string(),
        }
    }
}

/// Data for a single table row to be processed.
#[derive(Debug)]
pub struct TableRowData {
    /// Column values keyed by column name
    pub columns: HashMap<String, EthereumSqlTypeWrapper>,
    /// Network for this row
    pub network: String,
}

/// Checks if a value string contains arithmetic operators indicating it's a computed expression.
/// Computed expressions like "$value * 2", "$amount + $fee", "$ratio / 100" will return true.
fn is_arithmetic_expression(value: &str) -> bool {
    // Must contain at least one arithmetic operator
    // Check for operators that are not part of comparison (==, !=, >=, <=)
    let has_operator = value
        .chars()
        .enumerate()
        .any(|(i, c)| {
            if c == '*' || c == '/' {
                true
            } else if c == '+' || c == '-' {
                // Check it's not a unary operator at the start
                i > 0
            } else {
                false
            }
        });

    has_operator && value.contains('$')
}

/// Checks if a value string is a string template with embedded field references.
/// String templates like "Pool: $token0/$token1" or "$from-$to" will return true.
/// A single field reference like "$value" is NOT a template - it's a direct field access.
fn is_string_template(value: &str) -> bool {
    if !value.contains('$') {
        return false;
    }

    // Pure field reference starts with $ and has no other content before it
    // e.g., "$from" or "$data.amount" or "$ids[0]"
    if value.starts_with('$') {
        // Check if there's any content after the field reference
        // A field reference is: $fieldname or $field.nested or $field[0]
        let after_dollar = &value[1..];

        // Simple heuristic: if it's a pure field reference, it should only contain
        // alphanumeric, dots, underscores, and brackets
        // String templates have extra characters like spaces, colons, slashes, etc.
        let is_pure_field = after_dollar.chars().all(|c| {
            c.is_alphanumeric() || c == '.' || c == '_' || c == '[' || c == ']'
        });

        if is_pure_field {
            return false;
        }
    }

    // Has $ somewhere but isn't a pure field reference - it's a template
    true
}

/// Expands a string template by replacing all `$field` references with their values.
/// Returns None if any field reference cannot be resolved.
fn expand_string_template(
    template: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
) -> Option<String> {
    let mut result = String::with_capacity(template.len() * 2);
    let mut chars = template.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            // Extract the field name/path
            let mut field_name = String::new();

            while let Some(&next_c) = chars.peek() {
                if next_c.is_alphanumeric() || next_c == '.' || next_c == '_' || next_c == '[' || next_c == ']' {
                    field_name.push(chars.next().unwrap());
                } else {
                    break;
                }
            }

            if field_name.is_empty() {
                // Lone $ sign, keep it as is
                result.push('$');
                continue;
            }

            // Resolve the field value
            let value_str = resolve_field_to_string(&field_name, log_params, tx_metadata)?;
            result.push_str(&value_str);
        } else {
            result.push(c);
        }
    }

    Some(result)
}

/// Resolves a field reference to its string representation.
fn resolve_field_to_string(
    field_name: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
) -> Option<String> {
    // Check for built-in transaction metadata fields first
    match field_name {
        "block_number" => return Some(tx_metadata.block_number.to_string()),
        "block_timestamp" => {
            return tx_metadata.block_timestamp.map(|ts| ts.to_string());
        }
        "tx_hash" => return Some(format!("{:?}", tx_metadata.tx_hash)),
        "block_hash" => return Some(format!("{:?}", tx_metadata.block_hash)),
        "contract_address" => return Some(format!("{:?}", tx_metadata.contract_address)),
        "log_index" => return Some(tx_metadata.log_index.to_string()),
        "tx_index" => return Some(tx_metadata.tx_index.to_string()),
        _ => {}
    }

    // Resolve from log params
    let value = resolve_field_path(field_name, log_params)?;
    Some(dyn_sol_value_to_string(&value))
}

/// Converts a DynSolValue to a string representation suitable for concatenation.
fn dyn_sol_value_to_string(value: &DynSolValue) -> String {
    match value {
        DynSolValue::Address(addr) => format!("{:?}", addr),
        DynSolValue::Uint(val, _) => val.to_string(),
        DynSolValue::Int(val, _) => val.to_string(),
        DynSolValue::Bool(b) => b.to_string(),
        DynSolValue::String(s) => s.clone(),
        DynSolValue::Bytes(b) => format!("0x{}", hex::encode(b)),
        DynSolValue::FixedBytes(b, _) => format!("0x{}", hex::encode(b)),
        _ => format!("{:?}", value),
    }
}

/// Resolves a field path from event log parameters, supporting:
/// - Simple field access: `from` -> find param named "from"
/// - Nested tuple access: `data.amount` -> uses LogParam.get_param_value for named access
/// - Array indexing: `ids[0]` -> find param "ids", access element 0
/// - Combined paths: `data.ids[0]` -> get "data.ids" via nested access, then index
/// - Post-array field access: `transfers[0].amount` -> array element then named field
///
/// Strategy:
/// 1. Split the path at array indices: `transfers[0].amount` -> ["transfers", "[0]", "amount"]
/// 2. Track both the value AND the ABI components as we traverse
/// 3. For array access, components describe each element's structure
/// 4. For named field access after arrays, use components to find tuple position
fn resolve_field_path(field_path: &str, log_params: &[LogParam]) -> Option<DynSolValue> {
    use alloy::json_abi::Param;

    // Parse the path into segments, separating array indices
    let segments = parse_path_segments(field_path);
    if segments.is_empty() {
        return None;
    }

    // First segment is the field path (may include dots for nested access)
    let first_segment = &segments[0];
    if first_segment.starts_with('[') {
        // Can't start with an array index
        return None;
    }

    // Track both the value and the ABI components for named field resolution
    let (mut current_value, mut current_components): (DynSolValue, Vec<Param>) =
        if first_segment.contains('.') {
            // Nested path like "data.tokens" - need to traverse and get final components
            let (root, rest) = first_segment.split_once('.')?;
            let param = log_params.iter().find(|p| p.name == root)?;

            // Traverse the nested path to get value and final components
            let mut value = param.value.clone();
            let mut components = param.components.clone();

            for part in rest.split('.') {
                let (idx, nested_param) = components
                    .iter()
                    .enumerate()
                    .find(|(_, p)| p.name == part)?;

                value = value.as_fixed_seq()?.get(idx)?.clone();
                components = nested_param.components.clone();
            }

            (value, components)
        } else {
            // Simple field access
            let param = log_params.iter().find(|p| p.name == *first_segment)?;
            (param.value.clone(), param.components.clone())
        };

    // Process remaining segments (array indices and post-index field access)
    for segment in &segments[1..] {
        if segment.starts_with('[') && segment.ends_with(']') {
            // Array index segment
            let index_str = &segment[1..segment.len() - 1];
            let index: usize = index_str.parse().ok()?;

            current_value = match &current_value {
                DynSolValue::Array(arr) | DynSolValue::FixedArray(arr) => {
                    arr.get(index)?.clone()
                }
                _ => return None, // Not an array
            };
            // Components stay the same - they describe each element's structure
        } else {
            // Field name segment - could be after array index or nested access
            // First try numeric index for raw tuple access
            if let Ok(idx) = segment.parse::<usize>() {
                current_value = match &current_value {
                    DynSolValue::Tuple(items) => items.get(idx)?.clone(),
                    _ => return None,
                };
                // Update components to the nested field's components
                if let Some(nested) = current_components.get(idx) {
                    current_components = nested.components.clone();
                } else {
                    current_components = vec![];
                }
            } else {
                // Named field access - use components to find position
                let (idx, nested_param) = current_components
                    .iter()
                    .enumerate()
                    .find(|(_, p)| p.name == *segment)?;

                current_value = match &current_value {
                    DynSolValue::Tuple(items) => items.get(idx)?.clone(),
                    _ => return None,
                };
                // Update components to the nested field's components
                current_components = nested_param.components.clone();
            }
        }
    }

    Some(current_value)
}

/// Parses a field path into segments, grouping field paths together and separating array indices.
/// Examples:
///   "from" -> ["from"]
///   "ids[0]" -> ["ids", "[0]"]
///   "data.ids[0]" -> ["data.ids", "[0]"]
///   "data[0]" -> ["data", "[0]"]
///   "a[0][1]" -> ["a", "[0]", "[1]"]
///   "data.nested.field" -> ["data.nested.field"]
fn parse_path_segments(path: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut chars = path.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '[' => {
                if !current.is_empty() {
                    segments.push(std::mem::take(&mut current));
                }
                // Collect the array index including brackets
                current.push('[');
                while let Some(&next_c) = chars.peek() {
                    current.push(chars.next().unwrap());
                    if next_c == ']' {
                        break;
                    }
                }
                segments.push(std::mem::take(&mut current));
            }
            '.' if segments.iter().any(|s| s.starts_with('[')) => {
                // After an array index, dot separates new segments
                // (but before array index, dots are part of nested path)
                if !current.is_empty() {
                    segments.push(std::mem::take(&mut current));
                }
            }
            _ => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        segments.push(current);
    }

    segments
}

/// Extracts a value from event log parameters, transaction metadata, or returns a literal value.
///
/// Supported `$` references:
/// - `$block_number` - Block number (uint64)
/// - `$block_timestamp` - Block timestamp as TIMESTAMPTZ (may be None if not available)
/// - `$tx_hash` - Transaction hash (bytes32)
/// - `$contract_address` - Contract address (address)
/// - `$log_index` - Log index (uint256)
/// - `$tx_index` - Transaction index (uint64)
/// - `$<event_field>` - Any field from the event (e.g., `$from`, `$to`, `$value`)
/// - `$field.nested` - Nested tuple/struct access (e.g., `$data.amount`)
/// - `$field[0]` - Array indexing (e.g., `$ids[0]`, `$tokens[1]`)
/// - `$field[0].nested` - Combined array and field access (e.g., `$transfers[0].amount`)
///
/// Also supports computed columns with arithmetic expressions:
/// - `$value * 2` - Multiply event field by 2
/// - `$amount + $fee` - Add two event fields
/// - `$ratio / 100` - Divide event field by 100
/// - `($a + $b) * $c` - Complex expressions with parentheses
fn extract_value_from_event(
    value_ref: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
    column_type: &ColumnType,
) -> Option<EthereumSqlTypeWrapper> {
    // Check for arithmetic expression first (e.g., "$value * 2", "$amount + $fee")
    if is_arithmetic_expression(value_ref) {
        let json_data = log_params_to_json(log_params);
        return match evaluate_arithmetic(value_ref, &json_data) {
            Ok(ComputedValue::U256(val)) => {
                // Convert based on column type
                match column_type {
                    ColumnType::Uint64 => Some(EthereumSqlTypeWrapper::U64BigInt(val.to::<u64>())),
                    ColumnType::Uint128 => {
                        Some(EthereumSqlTypeWrapper::U256Numeric(val))
                    }
                    _ => Some(EthereumSqlTypeWrapper::U256Numeric(val)),
                }
            }
            Ok(ComputedValue::String(s)) => Some(EthereumSqlTypeWrapper::String(s)),
            Err(e) => {
                debug!("Arithmetic expression evaluation failed: {}. Expression: {}", e, value_ref);
                None
            }
        };
    }

    // Check for string template (e.g., "Pool: $token0/$token1", "$from-$to")
    if is_string_template(value_ref) {
        return expand_string_template(value_ref, log_params, tx_metadata)
            .map(EthereumSqlTypeWrapper::String);
    }

    if value_ref.starts_with('$') {
        let field_name = &value_ref[1..];

        // Check for built-in transaction metadata fields first
        match field_name {
            "block_number" => {
                // Use U64BigInt for proper BIGINT binary serialization
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number));
            }
            "block_timestamp" => {
                return tx_metadata.block_timestamp.and_then(|ts| {
                    DateTime::from_timestamp(ts.to::<i64>(), 0)
                        .map(|dt| EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)))
                });
            }
            "tx_hash" => {
                // Store as hex string for readability (e.g., "0x...")
                return Some(EthereumSqlTypeWrapper::String(format!(
                    "{:?}",
                    tx_metadata.tx_hash
                )));
            }
            "block_hash" => {
                // Store as hex string for readability (e.g., "0x...")
                return Some(EthereumSqlTypeWrapper::String(format!(
                    "{:?}",
                    tx_metadata.block_hash
                )));
            }
            "contract_address" => {
                return Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address));
            }
            "log_index" => {
                return Some(EthereumSqlTypeWrapper::U256(tx_metadata.log_index));
            }
            "tx_index" => {
                // Use U64BigInt for proper BIGINT binary serialization
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.tx_index));
            }
            _ => {}
        }

        // Handle nested tuple access (e.g., $value.amount.token) and array indexing (e.g., $ids[0])
        // Split into root field and nested path
        let value = resolve_field_path(field_name, log_params)?;

        Some(dyn_sol_value_to_wrapper(&value, column_type))
    } else {
        // Literal value
        Some(literal_to_wrapper(value_ref, column_type))
    }
}

/// Converts a DynSolValue to the appropriate EthereumSqlTypeWrapper.
/// Uses PostgreSQL-compatible types (U256Numeric for NUMERIC, U64BigInt for BIGINT).
fn dyn_sol_value_to_wrapper(
    value: &DynSolValue,
    column_type: &ColumnType,
) -> EthereumSqlTypeWrapper {
    match (value, column_type) {
        (DynSolValue::Address(addr), ColumnType::Address) => {
            EthereumSqlTypeWrapper::Address(*addr)
        }
        (DynSolValue::Uint(val, _), ColumnType::Uint256) => {
            // Use U256Numeric for NUMERIC columns in PostgreSQL
            EthereumSqlTypeWrapper::U256Numeric(*val)
        }
        (DynSolValue::Uint(val, _), ColumnType::Uint64) => {
            // Use U64BigInt for BIGINT columns in PostgreSQL
            EthereumSqlTypeWrapper::U64BigInt(val.to::<u64>())
        }
        (DynSolValue::Int(val, _), ColumnType::Int256) => {
            // Use I256Numeric for NUMERIC columns in PostgreSQL
            EthereumSqlTypeWrapper::I256Numeric(*val)
        }
        (DynSolValue::Bool(b), ColumnType::Bool) => EthereumSqlTypeWrapper::Bool(*b),
        (DynSolValue::String(s), ColumnType::String) => {
            EthereumSqlTypeWrapper::String(s.clone())
        }
        (DynSolValue::FixedBytes(bytes, _), ColumnType::Bytes32) => {
            if bytes.len() == 32 {
                EthereumSqlTypeWrapper::B256(alloy::primitives::B256::from_slice(bytes.as_slice()))
            } else {
                EthereumSqlTypeWrapper::Bytes(alloy::primitives::Bytes::copy_from_slice(
                    bytes.as_slice(),
                ))
            }
        }
        (DynSolValue::Uint(val, _), ColumnType::Timestamp) => {
            // Convert Unix timestamp uint256 to DateTime
            if let Some(dt) = DateTime::from_timestamp(val.to::<i64>(), 0) {
                EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc))
            } else {
                EthereumSqlTypeWrapper::U256Numeric(*val)
            }
        }
        // Array types - convert to VecAddress or serialize as JSON
        (DynSolValue::Array(items), ColumnType::Array(inner_type)) => {
            // Handle address arrays specially since we have VecAddress
            if **inner_type == ColumnType::Address {
                let addresses: Vec<Address> = items
                    .iter()
                    .filter_map(|item| {
                        if let DynSolValue::Address(addr) = item {
                            Some(*addr)
                        } else {
                            None
                        }
                    })
                    .collect();
                EthereumSqlTypeWrapper::VecAddress(addresses)
            } else {
                // For other array types, serialize as JSON string
                let json_array: Vec<String> = items
                    .iter()
                    .map(|item| format!("{:?}", item))
                    .collect();
                EthereumSqlTypeWrapper::String(format!("{:?}", json_array))
            }
        }
        // Small integer types - use proper wrapper for binary serialization
        (DynSolValue::Uint(val, _), ColumnType::Uint8) => {
            EthereumSqlTypeWrapper::U8(val.to::<u8>())
        }
        (DynSolValue::Uint(val, _), ColumnType::Uint16) => {
            EthereumSqlTypeWrapper::U16(val.to::<u16>())
        }
        (DynSolValue::Uint(val, _), ColumnType::Uint32) => {
            EthereumSqlTypeWrapper::U32(val.to::<u32>())
        }
        (DynSolValue::Int(val, _), ColumnType::Int8) => {
            EthereumSqlTypeWrapper::I8(val.as_i8())
        }
        (DynSolValue::Int(val, _), ColumnType::Int16) => {
            EthereumSqlTypeWrapper::I16(val.as_i16())
        }
        (DynSolValue::Int(val, _), ColumnType::Int32) => {
            EthereumSqlTypeWrapper::I32(val.as_i32())
        }
        (DynSolValue::Int(val, _), ColumnType::Int64) => {
            EthereumSqlTypeWrapper::I64(val.as_i64())
        }
        // Fallback conversions - use PostgreSQL-compatible types
        (DynSolValue::Uint(val, _), _) => EthereumSqlTypeWrapper::U256Numeric(*val),
        (DynSolValue::Int(val, _), _) => EthereumSqlTypeWrapper::I256Numeric(*val),
        (DynSolValue::Address(addr), _) => EthereumSqlTypeWrapper::Address(*addr),
        (DynSolValue::Bool(b), _) => EthereumSqlTypeWrapper::Bool(*b),
        (DynSolValue::String(s), _) => EthereumSqlTypeWrapper::String(s.clone()),
        _ => EthereumSqlTypeWrapper::String(format!("{:?}", value)),
    }
}

/// Converts a literal string value to the appropriate EthereumSqlTypeWrapper.
/// Uses PostgreSQL-compatible types (U256Numeric for NUMERIC, U64BigInt for BIGINT).
fn literal_to_wrapper(value: &str, column_type: &ColumnType) -> EthereumSqlTypeWrapper {
    match column_type {
        ColumnType::String => EthereumSqlTypeWrapper::String(value.to_string()),
        // 8-bit integers -> U8/I8 (serialized as INT2/SMALLINT)
        ColumnType::Uint8 => {
            if let Ok(num) = value.parse::<u8>() {
                EthereumSqlTypeWrapper::U8(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // 16-bit integers -> U16/I16 (serialized as INT2/SMALLINT)
        ColumnType::Uint16 => {
            if let Ok(num) = value.parse::<u16>() {
                EthereumSqlTypeWrapper::U16(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // 32-bit integers -> U32/I32 (serialized as INT4/INTEGER)
        ColumnType::Uint32 => {
            if let Ok(num) = value.parse::<u32>() {
                EthereumSqlTypeWrapper::U32(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Uint64 => {
            if let Ok(num) = value.parse::<u64>() {
                EthereumSqlTypeWrapper::U64BigInt(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Uint128 | ColumnType::Uint256 => {
            if let Ok(num) = value.parse::<alloy::primitives::U256>() {
                EthereumSqlTypeWrapper::U256Numeric(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // Signed 8-bit integers -> I8 (serialized as INT2/SMALLINT)
        ColumnType::Int8 => {
            if let Ok(num) = value.parse::<i8>() {
                EthereumSqlTypeWrapper::I8(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // Signed 16-bit integers -> I16 (serialized as INT2/SMALLINT)
        ColumnType::Int16 => {
            if let Ok(num) = value.parse::<i16>() {
                EthereumSqlTypeWrapper::I16(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // Signed 32-bit integers -> I32 (serialized as INT4/INTEGER)
        ColumnType::Int32 => {
            if let Ok(num) = value.parse::<i32>() {
                EthereumSqlTypeWrapper::I32(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Int64 => {
            if let Ok(num) = value.parse::<i64>() {
                EthereumSqlTypeWrapper::I64(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Int128 | ColumnType::Int256 => {
            if let Ok(num) = value.parse::<alloy::primitives::I256>() {
                EthereumSqlTypeWrapper::I256Numeric(num)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Bool => {
            let b = value.to_lowercase() == "true" || value == "1";
            EthereumSqlTypeWrapper::Bool(b)
        }
        ColumnType::Address => {
            if let Ok(addr) = value.parse::<alloy::primitives::Address>() {
                EthereumSqlTypeWrapper::Address(addr)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Bytes => {
            if let Ok(bytes) = value.parse::<alloy::primitives::Bytes>() {
                EthereumSqlTypeWrapper::Bytes(bytes)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Bytes32 => {
            if let Ok(bytes) = value.parse::<alloy::primitives::B256>() {
                EthereumSqlTypeWrapper::B256(bytes)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        ColumnType::Timestamp => {
            // Try parsing as Unix timestamp first, then as ISO 8601
            if let Ok(ts) = value.parse::<i64>() {
                if let Some(dt) = DateTime::from_timestamp(ts, 0) {
                    EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc))
                } else {
                    EthereumSqlTypeWrapper::String(value.to_string())
                }
            } else if let Ok(dt) = value.parse::<DateTime<Utc>>() {
                EthereumSqlTypeWrapper::DateTime(dt)
            } else {
                EthereumSqlTypeWrapper::String(value.to_string())
            }
        }
        // Arrays from literals are stored as JSON strings
        // (arrays from event data are handled in dyn_sol_value_to_wrapper)
        ColumnType::Array(_) => EthereumSqlTypeWrapper::String(value.to_string()),
    }
}

/// Converts log parameters to a JSON object for filter evaluation.
fn log_params_to_json(log_params: &[LogParam]) -> Value {
    let mut map = serde_json::Map::new();
    for param in log_params {
        let value = dyn_sol_value_to_json(&param.value);
        map.insert(param.name.clone(), value);
    }
    Value::Object(map)
}

/// Converts a DynSolValue to a JSON Value for filter evaluation.
fn dyn_sol_value_to_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Address(addr) => json!(format!("{:?}", addr)),
        DynSolValue::Uint(val, _) => {
            // For large values, use string to preserve precision
            if *val > U256::from(u64::MAX) {
                json!(val.to_string())
            } else {
                json!(val.to::<u64>())
            }
        }
        DynSolValue::Int(val, _) => json!(val.to_string()),
        DynSolValue::Bool(b) => json!(*b),
        DynSolValue::String(s) => json!(s),
        DynSolValue::Bytes(b) => json!(format!("0x{}", hex::encode(b))),
        DynSolValue::FixedBytes(b, _) => json!(format!("0x{}", hex::encode(b))),
        DynSolValue::Tuple(values) => {
            // Convert tuple to object with index keys
            let obj: serde_json::Map<String, Value> = values
                .iter()
                .enumerate()
                .map(|(i, v)| (i.to_string(), dyn_sol_value_to_json(v)))
                .collect();
            Value::Object(obj)
        }
        DynSolValue::Array(values) => {
            json!(values.iter().map(dyn_sol_value_to_json).collect::<Vec<_>>())
        }
        _ => json!(format!("{:?}", value)),
    }
}

/// Evaluates a filter expression against log parameters.
/// Uses the powerful filter module for complex expressions.
fn evaluate_filter(filter_expr: &str, log_params: &[LogParam]) -> bool {
    let json_data = log_params_to_json(log_params);
    match filter_by_expression(filter_expr, &json_data) {
        Ok(result) => result,
        Err(e) => {
            debug!("Filter evaluation failed: {}. Expression: {}", e, filter_expr);
            // On filter error, default to not matching (skip this event)
            false
        }
    }
}

/// Expands iterate bindings by extracting arrays and creating virtual log params for each element.
///
/// For example, with bindings `[$ids as id, $values as amount]` and arrays of length 3,
/// this returns 3 sets of log_params, each with additional synthetic params for `id` and `amount`.
///
/// Returns None if:
/// - Any binding references a non-existent field
/// - Any binding references a non-array field
/// - Parallel arrays have different lengths
fn expand_iterate_bindings(
    bindings: &[IterateBinding],
    log_params: &[LogParam],
) -> Option<Vec<Vec<LogParam>>> {
    if bindings.is_empty() {
        // No iteration - return the original params as a single iteration
        return Some(vec![log_params.to_vec()]);
    }

    // Extract arrays for each binding
    let mut arrays: Vec<(&IterateBinding, Vec<DynSolValue>)> = Vec::new();

    for binding in bindings {
        // Find the field in log_params (supports nested paths like "data.ids")
        let value = if binding.array_field.contains('.') {
            let (root, rest) = binding.array_field.split_once('.')?;
            let param = log_params.iter().find(|p| p.name == root)?;
            param.get_param_value(rest)?
        } else {
            let param = log_params.iter().find(|p| p.name == binding.array_field)?;
            param.value.clone()
        };

        // Extract array elements
        let elements = match value {
            DynSolValue::Array(arr) | DynSolValue::FixedArray(arr) => arr,
            _ => {
                debug!(
                    "iterate binding '{}' references non-array field",
                    binding.array_field
                );
                return None;
            }
        };

        arrays.push((binding, elements));
    }

    // Verify all arrays have the same length
    if arrays.is_empty() {
        return Some(vec![log_params.to_vec()]);
    }

    let expected_len = arrays[0].1.len();
    for (binding, arr) in &arrays {
        if arr.len() != expected_len {
            debug!(
                "iterate binding '{}' has length {} but expected {} (arrays must have equal length)",
                binding.array_field,
                arr.len(),
                expected_len
            );
            return None;
        }
    }

    // Generate expanded params for each index
    let mut result: Vec<Vec<LogParam>> = Vec::with_capacity(expected_len);

    for idx in 0..expected_len {
        // Clone the original params
        let mut expanded_params = log_params.to_vec();

        // Add synthetic params for each binding
        for (binding, arr) in &arrays {
            let element_value = arr[idx].clone();
            expanded_params.push(LogParam::new(binding.alias.clone(), element_value));
        }

        result.push(expanded_params);
    }

    Some(result)
}

/// Processes table operations for a batch of events.
///
/// # Arguments
/// * `tables` - The table configurations
/// * `event_name` - The name of the event being processed
/// * `events_data` - Batch of events with (log_params, network, tx_metadata)
/// * `postgres` - Optional PostgreSQL client
/// * `clickhouse` - Optional ClickHouse client
pub async fn process_table_operations(
    tables: &[TableRuntime],
    event_name: &str,
    events_data: &[(Vec<LogParam>, String, TxMetadata)], // (log_params, network, tx_metadata)
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
) -> Result<(), String> {
    for table_runtime in tables {
        // Find operations for this event
        let event_mapping = table_runtime
            .table
            .events
            .iter()
            .find(|e| e.event == event_name);

        let event_mapping = match event_mapping {
            Some(em) => em,
            None => continue,
        };

        for operation in &event_mapping.operations {
            let mut rows_to_process: Vec<TableRowData> = Vec::new();

            // Check if condition has @table references - push to SQL instead of Rust evaluation
            let (should_filter_in_rust, sql_condition) = if let Some(condition_expr) =
                operation.condition()
            {
                match parse_filter_expression(condition_expr) {
                    Ok(expr) => {
                        if expr.has_table_references() {
                            let sql = expr.to_sql_condition(&table_runtime.full_table_name);
                            (false, Some(sql))
                        } else {
                            (true, None)
                        }
                    }
                    Err(e) => {
                        debug!("Failed to parse condition for SQL generation: {}", e);
                        (true, None)
                    }
                }
            } else {
                (false, None)
            };

            for (log_params, network, tx_metadata) in events_data {
                // Expand iterate bindings - creates multiple virtual events from array fields
                let expanded_params_list =
                    match expand_iterate_bindings(&event_mapping.iterate, log_params) {
                        Some(params) => params,
                        None => {
                            debug!(
                                "Failed to expand iterate bindings for event {}",
                                event_name
                            );
                            continue;
                        }
                    };

                for expanded_log_params in &expanded_params_list {
                    if should_filter_in_rust {
                        if let Some(condition_expr) = operation.condition() {
                            if !evaluate_filter(condition_expr, expanded_log_params) {
                                continue;
                            }
                        }
                    }

                    let mut columns: HashMap<String, EthereumSqlTypeWrapper> = HashMap::new();

                    // Add where clause columns
                    for (column_name, value_ref) in &operation.where_clause {
                        let column_def = table_runtime
                            .table
                            .columns
                            .iter()
                            .find(|c| &c.name == column_name);

                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_value_from_event(
                                value_ref,
                                expanded_log_params,
                                tx_metadata,
                                column_def.resolved_type(),
                            ) {
                                columns.insert(column_name.clone(), value);
                            }
                        }
                    }

                    // Add set columns with their values
                    for set_col in &operation.set {
                        let column_def = table_runtime
                            .table
                            .columns
                            .iter()
                            .find(|c| c.name == set_col.column);

                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_value_from_event(
                                set_col.effective_value(),
                                expanded_log_params,
                                tx_metadata,
                                column_def.resolved_type(),
                            ) {
                                columns.insert(set_col.column.clone(), value);
                            }
                        }
                    }

                    if !columns.is_empty() {
                        // Auto-injected metadata columns
                        let sequence_id = compute_sequence_id(
                            tx_metadata.block_number,
                            tx_metadata.tx_index,
                            tx_metadata.log_index.to::<u64>(),
                        );
                        columns.insert(
                            injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                            EthereumSqlTypeWrapper::U128(sequence_id),
                        );
                        columns.insert(
                            injected_columns::LAST_UPDATED_BLOCK.to_string(),
                            EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number),
                        );
                        if let Some(ts) = tx_metadata.block_timestamp {
                            if let Some(dt) = DateTime::from_timestamp(ts.to::<i64>(), 0) {
                                columns.insert(
                                    injected_columns::LAST_UPDATED_AT.to_string(),
                                    EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)),
                                );
                            }
                        }
                        columns.insert(
                            injected_columns::TX_HASH.to_string(),
                            EthereumSqlTypeWrapper::String(format!("{:?}", tx_metadata.tx_hash)),
                        );
                        columns.insert(
                            injected_columns::BLOCK_HASH.to_string(),
                            EthereumSqlTypeWrapper::String(format!("{:?}", tx_metadata.block_hash)),
                        );
                        columns.insert(
                            injected_columns::CONTRACT_ADDRESS.to_string(),
                            EthereumSqlTypeWrapper::Address(tx_metadata.contract_address),
                        );

                        rows_to_process.push(TableRowData {
                            columns,
                            network: network.clone(),
                        });
                    }
                } // end for expanded_log_params
            }

            if rows_to_process.is_empty() {
                continue;
            }

            // Execute the operation
            if let Some(postgres) = &postgres {
                execute_postgres_operation(
                    postgres,
                    &table_runtime.full_table_name,
                    &table_runtime.table,
                    operation,
                    &rows_to_process,
                    sql_condition.as_deref(),
                )
                .await?;
            }

            if let Some(clickhouse) = &clickhouse {
                execute_clickhouse_operation(
                    clickhouse,
                    &table_runtime.full_table_name,
                    &table_runtime.table,
                    operation,
                    &rows_to_process,
                )
                .await?;
            }
        }
    }

    Ok(())
}

/// Maps ColumnType to BatchOperationSqlType.
fn column_type_to_batch_sql_type(column_type: &ColumnType) -> BatchOperationSqlType {
    match column_type {
        ColumnType::Address => BatchOperationSqlType::Char,
        // 8-bit and 16-bit integers -> SMALLINT (INT2)
        ColumnType::Uint8 | ColumnType::Uint16 | ColumnType::Int8 | ColumnType::Int16 => {
            BatchOperationSqlType::Smallint
        }
        // 32-bit integers -> INTEGER (INT4)
        ColumnType::Uint32 | ColumnType::Int32 => BatchOperationSqlType::Integer,
        // 64-bit integers
        ColumnType::Uint64 | ColumnType::Int64 => BatchOperationSqlType::Bigint,
        // Large integers -> NUMERIC
        ColumnType::Uint128
        | ColumnType::Uint256
        | ColumnType::Int128
        | ColumnType::Int256 => BatchOperationSqlType::Numeric,
        // Bytes types
        ColumnType::Bytes | ColumnType::Bytes32 => BatchOperationSqlType::Bytea,
        ColumnType::String => BatchOperationSqlType::Varchar,
        ColumnType::Bool => BatchOperationSqlType::Bool,
        ColumnType::Timestamp => BatchOperationSqlType::DateTime,
        // All array types use TEXT[] for simplicity
        ColumnType::Array(_) => BatchOperationSqlType::TextArray,
    }
}

/// Maps SetAction to BatchOperationAction.
fn set_action_to_batch_action(action: &SetAction) -> BatchOperationAction {
    match action {
        SetAction::Set => BatchOperationAction::Set,
        SetAction::Add => BatchOperationAction::Add,
        SetAction::Subtract => BatchOperationAction::Subtract,
        SetAction::Max => BatchOperationAction::Max,
        SetAction::Min => BatchOperationAction::Min,
        // Increment/Decrement are syntactic sugar for Add/Subtract with value "1"
        SetAction::Increment => BatchOperationAction::Add,
        SetAction::Decrement => BatchOperationAction::Subtract,
    }
}

/// Maps OperationType to BatchOperationType.
fn operation_type_to_batch_type(op_type: &OperationType) -> BatchOperationType {
    match op_type {
        OperationType::Upsert => BatchOperationType::Upsert,
        OperationType::Update => BatchOperationType::Update,
        OperationType::Delete => BatchOperationType::Delete,
    }
}

/// Executes a PostgreSQL operation for tables using the batch operations infrastructure.
///
/// # Arguments
/// * `sql_where` - Optional SQL WHERE condition for upsert operations.
///   Used when the `if`/`filter` condition contains `@table` references.
///   E.g., conditions like `$value > @balance` become SQL `EXCLUDED.value > table.balance`.
async fn execute_postgres_operation(
    postgres: &PostgresClient,
    table_name: &str,
    table_def: &Table,
    operation: &TableOperation,
    rows: &[TableRowData],
    sql_where: Option<&str>,
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    // Build rows of DynamicColumnDefinition for the batch operation
    let mut batch_rows: Vec<Vec<DynamicColumnDefinition>> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut columns: Vec<DynamicColumnDefinition> = Vec::new();

        if !table_def.cross_chain {
            columns.push(DynamicColumnDefinition::new(
                "network".to_string(),
                EthereumSqlTypeWrapper::String(row.network.clone()),
                BatchOperationSqlType::Varchar,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ));
        }

        for column in &table_def.columns {
            let column_type = column.resolved_type();
            let value = if let Some(v) = row.columns.get(&column.name) {
                v.clone()
            } else if let Some(default) = &column.default {
                literal_to_wrapper(default, column_type)
            } else {
                // Use zero/empty defaults - use PostgreSQL-compatible types
                match column_type {
                    ColumnType::Uint256 => EthereumSqlTypeWrapper::U256Numeric(U256::ZERO),
                    ColumnType::Int256 => {
                        EthereumSqlTypeWrapper::I256Numeric(alloy::primitives::I256::ZERO)
                    }
                    ColumnType::Uint64 => EthereumSqlTypeWrapper::U64BigInt(0),
                    _ => EthereumSqlTypeWrapper::String(String::new()),
                }
            };

            // Determine behavior - primary key columns come from where clauses
            let is_pk = table_def.is_primary_key_column(&column.name);
            let behavior = if is_pk {
                BatchOperationColumnBehavior::Distinct
            } else {
                BatchOperationColumnBehavior::Normal
            };

            // Determine action
            let action = if is_pk {
                BatchOperationAction::Where
            } else if let Some(set_col) = operation.set.iter().find(|s| s.column == column.name) {
                set_action_to_batch_action(&set_col.action)
            } else {
                BatchOperationAction::Nothing
            };

            columns.push(DynamicColumnDefinition::new(
                column.name.clone(),
                value,
                column_type_to_batch_sql_type(column_type),
                behavior,
                action,
            ));
        }

        // Auto-injected metadata columns
        if let Some(seq_id) = row.columns.get(injected_columns::RINDEXER_SEQUENCE_ID) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                seq_id.clone(),
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
            ));
        }
        if let Some(block) = row.columns.get(injected_columns::LAST_UPDATED_BLOCK) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::LAST_UPDATED_BLOCK.to_string(),
                block.clone(),
                BatchOperationSqlType::Bigint,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(ts) = row.columns.get(injected_columns::LAST_UPDATED_AT) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::LAST_UPDATED_AT.to_string(),
                ts.clone(),
                BatchOperationSqlType::DateTime,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(hash) = row.columns.get(injected_columns::TX_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::TX_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(hash) = row.columns.get(injected_columns::BLOCK_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::BLOCK_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(addr) = row.columns.get(injected_columns::CONTRACT_ADDRESS) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::CONTRACT_ADDRESS.to_string(),
                addr.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        batch_rows.push(columns);
    }

    let op_type = operation_type_to_batch_type(&operation.operation_type);
    // Extract short table name (after the schema prefix)
    let short_table_name = table_name.split('.').last().unwrap_or(table_name);
    let event_name = format!("Tables::{}", short_table_name);

    execute_dynamic_batch_operation(postgres, table_name, op_type, batch_rows, &event_name, sql_where).await?;

    let op_label = match operation.operation_type {
        OperationType::Upsert => "UPSERT",
        OperationType::Update => "UPDATE",
        OperationType::Delete => "DELETE",
    };

    info!(
        "Tables::{} - {} - {} rows",
        short_table_name,
        op_label,
        rows.len()
    );

    Ok(())
}

/// Executes a ClickHouse operation for tables using the batch operations infrastructure.
async fn execute_clickhouse_operation(
    clickhouse: &ClickhouseClient,
    table_name: &str,
    table_def: &Table,
    operation: &TableOperation,
    rows: &[TableRowData],
) -> Result<(), String> {
    if rows.is_empty() {
        return Ok(());
    }

    let mut batch_rows: Vec<Vec<DynamicColumnDefinition>> = Vec::with_capacity(rows.len());

    for row in rows {
        let mut columns: Vec<DynamicColumnDefinition> = Vec::new();

        if !table_def.cross_chain {
            columns.push(DynamicColumnDefinition::new(
                "network".to_string(),
                EthereumSqlTypeWrapper::String(row.network.clone()),
                BatchOperationSqlType::Varchar,
                BatchOperationColumnBehavior::Distinct,
                BatchOperationAction::Where,
            ));
        }

        for column in &table_def.columns {
            let column_type = column.resolved_type();
            let value = if let Some(v) = row.columns.get(&column.name) {
                v.clone()
            } else if let Some(default) = &column.default {
                literal_to_wrapper(default, column_type)
            } else {
                match column_type {
                    ColumnType::Uint256 => EthereumSqlTypeWrapper::U256Numeric(U256::ZERO),
                    ColumnType::Int256 => {
                        EthereumSqlTypeWrapper::I256Numeric(alloy::primitives::I256::ZERO)
                    }
                    ColumnType::Uint64 => EthereumSqlTypeWrapper::U64BigInt(0),
                    _ => EthereumSqlTypeWrapper::String(String::new()),
                }
            };

            let is_pk = table_def.is_primary_key_column(&column.name);
            let behavior = if is_pk {
                BatchOperationColumnBehavior::Distinct
            } else {
                BatchOperationColumnBehavior::Normal
            };

            let action = if is_pk {
                BatchOperationAction::Where
            } else if let Some(set_col) = operation.set.iter().find(|s| s.column == column.name) {
                set_action_to_batch_action(&set_col.action)
            } else {
                BatchOperationAction::Nothing
            };

            columns.push(DynamicColumnDefinition::new(
                column.name.clone(),
                value,
                column_type_to_batch_sql_type(column_type),
                behavior,
                action,
            ));
        }

        // Add auto-injected metadata columns

        // rindexer_sequence_id - used for ordering
        if let Some(seq_id) = row.columns.get(injected_columns::RINDEXER_SEQUENCE_ID) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::RINDEXER_SEQUENCE_ID.to_string(),
                seq_id.clone(),
                BatchOperationSqlType::Numeric,
                BatchOperationColumnBehavior::Sequence,
                BatchOperationAction::Set,
            ));
        }

        // last_updated_block
        if let Some(block) = row.columns.get(injected_columns::LAST_UPDATED_BLOCK) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::LAST_UPDATED_BLOCK.to_string(),
                block.clone(),
                BatchOperationSqlType::Bigint,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // last_updated_at
        if let Some(ts) = row.columns.get(injected_columns::LAST_UPDATED_AT) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::LAST_UPDATED_AT.to_string(),
                ts.clone(),
                BatchOperationSqlType::DateTime,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // tx_hash
        if let Some(hash) = row.columns.get(injected_columns::TX_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::TX_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // block_hash
        if let Some(hash) = row.columns.get(injected_columns::BLOCK_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::BLOCK_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // contract_address
        if let Some(addr) = row.columns.get(injected_columns::CONTRACT_ADDRESS) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::CONTRACT_ADDRESS.to_string(),
                addr.clone(),
                BatchOperationSqlType::Char,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        batch_rows.push(columns);
    }

    let op_type = operation_type_to_batch_type(&operation.operation_type);
    // Extract short table name (after the schema prefix)
    let short_table_name = table_name.split('.').last().unwrap_or(table_name);
    let event_name = format!("Tables::{}", short_table_name);

    execute_clickhouse_dynamic_batch_operation(clickhouse, table_name, op_type, batch_rows, &event_name).await?;

    let op_label = match operation.operation_type {
        OperationType::Upsert => "UPSERT",
        OperationType::Update => "UPDATE",
        OperationType::Delete => "DELETE",
    };

    info!(
        "Tables::{} - {} - {} rows",
        short_table_name,
        op_label,
        rows.len()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_evaluate_filter_equals() {
        let params = vec![LogParam::new(
            "from".to_string(),
            DynSolValue::Address(alloy::primitives::Address::ZERO),
        )];

        assert!(evaluate_filter(
            "from == 0x0000000000000000000000000000000000000000",
            &params
        ));
        assert!(!evaluate_filter(
            "from == 0x1111111111111111111111111111111111111111",
            &params
        ));
    }

    #[test]
    fn test_evaluate_filter_not_equals() {
        let params = vec![LogParam::new(
            "to".to_string(),
            DynSolValue::Address(alloy::primitives::Address::ZERO),
        )];

        assert!(!evaluate_filter(
            "to != 0x0000000000000000000000000000000000000000",
            &params
        ));
        assert!(evaluate_filter(
            "to != 0x1111111111111111111111111111111111111111",
            &params
        ));
    }

    #[test]
    fn test_evaluate_filter_complex() {
        let params = vec![
            LogParam::new(
                "from".to_string(),
                DynSolValue::Address(alloy::primitives::Address::ZERO),
            ),
            LogParam::new(
                "value".to_string(),
                DynSolValue::Uint(U256::from(1000u64), 256),
            ),
        ];

        // Complex AND expression
        assert!(evaluate_filter(
            "from == 0x0000000000000000000000000000000000000000 && value > 500",
            &params
        ));
        assert!(!evaluate_filter(
            "from == 0x0000000000000000000000000000000000000000 && value > 2000",
            &params
        ));

        // Complex OR expression
        assert!(evaluate_filter(
            "from != 0x0000000000000000000000000000000000000000 || value > 500",
            &params
        ));
    }

    #[test]
    fn test_is_string_template() {
        // Pure field references - NOT templates
        assert!(!is_string_template("$from"));
        assert!(!is_string_template("$data.amount"));
        assert!(!is_string_template("$ids[0]"));
        assert!(!is_string_template("$transfers[0].value"));
        assert!(!is_string_template("global"));  // No $ at all

        // String templates - ARE templates
        assert!(is_string_template("$from-$to"));
        assert!(is_string_template("Pool: $token0/$token1"));
        assert!(is_string_template("Transfer from $from"));
        assert!(is_string_template("Value: $value"));
        assert!(is_string_template("$from to $to"));
    }

    #[test]
    fn test_expand_string_template() {
        let params = vec![
            LogParam::new(
                "from".to_string(),
                DynSolValue::Address(
                    "0x1111111111111111111111111111111111111111".parse().unwrap()
                ),
            ),
            LogParam::new(
                "to".to_string(),
                DynSolValue::Address(
                    "0x2222222222222222222222222222222222222222".parse().unwrap()
                ),
            ),
            LogParam::new(
                "value".to_string(),
                DynSolValue::Uint(U256::from(1000u64), 256),
            ),
        ];

        let tx_metadata = TxMetadata {
            block_number: 12345,
            block_timestamp: Some(U256::from(1700000000u64)),
            tx_hash: B256::ZERO,
            block_hash: B256::ZERO,
            contract_address: Address::ZERO,
            log_index: U256::from(0u64),
            tx_index: 0,
        };

        // Simple concatenation
        let result = expand_string_template("$from-$to", &params, &tx_metadata).unwrap();
        assert_eq!(
            result,
            "0x1111111111111111111111111111111111111111-0x2222222222222222222222222222222222222222"
        );

        // With prefix text
        let result = expand_string_template("Transfer: $value", &params, &tx_metadata).unwrap();
        assert_eq!(result, "Transfer: 1000");

        // Multiple fields with separators
        let result = expand_string_template("$from -> $to: $value", &params, &tx_metadata).unwrap();
        assert_eq!(
            result,
            "0x1111111111111111111111111111111111111111 -> 0x2222222222222222222222222222222222222222: 1000"
        );

        // With tx metadata
        let result = expand_string_template("Block $block_number", &params, &tx_metadata).unwrap();
        assert_eq!(result, "Block 12345");

        // Non-existent field returns None
        let result = expand_string_template("$nonexistent", &params, &tx_metadata);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_field_path_post_array_access() {
        use alloy::json_abi::Param;

        // Create an array of transfer structs: [{from, to, amount}, {from, to, amount}]
        let transfer1 = DynSolValue::Tuple(vec![
            DynSolValue::Address("0x1111111111111111111111111111111111111111".parse().unwrap()),
            DynSolValue::Address("0x2222222222222222222222222222222222222222".parse().unwrap()),
            DynSolValue::Uint(U256::from(100u64), 256),
        ]);
        let transfer2 = DynSolValue::Tuple(vec![
            DynSolValue::Address("0x3333333333333333333333333333333333333333".parse().unwrap()),
            DynSolValue::Address("0x4444444444444444444444444444444444444444".parse().unwrap()),
            DynSolValue::Uint(U256::from(200u64), 256),
        ]);

        // Create the array
        let transfers_array = DynSolValue::Array(vec![transfer1, transfer2]);

        // Create ABI components describing the struct fields
        let components = vec![
            Param {
                name: "from".to_string(),
                ty: "address".to_string(),
                internal_type: None,
                components: vec![],
            },
            Param {
                name: "to".to_string(),
                ty: "address".to_string(),
                internal_type: None,
                components: vec![],
            },
            Param {
                name: "amount".to_string(),
                ty: "uint256".to_string(),
                internal_type: None,
                components: vec![],
            },
        ];

        // Create LogParam with components
        let params = vec![LogParam {
            name: "transfers".to_string(),
            value: transfers_array,
            components,
        }];

        // Test post-array field access: $transfers[0].amount
        let result = resolve_field_path("transfers[0].amount", &params);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), DynSolValue::Uint(U256::from(100u64), 256));

        // Test post-array field access: $transfers[1].from
        let result = resolve_field_path("transfers[1].from", &params);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            DynSolValue::Address("0x3333333333333333333333333333333333333333".parse().unwrap())
        );

        // Test post-array field access: $transfers[0].to
        let result = resolve_field_path("transfers[0].to", &params);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            DynSolValue::Address("0x2222222222222222222222222222222222222222".parse().unwrap())
        );

        // Test numeric index still works: $transfers[1].1 (second field = to)
        let result = resolve_field_path("transfers[1].1", &params);
        assert!(result.is_some());
        assert_eq!(
            result.unwrap(),
            DynSolValue::Address("0x4444444444444444444444444444444444444444".parse().unwrap())
        );

        // Test non-existent field returns None
        let result = resolve_field_path("transfers[0].nonexistent", &params);
        assert!(result.is_none());

        // Test out of bounds returns None
        let result = resolve_field_path("transfers[5].amount", &params);
        assert!(result.is_none());
    }
}
