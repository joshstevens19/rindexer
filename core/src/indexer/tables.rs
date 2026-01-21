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
//! - `rindexer_last_updated_block` (BIGINT) - The block number when the row was last updated
//! - `rindexer_last_updated_at` (TIMESTAMPTZ) - The timestamp when the row was last updated
//! - `rindexer_tx_hash` (CHAR(66)) - The transaction hash of the event that last updated this row
//! - `rindexer_block_hash` (CHAR(66)) - The block hash of the event that last updated this row
//! - `rindexer_contract_address` (CHAR(42)) - The contract address that emitted the event
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
//! - **View calls**: `$call($rindexer_contract_address, "balanceOf(address)", $holder)` (on-chain data)
//!   - **Position-based access**: `$call($addr, "getReserves()")[0]` - access tuple/array elements by index
//!   - **Named field access**: `$call($addr, "getReserves() returns (uint112 reserve0, uint112 reserve1)").reserve0`
//!   - **Chained access**: `$call($addr, "getData() returns ((uint256 x, uint256 y) point)").point.x`
//! - **Constants**: `$constant(name)` - Reference user-defined constants from the YAML config
//!   - **Simple constants**: Same value for all networks
//!   - **Network-scoped constants**: Different value per network (auto-resolved based on current network)
//! - **Transaction metadata** (all prefixed with `rindexer_` to avoid conflicts with event fields):
//!   - `$rindexer_block_number` - The block number
//!   - `$rindexer_block_timestamp` - The block timestamp (as TIMESTAMPTZ)
//!   - `$rindexer_tx_hash` - The transaction hash (as hex string)
//!   - `$rindexer_block_hash` - The block hash (as hex string)
//!   - `$rindexer_contract_address` - The contract address that emitted the event
//!   - `$rindexer_log_index` - The log index within the transaction
//!   - `$rindexer_tx_index` - The transaction index within the block
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

use alloy::dyn_abi::{DynSolType, DynSolValue};
use alloy::primitives::{Address, Bytes, B256, U256};
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

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
use crate::manifest::core::Constants;
use crate::provider::JsonRpcCachedProvider;
use crate::types::core::LogParam;

/// Cache key type for view calls: (network, contract_address, calldata, block_number).
type ViewCallCacheKey = (String, Address, Bytes, u64);

/// Global cache for view call results. Key is (network, contract, calldata, block_number).
/// Uses block_number for determinism - same call at same block always returns same result.
static VIEW_CALL_CACHE: Lazy<RwLock<HashMap<ViewCallCacheKey, DynSolValue>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Default limit for concurrent view calls.
const DEFAULT_MAX_CONCURRENT_VIEW_CALLS: usize = 10;

/// Semaphore to limit concurrent view calls and avoid overwhelming RPC nodes.
/// Initialized with default, can be reconfigured via `configure_view_call_limit`.
static VIEW_CALL_SEMAPHORE: Lazy<RwLock<std::sync::Arc<tokio::sync::Semaphore>>> =
    Lazy::new(|| {
        RwLock::new(std::sync::Arc::new(tokio::sync::Semaphore::new(
            DEFAULT_MAX_CONCURRENT_VIEW_CALLS,
        )))
    });

/// Configure the maximum number of concurrent view calls.
/// Should be called once at startup before any view calls are made.
/// If not called, defaults to 10 concurrent calls.
pub async fn configure_view_call_limit(limit: usize) {
    let mut semaphore = VIEW_CALL_SEMAPHORE.write().await;
    *semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(limit));
    info!("View call concurrency limit set to {}", limit);
}

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
        let full_table_name = generate_table_full_name(indexer_name, contract_name, &table.name);
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
/// Computed expressions like "$value * 2", "$amount + $fee", "$ratio / 100", "10 ^ $decimals" will return true.
/// Also supports $call() in arithmetic: "$amount / (10 ^ $call($asset, \"decimals()\"))"
fn is_arithmetic_expression(value: &str) -> bool {
    // Must contain at least one arithmetic operator
    // Check for operators that are not part of comparison (==, !=, >=, <=)
    let has_operator = value.chars().enumerate().any(|(i, c)| {
        if c == '*' || c == '/' || c == '^' {
            true
        } else if c == '+' || c == '-' {
            // Check it's not a unary operator at the start
            i > 0
        } else {
            false
        }
    });

    // Must have operators AND contain either $field or $call references
    has_operator && (value.contains('$'))
}

/// Checks if an arithmetic expression contains $call() patterns that need async resolution.
fn arithmetic_has_calls(value: &str) -> bool {
    value.contains("$call(")
}

/// Finds all $call(...) patterns in a string, handling nested parentheses.
/// Returns a vector of (start_index, end_index, call_expression) for each match.
fn find_call_patterns(value: &str) -> Vec<(usize, usize, String)> {
    let mut results = Vec::new();
    let mut search_start = 0;

    while let Some(start) = value[search_start..].find("$call(") {
        let absolute_start = search_start + start;
        let call_start = absolute_start + 6; // Skip "$call("

        // Find matching closing paren, handling nested parens
        let mut depth = 1;
        let mut end_pos = None;

        for (i, c) in value[call_start..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end_pos = Some(call_start + i);
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Some(end) = end_pos {
            let call_expr = value[absolute_start..=end].to_string();
            results.push((absolute_start, end + 1, call_expr));
            search_start = end + 1;
        } else {
            // Malformed - no closing paren, skip this match
            search_start = absolute_start + 6;
        }
    }

    results
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
    if let Some(after_dollar) = value.strip_prefix('$') {
        // Simple heuristic: if it's a pure field reference, it should only contain
        // alphanumeric, dots, underscores, and brackets
        // String templates have extra characters like spaces, colons, slashes, etc.
        let is_pure_field = after_dollar
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '[' || c == ']');

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
                if next_c.is_alphanumeric()
                    || next_c == '.'
                    || next_c == '_'
                    || next_c == '['
                    || next_c == ']'
                {
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
    // Check for built-in transaction metadata fields first (all prefixed with rindexer_)
    match field_name {
        "rindexer_block_number" => return Some(tx_metadata.block_number.to_string()),
        "rindexer_block_timestamp" => {
            return tx_metadata.block_timestamp.map(|ts| ts.to_string());
        }
        "rindexer_tx_hash" => return Some(format!("{:?}", tx_metadata.tx_hash)),
        "rindexer_block_hash" => return Some(format!("{:?}", tx_metadata.block_hash)),
        "rindexer_contract_address" => return Some(format!("{:?}", tx_metadata.contract_address)),
        "rindexer_log_index" => return Some(tx_metadata.log_index.to_string()),
        "rindexer_tx_index" => return Some(tx_metadata.tx_index.to_string()),
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

/// Checks if a value string is a view call expression like `$call(address, "signature", args...)`.
/// May have accessor after: `$call(...)[0]` or `$call(...).fieldName`
fn is_view_call(value: &str) -> bool {
    // Must start with $call( and have at least one closing paren
    value.starts_with("$call(") && value.contains(')')
}

/// Checks if a value string is a constant reference like `$constant(name)`.
fn is_constant_ref(value: &str) -> bool {
    value.starts_with("$constant(") && value.ends_with(')')
}

/// Parses a `$constant(name)` expression and returns the constant name.
fn parse_constant_ref(value: &str) -> Option<&str> {
    if !is_constant_ref(value) {
        return None;
    }
    // Extract the constant name between $constant( and )
    let start = "$constant(".len();
    let end = value.len() - 1; // Exclude the closing )
    if start >= end {
        return None;
    }
    Some(value[start..end].trim())
}

/// Resolves a constant reference to its value for the given network.
/// Returns the resolved string value, or None if the constant doesn't exist
/// or isn't defined for this network.
fn resolve_constant<'a>(
    constant_name: &str,
    constants: &'a Constants,
    network: &str,
) -> Option<&'a str> {
    constants.get(constant_name).and_then(|c| c.resolve(network))
}

/// Resolves all `$constant(name)` references in a string value.
/// If the entire value is a constant reference, returns the resolved value.
/// This does NOT do string interpolation - constants must be the entire value.
fn resolve_constants_in_value<'a>(
    value: &'a str,
    constants: &'a Constants,
    network: &str,
) -> Option<String> {
    if is_constant_ref(value) {
        let name = parse_constant_ref(value)?;
        resolve_constant(name, constants, network).map(|s| s.to_string())
    } else {
        // Not a constant reference, return as-is
        Some(value.to_string())
    }
}

/// Resolves all `$constant(name)` references embedded anywhere in a string value.
/// Unlike `resolve_constants_in_value`, this handles constants inside larger expressions.
/// e.g., "$call($constant(oracle), \"getPrice()\")" -> "$call(0x1234..., \"getPrice()\")"
fn resolve_all_constants_in_value(
    value: &str,
    constants: &Constants,
    network: &str,
) -> Option<String> {
    let mut result = value.to_string();
    let mut search_start = 0;

    while let Some(start) = result[search_start..].find("$constant(") {
        let absolute_start = search_start + start;
        let const_start = absolute_start + 10; // Skip "$constant("

        // Find closing paren
        if let Some(end_offset) = result[const_start..].find(')') {
            let end = const_start + end_offset;
            let const_name = &result[const_start..end];

            // Resolve the constant
            if let Some(resolved) = resolve_constant(const_name, constants, network) {
                // Replace $constant(name) with the resolved value
                result.replace_range(absolute_start..=end, resolved);
                // Continue searching from after the replacement
                search_start = absolute_start + resolved.len();
            } else {
                // Constant not found, skip this one
                search_start = end + 1;
            }
        } else {
            // Malformed - no closing paren
            break;
        }
    }

    Some(result)
}

/// Parsed view call expression.
#[derive(Debug)]
struct ViewCall {
    contract_address: String, // Either literal address or $field reference
    function_sig: String,     // e.g., "balanceOf(address)" or "decimals()"
    args: Vec<String>,        // Argument values (can be $field references)
    accessor: Option<String>, // Optional accessor like "[0]" or ".fieldName" or ".field[0].nested"
    return_fields: Vec<ReturnField>, // Parsed from "returns (type name, ...)" if present
}

/// A parsed return field from "returns (type name, ...)" syntax.
#[derive(Debug, Clone)]
struct ReturnField {
    name: String,
    type_str: String,
    children: Vec<ReturnField>, // For nested tuples like "(uint256 x, uint256 y) coords"
}

/// Parses a `$call(address, "signature", args...)` expression with optional accessor.
/// Supports:
/// - `$call($addr, "totalSupply()")` - simple, returns value directly
/// - `$call($addr, "getReserves()")[0]` - position-based access
/// - `$call($addr, "getReserves() returns (uint112 reserve0, uint112 reserve1)").reserve0` - named access
fn parse_view_call(value: &str) -> Option<ViewCall> {
    // Find the matching closing paren for $call(
    let start = "$call(".len();
    let mut paren_depth = 1;
    let mut call_end = None;

    for (i, c) in value.chars().enumerate().skip(start) {
        match c {
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth == 0 {
                    call_end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }

    let call_end = call_end?;
    let inner = &value[start..call_end];

    // Extract accessor if present (everything after the closing paren)
    let accessor = if call_end + 1 < value.len() {
        let acc = value[call_end + 1..].trim();
        if acc.is_empty() {
            None
        } else {
            Some(acc.to_string())
        }
    } else {
        None
    };

    // Split by comma, respecting quoted strings
    let parts = split_call_args(inner);
    if parts.len() < 2 {
        return None;
    }

    let contract_address = parts[0].trim().to_string();
    let full_sig = parts[1].trim().trim_matches('"').to_string();
    let args: Vec<String> = parts[2..].iter().map(|s| s.trim().to_string()).collect();

    // Parse "returns (...)" if present
    let (function_sig, return_fields) = parse_function_sig_with_returns(&full_sig);

    Some(ViewCall { contract_address, function_sig, args, accessor, return_fields })
}

/// Parses a function signature that may include "returns (type name, ...)".
/// Returns (clean_sig, return_fields).
fn parse_function_sig_with_returns(sig: &str) -> (String, Vec<ReturnField>) {
    if let Some(returns_idx) = sig.to_lowercase().find(" returns ") {
        let clean_sig = sig[..returns_idx].trim().to_string();
        let returns_part = &sig[returns_idx + 9..].trim(); // skip " returns "
        let return_fields = parse_return_fields(returns_part);
        (clean_sig, return_fields)
    } else {
        (sig.to_string(), vec![])
    }
}

/// Parses return fields from "(type name, type name, ...)" syntax.
/// Supports nested tuples like "(uint256 x, uint256 y) coords".
fn parse_return_fields(s: &str) -> Vec<ReturnField> {
    let s = s.trim();
    if !s.starts_with('(') || !s.ends_with(')') {
        return vec![];
    }

    // Strip outer parens
    let inner = &s[1..s.len() - 1];
    parse_return_field_list(inner)
}

/// Parses a comma-separated list of return fields.
fn parse_return_field_list(s: &str) -> Vec<ReturnField> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut paren_depth = 0;

    for c in s.chars() {
        match c {
            '(' => {
                paren_depth += 1;
                current.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current.push(c);
            }
            ',' if paren_depth == 0 => {
                if let Some(field) = parse_single_return_field(current.trim()) {
                    fields.push(field);
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }

    // Don't forget the last field
    if !current.trim().is_empty() {
        if let Some(field) = parse_single_return_field(current.trim()) {
            fields.push(field);
        }
    }

    fields
}

/// Parses a single return field like "uint256 amount" or "(uint256 x, uint256 y) coords".
fn parse_single_return_field(s: &str) -> Option<ReturnField> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Check for nested tuple: "(type name, ...) fieldName"
    if s.starts_with('(') {
        // Find matching closing paren
        let mut paren_depth = 0;
        let mut tuple_end = None;
        for (i, c) in s.chars().enumerate() {
            match c {
                '(' => paren_depth += 1,
                ')' => {
                    paren_depth -= 1;
                    if paren_depth == 0 {
                        tuple_end = Some(i);
                        break;
                    }
                }
                _ => {}
            }
        }

        if let Some(end) = tuple_end {
            let tuple_part = &s[..=end];
            let name = s[end + 1..].trim().to_string();
            let children = parse_return_fields(tuple_part);
            return Some(ReturnField { name, type_str: "tuple".to_string(), children });
        }
    }

    // Simple field: "type name" or just "type" (unnamed)
    let parts: Vec<&str> = s.split_whitespace().collect();
    match parts.len() {
        1 => Some(ReturnField {
            name: String::new(), // Unnamed, must use position
            type_str: parts[0].to_string(),
            children: vec![],
        }),
        2 => Some(ReturnField {
            name: parts[1].to_string(),
            type_str: parts[0].to_string(),
            children: vec![],
        }),
        _ => None,
    }
}

/// Applies an accessor path to a DynSolValue.
/// Supports: [0], .fieldName, and chains like [0].field.nested
fn apply_accessor(
    value: DynSolValue,
    accessor: &str,
    return_fields: &[ReturnField],
) -> Option<DynSolValue> {
    if accessor.is_empty() {
        return Some(value);
    }

    let segments = parse_accessor_segments(accessor);
    let mut current = value;
    let mut current_fields = return_fields.to_vec();

    for segment in segments {
        match segment {
            AccessorSegment::Index(idx) => {
                // Array/tuple index access
                current = match &current {
                    DynSolValue::Tuple(items)
                    | DynSolValue::Array(items)
                    | DynSolValue::FixedArray(items) => items.get(idx)?.clone(),
                    _ => return None,
                };
                // Update current_fields for nested access
                if idx < current_fields.len() {
                    current_fields = current_fields[idx].children.clone();
                } else {
                    current_fields = vec![];
                }
            }
            AccessorSegment::Field(name) => {
                // Named field access - look up position from return_fields
                let (idx, field) =
                    current_fields.iter().enumerate().find(|(_, f)| f.name == name)?;

                current = match &current {
                    DynSolValue::Tuple(items)
                    | DynSolValue::Array(items)
                    | DynSolValue::FixedArray(items) => items.get(idx)?.clone(),
                    _ => return None,
                };
                current_fields = field.children.clone();
            }
        }
    }

    Some(current)
}

#[derive(Debug)]
enum AccessorSegment {
    Index(usize),
    Field(String),
}

/// Parses accessor string into segments.
/// "[0].field[1].nested" -> [Index(0), Field("field"), Index(1), Field("nested")]
fn parse_accessor_segments(accessor: &str) -> Vec<AccessorSegment> {
    let mut segments = Vec::new();
    let mut remaining = accessor.trim();

    while !remaining.is_empty() {
        if remaining.starts_with('[') {
            // Index access
            if let Some(end) = remaining.find(']') {
                if let Ok(idx) = remaining[1..end].parse::<usize>() {
                    segments.push(AccessorSegment::Index(idx));
                }
                remaining = &remaining[end + 1..];
            } else {
                break;
            }
        } else if remaining.starts_with('.') {
            // Field access
            remaining = &remaining[1..]; // skip the dot
                                         // Find end of field name (next . or [ or end)
            let end = remaining.find(['.', '[']).unwrap_or(remaining.len());
            let field_name = &remaining[..end];
            if !field_name.is_empty() {
                segments.push(AccessorSegment::Field(field_name.to_string()));
            }
            remaining = &remaining[end..];
        } else {
            // Unexpected character, stop parsing
            break;
        }
    }

    segments
}

/// Splits comma-separated arguments, respecting quoted strings.
fn split_call_args(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut paren_depth = 0;

    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                current.push(c);
            }
            '(' => {
                paren_depth += 1;
                current.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current.push(c);
            }
            ',' if !in_quotes && paren_depth == 0 => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
}

/// Executes a view call against the blockchain and applies any accessor.
async fn execute_view_call(
    view_call: &ViewCall,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
    provider: &JsonRpcCachedProvider,
    network: &str,
    constants: &Constants,
) -> Option<DynSolValue> {
    use alloy::primitives::keccak256;

    // Resolve contract address - may be a constant, field reference, or literal
    let contract_address: Address = if view_call.contract_address.starts_with("$constant(") {
        // Resolve constant reference
        let resolved = resolve_constants_in_value(&view_call.contract_address, constants, network)?;
        resolved.parse().ok()?
    } else if view_call.contract_address.starts_with('$') {
        let field_name = &view_call.contract_address[1..];
        if field_name == "rindexer_contract_address" {
            tx_metadata.contract_address
        } else {
            let value = resolve_field_path(field_name, log_params)?;
            match value {
                DynSolValue::Address(addr) => addr,
                _ => return None,
            }
        }
    } else {
        view_call.contract_address.parse().ok()?
    };

    // Parse function signature to get types
    // e.g., "balanceOf(address)" -> selector + encode args
    let (func_name, param_types) = parse_function_signature(&view_call.function_sig)?;

    // Build function selector (first 4 bytes of keccak256 of signature)
    let selector = &keccak256(view_call.function_sig.as_bytes())[..4];

    // Encode arguments
    let mut encoded_args = Vec::new();
    for (i, arg_str) in view_call.args.iter().enumerate() {
        let param_type = param_types.get(i)?;
        let value =
            resolve_arg_value(arg_str, log_params, tx_metadata, param_type, constants, network)?;
        encoded_args.push(value);
    }

    // Build calldata: selector + encoded args
    let calldata = if encoded_args.is_empty() {
        Bytes::copy_from_slice(selector)
    } else {
        let encoded = DynSolValue::Tuple(encoded_args).abi_encode_params();
        let mut data = selector.to_vec();
        data.extend(encoded);
        Bytes::from(data)
    };

    // Check cache first (no semaphore needed for cache lookups)
    let cache_key =
        (network.to_string(), contract_address, calldata.clone(), tx_metadata.block_number);
    {
        let cache = VIEW_CALL_CACHE.read().await;
        if let Some(cached) = cache.get(&cache_key) {
            debug!("View call cache hit for {}::{}", contract_address, func_name);
            // Apply accessor to cached result
            return apply_accessor_if_present(cached.clone(), view_call);
        }
    }

    // Acquire semaphore permit to limit concurrent RPC calls
    // This prevents overwhelming the RPC node while maintaining good throughput
    let semaphore = VIEW_CALL_SEMAPHORE.read().await.clone();
    let _permit = semaphore.acquire().await.ok()?;

    // Double-check cache after acquiring permit (another task may have populated it)
    {
        let cache = VIEW_CALL_CACHE.read().await;
        if let Some(cached) = cache.get(&cache_key) {
            return apply_accessor_if_present(cached.clone(), view_call);
        }
    }

    // Execute the call using the provider's eth_call method
    let result_bytes: String =
        match provider.eth_call(contract_address, calldata.clone(), tx_metadata.block_number).await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("View call failed for {}::{}: {}", contract_address, func_name, e);
                return None;
            }
        };

    // Decode the result
    let result_bytes = hex::decode(result_bytes.trim_start_matches("0x")).ok()?;

    // Determine return type - use explicit return_fields if provided, otherwise auto-detect
    let decoded = if !view_call.return_fields.is_empty() {
        let return_type = build_return_type_from_fields(&view_call.return_fields);
        return_type.abi_decode(&result_bytes).ok()?
    } else {
        // Auto-detect the return type from the raw bytes
        try_decode_return_value(&result_bytes)?
    };

    // Cache the full result (before accessor is applied)
    {
        let mut cache = VIEW_CALL_CACHE.write().await;
        cache.insert(cache_key, decoded.clone());
    }

    // Apply accessor if present
    apply_accessor_if_present(decoded, view_call)
}

/// Resolves all $call(...) patterns in an arithmetic expression, replacing them with their values.
/// Returns the modified expression string with calls replaced by their numeric values.
async fn resolve_calls_in_arithmetic_expression(
    expression: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
    provider: &JsonRpcCachedProvider,
    network: &str,
    constants: &Constants,
) -> Option<String> {
    let call_patterns = find_call_patterns(expression);
    if call_patterns.is_empty() {
        return Some(expression.to_string());
    }

    let mut result = expression.to_string();

    // Process calls in reverse order so indices remain valid after replacements
    for (start, end, call_expr) in call_patterns.into_iter().rev() {
        // Parse and execute the view call
        let view_call = parse_view_call(&call_expr)?;
        let call_result =
            execute_view_call(&view_call, log_params, tx_metadata, provider, network, constants)
                .await?;

        // Convert result to string for substitution
        let value_str = match call_result {
            DynSolValue::Uint(val, _) => val.to_string(),
            DynSolValue::Int(val, _) => val.to_string(),
            DynSolValue::Bool(b) => if b { "1" } else { "0" }.to_string(),
            _ => {
                tracing::warn!(
                    "View call in arithmetic returned non-numeric value: {:?}",
                    call_result
                );
                return None;
            }
        };

        // Replace the call expression with the value
        result.replace_range(start..end, &value_str);
    }

    Some(result)
}

/// Applies accessor to a view call result if one is specified.
fn apply_accessor_if_present(value: DynSolValue, view_call: &ViewCall) -> Option<DynSolValue> {
    match &view_call.accessor {
        Some(accessor) => apply_accessor(value, accessor, &view_call.return_fields),
        None => Some(value),
    }
}

/// Builds a DynSolType from parsed return fields.
/// This allows proper ABI decoding when "returns (...)" syntax is used.
fn build_return_type_from_fields(fields: &[ReturnField]) -> DynSolType {
    if fields.len() == 1 && fields[0].children.is_empty() {
        // Single return value - parse type directly
        fields[0].type_str.parse().unwrap_or(DynSolType::Uint(256))
    } else {
        // Multiple return values or nested tuple - build tuple type
        let inner_types: Vec<DynSolType> = fields
            .iter()
            .map(|f| {
                if !f.children.is_empty() {
                    // Nested tuple
                    build_return_type_from_fields(&f.children)
                } else {
                    f.type_str.parse().unwrap_or(DynSolType::Uint(256))
                }
            })
            .collect();
        DynSolType::Tuple(inner_types)
    }
}

/// Parses a function signature like "balanceOf(address)" into (name, param_types).
fn parse_function_signature(sig: &str) -> Option<(String, Vec<DynSolType>)> {
    let open_paren = sig.find('(')?;
    let close_paren = sig.rfind(')')?;

    let name = sig[..open_paren].to_string();
    let params_str = &sig[open_paren + 1..close_paren];

    let param_types: Vec<DynSolType> = if params_str.is_empty() {
        vec![]
    } else {
        params_str.split(',').filter_map(|p| p.trim().parse::<DynSolType>().ok()).collect()
    };

    Some((name, param_types))
}

/// Resolves an argument value from a string (literal, $field reference, or $constant).
fn resolve_arg_value(
    arg_str: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
    expected_type: &DynSolType,
    constants: &Constants,
    network: &str,
) -> Option<DynSolValue> {
    let arg_str = arg_str.trim();

    // Check for constant reference first
    if is_constant_ref(arg_str) {
        let resolved = resolve_constants_in_value(arg_str, constants, network)?;
        return parse_literal_as_type(&resolved, expected_type);
    }

    if let Some(field_name) = arg_str.strip_prefix('$') {
        // Check tx metadata (all prefixed with rindexer_)
        match field_name {
            "rindexer_contract_address" => {
                return Some(DynSolValue::Address(tx_metadata.contract_address))
            }
            "rindexer_block_number" => {
                return Some(DynSolValue::Uint(U256::from(tx_metadata.block_number), 256))
            }
            _ => {}
        }

        // Resolve from log params
        resolve_field_path(field_name, log_params)
    } else {
        // Parse literal value based on expected type
        parse_literal_as_type(arg_str, expected_type)
    }
}

/// Parses a literal string as the expected DynSolType.
fn parse_literal_as_type(value: &str, sol_type: &DynSolType) -> Option<DynSolValue> {
    match sol_type {
        DynSolType::Address => {
            let addr: Address = value.parse().ok()?;
            Some(DynSolValue::Address(addr))
        }
        DynSolType::Uint(bits) => {
            let num: U256 = value.parse().ok()?;
            Some(DynSolValue::Uint(num, *bits))
        }
        DynSolType::Bool => {
            let b = value.to_lowercase() == "true" || value == "1";
            Some(DynSolValue::Bool(b))
        }
        DynSolType::String => Some(DynSolValue::String(value.to_string())),
        DynSolType::Bytes => {
            let bytes = hex::decode(value.trim_start_matches("0x")).ok()?;
            Some(DynSolValue::Bytes(bytes))
        }
        _ => None,
    }
}

/// Try to decode return value bytes with intelligent type detection.
/// Tries multiple ABI decodings and returns the first successful one.
fn try_decode_return_value(bytes: &[u8]) -> Option<DynSolValue> {
    // Empty or too short - can't decode
    if bytes.is_empty() {
        return None;
    }

    // Try to detect ABI-encoded string/bytes (dynamic types)
    // Dynamic types have: [offset (32 bytes)][length (32 bytes)][data...]
    // The offset for a single return value is typically 0x20 (32)
    if bytes.len() >= 64 {
        let offset = U256::from_be_slice(&bytes[0..32]);
        if offset == U256::from(32) && bytes.len() >= 64 {
            let length = U256::from_be_slice(&bytes[32..64]);
            let length_usize = length.to::<usize>();

            // Sanity check: length should be reasonable and data should exist
            if length_usize < 10000 && bytes.len() >= 64 + length_usize {
                // Try decoding as string
                if let Ok(decoded) = DynSolType::String.abi_decode(bytes) {
                    if let DynSolValue::String(s) = &decoded {
                        // Validate it's actually valid UTF-8 text (not random bytes)
                        if s.chars()
                            .all(|c| c.is_ascii_graphic() || c.is_ascii_whitespace() || c == '/')
                        {
                            return Some(decoded);
                        }
                    }
                }

                // Try decoding as bytes
                if let Ok(decoded) = DynSolType::Bytes.abi_decode(bytes) {
                    return Some(decoded);
                }
            }
        }
    }

    // NOTE: We intentionally do NOT auto-detect addresses or bools here because:
    // - Any uint256 value < 2^160 has 12+ leading zeros (same as address encoding)
    // - Values 0 and 1 are common numeric returns (like balanceOf returning 0)
    // - The caller's column_type will guide proper conversion in dyn_sol_value_to_wrapper
    // - When column is bool, uint 0/1 will be converted appropriately

    // Default: decode as uint256
    DynSolType::Uint(256).abi_decode(bytes).ok()
}

/// Async version of extract_value_from_event that supports view calls and constants.
/// Falls back to sync extraction for non-view-call values.
async fn extract_value_from_event_async(
    value_ref: &str,
    log_params: &[LogParam],
    tx_metadata: &TxMetadata,
    column_type: &ColumnType,
    provider: Option<&JsonRpcCachedProvider>,
    network: &str,
    constants: &Constants,
) -> Option<EthereumSqlTypeWrapper> {
    // First resolve any constants in the value (handles $constant(...) anywhere in string)
    let resolved_constants: String;
    let after_constants = if value_ref.contains("$constant(") {
        resolved_constants = resolve_all_constants_in_value(value_ref, constants, network)?;
        resolved_constants.as_str()
    } else {
        value_ref
    };

    // Check for arithmetic expression with $call() patterns
    // e.g., "$amount / (10 ^ $call($asset, \"decimals()\")) * $call($oracle, \"getAssetPrice(address)\", $asset)"
    if is_arithmetic_expression(after_constants) && arithmetic_has_calls(after_constants) {
        let provider = provider?;
        // Resolve all $call() patterns first, then evaluate arithmetic
        let resolved_expr = resolve_calls_in_arithmetic_expression(
            after_constants,
            log_params,
            tx_metadata,
            provider,
            network,
            constants,
        )
        .await?;

        // Now evaluate the arithmetic with calls resolved to values
        let json_data = log_params_to_json(log_params);
        return match evaluate_arithmetic(&resolved_expr, &json_data) {
            Ok(ComputedValue::U256(val)) => match column_type {
                ColumnType::Uint64 => Some(EthereumSqlTypeWrapper::U64BigInt(val.to::<u64>())),
                ColumnType::Uint128 => Some(EthereumSqlTypeWrapper::U256Numeric(val)),
                _ => Some(EthereumSqlTypeWrapper::U256Numeric(val)),
            },
            Ok(ComputedValue::String(s)) => Some(EthereumSqlTypeWrapper::String(s)),
            Err(e) => {
                tracing::debug!(
                    "Arithmetic expression with calls evaluation failed: {}. Expression: {}",
                    e,
                    resolved_expr
                );
                None
            }
        };
    }

    // Check for standalone view call (not in arithmetic)
    if is_view_call(after_constants) {
        let provider = provider?;
        let view_call = parse_view_call(after_constants)?;
        let result =
            execute_view_call(&view_call, log_params, tx_metadata, provider, network, constants)
                .await?;
        return Some(dyn_sol_value_to_wrapper(&result, column_type));
    }

    // Fall back to sync extraction for everything else
    extract_value_from_event(after_constants, log_params, tx_metadata, column_type)
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
                let (idx, nested_param) =
                    components.iter().enumerate().find(|(_, p)| p.name == part)?;

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
                DynSolValue::Array(arr) | DynSolValue::FixedArray(arr) => arr.get(index)?.clone(),
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
                let (idx, nested_param) =
                    current_components.iter().enumerate().find(|(_, p)| p.name == *segment)?;

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
                    ColumnType::Uint128 => Some(EthereumSqlTypeWrapper::U256Numeric(val)),
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

    if let Some(field_name) = value_ref.strip_prefix('$') {
        // Check for built-in transaction metadata fields first (all prefixed with rindexer_)
        match field_name {
            "rindexer_block_number" => {
                // Use U64BigInt for proper BIGINT binary serialization
                return Some(EthereumSqlTypeWrapper::U64BigInt(tx_metadata.block_number));
            }
            "rindexer_block_timestamp" => {
                return tx_metadata.block_timestamp.and_then(|ts| {
                    DateTime::from_timestamp(ts.to::<i64>(), 0)
                        .map(|dt| EthereumSqlTypeWrapper::DateTime(dt.with_timezone(&Utc)))
                });
            }
            "rindexer_tx_hash" => {
                // Store as hex string in CHAR(66) column
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.tx_hash
                )));
            }
            "rindexer_block_hash" => {
                // Store as hex string in CHAR(66) column
                return Some(EthereumSqlTypeWrapper::StringChar(format!(
                    "{:?}",
                    tx_metadata.block_hash
                )));
            }
            "rindexer_contract_address" => {
                return Some(EthereumSqlTypeWrapper::Address(tx_metadata.contract_address));
            }
            "rindexer_log_index" => {
                return Some(EthereumSqlTypeWrapper::U256(tx_metadata.log_index));
            }
            "rindexer_tx_index" => {
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
        (DynSolValue::Address(addr), ColumnType::Address) => EthereumSqlTypeWrapper::Address(*addr),
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
        // Uint to Bool conversion (for when view call returns 0/1 but column is bool)
        (DynSolValue::Uint(val, _), ColumnType::Bool) => {
            EthereumSqlTypeWrapper::Bool(!val.is_zero())
        }
        (DynSolValue::String(s), ColumnType::String) => {
            // Sanitize string: remove null bytes which PostgreSQL doesn't accept in VARCHAR
            let sanitized = s.replace('\0', "");
            EthereumSqlTypeWrapper::String(sanitized)
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
                let json_array: Vec<String> =
                    items.iter().map(|item| format!("{:?}", item)).collect();
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
        (DynSolValue::Int(val, _), ColumnType::Int8) => EthereumSqlTypeWrapper::I8(val.as_i8()),
        (DynSolValue::Int(val, _), ColumnType::Int16) => EthereumSqlTypeWrapper::I16(val.as_i16()),
        (DynSolValue::Int(val, _), ColumnType::Int32) => EthereumSqlTypeWrapper::I32(val.as_i32()),
        (DynSolValue::Int(val, _), ColumnType::Int64) => EthereumSqlTypeWrapper::I64(val.as_i64()),
        // Cross-type conversions: when auto-detected type differs from column type
        // This happens because try_decode_return_value defaults to uint256 for unknown types
        (DynSolValue::Uint(val, _), ColumnType::Int256) => {
            // Reinterpret uint256 as int256 (same byte representation, different sign interpretation)
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I256Numeric(i256)
        }
        (DynSolValue::Uint(val, _), ColumnType::Int128) => {
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I256Numeric(i256)
        }
        (DynSolValue::Uint(val, _), ColumnType::Int64) => {
            // For smaller signed types, convert via i256 to handle potential negative values
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I64(i256.as_i64())
        }
        (DynSolValue::Uint(val, _), ColumnType::Int32) => {
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I32(i256.as_i32())
        }
        (DynSolValue::Uint(val, _), ColumnType::Int16) => {
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I16(i256.as_i16())
        }
        (DynSolValue::Uint(val, _), ColumnType::Int8) => {
            let i256 = alloy::primitives::I256::from_raw(*val);
            EthereumSqlTypeWrapper::I8(i256.as_i8())
        }
        // Address conversion: uint256 values can be converted to addresses (lower 20 bytes)
        (DynSolValue::Uint(val, _), ColumnType::Address) => {
            // Take lower 20 bytes of uint256 as address
            let bytes: [u8; 32] = val.to_be_bytes();
            let addr = Address::from_slice(&bytes[12..32]);
            EthereumSqlTypeWrapper::Address(addr)
        }
        // Fallback conversions - use PostgreSQL-compatible types
        (DynSolValue::Uint(val, _), _) => EthereumSqlTypeWrapper::U256Numeric(*val),
        (DynSolValue::Int(val, _), _) => EthereumSqlTypeWrapper::I256Numeric(*val),
        // Bool to numeric conversions (true=1, false=0)
        (DynSolValue::Bool(b), ColumnType::Uint256 | ColumnType::Uint128) => {
            EthereumSqlTypeWrapper::U256Numeric(if *b { U256::from(1) } else { U256::ZERO })
        }
        (DynSolValue::Bool(b), ColumnType::Uint64) => {
            EthereumSqlTypeWrapper::U64BigInt(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Uint32) => {
            EthereumSqlTypeWrapper::U32(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Uint16) => {
            EthereumSqlTypeWrapper::U16(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Uint8) => {
            EthereumSqlTypeWrapper::U8(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Int256 | ColumnType::Int128) => {
            EthereumSqlTypeWrapper::I256Numeric(if *b {
                alloy::primitives::I256::try_from(1).unwrap()
            } else {
                alloy::primitives::I256::ZERO
            })
        }
        (DynSolValue::Bool(b), ColumnType::Int64) => {
            EthereumSqlTypeWrapper::I64(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Int32) => {
            EthereumSqlTypeWrapper::I32(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Int16) => {
            EthereumSqlTypeWrapper::I16(if *b { 1 } else { 0 })
        }
        (DynSolValue::Bool(b), ColumnType::Int8) => {
            EthereumSqlTypeWrapper::I8(if *b { 1 } else { 0 })
        }
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
                debug!("iterate binding '{}' references non-array field", binding.array_field);
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
/// * `providers` - RPC providers for view calls (keyed by network name)
/// * `constants` - User-defined constants from the manifest (can be network-scoped)
pub async fn process_table_operations(
    tables: &[TableRuntime],
    event_name: &str,
    events_data: &[(Vec<LogParam>, String, TxMetadata)], // (log_params, network, tx_metadata)
    postgres: Option<Arc<PostgresClient>>,
    clickhouse: Option<Arc<ClickhouseClient>>,
    providers: Arc<std::collections::HashMap<String, Arc<crate::provider::JsonRpcCachedProvider>>>,
    constants: &Constants,
) -> Result<(), String> {
    for table_runtime in tables {
        // Find operations for this event
        let event_mapping = table_runtime.table.events.iter().find(|e| e.event == event_name);

        let event_mapping = match event_mapping {
            Some(em) => em,
            None => continue,
        };

        for operation in &event_mapping.operations {
            let mut rows_to_process: Vec<TableRowData> = Vec::new();

            // Check if condition has @table references - push to SQL instead of Rust evaluation
            let (should_filter_in_rust, sql_condition) =
                if let Some(condition_expr) = operation.condition() {
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
                            debug!("Failed to expand iterate bindings for event {}", event_name);
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

                    // Get provider for this network (for view calls)
                    let provider = providers.get(network).map(|p| p.as_ref());

                    // Add where clause columns
                    for (column_name, value_ref) in &operation.where_clause {
                        let column_def =
                            table_runtime.table.columns.iter().find(|c| &c.name == column_name);

                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_value_from_event_async(
                                value_ref,
                                expanded_log_params,
                                tx_metadata,
                                column_def.resolved_type(),
                                provider,
                                network,
                                constants,
                            )
                            .await
                            {
                                columns.insert(column_name.clone(), value);
                            }
                        }
                    }

                    // Add set columns with their values
                    for set_col in &operation.set {
                        let column_def =
                            table_runtime.table.columns.iter().find(|c| c.name == set_col.column);

                        if let Some(column_def) = column_def {
                            if let Some(value) = extract_value_from_event_async(
                                set_col.effective_value(),
                                expanded_log_params,
                                tx_metadata,
                                column_def.resolved_type(),
                                provider,
                                network,
                                constants,
                            )
                            .await
                            {
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
                            EthereumSqlTypeWrapper::StringChar(format!(
                                "{:?}",
                                tx_metadata.tx_hash
                            )),
                        );
                        columns.insert(
                            injected_columns::BLOCK_HASH.to_string(),
                            EthereumSqlTypeWrapper::StringChar(format!(
                                "{:?}",
                                tx_metadata.block_hash
                            )),
                        );
                        columns.insert(
                            injected_columns::CONTRACT_ADDRESS.to_string(),
                            EthereumSqlTypeWrapper::Address(tx_metadata.contract_address),
                        );

                        rows_to_process.push(TableRowData { columns, network: network.clone() });
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
        ColumnType::Address => BatchOperationSqlType::Address,
        // 8-bit and 16-bit integers -> SMALLINT (INT2)
        ColumnType::Uint8 | ColumnType::Uint16 | ColumnType::Int8 | ColumnType::Int16 => {
            BatchOperationSqlType::Smallint
        }
        // 32-bit integers -> INTEGER (INT4)
        ColumnType::Uint32 | ColumnType::Int32 => BatchOperationSqlType::Integer,
        // 64-bit integers
        ColumnType::Uint64 | ColumnType::Int64 => BatchOperationSqlType::Bigint,
        // Large integers -> NUMERIC
        ColumnType::Uint128 | ColumnType::Uint256 | ColumnType::Int128 | ColumnType::Int256 => {
            BatchOperationSqlType::Numeric
        }
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
        OperationType::Insert => BatchOperationType::Insert,
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

    // For Insert operations, don't use Distinct behavior (no deduplication)
    let is_insert = operation.operation_type == OperationType::Insert;

    for row in rows {
        let mut columns: Vec<DynamicColumnDefinition> = Vec::new();

        if !table_def.cross_chain {
            // For Insert, network is just a normal column (no dedup)
            let network_behavior = if is_insert {
                BatchOperationColumnBehavior::Normal
            } else {
                BatchOperationColumnBehavior::Distinct
            };
            columns.push(DynamicColumnDefinition::new(
                "network".to_string(),
                EthereumSqlTypeWrapper::String(row.network.clone()),
                BatchOperationSqlType::Varchar,
                network_behavior,
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
            // For Insert, no columns should be Distinct (no deduplication)
            let is_pk = table_def.is_primary_key_column(&column.name);
            let behavior = if is_insert {
                BatchOperationColumnBehavior::Normal
            } else if is_pk {
                BatchOperationColumnBehavior::Distinct
            } else {
                BatchOperationColumnBehavior::Normal
            };

            // Determine action
            let action = if is_pk && !is_insert {
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
                BatchOperationSqlType::Custom("CHAR(66)"),
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(hash) = row.columns.get(injected_columns::BLOCK_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::BLOCK_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Custom("CHAR(66)"),
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }
        if let Some(addr) = row.columns.get(injected_columns::CONTRACT_ADDRESS) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::CONTRACT_ADDRESS.to_string(),
                addr.clone(),
                BatchOperationSqlType::Address,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        batch_rows.push(columns);
    }

    let op_type = operation_type_to_batch_type(&operation.operation_type);
    // Extract short table name (after the schema prefix)
    let short_table_name = table_name.split('.').next_back().unwrap_or(table_name);
    let event_name = format!("Tables::{}", short_table_name);

    execute_dynamic_batch_operation(
        postgres,
        table_name,
        op_type,
        batch_rows,
        &event_name,
        sql_where,
    )
    .await?;

    let op_label = match operation.operation_type {
        OperationType::Upsert => "UPSERT",
        OperationType::Insert => "INSERT",
        OperationType::Update => "UPDATE",
        OperationType::Delete => "DELETE",
    };

    info!("Tables::{} - {} - {} rows", short_table_name, op_label, rows.len());

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

    // For Insert operations, don't use Distinct behavior (no deduplication)
    let is_insert = operation.operation_type == OperationType::Insert;

    for row in rows {
        let mut columns: Vec<DynamicColumnDefinition> = Vec::new();

        if !table_def.cross_chain {
            // For Insert, network is just a normal column (no dedup)
            let network_behavior = if is_insert {
                BatchOperationColumnBehavior::Normal
            } else {
                BatchOperationColumnBehavior::Distinct
            };
            columns.push(DynamicColumnDefinition::new(
                "network".to_string(),
                EthereumSqlTypeWrapper::String(row.network.clone()),
                BatchOperationSqlType::Varchar,
                network_behavior,
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

            // Determine behavior - primary key columns come from where clauses
            // For Insert, no columns should be Distinct (no deduplication)
            let is_pk = table_def.is_primary_key_column(&column.name);
            let behavior = if is_insert {
                BatchOperationColumnBehavior::Normal
            } else if is_pk {
                BatchOperationColumnBehavior::Distinct
            } else {
                BatchOperationColumnBehavior::Normal
            };

            let action = if is_pk && !is_insert {
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
                BatchOperationSqlType::Custom("CHAR(66)"),
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // block_hash
        if let Some(hash) = row.columns.get(injected_columns::BLOCK_HASH) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::BLOCK_HASH.to_string(),
                hash.clone(),
                BatchOperationSqlType::Custom("CHAR(66)"),
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        // contract_address
        if let Some(addr) = row.columns.get(injected_columns::CONTRACT_ADDRESS) {
            columns.push(DynamicColumnDefinition::new(
                injected_columns::CONTRACT_ADDRESS.to_string(),
                addr.clone(),
                BatchOperationSqlType::Address,
                BatchOperationColumnBehavior::Normal,
                BatchOperationAction::Set,
            ));
        }

        batch_rows.push(columns);
    }

    let op_type = operation_type_to_batch_type(&operation.operation_type);
    // Extract short table name (after the schema prefix)
    let short_table_name = table_name.split('.').next_back().unwrap_or(table_name);
    let event_name = format!("Tables::{}", short_table_name);

    execute_clickhouse_dynamic_batch_operation(
        clickhouse,
        table_name,
        op_type,
        batch_rows,
        &event_name,
    )
    .await?;

    let op_label = match operation.operation_type {
        OperationType::Upsert => "UPSERT",
        OperationType::Insert => "INSERT",
        OperationType::Update => "UPDATE",
        OperationType::Delete => "DELETE",
    };

    info!("Tables::{} - {} - {} rows", short_table_name, op_label, rows.len());

    Ok(())
}

// =============================================================================
// Public helper functions for cron scheduler
// =============================================================================

/// Internal PostgreSQL operation execution - used by cron scheduler.
/// This is a public wrapper around `execute_postgres_operation`.
pub async fn execute_postgres_operation_internal(
    postgres: &PostgresClient,
    table_name: &str,
    table_def: &Table,
    operation: &TableOperation,
    rows: &[TableRowData],
    sql_where: Option<&str>,
) -> Result<(), String> {
    execute_postgres_operation(postgres, table_name, table_def, operation, rows, sql_where).await
}

/// Internal ClickHouse operation execution - used by cron scheduler.
/// This is a public wrapper around `execute_clickhouse_operation`.
pub async fn execute_clickhouse_operation_internal(
    clickhouse: &ClickhouseClient,
    table_name: &str,
    table_def: &Table,
    operation: &TableOperation,
    rows: &[TableRowData],
) -> Result<(), String> {
    execute_clickhouse_operation(clickhouse, table_name, table_def, operation, rows).await
}

/// Execute a view call for cron operations (no event data available).
///
/// This function parses and executes view calls like `$call($contract, "balanceOf(address)", "0x...")`.
/// It's similar to `execute_view_call` but uses contract_address instead of event data.
pub async fn execute_view_call_for_cron(
    value_ref: &str,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    column_type: &ColumnType,
    provider: &JsonRpcCachedProvider,
    network: &str,
) -> Option<EthereumSqlTypeWrapper> {
    // Parse the view call
    let view_call = parse_view_call(value_ref)?;

    // Execute the view call with empty log_params (cron has no event data)
    // We need to modify arg resolution to handle $contract and other cron-specific values
    let result = execute_view_call_for_cron_internal(
        &view_call,
        tx_metadata,
        contract_address,
        provider,
        network,
    )
    .await?;

    Some(dyn_sol_value_to_wrapper(&result, column_type))
}

/// Internal function to execute a view call for cron operations.
async fn execute_view_call_for_cron_internal(
    view_call: &ViewCall,
    tx_metadata: &TxMetadata,
    contract_address: &Address,
    provider: &JsonRpcCachedProvider,
    network: &str,
) -> Option<DynSolValue> {
    use alloy::primitives::keccak256;

    // Resolve contract address
    let resolved_address: Address = if view_call.contract_address.starts_with('$') {
        let field_name = &view_call.contract_address[1..];
        match field_name {
            "contract" | "rindexer_contract_address" => *contract_address,
            _ => {
                warn!("Unknown contract reference in cron view call: {}", field_name);
                return None;
            }
        }
    } else {
        view_call.contract_address.parse().ok()?
    };

    // Parse function signature to get types
    let (func_name, param_types) = parse_function_signature(&view_call.function_sig)?;

    // Build function selector (first 4 bytes of keccak256 of signature)
    let selector = &keccak256(view_call.function_sig.as_bytes())[..4];

    // Encode arguments (for cron, we only support literals and $contract)
    let mut encoded_args = Vec::new();
    for (i, arg_str) in view_call.args.iter().enumerate() {
        let param_type = param_types.get(i)?;
        let value = resolve_cron_arg_value(arg_str.trim(), contract_address, param_type)?;
        encoded_args.push(value);
    }

    // Build calldata: selector + encoded args
    let calldata = if encoded_args.is_empty() {
        Bytes::copy_from_slice(selector)
    } else {
        let encoded = DynSolValue::Tuple(encoded_args).abi_encode_params();
        let mut data = selector.to_vec();
        data.extend(encoded);
        Bytes::from(data)
    };

    // Check cache first
    let cache_key =
        (network.to_string(), resolved_address, calldata.clone(), tx_metadata.block_number);
    {
        let cache = VIEW_CALL_CACHE.read().await;
        if let Some(cached) = cache.get(&cache_key) {
            debug!("View call cache hit for {}::{}", resolved_address, func_name);
            return apply_accessor_if_present(cached.clone(), view_call);
        }
    }

    // Acquire semaphore permit to limit concurrent RPC calls
    let semaphore = VIEW_CALL_SEMAPHORE.read().await.clone();
    let _permit = semaphore.acquire().await.ok()?;

    // Double-check cache after acquiring permit
    {
        let cache = VIEW_CALL_CACHE.read().await;
        if let Some(cached) = cache.get(&cache_key) {
            return apply_accessor_if_present(cached.clone(), view_call);
        }
    }

    // Execute the call
    let result_bytes: String =
        match provider.eth_call(resolved_address, calldata.clone(), tx_metadata.block_number).await
        {
            Ok(r) => r,
            Err(e) => {
                warn!("View call failed for {}::{}: {}", resolved_address, func_name, e);
                return None;
            }
        };

    // Decode the result
    let result_bytes = hex::decode(result_bytes.trim_start_matches("0x")).ok()?;

    // Determine return type - use explicit return_fields if provided, otherwise auto-detect
    let decoded = if !view_call.return_fields.is_empty() {
        let return_type = build_return_type_from_fields(&view_call.return_fields);
        return_type.abi_decode(&result_bytes).ok()?
    } else {
        // Auto-detect the return type from the raw bytes
        try_decode_return_value(&result_bytes)?
    };

    // Cache the result
    {
        let mut cache = VIEW_CALL_CACHE.write().await;
        cache.insert(cache_key, decoded.clone());
    }

    apply_accessor_if_present(decoded, view_call)
}

/// Resolves an argument value for cron operations (no event data).
/// Only supports literals and $contract.
fn resolve_cron_arg_value(
    arg_str: &str,
    contract_address: &Address,
    expected_type: &DynSolType,
) -> Option<DynSolValue> {
    if let Some(field_name) = arg_str.strip_prefix('$') {
        match field_name {
            "contract" | "rindexer_contract_address" => {
                return Some(DynSolValue::Address(*contract_address));
            }
            _ => {
                warn!("Unknown cron argument reference: {}", arg_str);
                return None;
            }
        }
    }

    // Parse literal value
    parse_literal_to_dyn_sol_value(arg_str, expected_type)
}

/// Parse a literal value to a DynSolValue based on expected type.
fn parse_literal_to_dyn_sol_value(value: &str, expected_type: &DynSolType) -> Option<DynSolValue> {
    match expected_type {
        DynSolType::Address => {
            let addr: Address = value.trim_matches('"').parse().ok()?;
            Some(DynSolValue::Address(addr))
        }
        DynSolType::Uint(bits) => {
            let num: U256 = value.parse().ok()?;
            Some(DynSolValue::Uint(num, *bits))
        }
        DynSolType::Int(bits) => {
            let num: alloy::primitives::I256 = value.parse().ok()?;
            Some(DynSolValue::Int(num, *bits))
        }
        DynSolType::Bool => {
            let b: bool = value.parse().ok()?;
            Some(DynSolValue::Bool(b))
        }
        DynSolType::String => Some(DynSolValue::String(value.trim_matches('"').to_string())),
        DynSolType::Bytes => {
            let bytes = hex::decode(value.trim_start_matches("0x")).ok()?;
            Some(DynSolValue::Bytes(bytes))
        }
        DynSolType::FixedBytes(size) => {
            let bytes = hex::decode(value.trim_start_matches("0x")).ok()?;
            if bytes.len() != *size {
                return None;
            }
            Some(DynSolValue::FixedBytes(alloy::primitives::FixedBytes::from_slice(&bytes), *size))
        }
        _ => None,
    }
}

/// Parse a literal value for a column type.
/// This is used by the cron scheduler to convert literal values to EthereumSqlTypeWrapper.
pub fn parse_literal_value_for_column(
    value: &str,
    column_type: &ColumnType,
) -> Option<EthereumSqlTypeWrapper> {
    Some(literal_to_wrapper(value, column_type))
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

        assert!(evaluate_filter("from == 0x0000000000000000000000000000000000000000", &params));
        assert!(!evaluate_filter("from == 0x1111111111111111111111111111111111111111", &params));
    }

    #[test]
    fn test_evaluate_filter_not_equals() {
        let params = vec![LogParam::new(
            "to".to_string(),
            DynSolValue::Address(alloy::primitives::Address::ZERO),
        )];

        assert!(!evaluate_filter("to != 0x0000000000000000000000000000000000000000", &params));
        assert!(evaluate_filter("to != 0x1111111111111111111111111111111111111111", &params));
    }

    #[test]
    fn test_evaluate_filter_complex() {
        let params = vec![
            LogParam::new(
                "from".to_string(),
                DynSolValue::Address(alloy::primitives::Address::ZERO),
            ),
            LogParam::new("value".to_string(), DynSolValue::Uint(U256::from(1000u64), 256)),
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
        assert!(!is_string_template("global")); // No $ at all

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
                DynSolValue::Address("0x1111111111111111111111111111111111111111".parse().unwrap()),
            ),
            LogParam::new(
                "to".to_string(),
                DynSolValue::Address("0x2222222222222222222222222222222222222222".parse().unwrap()),
            ),
            LogParam::new("value".to_string(), DynSolValue::Uint(U256::from(1000u64), 256)),
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

        // With tx metadata (uses rindexer_ prefix)
        let result =
            expand_string_template("Block $rindexer_block_number", &params, &tx_metadata).unwrap();
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
        let params =
            vec![LogParam { name: "transfers".to_string(), value: transfers_array, components }];

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

    #[test]
    fn test_is_view_call() {
        // Valid view call expressions
        assert!(is_view_call("$call(0x1234, \"balanceOf(address)\", $holder)"));
        assert!(is_view_call("$call($contract_address, \"decimals()\")"));
        assert!(is_view_call("$call($token, \"totalSupply()\")"));

        // Not view calls
        assert!(!is_view_call("$from"));
        assert!(!is_view_call("$value"));
        assert!(!is_view_call("call(something)"));
        assert!(!is_view_call("$call("));
        assert!(!is_view_call("$call"));
    }

    #[test]
    fn test_parse_view_call() {
        // Simple call with no args
        let result = parse_view_call("$call(0x1234, \"decimals()\")");
        assert!(result.is_some());
        let vc = result.unwrap();
        assert_eq!(vc.contract_address, "0x1234");
        assert_eq!(vc.function_sig, "decimals()");
        assert!(vc.args.is_empty());

        // Call with one argument
        let result = parse_view_call("$call($contract_address, \"balanceOf(address)\", $holder)");
        assert!(result.is_some());
        let vc = result.unwrap();
        assert_eq!(vc.contract_address, "$contract_address");
        assert_eq!(vc.function_sig, "balanceOf(address)");
        assert_eq!(vc.args, vec!["$holder"]);

        // Call with multiple arguments
        let result =
            parse_view_call("$call(0xABCD, \"allowance(address,address)\", $owner, $spender)");
        assert!(result.is_some());
        let vc = result.unwrap();
        assert_eq!(vc.contract_address, "0xABCD");
        assert_eq!(vc.function_sig, "allowance(address,address)");
        assert_eq!(vc.args, vec!["$owner", "$spender"]);

        // Invalid - missing signature
        let result = parse_view_call("$call(0x1234)");
        assert!(result.is_none());
    }

    #[test]
    fn test_split_call_args() {
        // Simple arguments
        let result = split_call_args("a, b, c");
        assert_eq!(result, vec!["a", " b", " c"]);

        // Quoted strings
        let result = split_call_args("0x1234, \"balanceOf(address)\", $holder");
        assert_eq!(result, vec!["0x1234", " \"balanceOf(address)\"", " $holder"]);

        // Commas inside quotes
        let result = split_call_args("$addr, \"transfer(address,uint256)\", $to, $amount");
        assert_eq!(result, vec!["$addr", " \"transfer(address,uint256)\"", " $to", " $amount"]);

        // Nested parentheses
        let result = split_call_args("foo(1,2), bar");
        assert_eq!(result, vec!["foo(1,2)", " bar"]);
    }

    #[test]
    fn test_parse_function_signature() {
        // No params
        let result = parse_function_signature("decimals()");
        assert!(result.is_some());
        let (name, params) = result.unwrap();
        assert_eq!(name, "decimals");
        assert!(params.is_empty());

        // Single param
        let result = parse_function_signature("balanceOf(address)");
        assert!(result.is_some());
        let (name, params) = result.unwrap();
        assert_eq!(name, "balanceOf");
        assert_eq!(params.len(), 1);

        // Multiple params
        let result = parse_function_signature("transfer(address,uint256)");
        assert!(result.is_some());
        let (name, params) = result.unwrap();
        assert_eq!(name, "transfer");
        assert_eq!(params.len(), 2);

        // Invalid - no parens
        let result = parse_function_signature("invalid");
        assert!(result.is_none());
    }

    #[test]
    fn test_try_decode_return_value() {
        // Empty bytes returns None
        assert!(try_decode_return_value(&[]).is_none());

        // uint256 value (32 bytes, big-endian)
        let mut uint_bytes = [0u8; 32];
        uint_bytes[31] = 42; // value = 42
        let result = try_decode_return_value(&uint_bytes);
        assert!(result.is_some());
        if let Some(DynSolValue::Uint(val, 256)) = result {
            assert_eq!(val, alloy::primitives::U256::from(42));
        } else {
            panic!("Expected Uint(256)");
        }

        // ABI-encoded string "ETH"
        // Format: [offset=32][length=3]["ETH" + padding]
        let string_bytes = hex::decode(
            "0000000000000000000000000000000000000000000000000000000000000020\
             0000000000000000000000000000000000000000000000000000000000000003\
             4554480000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let result = try_decode_return_value(&string_bytes);
        assert!(result.is_some());
        if let Some(DynSolValue::String(s)) = result {
            assert_eq!(s, "ETH");
        } else {
            panic!("Expected String");
        }
    }
}
