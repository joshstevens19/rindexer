use std::{collections::HashMap, sync::Arc};

use alloy::primitives::{B256, U64};
use anyhow::Context;
use tokio_postgres::Transaction as PgTransaction;

use crate::database::clickhouse::client::ClickhouseClient;
use crate::database::postgres::client::PostgresClient;
use crate::event::{
    parse_arithmetic_expression, parse_filter_expression, Accessor, ArithmeticExpr, ConditionLeft,
    VariableSource,
};
use crate::helpers::{camel_to_snake, generate_random_id};
use crate::manifest::contract::{ColumnType, IterateBinding, SetAction};
use crate::metrics::indexing as metrics;
use crate::provider::ChainProvider;

use super::validate_sql_condition;
use super::window::BlockChainWindow;

#[derive(Clone)]
pub struct EventTableInfo {
    pub schema: String,
    pub table_name: String,
    /// Full table path: schema.table_name
    pub full_name: String,
    /// Checkpoint table name in rindexer_internal (without schema prefix)
    pub checkpoint_table: String,
    /// Indexer name for stream-payload metadata (not used in SQL).
    pub indexer_name: String,
    /// Contract name for stream-payload metadata (not used in SQL).
    pub contract_name: String,
    /// Event name for stream-payload metadata (not used in SQL).
    pub event_name: String,
}

impl EventTableInfo {
    pub fn try_new(
        schema: String,
        table_name: String,
        checkpoint_table: String,
        indexer_name: String,
        contract_name: String,
        event_name: String,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&schema, "event table schema")?;
        super::validate_sql_identifier(&table_name, "event table name")?;
        super::validate_sql_identifier(&checkpoint_table, "checkpoint table name")?;
        // indexer/contract/event names are metadata for the stream payload,
        // not used in SQL, so no SQL validation needed.
        let full_name = format!("{}.{}", schema, table_name);
        Ok(Self {
            schema,
            table_name,
            full_name,
            checkpoint_table,
            indexer_name,
            contract_name,
            event_name,
        })
    }
}

/// Per-table summary emitted to downstream consumers in the `__rindexer_reorg`
/// stream payload. Tells consumers which source event tables were invalidated
/// so they can act programmatically.
#[derive(Clone, Debug)]
pub struct AffectedTable {
    pub schema: String,
    pub table_name: String,
    /// TODO(future): per-table counts are not available from the DB layer today;
    /// the total is on `ReorgTaskResult.events_deleted`. Set to 0 until a
    /// cheap per-table tally is added.
    pub rows_deleted: u64,
    pub indexer_name: String,
    pub contract_name: String,
    /// "NativeTransfer" for native-transfer tables.
    pub event_name: String,
}

/// Describes how to reverse one column's accumulation during reorg.
#[derive(Clone, Debug)]
pub struct DerivedColumnRollback {
    /// Column in the derived table (e.g., "balance")
    pub derived_column: String,
    /// Column in the source event table (e.g., "value")
    pub event_column: String,
    /// The forward action that was applied (Add, Subtract, Increment, Decrement)
    pub action: SetAction,
}

impl DerivedColumnRollback {
    pub fn try_new(
        derived_column: String,
        event_column: String,
        action: SetAction,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&derived_column, "derived column")?;
        parse_event_operand(&event_column, "rollback event column")?;
        Ok(Self { derived_column, event_column, action })
    }
}

/// Links a source event table to the derived table for reversal.
#[derive(Clone, Debug)]
pub struct DerivedTableRollbackOp {
    /// Source event table (e.g., "myindexer_mycontract.transfer")
    pub event_table: String,
    /// WHERE clause: (derived_table_col, event_table_col) pairs
    pub where_columns: Vec<(String, String)>,
    /// Columns to reverse
    pub columns: Vec<DerivedColumnRollback>,
    /// Optional SQL condition re-evaluated against event data.
    pub condition: Option<String>,
}

impl DerivedTableRollbackOp {
    pub fn try_new(
        event_table: String,
        where_columns: Vec<(String, String)>,
        columns: Vec<DerivedColumnRollback>,
        condition: Option<String>,
    ) -> anyhow::Result<Self> {
        // event_table is "schema.table" — validate both parts
        if let Some((schema, table)) = event_table.split_once('.') {
            super::validate_sql_identifier(schema, "rollback op event table schema")?;
            super::validate_sql_identifier(table, "rollback op event table name")?;
        } else {
            super::validate_sql_identifier(&event_table, "rollback op event table")?;
        }
        for (dt_col, ev_col) in &where_columns {
            super::validate_sql_identifier(dt_col, "rollback op WHERE derived column")?;
            parse_event_operand(ev_col, "rollback op WHERE event column")?;
        }
        if let Some(cond) = &condition {
            validate_sql_condition(cond)?;
        }
        Ok(Self { event_table, where_columns, columns, condition })
    }
}

/// Optional execution metadata for custom-table rollback operations.
///
/// This sidecar keeps [`DerivedTableRollbackOp`] source-compatible for callers
/// that construct it with struct literals. Operation indexes correspond to the
/// order of `DerivedTableInfo::rollback_ops` for the named table.
#[derive(Clone, Debug, Default)]
pub struct DerivedTableRollbackPlan {
    operations: HashMap<String, HashMap<usize, DerivedTableRollbackMetadata>>,
}

#[derive(Clone, Debug)]
pub(crate) struct DerivedTableRollbackMetadata {
    pub(crate) iterate: Vec<RollbackIterateBinding>,
    pub(crate) source_column_types: HashMap<String, ColumnType>,
    pub(crate) derived_column_types: HashMap<String, ColumnType>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IterateStorage {
    NativeArray,
    PostgresJsonb,
}

#[derive(Clone, Debug)]
pub(crate) struct RollbackIterateBinding {
    pub(crate) array_field: String,
    pub(crate) alias: String,
    storage: IterateStorage,
}

impl DerivedTableRollbackPlan {
    /// Add metadata for one rollback operation.
    /// Tuple-array element types use dotted keys such as `accounts.owner`;
    /// their raw PostgreSQL column is represented as JSONB.
    ///
    /// # Errors
    ///
    /// Returns an error when the table name or iterate bindings are invalid,
    /// or when metadata was already registered for the same operation index.
    pub fn try_add_operation(
        &mut self,
        derived_table: String,
        operation_index: usize,
        bindings: Vec<IterateBinding>,
        mut source_column_types: HashMap<String, ColumnType>,
        derived_column_types: HashMap<String, ColumnType>,
    ) -> anyhow::Result<()> {
        if let Some((schema, table)) = derived_table.split_once('.') {
            super::validate_sql_identifier(schema, "rollback plan derived table schema")?;
            super::validate_sql_identifier(table, "rollback plan derived table name")?;
        } else {
            super::validate_sql_identifier(&derived_table, "rollback plan derived table name")?;
        }

        let mut iterate: Vec<RollbackIterateBinding> = Vec::with_capacity(bindings.len());
        for binding in bindings {
            let binding = IterateBinding {
                array_field: event_field_to_db_column(&binding.array_field),
                alias: event_field_to_db_column(&binding.alias),
            };
            super::validate_sql_identifier(&binding.array_field, "rollback iterate array column")?;
            super::validate_sql_identifier(&binding.alias, "rollback iterate alias")?;
            anyhow::ensure!(
                !iterate.iter().any(|existing| existing.alias == binding.alias),
                "duplicate rollback iterate alias '{}'",
                binding.alias
            );
            let jsonb_prefix = format!("{}.", binding.array_field);
            let jsonb_element_types = source_column_types
                .iter()
                .filter_map(|(field, column_type)| {
                    field
                        .strip_prefix(&jsonb_prefix)
                        .map(|path| (path.to_string(), column_type.clone()))
                })
                .collect::<Vec<_>>();
            let storage = if jsonb_element_types.is_empty() {
                if let Some(ColumnType::Array(inner)) =
                    source_column_types.get(&binding.array_field).cloned()
                {
                    source_column_types.insert(binding.alias.clone(), *inner);
                }
                IterateStorage::NativeArray
            } else {
                for (path, column_type) in jsonb_element_types {
                    source_column_types.insert(format!("{}.{}", binding.alias, path), column_type);
                }
                IterateStorage::PostgresJsonb
            };
            iterate.push(RollbackIterateBinding {
                array_field: binding.array_field,
                alias: binding.alias,
                storage,
            });
        }

        let table_operations = self.operations.entry(derived_table.clone()).or_default();
        anyhow::ensure!(
            !table_operations.contains_key(&operation_index),
            "duplicate rollback metadata for table '{}' operation {}",
            derived_table,
            operation_index
        );
        table_operations.insert(
            operation_index,
            DerivedTableRollbackMetadata { iterate, source_column_types, derived_column_types },
        );
        Ok(())
    }

    pub(crate) fn operation(
        &self,
        derived_table: &str,
        operation_index: usize,
    ) -> Option<&DerivedTableRollbackMetadata> {
        self.operations.get(derived_table)?.get(&operation_index)
    }
}

/// Describes a non-reversible column (Set/Max/Min) that uses the operation journal
/// for recalculation during reorg.
#[derive(Clone, Debug)]
pub struct DerivedColumnJournal {
    /// Column in the derived table (e.g., "max_trade")
    pub derived_column: String,
    /// The action: Set, Max, or Min — determines the recalculation aggregate
    pub action: SetAction,
    /// WHERE clause columns in the derived table (for matching journal where_key)
    pub where_columns: Vec<String>,
}

impl DerivedColumnJournal {
    pub fn try_new(
        derived_column: String,
        action: SetAction,
        where_columns: Vec<String>,
    ) -> anyhow::Result<Self> {
        super::validate_sql_identifier(&derived_column, "journal derived column")?;
        for col in &where_columns {
            super::validate_sql_identifier(col, "journal WHERE column")?;
        }
        Ok(Self { derived_column, action, where_columns })
    }
}

/// Metadata for a derived/custom table needed during reorg rollback.
#[derive(Clone)]
pub struct DerivedTableInfo {
    pub full_table_name: String,
    pub cross_chain: bool,
    /// Reversible operations (Add/Subtract/Increment/Decrement) — snapshot + reverse.
    pub rollback_ops: Vec<DerivedTableRollbackOp>,
    /// Non-reversible columns (Set/Max/Min) — recalculated from operation journal.
    pub journal_columns: Vec<DerivedColumnJournal>,
}

impl DerivedTableInfo {
    pub fn try_new(
        full_table_name: String,
        cross_chain: bool,
        rollback_ops: Vec<DerivedTableRollbackOp>,
        journal_columns: Vec<DerivedColumnJournal>,
    ) -> anyhow::Result<Self> {
        // full_table_name is "schema.table" — validate both parts
        if let Some((schema, table)) = full_table_name.split_once('.') {
            super::validate_sql_identifier(schema, "derived table schema")?;
            super::validate_sql_identifier(table, "derived table name")?;
        } else {
            super::validate_sql_identifier(&full_table_name, "derived table name")?;
        }
        Ok(Self { full_table_name, cross_chain, rollback_ops, journal_columns })
    }
}

pub struct ReorgTask {
    pub network: String,
    pub fork_point: u64,
    pub detection_point: u64,
    pub event_tables: Vec<EventTableInfo>,
    pub derived_tables: Vec<DerivedTableInfo>,
    /// Pre-fetched canonical blocks `(block_number, block_hash, parent_hash)` from
    /// `find_fork_point`, so `execute()` can skip a redundant RPC round-trip.
    pub canonical_blocks: Vec<(u64, B256, B256)>,
}

pub struct ReorgTaskResult {
    pub events_deleted: u64,
    pub duration_secs: f64,
    pub affected_tx_hashes: Vec<String>,
    /// Per-table summary of which source event tables were rolled back.
    /// Derived tables are intentionally NOT included — this is about source
    /// event tables downstream consumers may need to know about.
    pub affected_tables: Vec<AffectedTable>,
}

#[derive(Clone, Copy, PartialEq)]
enum SnapshotBackend {
    Postgres,
    Clickhouse,
}

/// A snapshot temp table name and the info needed to apply the reversal later.
struct ReversalSnapshot {
    backend: SnapshotBackend,
    temp_table: String,
    derived_table: String,
    cross_chain: bool,
    network: String,
    where_columns: Vec<(String, String)>,
    set_ops: Vec<ReversalSetOp>,
}

/// A single reversal SET assignment, kept structured (rather than as a
/// pre-rendered SQL string) so each backend can quote the derived column with
/// its own convention. `derived_column` is reversed by `op_symbol` against the
/// snapshot aggregate exposed under `agg_alias`.
#[derive(Clone)]
struct ReversalSetOp {
    derived_column: String,
    op_symbol: &'static str,
    agg_alias: String,
}

/// Quote a SQL identifier for Postgres (double quotes), escaping any embedded
/// quotes. Required because user-defined event/derived columns can collide with
/// SQL reserved words (e.g. `to`, `from`).
fn quote_pg_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Quote a SQL identifier for ClickHouse (backticks), escaping any embedded
/// backticks. Mirrors `quote_pg_ident` for the ClickHouse backend.
fn quote_ch_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "\\`"))
}

#[derive(Clone, Copy)]
enum SqlDialect {
    Postgres,
    Clickhouse,
}

impl SqlDialect {
    fn quote(self, name: &str) -> String {
        match self {
            Self::Postgres => quote_pg_ident(name),
            Self::Clickhouse => quote_ch_ident(name),
        }
    }

    fn cast(self, expression: String, column_type: &ColumnType) -> String {
        match self {
            // Raw PostgreSQL event tables store 136-256 bit integers as decimal
            // strings, while custom tables use NUMERIC. Reapply the forward
            // custom-table coercion before grouping, comparing, or summing.
            Self::Postgres => {
                format!("CAST({} AS {})", expression, column_type.to_postgres_type())
            }
            // Raw and custom ClickHouse tables share ColumnType's native types.
            Self::Clickhouse => expression,
        }
    }

    fn array_element(self, expression: String, index: usize) -> String {
        let sql_index = index.saturating_add(1);
        match self {
            Self::Postgres => format!("({})[{}]", expression, sql_index),
            Self::Clickhouse => format!("arrayElement({}, {})", expression, sql_index),
        }
    }
}

fn event_field_to_db_column(field: &str) -> String {
    match field {
        "rindexer_block_number" => "block_number".to_string(),
        "rindexer_block_timestamp" => "block_timestamp".to_string(),
        "rindexer_tx_hash" => "tx_hash".to_string(),
        "rindexer_block_hash" => "block_hash".to_string(),
        "rindexer_contract_address" => "contract_address".to_string(),
        "rindexer_log_index" => "log_index".to_string(),
        "rindexer_tx_index" => "tx_index".to_string(),
        _ => field.split('.').map(camel_to_snake).collect::<Vec<_>>().join("_"),
    }
}

fn parse_event_operand<'a>(operand: &'a str, kind: &str) -> anyhow::Result<ConditionLeft<'a>> {
    let expression = parse_arithmetic_expression(operand)
        .map_err(|error| anyhow::anyhow!("invalid {kind} '{operand}': {error}"))?;
    let ArithmeticExpr::Variable(variable) = expression else {
        anyhow::bail!("{kind} '{operand}' must be a single event-field reference");
    };
    anyhow::ensure!(
        variable.source() == VariableSource::Event,
        "{kind} '{operand}' must reference event data, not table state"
    );
    Ok(variable)
}

struct ReversalSource<'a> {
    dialect: SqlDialect,
    iterate: &'a [RollbackIterateBinding],
    source_column_types: &'a HashMap<String, ColumnType>,
}

impl ReversalSource<'_> {
    fn column(&self, field: &str) -> String {
        let column = event_field_to_db_column(field);
        if let Some(index) = self.iterate.iter().position(|binding| binding.alias == column) {
            match self.dialect {
                SqlDialect::Postgres => {
                    format!("_rindexer_iter.{}", self.dialect.quote(&column))
                }
                SqlDialect::Clickhouse => {
                    format!("tupleElement(_rindexer_iter, {})", index + 1)
                }
            }
        } else {
            format!("_rindexer_event.{}", self.dialect.quote(&column))
        }
    }

    fn variable_expression(&self, variable: &ConditionLeft<'_>) -> String {
        let base = event_field_to_db_column(variable.base_name());
        if let Some(binding) = self.iterate.iter().find(|binding| binding.alias == base) {
            return self.iterate_variable_expression(variable, binding);
        }

        let mut field_parts = vec![variable.base_name()];
        let mut accessors = variable.accessors().iter().peekable();

        // Non-array tuple fields are flattened in raw event tables, so
        // `$data.amount` addresses the `data_amount` column directly.
        while let Some(Accessor::Key(key)) = accessors.peek() {
            field_parts.push(key);
            accessors.next();
        }

        let field = field_parts.join(".");
        let db_column = event_field_to_db_column(&field);
        let mut expression = self.column(&field);
        let mut column_type = self
            .source_column_types
            .get(&db_column)
            .cloned()
            .or_else(|| ColumnType::from_tx_metadata_field(variable.base_name()));

        for accessor in accessors {
            match accessor {
                Accessor::Index(index) => {
                    match column_type {
                        Some(ColumnType::Array(inner)) => {
                            expression = self.dialect.array_element(expression, *index);
                            column_type = Some(*inner);
                        }
                        _ => {
                            // Tuple arrays are JSONB in PostgreSQL and use
                            // zero-based JSON indexing rather than one-based
                            // native SQL-array indexing.
                            expression = match self.dialect {
                                SqlDialect::Postgres => format!("({} -> {})", expression, index),
                                SqlDialect::Clickhouse => expression,
                            };
                            column_type = None;
                        }
                    }
                }
                Accessor::Key(key) => {
                    // Tuple arrays are JSONB in PostgreSQL and unsupported in
                    // ClickHouse raw storage. Preserve their remaining path as
                    // a JSON lookup when encountered after an array index.
                    expression = match self.dialect {
                        SqlDialect::Postgres => {
                            format!("({} ->> '{}')", expression, key.replace('\'', "''"))
                        }
                        SqlDialect::Clickhouse => {
                            format!("JSONExtractRaw({}, '{}')", expression, key.replace('\'', "''"))
                        }
                    };
                    column_type = None;
                }
            }
        }

        match column_type {
            Some(column_type) => self.dialect.cast(expression, &column_type),
            None => expression,
        }
    }

    fn iterate_variable_expression(
        &self,
        variable: &ConditionLeft<'_>,
        binding: &RollbackIterateBinding,
    ) -> String {
        let mut expression = self.column(&binding.alias);
        let mut column_type = self.source_column_types.get(&binding.alias).cloned();

        if binding.storage == IterateStorage::PostgresJsonb {
            let mut field = binding.alias.clone();
            let path = variable
                .accessors()
                .iter()
                .map(|accessor| match accessor {
                    Accessor::Key(key) => {
                        field.push('.');
                        field.push_str(key);
                        column_type = self.source_column_types.get(&field).cloned();
                        key.to_string()
                    }
                    Accessor::Index(index) => {
                        if let Some(ColumnType::Array(inner)) = column_type.take() {
                            column_type = Some(*inner);
                        }
                        index.to_string()
                    }
                })
                .collect::<Vec<_>>();
            if !path.is_empty() {
                let path = path
                    .iter()
                    .map(|part| format!("'{}'", part.replace('\'', "''")))
                    .collect::<Vec<_>>()
                    .join(", ");
                expression = match self.dialect {
                    SqlDialect::Postgres => {
                        format!("jsonb_extract_path_text({}, {})", expression, path)
                    }
                    SqlDialect::Clickhouse => expression,
                };
            }
        } else {
            for accessor in variable.accessors() {
                if let Accessor::Index(index) = accessor {
                    if let Some(ColumnType::Array(inner)) = column_type.take() {
                        expression = self.dialect.array_element(expression, *index);
                        column_type = Some(*inner);
                    }
                }
            }
        }

        match column_type {
            Some(column_type) => self.dialect.cast(expression, &column_type),
            None => expression,
        }
    }

    fn operand(&self, operand: &str) -> anyhow::Result<String> {
        let variable = parse_event_operand(operand, "rollback operand")?;
        Ok(self.variable_expression(&variable))
    }

    fn iterate_length_mismatch(&self) -> Option<String> {
        let (first, rest) = self.iterate.split_first()?;
        if rest.is_empty() {
            return None;
        }

        let length = |binding: &RollbackIterateBinding| {
            let column = format!("_rindexer_event.{}", self.dialect.quote(&binding.array_field));
            match self.dialect {
                SqlDialect::Postgres => match binding.storage {
                    IterateStorage::NativeArray => {
                        format!("COALESCE(cardinality({}), -1)", column)
                    }
                    IterateStorage::PostgresJsonb => {
                        format!("COALESCE(jsonb_array_length({}), -1)", column)
                    }
                },
                SqlDialect::Clickhouse => format!("length({})", column),
            }
        };
        let first_length = length(first);
        Some(
            rest.iter()
                .map(|binding| format!("{} <> {}", first_length, length(binding)))
                .collect::<Vec<_>>()
                .join(" OR "),
        )
    }

    fn from(&self, event_table: &str) -> String {
        if self.iterate.is_empty() {
            return format!("{} AS _rindexer_event", event_table);
        }

        match self.dialect {
            SqlDialect::Postgres => {
                let iterators = self
                    .iterate
                    .iter()
                    .map(|binding| {
                        let column =
                            format!("_rindexer_event.{}", self.dialect.quote(&binding.array_field));
                        match binding.storage {
                            IterateStorage::NativeArray => format!("unnest({})", column),
                            IterateStorage::PostgresJsonb => {
                                format!("jsonb_array_elements({})", column)
                            }
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let aliases = self
                    .iterate
                    .iter()
                    .map(|binding| self.dialect.quote(&binding.alias))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{} AS _rindexer_event CROSS JOIN LATERAL ROWS FROM ({}) AS _rindexer_iter({})",
                    event_table, iterators, aliases
                )
            }
            SqlDialect::Clickhouse => {
                let arrays = self
                    .iterate
                    .iter()
                    .map(|binding| {
                        format!("_rindexer_event.{}", self.dialect.quote(&binding.array_field))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "{} AS _rindexer_event ARRAY JOIN arrayZip({}) AS _rindexer_iter",
                    event_table, arrays
                )
            }
        }
    }
}

fn render_reorg_condition(condition: &str, source: &ReversalSource<'_>) -> anyhow::Result<String> {
    let expression = match parse_filter_expression(condition) {
        Ok(expression) => expression,
        Err(_) => {
            validate_sql_condition(condition)?;
            return Ok(condition.to_string());
        }
    };
    expression
        .to_sql_event_condition(|variable| source.variable_expression(variable))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "rollback condition '{condition}' references table state, which cannot be reconstructed from raw events"
            )
        })
}

impl ReorgTask {
    /// Returns ` AND network = '<network>'` when not cross-chain, empty string otherwise.
    fn network_filter(&self, cross_chain: bool) -> String {
        if cross_chain {
            String::new()
        } else {
            format!(" AND network = '{}'", self.network)
        }
    }

    async fn validate_iterate_lengths(
        &self,
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
        rollback_plan: &DerivedTableRollbackPlan,
    ) -> anyhow::Result<()> {
        for dt in &self.derived_tables {
            for (op_index, op) in dt.rollback_ops.iter().enumerate() {
                let Some(metadata) = rollback_plan.operation(&dt.full_table_name, op_index) else {
                    continue;
                };
                for dialect in [SqlDialect::Postgres, SqlDialect::Clickhouse] {
                    let source = ReversalSource {
                        dialect,
                        iterate: &metadata.iterate,
                        source_column_types: &metadata.source_column_types,
                    };
                    let Some(mismatch) = source.iterate_length_mismatch() else {
                        continue;
                    };
                    let query = format!(
                        "SELECT count(*) FROM {} AS _rindexer_event WHERE _rindexer_event.{} >= {} AND _rindexer_event.{} <= {} AND _rindexer_event.{} = '{}' AND ({})",
                        op.event_table,
                        dialect.quote("block_number"),
                        self.fork_point,
                        dialect.quote("block_number"),
                        self.detection_point,
                        dialect.quote("network"),
                        self.network,
                        mismatch,
                    );

                    let mismatch_count = match (dialect, pg, ch) {
                        (SqlDialect::Postgres, Some(pg), _) => {
                            pg.query_one(&query, &[])
                                .await
                                .context("failed to validate PostgreSQL rollback iterate lengths")?
                                .get::<_, i64>(0) as u64
                        }
                        (SqlDialect::Clickhouse, _, Some(ch)) => ch
                            .query_one::<u64>(&query)
                            .await
                            .context("failed to validate ClickHouse rollback iterate lengths")?,
                        _ => continue,
                    };
                    anyhow::ensure!(
                        mismatch_count == 0,
                        "rollback iterate arrays have unequal lengths in {} between blocks {} and {}; aborting before event deletion",
                        op.event_table,
                        self.fork_point,
                        self.detection_point,
                    );
                }
            }
        }
        Ok(())
    }

    /// Phase 1: Before event deletion, snapshot aggregated event data into temp tables.
    /// Returns the snapshots needed for phase 2.
    /// Fails the entire reorg task if any snapshot cannot be created — this prevents
    /// event deletion from proceeding without proper reversal data.
    async fn snapshot_for_reversal(
        &self,
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
        rollback_plan: &DerivedTableRollbackPlan,
        attempt_id: &str,
    ) -> anyhow::Result<Vec<ReversalSnapshot>> {
        self.validate_iterate_lengths(pg, ch, rollback_plan).await?;

        let mut snapshots = Vec::new();
        let create_result =
            self.create_reversal_snapshots(pg, ch, rollback_plan, attempt_id, &mut snapshots).await;
        if let Err(error) = create_result {
            Self::cleanup_reversal_snapshots(&snapshots, pg, ch).await;
            return Err(error);
        }
        Ok(snapshots)
    }

    async fn create_reversal_snapshots(
        &self,
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
        rollback_plan: &DerivedTableRollbackPlan,
        attempt_id: &str,
        snapshots: &mut Vec<ReversalSnapshot>,
    ) -> anyhow::Result<()> {
        let mut snap_idx = 0usize;
        let empty_column_types = HashMap::new();

        for dt in &self.derived_tables {
            for (op_index, op) in dt.rollback_ops.iter().enumerate() {
                let metadata = rollback_plan.operation(&dt.full_table_name, op_index);
                let iterate = metadata.map_or(&[][..], |metadata| metadata.iterate.as_slice());
                let source_column_types = metadata
                    .map(|metadata| &metadata.source_column_types)
                    .unwrap_or(&empty_column_types);
                let derived_column_types = metadata
                    .map(|metadata| &metadata.derived_column_types)
                    .unwrap_or(&empty_column_types);

                // (is_counter, source event column, derived column, agg alias) per reversible
                // column. The SELECT aggregate is assembled per-backend so each
                // can quote identifiers with its own convention.
                let mut agg_specs: Vec<(bool, String, String, String)> =
                    Vec::with_capacity(op.columns.len());
                let mut set_ops: Vec<ReversalSetOp> = Vec::new();

                for (col_idx, col) in op.columns.iter().enumerate() {
                    let Some(reversed) = col.action.reverse() else {
                        tracing::warn!(
                            table = %dt.full_table_name,
                            column = %col.derived_column,
                            action = ?col.action,
                            "Non-reversible action — skipping column during reorg rollback"
                        );
                        continue;
                    };

                    let op_symbol = match reversed {
                        SetAction::Add | SetAction::Increment => "+",
                        SetAction::Subtract | SetAction::Decrement => "-",
                        other => anyhow::bail!(
                            "unexpected reversed action {:?} for column {}",
                            other,
                            col.derived_column,
                        ),
                    };

                    // Aggregate aliases are synthesized internal identifiers — never
                    // derived from user column names — so they are always valid SQL
                    // identifiers and keep user-controlled text out of alias positions.
                    let agg_alias = format!("_rindexer_agg_{}", col_idx);
                    agg_specs.push((
                        col.action.is_counter_action(),
                        col.event_column.clone(),
                        col.derived_column.clone(),
                        agg_alias.clone(),
                    ));
                    set_ops.push(ReversalSetOp {
                        derived_column: col.derived_column.clone(),
                        op_symbol,
                        agg_alias,
                    });
                }

                if set_ops.is_empty() || agg_specs.is_empty() {
                    continue;
                }

                let snapshot_where_columns = op
                    .where_columns
                    .iter()
                    .enumerate()
                    .map(|(index, (derived_column, _))| {
                        (derived_column.clone(), format!("_rindexer_where_{}", index))
                    })
                    .collect::<Vec<_>>();

                let temp_base = format!("_rindexer_reorg_snap_{}_{}", attempt_id, snap_idx);
                snap_idx += 1;

                // Build the SELECT per-backend. Group/where and aggregate-source
                // columns are user-controlled identifiers that may collide with SQL
                // reserved words (e.g. `to`, `from`), so each backend quotes them with
                // its own convention (Postgres double quotes, ClickHouse backticks).
                let build_select = |dialect: SqlDialect| -> anyhow::Result<String> {
                    let source = ReversalSource { dialect, iterate, source_column_types };
                    let group_expressions = op
                        .where_columns
                        .iter()
                        .map(|(derived_column, event_column)| {
                            let expression = source.operand(event_column)?;
                            Ok(match derived_column_types.get(derived_column) {
                                Some(column_type) => dialect.cast(expression, column_type),
                                None => expression,
                            })
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let group = group_expressions.join(", ");
                    let mut group_select = snapshot_where_columns
                        .iter()
                        .zip(&group_expressions)
                        .map(|((_, snapshot_column), expression)| {
                            format!("{} AS {}", expression, dialect.quote(snapshot_column))
                        })
                        .collect::<Vec<_>>();
                    if matches!(dialect, SqlDialect::Clickhouse) {
                        // Older ClickHouse StorageJoin implementations reject
                        // composite and UInt256 keys. A single serialized tuple
                        // supports the full range of custom-table key types.
                        let key = if group.is_empty() {
                            "''".to_string()
                        } else {
                            format!("toJSONString(tuple({}))", group)
                        };
                        group_select.push(format!("{} AS _rindexer_key", key));
                    }
                    let group_select = group_select.join(", ");
                    let aggs = agg_specs
                        .iter()
                        .map(|(is_counter, event_column, derived_column, alias)| {
                            if *is_counter {
                                Ok(format!("COUNT(*) AS {}", alias))
                            } else {
                                let expression = source.operand(event_column)?;
                                let expression = match derived_column_types.get(derived_column) {
                                    Some(column_type) => dialect.cast(expression, column_type),
                                    None => expression,
                                };
                                Ok(format!("SUM({}) AS {}", expression, alias))
                            }
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?
                        .join(", ");
                    // The source event table is always network-scoped, even when
                    // the derived table combines contributions across networks.
                    let network_filter = format!(
                        " AND _rindexer_event.{} = '{}'",
                        dialect.quote("network"),
                        self.network
                    );
                    let condition_filter = match &op.condition {
                        Some(condition) => {
                            let condition = render_reorg_condition(condition, &source)?;
                            format!(" AND ({})", condition)
                        }
                        None => String::new(),
                    };
                    let select = if group_select.is_empty() {
                        aggs
                    } else {
                        format!("{}, {}", group_select, aggs)
                    };
                    let group_by = if group.is_empty() {
                        " HAVING COUNT(*) > 0".to_string()
                    } else {
                        format!(" GROUP BY {}", group)
                    };
                    Ok(format!(
                        "SELECT {} FROM {} WHERE _rindexer_event.{} >= {} AND _rindexer_event.{} <= {}{}{}{}",
                        select,
                        source.from(&op.event_table),
                        dialect.quote("block_number"),
                        self.fork_point,
                        dialect.quote("block_number"),
                        self.detection_point,
                        network_filter,
                        condition_filter,
                        group_by,
                    ))
                };

                if let Some(pg) = pg {
                    let pg_temp = format!("{}_pg", temp_base);
                    let pg_create = format!(
                        "CREATE TEMP TABLE {} ON COMMIT DROP AS {}",
                        pg_temp,
                        build_select(SqlDialect::Postgres)?
                    );
                    snapshots.push(ReversalSnapshot {
                        backend: SnapshotBackend::Postgres,
                        temp_table: pg_temp.clone(),
                        derived_table: dt.full_table_name.clone(),
                        cross_chain: dt.cross_chain,
                        network: self.network.clone(),
                        where_columns: snapshot_where_columns.clone(),
                        set_ops: set_ops.clone(),
                    });
                    pg.batch_execute(&pg_create).await.with_context(|| {
                        format!(
                            "Failed to create PG reorg reversal snapshot for {}",
                            dt.full_table_name
                        )
                    })?;
                    tracing::debug!(temp_table = %pg_temp, "Created PG reorg reversal snapshot");
                }

                if let Some(ch) = ch {
                    let ch_temp = format!("rindexer_internal.{}_ch", temp_base);
                    // Join engine (ANY/LEFT, keyed by the snapshot-side where
                    // columns) so the reversal UPDATE can look up per-row
                    // aggregates via joinGet(). ClickHouse mutations don't
                    // support correlated subqueries the way Postgres does, so
                    // we cannot use `(SELECT ... WHERE snap.k = dt.k LIMIT 1)`.
                    let ch_create = format!(
                        "CREATE TABLE {} ENGINE = Join(ANY, LEFT, _rindexer_key) AS {}",
                        ch_temp,
                        build_select(SqlDialect::Clickhouse)?,
                    );
                    snapshots.push(ReversalSnapshot {
                        backend: SnapshotBackend::Clickhouse,
                        temp_table: ch_temp.clone(),
                        derived_table: dt.full_table_name.clone(),
                        cross_chain: dt.cross_chain,
                        network: self.network.clone(),
                        where_columns: snapshot_where_columns.clone(),
                        set_ops: set_ops.clone(),
                    });
                    ch.execute(&ch_create).await.with_context(|| {
                        format!(
                            "Failed to create CH reorg reversal snapshot for {}",
                            dt.full_table_name
                        )
                    })?;
                    tracing::debug!(temp_table = %ch_temp, "Created CH reorg reversal snapshot");
                }
            }
        }
        Ok(())
    }

    /// Phase 2: After event deletion, apply reverse UPDATEs from snapshots.
    async fn apply_reversal_from_snapshots(
        snapshots: &[ReversalSnapshot],
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> anyhow::Result<()> {
        for snap in snapshots {
            let where_join: Vec<String> = snap
                .where_columns
                .iter()
                .map(|(dt_col, ev_col)| {
                    format!("dt.{} = snap.{}", quote_pg_ident(dt_col), quote_pg_ident(ev_col))
                })
                .collect();

            match snap.backend {
                SnapshotBackend::Postgres => {
                    let Some(pg) = pg else { continue };
                    let pg_set_clauses: Vec<String> = snap
                        .set_ops
                        .iter()
                        .map(|s| {
                            let col = quote_pg_ident(&s.derived_column);
                            format!("{} = dt.{} {} snap.{}", col, col, s.op_symbol, s.agg_alias)
                        })
                        .collect();
                    let mut pg_scope = where_join.clone();
                    if !snap.cross_chain {
                        pg_scope.push(format!("dt.network = '{}'", snap.network));
                    }
                    let pg_scope = if pg_scope.is_empty() {
                        "TRUE".to_string()
                    } else {
                        pg_scope.join(" AND ")
                    };
                    let update_sql = format!(
                        "UPDATE {} AS dt SET {} FROM {} AS snap WHERE {}",
                        snap.derived_table,
                        pg_set_clauses.join(", "),
                        snap.temp_table,
                        pg_scope,
                    );
                    pg.batch_execute(&update_sql).await.with_context(|| {
                        format!(
                            "PostgreSQL: failed to reverse accumulative ops for {}",
                            snap.derived_table
                        )
                    })?;
                    tracing::info!(
                        table = %snap.derived_table,
                        "PostgreSQL: reversed accumulative ops"
                    );
                }
                SnapshotBackend::Clickhouse => {
                    let Some(ch) = ch else { continue };

                    // ClickHouse ALTER TABLE ... UPDATE with per-row aggregate lookups
                    // against a Join-engine snapshot table. joinGet() is the mutation-
                    // safe equivalent of PG's correlated subquery.
                    let dt_keys = snap
                        .where_columns
                        .iter()
                        .map(|(dt_col, _)| quote_ch_ident(dt_col))
                        .collect::<Vec<_>>()
                        .join(", ");
                    let join_get_key = if dt_keys.is_empty() {
                        "''".to_string()
                    } else {
                        format!("toJSONString(tuple({}))", dt_keys)
                    };

                    // Build CH set clauses directly from the structured ops: the
                    // per-row aggregate is looked up via joinGet() against the
                    // Join-engine snapshot, keyed by the derived-table where columns.
                    let ch_set_clauses: Vec<String> = snap
                        .set_ops
                        .iter()
                        .map(|s| {
                            let col = quote_ch_ident(&s.derived_column);
                            format!(
                                "{} = {} {} joinGet('{}', '{}', {})",
                                col, col, s.op_symbol, snap.temp_table, s.agg_alias, join_get_key
                            )
                        })
                        .collect();

                    let ch_network = if snap.cross_chain {
                        "1 = 1".to_string()
                    } else {
                        format!("network = '{}'", snap.network)
                    };

                    // Only update rows that have a matching entry in the snapshot
                    let ch_scope = if snap.where_columns.is_empty() {
                        ch_network.clone()
                    } else {
                        format!(
                            "{} AND {} IN (SELECT _rindexer_key FROM {})",
                            ch_network, join_get_key, snap.temp_table
                        )
                    };

                    let ch_update = format!(
                        "ALTER TABLE {} UPDATE {} WHERE {} SETTINGS mutations_sync = 1",
                        snap.derived_table,
                        ch_set_clauses.join(", "),
                        ch_scope,
                    );

                    ch.execute(&ch_update).await.with_context(|| {
                        format!(
                            "ClickHouse: failed to reverse accumulative ops for {}",
                            snap.derived_table
                        )
                    })?;
                    tracing::info!(
                        table = %snap.derived_table,
                        "ClickHouse: reversed accumulative ops"
                    );
                }
            }
        }
        Ok(())
    }

    async fn cleanup_reversal_snapshots(
        snapshots: &[ReversalSnapshot],
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) {
        for snap in snapshots.iter().rev() {
            let result = match snap.backend {
                SnapshotBackend::Postgres => match pg {
                    Some(pg) => pg
                        .batch_execute(&format!("DROP TABLE IF EXISTS {}", snap.temp_table))
                        .await
                        .map_err(anyhow::Error::from),
                    None => continue,
                },
                SnapshotBackend::Clickhouse => match ch {
                    Some(ch) => ch
                        .execute(&format!("DROP TABLE IF EXISTS {}", snap.temp_table))
                        .await
                        .map_err(anyhow::Error::from),
                    None => continue,
                },
            };
            if let Err(error) = result {
                tracing::warn!(
                    temp_table = %snap.temp_table,
                    %error,
                    "Failed to clean up reorg reversal snapshot"
                );
            }
        }
    }

    /// Recalculate non-reversible columns (Set/Max/Min) from the operation journal.
    /// Deletes journal entries in the reorg range, then recalculates from remaining entries.
    async fn recalculate_from_journal(
        &self,
        pg: Option<&PgTransaction<'_>>,
        ch: Option<&Arc<ClickhouseClient>>,
    ) -> anyhow::Result<()> {
        for dt in &self.derived_tables {
            if dt.journal_columns.is_empty() {
                continue;
            }

            let network_filter = self.network_filter(dt.cross_chain);

            // Delete journal entries in the reorg range
            let pg_delete = format!(
                "DELETE FROM rindexer_internal.derived_op_log \
                 WHERE derived_table = '{}' AND block_number >= {}{}",
                dt.full_table_name, self.fork_point, network_filter,
            );
            let ch_delete = format!(
                "ALTER TABLE rindexer_internal.derived_op_log DELETE \
                 WHERE derived_table = '{}' AND block_number >= {}{} SETTINGS mutations_sync = 1",
                dt.full_table_name, self.fork_point, network_filter,
            );

            if let Some(pg) = pg {
                pg.batch_execute(&pg_delete).await.with_context(|| {
                    format!(
                        "PG: failed to delete journal entries for reorg range in {}",
                        dt.full_table_name
                    )
                })?;
            }
            if let Some(ch) = ch {
                ch.execute(&ch_delete).await.with_context(|| {
                    format!(
                        "CH: failed to delete journal entries for reorg range in {}",
                        dt.full_table_name
                    )
                })?;
            }

            // Recalculate each non-reversible column from remaining journal entries
            for jc in &dt.journal_columns {
                let network_join = if dt.cross_chain {
                    String::new()
                } else {
                    format!(" AND dt.network = '{}'", self.network)
                };

                // --- Postgres recalculation ---
                if let Some(pg) = pg {
                    let update_sql = if matches!(jc.action, SetAction::Set) {
                        format!(
                            "UPDATE {} AS dt SET {} = sub.value \
                             FROM ( \
                                 SELECT DISTINCT ON (where_key) where_key, value \
                                 FROM rindexer_internal.derived_op_log \
                                 WHERE derived_table = '{}' AND column_name = '{}'{} \
                                 ORDER BY where_key, block_number DESC, tx_index DESC, log_index DESC \
                             ) sub \
                             WHERE {} {}",
                            dt.full_table_name, jc.derived_column,
                            dt.full_table_name, jc.derived_column, network_filter,
                            Self::journal_where_key_join(&jc.where_columns, "dt", "sub"),
                            network_join,
                        )
                    } else {
                        let agg_fn = match jc.action {
                            SetAction::Max => "MAX(value)",
                            SetAction::Min => "MIN(value)",
                            _ => continue,
                        };
                        format!(
                            "UPDATE {} AS dt SET {} = sub.recalc \
                             FROM ( \
                                 SELECT where_key, {}::NUMERIC AS recalc \
                                 FROM rindexer_internal.derived_op_log \
                                 WHERE derived_table = '{}' AND column_name = '{}'{} \
                                 GROUP BY where_key \
                             ) sub \
                             WHERE {} {}",
                            dt.full_table_name,
                            jc.derived_column,
                            agg_fn,
                            dt.full_table_name,
                            jc.derived_column,
                            network_filter,
                            Self::journal_where_key_join(&jc.where_columns, "dt", "sub"),
                            network_join,
                        )
                    };

                    pg.batch_execute(&update_sql).await.with_context(|| {
                        format!(
                            "PG: failed to recalculate journal column {} in {}",
                            jc.derived_column, dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        table = %dt.full_table_name,
                        column = %jc.derived_column,
                        "PG: recalculated non-reversible column from journal"
                    );
                }

                // --- ClickHouse recalculation ---
                if let Some(ch) = ch {
                    let ch_network = if dt.cross_chain {
                        "1 = 1".to_string()
                    } else {
                        format!("network = '{}'", self.network)
                    };

                    let ch_where_key_expr = Self::journal_where_key_concat_ch(&jc.where_columns);

                    let ch_subquery = if matches!(jc.action, SetAction::Set) {
                        format!(
                            "(SELECT value FROM rindexer_internal.derived_op_log \
                             WHERE derived_table = '{}' AND column_name = '{}' \
                             AND where_key = {} {} \
                             ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT 1)",
                            dt.full_table_name,
                            jc.derived_column,
                            ch_where_key_expr,
                            network_filter,
                        )
                    } else {
                        let agg_fn = match jc.action {
                            SetAction::Max => "max(value)",
                            SetAction::Min => "min(value)",
                            _ => continue,
                        };
                        format!(
                            "(SELECT {} FROM rindexer_internal.derived_op_log \
                             WHERE derived_table = '{}' AND column_name = '{}' \
                             AND where_key = {}{})",
                            agg_fn,
                            dt.full_table_name,
                            jc.derived_column,
                            ch_where_key_expr,
                            network_filter,
                        )
                    };

                    let ch_update = format!(
                        "ALTER TABLE {} UPDATE {} = {} WHERE {} SETTINGS mutations_sync = 1",
                        dt.full_table_name, jc.derived_column, ch_subquery, ch_network,
                    );

                    ch.execute(&ch_update).await.with_context(|| {
                        format!(
                            "CH: failed to recalculate journal column {} in {}",
                            jc.derived_column, dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        table = %dt.full_table_name,
                        column = %jc.derived_column,
                        "CH: recalculated non-reversible column from journal"
                    );
                }
            }
        }
        Ok(())
    }

    /// Build a WHERE clause joining derived table rows to journal where_key.
    /// The journal stores where_key as "col1=val1,col2=val2", so we match using
    /// string concatenation on the derived table side.
    fn journal_where_key_join(where_columns: &[String], dt_alias: &str, sub_alias: &str) -> String {
        if where_columns.is_empty() {
            return format!("{}.network = {}.where_key", dt_alias, sub_alias);
        }

        let concat_parts: Vec<String> = where_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == 0 {
                    format!("'{}=' || {}.{}::TEXT", col, dt_alias, col)
                } else {
                    format!("',{}=' || {}.{}::TEXT", col, dt_alias, col)
                }
            })
            .collect();

        format!("{}.where_key = {}", sub_alias, concat_parts.join(" || "))
    }

    /// Build a ClickHouse expression that reconstructs the where_key string
    /// from table columns using `concat()`.
    fn journal_where_key_concat_ch(where_columns: &[String]) -> String {
        if where_columns.is_empty() {
            return "network".to_string();
        }
        let parts: Vec<String> = where_columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                if i == 0 {
                    format!("'{}=', toString({})", col, col)
                } else {
                    format!("',{}=', toString({})", col, col)
                }
            })
            .collect();
        format!("concat({})", parts.join(", "))
    }

    pub async fn execute(
        &self,
        window: &mut BlockChainWindow,
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
        provider: Option<&Arc<dyn ChainProvider>>,
    ) -> anyhow::Result<ReorgTaskResult> {
        let rollback_plan = DerivedTableRollbackPlan::default();
        self.execute_with_rollback_plan(window, postgres, clickhouse, provider, &rollback_plan)
            .await
    }

    /// Execute a reorg with optional iterate and type metadata for derived-table rollback.
    ///
    /// Existing callers can continue using [`Self::execute`]. This additive entry point
    /// is used by the runtime-generated coordinator plan and by integrations that need
    /// custom-table iterate rollback without changing `ReorgTask` struct literals.
    pub async fn execute_with_rollback_plan(
        &self,
        window: &mut BlockChainWindow,
        postgres: Option<&PostgresClient>,
        clickhouse: Option<&Arc<ClickhouseClient>>,
        provider: Option<&Arc<dyn ChainProvider>>,
        rollback_plan: &DerivedTableRollbackPlan,
    ) -> anyhow::Result<ReorgTaskResult> {
        // Validate network before any SQL interpolation
        super::validate_sql_value(&self.network, "reorg task network")?;

        let start = std::time::Instant::now();

        tracing::info!(
            network = %self.network,
            fork_point = self.fork_point,
            detection_point = self.detection_point,
            depth = self.detection_point.saturating_sub(self.fork_point) + 1,
            "Starting reorg task"
        );

        // Use pre-fetched canonical blocks if available; otherwise fall back to RPC.
        let canonical: Vec<(u64, B256, B256)> = if !self.canonical_blocks.is_empty() {
            self.canonical_blocks
                .iter()
                .filter(|(n, _, _)| *n >= self.fork_point && *n <= self.detection_point)
                .copied()
                .collect()
        } else if let Some(provider) = provider {
            let block_numbers: Vec<U64> =
                (self.fork_point..=self.detection_point).map(|n| U64::from(n)).collect();
            match provider.get_block_by_number_batch(&block_numbers, false).await {
                Ok(blocks) => blocks
                    .iter()
                    .map(|b| (b.header.number, b.header.hash, b.header.parent_hash))
                    .collect(),
                Err(e) => {
                    tracing::error!("Failed to fetch corrected blocks for reorg range: {}", e);
                    vec![]
                }
            }
        } else {
            vec![]
        };

        let corrected_blocks_owned: Vec<(u64, String, String)> = canonical
            .iter()
            .map(|(n, h, p)| (*n, format!("{:#x}", h), format!("{:#x}", p)))
            .collect();

        let corrected_blocks: Vec<(u64, &str, &str)> =
            corrected_blocks_owned.iter().map(|(n, h, p)| (*n, h.as_str(), p.as_str())).collect();

        let mut pg_connection = match postgres {
            Some(pg) => Some(pg.raw_connection().await.context("PostgreSQL connection failed")?),
            None => None,
        };
        let pg_transaction = match pg_connection.as_mut() {
            Some(connection) => {
                Some(connection.transaction().await.context("PostgreSQL transaction failed")?)
            }
            None => None,
        };
        let pg = pg_transaction.as_ref();

        // One nonce scopes every snapshot created by this execution. PostgreSQL
        // still relies on its session-local temp namespace; ClickHouse needs the
        // nonce because its snapshot tables are database-global.
        let attempt_id = generate_random_id(24).to_ascii_lowercase();
        let reversal_snapshots =
            self.snapshot_for_reversal(pg, clickhouse, rollback_plan, &attempt_id).await?;

        let rollback_result: anyhow::Result<(u64, Vec<String>)> = async {
            let mut affected_tx_hashes = Vec::new();
            let mut total_deleted = 0u64;

            if let Some(pg) = pg {
                let table_names =
                    self.event_tables.iter().map(|t| t.full_name.as_str()).collect::<Vec<_>>();
                let checkpoint_tables = self
                    .event_tables
                    .iter()
                    .map(|t| t.checkpoint_table.as_str())
                    .collect::<Vec<_>>();
                let (deleted, tx_hashes) = PostgresClient::reorg_rollback_in_transaction(
                    pg,
                    &table_names,
                    &self.network,
                    self.fork_point,
                    self.detection_point,
                    &corrected_blocks,
                    &checkpoint_tables,
                )
                .await
                .context("PostgreSQL reorg rollback transaction failed")?;
                total_deleted = deleted;
                affected_tx_hashes = tx_hashes;
            }

            if let Some(ch) = clickhouse {
                let tables = self
                    .event_tables
                    .iter()
                    .map(|t| (t.schema.clone(), t.table_name.clone()))
                    .collect::<Vec<_>>();
                let checkpoint_tables = self
                    .event_tables
                    .iter()
                    .map(|t| t.checkpoint_table.clone())
                    .collect::<Vec<_>>();
                let (ch_deleted, ch_tx_hashes) = ch
                    .reorg_rollback(
                        &tables,
                        &self.network,
                        self.fork_point,
                        self.detection_point,
                        &checkpoint_tables,
                        &corrected_blocks,
                    )
                    .await
                    .context("ClickHouse reorg rollback failed")?;

                if pg.is_none() {
                    total_deleted = ch_deleted;
                    affected_tx_hashes = ch_tx_hashes;
                } else if ch_deleted != total_deleted {
                    tracing::warn!(
                        network = %self.network,
                        postgres_deleted = total_deleted,
                        clickhouse_deleted = ch_deleted,
                        "Reorg rollback: postgres and clickhouse deleted counts differ"
                    );
                }
            }

            Self::apply_reversal_from_snapshots(&reversal_snapshots, pg, clickhouse)
                .await
                .context("Accumulative reversal from snapshots failed")?;
            self.recalculate_from_journal(pg, clickhouse)
                .await
                .context("Journal recalculation failed")?;

            for dt in &self.derived_tables {
                if !dt.rollback_ops.is_empty() || !dt.journal_columns.is_empty() {
                    continue;
                }
                let network_filter = self.network_filter(dt.cross_chain);

                if let Some(pg) = pg {
                    let query = format!(
                        "DELETE FROM {} WHERE rindexer_block_number >= {}{}",
                        dt.full_table_name, self.fork_point, network_filter
                    );
                    pg.batch_execute(&query).await.with_context(|| {
                        format!(
                            "PostgreSQL: failed to delete derived table rows in {}",
                            dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        "PostgreSQL: deleted derived table rows from block >= {} in {}",
                        self.fork_point,
                        dt.full_table_name
                    );
                }
                if let Some(ch) = clickhouse {
                    let query = format!(
                        "ALTER TABLE {} DELETE WHERE rindexer_block_number >= {}{} SETTINGS mutations_sync = 1",
                        dt.full_table_name, self.fork_point, network_filter
                    );
                    ch.execute(&query).await.with_context(|| {
                        format!(
                            "ClickHouse: failed to delete derived table rows in {}",
                            dt.full_table_name
                        )
                    })?;
                    tracing::info!(
                        "ClickHouse: deleted derived table rows from block >= {} in {}",
                        self.fork_point,
                        dt.full_table_name
                    );
                }
            }

            Ok((total_deleted, affected_tx_hashes))
        }
        .await;

        Self::cleanup_reversal_snapshots(&reversal_snapshots, pg, clickhouse).await;
        let (total_deleted, affected_tx_hashes) = rollback_result?;
        if let Some(transaction) = pg_transaction {
            transaction.commit().await.context("PostgreSQL reorg commit failed")?;
        }

        // Update the in-memory window after all DB changes succeed.
        // When canonical blocks are available (parent-hash detection), overwrite with corrected hashes.
        // When canonical blocks are empty (removed-logs / ExEx detection), remove stale entries
        // so the next parent-hash check doesn't immediately re-trigger.
        if !canonical.is_empty() {
            window.update_range(&canonical);
        } else {
            window.remove_from(self.fork_point);
        }

        let duration = start.elapsed().as_secs_f64();
        metrics::record_reorg_handling_duration(&self.network, duration);
        metrics::record_reorg_events_deleted(&self.network, total_deleted);

        tracing::info!(
            network = %self.network,
            events_deleted = total_deleted,
            duration_secs = duration,
            "Reorg task completed"
        );

        // Build the per-table summary for downstream stream consumers. Only
        // source event tables that were rolled back appear here — derived
        // tables are intentionally excluded.
        let affected_tables: Vec<AffectedTable> = self
            .event_tables
            .iter()
            .map(|t| AffectedTable {
                schema: t.schema.clone(),
                table_name: t.table_name.clone(),
                // TODO(future): per-table counts from DB layer; total is on
                // `events_deleted`.
                rows_deleted: 0,
                indexer_name: t.indexer_name.clone(),
                contract_name: t.contract_name.clone(),
                event_name: t.event_name.clone(),
            })
            .collect();

        Ok(ReorgTaskResult {
            events_deleted: total_deleted,
            duration_secs: duration,
            affected_tx_hashes,
            affected_tables,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ======================================================================
    // EventTableInfo::try_new
    // ======================================================================

    #[test]
    fn test_event_table_info_try_new_happy_path() {
        let info = EventTableInfo::try_new(
            "my_schema".to_string(),
            "transfer".to_string(),
            "my_schema_transfer".to_string(),
            "my_indexer".to_string(),
            "USDC".to_string(),
            "Transfer".to_string(),
        )
        .expect("valid identifiers should construct");
        assert_eq!(info.schema, "my_schema");
        assert_eq!(info.table_name, "transfer");
        assert_eq!(info.full_name, "my_schema.transfer");
        assert_eq!(info.checkpoint_table, "my_schema_transfer");
        assert_eq!(info.indexer_name, "my_indexer");
        assert_eq!(info.contract_name, "USDC");
        assert_eq!(info.event_name, "Transfer");
    }

    #[test]
    fn test_event_table_info_rejects_sql_injection_in_schema() {
        let err = EventTableInfo::try_new(
            "schema'; DROP".to_string(),
            "transfer".to_string(),
            "schema_transfer".to_string(),
            "idx".to_string(),
            "USDC".to_string(),
            "Transfer".to_string(),
        );
        assert!(err.is_err(), "schema with SQL injection chars must be rejected");
    }

    #[test]
    fn test_event_table_info_rejects_sql_injection_in_table_name() {
        let err = EventTableInfo::try_new(
            "schema".to_string(),
            "transfer; DROP TABLE".to_string(),
            "schema_transfer".to_string(),
            "idx".to_string(),
            "USDC".to_string(),
            "Transfer".to_string(),
        );
        assert!(err.is_err(), "table name with SQL injection chars must be rejected");
    }

    #[test]
    fn test_event_table_info_rejects_sql_injection_in_checkpoint_table() {
        let err = EventTableInfo::try_new(
            "schema".to_string(),
            "transfer".to_string(),
            "check'--".to_string(),
            "idx".to_string(),
            "USDC".to_string(),
            "Transfer".to_string(),
        );
        assert!(err.is_err(), "checkpoint table with SQL injection chars must be rejected");
    }

    #[test]
    fn test_event_table_info_does_not_validate_metadata_fields() {
        // indexer/contract/event are metadata for stream payload — they must
        // accept arbitrary strings (hyphens, spaces, etc.).
        let info = EventTableInfo::try_new(
            "schema".to_string(),
            "transfer".to_string(),
            "schema_transfer".to_string(),
            "my-indexer-with-hyphens".to_string(),
            "Contract With Spaces".to_string(),
            "Event Name; DROP".to_string(),
        )
        .expect("metadata fields must not be SQL-validated");
        assert_eq!(info.indexer_name, "my-indexer-with-hyphens");
        assert_eq!(info.contract_name, "Contract With Spaces");
        assert_eq!(info.event_name, "Event Name; DROP");
    }

    // ======================================================================
    // AffectedTable construction + JSON shape
    // ======================================================================

    #[test]
    fn test_affected_table_struct_and_json_shape() {
        let at = AffectedTable {
            schema: "s1".to_string(),
            table_name: "t1".to_string(),
            rows_deleted: 0,
            indexer_name: "idx".to_string(),
            contract_name: "USDC".to_string(),
            event_name: "NativeTransfer".to_string(),
        };
        // The struct is used inside stream payloads; mirror the JSON shape
        // that downstream consumers rely on (field-by-field).
        let json = serde_json::json!({
            "schema": at.schema,
            "table_name": at.table_name,
            "rows_deleted": at.rows_deleted,
            "indexer_name": at.indexer_name,
            "contract_name": at.contract_name,
            "event_name": at.event_name,
        });
        assert_eq!(json["schema"], "s1");
        assert_eq!(json["table_name"], "t1");
        assert_eq!(json["rows_deleted"], 0);
        assert_eq!(json["indexer_name"], "idx");
        assert_eq!(json["contract_name"], "USDC");
        assert_eq!(json["event_name"], "NativeTransfer");
    }

    // ======================================================================
    // DerivedColumnRollback::try_new
    // ======================================================================

    #[test]
    fn test_derived_column_rollback_happy_path() {
        let rb = DerivedColumnRollback::try_new(
            "balance".to_string(),
            "value".to_string(),
            SetAction::Add,
        )
        .expect("valid columns should construct");
        assert_eq!(rb.derived_column, "balance");
        assert_eq!(rb.event_column, "value");
        assert!(matches!(rb.action, SetAction::Add));
    }

    #[test]
    fn test_derived_column_rollback_rejects_sql_injection() {
        let err = DerivedColumnRollback::try_new(
            "balance'; DROP".to_string(),
            "value".to_string(),
            SetAction::Add,
        );
        assert!(err.is_err(), "derived_column with SQL injection must be rejected");

        let err = DerivedColumnRollback::try_new(
            "balance".to_string(),
            "value--".to_string(),
            SetAction::Add,
        );
        assert!(err.is_err(), "event_column with SQL injection must be rejected");
    }

    // ======================================================================
    // DerivedColumnJournal::try_new
    // ======================================================================

    #[test]
    fn test_derived_column_journal_happy_path_empty_where_columns() {
        let jc = DerivedColumnJournal::try_new("max_trade".to_string(), SetAction::Max, vec![])
            .expect("empty where_columns is allowed");
        assert_eq!(jc.derived_column, "max_trade");
        assert!(matches!(jc.action, SetAction::Max));
        assert!(jc.where_columns.is_empty());
    }

    #[test]
    fn test_derived_column_journal_multiple_where_columns() {
        let jc = DerivedColumnJournal::try_new(
            "latest".to_string(),
            SetAction::Set,
            vec!["user".to_string(), "token".to_string()],
        )
        .expect("multiple valid where_columns should be accepted");
        assert_eq!(jc.where_columns, vec!["user".to_string(), "token".to_string()]);
    }

    #[test]
    fn test_derived_column_journal_rejects_sql_injection() {
        let err = DerivedColumnJournal::try_new("bad'--".to_string(), SetAction::Set, vec![]);
        assert!(err.is_err(), "derived_column with SQL injection must be rejected");

        let err = DerivedColumnJournal::try_new(
            "latest".to_string(),
            SetAction::Set,
            vec!["good".to_string(), "bad; DROP".to_string()],
        );
        assert!(err.is_err(), "where_columns entry with SQL injection must be rejected");
    }

    // ======================================================================
    // DerivedTableRollbackOp::try_new
    // ======================================================================

    #[test]
    fn test_derived_table_rollback_op_happy_path() {
        let columns = vec![DerivedColumnRollback::try_new(
            "balance".to_string(),
            "value".to_string(),
            SetAction::Add,
        )
        .unwrap()];
        let op = DerivedTableRollbackOp::try_new(
            "myschema.transfer".to_string(),
            vec![("user".to_string(), "from_addr".to_string())],
            columns,
            None,
        )
        .expect("valid op should construct");
        assert_eq!(op.event_table, "myschema.transfer");
        assert_eq!(op.where_columns.len(), 1);
        assert_eq!(op.columns.len(), 1);
        assert!(op.condition.is_none());
    }

    #[test]
    fn test_reorg_condition_preserves_nested_and_index_accessors() {
        let source_column_types = HashMap::from([
            ("data_amount".to_string(), ColumnType::Uint256),
            ("ids".to_string(), ColumnType::Array(Box::new(ColumnType::Uint256))),
        ]);
        let pg_source = ReversalSource {
            dialect: SqlDialect::Postgres,
            iterate: &[],
            source_column_types: &source_column_types,
        };
        assert_eq!(
            render_reorg_condition("$data.amount > 0", &pg_source).unwrap(),
            "CAST(_rindexer_event.\"data_amount\" AS NUMERIC) > 0"
        );
        assert_eq!(
            render_reorg_condition("$ids[0] > 0", &pg_source).unwrap(),
            "CAST((_rindexer_event.\"ids\")[1] AS NUMERIC) > 0"
        );

        let ch_source = ReversalSource {
            dialect: SqlDialect::Clickhouse,
            iterate: &[],
            source_column_types: &source_column_types,
        };
        assert_eq!(
            render_reorg_condition("$data.amount > 0", &ch_source).unwrap(),
            "_rindexer_event.`data_amount` > 0"
        );
        assert_eq!(
            render_reorg_condition("$ids[0] > 0", &ch_source).unwrap(),
            "arrayElement(_rindexer_event.`ids`, 1) > 0"
        );
        assert_eq!(
            pg_source.operand("data.amount").unwrap(),
            "CAST(_rindexer_event.\"data_amount\" AS NUMERIC)"
        );
        assert_eq!(
            pg_source.operand("ids[0]").unwrap(),
            "CAST((_rindexer_event.\"ids\")[1] AS NUMERIC)"
        );
        assert!(
            render_reorg_condition("$ids[0] > @balance", &pg_source).is_err(),
            "table-state conditions must fail before raw events are deleted"
        );
        assert_eq!(
            render_reorg_condition("id::NUMERIC > 7", &pg_source).unwrap(),
            "id::NUMERIC > 7",
            "validated legacy SQL conditions remain compatible"
        );
    }

    // ======================================================================
    // DerivedTableInfo::try_new
    // ======================================================================

    #[test]
    fn test_derived_table_info_happy_path_empty_ops() {
        let dt = DerivedTableInfo::try_new("myschema.balances".to_string(), false, vec![], vec![])
            .expect("valid name should construct");
        assert_eq!(dt.full_table_name, "myschema.balances");
        assert!(!dt.cross_chain);
        assert!(dt.rollback_ops.is_empty());
        assert!(dt.journal_columns.is_empty());
    }

    #[test]
    fn test_derived_table_info_happy_path_with_one_op() {
        let op = DerivedTableRollbackOp::try_new(
            "myschema.transfer".to_string(),
            vec![("user".to_string(), "from_addr".to_string())],
            vec![DerivedColumnRollback::try_new(
                "balance".to_string(),
                "value".to_string(),
                SetAction::Subtract,
            )
            .unwrap()],
            None,
        )
        .unwrap();
        let dt = DerivedTableInfo::try_new("myschema.balances".to_string(), true, vec![op], vec![])
            .expect("valid with one rollback op should construct");
        assert_eq!(dt.full_table_name, "myschema.balances");
        assert!(dt.cross_chain);
        assert_eq!(dt.rollback_ops.len(), 1);
    }
}
