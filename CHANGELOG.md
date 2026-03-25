# Changelog

All notable changes to rindexer will be documented in this file.

## [Unreleased]

### Bug Fixes

- **fix(tables): UNNEST batch upsert accumulation for duplicate keys** ([#383](https://github.com/joshstevens19/rindexer/issues/383))

  When multiple events in the same `eth_getLogs` batch targeted the same primary key with arithmetic actions (`add`, `subtract`, `max`, `min`), only the last value was applied instead of accumulating all values.

  **Root cause:** The `to_process` CTE used `SELECT DISTINCT ON (key) ... ORDER BY key, seq DESC`, which kept only one row per key — discarding earlier values before they reached the `INSERT ... ON CONFLICT DO UPDATE` statement.

  **Fix:** Added `build_to_process_cte_aggregated()` which uses `GROUP BY` with appropriate aggregations when arithmetic columns are present:
  - `add`/`subtract` columns → `SUM()` (pre-aggregated before INSERT)
  - `max` columns → `MAX()`
  - `min` columns → `MIN()`
  - `set` columns → last value by sequence (`array_agg(col ORDER BY seq DESC)[1]`)
  - Sequence column → `MAX()` (preserves latest sequence per group)

  Non-arithmetic upserts (`set`-only) continue to use the optimized `DISTINCT ON` path — no behavior change for existing configurations without arithmetic actions.

  **Impact:** Affects no-code tables with `action: add`, `subtract`, `max`, or `min` when a single `eth_getLogs` response contains multiple events targeting the same table key. Common for high-volume contracts (e.g., many transfers to the same address in a 2000-block batch). Does NOT affect raw event tables or `insert`-only tables.

  **Files changed:**
  - `core/src/database/postgres/batch_operations/query_builder.rs` — new `ColumnAggregate` enum + `build_to_process_cte_aggregated()`
  - `core/src/database/postgres/batch_operations/dynamic.rs` — route to aggregated CTE when arithmetic columns detected
  - `core/src/database/postgres/batch_operations/mod.rs` — re-exports

  **Repro:** `repro/issue-383-unnest-batch-accumulation/` — Anvil-based scenario with 3 transfers to the same address in one batch, verifying correct balance accumulation.
