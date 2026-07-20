# AGENTS.md — working on rindexer

Guidance for coding agents (and humans) contributing to this repo. Every command and
path in this file is taken from the repo's actual CI workflows, Makefiles, and source —
follow it exactly rather than guessing.

rindexer is an open-source, high-speed EVM indexing toolset written in Rust, compatible
with any EVM chain. Users index chain events either with a single `rindexer.yaml` file
("no-code" mode) or by generating a typed Rust project ("rust" mode), and get storage
(PostgreSQL / ClickHouse / CSV), streams (Kafka, webhooks, SNS, Redis, RabbitMQ,
Cloudflare Queues), chat alerts, and a GraphQL API out of the box. Docs: https://rindexer.xyz

## Repo map

| Path | What it is |
|---|---|
| `core/` | The `rindexer` crate — all indexing logic, manifest parsing, databases, streams, reorg handling, codegen templates |
| `cli/` | The `rindexer_cli` crate — the `rindexer` binary (clap CLI wrapping core) |
| `graphql/` | Node.js PostGraphile server, compiled to a binary by `core/build.rs` and embedded into the Rust binary |
| `documentation/` | Docs site (vocs). **Contains the changelog** — see below |
| `e2e-tests/` | Standalone E2E harness (workspace member, custom runner binary, not `#[test]`-based) |
| `examples/` | 22 example projects: no-code YAML, Rust projects (4 are workspace members), and `tables_*` examples |
| `xtask/` | Dev tooling (`cargo xtask …`) for maintaining `.blockclock` timestamp files — not part of releases |
| `helm/` | Kubernetes chart (`helm/rindexer/`). Not versioned/published by CI |
| `providers/` | Deployment templates (Railway; `providers/aws/` is an empty placeholder) |

There is no root Makefile. Makefiles exist only at `cli/Makefile` and `e2e-tests/Makefile`.

## ⚠️ The changelog rule — always update it

**Every change you make must be reflected in the changelog, in the same PR/commit as the
code.** The changelog is NOT a root `CHANGELOG.md` — it lives at:

```
documentation/docs/pages/docs/changelog.mdx
```

### How to add an entry

Add a bullet to the **unreleased section at the very top of the file** (above the
`## Releases` heading), under the heading matching your change type. The top of the file
looks like this — add bullets directly below the divider of the right section:

```markdown
# Changelog

### Breaking changes
-------------------------------------------------
- breaking: <entry>

### Bug fixes
-------------------------------------------------
- fix: <entry>

### Features
-------------------------------------------------
- feat: <entry>

## Releases
-------------------------------------------------
```

Entry format rules (copied from real entries):

- Prefix with `- fix:`, `- feat:`, or `- breaking:` to match the section.
- Small change → one lowercase sentence: `- fix: disable reth on windows`.
- Significant change → bold title, em-dash, then user-facing detail in the same bullet:
  `- feat: **Parallel historical backfill** — new `fetch_concurrency` network config splits historic block ranges across N concurrent workers…`
- Write for **users of rindexer**, not for reviewers of your diff: name the YAML
  field / env var / metric / CLI flag in backticks, state defaults, and call out
  migration impact (e.g. "run `rindexer codegen typings` to regenerate",
  "downstream consumers need to allow the new field").
- Link issues inline where relevant: `([#383](https://github.com/joshstevens19/rindexer/issues/383))`.

### What NOT to do in that file

- **Never edit anything below the `## Releases` heading** and never restructure the
  headings. `.github/workflows/edit-releases.yml` and `migrate-releases.yml`
  machine-parse this file (they split on `## Releases` and match `X.Y.Z-beta` version
  headings) to generate GitHub release notes — format deviations break them.
- Don't add new dashed dividers; the `### <Section>` + 49-dash divider lines already
  exist at the top. Just add bullets beneath them.
- Don't create a dated `# X.Y.Z-beta - <date>` heading — that happens at release time,
  not in feature/fix PRs.

The only changes exempt from a changelog entry are ones with zero user-visible effect
(CI config, comment/typo fixes, internal docs like this file). When in doubt, add an
entry — features are expected to ship with one 100% of the time.

## Breaking changes — what counts and how to handle them

A change is breaking if an existing user's project stops working **or silently changes
behavior** after upgrading — not just Rust API breakage. Every breaking change needs a
`- breaking:` changelog entry with explicit migration instructions (see the 0.41.0
`Arc<dyn ChainProvider>` entry for the expected shape: what breaks, what to change,
what command to re-run).

### rindexer.yaml is the most sensitive surface

Users have long-lived `rindexer.yaml` files that must keep working across upgrades.
Rules, grounded in how the manifest actually behaves:

- **New fields must be optional AND round-trip-safe.** The established pattern is
  `#[serde(default, skip_serializing_if = "Option::is_none")]`. The `skip_serializing_if`
  half is not cosmetic: `rindexer add contract`, `rindexer new`, and `rindexer phantom`
  deserialize the user's whole manifest and write it back (`write_manifest` in
  `core/src/manifest/yaml.rs`) — a field without it gets injected into every user's
  YAML file the next time they run one of those commands.
- **Never rename or remove a YAML key outright.** Manifest structs do NOT use
  `deny_unknown_fields`, so an obsolete key won't error — it is **silently ignored**,
  which is worse than a crash: the user's config quietly stops taking effect. If a
  rename is truly needed, keep the old key readable via `#[serde(alias = "old_name")]`
  (no precedent in this repo yet — flag it in the PR) and add a `- breaking:` entry
  telling users to migrate.
- **Changing a default value is breaking** even though every existing YAML still
  parses — behavior changes silently under users' feet. `- breaking:` entry, state
  old and new default.
- **Tightening validation** (startup now errors on previously-accepted config) is
  user-visible and must be in the changelog; it can be framed as a feature when it
  replaces silent misbehavior (0.41.0 did this for `delivery: finalized` on a network
  without live indexing) — but the entry must state that previously-running configs
  now error.
- **Changing a field's type or making an optional field required**: breaking; avoid,
  or accept both shapes (see `types/single_or_array.rs::StringOrArray` for the
  existing accept-both pattern).

### Other breaking surfaces — quick checklist

- Generated-code API (`core/src/lib.rs`, `core/src/generator/` templates) — users must
  re-run `rindexer codegen typings`; the changelog entry must say so.
- Database schema of **existing** tables (column renames/type changes) — users have
  data and downstream SQL/GraphQL queries against these.
- Stream payload JSON, log message text, Prometheus metric names — see "Surfaces that
  are API even though they don't look like it".
- CLI commands, flags, and exit behavior.

## Environment & build

- Rust: **stable** (`rust-toolchain.toml`). No nightly anywhere, including rustfmt.
- **Node.js + npm are required to compile the workspace.** `core/build.rs` builds the
  `graphql/` PostGraphile server into a standalone binary (via `@yao-pkg/pkg`) and
  embeds it with `include_bytes!`, and copies BlockClock resources. CI uses Node 22;
  the docs site needs Node >= 20. The first build is slow for this reason.
- Linux additionally needs `libssl-dev` and `pkg-config` (installed in every CI job).
- Docker daemon required for the docker-backed tests in `core/tests/` (testcontainers)
  and the e2e harness. Foundry (`anvil`) required for the e2e harness.

Build commands:

```bash
cargo build                                    # debug build of everything
cd cli && make prod_build                      # production build: RUSTFLAGS='-C target-cpu=native' cargo build --release --features jemalloc,reth
```

Release profiles in `core/Cargo.toml` and `cli/Cargo.toml` use `lto = "fat"` +
`codegen-units = 1` — release builds are very slow by design.

### Feature flags

| Crate | Features |
|---|---|
| `rindexer` (core) | `jemalloc`, `debug-json`, `kafka` (rdkafka; builds bundled librdkafka from source — via cmake on Windows only), `reth` (embedded reth node/ExEx — huge dep tree). **No default features.** |
| `rindexer_cli` (cli) | `default = ["kafka"]`, plus `jemalloc`, `reth`, `kafka` (forwarding to core) |

`reth`-gated code does not compile in a default build, and `kafka`-gated code does not
compile when building core alone (`cargo build -p rindexer`) — a default **workspace**
build does enable `kafka`, via the CLI's default feature and cargo feature unification.
If you touch code near `#[cfg(feature = …)]`, verify with `--all-features` (CI does;
see below). Windows release binaries are built **without** `reth`.

### Runtime env vars & logging

- The CLI loads `.env` from the project path before every command
  (`load_env_from_project_path` in `cli/src/main.rs`), and `rindexer.yaml` supports
  `${ENV_VAR}` substitution (`substitute_env_variables` in `core/src/manifest/yaml.rs`).
- Key env vars: `DATABASE_URL` (+ `DATABASE_POOL_SIZE`) for Postgres;
  `CLICKHOUSE_URL` / `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB`
  (+ `RINDEXER_CLICKHOUSE_BATCH_SIZE`) for ClickHouse.
- Logging is `tracing` with an env filter — control verbosity with `RUST_LOG`
  (the e2e Makefile targets set it per target; `RUST_BACKTRACE='full'` used in
  `cli/Makefile` run targets).

## Required checks — mirror CI before you're done

CI (`.github/workflows/ci.yml`) gates every PR with exactly these; run them locally:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --exclude rindexer_rust_playground --workspace
cargo nextest run --exclude rindexer_rust_playground --workspace --all-features
```

- **Use `cargo nextest`, never plain `cargo test`, for workspace runs.** nextest runs
  each test in its own process; the docker-backed tests under `core/tests/e2e_*.rs`
  mutate process env (`DATABASE_URL`, `CLICKHOUSE_*`) and race under `cargo test`.
  Install once with `cargo install cargo-nextest --locked`.
- Formatting rules are in `rustfmt.toml` (`use_small_heuristics = "Max"` — aggressive
  single-line packing; `reorder_imports`; `use_field_init_shorthand`). Run `cargo fmt`
  and let it decide; don't fight it.
- The `--all-features` test run compiles `reth` + `kafka` and is substantially heavier.
- Coverage job: `cargo llvm-cov nextest -p rindexer -p rindexer_cli --all-features`
  (uploaded to Codecov, `fail_ci_if_error: true`).

A separate E2E workflow (`.github/workflows/e2e.yml`, 30-min timeout) runs on PRs
touching `core/**`, `cli/**`, `e2e-tests/**`, `graphql/**`, or `Cargo.lock`.

## Testing

Three distinct layers — know which one you're adding to:

1. **Inline unit tests** — `#[cfg(test)]` modules inside `core/src/**` (the dominant
   pattern; ~50 files have them). Add yours next to the code you change.
2. **Docker-backed integration tests** — `core/tests/*.rs` (`e2e_reorg.rs`,
   `e2e_reorg_streams.rs`, `e2e_finalized_delivery.rs`, `e2e_parallel_fetch.rs`,
   `e2e_hist_to_live_dup.rs`, `clickhouse_type_e2e.rs`, `postgres_bytea_array.rs`,
   `yaml_advanced_tables.rs`). They spin up real Postgres/ClickHouse/Redis/RabbitMQ/
   Kafka containers via `testcontainers` — Docker required, no manual service setup.
   Run one file: `cargo nextest run -p rindexer --test e2e_reorg`
3. **E2E harness** — `e2e-tests/` is a custom runner binary that spawns Anvil, launches
   the built `rindexer_cli` binary against ephemeral projects, and asserts indexed
   results. From `e2e-tests/`:

   ```bash
   make dev-setup                 # install Foundry deps + build rindexer (release) + build harness
   make run-tests                 # full suite
   make run-test TEST=<test_name> # single test
   make run-tests-debug           # RUST_LOG=info
   ```

   Tests needing `MAINNET_RPC_URL` self-skip when it's unset. Adding a test = new
   module implementing `TestModule` in `e2e-tests/src/tests/` + register it in
   `e2e-tests/src/tests/registry.rs` (`get_all_tests()`).

Do not run or un-ignore the `#[ignore]` tests in `core/src/chat/` (opsgenie, twilio,
pagerduty) or `core/src/blockclock/runlencoder.rs` — they hit real external services
and need real credentials.

New features are expected to ship with tests — recent substantive PRs consistently
include core changes + tests (+ docs + changelog) in the same commit.

## core/ architecture — where things live

| Subsystem | Path | Notes |
|---|---|---|
| YAML manifest (`rindexer.yaml`) | `core/src/manifest/` | `Manifest` struct in `core.rs`; one file per YAML section (`network.rs`, `contract.rs`, `storage.rs`, `stream.rs`, `chat.rs`, …); parsing + env substitution in `yaml.rs` |
| Engine entry | `core/src/start.rs`, `core/src/indexer/` | `start_rindexer` (rust mode) / `start_rindexer_no_code` (yaml mode); `indexer/no_code.rs` is the no-code runtime |
| RPC providers | `core/src/provider.rs` | `JsonRpcCachedProvider` wrapping alloy; mock helpers built in; throttled by `adaptive_concurrency.rs` |
| PostgreSQL | `core/src/database/postgres/` | client, generate, setup, schema_sync, migrations, indexes, batch_operations |
| ClickHouse | `core/src/database/clickhouse/` | deliberate mirror of the postgres module |
| Shared DB layer | `core/src/database/generate.rs`, `core/src/database/sql_type_wrapper.rs` | `EthereumSqlTypeWrapper` maps Solidity types to **both** PG (`to_type`) and ClickHouse (`to_clickhouse_value`) |
| Streams | `core/src/streams/` | one file per sink; `clients.rs::StreamsClients` is the fan-out entry with finalized-delivery buffering |
| Reorg handling | `core/src/indexer/reorg/` | `ReorgCoordinator`, reversal SQL builder (`task.rs`), persistence, block-hash window |
| Native transfers | `core/src/indexer/native_transfer.rs` | trace/block-based ETH transfer indexing |
| Codegen templates | `core/src/generator/` | generates users' Rust projects — see "Codegen coupling" |
| GraphQL | `core/src/api/` | spawns the embedded Node binary built from `graphql/` — not a Rust server |
| Metrics | `core/src/metrics/` | all Prometheus statics in `definitions.rs` |
| Chat/alerts | `core/src/chat/` | Telegram/Discord/Slack/Twilio/PagerDuty/Opsgenie |
| Reth ExEx | `core/src/reth/` | feature-gated in-process reth node feeding `ChainStateNotification`s |
| Hot reload | `core/src/hot_reload/` | watches rindexer.yaml, computes restart plan |
| Blockclock | `core/src/blockclock/` + `core/resources/blockclock/` | RPC-free block-timestamp resolution from run-length-encoded files (maintained via `cargo xtask`) |

### Critical gotchas before editing core/

1. **PG and ClickHouse are parallel implementations.** A storage-behavior change
   usually needs: `database/postgres/…` AND `database/clickhouse/…` AND
   `sql_type_wrapper.rs` (both `to_type` and `to_clickhouse_value`) AND both init
   paths in `indexer/start.rs` (`initialize_database` / `initialize_clickhouse`).
   `indexer/no_code.rs` writes to Postgres, ClickHouse, and CSV in the same handler.
2. **SQL is built by string formatting** throughout (`database/generate.rs`, batch
   query builders, `indexer/reorg/task.rs`). Identifier quoting differs by database —
   double quotes for PG, backticks for ClickHouse (helpers in `reorg/task.rs`). Any
   user-supplied identifier must go through the validators in `reorg/mod.rs`
   (`validate_sql_identifier`, `validate_sql_condition`).
3. **`core/src/generator/*_bindings.rs` emit Rust source from `format!`/raw-string
   templates.** Doubled braces `{{ }}` are literal braces. Edits here change what
   `rindexer codegen typings|indexer` produces for every user — output must compile in
   users' projects, and checked-in example typings must be regenerated (below).
4. **The GraphQL server is a Node binary**, embedded at compile time. Editing anything
   under `graphql/` triggers a core rebuild on next `cargo build`.
5. **Stream retries must stay publisher-local** (see comment in `streams/mod.rs`):
   callback-level retry would re-run DB inserts and duplicate rows — event tables have
   no UNIQUE constraint.
6. **`core/src/lib.rs` is a public API consumed by generated user projects**
   (`start_rindexer`, `EthereumSqlTypeWrapper`, etc.). Renaming or changing signatures
   there breaks users' generated code — coordinate with `generator/` templates and add
   a `- breaking:` changelog entry.

### Surfaces that are API even though they don't look like it

Users integrate with rindexer through more than the Rust API. Treat changes to any of
these as user-facing (changelog entry, ideally additive):

- **Log message text.** Users build log-grep alerts on specific info/warn lines —
  changing or removing one silently breaks their monitoring. If you must change a
  message, say so explicitly in the changelog (see the 0.41.0 heartbeat-log entry for
  the expected wording).
- **Stream payload JSON** (`EventMessage`, `__rindexer_reorg`). Downstream consumers
  may use strict schema validation — prefer additive fields and call new fields out in
  the changelog.
- **Prometheus metric names and labels.** All metrics are prefixed `rindexer_`
  (`core/src/metrics/definitions.rs`); renaming one breaks users' dashboards/alerts.
- **Database table/column naming and internal tables** (`rindexer_internal.*`) —
  users query these directly via SQL and GraphQL.

## Recipes for common changes

**Adding a YAML manifest field**
1. Add the field to the right struct in `core/src/manifest/*.rs` — use
   `#[serde(default, skip_serializing_if = "Option::is_none")]` so existing YAML files
   keep parsing AND the field isn't injected into users' files on manifest round-trips
   (see "Breaking changes" above).
2. Wire the behavior in core, and decide how `rindexer start --watch` should treat it:
   `core/src/hot_reload/diff.rs` classifies manifest changes per-field
   (`ManifestChange` → `ReloadAction`: hot-apply vs selective restart vs full restart) —
   check your field lands in the right bucket.
3. Document it in the matching page under
   `documentation/docs/pages/docs/start-building/yaml-config/`.
4. Tests + changelog entry naming the field and its default.

**Adding a stream sink**
One file per sink in `core/src/streams/` → wire into `streams/clients.rs`
(`StreamsClients`) → manifest config in `core/src/manifest/stream.rs` → docs page in
`start-building/streams/` + `vocs.config.tsx` sidebar → changelog.

**Adding a chat/alert provider**
`core/src/chat/<provider>.rs` → wire into `chat/clients.rs` → manifest config in
`core/src/manifest/chat.rs` → docs page in `start-building/chatbots/` + sidebar →
changelog. (The PagerDuty/OpsGenie PR is the reference example of this shape.)

**Adding a metric**
Define it in `core/src/metrics/definitions.rs` with the `rindexer_` prefix → record it
via the per-domain helpers (`metrics/{indexing,rpc,database,streams}.rs`) → document in
`start-building/metrics.mdx` → changelog entry naming the metric.

**Changing storage behavior**
Remember gotcha #1: Postgres and ClickHouse are parallel implementations — change both
sides, `sql_type_wrapper.rs`, and add coverage in `core/tests/` (there are existing
docker-backed tests for both DBs to extend).

## Codegen coupling — regenerate example typings

Files under `examples/*/src/rindexer_lib/typings/` carry the header
`// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY.` — never hand-edit them.

If your change alters codegen output (anything in `core/src/generator/`), regenerate the
checked-in examples so they match, from `cli/`:

```bash
make playground_codegen_typings        # examples/rindexer_rust_playground
make playground_codegen_indexer
make factory_indexing_codegen_typings  # examples/rindexer_factory_indexing
make factory_indexing_codegen_indexer
```

(Workspace-member Rust examples — `rindexer_rust_playground`, `rust_clickhouse`,
`clickhouse_factory_indexing`, `rindexer_factory_indexing` — compile on every workspace
build, so stale typings fail clippy/build.)

Note: `cli/Makefile` also has a "LOCAL NONE CHECKED IN PROJECT COMMANDS" section
pointing at example dirs that don't exist in the repo — ignore those targets. Use
`examples/rindexer_demo_cli` to play with the CLI locally, but don't commit changes
to it.

## Documentation site

The site is [vocs](https://vocs.dev) under `documentation/` (Node >= 20):

```bash
cd documentation
npm i
npm run dev      # local dev server
npm run build    # validate the site builds — CI does NOT check docs, so do this manually
```

Rules:

- **New/changed YAML manifest fields must be documented.** Manifest structs in
  `core/src/manifest/*.rs` map to pages in
  `documentation/docs/pages/docs/start-building/yaml-config/` (`networks.mdx`,
  `contracts.mdx`, `storage.mdx`, `config.mdx`, `global.mdx`, `graphql.mdx`,
  `native-transfers.mdx`, `top-level-fields.mdx`). Stream configs →
  `start-building/streams/`, chat configs → `start-building/chatbots/`, custom tables →
  `start-building/tables/`, CLI flags → `references/cli.mdx`.
- **New pages must be added to the `sidebar` array in `documentation/vocs.config.tsx`**
  — there is no auto-discovery.
- No CI job builds or lints the docs, so breakage ships silently unless you run
  `npm run build` yourself.

## Release process — what you must never touch

Releases are automated from branch names and commit messages, so ordinary PRs must
avoid tripping them:

- **Never bump `version` in `cli/Cargo.toml` or `core/Cargo.toml`.** Pushing a
  `release/X.Y.Z` branch triggers `.github/workflows/release.yml`, which runs
  `cargo set-version` from the branch name and opens the release PR automatically.
- **Never push branches named `release/**`** unless you are actually cutting a release.
- **Never write commit messages matching `Release vX.Y.Z`** — merging such a commit to
  `master` is the trigger for binary builds + the GitHub Release.
- Docker images (`ghcr.io/joshstevens19/rindexer`) publish automatically after a
  GitHub Release via `.github/workflows/docker.yml` (tags: SHA + `latest`).
- `helm/rindexer/Chart.yaml` versions are intentionally not synced with crate versions;
  touch the chart only when the deploy surface changes (new env vars, ports, resources).
- `xtask` is dev tooling only (blockclock maintenance via `cargo xtask …`); it is not
  versioned or released.

At release time (maintainer flow): entries staged in the changelog's top unreleased
section are moved under a new `# X.Y.Z-beta - <date>` heading in the `## Releases`
section on the release branch.

## Git & PR conventions

- Default branch: `master`. Squash-merge; subjects get the PR suffix `(#NNN)`.
- Commit/PR title style: conventional prefixes — `feat:`, `fix:`, `chore:`,
  `refactor:`, `build:`, optionally scoped (`feat(reorg):`, `fix(streams):`). Match
  the prefix to your changelog entry.
- Work branches: `feat/*`, `fix/*`, or `<github-username>/<topic>`.
- The expected PR shape, seen consistently in history: **code + tests + docs +
  changelog entry in one PR**. A feature touching the manifest typically changes
  `core/src/manifest/…`, adds/updates tests (`core/tests/` or `e2e-tests/`), adds a
  docs page or section (+ `vocs.config.tsx` sidebar if a new page), and adds the
  changelog bullet.

## Pre-submit checklist

1. `cargo fmt --all -- --check` passes.
2. `cargo clippy --workspace --all-targets -- -D warnings` passes.
3. `cargo nextest run --exclude rindexer_rust_playground --workspace` passes
   (and `--all-features` if you touched `kafka`/`reth`-gated or feature-adjacent code).
4. Changelog entry added to the top unreleased section of
   `documentation/docs/pages/docs/changelog.mdx` (or the change genuinely has zero
   user-visible effect).
5. Docs updated for any new YAML field, CLI flag, env var, or metric; sidebar updated
   for new pages; `npm run build` in `documentation/` if docs changed.
6. Example typings regenerated if you touched `core/src/generator/`.
7. No version bumps, no `release/**` branch, no `Release vX.Y.Z` commit message.
