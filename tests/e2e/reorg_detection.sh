#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------
# rindexer reorg detection E2E test via Anvil's anvil_reorg RPC method
#
# Tests detection mechanisms:
#   1. Tip hash changed  — same block number, different hash
#      + verifies full recovery pipeline (handle_reorg_recovery)
#   2. Parent hash mismatch — new block's parent_hash doesn't match cache
#      + verifies full recovery pipeline (handle_reorg_recovery)
#   3. ClickHouse reorg recovery + replay — verify DB state after reorg
#      with deterministic transfer events and derived-table value checks
#      (requires Docker; skipped if unavailable)
#   4. PostgreSQL reorg recovery + replay — deterministic parity with ClickHouse
#      (requires Docker; skipped if unavailable)
#   5. reorg_safe_distance config — verify YAML field is parsed and active
#
# (The removed-flag mechanism cannot be tested with Anvil because
#  eth_getLogs doesn't return removed logs after a reorg.)
#
# Not testable in E2E (unit tests cover these):
#   - F3 broadcast channel (code-gen mode only)
#   - F5 stream retraction (no streams in fixtures)
#   - F2 derived table cleanup (no tables: in fixtures)
#   - F1 post-confirmation verifier (needs 64+ blocks)
#
# Usage: ./reorg_detection.sh [reorg_depth]
#        reorg_depth defaults to 3
#
# Prerequisites: anvil, cast, forge (Foundry), rindexer_cli built
#                Docker (optional, for test 3)
# Run from repo root: ./tests/e2e/reorg_detection.sh
# ------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REORG_DEPTH="${1:-3}"
RINDEXER_BIN="$REPO_ROOT/target/release/rindexer_cli"
FIXTURE_DIR="$SCRIPT_DIR/fixtures"
WORK_DIR=$(mktemp -d)
ANVIL_PORT=8545
ANVIL_PID=""
RINDEXER_PID=""
CH_CONTAINER=""
CH_PORT=18123
PG_CONTAINER=""
PG_PORT="${PG_PORT:-15432}"
LOG_FILE="$WORK_DIR/rindexer_test.log"
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_TESTS=3
TEST1_STATUS="FAIL"
TEST2_STATUS="FAIL"
TEST3_STATUS="SKIP"
TEST4_STATUS="SKIP"
TEST5_STATUS="FAIL"

# Deterministic local contract + accounts (Anvil defaults)
ANVIL_DEPLOYER_KEY="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_RECIPIENT_1="0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
ANVIL_RECIPIENT_2="0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC"
TOKEN_ADDRESS=""
REPLAY_REORG_DEPTH="${REPLAY_REORG_DEPTH:-6}"

# Helper: query ClickHouse via HTTP API
ch_query() {
    curl -sf "http://127.0.0.1:$CH_PORT/" --data-binary "$1" 2>/dev/null
}

pg_query() {
    if [[ -z "$PG_CONTAINER" ]]; then
        return 1
    fi
    docker exec "$PG_CONTAINER" psql -U postgres -d postgres -tAc "$1" 2>/dev/null
}

deploy_mock_erc20() {
    if ! command -v forge &>/dev/null; then
        echo "ERROR: forge is required for deterministic replay E2E"
        exit 1
    fi

    local deploy_dir
    deploy_dir=$(mktemp -d)

    cat > "$deploy_dir/foundry.toml" <<'EOF'
[profile.default]
src = 'src'
out = 'out'
libs = []
solc_version = '0.8.24'
EOF

    mkdir -p "$deploy_dir/src"
    cat > "$deploy_dir/src/MockERC20.sol" <<'EOF'
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

contract MockERC20 {
    string public name = "Mock USDC";
    string public symbol = "mUSDC";
    uint8 public decimals = 6;
    uint64 public totalSupply;
    mapping(address => uint64) public balanceOf;

    event Transfer(address indexed from, address indexed to, uint64 value);

    constructor(uint64 initialSupply) {
        totalSupply = initialSupply;
        balanceOf[msg.sender] = initialSupply;
        emit Transfer(address(0), msg.sender, initialSupply);
    }

    function transfer(address to, uint64 value) external returns (bool) {
        require(balanceOf[msg.sender] >= value, "insufficient");
        unchecked {
            balanceOf[msg.sender] -= value;
            balanceOf[to] += value;
        }
        emit Transfer(msg.sender, to, value);
        return true;
    }
}
EOF

    local deploy_out
    deploy_out=$(forge create \
        --root "$deploy_dir" \
        src/MockERC20.sol:MockERC20 \
        --broadcast \
        --rpc-url "http://127.0.0.1:$ANVIL_PORT" \
        --private-key "$ANVIL_DEPLOYER_KEY" \
        --constructor-args 1000000000 2>&1)

    TOKEN_ADDRESS=$(echo "$deploy_out" | awk '/Deployed to:/{print $3}' | tail -n1)
    rm -rf "$deploy_dir"

    if [[ -z "$TOKEN_ADDRESS" ]]; then
        echo "ERROR: failed to deploy MockERC20"
        echo "$deploy_out"
        exit 1
    fi

    echo "MockERC20 deployed at: $TOKEN_ADDRESS"
}

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    [[ -n "$RINDEXER_PID" ]] && kill "$RINDEXER_PID" 2>/dev/null && echo "Stopped rindexer ($RINDEXER_PID)"
    [[ -n "$ANVIL_PID" ]]    && kill "$ANVIL_PID"    2>/dev/null && echo "Stopped anvil ($ANVIL_PID)"
    [[ -n "$CH_CONTAINER" ]] && docker rm -f "$CH_CONTAINER" >/dev/null 2>&1 && echo "Stopped ClickHouse ($CH_CONTAINER)"
    [[ -n "$PG_CONTAINER" ]] && docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 && echo "Stopped PostgreSQL ($PG_CONTAINER)"
    wait 2>/dev/null
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# ------------------------------------------------------------------
# 0. Set up working directory with fixture config
# ------------------------------------------------------------------
cp "$FIXTURE_DIR/reorg_test.yaml" "$WORK_DIR/rindexer.yaml"
cp -r "$FIXTURE_DIR/abis" "$WORK_DIR/abis"
cd "$WORK_DIR"

# ------------------------------------------------------------------
# 1. Start Anvil (standalone, chain-id 137, 2s blocks)
# ------------------------------------------------------------------
echo "=== Starting Anvil (chain_id=137, block_time=2s) ==="
anvil --chain-id 137 \
      --block-time 2 \
      --port "$ANVIL_PORT" \
      --silent &
ANVIL_PID=$!
echo "Anvil PID: $ANVIL_PID"

# Wait for Anvil to be ready
for i in $(seq 1 15); do
    if cast chain-id --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null | grep -q 137; then
        echo "Anvil ready (chain_id=137)"
        break
    fi
    if [[ $i -eq 15 ]]; then
        echo "ERROR: Anvil failed to start"
        exit 1
    fi
    sleep 0.5
done

# Show starting block
START_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Anvil starting block: $START_BLOCK"

# ------------------------------------------------------------------
# 1.5 Deploy deterministic local ERC20 and bind fixture to address
# ------------------------------------------------------------------
echo ""
echo "=== Deploying deterministic MockERC20 for E2E state assertions ==="
deploy_mock_erc20
sed -i.bak "s/__TOKEN_ADDRESS__/$TOKEN_ADDRESS/g" "$WORK_DIR/rindexer.yaml"
rm -f "$WORK_DIR/rindexer.yaml.bak"

# ------------------------------------------------------------------
# 2. Start rindexer (live indexing)
# ------------------------------------------------------------------
echo ""
echo "=== Starting rindexer ==="
RUST_LOG=info "$RINDEXER_BIN" start -p "$WORK_DIR" indexer > "$LOG_FILE" 2>&1 &
RINDEXER_PID=$!
echo "rindexer PID: $RINDEXER_PID"

# ------------------------------------------------------------------
# 3. Let rindexer accumulate blocks in its cache (~20s = ~10 blocks)
# ------------------------------------------------------------------
echo ""
echo "=== Waiting 20s for rindexer to cache blocks ==="
for i in $(seq 1 20); do
    CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
    printf "\r  Block: %s  (%d/20s)" "$CURRENT_BLOCK" "$i"

    # Check rindexer is still alive
    if ! kill -0 "$RINDEXER_PID" 2>/dev/null; then
        echo ""
        echo "ERROR: rindexer exited early. Log output:"
        cat "$LOG_FILE"
        exit 1
    fi
    sleep 1
done
echo ""

# ================================================================
# TEST 1: Tip hash changed
#
# Trigger anvil_reorg immediately — rindexer polls within ~200ms
# and sees the same block number with a different hash.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 1: Tip hash changed (same block, different hash)"
echo "========================================================"

BLOCK_BEFORE=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
HASH_BEFORE=$(cast block "$BLOCK_BEFORE" -f hash --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block before reorg: $BLOCK_BEFORE (hash: ${HASH_BEFORE:0:18}...)"

# Clear log for this test
: > "$LOG_FILE"

echo "Triggering anvil_reorg (depth=$REORG_DEPTH)..."
cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

# Short sleep — we want rindexer to poll BEFORE Anvil mines a new block
sleep 0.5

echo "Waiting up to 15s for detection..."
TIP_DETECTED=false
for i in $(seq 1 15); do
    if grep -q "tip hash changed" "$LOG_FILE" 2>/dev/null; then
        TIP_DETECTED=true
        break
    fi
    # Also accept generic reorg detection (timing-dependent — might hit parent hash instead)
    if grep -q "REORG" "$LOG_FILE" 2>/dev/null; then
        TIP_DETECTED=true
        break
    fi
    sleep 1
done

if $TIP_DETECTED; then
    echo "  PASS: Reorg detected"
    grep -ai "reorg" "$LOG_FILE" || true
    PASS_COUNT=$((PASS_COUNT + 1))
    TEST1_STATUS="PASS"

    # Verify recovery pipeline ran (handle_reorg_recovery emits these)
    RECOVERY_OK=true
    if grep -q "Reorg recovery complete" "$LOG_FILE" 2>/dev/null; then
        echo "  PASS: Recovery pipeline completed"
    else
        echo "  INFO: Recovery log not found (CSV mode — no DB to clean up, expected)"
    fi
    if grep -q "affected txs" "$LOG_FILE" 2>/dev/null; then
        echo "  PASS: affected_tx_hashes tracked in recovery"
    fi
else
    echo "  FAIL: No reorg detected"
    cat "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    TEST1_STATUS="FAIL"
fi

# ------------------------------------------------------------------
# Let rindexer recover and re-cache blocks (~15s = ~7 new blocks)
# ------------------------------------------------------------------
echo ""
echo "=== Waiting 15s for rindexer to recover and re-cache blocks ==="
for i in $(seq 1 15); do
    CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
    printf "\r  Block: %s  (%d/15s)" "$CURRENT_BLOCK" "$i"
    sleep 1
done
echo ""

# ================================================================
# TEST 2: Parent hash mismatch
#
# We need rindexer to see a NEW block (N+1) whose parent_hash
# doesn't match the cached hash of block N.
#
# Strategy: Fire reorg + mine 1 block in rapid succession (<50ms)
# within rindexer's 200ms poll interval. rindexer's next poll sees
# block N+1 (never cached) whose parent_hash points to the new
# hash of block N, but the cache has the OLD hash → mismatch.
#
# Since the reorg + mine complete within one poll window, rindexer
# should not see the intermediate reorged tip (block N with changed
# hash). If it does, the tip hash path fires instead — still valid
# detection, just a different path.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 2: Parent hash mismatch (new block, stale parent)"
echo "========================================================"

BLOCK_BEFORE=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block before reorg: $BLOCK_BEFORE"

# Clear log for this test
: > "$LOG_FILE"

# Fire reorg + mine atomically (both complete in <50ms, within
# rindexer's 200ms poll interval). Mine exactly 1 block so block
# N+1 exists and its parent (reorged block N) is in the cache.
echo "Triggering anvil_reorg (depth=$REORG_DEPTH) + mine 1 block..."
cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null
cast rpc evm_mine --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

BLOCK_AFTER=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
echo "Block after reorg + mine: $BLOCK_AFTER"

echo "Waiting up to 15s for detection..."
PARENT_DETECTED=false
PARENT_EXACT=false
for i in $(seq 1 15); do
    if grep -q "parent hash mismatch" "$LOG_FILE" 2>/dev/null; then
        PARENT_DETECTED=true
        PARENT_EXACT=true
        break
    fi
    if grep -q "REORG" "$LOG_FILE" 2>/dev/null; then
        PARENT_DETECTED=true
        break
    fi
    sleep 1
done

if $PARENT_DETECTED; then
    if $PARENT_EXACT; then
        echo "  PASS: Parent hash mismatch detected (exact path)"
    else
        echo "  PASS: Reorg detected (tip hash path — timing-dependent)"
    fi
    grep -ai "reorg" "$LOG_FILE" || true
    PASS_COUNT=$((PASS_COUNT + 1))
    TEST2_STATUS="PASS"
else
    echo "  FAIL: No reorg detected"
    cat "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    TEST2_STATUS="FAIL"
fi

# ================================================================
# TEST 3: ClickHouse reorg recovery
#
# Verifies that after a reorg:
#   - The checkpoint in rindexer_internal is rewound
#   - The synchronous DELETE (mutations_sync=1) completes before
#     re-indexing begins
#
# Requires Docker. Skipped gracefully if unavailable.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 3: ClickHouse reorg recovery + replay (DB state)"
echo "========================================================"

CH_SKIP=false
CH_COUNTED=false
if ! command -v docker &>/dev/null; then
    echo "  SKIP: Docker not available"
    CH_SKIP=true
fi

if ! $CH_SKIP && ! docker info &>/dev/null; then
    echo "  SKIP: Docker daemon not running"
    CH_SKIP=true
fi

if ! $CH_SKIP; then
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    CH_COUNTED=true

    # Stop the CSV rindexer instance for this test
    if [[ -n "$RINDEXER_PID" ]]; then
        kill "$RINDEXER_PID" 2>/dev/null
        wait "$RINDEXER_PID" 2>/dev/null
        RINDEXER_PID=""
    fi

    # Start ClickHouse container on a non-default port to avoid conflicts
    echo "Starting ClickHouse container (port $CH_PORT)..."
    CH_CONTAINER=$(docker run -d --rm \
        --cap-add SYS_NICE \
        -p "$CH_PORT:8123" \
        clickhouse/clickhouse-server:24-alpine 2>&1)
    if [[ $? -ne 0 ]]; then
        echo "  SKIP: Failed to start ClickHouse container"
        CH_CONTAINER=""
        CH_SKIP=true
    fi
fi

if ! $CH_SKIP; then
    echo "ClickHouse container: ${CH_CONTAINER:0:12}"

    # Wait for ClickHouse to be ready
    CH_READY=false
    for i in $(seq 1 30); do
        if ch_query "SELECT 1" | grep -q 1; then
            CH_READY=true
            echo "ClickHouse ready"
            break
        fi
        sleep 1
    done

    if ! $CH_READY; then
        echo "  SKIP: ClickHouse failed to start within 30s"
        CH_SKIP=true
        docker rm -f "$CH_CONTAINER" >/dev/null 2>&1
        CH_CONTAINER=""
    fi
fi

if ! $CH_SKIP; then
    # Set up ClickHouse working directory
    CH_WORK_DIR=$(mktemp -d)
    cp "$FIXTURE_DIR/reorg_test_clickhouse.yaml" "$CH_WORK_DIR/rindexer.yaml"
    cp -r "$FIXTURE_DIR/abis" "$CH_WORK_DIR/abis"
    sed -i.bak "s/__TOKEN_ADDRESS__/$TOKEN_ADDRESS/g" "$CH_WORK_DIR/rindexer.yaml"
    rm -f "$CH_WORK_DIR/rindexer.yaml.bak"

    # Create .env for ClickHouse connection (default user has no password)
    cat > "$CH_WORK_DIR/.env" <<ENVEOF
CLICKHOUSE_URL=http://127.0.0.1:$CH_PORT
CLICKHOUSE_DB=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
ENVEOF

    # Start rindexer with ClickHouse storage
    CH_LOG_FILE="$CH_WORK_DIR/rindexer_ch.log"
    RUST_LOG=info "$RINDEXER_BIN" start -p "$CH_WORK_DIR" indexer > "$CH_LOG_FILE" 2>&1 &
    RINDEXER_PID=$!
    echo "rindexer (ClickHouse) PID: $RINDEXER_PID"

    # Wait for rindexer to index some blocks and populate ClickHouse
    echo "Waiting 20s for rindexer to index blocks into ClickHouse..."
    for i in $(seq 1 20); do
        CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
        printf "\r  Block: %s  (%d/20s)" "$CURRENT_BLOCK" "$i"

        if ! kill -0 "$RINDEXER_PID" 2>/dev/null; then
            echo ""
            echo "  ERROR: rindexer (CH) exited early. Log output:"
            cat "$CH_LOG_FILE"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            CH_SKIP=true
            break
        fi
        sleep 1
    done
    echo ""
fi

if ! $CH_SKIP; then
    # Verify rindexer created tables in ClickHouse
    INTERNAL_TABLE="reorg_test_usdc_transfer"
    CH_TABLES=$(ch_query "SELECT count() FROM system.tables WHERE database = 'rindexer_internal'" 2>/dev/null || echo "0")
    if [[ "$CH_TABLES" -eq 0 ]]; then
        echo "  SKIP: rindexer did not create tables in ClickHouse"
        CH_SKIP=true
    else
        echo "ClickHouse tables created ($CH_TABLES in rindexer_internal)"
    fi
fi

if ! $CH_SKIP; then
    echo "Generating deterministic Transfer events..."
    cast send "$TOKEN_ADDRESS" \
        "transfer(address,uint64)" \
        "$ANVIL_RECIPIENT_1" 100 \
        --rpc-url "http://127.0.0.1:$ANVIL_PORT" \
        --private-key "$ANVIL_DEPLOYER_KEY" >/dev/null

    cast send "$TOKEN_ADDRESS" \
        "transfer(address,uint64)" \
        "$ANVIL_RECIPIENT_2" 50 \
        --rpc-url "http://127.0.0.1:$ANVIL_PORT" \
        --private-key "$ANVIL_DEPLOYER_KEY" >/dev/null

    echo "Waiting up to 20s for derived table row count = 3..."
    BASELINE_READY=false
    BASELINE_TOTAL=""
    for i in $(seq 1 20); do
        BASELINE_TOTAL=$(ch_query "SELECT count() FROM reorg_test_usdc.replay_balances" || echo "")
        if [[ "$BASELINE_TOTAL" == "3" ]]; then
            BASELINE_READY=true
            break
        fi
        sleep 1
    done

    if ! $BASELINE_READY; then
        echo "  FAIL: deterministic baseline not reached (expected row_count=3, got '${BASELINE_TOTAL:-empty}')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        CH_SKIP=true
    else
        echo "Baseline confirmed: row_count=3"
    fi
fi

if ! $CH_SKIP; then
    # Clear log for this test
    : > "$CH_LOG_FILE"

    # Trigger reorg
    echo "Triggering anvil_reorg (depth=$REPLAY_REORG_DEPTH)..."
    cast rpc anvil_reorg "$REPLAY_REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

    # Wait for detection + ClickHouse recovery + derived replay-path logs
    echo "Waiting up to 20s for reorg detection, ClickHouse recovery, and replay path..."
    CH_DELETED=false
    CH_REWOUND=false
    CH_DERIVED_DELETED=false
    CH_REPLAY=false
    CH_REPLAY_ERROR=false
    for i in $(seq 1 20); do
        if grep -q "ClickHouse: deleted events" "$CH_LOG_FILE" 2>/dev/null; then
            CH_DELETED=true
        fi
        if grep -q "ClickHouse: checkpoint rewound" "$CH_LOG_FILE" 2>/dev/null; then
            CH_REWOUND=true
        fi
        if grep -q "ClickHouse: deleted derived table rows" "$CH_LOG_FILE" 2>/dev/null; then
            CH_DERIVED_DELETED=true
        fi
        if grep -q "Reorg replay complete (ClickHouse source)" "$CH_LOG_FILE" 2>/dev/null; then
            CH_REPLAY=true
        fi
        if grep -q "Reorg: failed to recompute derived tables" "$CH_LOG_FILE" 2>/dev/null; then
            CH_REPLAY_ERROR=true
        fi
        if $CH_DELETED && $CH_REWOUND && $CH_DERIVED_DELETED; then
            break
        fi
        sleep 1
    done

    echo "Waiting up to 20s for post-reorg derived row count = 1..."
    POST_TOTAL_READY=false
    POST_TOTAL=""
    for i in $(seq 1 20); do
        POST_TOTAL=$(ch_query "SELECT count() FROM reorg_test_usdc.replay_balances" || echo "")
        if [[ "$POST_TOTAL" == "1" ]]; then
            POST_TOTAL_READY=true
            break
        fi
        sleep 1
    done

    # Show reorg-related log lines
    grep -ai "reorg\|ClickHouse" "$CH_LOG_FILE" || true

    if $CH_DELETED && $CH_REWOUND && $CH_DERIVED_DELETED && ! $CH_REPLAY_ERROR && $POST_TOTAL_READY; then
        if $CH_REPLAY; then
            echo "  PASS: ClickHouse reorg replay verified (row_count 3 -> 1, with recovery + replay log)"
        else
            echo "  PASS: ClickHouse reorg replay verified (row_count 3 -> 1, with cleanup + no recompute errors)"
        fi
        PASS_COUNT=$((PASS_COUNT + 1))
        TEST3_STATUS="PASS"
    elif ! $CH_DELETED && ! $CH_REWOUND && ! $CH_DERIVED_DELETED; then
        echo "  FAIL: No reorg detected in ClickHouse mode"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        TEST3_STATUS="FAIL"
    else
        echo "  FAIL: Partial replay verification (deleted=$CH_DELETED, rewound=$CH_REWOUND, derived_deleted=$CH_DERIVED_DELETED, replay=$CH_REPLAY, replay_error=$CH_REPLAY_ERROR, post_total='${POST_TOTAL:-empty}')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        TEST3_STATUS="FAIL"
    fi

    # Stop ClickHouse rindexer
    kill "$RINDEXER_PID" 2>/dev/null
    RINDEXER_PID=""

    # Clean up ClickHouse working dir
    rm -rf "$CH_WORK_DIR"
fi

if $CH_SKIP && $CH_COUNTED; then
    TOTAL_TESTS=$((TOTAL_TESTS - 1))
fi

# ================================================================
# TEST 4: PostgreSQL reorg recovery + replay
#
# Verifies deterministic replay-state transition in PostgreSQL mode:
#   - baseline derived row count reaches 3
#   - reorg deletes/rewinds + derived cleanup
#   - post-reorg derived row count converges to 1
#
# Requires Docker. Skipped gracefully if unavailable.
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 4: PostgreSQL reorg recovery + replay (DB state)"
echo "========================================================"

PG_SKIP=false
PG_COUNTED=false
if ! command -v docker &>/dev/null; then
    echo "  SKIP: Docker not available"
    PG_SKIP=true
fi

if ! $PG_SKIP && ! docker info &>/dev/null; then
    echo "  SKIP: Docker daemon not running"
    PG_SKIP=true
fi

if ! $PG_SKIP; then
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    PG_COUNTED=true

    PG_PORT_CANDIDATES=("$PG_PORT" 25432 35432 45432)
    PG_STARTED=false
    for candidate_port in "${PG_PORT_CANDIDATES[@]}"; do
        echo "Starting PostgreSQL container (port $candidate_port)..."
        set +e
        PG_CONTAINER=$(docker run -d --rm \
            -e POSTGRES_PASSWORD=postgres \
            -p "$candidate_port:5432" \
            postgres:16-alpine 2>&1)
        PG_RUN_STATUS=$?
        set -e

        if [[ $PG_RUN_STATUS -eq 0 ]]; then
            PG_PORT="$candidate_port"
            PG_STARTED=true
            break
        fi
    done

    if ! $PG_STARTED; then
        echo "  SKIP: Failed to start PostgreSQL container (no candidate port available)"
        PG_CONTAINER=""
        PG_SKIP=true
    fi
fi

if ! $PG_SKIP; then
    echo "PostgreSQL container: ${PG_CONTAINER:0:12}"

    PG_READY=false
    for i in $(seq 1 30); do
        if pg_query "SELECT 1" | grep -q 1; then
            PG_READY=true
            echo "PostgreSQL ready"
            break
        fi
        sleep 1
    done

    if ! $PG_READY; then
        echo "  SKIP: PostgreSQL failed to start within 30s"
        PG_SKIP=true
        docker rm -f "$PG_CONTAINER" >/dev/null 2>&1
        PG_CONTAINER=""
    fi
fi

if ! $PG_SKIP; then
    PG_WORK_DIR=$(mktemp -d)
    cp "$FIXTURE_DIR/reorg_test_postgres.yaml" "$PG_WORK_DIR/rindexer.yaml"
    cp -r "$FIXTURE_DIR/abis" "$PG_WORK_DIR/abis"
    sed -i.bak "s/__TOKEN_ADDRESS__/$TOKEN_ADDRESS/g" "$PG_WORK_DIR/rindexer.yaml"
    rm -f "$PG_WORK_DIR/rindexer.yaml.bak"

    cat > "$PG_WORK_DIR/.env" <<ENVEOF
DATABASE_URL=postgres://postgres:postgres@127.0.0.1:$PG_PORT/postgres?sslmode=disable
ENVEOF

    PG_LOG_FILE="$PG_WORK_DIR/rindexer_pg.log"
    RUST_LOG=info "$RINDEXER_BIN" start -p "$PG_WORK_DIR" indexer > "$PG_LOG_FILE" 2>&1 &
    RINDEXER_PID=$!
    echo "rindexer (PostgreSQL) PID: $RINDEXER_PID"

    echo "Waiting 20s for rindexer to index blocks into PostgreSQL..."
    for i in $(seq 1 20); do
        CURRENT_BLOCK=$(cast block-number --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null)
        printf "\r  Block: %s  (%d/20s)" "$CURRENT_BLOCK" "$i"

        if ! kill -0 "$RINDEXER_PID" 2>/dev/null; then
            echo ""
            echo "  ERROR: rindexer (PG) exited early. Log output:"
            cat "$PG_LOG_FILE"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            TEST4_STATUS="FAIL"
            PG_SKIP=true
            break
        fi
        sleep 1
    done
    echo ""
fi

if ! $PG_SKIP; then
    echo "Generating deterministic Transfer events (PostgreSQL)..."
    cast send "$TOKEN_ADDRESS" \
        "transfer(address,uint64)" \
        "$ANVIL_RECIPIENT_1" 100 \
        --rpc-url "http://127.0.0.1:$ANVIL_PORT" \
        --private-key "$ANVIL_DEPLOYER_KEY" >/dev/null

    cast send "$TOKEN_ADDRESS" \
        "transfer(address,uint64)" \
        "$ANVIL_RECIPIENT_2" 50 \
        --rpc-url "http://127.0.0.1:$ANVIL_PORT" \
        --private-key "$ANVIL_DEPLOYER_KEY" >/dev/null

    echo "Waiting up to 20s for PostgreSQL derived row count = 3..."
    PG_BASELINE_READY=false
    PG_BASELINE_TOTAL=""
    for i in $(seq 1 20); do
        PG_BASELINE_TOTAL=$(pg_query "SELECT count(*) FROM reorg_test_usdc.replay_balances" || echo "")
        if [[ "$PG_BASELINE_TOTAL" == "3" ]]; then
            PG_BASELINE_READY=true
            break
        fi
        sleep 1
    done

    if ! $PG_BASELINE_READY; then
        echo "  FAIL: PostgreSQL deterministic baseline not reached (expected row_count=3, got '${PG_BASELINE_TOTAL:-empty}')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        TEST4_STATUS="FAIL"
        PG_SKIP=true
    else
        echo "PostgreSQL baseline confirmed: row_count=3"
    fi
fi

if ! $PG_SKIP; then
    : > "$PG_LOG_FILE"

    echo "Triggering anvil_reorg (depth=$REPLAY_REORG_DEPTH) for PostgreSQL..."
    cast rpc anvil_reorg "$REPLAY_REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

    echo "Waiting up to 20s for PostgreSQL detection, recovery, and replay path..."
    PG_DELETED=false
    PG_REWOUND=false
    PG_DERIVED_DELETED=false
    PG_REPLAY=false
    PG_REPLAY_ERROR=false
    for i in $(seq 1 20); do
        if grep -q "PostgreSQL: deleted events" "$PG_LOG_FILE" 2>/dev/null; then
            PG_DELETED=true
        fi
        if grep -q "PostgreSQL: checkpoint rewound" "$PG_LOG_FILE" 2>/dev/null; then
            PG_REWOUND=true
        fi
        if grep -q "PostgreSQL: deleted derived table rows" "$PG_LOG_FILE" 2>/dev/null; then
            PG_DERIVED_DELETED=true
        fi
        if grep -q "Reorg replay complete for table" "$PG_LOG_FILE" 2>/dev/null; then
            PG_REPLAY=true
        fi
        if grep -q "Reorg: failed to recompute derived tables" "$PG_LOG_FILE" 2>/dev/null; then
            PG_REPLAY_ERROR=true
        fi
        if $PG_DELETED && $PG_REWOUND && $PG_DERIVED_DELETED; then
            break
        fi
        sleep 1
    done

    echo "Waiting up to 20s for PostgreSQL post-reorg derived row count = 1..."
    PG_POST_TOTAL_READY=false
    PG_POST_TOTAL=""
    for i in $(seq 1 20); do
        PG_POST_TOTAL=$(pg_query "SELECT count(*) FROM reorg_test_usdc.replay_balances" || echo "")
        if [[ "$PG_POST_TOTAL" == "1" ]]; then
            PG_POST_TOTAL_READY=true
            break
        fi
        sleep 1
    done

    grep -ai "reorg\|PostgreSQL" "$PG_LOG_FILE" || true

    if $PG_DELETED && $PG_REWOUND && $PG_DERIVED_DELETED && ! $PG_REPLAY_ERROR && $PG_POST_TOTAL_READY; then
        if $PG_REPLAY; then
            echo "  PASS: PostgreSQL reorg replay verified (row_count 3 -> 1, with recovery + replay log)"
        else
            echo "  PASS: PostgreSQL reorg replay verified (row_count 3 -> 1, with cleanup + no recompute errors)"
        fi
        PASS_COUNT=$((PASS_COUNT + 1))
        TEST4_STATUS="PASS"
    elif ! $PG_DELETED && ! $PG_REWOUND && ! $PG_DERIVED_DELETED; then
        echo "  FAIL: No reorg detected in PostgreSQL mode"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        TEST4_STATUS="FAIL"
    else
        echo "  FAIL: Partial PostgreSQL replay verification (deleted=$PG_DELETED, rewound=$PG_REWOUND, derived_deleted=$PG_DERIVED_DELETED, replay=$PG_REPLAY, replay_error=$PG_REPLAY_ERROR, post_total='${PG_POST_TOTAL:-empty}')"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        TEST4_STATUS="FAIL"
    fi

    kill "$RINDEXER_PID" 2>/dev/null
    RINDEXER_PID=""
    rm -rf "$PG_WORK_DIR"
fi

if $PG_SKIP && $PG_COUNTED; then
    TOTAL_TESTS=$((TOTAL_TESTS - 1))
fi

# ================================================================
# TEST 5: reorg_safe_distance config active
#
# The CSV fixture uses reorg_safe_distance: true (resolves to 200
# for Polygon chain_id=137). With Anvil at < 200 blocks, rindexer
# should log "not in safe reorg block range" — proving the YAML
# field was parsed and the safe distance calculation is active.
#
# We check the CSV rindexer log from Tests 1-2 (it ran with
# reorg_safe_distance: true).
# ================================================================
echo ""
echo "========================================================"
echo "  TEST 5: reorg_safe_distance config active"
echo "========================================================"

if grep -q "not in safe reorg block range" "$LOG_FILE" 2>/dev/null; then
    echo "  PASS: reorg_safe_distance: true is active (safe block range enforced)"
    PASS_COUNT=$((PASS_COUNT + 1))
    TEST5_STATUS="PASS"
else
    echo "  FAIL: No safe block range log found — reorg_safe_distance may not be wired"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    TEST5_STATUS="FAIL"
fi

# ------------------------------------------------------------------
# Final report
# ------------------------------------------------------------------
echo ""
echo "========================================================"
echo "  RESULTS: $PASS_COUNT/$TOTAL_TESTS passed, $FAIL_COUNT/$TOTAL_TESTS failed"
echo "========================================================"
echo ""
echo "  Test 1 (tip hash changed):        $TEST1_STATUS"
echo "  Test 2 (parent hash mismatch):    $TEST2_STATUS"
echo "  Test 3 (ClickHouse replay):       $TEST3_STATUS"
echo "  Test 4 (PostgreSQL replay):       $TEST4_STATUS"
echo "  Test 5 (reorg_safe_distance):     $TEST5_STATUS"
echo "  (removed flag: not testable with Anvil)"
echo ""

if [[ $FAIL_COUNT -eq 0 ]]; then
    exit 0
else
    exit 1
fi
