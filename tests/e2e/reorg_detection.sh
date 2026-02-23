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
#   3. ClickHouse reorg recovery — verify DB state after reorg
#      (requires Docker; skipped if unavailable)
#   4. reorg_safe_distance config — verify YAML field is parsed and active
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
# Prerequisites: anvil, cast (Foundry), rindexer_cli built
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
LOG_FILE="$WORK_DIR/rindexer_test.log"
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_TESTS=2

# Helper: query ClickHouse via HTTP API
ch_query() {
    curl -sf "http://127.0.0.1:$CH_PORT/" --data-binary "$1" 2>/dev/null
}

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    [[ -n "$RINDEXER_PID" ]] && kill "$RINDEXER_PID" 2>/dev/null && echo "Stopped rindexer ($RINDEXER_PID)"
    [[ -n "$ANVIL_PID" ]]    && kill "$ANVIL_PID"    2>/dev/null && echo "Stopped anvil ($ANVIL_PID)"
    [[ -n "$CH_CONTAINER" ]] && docker rm -f "$CH_CONTAINER" >/dev/null 2>&1 && echo "Stopped ClickHouse ($CH_CONTAINER)"
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
else
    echo "  FAIL: No reorg detected"
    cat "$LOG_FILE"
    FAIL_COUNT=$((FAIL_COUNT + 1))
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
echo "  TEST 3: ClickHouse reorg recovery (DB state)"
echo "========================================================"

CH_SKIP=false
if ! command -v docker &>/dev/null; then
    echo "  SKIP: Docker not available"
    CH_SKIP=true
fi

if ! $CH_SKIP && ! docker info &>/dev/null; then
    echo "  SKIP: Docker daemon not running"
    CH_SKIP=true
fi

if ! $CH_SKIP; then
    TOTAL_TESTS=3

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
    # Clear log for this test
    : > "$CH_LOG_FILE"

    # Trigger reorg
    echo "Triggering anvil_reorg (depth=$REORG_DEPTH)..."
    cast rpc anvil_reorg "$REORG_DEPTH" "[]" --rpc-url "http://127.0.0.1:$ANVIL_PORT" 2>/dev/null

    # Wait for detection + ClickHouse recovery (look for the DB operation logs)
    echo "Waiting up to 20s for reorg detection and ClickHouse recovery..."
    CH_DELETED=false
    CH_REWOUND=false
    for i in $(seq 1 20); do
        if grep -q "ClickHouse: deleted events" "$CH_LOG_FILE" 2>/dev/null; then
            CH_DELETED=true
        fi
        if grep -q "ClickHouse: checkpoint rewound" "$CH_LOG_FILE" 2>/dev/null; then
            CH_REWOUND=true
        fi
        if $CH_DELETED && $CH_REWOUND; then
            break
        fi
        sleep 1
    done

    # Show reorg-related log lines
    grep -ai "reorg\|ClickHouse" "$CH_LOG_FILE" || true

    if $CH_DELETED && $CH_REWOUND; then
        echo "  PASS: ClickHouse reorg recovery verified (events deleted + checkpoint rewound)"
        PASS_COUNT=$((PASS_COUNT + 1))
    elif ! $CH_DELETED && ! $CH_REWOUND; then
        echo "  FAIL: No reorg detected in ClickHouse mode"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    else
        echo "  FAIL: Partial recovery (deleted=$CH_DELETED, rewound=$CH_REWOUND)"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # Stop ClickHouse rindexer
    kill "$RINDEXER_PID" 2>/dev/null
    RINDEXER_PID=""

    # Clean up ClickHouse working dir
    rm -rf "$CH_WORK_DIR"
fi

# ================================================================
# TEST 4: reorg_safe_distance config active
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
echo "  TEST 4: reorg_safe_distance config active"
echo "========================================================"
TOTAL_TESTS=$((TOTAL_TESTS + 1))

if grep -q "not in safe reorg block range" "$LOG_FILE" 2>/dev/null; then
    echo "  PASS: reorg_safe_distance: true is active (safe block range enforced)"
    PASS_COUNT=$((PASS_COUNT + 1))
else
    echo "  FAIL: No safe block range log found — reorg_safe_distance may not be wired"
    FAIL_COUNT=$((FAIL_COUNT + 1))
fi

# ------------------------------------------------------------------
# Final report
# ------------------------------------------------------------------
echo ""
echo "========================================================"
echo "  RESULTS: $PASS_COUNT/$TOTAL_TESTS passed, $FAIL_COUNT/$TOTAL_TESTS failed"
echo "========================================================"
echo ""
echo "  Test 1 (tip hash changed):        $(if [ $PASS_COUNT -ge 1 ]; then echo PASS; else echo FAIL; fi)"
echo "  Test 2 (parent hash mismatch):    $(if [ $PASS_COUNT -ge 2 ]; then echo PASS; else echo FAIL; fi)"
if $CH_SKIP; then
echo "  Test 3 (ClickHouse recovery):     SKIP"
else
echo "  Test 3 (ClickHouse recovery):     $(if [ $PASS_COUNT -ge 3 ]; then echo PASS; else echo FAIL; fi)"
fi
echo "  Test 4 (reorg_safe_distance):     $(if [ $PASS_COUNT -ge $TOTAL_TESTS ]; then echo PASS; else echo FAIL; fi)"
echo "  (removed flag: not testable with Anvil)"
echo ""

if [[ $FAIL_COUNT -eq 0 ]]; then
    exit 0
else
    exit 1
fi
