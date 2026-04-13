#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 2: Buffer Overflow Recovery ==="
echo "Stops Redpanda to force collector buffer growth, then restarts and"
echo "verifies the collector recovers with buffer_overflow gaps recorded."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 0 "Stopping backup collector (this test only exercises primary)..."
$COMPOSE stop collector-backup 2>&1

step 1 "Stopping redpanda to force collector buffer growth..."
event_start_ns=$(ts_now_ns)
$COMPOSE stop redpanda 2>&1

step 2 "Waiting for collector buffer to overflow..."
wait_for_overflow 60

step 3 "Restarting redpanda..."
$COMPOSE up -d redpanda 2>&1
echo "   Waiting for redpanda to become healthy..."
# We don't use wait_healthy here because the writer is expected to have crashed
# during the redpanda outage (session timeout).
wait_service_healthy redpanda

step 4 "Force-restarting writer (crashes on rebalance after broker reconnect)..."
$COMPOSE stop writer 2>&1 || true
$COMPOSE up -d writer 2>&1
event_end_ns=$(ts_now_ns)
echo "   Waiting for services to become healthy..."
wait_service_healthy redpanda 60
wait_service_healthy collector 60
wait_service_healthy writer 60

step 5 "Waiting for buffer_overflow gaps to appear in archive..."
wait_for_gaps "buffer_overflow" 60

section "Verification"

assert_container_healthy "collector"
assert_container_healthy "writer"
assert_container_healthy "redpanda"

# Collector should have emitted buffer_overflow gaps
overflow_gaps=$(count_gaps "buffer_overflow")
assert_gt "buffer_overflow gaps exist in archive" "$overflow_gaps" 0

# Validate buffer_overflow gap timestamps (gap_end_ts > gap_start_ts > 0)
if validate_any_gap_timestamps; then
    pass "all gap records have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

# Writer was force-restarted, so restart_gap records may exist.
# If present, they must have planned=false.
if validate_restart_gap_fields; then
    pass "restart_gap metadata valid (planned=false if gaps exist)"
else
    fail "restart_gap metadata validation failed"
fi

# Validate buffer_overflow gap timestamps are in the right ballpark
if validate_gap_window_accuracy "buffer_overflow" "$event_start_ns" "$event_end_ns" 120; then
    pass "buffer_overflow gap timestamps accurate (within 120s tolerance)"
else
    fail "buffer_overflow gap timestamp accuracy check failed"
fi

# Data integrity
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

# Archive should have data
total=$(count_envelopes)
assert_gt "archive has envelopes spanning the outage" "$total" 100

step 6 "Stopping collector to quiesce input before archive verification..."
$COMPOSE stop collector collector-backup 2>&1
if wait_for_writer_lag_below 10 90; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

step 7 "Running cryptolake verify..."
if UV_CACHE_DIR="${REPO_ROOT}/.tmp/uv-cache" \
    uv run cryptolake verify \
        --date "${test_date}" \
        --base-dir "${TEST_DATA_DIR}" \
        --full \
        --repair-checksums; then
    pass "cryptolake verify passed"
else
    fail "cryptolake verify failed"
fi

print_test_report
teardown_stack
print_results
