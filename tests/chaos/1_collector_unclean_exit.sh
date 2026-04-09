#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 1: Kill Collector (Unclean Exit) ==="
echo "Verifies that killing the primary collector triggers writer failover"
echo "to backup topics, then switchback when the primary restarts."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-kill state..."
pre_kill=$(count_envelopes)
event_start_ns=$(ts_now_ns)

step 2 "Killing collector to simulate unclean exit..."
docker kill "${COLLECTOR_CONTAINER}"

step 3 "Waiting for writer to activate failover (up to 30s)..."
if wait_for_failover_state 1 30; then
    pass "writer activated failover to backup"
else
    fail "writer did not activate failover"
fi

step 4 "Letting backup data flow for 15s..."
wait_for_data 15

step 5 "Restarting collector..."
$COMPOSE up -d collector 2>&1
event_end_ns=$(ts_now_ns)

step 6 "Waiting for collector healthcheck..."
if wait_service_healthy collector 60; then
    pass "collector restarted and healthy"
else
    fail "collector failed to become healthy"
fi

step 7 "Waiting for writer to switchback to primary (up to 60s)..."
if wait_for_failover_state 0 60; then
    pass "writer switched back to primary"
else
    fail "writer did not switchback to primary"
fi

step 8 "Checking collector skipped depth snapshot (backup had recent data)..."
if $COMPOSE logs collector 2>&1 | grep -q "depth_resync_skipped_snapshot"; then
    pass "collector skipped depth resync snapshot"
else
    fail "collector did NOT skip depth resync snapshot"
fi

section "Verification"

# Failover metrics
failover_total=$(get_writer_metric writer_failover_total 2>/dev/null || echo "0")
assert_gt "at least one failover activation recorded" "$failover_total" 0

switchback_total=$(get_writer_metric writer_switchback_total 2>/dev/null || echo "0")
assert_gt "at least one switchback recorded" "$switchback_total" 0

failover_active=$(get_writer_metric writer_failover_active 2>/dev/null || echo "-1")
assert_eq "failover is inactive after switchback" "0" "$failover_active"

# Both collector and writer should be healthy after recovery
assert_container_healthy "collector"
assert_container_healthy "collector-backup"
assert_container_healthy "writer"

# Data integrity should be intact
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Archive should have data from both sessions and more than before kill
post_recovery=$(count_envelopes)
assert_gt "archive has envelopes from both sessions" "$post_recovery" "$pre_kill"

# Validate any gap records have valid timestamps
if validate_any_gap_timestamps; then
    pass "all gap records (if any) have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

step 9 "Stopping both collectors to quiesce input before archive verification..."
$COMPOSE stop collector collector-backup 2>&1
if wait_for_writer_lag_below 10 90; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

step 10 "Running cryptolake verify..."
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
