#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 17: Collector Failover to Backup ==="
echo "Verifies that when the primary collector dies, the writer switches"
echo "to consuming from backup.* topics in real time, then switches back"
echo "when the primary resumes — with zero duplicates and seamless data."
echo ""

setup_stack

section "Start backup collector"
step 1 "Starting collector-backup service..."
$COMPOSE up -d collector-backup 2>&1
if wait_service_healthy collector-backup 60; then
    pass "collector-backup is healthy"
else
    fail "collector-backup failed to become healthy"
fi

step 2 "Letting both collectors publish data..."
wait_for_data 25

section "Scenario: Kill primary, verify failover"
step 3 "Recording pre-kill state..."
pre_kill=$(count_envelopes)
event_start_ns=$(ts_now_ns)

step 4 "Killing primary collector (SIGKILL)..."
docker kill "${COLLECTOR_CONTAINER}"

step 5 "Waiting for writer to activate failover (up to 30s)..."
if wait_for_failover_state 1 30; then
    pass "writer activated failover"
else
    fail "writer did not activate failover"
fi

step 6 "Letting backup data flow for 30s..."
wait_for_data 30

step 7 "Checking failover is still active after 30s of backup consumption..."
failover_still_active=$(get_writer_metric writer_failover_active 2>/dev/null || echo "0")
assert_eq "failover still active during backup consumption" "1" "$failover_still_active"

section "Scenario: Restart primary, verify switchback"
step 8 "Restarting primary collector..."
$COMPOSE up -d collector 2>&1
event_end_ns=$(ts_now_ns)

step 9 "Waiting for primary collector healthcheck..."
if wait_service_healthy collector 60; then
    pass "primary collector is healthy"
else
    fail "primary collector failed to become healthy"
fi

step 10 "Waiting for writer to switchback to primary (up to 60s)..."
if wait_for_failover_state 0 60; then
    pass "writer switched back to primary"
else
    fail "writer did not switchback to primary"
fi

section "Verification"

# Failover metrics
failover_total=$(get_writer_metric writer_failover_total 2>/dev/null || echo "0")
assert_gt "at least one failover activation recorded" "$failover_total" 0

switchback_total=$(get_writer_metric writer_switchback_total 2>/dev/null || echo "0")
assert_gt "at least one switchback recorded" "$switchback_total" 0

final_failover_records=$(get_writer_metric writer_failover_records_total 2>/dev/null || echo "0")
assert_gt "records consumed from backup topics" "$final_failover_records" 0

# Failover should now be inactive
failover_active=$(get_writer_metric writer_failover_active 2>/dev/null || echo "-1")
assert_eq "failover is inactive after switchback" "0" "$failover_active"

# Both collectors and writer should be healthy
assert_container_healthy "collector"
assert_container_healthy "collector-backup"
assert_container_healthy "writer"

# Data integrity: no corrupt frames, no duplicate offsets
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Archive should have more data than before the kill (data continued via backup)
post_recovery=$(count_envelopes)
assert_gt "archive grew during failover period" "$post_recovery" "$pre_kill"

# Validate gap timestamps if any gaps were emitted
if validate_any_gap_timestamps; then
    pass "all gap records (if any) have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

step 11 "Stopping both collectors to quiesce input..."
$COMPOSE stop collector collector-backup 2>&1
if wait_for_writer_lag_below 0 30; then
    pass "writer drained remaining backlog"
else
    fail "writer still had backlog after collector stop"
fi

step 12 "Running cryptolake verify..."
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
