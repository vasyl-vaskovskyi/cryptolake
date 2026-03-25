#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 1: Kill Collector (Unclean Exit) ==="
echo "Verifies that killing the collector process produces restart_gap records"
echo "in the archive with component=collector and cause=unclean_exit."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-kill state..."
pre_kill=$(count_envelopes)
event_start_ns=$(ts_now_ns)

step 2 "Killing collector to simulate unclean exit..."
docker kill "${COLLECTOR_CONTAINER}"
sleep 5

step 3 "Restarting collector..."
$COMPOSE up -d collector 2>&1
event_end_ns=$(ts_now_ns)

step 4 "Waiting for restart_gap records to appear in archive..."
wait_for_gaps "restart_gap" 60

step 5 "Waiting for collector healthcheck..."
if ! wait_service_healthy collector 30; then
    :
fi

section "Verification"

# Writer should detect the session change and emit restart_gap records
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

# Validate structured restart_gap metadata: component=collector, cause=unclean_exit
if validate_restart_gap_fields "collector" "unclean_exit"; then
    pass "restart_gap metadata: component=collector, cause=unclean_exit, planned=false"
else
    fail "restart_gap metadata validation failed"
fi

# Validate gap timestamp ordering (gap_end_ts > gap_start_ts > 0)
if validate_any_gap_timestamps; then
    pass "all gap records have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

# Both collector and writer should be healthy after recovery
assert_container_healthy "collector"
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

# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 60; then
    pass "restart_gap gap timestamps are accurate (within 60s tolerance)"
else
    fail "restart_gap gap timestamp accuracy check failed"
fi

step 7 "Stopping collector to quiesce input before archive verification..."
$COMPOSE stop collector 2>&1
if wait_for_writer_lag_below 0 30; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

step 8 "Running cryptolake verify..."
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
