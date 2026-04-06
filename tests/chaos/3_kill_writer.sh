#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 3: Kill Writer ==="
echo "Verifies that the writer catches up after being killed while data"
echo "accumulates in Redpanda, and runs cryptolake verify successfully."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-kill envelope count..."
pre_kill=$(count_envelopes)

step 2 "Killing writer container..."
event_start_ns=$(ts_now_ns)
docker kill "${WRITER_CONTAINER}"

step 3 "Waiting 20s (data accumulates in Redpanda)..."
sleep 20

step 4 "Restarting writer..."
$COMPOSE up -d writer 2>&1
event_end_ns=$(ts_now_ns)

step 5 "Waiting for archive writes to resume..."
if wait_for_envelope_count_gt "$pre_kill" 60; then
    pass "writer resumed writing after restart"
else
    fail "writer did not resume writing after restart"
fi

step 6 "Waiting for writer healthcheck..."
if ! wait_service_healthy writer 30; then
    :
fi

section "Verification"

assert_container_healthy "writer"
assert_container_healthy "collector"
assert_container_healthy "collector-backup"

# Writer should have caught up — more data than before the kill
post_recovery=$(count_envelopes)
assert_gt "writer caught up (more envelopes after recovery)" "$post_recovery" "$pre_kill"

# Data integrity
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Validate any gap records have valid timestamps (gap_end_ts > gap_start_ts > 0)
if validate_any_gap_timestamps; then
    pass "all gap records (if any) have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

# Validate gap metadata: docker kill sends SIGKILL (unplanned), so planned=false
# Writer may or may not produce gaps depending on SIGTERM handling,
# but if gaps exist they must have correct metadata.
if validate_restart_gap_fields; then
    pass "restart_gap metadata valid (planned=false if gaps exist)"
else
    fail "restart_gap metadata validation failed"
fi

# Validate gap timestamps are in the right ballpark (if any gaps exist)
gaps=$(count_gaps "restart_gap")
if (( gaps > 0 )); then
    if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 60; then
        pass "restart_gap timestamps accurate (within 60s tolerance)"
    else
        fail "restart_gap timestamp accuracy check failed"
    fi
else
    pass "no restart_gap records (writer handled shutdown gracefully)"
fi

step 8 "Stopping both collectors to quiesce input before archive verification..."
$COMPOSE stop collector collector-backup 2>&1
if wait_for_writer_lag_below 0 90; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

step 9 "Running cryptolake verify..."
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
