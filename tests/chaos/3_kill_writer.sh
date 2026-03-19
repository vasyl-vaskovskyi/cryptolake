#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos: Kill Writer ==="
echo "Verifies that the writer catches up after being killed while data"
echo "accumulates in Redpanda, and runs cryptolake verify successfully."
echo ""

setup_stack
wait_for_data 30

echo "1. Recording pre-kill envelope count..."
pre_kill=$(count_envelopes)

echo "2. Killing writer container..."
docker kill "${WRITER_CONTAINER}"

echo "3. Waiting 30s (data accumulates in Redpanda)..."
sleep 30

echo "4. Restarting writer..."
$COMPOSE up -d writer 2>&1

echo "5. Waiting for archive writes to resume..."
if wait_for_envelope_count_gt "$pre_kill" 90; then
    pass "writer resumed writing after restart"
else
    fail "writer did not resume writing after restart"
fi

echo "6. Waiting for writer healthcheck..."
if ! wait_service_healthy writer 30; then
    :
fi

echo "7. Verifying results..."

assert_container_healthy "writer"
assert_container_healthy "collector"

# Writer should have caught up — more data than before the kill
post_recovery=$(count_envelopes)
assert_gt "writer caught up (more envelopes after recovery)" "$post_recovery" "$pre_kill"

# Data integrity
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

echo "8. Stopping collector to quiesce input before archive verification..."
$COMPOSE stop collector 2>&1
if wait_for_writer_lag_below 0 30; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

echo "9. Running cryptolake verify..."
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
