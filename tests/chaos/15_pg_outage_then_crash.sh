#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: PG Outage + Writer Crash (Compound Failure) ==="
echo "Kills PG, lets writer run without commits, then kills writer too."
echo "Verifies recovery after both are restored."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-chaos envelope count..."
pre_chaos=$(count_envelopes)

echo "2. Killing PostgreSQL..."
$COMPOSE kill postgres 2>&1

echo "3. Waiting 15s (writer runs without PG)..."
sleep 15

echo "4. Killing writer (compound failure)..."
docker kill "${WRITER_CONTAINER}" 2>/dev/null || echo "   (writer already exited — expected in compound failure)"

echo "5. Restoring PostgreSQL..."
$COMPOSE up -d postgres 2>&1
wait_service_healthy postgres 60

echo "6. Restoring writer..."
$COMPOSE up -d writer 2>&1
if ! wait_service_healthy writer 30; then :; fi

echo "7. Waiting for data flow to resume..."
if wait_for_envelope_count_gt "$pre_chaos" 60; then
    pass "data flow resumed after compound failure"
else
    fail "data flow did not resume"
fi

echo "8. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK after compound failure"
else
    fail "data integrity check failed"
fi

post_recovery=$(count_envelopes)
assert_gt "archive grew after recovery" "$post_recovery" "$pre_chaos"

print_test_report
teardown_stack
print_results
