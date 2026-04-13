#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 12: PG Kill During Commit ==="
echo "Kills PostgreSQL while data is flowing, verifies the writer"
echo "recovers after PG comes back without data loss."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-kill envelope count..."
pre_kill=$(count_envelopes)

step 2 "Killing PostgreSQL..."
event_start_ns=$(ts_now_ns)
$COMPOSE kill postgres 2>&1

step 3 "Waiting 15s (writer continues to receive but can't commit to PG)..."
sleep 15

step 4 "Restarting PostgreSQL..."
$COMPOSE up -d postgres 2>&1
wait_service_healthy postgres 60
event_end_ns=$(ts_now_ns)

step 5 "Waiting for writer to recover and resume..."
if wait_for_envelope_count_gt "$pre_kill" 60; then
    pass "writer resumed after PG recovery"
else
    fail "writer did not resume after PG recovery"
fi

section "Verification"
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

post_recovery=$(count_envelopes)
assert_gt "archive grew after PG recovery" "$post_recovery" "$pre_kill"

print_test_report
teardown_stack
print_results
