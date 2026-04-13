#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 15: Redpanda Leader Change ==="
echo "Restarts Redpanda to force partition leadership changes."
echo "Verifies no data loss across the re-election."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-restart envelope count..."
pre_restart=$(count_envelopes)

step 2 "Restarting Redpanda (forces leader re-election)..."
$COMPOSE restart redpanda 2>&1
wait_service_healthy redpanda 60

step 3 "Waiting for writer and collector to reconnect..."
wait_healthy

step 4 "Waiting for data flow to resume..."
if wait_for_envelope_count_gt "$pre_restart" 60; then
    pass "data flow resumed after Redpanda restart"
else
    fail "data flow did not resume"
fi

section "Verification"
assert_container_healthy "writer"
assert_container_healthy "collector"
assert_container_healthy "redpanda"

if check_integrity; then
    pass "data integrity OK after leader re-election"
else
    fail "data integrity check failed"
fi

post_restart=$(count_envelopes)
assert_gt "archive grew after Redpanda restart" "$post_restart" "$pre_restart"

print_test_report
teardown_stack
print_results
