#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Depth Reconnect Inflight ==="
echo "Kills the collector during active depth flow and verifies that depth"
echo "data resynchronises correctly after reconnect."
echo ""

setup_stack

echo "1. Letting collector stream depth updates for 30s..."
wait_for_data 30

echo "2. Killing collector during active depth flow..."
docker kill "${COLLECTOR_CONTAINER}"
sleep 5

echo "3. Restarting collector..."
$COMPOSE up -d collector 2>&1

echo "4. Waiting 60s for depth snapshot resync..."
sleep 60

echo "5. Verifying results..."

assert_container_healthy "collector"
assert_container_healthy "writer"

# Writer should detect collector session change
gaps=$(count_gaps "collector_restart")
assert_gt "collector_restart gaps exist after depth reconnect" "$gaps" 0

# Data integrity
if check_integrity; then
    pass "data integrity OK (depth replay remains valid after reconnect)"
else
    fail "data integrity check failed"
fi

# Archive should have depth data
total=$(count_envelopes)
assert_gt "archive has envelopes" "$total" 0

print_test_report
teardown_stack
print_results
