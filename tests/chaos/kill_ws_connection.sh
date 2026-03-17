#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Kill WebSocket Connection ==="
echo "Verifies that killing the collector produces collector_restart gap"
echo "records in the archive when the writer detects the session change."
echo ""

setup_stack
wait_for_data 30

echo "1. Killing collector to simulate ws_disconnect..."
docker kill "${COLLECTOR_CONTAINER}"
sleep 10

echo "2. Restarting collector..."
$COMPOSE up -d collector 2>&1
wait_for_data 40

echo "3. Verifying results..."

# Writer should detect the session change and emit gaps
gaps=$(count_gaps "collector_restart")
assert_gt "collector_restart gaps exist in archive" "$gaps" 0

# Both collector and writer should be healthy after recovery
assert_container_healthy "collector"
assert_container_healthy "writer"

# Data integrity should be intact
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Archive should have data from both sessions
total=$(count_envelopes)
assert_gt "archive has envelopes from both sessions" "$total" 100

print_test_report
teardown_stack
print_results
