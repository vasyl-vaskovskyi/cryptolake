#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: True WebSocket Disconnect ==="
echo "Blocks collector network to trigger ws_disconnect gaps while"
echo "the collector process stays alive, then restores and verifies."
echo ""

setup_stack
wait_for_data 30

echo "1. Recording pre-disconnect timestamps..."
event_start_ns=$(ts_now_ns)

echo "2. Blocking collector network (iptables DROP)..."
block_egress "${COLLECTOR_CONTAINER}"

echo "3. Waiting 30s for WebSocket timeout and disconnect detection..."
sleep 30

echo "4. Restoring collector network..."
unblock_egress "${COLLECTOR_CONTAINER}"
event_end_ns=$(ts_now_ns)

echo "5. Waiting 60s for reconnection and data flow..."
sleep 60

echo "6. Verifying results..."

# Collector should still be running (not killed, just disconnected)
assert_container_healthy "collector"
assert_container_healthy "writer"

# ws_disconnect gaps should exist in archive
wait_for_gaps "ws_disconnect" 90
ws_gaps=$(count_gaps "ws_disconnect")
assert_gt "ws_disconnect gaps exist in archive" "$ws_gaps" 0

# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "ws_disconnect" "$event_start_ns" "$event_end_ns" 60; then
    pass "ws_disconnect gap timestamps are accurate (within 60s tolerance)"
else
    fail "ws_disconnect gap timestamp accuracy check failed"
fi

# Data integrity should be intact after reconnection
if check_integrity; then
    pass "data integrity OK after ws_disconnect recovery"
else
    fail "data integrity check failed"
fi

# Archive should have data from before and after the disconnect
total=$(count_envelopes)
assert_gt "archive has envelopes spanning the disconnect" "$total" 100

print_test_report
teardown_stack
print_results
