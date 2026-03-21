#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Corrupt Message in Redpanda ==="
echo "Injects corrupt (non-JSON) messages and verifies the writer"
echo "skips them and continues processing valid data."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-injection envelope count..."
pre_inject=$(count_envelopes)

echo "2. Injecting corrupt messages into Redpanda..."
inject_corrupt_message "binance.trades"
inject_corrupt_message "binance.bookticker"

echo "3. Waiting for writer to process past corrupt messages..."
if wait_for_envelope_count_gt "$pre_inject" 60; then
    pass "writer continued processing after corrupt messages"
else
    fail "writer stopped processing after corrupt messages"
fi

echo "4. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

post_inject=$(count_envelopes)
assert_gt "archive grew after corrupt injection" "$post_inject" "$pre_inject"

print_test_report
teardown_stack
print_results
