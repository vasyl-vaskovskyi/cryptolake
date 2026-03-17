#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Buffer Overflow Recovery ==="
echo "Stops Redpanda to force collector buffer growth, then restarts and"
echo "verifies the collector recovers with buffer_overflow gaps recorded."
echo ""

setup_stack
wait_for_data 30

echo "1. Stopping redpanda to force collector buffer growth..."
$COMPOSE stop redpanda 2>&1

echo "2. Waiting 45s while collector buffers messages..."
sleep 45

echo "3. Restarting redpanda..."
$COMPOSE up -d redpanda 2>&1

echo "4. Waiting 60s for buffer drain and recovery..."
sleep 60

echo "5. Verifying results..."

assert_container_healthy "collector"
assert_container_healthy "writer"
assert_container_healthy "redpanda"

# Collector should have emitted buffer_overflow gaps
overflow_gaps=$(count_gaps "buffer_overflow")
assert_gt "buffer_overflow gaps exist in archive" "$overflow_gaps" 0

# Data integrity
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

print_test_report
teardown_stack
print_results
