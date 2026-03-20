#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Buffer Overflow Recovery ==="
echo "Stops Redpanda to force collector buffer growth, then restarts and"
echo "verifies the collector recovers with buffer_overflow gaps recorded."
echo ""

setup_stack
wait_for_data 30

echo "1. Stopping redpanda to force collector buffer growth..."
$COMPOSE stop redpanda 2>&1

echo "2. Waiting for collector buffer to overflow..."
wait_for_overflow 120

echo "3. Restarting redpanda..."
$COMPOSE up -d redpanda 2>&1
echo "   Waiting for redpanda to become healthy..."
# We don't use wait_healthy here because the writer is expected to have crashed
# during the redpanda outage (session timeout).
wait_service_healthy redpanda

echo "4. Force-restarting writer (crashes on rebalance after broker reconnect)..."
$COMPOSE stop writer 2>&1 || true
$COMPOSE up -d writer 2>&1
echo "   Waiting for all services to become healthy..."
wait_healthy

echo "5. Waiting for buffer_overflow gaps to appear in archive..."
wait_for_gaps "buffer_overflow" 90

echo "6. Verifying results..."

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
