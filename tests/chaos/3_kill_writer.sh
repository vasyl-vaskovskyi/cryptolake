#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

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

echo "5. Waiting 45s for catch-up..."
sleep 45

echo "6. Verifying results..."

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

print_test_report
teardown_stack
print_results
