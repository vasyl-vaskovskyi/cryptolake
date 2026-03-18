#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Writer Crash Before Commit ==="
echo "Sends SIGKILL to the writer (simulates crash mid-flush) and verifies"
echo "no duplicates, no corrupt zstd frames after recovery."
echo ""

setup_stack
wait_for_data 30

echo "1. Sending SIGKILL to writer (simulates crash mid-flush)..."
docker kill -s KILL "${WRITER_CONTAINER}"

echo "2. Restarting writer..."
$COMPOSE up -d writer 2>&1

echo "3. Waiting 45s for recovery..."
sleep 45

echo "4. Verifying results..."

assert_container_healthy "writer"
assert_container_healthy "collector"

# No corrupt files, no duplicate offsets
if check_integrity; then
    pass "data integrity OK (no corrupt zstd frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Archive should have data
total=$(count_envelopes)
assert_gt "archive has envelopes" "$total" 0

print_test_report
teardown_stack
print_results
