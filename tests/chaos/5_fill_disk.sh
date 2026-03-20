#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Fill Disk ==="
echo "Creates a large file in the data volume to simulate disk pressure,"
echo "verifies the writer handles it, and recovers after cleanup."
echo ""

setup_stack
wait_for_data 30

echo "1. Filling data volume to near-capacity inside writer container..."
# Get available space in MB, fill to leave only 50MB free
avail_mb=$(docker compose -f "${COMPOSE_FILE}" exec writer \
    df -m "${TEST_DATA_DIR}" | awk 'NR==2{print $4}')
fill_mb=$((avail_mb - 50))
if (( fill_mb > 100 )); then
    docker compose -f "${COMPOSE_FILE}" exec writer \
        dd if=/dev/zero of="${TEST_DATA_DIR}/fill_disk.tmp" bs=1M count="${fill_mb}" 2>/dev/null || true
    echo "   Filled ${fill_mb}MB, leaving ~50MB free"
else
    echo "   WARNING: Only ${avail_mb}MB available, skipping fill (need >150MB)"
fi

echo "2. Checking disk usage inside writer..."
docker compose -f "${COMPOSE_FILE}" exec writer df -h "${TEST_DATA_DIR}"

echo "3. Waiting 30s under disk pressure..."
sleep 30

echo "4. Checking writer health under pressure..."
assert_container_healthy "writer"

echo "5. Cleaning up fill file..."
docker compose -f "${COMPOSE_FILE}" exec writer rm -f "${TEST_DATA_DIR}/fill_disk.tmp" 2>/dev/null || true

echo "6. Waiting 30s for recovery..."
sleep 30

echo "7. Verifying results..."

assert_container_healthy "writer"
assert_container_healthy "collector"

# Data integrity
if check_integrity; then
    pass "data integrity OK after disk pressure"
else
    fail "data integrity check failed"
fi

# Archive should have data
total=$(count_envelopes)
assert_gt "archive has envelopes" "$total" 0

print_test_report
teardown_stack
print_results
