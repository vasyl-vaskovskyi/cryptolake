#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos 4: Fill Disk ==="
echo "Uses a size-limited tmpfs volume (150MB) for the writer's data dir"
echo "to test real disk-pressure and ENOSPC handling."
echo ""

# ---------------------------------------------------------------------------
# Override: mount a small tmpfs volume for the writer instead of the host dir.
# This lets us fill the disk genuinely without touching the host filesystem.
# ---------------------------------------------------------------------------
DISK_OVERRIDE="${REPO_ROOT}/docker-compose.disk-test.yml"
DISK_DATA_DIR="/data/disk_test"

cat > "${DISK_OVERRIDE}" <<'YAML'
services:
  writer:
    volumes:
      - ./config:/app/config:ro
      - disk_test_data:/data/disk_test
    environment:
      - CONFIG_PATH=/app/config/config.test.yaml
      - DATABASE__URL=postgresql://cryptolake:${POSTGRES_PASSWORD:-postgres}@postgres:5432/cryptolake
      - MONITORING__WEBHOOK_URL=${WEBHOOK_URL:-}
      - HOST_DATA_DIR=/data/disk_test
      - TEST_DURATION_SECONDS=${TEST_DURATION_SECONDS:-600}
      - CRYPTOLAKE_TEST_BOOT_ID=${CRYPTOLAKE_TEST_BOOT_ID:-}
volumes:
  disk_test_data:
    driver: local
    driver_opts:
      type: tmpfs
      device: tmpfs
      o: "size=150m"
YAML

# Use both compose files — the override replaces the writer's volumes and env.
COMPOSE_DISK="docker compose -f ${COMPOSE_FILE} -f ${DISK_OVERRIDE}"

_DISK_CLEANUP_DONE=false
cleanup() {
    if $_DISK_CLEANUP_DONE; then return 0; fi
    _DISK_CLEANUP_DONE=true
    section "Teardown"
    $COMPOSE_DISK down -v --remove-orphans --rmi local 2>&1 || true
    rm -rf "${TEST_DATA_DIR:?}/binance"
    rm -f "${DISK_OVERRIDE}"
    echo "  Cleanup complete"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Setup: start the stack with the small volume
# ---------------------------------------------------------------------------
preflight_checks
TEST_START="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
SECONDS=0
section "Setup"
echo "  Start time: ${TEST_START}"
# Ensure clean state from any previous test
echo "  Cleaning leftover containers/volumes/networks..."
$COMPOSE_DISK down -v --remove-orphans 2>/dev/null || true
$COMPOSE down -v --remove-orphans 2>/dev/null || true
rm -rf "${TEST_DATA_DIR:?}/binance"
$COMPOSE_DISK build --quiet 2>&1
TEST_DURATION_SECONDS=600 $COMPOSE_DISK up -d 2>&1
# Wait for health using the override compose
attempts=0; max_attempts=60
while (( attempts < max_attempts )); do
    all_healthy=true
    for svc in postgres redpanda collector writer; do
        status=$($COMPOSE_DISK ps "$svc" --format '{{.Status}}' 2>/dev/null || echo "missing")
        if ! echo "$status" | grep -q "(healthy)"; then all_healthy=false; break; fi
    done
    if $all_healthy; then break; fi
    attempts=$((attempts + 1)); sleep 2
done
if ! $all_healthy; then
    echo "ERROR: timed out waiting for healthy services"
    $COMPOSE_DISK ps 2>&1
    exit 1
fi
echo "--- Stack is ready (150MB tmpfs data volume) ---"

section "Scenario"
step 1 "Letting data flow for 30s..."
sleep 30

step 2 "Checking volume usage before fill..."
$COMPOSE_DISK exec writer df -h "${DISK_DATA_DIR}" 2>&1

step 3 "Filling tmpfs to zero free space..."
# First pass: fill in 1MB blocks to get close
avail_mb=$($COMPOSE_DISK exec writer \
    df -m "${DISK_DATA_DIR}" | awk 'NR==2{print $4}')
fill_mb=$((avail_mb - 1))
if (( fill_mb > 0 )); then
    $COMPOSE_DISK exec writer \
        dd if=/dev/zero of="${DISK_DATA_DIR}/fill_disk.tmp" bs=1M count="${fill_mb}" 2>/dev/null || true
fi
# Second pass: fill remaining bytes until ENOSPC (dd exits non-zero, || true absorbs it)
$COMPOSE_DISK exec writer \
    dd if=/dev/zero of="${DISK_DATA_DIR}/fill_disk_last.tmp" bs=4K 2>/dev/null || true
echo "   Filled to capacity"

step 4 "Checking volume usage after fill..."
$COMPOSE_DISK exec writer df -h "${DISK_DATA_DIR}" 2>&1

step 5 "Waiting 30s for writer to encounter ENOSPC..."
sleep 30

step 6 "Checking writer status under disk pressure..."
writer_status=$($COMPOSE_DISK ps writer --format '{{.Status}}' 2>/dev/null || echo "missing")
echo "   Writer status: ${writer_status}"
# The writer catches OSError in _write_to_disk(), truncates partial frames,
# emits write_error gaps, and continues. It should survive ENOSPC.
if echo "$writer_status" | grep -q "(healthy)"; then
    pass "writer survived disk pressure (healthy — OSError caught)"
else
    fail "writer crashed under disk pressure (should survive — _write_to_disk catches OSError)"
fi

step 7 "Cleaning up fill files and freeing space..."
# If the writer crashed, exec won't work — start a temp container to clean the volume.
$COMPOSE_DISK exec writer rm -f "${DISK_DATA_DIR}/fill_disk.tmp" "${DISK_DATA_DIR}/fill_disk_last.tmp" 2>/dev/null \
    || docker run --rm -v "$(docker volume ls -q | grep disk_test_data | head -1)":/data/disk_test alpine \
           rm -f /data/disk_test/fill_disk.tmp /data/disk_test/fill_disk_last.tmp 2>/dev/null || true

step 8 "Restarting writer for recovery..."
# Force-restart so _recover_files truncates corrupt partial frames back
# to PG-recorded byte sizes.  "up -d" is a no-op for a running container.
$COMPOSE_DISK stop writer 2>&1
$COMPOSE_DISK up -d writer 2>&1
sleep 30

section "Verification"

# Writer should recover after disk space is freed
writer_status=$($COMPOSE_DISK ps writer --format '{{.Status}}' 2>/dev/null || echo "missing")
if echo "$writer_status" | grep -q "(healthy)"; then
    pass "writer recovered after disk space freed"
else
    fail "writer did not recover after disk space freed (status: ${writer_status})"
fi

# Collector should still be healthy
collector_status=$($COMPOSE_DISK ps collector --format '{{.Status}}' 2>/dev/null || echo "missing")
if echo "$collector_status" | grep -q "(healthy)"; then
    pass "collector remained healthy throughout"
else
    fail "collector unhealthy (status: ${collector_status})"
fi

# Copy archive data from the container to host for validation
echo "   Copying archive data from container for validation..."
rm -rf "${TEST_DATA_DIR:?}/binance"
docker cp "${WRITER_CONTAINER}:${DISK_DATA_DIR}/binance" "${TEST_DATA_DIR}/" 2>/dev/null || true

# Data integrity after recovery
if check_integrity; then
    pass "data integrity OK after disk-pressure recovery"
else
    fail "data integrity check failed after recovery"
fi

# Archive should have data from before the fill
total=$(count_envelopes)
assert_gt "archive has envelopes (data survived disk pressure)" "$total" 0

# Writer should have emitted write_error gaps during ENOSPC.
# The restart at step 8 will also produce a restart_gap, but the key assertion
# is that write_error gaps exist from the disk pressure itself.
wait_for_gaps "write_error" 45 || true
write_err_gaps=$(count_gaps "write_error")
assert_gt "write_error gaps exist (ENOSPC correctly recorded)" "$write_err_gaps" 0

# The forced restart in step 8 may also produce restart_gap records.
restart_gaps=$(count_gaps "restart_gap")
if (( restart_gaps > 0 )); then
    pass "restart_gap records also present from forced restart (count=${restart_gaps})"
else
    pass "no restart_gap records (writer recovered without session change)"
fi

# Verify the volume is actually small (sanity check)
vol_size=$($COMPOSE_DISK exec writer df -m "${DISK_DATA_DIR}" | awk 'NR==2{print $2}')
if (( vol_size <= 200 )); then
    pass "data volume was genuinely small (${vol_size}MB — real disk-pressure test)"
else
    fail "data volume was ${vol_size}MB — not a real disk-pressure test"
fi

print_test_report
cleanup
print_results
