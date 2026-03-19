#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Host Reboot Restart Gap ==="
echo "Simulates a host reboot by restarting the stack with a new boot ID."
echo "Verifies restart_gap records have component=host, cause=host_reboot."
echo ""

OLD_BOOT_ID="test-boot-old-$(date +%s)"
NEW_BOOT_ID="test-boot-new-$(date +%s)"

echo "Old boot ID: $OLD_BOOT_ID"
echo "New boot ID: $NEW_BOOT_ID"

# Start stack with the old boot ID
export CRYPTOLAKE_TEST_BOOT_ID="${OLD_BOOT_ID}"
setup_stack
wait_for_data 30
event_start_ns=$(ts_now_ns)

echo "1. Stopping all services (simulating host power-off)..."
# No maintenance intent — this is an unplanned reboot
$COMPOSE down 2>&1

echo "2. Bringing stack back up with new boot ID (simulating reboot)..."
export CRYPTOLAKE_TEST_BOOT_ID="${NEW_BOOT_ID}"
$COMPOSE up -d 2>&1
event_end_ns=$(ts_now_ns)
wait_healthy
wait_for_data 40

echo "3. Verifying results..."

# Writer should detect boot ID change and emit restart_gap with component=host
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

validate_host_reboot_gap() {
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
found = 0
errors = []
for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == 'restart_gap':
            found += 1
            if env.get('component') != 'host':
                errors.append(f'expected component=host, got {env.get(\"component\")}')
            if env.get('cause') != 'host_reboot':
                errors.append(f'expected cause=host_reboot, got {env.get(\"cause\")}')
            if env.get('planned') is not False:
                errors.append(f'expected planned=false, got {env.get(\"planned\")}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with component=host, cause=host_reboot, planned=false')
"
}

if validate_host_reboot_gap; then
    pass "restart_gap metadata: component=host, cause=host_reboot, planned=false"
else
    fail "restart_gap host reboot metadata validation failed"
fi

assert_container_healthy "collector"
assert_container_healthy "writer"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

total=$(count_envelopes)
assert_gt "archive has envelopes from both boot sessions" "$total" 100

# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 120; then
    pass "restart_gap gap timestamps are accurate (within 120s tolerance)"
else
    fail "restart_gap gap timestamp accuracy check failed"
fi

print_test_report
teardown_stack
print_results
