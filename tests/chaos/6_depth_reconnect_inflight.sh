#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Depth Reconnect Inflight ==="
echo "Kills the collector during active depth flow and verifies that depth"
echo "data resynchronises correctly after reconnect."
echo ""

setup_stack

echo "1. Letting collector stream depth updates for 30s..."
wait_for_data 30

echo "2. Killing collector during active depth flow..."
docker kill "${COLLECTOR_CONTAINER}"
sleep 5

echo "3. Restarting collector..."
$COMPOSE up -d collector 2>&1

echo "4. Waiting 60s for depth snapshot resync..."
sleep 60

echo "5. Verifying results..."

assert_container_healthy "collector"
assert_container_healthy "writer"

# Writer should detect collector session change and emit restart_gap
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist after depth reconnect" "$gaps" 0

# Validate restart_gap metadata: collector was killed, so expect unclean_exit
validate_depth_reconnect_gap() {
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
            if env.get('component') != 'collector':
                errors.append(f'expected component=collector, got {env.get(\"component\")}')
            if env.get('cause') != 'unclean_exit':
                errors.append(f'expected cause=unclean_exit, got {env.get(\"cause\")}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with component=collector, cause=unclean_exit')
"
}

if validate_depth_reconnect_gap; then
    pass "restart_gap metadata: component=collector, cause=unclean_exit"
else
    fail "restart_gap metadata validation failed"
fi

# Data integrity
if check_integrity; then
    pass "data integrity OK (depth replay remains valid after reconnect)"
else
    fail "data integrity check failed"
fi

# Archive should have depth data
total=$(count_envelopes)
assert_gt "archive has envelopes" "$total" 0

print_test_report
teardown_stack
print_results
