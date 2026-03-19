#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Kill Collector (Unclean Exit) ==="
echo "Verifies that killing the collector process produces restart_gap records"
echo "in the archive with component=collector and cause=unclean_exit."
echo ""

setup_stack
wait_for_data 30
event_start_ns=$(ts_now_ns)

echo "1. Killing collector to simulate unclean exit..."
docker kill "${COLLECTOR_CONTAINER}"
sleep 10

echo "2. Restarting collector..."
$COMPOSE up -d collector 2>&1
event_end_ns=$(ts_now_ns)
wait_for_data 40

echo "3. Verifying results..."

# Writer should detect the session change and emit restart_gap records
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

# Validate structured restart_gap metadata: component=collector, cause=unclean_exit
validate_restart_gap_fields() {
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
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == 'restart_gap':
            found += 1
            if env.get('component') != 'collector':
                errors.append(f'expected component=collector, got {env.get(\"component\")}')
            if env.get('cause') != 'unclean_exit':
                errors.append(f'expected cause=unclean_exit, got {env.get(\"cause\")}')
            if env.get('planned') is not False:
                errors.append(f'expected planned=false, got {env.get(\"planned\")}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with component=collector, cause=unclean_exit, planned=false')
"
}

if validate_restart_gap_fields; then
    pass "restart_gap metadata: component=collector, cause=unclean_exit, planned=false"
else
    fail "restart_gap metadata validation failed"
fi

# Both collector and writer should be healthy after recovery
assert_container_healthy "collector"
assert_container_healthy "writer"

# Data integrity should be intact
if check_integrity; then
    pass "data integrity OK (no corrupt frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

# Archive should have data from both sessions
total=$(count_envelopes)
assert_gt "archive has envelopes from both sessions" "$total" 100

# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 60; then
    pass "restart_gap gap timestamps are accurate (within 60s tolerance)"
else
    fail "restart_gap gap timestamp accuracy check failed"
fi

print_test_report
teardown_stack
print_results
