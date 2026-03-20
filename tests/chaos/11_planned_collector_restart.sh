#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Planned Collector-Only Restart ==="
echo "Stops only the collector with maintenance intent, restarts it,"
echo "and verifies restart_gap with component=collector, cause=operator_shutdown, planned=true."
echo ""

DB_URL="${DB_URL:-postgresql://cryptolake:postgres@localhost:5432/cryptolake}"

setup_stack
wait_for_data 30

MAINT_ID="chaos-collector-$(date -u '+%Y%m%dT%H%M%SZ')"

echo "1. Recording maintenance intent for collector restart..."
uv run cryptolake mark-maintenance \
  --db-url "${DB_URL}" \
  --scope collector \
  --maintenance-id "${MAINT_ID}" \
  --reason "chaos test: planned collector restart" \
  --ttl-minutes 30

echo "2. Gracefully stopping collector..."
event_start_ns=$(ts_now_ns)
$COMPOSE stop collector 2>&1

echo "3. Waiting 10s (writer continues, Redpanda buffers)..."
sleep 10

echo "4. Restarting collector..."
$COMPOSE up -d collector 2>&1
event_end_ns=$(ts_now_ns)
wait_healthy
wait_for_data 40

echo "5. Verifying results..."

# Writer should detect the session change and emit restart_gap records
wait_for_gaps "restart_gap" 90
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

# Validate structured restart_gap metadata: component=collector, planned=true
validate_planned_collector_gap() {
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
found = 0
errors = []
maint_id = '${MAINT_ID}'
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
            if env.get('cause') != 'operator_shutdown':
                errors.append(f'expected cause=operator_shutdown, got {env.get(\"cause\")}')
            if env.get('planned') is not True:
                errors.append(f'expected planned=true, got {env.get(\"planned\")}')
            if 'maintenance_id' not in env:
                errors.append('expected maintenance_id to be present')
            if env.get('maintenance_id') != maint_id:
                errors.append(f'expected maintenance_id={maint_id}, got {env.get(\"maintenance_id\")}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with component=collector, cause=operator_shutdown, planned=true')
"
}

if validate_planned_collector_gap; then
    pass "restart_gap metadata: component=collector, cause=operator_shutdown, planned=true"
else
    fail "restart_gap planned collector metadata validation failed"
fi

assert_container_healthy "collector"
assert_container_healthy "writer"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

total=$(count_envelopes)
assert_gt "archive has envelopes from both sessions" "$total" 100

if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 120; then
    pass "restart_gap gap timestamps are accurate (within 120s tolerance)"
else
    fail "restart_gap gap timestamp accuracy check failed"
fi

print_test_report
teardown_stack
print_results
