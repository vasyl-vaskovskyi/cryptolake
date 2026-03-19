#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Full-Stack Planned Restart ==="
echo "Verifies that a planned full-stack restart (docker compose down/up)"
echo "with a maintenance intent produces restart_gap records with planned=true."
echo ""

DB_URL="${DB_URL:-postgresql://cryptolake:cryptolake@localhost:5432/cryptolake}"

setup_stack
wait_for_data 30

echo "1. Recording maintenance intent before shutdown..."
uv run cryptolake mark-maintenance \
  --db-url "${DB_URL}" \
  --scope system \
  --maintenance-id "chaos-test-$(date -u '+%Y%m%dT%H%M%SZ')" \
  --reason "chaos test: planned full-stack restart" \
  --ttl-minutes 30

echo "2. Performing full-stack shutdown (docker compose down)..."
$COMPOSE down 2>&1

echo "3. Bringing stack back up..."
$COMPOSE up -d 2>&1
wait_healthy
wait_for_data 40

echo "4. Verifying results..."

# Writer should detect the session change and emit restart_gap records
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

# Validate structured restart_gap metadata: planned=true
validate_planned_restart_gap() {
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
            if env.get('planned') is not True:
                errors.append(f'expected planned=true, got {env.get(\"planned\")}')
            if 'maintenance_id' not in env:
                errors.append('expected maintenance_id to be present')
            if 'classifier' not in env:
                errors.append('expected classifier field to be present')
            if 'evidence' not in env:
                errors.append('expected evidence field to be present')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with planned=true and maintenance_id present')
"
}

if validate_planned_restart_gap; then
    pass "restart_gap metadata: planned=true with maintenance_id"
else
    fail "restart_gap planned metadata validation failed"
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

print_test_report
teardown_stack
print_results
