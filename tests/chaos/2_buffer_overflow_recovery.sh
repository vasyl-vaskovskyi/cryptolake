#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos: Buffer Overflow Recovery ==="
echo "Stops Redpanda to force collector buffer growth, then restarts and"
echo "verifies the collector recovers with buffer_overflow gaps recorded."
echo ""

setup_stack
wait_for_data 30

echo "1. Stopping redpanda to force collector buffer growth..."
event_start_ns=$(ts_now_ns)
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
event_end_ns=$(ts_now_ns)
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

# Validate buffer_overflow gap timestamps (gap_end_ts > gap_start_ts > 0)
validate_any_gap_timestamps() {
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
errors = []
found = 0
for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap':
            found += 1
            gs = env.get('gap_start_ts', 0)
            ge = env.get('gap_end_ts', 0)
            if gs <= 0:
                errors.append(f'gap_start_ts invalid: {gs}')
            if ge <= 0:
                errors.append(f'gap_end_ts invalid: {ge}')
            if ge <= gs:
                errors.append(f'gap_end_ts ({ge}) <= gap_start_ts ({gs})')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
print(f'OK: {found} gap record(s) all have valid timestamps')
"
}

if validate_any_gap_timestamps; then
    pass "all gap records have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

# Writer was force-restarted, so restart_gap records may exist.
# If present, they must have planned=false.
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
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == 'restart_gap':
            found += 1
            planned = env.get('planned')
            if planned is not False:
                errors.append(f'expected planned=false, got {planned}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('OK: no restart_gap records (writer recovered cleanly)')
else:
    print(f'OK: {found} restart_gap record(s) with planned=false')
"
}

if validate_restart_gap_fields; then
    pass "restart_gap metadata valid (planned=false if gaps exist)"
else
    fail "restart_gap metadata validation failed"
fi

# Validate buffer_overflow gap timestamps are in the right ballpark
if validate_gap_window_accuracy "buffer_overflow" "$event_start_ns" "$event_end_ns" 120; then
    pass "buffer_overflow gap timestamps accurate (within 120s tolerance)"
else
    fail "buffer_overflow gap timestamp accuracy check failed"
fi

# Data integrity
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

# Archive should have data
total=$(count_envelopes)
assert_gt "archive has envelopes spanning the outage" "$total" 100

echo "7. Stopping collector to quiesce input before archive verification..."
$COMPOSE stop collector 2>&1
if wait_for_writer_lag_below 0 30; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

echo "8. Running cryptolake verify..."
if UV_CACHE_DIR="${REPO_ROOT}/.tmp/uv-cache" \
    uv run cryptolake verify \
        --date "${test_date}" \
        --base-dir "${TEST_DATA_DIR}" \
        --full \
        --repair-checksums; then
    pass "cryptolake verify passed"
else
    fail "cryptolake verify failed"
fi

print_test_report
teardown_stack
print_results
