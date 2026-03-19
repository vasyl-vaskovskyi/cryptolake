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

# Validate any gap records have valid timestamps (gap_end_ts > gap_start_ts > 0)
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
print(f'OK: {found} gap record(s) all have valid timestamps' if found else 'OK: no gap records (expected for writer-only crash)')
"
}

if validate_any_gap_timestamps; then
    pass "all gap records (if any) have valid timestamps"
else
    fail "gap timestamp validation failed"
fi

# Archive should have data
total=$(count_envelopes)
assert_gt "archive has envelopes" "$total" 0

print_test_report
teardown_stack
print_results
