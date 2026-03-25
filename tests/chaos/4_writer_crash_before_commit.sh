#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

test_date="$(date -u '+%Y-%m-%d')"

echo "=== Chaos 4: Writer Crash Before Commit ==="
echo "Sends SIGKILL to the writer (simulates crash mid-flush) and verifies"
echo "no duplicates, no corrupt zstd frames after recovery."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-crash envelope count..."
pre_kill=$(count_envelopes)

step 2 "Capturing event timestamp and sending SIGKILL to writer..."
event_start_ns=$(ts_now_ns)
docker kill -s KILL "${WRITER_CONTAINER}"

step 3 "Restarting writer..."
$COMPOSE up -d writer 2>&1
event_end_ns=$(ts_now_ns)

step 4 "Waiting for writer to recover and resume writing..."
if wait_for_envelope_count_gt "$pre_kill" 60; then
    pass "writer resumed writing after crash recovery"
else
    fail "writer did not resume writing after crash recovery"
fi

step 5 "Waiting for writer healthcheck..."
if ! wait_service_healthy writer 30; then
    :
fi

section "Verification"

assert_container_healthy "writer"
assert_container_healthy "collector"

# Writer should have caught up — more data than before the crash
post_recovery=$(count_envelopes)
assert_gt "writer caught up (more envelopes after recovery)" "$post_recovery" "$pre_kill"

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

# Validate gap record metadata: SIGKILL is always unplanned
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
    print('OK: no restart_gap records (writer crash with collector still running)')
else:
    print(f'OK: {found} restart_gap record(s) with planned=false')
"
}

if validate_restart_gap_fields; then
    pass "restart_gap metadata valid (planned=false if gaps exist)"
else
    fail "restart_gap metadata validation failed"
fi

# Validate gap timestamps are in the right ballpark (if any gaps exist)
gaps=$(count_gaps "restart_gap")
if (( gaps > 0 )); then
    if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 60; then
        pass "restart_gap timestamps accurate (within 60s tolerance)"
    else
        fail "restart_gap timestamp accuracy check failed"
    fi
else
    pass "no restart_gap records (expected for writer-only crash with collector up)"
fi

# No corrupt files, no duplicate offsets
if check_integrity; then
    pass "data integrity OK (no corrupt zstd frames, no duplicate offsets)"
else
    fail "data integrity check failed"
fi

step 7 "Stopping collector to quiesce input before archive verification..."
$COMPOSE stop collector 2>&1
if wait_for_writer_lag_below 0 30; then
    pass "writer drained remaining backlog after collector stop"
else
    fail "writer still had backlog after collector stop"
fi

step 8 "Running cryptolake verify..."
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
