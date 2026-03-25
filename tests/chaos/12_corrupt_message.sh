#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 12: Corrupt Message ==="
echo "Injects corrupt (non-JSON) messages and verifies the writer"
echo "skips them and continues processing valid data."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Recording pre-injection envelope count..."
pre_inject=$(count_envelopes)

step 2 "Injecting corrupt messages into Redpanda..."
inject_corrupt_message "binance.trades"
inject_corrupt_message "binance.bookticker"

step 3 "Waiting for writer to process past corrupt messages..."
if wait_for_envelope_count_gt "$pre_inject" 60; then
    pass "writer continued processing after corrupt messages"
else
    fail "writer stopped processing after corrupt messages"
fi

step 4 "Waiting for deserialization_error gaps to flush to archive..."
# The gap envelopes are in the buffer — wait for the timer-based flush (10s)
wait_for_gaps "deserialization_error" 30

section "Verification"
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

post_inject=$(count_envelopes)
assert_gt "archive grew after corrupt injection" "$post_inject" "$pre_inject"

# Verify deserialization_error gaps were emitted for the corrupt messages
deser_gaps=$(count_gaps "deserialization_error")
assert_gt "deserialization_error gaps exist for corrupt messages" "$deser_gaps" 0

# Validate gap record details
validate_deserialization_gaps() {
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
        if env.get('type') == 'gap' and env.get('reason') == 'deserialization_error':
            found += 1
            gs = env.get('gap_start_ts', 0)
            ge = env.get('gap_end_ts', 0)
            if gs <= 0:
                errors.append(f'gap_start_ts invalid: {gs}')
            if ge <= 0:
                errors.append(f'gap_end_ts invalid: {ge}')
            detail = env.get('detail', '')
            if 'Corrupt message' not in detail:
                errors.append(f'unexpected detail: {detail}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no deserialization_error gaps found')
    exit(1)
print(f'OK: {found} deserialization_error gap(s) with valid metadata')
"
}

if validate_deserialization_gaps; then
    pass "deserialization_error gap metadata valid"
else
    fail "deserialization_error gap validation failed"
fi

print_test_report
teardown_stack
print_results
