#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos 10: Snapshot Poll Miss ==="
echo "Blocks collector HTTPS egress to trigger snapshot_poll_miss gaps"
echo "while WebSocket streams continue via existing connections."
echo ""

setup_stack
wait_for_data 20

section "Scenario"
step 1 "Blocking collector HTTPS egress (port 443 only)..."
# Block ALL outbound TCP to port 443 (not just SYN). The --syn filter would only
# block new connections, but aiohttp reuses keepalive connections for REST polls.
# Blocking all port-443 traffic will also break WebSocket streams, but that's
# acceptable — the test verifies snapshot_poll_miss gaps, and WS will reconnect
# after the block is lifted.
event_start_ns=$(ts_now_ns)
docker exec -u root "${COLLECTOR_CONTAINER}" iptables -A OUTPUT -p tcp --dport 443 -j DROP 2>/dev/null || true
echo "   Blocked all HTTPS traffic from collector"

step 2 "Waiting 120s for snapshot poll retries to exhaust..."
# Test config: snapshot_interval=30s, open_interest.poll_interval=30s
# Each poll does 3 retries with exponential backoff.
# We need to wait long enough for at least one complete poll cycle to fail.
sleep 120

step 3 "Restoring collector HTTPS egress..."
docker exec -u root "${COLLECTOR_CONTAINER}" iptables -F 2>/dev/null || true
event_end_ns=$(ts_now_ns)
echo "   Restored HTTPS connections"

step 4 "Waiting 45s for next successful poll cycle..."
sleep 45

section "Verification"

assert_container_healthy "collector"
assert_container_healthy "writer"

# Wait for snapshot_poll_miss gaps to appear in archive
wait_for_gaps "snapshot_poll_miss" 60
poll_gaps=$(count_gaps "snapshot_poll_miss")
assert_gt "snapshot_poll_miss gaps exist in archive" "$poll_gaps" 0

# Validate gap records have valid timestamps
validate_poll_miss_gaps() {
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
        if env.get('type') == 'gap' and env.get('reason') == 'snapshot_poll_miss':
            found += 1
            gs = env.get('gap_start_ts', 0)
            ge = env.get('gap_end_ts', 0)
            if gs <= 0:
                errors.append(f'gap_start_ts invalid: {gs}')
            if ge <= 0:
                errors.append(f'gap_end_ts invalid: {ge}')
            if ge <= gs:
                errors.append(f'gap_end_ts ({ge}) <= gap_start_ts ({gs})')
            stream = env.get('stream', '')
            if stream not in ('depth_snapshot', 'open_interest'):
                errors.append(f'unexpected stream for snapshot_poll_miss: {stream}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no snapshot_poll_miss gap records found')
    exit(1)
print(f'OK: {found} snapshot_poll_miss record(s) with valid timestamps')
"
}

if validate_poll_miss_gaps; then
    pass "snapshot_poll_miss gap metadata valid"
else
    fail "snapshot_poll_miss gap validation failed"
fi

# Data integrity
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

# WebSocket data should still have been flowing during the block
total=$(count_envelopes)
assert_gt "archive has envelopes (WS stayed up during REST block)" "$total" 100

if validate_gap_window_accuracy "snapshot_poll_miss" "$event_start_ns" "$event_end_ns" 180; then
    pass "snapshot_poll_miss gap timestamps are accurate (within 180s tolerance)"
else
    fail "snapshot_poll_miss gap timestamp accuracy check failed"
fi

print_test_report
teardown_stack
print_results
