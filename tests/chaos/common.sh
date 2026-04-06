#!/usr/bin/env bash
# Shared setup/teardown/assertion helpers for chaos tests.
# Source this file from each chaos script: source "$(dirname "$0")/common.sh"
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Source .env so tests have the same env vars as Docker Compose
# (e.g. POSTGRES_PASSWORD for mark-maintenance CLI).
# shellcheck disable=SC1091
[[ -f "${REPO_ROOT}/.env" ]] && set -a && source "${REPO_ROOT}/.env" && set +a

COMPOSE_FILE="${REPO_ROOT}/docker-compose.test.yml"
COMPOSE="docker compose -f ${COMPOSE_FILE}"
PROD_COMPOSE="docker compose -f ${REPO_ROOT}/docker-compose.yml"
TEST_DATA_DIR="${TEST_DATA_DIR:-/Users/vasyl.vaskovskyi/data/archive/test_data}"
HOST_DATA_DIR="${HOST_DATA_DIR:-/Users/vasyl.vaskovskyi/data/archive}"
export TEST_DATA_DIR

# Container names used by the test compose project (cryptolake-test)
COLLECTOR_CONTAINER="cryptolake-test-collector-1"
BACKUP_COLLECTOR_CONTAINER="cryptolake-test-collector-backup-1"
WRITER_CONTAINER="cryptolake-test-writer-1"
REDPANDA_CONTAINER="cryptolake-test-redpanda-1"

# ---------------------------------------------------------------------------
# Safety guards — prevent chaos tests from damaging production
# ---------------------------------------------------------------------------

preflight_checks() {
    # 1. Refuse to run if TEST_DATA_DIR overlaps with production data dir
    local real_test real_prod
    real_test="$(cd "${TEST_DATA_DIR}" 2>/dev/null && pwd -P || echo "${TEST_DATA_DIR}")"
    real_prod="$(cd "${HOST_DATA_DIR}" 2>/dev/null && pwd -P || echo "${HOST_DATA_DIR}")"
    if [[ "${real_test}" == "${real_prod}" ]]; then
        echo "ERROR: TEST_DATA_DIR (${TEST_DATA_DIR}) is the same as HOST_DATA_DIR (${HOST_DATA_DIR})."
        echo "       Chaos tests delete data in TEST_DATA_DIR — this would destroy production archives."
        echo "       Set TEST_DATA_DIR to a separate path (e.g. ${HOST_DATA_DIR}/test_data)."
        exit 1
    fi

    # 2. Refuse to run if the production stack is up
    local prod_running
    prod_running=$($PROD_COMPOSE ps -q 2>/dev/null | head -1 || true)
    if [[ -n "${prod_running}" ]]; then
        echo "ERROR: Production stack (cryptolake) is running."
        echo "       Chaos tests share host ports with production and would cause conflicts."
        echo "       Stop the production stack first:  docker compose down"
        exit 1
    fi
}

TEST_START=""
PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  ✓ $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  ✗ $1"; }

section() { echo ""; echo "--- $1 ---"; }
step() { echo "  Step $1: $2"; }

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$expected" == "$actual" ]]; then pass "$desc"; else fail "$desc (expected=$expected, actual=$actual)"; fi
}

assert_gt() {
    local desc="$1" val="$2" threshold="$3"
    if (( val > threshold )); then pass "$desc"; else fail "$desc (val=$val, threshold=$threshold)"; fi
}

assert_container_healthy() {
    local svc="$1"
    local status
    status=$($COMPOSE ps "$svc" --format '{{.Status}}' 2>/dev/null)
    if echo "$status" | grep -q "(healthy)"; then pass "$svc is healthy"; else fail "$svc is NOT healthy (status: $status)"; fi
}

setup_stack() {
    preflight_checks
    TEST_START="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    SECONDS=0
    section "Setup"
    echo "  Start time: ${TEST_START}"
    rm -rf "${TEST_DATA_DIR:?}/binance"
    $COMPOSE build --quiet 2>&1
    TEST_DURATION_SECONDS=600 $COMPOSE up -d 2>&1
    wait_healthy
    echo "--- Stack is ready ---"
}

wait_healthy() {
    local attempts=0 max_attempts=40
    while (( attempts < max_attempts )); do
        local all_healthy=true
        for svc in postgres redpanda collector writer; do
            local status
            status=$($COMPOSE ps "$svc" --format '{{.Status}}' 2>/dev/null || echo "missing")
            if ! echo "$status" | grep -q "(healthy)"; then
                all_healthy=false
                break
            fi
        done
        if $all_healthy; then return 0; fi
        attempts=$((attempts + 1))
        sleep 2
    done
    echo "ERROR: timed out waiting for healthy services"
    $COMPOSE ps 2>&1
    return 1
}

wait_for_data() {
    local seconds="${1:-30}"
    echo "  Waiting ${seconds}s for data to flow..."
    sleep "$seconds"
}

teardown_stack() {
    section "Teardown"
    $COMPOSE down -v --rmi local 2>&1 || true
    rm -rf "${TEST_DATA_DIR:?}/binance"
    echo "  Cleanup complete"
}

# Count gap records in archive files by reason.
# Usage: count_gaps "collector_restart"  →  prints count
count_gaps() {
    local reason="$1"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
count = 0
for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == '${reason}':
            count += 1
print(count)
"
}

# Count total envelopes across all archive files.
count_envelopes() {
    uv run python -c "
import zstandard as zstd
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
total = 0
for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    total += data.strip().count(b'\n') + 1
print(total)
"
}

# Check data integrity: no corrupt frames, no duplicate offsets.
# Returns 0 if OK, 1 if issues found.
check_integrity() {
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
issues = 0
for f in sorted(base.rglob('*.zst')):
    try:
        with open(f, 'rb') as fh:
            data = zstd.ZstdDecompressor().stream_reader(fh).read()
        lines = data.strip().split(b'\n')
        offsets = []
        for l in lines:
            if not l:
                continue
            env = orjson.loads(l)
            if '_offset' in env and env['_offset'] >= 0:
                offsets.append(env['_offset'])
        dups = len(offsets) - len(set(offsets))
        if dups > 0:
            print(f'DUPLICATE OFFSETS in {f.relative_to(base)}: {dups}')
            issues += 1
    except Exception as e:
        print(f'CORRUPT FILE {f.relative_to(base)}: {e}')
        issues += 1
exit(1 if issues else 0)
"
}

# ---------------------------------------------------------------------------
# Condition-based waiting helpers
# ---------------------------------------------------------------------------

# Wait for collector to start dropping messages (buffer overflow).
# Polls collector_messages_dropped_total via the Prometheus endpoint
# inside the collector container.
# Usage: wait_for_overflow [max_seconds]
wait_for_overflow() {
    local max_wait="${1:-120}"
    echo "   Polling collector metrics for message drops..."
    for i in $(seq 1 "$max_wait"); do
        drops=$($COMPOSE exec -T collector python -c "
from urllib.request import urlopen
data = urlopen('http://localhost:8000/metrics', timeout=2).read().decode()
total = 0
for line in data.splitlines():
    if line.startswith('collector_messages_dropped_total{'):
        total += float(line.split()[-1])
print(int(total))
" 2>/dev/null || echo "0")
        if (( drops > 0 )); then
            echo "   Buffer overflow detected after ${i}s (${drops} messages dropped)"
            return 0
        fi
        sleep 1
    done
    echo "   WARNING: No overflow detected after ${max_wait}s"
    return 1
}

# Wait for specific gap type to appear in the archive.
# Polls the archive directory for gap records matching the given reason.
# Usage: wait_for_gaps "buffer_overflow" [max_seconds]
wait_for_gaps() {
    local reason="$1"
    local max_wait="${2:-90}"
    echo "   Polling archive for ${reason} gaps..."
    local elapsed=0
    while (( elapsed < max_wait )); do
        count=$(count_gaps "$reason" 2>/dev/null || echo "0")
        if (( count > 0 )); then
            echo "   Found ${count} ${reason} gap(s) in archive after ${elapsed}s"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    echo "   WARNING: No ${reason} gaps found after ${max_wait}s"
    return 1
}

# Wait for the archive envelope count to exceed a threshold.
# Usage: wait_for_envelope_count_gt [threshold] [max_seconds]
wait_for_envelope_count_gt() {
    local threshold="$1"
    local max_wait="${2:-90}"
    echo "   Polling archive until envelope count exceeds ${threshold}..."
    for i in $(seq 1 "$max_wait"); do
        total="$(count_envelopes 2>/dev/null || echo "0")"
        if [[ "${total}" =~ ^[0-9]+$ ]] && (( total > threshold )); then
            echo "   Archive count grew after ${i}s (count=${total})"
            return 0
        fi
        sleep 1
    done
    echo "   WARNING: Archive count did not exceed ${threshold} after ${max_wait}s"
    return 1
}

# Read total writer consumer lag across all streams from the writer metrics endpoint.
# Prints an integer lag value, or returns non-zero if metrics are not ready yet.
get_writer_total_lag() {
    $COMPOSE exec -T writer python -c "
from urllib.request import urlopen

data = urlopen('http://localhost:8001/metrics', timeout=2).read().decode()
total = 0.0
samples = 0
for line in data.splitlines():
    if line.startswith('writer_consumer_lag{'):
        total += float(line.split()[-1])
        samples += 1
if samples == 0:
    raise SystemExit(1)
print(int(total))
" 2>/dev/null
}

# Wait for the writer to drain its Redpanda backlog after restart.
# Usage: wait_for_writer_lag_below [threshold] [max_seconds]
wait_for_writer_lag_below() {
    local threshold="${1:-0}"
    local max_wait="${2:-120}"
    echo "   Polling writer lag until <= ${threshold}..."
    for i in $(seq 1 "$max_wait"); do
        lag="$(get_writer_total_lag 2>/dev/null || true)"
        if [[ "${lag}" =~ ^[0-9]+$ ]] && (( lag <= threshold )); then
            echo "   Writer lag drained after ${i}s (lag=${lag})"
            return 0
        fi
        sleep 1
    done
    echo "   WARNING: Writer lag stayed above ${threshold} after ${max_wait}s"
    return 1
}

# Wait for a single service to become healthy.
# Usage: wait_service_healthy "redpanda" [max_seconds]
wait_service_healthy() {
    local svc="$1"
    local max_wait="${2:-60}"
    for i in $(seq 1 "$max_wait"); do
        if $COMPOSE ps "$svc" --format '{{.Status}}' 2>/dev/null | grep -q "(healthy)"; then
            return 0
        fi
        sleep 2
    done
    echo "   WARNING: ${svc} not healthy after $((max_wait * 2))s"
    return 1
}

# ---------------------------------------------------------------------------
# Network fault injection helpers
# ---------------------------------------------------------------------------

# Block outbound traffic from a container using iptables inside the container.
# Requires the container to have NET_ADMIN capability.
# Usage: block_egress <container_name>
block_egress() {
    local container="${1:?Usage: block_egress <container_name>}"
    docker exec -u root "${container}" iptables -A OUTPUT -j DROP 2>/dev/null || true
    docker exec -u root "${container}" iptables -A INPUT -j DROP 2>/dev/null || true
    echo "   Blocked network traffic for ${container}"
}

# Restore outbound traffic for a container.
# Usage: unblock_egress <container_name>
unblock_egress() {
    local container="${1:?Usage: unblock_egress <container_name>}"
    docker exec -u root "${container}" iptables -F 2>/dev/null || true
    echo "   Restored network traffic for ${container}"
}

# Validate that gap_start_ts and gap_end_ts in archived gap records are within
# a reasonable tolerance of the actual chaos event times.
# Usage: validate_gap_window_accuracy <reason> <event_start_epoch_ns> <event_end_epoch_ns> <tolerance_seconds>
validate_gap_window_accuracy() {
    local reason="$1"
    local event_start_ns="$2"
    local event_end_ns="$3"
    local tolerance_s="${4:-30}"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path

base = Path('${TEST_DATA_DIR}')
reason = '${reason}'
event_start = ${event_start_ns}
event_end = ${event_end_ns}
tol_ns = int(${tolerance_s}) * 1_000_000_000
errors = []
found = 0

for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == reason:
            found += 1
            gs = env.get('gap_start_ts', 0)
            ge = env.get('gap_end_ts', 0)
            if gs == 0 or ge == 0:
                errors.append(f'gap record missing timestamps: gap_start_ts={gs}, gap_end_ts={ge}')
                continue
            if ge <= gs:
                errors.append(f'gap_end_ts ({ge}) <= gap_start_ts ({gs})')
            if abs(gs - event_start) > tol_ns:
                errors.append(f'gap_start_ts off by {abs(gs - event_start)/1e9:.1f}s (tolerance: ${tolerance_s}s)')
            if event_end > 0 and abs(ge - event_end) > tol_ns:
                errors.append(f'gap_end_ts off by {abs(ge - event_end)/1e9:.1f}s (tolerance: ${tolerance_s}s)')

if found == 0:
    print(f'ERROR: no {reason} gap records found')
    exit(1)
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
print(f'OK: {found} {reason} gap(s) with valid timestamps (tolerance: ${tolerance_s}s)')
"
}

# Capture current time as nanosecond epoch (for gap-window validation).
# Usage: ts_now_ns  →  prints nanosecond epoch
ts_now_ns() {
    python3 -c "import time; print(time.time_ns())"
}

# ---------------------------------------------------------------------------
# Writer failover metric helpers
# ---------------------------------------------------------------------------

# Read a single writer metric value by name.
# Usage: get_writer_metric "writer_failover_active"  →  prints value
get_writer_metric() {
    local metric_name="${1:?Usage: get_writer_metric <metric_name>}"
    $COMPOSE exec -T writer python -c "
from urllib.request import urlopen
data = urlopen('http://localhost:8001/metrics', timeout=2).read().decode()
for line in data.splitlines():
    if line.startswith('${metric_name}') and not line.startswith('#'):
        val = line.split()[-1]
        # Handle both '1.0' and '1' formats
        print(int(float(val)))
        raise SystemExit(0)
print(0)
" 2>/dev/null
}

# Wait for writer_failover_active to reach expected value.
# Usage: wait_for_failover_state <expected_value> [max_seconds]
wait_for_failover_state() {
    local expected="${1:?Usage: wait_for_failover_state <0|1> [max_seconds]}"
    local max_wait="${2:-60}"
    local label="active"
    [[ "$expected" == "0" ]] && label="inactive"
    echo "   Polling writer until failover is ${label}..."
    for i in $(seq 1 "$max_wait"); do
        val="$(get_writer_metric writer_failover_active 2>/dev/null || echo "-1")"
        if [[ "$val" == "$expected" ]]; then
            echo "   Writer failover is ${label} after ${i}s"
            return 0
        fi
        sleep 1
    done
    echo "   WARNING: Writer failover did not become ${label} after ${max_wait}s"
    return 1
}

# ---------------------------------------------------------------------------
# Message injection helpers
# ---------------------------------------------------------------------------

# Inject a corrupt (non-JSON) message into a Redpanda topic.
# Usage: inject_corrupt_message <topic> [message]
inject_corrupt_message() {
    local topic="${1:?Usage: inject_corrupt_message <topic> [message]}"
    local msg="${2:-NOT_VALID_JSON{{{corrupt}}}"
    $COMPOSE exec -T redpanda rpk topic produce "$topic" <<< "$msg"
    echo "   Injected corrupt message into ${topic}"
}

# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------

# Print per-file record counts and time ranges from the archive.
print_archive_stats() {
    echo ""
    echo "--- Archive File Summary ---"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
from datetime import datetime, timezone

base = Path('${TEST_DATA_DIR}')
files = sorted(base.rglob('*.zst'))
if not files:
    print('  (no archive files found)')
else:
    total = 0
    fmt = lambda ns: datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3] if ns else '?'
    for f in files:
        with open(f, 'rb') as fh:
            data = zstd.ZstdDecompressor().stream_reader(fh).read()
        lines = [l for l in data.strip().split(b'\n') if l]
        count = len(lines)
        total += count
        first = orjson.loads(lines[0])
        last  = orjson.loads(lines[-1])
        first_ts = first.get('received_at', 0)
        last_ts  = last.get('received_at', 0)
        types = {}
        for l in lines:
            t = orjson.loads(l).get('type', '?')
            types[t] = types.get(t, 0) + 1
        type_str = ', '.join(f'{t}={n}' for t, n in sorted(types.items()))
        rel = f.relative_to(base)
        print(f'  {rel}')
        print(f'    records: {count}  ({type_str})')
        print(f'    window:  {fmt(first_ts)} - {fmt(last_ts)} UTC')
    print(f'  ─────────────────────────────────────')
    print(f'  Total: {total} records across {len(files)} file(s)')
"
}

# Print detailed gap information: reason, stream, time range, duration.
print_gap_details() {
    echo ""
    echo "--- Gap Details ---"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
from datetime import datetime, timezone

base = Path('${TEST_DATA_DIR}')
gaps = []
for f in sorted(base.rglob('*.zst')):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap':
            gaps.append(env)

if not gaps:
    print('  (no gaps detected)')
else:
    fmt = lambda ns: datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime('%H:%M:%S.%f')[:-3] if ns else '?'
    gaps.sort(key=lambda g: g.get('gap_start_ts', 0))
    by_reason = {}
    for g in gaps:
        reason   = g.get('reason', '?')
        start_ns = g.get('gap_start_ts', 0)
        end_ns   = g.get('gap_end_ts', 0)
        dur_s    = (end_ns - start_ns) / 1e9 if (start_ns and end_ns) else 0
        stream   = g.get('stream', '?')
        symbol   = g.get('symbol', '?')
        detail   = g.get('detail', '')
        by_reason[reason] = by_reason.get(reason, 0) + 1
        print(f'  [{reason}] {symbol}/{stream}')
        print(f'    from {fmt(start_ns)} to {fmt(end_ns)} UTC  (duration: {dur_s:.1f}s)')
        if detail:
            print(f'    detail: {detail}')
    print(f'  ─────────────────────────────────────')
    summary = ', '.join(f'{r}={n}' for r, n in sorted(by_reason.items()))
    print(f'  Total: {len(gaps)} gap(s)  ({summary})')
"
}

# Print whatsapp-bridge logs showing alert deliveries with delivery summary.
print_whatsapp_log() {
    echo ""
    echo "--- WhatsApp Bridge Log ---"
    local bridge_log
    bridge_log=$($COMPOSE logs whatsapp-bridge --no-log-prefix 2>/dev/null || echo "(bridge not running)")
    if [[ -z "$bridge_log" || "$bridge_log" == "(bridge not running)" ]]; then
        echo "  (no whatsapp-bridge logs available)"
    else
        echo "$bridge_log" | sed 's/^/  /'
        echo "  ─────────────────────────────────────"
        local sent failed total
        sent=$(echo "$bridge_log" | grep -c 'callmebot status=200' || true)
        failed=$(echo "$bridge_log" | grep -c 'callmebot error' || true)
        total=$((sent + failed))
        echo "  Delivery: ${sent} sent, ${failed} failed, ${total} total"
    fi
}

# Full test report — call this BEFORE teardown (while data still exists).
print_test_report() {
    print_archive_stats
    print_gap_details
    print_whatsapp_log
}

print_results() {
    local test_end
    test_end="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    local elapsed=$SECONDS
    echo ""
    echo "==========================================="
    echo "  Results: ${PASS} passed, ${FAIL} failed"
    echo "  Start:    ${TEST_START}"
    echo "  End:      ${test_end}"
    echo "  Duration: ${elapsed}s"
    echo "==========================================="
    if (( FAIL > 0 )); then exit 1; fi
}

# Validate gap timestamp ordering: gap_end_ts > gap_start_ts > 0 for all gap records.
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
print(f'OK: {found} gap record(s) all have valid timestamps' if found else 'OK: no gap records')
"
}

# Validate restart_gap metadata fields.
# Usage: validate_restart_gap_fields [expected_component] [expected_cause]
# If no args, just validates planned=false on any restart_gap.
validate_restart_gap_fields() {
    local expected_component="${1:-}"
    local expected_cause="${2:-}"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
found = 0
errors = []
expected_component = '${expected_component}'
expected_cause = '${expected_cause}'
for f in base.rglob('*.zst'):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') == 'gap' and env.get('reason') == 'restart_gap':
            found += 1
            if expected_component and env.get('component') != expected_component:
                errors.append(f'expected component={expected_component}, got {env.get(\"component\")}')
            if expected_cause and env.get('cause') != expected_cause:
                errors.append(f'expected cause={expected_cause}, got {env.get(\"cause\")}')
            planned = env.get('planned')
            if planned is not False:
                errors.append(f'expected planned=false, got {planned}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('OK: no restart_gap records')
else:
    msg_parts = ['planned=false']
    if expected_component: msg_parts.insert(0, f'component={expected_component}')
    if expected_cause: msg_parts.insert(1, f'cause={expected_cause}')
    print(f'OK: {found} restart_gap record(s) with {\", \".join(msg_parts)}')
"
}
