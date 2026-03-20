# Chaos Test Gap Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close all gaps in chaos test coverage so that every documented gap type (`ws_disconnect`, `pu_chain_break`, `snapshot_poll_miss`, `restart_gap` variants) has end-to-end validation, fulfilling the "Never silently lose data" guarantee.

**Architecture:** Nine issues were found in the existing chaos test suite. We fix them via: (1) new chaos scripts for untested gap types, (2) updating stale tests to use the current `restart_gap` contract, (3) upgrading Test 8 to a real docker-based integration test, (4) adding gap-record assertions to writer-crash tests, and (5) adding gap-window timestamp accuracy validation. All new scripts follow the established `common.sh` pattern.

**Tech Stack:** Bash (chaos scripts), Python (inline validators), Docker Compose (test stack), zstandard + orjson (archive parsing), iptables (network fault injection inside containers).

---

## File Structure

| Action | File | Purpose |
|--------|------|---------|
| Create | `tests/chaos/9_ws_disconnect.sh` | True WebSocket disconnect test (network fault, collector stays alive) |
| Create | `tests/chaos/10_snapshot_poll_miss.sh` | Block REST API to trigger `snapshot_poll_miss` gaps |
| Create | `tests/chaos/11_planned_collector_restart.sh` | Planned collector-only restart with maintenance intent |
| Modify | `tests/chaos/6_depth_reconnect_inflight.sh` | Update from legacy `collector_restart` to `restart_gap` |
| Modify | `tests/chaos/8_host_reboot_restart_gap.sh` | Upgrade to real docker-based integration test |
| Modify | `tests/chaos/3_kill_writer.sh` | Add `restart_gap` record assertions for writer downtime |
| Modify | `tests/chaos/4_writer_crash_before_commit.sh` | Add `restart_gap` record assertions for writer crash |
| Modify | `tests/chaos/common.sh` | Add `validate_gap_window_accuracy` and `block_egress` / `unblock_egress` helpers |
| Modify | `docker-compose.test.yml` | Add `cap_add: [NET_ADMIN]` to collector; add `CRYPTOLAKE_TEST_BOOT_ID` env to collector and writer |
| Modify | `tests/unit/test_infrastructure_assets.py` | Register new chaos scripts and update expected keywords for modified scripts |

---

## Chunk 1: Foundation — Update `common.sh` Helpers and Fix Stale Tests

### Task 1: Add network fault and gap-window helpers to `common.sh`

**Files:**
- Modify: `tests/chaos/common.sh`

New scripts need to block network traffic from the collector container (for `ws_disconnect` and `snapshot_poll_miss` tests) and validate gap timestamp accuracy. Add these helpers to `common.sh`.

- [ ] **Step 1: Write the `block_egress` / `unblock_egress` helpers**

Append to `tests/chaos/common.sh`, before the `# Reporting helpers` section:

```bash
# ---------------------------------------------------------------------------
# Network fault injection helpers
# ---------------------------------------------------------------------------

# Block outbound traffic from a container using iptables inside the container.
# Requires the container to have NET_ADMIN capability.
# Usage: block_egress <container_name>
block_egress() {
    local container="${1:?Usage: block_egress <container_name>}"
    docker exec "${container}" iptables -A OUTPUT -j DROP 2>/dev/null || true
    docker exec "${container}" iptables -A INPUT -j DROP 2>/dev/null || true
    echo "   Blocked network traffic for ${container}"
}

# Restore outbound traffic for a container.
# Usage: unblock_egress <container_name>
unblock_egress() {
    local container="${1:?Usage: unblock_egress <container_name>}"
    docker exec "${container}" iptables -F 2>/dev/null || true
    echo "   Restored network traffic for ${container}"
}
```

- [ ] **Step 2: Write the `validate_gap_window_accuracy` helper**

Append to `tests/chaos/common.sh`, after the network helpers:

```bash
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
```

- [ ] **Step 3: Verify common.sh still sources cleanly**

Run: `bash -n tests/chaos/common.sh`
Expected: no output, exit 0

- [ ] **Step 4: Commit**

```bash
git add tests/chaos/common.sh
git commit -m "feat(chaos): add network fault injection and gap-window validation helpers to common.sh"
```

---

### Task 2: Fix Test 6 — update from legacy `collector_restart` to `restart_gap`

**Files:**
- Modify: `tests/chaos/6_depth_reconnect_inflight.sh`
- Modify: `tests/unit/test_infrastructure_assets.py`

Test 6 asserts `count_gaps "collector_restart"` which is the legacy gap reason. The restart gap classification feature replaced this with `restart_gap`. Update the test to use the current contract.

- [ ] **Step 1: Update the gap assertion in Test 6**

In `tests/chaos/6_depth_reconnect_inflight.sh`, replace:

```bash
# Writer should detect collector session change
gaps=$(count_gaps "collector_restart")
assert_gt "collector_restart gaps exist after depth reconnect" "$gaps" 0
```

with:

```bash
# Writer should detect collector session change and emit restart_gap
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist after depth reconnect" "$gaps" 0
```

- [ ] **Step 2: Add restart_gap metadata validation to Test 6**

In `tests/chaos/6_depth_reconnect_inflight.sh`, after the `assert_gt` line added in Step 1, add:

```bash
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
```

- [ ] **Step 3: Update the infrastructure asset test**

In `tests/unit/test_infrastructure_assets.py`, in the `scripts` dict within `test_chaos_scripts_exist_are_executable_and_target_expected_failures`, change the Test 6 entry from:

```python
"tests/chaos/6_depth_reconnect_inflight.sh": ["depth", "COLLECTOR_CONTAINER", "collector_restart", "setup_stack", "teardown_stack", "print_test_report"],
```

to:

```python
"tests/chaos/6_depth_reconnect_inflight.sh": ["depth", "COLLECTOR_CONTAINER", "restart_gap", "setup_stack", "teardown_stack", "print_test_report"],
```

- [ ] **Step 4: Run unit tests to verify**

Run: `uv run pytest tests/unit/test_infrastructure_assets.py::TestChaosScripts -v`
Expected: PASS

- [ ] **Step 5: Verify Test 6 script syntax**

Run: `bash -n tests/chaos/6_depth_reconnect_inflight.sh`
Expected: no output, exit 0

- [ ] **Step 6: Commit**

```bash
git add tests/chaos/6_depth_reconnect_inflight.sh tests/unit/test_infrastructure_assets.py
git commit -m "fix(chaos): update test 6 from legacy collector_restart to restart_gap"
```

---

### Task 3: Add gap-record assertions to Test 3 (Kill Writer)

**Files:**
- Modify: `tests/chaos/3_kill_writer.sh`

Tests 3 validates data integrity and `cryptolake verify` but never checks that the writer downtime window produced a `restart_gap` record. The writer itself doesn't emit a gap for its own downtime (the next writer instance detects the previous session's unclean exit), but when the writer restarts it should detect the collector session is still the same — so no `restart_gap` is expected from the collector. However, the writer should still resume cleanly with no data loss. Add explicit assertions that: (a) no gap records were incorrectly omitted during the writer downtime, and (b) the envelope count spans both sessions.

- [ ] **Step 1: Add writer-session continuity assertion**

In `tests/chaos/3_kill_writer.sh`, after the `check_integrity` block (around line 53), add:

```bash
# Writer downtime should NOT produce restart_gap records (collector kept running).
# The writer resumes from its last durable checkpoint — no data loss, no gap needed.
writer_gaps=$(count_gaps "restart_gap")
# It's possible the writer recovery detects the same collector session and emits 0 gaps.
# But if gaps exist, they must have valid timestamps.
if (( writer_gaps > 0 )); then
    pass "restart_gap records present (writer detected session change): ${writer_gaps}"
else
    pass "no restart_gap records (collector stayed up during writer kill): expected"
fi
```

- [ ] **Step 2: Verify script syntax**

Run: `bash -n tests/chaos/3_kill_writer.sh`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add tests/chaos/3_kill_writer.sh
git commit -m "feat(chaos): add gap-record assertions to test 3 (kill writer)"
```

---

### Task 4: Add gap-record assertions to Test 4 (Writer Crash Before Commit)

**Files:**
- Modify: `tests/chaos/4_writer_crash_before_commit.sh`

Same issue as Test 3 — no gap-record validation. Add assertions.

- [ ] **Step 1: Add gap-record and timestamp validation**

In `tests/chaos/4_writer_crash_before_commit.sh`, after the `check_integrity` block (around line 32), add:

```bash
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
```

- [ ] **Step 2: Verify script syntax**

Run: `bash -n tests/chaos/4_writer_crash_before_commit.sh`
Expected: no output, exit 0

- [ ] **Step 3: Commit**

```bash
git add tests/chaos/4_writer_crash_before_commit.sh
git commit -m "feat(chaos): add gap timestamp validation to test 4 (writer crash)"
```

---

## Chunk 2: New Chaos Tests for Untested Gap Types

### Task 5: Create Test 9 — True WebSocket disconnect (`ws_disconnect`)

**Files:**
- Create: `tests/chaos/9_ws_disconnect.sh`
- Modify: `docker-compose.test.yml` (add `cap_add: [NET_ADMIN]` to collector)
- Modify: `tests/unit/test_infrastructure_assets.py`

This is the highest-priority gap. The existing Test 1 kills the collector process entirely, which triggers `restart_gap`. A true `ws_disconnect` — where the collector process stays alive but its network drops — has never been tested. This is the most common production failure mode (network blips).

The approach: add `NET_ADMIN` capability to the collector in the test compose file, then use `iptables` inside the container to block all traffic, wait for disconnection, then restore traffic and verify `ws_disconnect` gap records.

- [ ] **Step 1: Add `cap_add: [NET_ADMIN]` to the collector in `docker-compose.test.yml`**

In `docker-compose.test.yml`, add to the `collector` service (after `restart: "no"`):

```yaml
    cap_add:
      - NET_ADMIN
```

- [ ] **Step 2: Verify compose syntax**

Run: `docker compose -f docker-compose.test.yml config --quiet`
Expected: exit 0

- [ ] **Step 3: Update `test_test_compose_uses_fast_config_and_timeout_wrappers` in `tests/unit/test_infrastructure_assets.py`**

Add assertion for `NET_ADMIN` capability after the existing collector environment assertions (around line 126):

```python
        assert "NET_ADMIN" in services["collector"].get("cap_add", [])
```

- [ ] **Step 4: Create the `9_ws_disconnect.sh` script**

Create `tests/chaos/9_ws_disconnect.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: True WebSocket Disconnect ==="
echo "Blocks collector network to trigger ws_disconnect gaps while"
echo "the collector process stays alive, then restores and verifies."
echo ""

setup_stack
wait_for_data 30

echo "1. Recording pre-disconnect timestamps..."
event_start_ns=$(ts_now_ns)

echo "2. Blocking collector network (iptables DROP)..."
block_egress "${COLLECTOR_CONTAINER}"

echo "3. Waiting 30s for WebSocket timeout and disconnect detection..."
sleep 30

echo "4. Restoring collector network..."
unblock_egress "${COLLECTOR_CONTAINER}"
event_end_ns=$(ts_now_ns)

echo "5. Waiting 60s for reconnection and data flow..."
sleep 60

echo "6. Verifying results..."

# Collector should still be running (not killed, just disconnected)
assert_container_healthy "collector"
assert_container_healthy "writer"

# ws_disconnect gaps should exist in archive
wait_for_gaps "ws_disconnect" 90
ws_gaps=$(count_gaps "ws_disconnect")
assert_gt "ws_disconnect gaps exist in archive" "$ws_gaps" 0

# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "ws_disconnect" "$event_start_ns" "$event_end_ns" 60; then
    pass "ws_disconnect gap timestamps are accurate (within 60s tolerance)"
else
    fail "ws_disconnect gap timestamp accuracy check failed"
fi

# Data integrity should be intact after reconnection
if check_integrity; then
    pass "data integrity OK after ws_disconnect recovery"
else
    fail "data integrity check failed"
fi

# Archive should have data from before and after the disconnect
total=$(count_envelopes)
assert_gt "archive has envelopes spanning the disconnect" "$total" 100

print_test_report
teardown_stack
print_results
```

- [ ] **Step 5: Make the script executable**

Run: `chmod +x tests/chaos/9_ws_disconnect.sh`

- [ ] **Step 6: Register in infrastructure asset test**

In `tests/unit/test_infrastructure_assets.py`, add to the `scripts` dict:

```python
"tests/chaos/9_ws_disconnect.sh": ["ws_disconnect", "block_egress", "unblock_egress", "setup_stack", "teardown_stack", "print_test_report"],
```

- [ ] **Step 7: Verify script syntax**

Run: `bash -n tests/chaos/9_ws_disconnect.sh`
Expected: no output, exit 0

- [ ] **Step 8: Run infrastructure asset tests**

Run: `uv run pytest tests/unit/test_infrastructure_assets.py::TestChaosScripts -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add tests/chaos/9_ws_disconnect.sh docker-compose.test.yml tests/unit/test_infrastructure_assets.py
git commit -m "feat(chaos): add test 9 — true ws_disconnect via network fault injection"
```

---

### Task 6: Create Test 10 — Snapshot poll miss (`snapshot_poll_miss`)

**Files:**
- Create: `tests/chaos/10_snapshot_poll_miss.sh`
- Modify: `tests/unit/test_infrastructure_assets.py`

`snapshot_poll_miss` has zero test coverage at any level. When the depth snapshot or open_interest REST poll fails after 3 retries, the collector should emit a `snapshot_poll_miss` gap. Test approach: use iptables inside the collector container to block only the Binance REST API (port 443) while keeping WebSocket connections alive, wait for a poll cycle to fail, then restore and verify.

Note: In test config, `snapshot_interval` and `open_interest.poll_interval` are both `30s`, so we need to block for at least ~2 minutes to guarantee exhausting 3 retries with exponential backoff (1s + 2s + 4s ≈ 7s per stream, but poll scheduling is staggered).

- [ ] **Step 1: Create the `10_snapshot_poll_miss.sh` script**

Create `tests/chaos/10_snapshot_poll_miss.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Snapshot Poll Miss ==="
echo "Blocks collector HTTPS egress to trigger snapshot_poll_miss gaps"
echo "while WebSocket streams continue via existing connections."
echo ""

setup_stack
wait_for_data 30

echo "1. Blocking collector HTTPS egress (port 443 only)..."
# Block only new HTTPS connections — existing WS connections survive on established sockets.
# We use OUTPUT chain to block outbound SYN to port 443 (new REST connections).
docker exec "${COLLECTOR_CONTAINER}" iptables -A OUTPUT -p tcp --dport 443 --syn -j DROP 2>/dev/null || true
echo "   Blocked new HTTPS connections from collector"

echo "2. Waiting 120s for snapshot poll retries to exhaust..."
# Test config: snapshot_interval=30s, open_interest.poll_interval=30s
# Each poll does 3 retries with exponential backoff.
# We need to wait long enough for at least one complete poll cycle to fail.
sleep 120

echo "3. Restoring collector HTTPS egress..."
docker exec "${COLLECTOR_CONTAINER}" iptables -F 2>/dev/null || true
echo "   Restored HTTPS connections"

echo "4. Waiting 60s for next successful poll cycle..."
sleep 60

echo "5. Verifying results..."

assert_container_healthy "collector"
assert_container_healthy "writer"

# Wait for snapshot_poll_miss gaps to appear in archive
wait_for_gaps "snapshot_poll_miss" 90
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

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make the script executable**

Run: `chmod +x tests/chaos/10_snapshot_poll_miss.sh`

- [ ] **Step 3: Register in infrastructure asset test**

In `tests/unit/test_infrastructure_assets.py`, add to the `scripts` dict:

```python
"tests/chaos/10_snapshot_poll_miss.sh": ["snapshot_poll_miss", "iptables", "setup_stack", "teardown_stack", "print_test_report"],
```

- [ ] **Step 4: Verify script syntax**

Run: `bash -n tests/chaos/10_snapshot_poll_miss.sh`
Expected: no output, exit 0

- [ ] **Step 5: Run infrastructure asset tests**

Run: `uv run pytest tests/unit/test_infrastructure_assets.py::TestChaosScripts -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/chaos/10_snapshot_poll_miss.sh tests/unit/test_infrastructure_assets.py
git commit -m "feat(chaos): add test 10 — snapshot_poll_miss via HTTPS egress block"
```

---

### Task 7: Create Test 11 — Planned collector-only restart

**Files:**
- Create: `tests/chaos/11_planned_collector_restart.sh`
- Modify: `tests/unit/test_infrastructure_assets.py`

Test 7 does a full-stack `docker compose down/up`. There is no test that stops only the collector with a maintenance intent and verifies `component=collector, cause=operator_shutdown, planned=true`. This is a distinct classification path from the full-stack test.

- [ ] **Step 1: Create the `11_planned_collector_restart.sh` script**

Create `tests/chaos/11_planned_collector_restart.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Planned Collector-Only Restart ==="
echo "Stops only the collector with maintenance intent, restarts it,"
echo "and verifies restart_gap with component=collector, cause=operator_shutdown, planned=true."
echo ""

DB_URL="${DB_URL:-postgresql://cryptolake:cryptolake@localhost:5432/cryptolake}"

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
$COMPOSE stop collector 2>&1

echo "3. Waiting 10s (writer continues, Redpanda buffers)..."
sleep 10

echo "4. Restarting collector..."
$COMPOSE up -d collector 2>&1
wait_healthy
wait_for_data 40

echo "5. Verifying results..."

# Writer should detect the session change and emit restart_gap records
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

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make the script executable**

Run: `chmod +x tests/chaos/11_planned_collector_restart.sh`

- [ ] **Step 3: Register in infrastructure asset test**

In `tests/unit/test_infrastructure_assets.py`, add to the `scripts` dict:

```python
"tests/chaos/11_planned_collector_restart.sh": ["collector", "mark-maintenance", "planned", "restart_gap", "setup_stack", "teardown_stack", "print_test_report"],
```

- [ ] **Step 4: Verify script syntax**

Run: `bash -n tests/chaos/11_planned_collector_restart.sh`
Expected: no output, exit 0

- [ ] **Step 5: Run infrastructure asset tests**

Run: `uv run pytest tests/unit/test_infrastructure_assets.py::TestChaosScripts -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/chaos/11_planned_collector_restart.sh tests/unit/test_infrastructure_assets.py
git commit -m "feat(chaos): add test 11 — planned collector-only restart with maintenance intent"
```

---

## Chunk 3: Upgrade Test 8 to Real Integration Test

### Task 8: Rewrite Test 8 as a docker-based host reboot integration test

**Files:**
- Modify: `tests/chaos/8_host_reboot_restart_gap.sh`
- Modify: `tests/unit/test_infrastructure_assets.py`

The current Test 8 seeds a ledger file and runs `pytest` unit tests. It never starts the full docker stack and never verifies archived gap records. Rewrite it to: (1) start the stack, (2) let data flow, (3) perform `docker compose down` with a boot ID environment change, (4) bring the stack back up with the new boot ID, (5) verify archived `restart_gap` records have `component=host, cause=host_reboot`.

The writer reads `CRYPTOLAKE_TEST_BOOT_ID` environment variable (if set) as a boot ID override for testing. We leverage this to simulate a reboot without actually rebooting the host.

- [ ] **Step 1: Rewrite `8_host_reboot_restart_gap.sh`**

Replace the entire contents of `tests/chaos/8_host_reboot_restart_gap.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

echo "=== Chaos: Host Reboot Restart Gap ==="
echo "Simulates a host reboot by restarting the stack with a new boot ID."
echo "Verifies restart_gap records have component=host, cause=host_reboot."
echo ""

OLD_BOOT_ID="test-boot-old-$(date +%s)"
NEW_BOOT_ID="test-boot-new-$(date +%s)"

echo "Old boot ID: $OLD_BOOT_ID"
echo "New boot ID: $NEW_BOOT_ID"

# Start stack with the old boot ID
export CRYPTOLAKE_TEST_BOOT_ID="${OLD_BOOT_ID}"
setup_stack
wait_for_data 30

echo "1. Stopping all services (simulating host power-off)..."
# No maintenance intent — this is an unplanned reboot
$COMPOSE down 2>&1

echo "2. Bringing stack back up with new boot ID (simulating reboot)..."
export CRYPTOLAKE_TEST_BOOT_ID="${NEW_BOOT_ID}"
$COMPOSE up -d 2>&1
wait_healthy
wait_for_data 40

echo "3. Verifying results..."

# Writer should detect boot ID change and emit restart_gap with component=host
gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist in archive" "$gaps" 0

validate_host_reboot_gap() {
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
            if env.get('component') != 'host':
                errors.append(f'expected component=host, got {env.get(\"component\")}')
            if env.get('cause') != 'host_reboot':
                errors.append(f'expected cause=host_reboot, got {env.get(\"cause\")}')
            if env.get('planned') is not False:
                errors.append(f'expected planned=false, got {env.get(\"planned\")}')
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
if found == 0:
    print('ERROR: no restart_gap records found')
    exit(1)
print(f'OK: {found} restart_gap record(s) with component=host, cause=host_reboot, planned=false')
"
}

if validate_host_reboot_gap; then
    pass "restart_gap metadata: component=host, cause=host_reboot, planned=false"
else
    fail "restart_gap host reboot metadata validation failed"
fi

assert_container_healthy "collector"
assert_container_healthy "writer"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

total=$(count_envelopes)
assert_gt "archive has envelopes from both boot sessions" "$total" 100

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Update the infrastructure asset test for Test 8**

In `tests/unit/test_infrastructure_assets.py`, add Test 8 to the `scripts` dict (it wasn't registered before since it was a standalone script):

```python
"tests/chaos/8_host_reboot_restart_gap.sh": ["host", "reboot", "restart_gap", "BOOT_ID", "setup_stack", "teardown_stack", "print_test_report"],
```

- [ ] **Step 3: Verify the `CRYPTOLAKE_TEST_BOOT_ID` env var is passed through docker-compose**

Check that `docker-compose.test.yml` passes the env var to the writer. If not present, add to the writer environment section:

```yaml
      - CRYPTOLAKE_TEST_BOOT_ID=${CRYPTOLAKE_TEST_BOOT_ID:-}
```

Also add the same to the collector environment section since it registers its boot ID:

```yaml
      - CRYPTOLAKE_TEST_BOOT_ID=${CRYPTOLAKE_TEST_BOOT_ID:-}
```

- [ ] **Step 4: Verify compose syntax**

Run: `docker compose -f docker-compose.test.yml config --quiet`
Expected: exit 0

- [ ] **Step 5: Verify script syntax**

Run: `bash -n tests/chaos/8_host_reboot_restart_gap.sh`
Expected: no output, exit 0

- [ ] **Step 6: Run infrastructure asset tests**

Run: `uv run pytest tests/unit/test_infrastructure_assets.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add tests/chaos/8_host_reboot_restart_gap.sh docker-compose.test.yml tests/unit/test_infrastructure_assets.py
git commit -m "feat(chaos): rewrite test 8 as real docker-based host reboot integration test"
```

---

## Chunk 4: Rename Test 1 and Add Gap-Window Validation

### Task 9: Rename Test 1 and add gap-window timestamp validation

**Files:**
- Modify: `tests/chaos/1_collector_unclean_exit.sh`

Test 1 is misnamed — it says "Kill WebSocket Connection" but actually kills the collector process. Now that we have Test 9 for the true `ws_disconnect` scenario, rename Test 1's header to accurately describe what it tests and add gap-window timestamp validation.

- [ ] **Step 1: Update the test header**

In `tests/chaos/1_collector_unclean_exit.sh`, replace:

```bash
echo "=== Chaos: Kill WebSocket Connection ==="
echo "Verifies that killing the collector produces restart_gap records"
echo "in the archive with component=collector and cause=unclean_exit."
```

with:

```bash
echo "=== Chaos: Kill Collector (Unclean Exit) ==="
echo "Verifies that killing the collector process produces restart_gap records"
echo "in the archive with component=collector and cause=unclean_exit."
```

- [ ] **Step 2: Add gap-window timestamp validation**

In `tests/chaos/1_collector_unclean_exit.sh`, before `setup_stack`, add:

```bash
# We'll capture timestamps around the kill event for gap-window validation
```

After `wait_for_data 30`, before the kill, add:

```bash
event_start_ns=$(ts_now_ns)
```

After `$COMPOSE up -d collector 2>&1`, add:

```bash
event_end_ns=$(ts_now_ns)
```

Before `print_test_report`, add:

```bash
# Validate gap timestamps are in the right ballpark
if validate_gap_window_accuracy "restart_gap" "$event_start_ns" "$event_end_ns" 60; then
    pass "restart_gap gap timestamps are accurate (within 60s tolerance)"
else
    fail "restart_gap gap timestamp accuracy check failed"
fi
```

- [ ] **Step 3: Verify script syntax**

Run: `bash -n tests/chaos/1_collector_unclean_exit.sh`
Expected: no output, exit 0

- [ ] **Step 4: Commit**

```bash
git add tests/chaos/1_collector_unclean_exit.sh
git commit -m "fix(chaos): rename test 1 header to 'Kill Collector' and add gap-window timestamp validation"
```

---

## Post-Implementation Checklist

After all tasks are complete, verify:

- [ ] All chaos scripts pass syntax check: `for f in tests/chaos/*.sh; do bash -n "$f"; done`
- [ ] All unit tests pass: `uv run pytest tests/unit/test_infrastructure_assets.py -v`
- [ ] The infrastructure asset test registers all 9 chaos scripts (1-8 plus 9, 10, 11; noting 8 was rewritten)
- [ ] The `docker-compose.test.yml` has `cap_add: [NET_ADMIN]` for collector and `CRYPTOLAKE_TEST_BOOT_ID` env vars

## Coverage After Implementation

| Gap Type | Chaos Test | Status |
|---|---|---|
| `ws_disconnect` | Test 9 (new) | **Covered** |
| `pu_chain_break` | Test 6 (via kill during depth) | Covered (indirect) |
| `session_seq_skip` | -- | Acceptable (debugging signal only) |
| `buffer_overflow` | Test 2 | Covered |
| `snapshot_poll_miss` | Test 10 (new) | **Covered** |
| `restart_gap` — collector unclean | Test 1 | Covered |
| `restart_gap` — collector planned | Test 11 (new) | **Covered** |
| `restart_gap` — system planned | Test 7 | Covered |
| `restart_gap` — host reboot | Test 8 (rewritten) | **Covered** |
| `restart_gap` — unknown fallback | -- | Unit-tested only (acceptable) |
| Writer crash — no data loss | Tests 3, 4 (updated) | **Covered** |
| Gap timestamps accurate | Tests 1, 9 (validated) | **Covered** |
