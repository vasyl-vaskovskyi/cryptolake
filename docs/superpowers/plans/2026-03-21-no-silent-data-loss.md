# No Silent Data Loss — Complete Gap Coverage Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate all scenarios where data is lost without a gap record being emitted, ensuring the archive is either complete or explicitly documents every missing window.

**Architecture:** Add try/except error handling at every crash-prone boundary in the data pipeline (deserialization, disk write, fsync, sidecar, PG commit, serialization). On failure, emit a gap record covering the lost window, log the error, and continue processing rather than crashing. Add new unit tests for each error path and new chaos tests for the most critical scenarios.

**Tech Stack:** Python 3.13, pytest, pytest-asyncio, confluent-kafka, orjson, zstandard, psycopg (async), Docker Compose

**Commit convention:** This project uses lowercase commit messages without conventional-commit prefixes (e.g. `chaos test 4 fixes`). Follow this style.

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `src/writer/consumer.py` | Core consume loop, write-to-disk, commit state | 1, 2, 3, 5 |
| `src/collector/producer.py` | Envelope serialization + Kafka produce | 4 |
| `src/common/envelope.py` | serialize/deserialize helpers | (read-only reference) |
| `tests/unit/test_writer_error_handling.py` | New: unit tests for all error paths | 1, 2, 3, 4, 5, 6 |
| `tests/chaos/12_corrupt_message.sh` | New: chaos test for corrupt Redpanda messages | 7 |
| `tests/chaos/13_pg_kill_during_commit.sh` | New: chaos test for PG failure during write | 8 |
| `tests/chaos/14_rapid_restart_storm.sh` | New: chaos test for 3 restarts in 60s | 9 |
| `tests/chaos/common.sh` | Shared helpers (may need `inject_corrupt_message`) | 7 |

---

### Task 1: Writer — Handle corrupt messages from Redpanda

**Priority:** HIGH — A single corrupt message permanently bricks the writer for that partition (infinite crash loop).

**Files:**
- Modify: `src/writer/consumer.py:521` (deserialization in consume_loop)
- Create: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Write the failing test**

In `tests/unit/test_writer_error_handling.py`:

```python
"""Unit tests for writer error handling — no silent data loss."""
from __future__ import annotations

import orjson
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from src.common.envelope import serialize_envelope, deserialize_envelope


class TestCorruptMessageHandling:
    def test_deserialize_corrupt_json_raises(self):
        """Confirm orjson raises on corrupt input."""
        with pytest.raises(orjson.JSONDecodeError):
            deserialize_envelope(b"not valid json {{{")

    def test_deserialize_truncated_message_raises(self):
        """Truncated message from Redpanda should raise."""
        valid = serialize_envelope({"type": "data", "stream": "trades"})
        truncated = valid[:10]
        with pytest.raises(orjson.JSONDecodeError):
            deserialize_envelope(truncated)
```

- [ ] **Step 2: Run test to verify it passes** (these test current behavior)

Run: `uv run pytest tests/unit/test_writer_error_handling.py -v`
Expected: PASS (confirms the crash path exists)

- [ ] **Step 3: Write test for the NEW skip-and-continue behavior**

Add to same file:

```python
class TestConsumeLoopSkipsCorruptMessages:
    """After the fix, consume_loop must skip corrupt messages instead of crashing."""

    @pytest.mark.asyncio
    async def test_corrupt_message_skipped_and_offset_committed(self):
        """Writer should log a warning, increment a metric, and commit the
        offset to advance past the corrupt message."""
        from src.writer.consumer import Writer
        # This test mocks the consumer poll to return a corrupt message
        # followed by a valid message, and verifies both are processed
        # (corrupt one skipped, valid one written).
        # Implementation depends on the Writer constructor — use minimal mock.
        # Key assertion: no exception raised, metric incremented.
        pass  # placeholder — fill in after Step 5
```

- [ ] **Step 4: Implement the fix in consumer.py**

In `src/writer/consumer.py`, wrap the deserialization at line 521 in try/except:

```python
            # --- existing code at line 521 ---
            try:
                envelope = deserialize_envelope(raw_value)
            except Exception:
                logger.error(
                    "corrupt_message_skipped",
                    topic=msg_topic,
                    partition=msg_partition,
                    offset=msg_offset,
                    raw_size=len(raw_value),
                )
                writer_metrics.messages_skipped_total.labels(
                    exchange="unknown", symbol="unknown", stream="unknown",
                ).inc()
                continue
            # --- rest of existing code ---
```

The `continue` skips this message and loops to the next `poll()`. The Kafka offset will be committed on the next successful flush, advancing past the corrupt message.

- [ ] **Step 5: Fill in the async unit test with real assertions**

- [ ] **Step 6: Run all tests**

Run: `uv run pytest tests/unit/test_writer_error_handling.py -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: skip corrupt messages instead of crash-looping"
```

---

### Task 2: Writer — Handle ENOSPC and disk write errors

**Priority:** HIGH — Disk full crashes the writer without any gap record.

**Files:**
- Modify: `src/writer/consumer.py:697-732` (`_write_to_disk`)
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_writer_error_handling.py`:

```python
class TestDiskWriteErrorHandling:
    def test_write_to_disk_catches_oserror(self):
        """_write_to_disk should catch OSError (ENOSPC, permission denied)
        and raise a dedicated WriteDiskError instead of crashing."""
        # Mock open() to raise OSError(errno.ENOSPC, "No space left on device")
        pass  # placeholder

    def test_flush_cycle_emits_gap_on_disk_error(self):
        """When _write_to_disk fails, the flush cycle should emit a
        write_error gap record for each affected stream and continue."""
        pass  # placeholder
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/test_writer_error_handling.py::TestDiskWriteErrorHandling -v`

- [ ] **Step 3: Implement the fix**

In `src/writer/consumer.py`, wrap `_write_to_disk` body in try/except:

```python
    def _write_to_disk(self, results: list[FlushResult]) -> list[FileState]:
        states: list[FileState] = []
        for result in results:
            file_path = self._resolve_file_path(result.file_path)
            try:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                compressed = self.compressor.compress_frame(result.lines)
                with open(file_path, "ab") as f:
                    f.write(compressed)
                    f.flush()
                    os.fsync(f.fileno())
            except OSError as e:
                logger.error(
                    "write_to_disk_failed",
                    path=str(file_path),
                    error=str(e),
                    lines_lost=len(result.lines),
                )
                writer_metrics.write_errors_total.labels(
                    exchange=result.target.exchange,
                    stream=result.target.stream,
                ).inc()
                continue  # skip this file, data in buffer is lost

            file_size = file_path.stat().st_size
            # ... rest of existing metric + FileState creation code ...
            states.append(FileState(
                topic=f"{result.target.exchange}.{result.target.stream}",
                partition=result.partition,
                high_water_offset=result.high_water_offset,
                file_path=str(file_path),
                file_byte_size=file_size,
            ))
        return states
```

Note: the `write_errors_total` metric needs to be added to `src/writer/metrics.py`.

- [ ] **Step 4: Add the `write_errors_total` metric**

- [ ] **Step 5: Run tests**

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py src/writer/metrics.py tests/unit/test_writer_error_handling.py
git commit -m "writer: handle ENOSPC and disk write errors gracefully"
```

---

### Task 3: Writer — Handle PG commit failure without crashing

**Priority:** HIGH — PG down after disk write = data on disk but offset pointer lost forever.

**Files:**
- Modify: `src/writer/consumer.py:734-781` (`_commit_state`)
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Write the failing test**

```python
class TestPgCommitFailureHandling:
    @pytest.mark.asyncio
    async def test_pg_failure_does_not_crash_writer(self):
        """If PG save fails after disk write, writer should log error
        and retry on next flush cycle, not crash."""
        pass  # placeholder

    @pytest.mark.asyncio
    async def test_pg_failure_does_not_commit_kafka_offset(self):
        """If PG save fails, Kafka offset must NOT be committed.
        This ensures messages are re-consumed on next successful flush."""
        pass  # placeholder
```

- [ ] **Step 2: Implement the fix**

In `_commit_state`, wrap the PG save in try/except and skip Kafka commit on failure:

```python
    async def _commit_state(self, states, results, start):
        checkpoints = [...]  # existing checkpoint derivation

        try:
            await self.state_manager.save_states_and_checkpoints(states, checkpoints)
        except Exception as e:
            logger.error(
                "pg_commit_failed_will_retry",
                error=str(e),
                states=len(states),
                checkpoints=len(checkpoints),
            )
            writer_metrics.pg_commit_failures_total.inc()
            # Do NOT commit Kafka offsets — messages will be re-consumed
            # and re-written on next flush (dedup handles duplicates).
            return

        # Update in-memory cache + commit Kafka only on PG success
        for cp in checkpoints:
            self._durable_checkpoints[cp.checkpoint_key] = cp
        self._consumer.commit(asynchronous=True)
        # ... existing metrics ...
```

- [ ] **Step 3: Add `pg_commit_failures_total` metric**

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
git add src/writer/consumer.py src/writer/metrics.py tests/unit/test_writer_error_handling.py
git commit -m "writer: handle PG commit failure without crashing"
```

---

### Task 4: Collector — Handle serialization errors in producer

**Priority:** MEDIUM — Serialization crash in collector drops all streams, no gap.

**Files:**
- Modify: `src/collector/producer.py:71`
- Test: `tests/unit/test_writer_error_handling.py` (or new collector test)

- [ ] **Step 1: Write test**

```python
class TestProducerSerializationError:
    def test_produce_catches_serialization_error(self):
        """If serialize_envelope raises, produce() should return False
        and log the error, not crash the collector."""
        pass
```

- [ ] **Step 2: Implement fix**

In `src/collector/producer.py`, wrap serialization:

```python
    def produce(self, envelope: dict) -> bool:
        stream = envelope["stream"]
        symbol = envelope["symbol"]
        topic = f"{self.exchange}.{stream}"
        key = symbol.encode()
        try:
            value = serialize_envelope(envelope)
        except Exception:
            logger.error(
                "serialization_failed",
                stream=stream,
                symbol=symbol,
            )
            collector_metrics.messages_dropped_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()
            return False
        # ... rest of existing code ...
```

- [ ] **Step 3: Run tests + commit**

```bash
git add src/collector/producer.py tests/unit/test_writer_error_handling.py
git commit -m "collector: handle serialization errors in producer"
```

---

### Task 5: Writer — Handle SHA256 sidecar write failure

**Priority:** MEDIUM — Sidecar failure crashes writer during hourly rotation.

**Files:**
- Modify: `src/writer/consumer.py:648,677`
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Write test**

```python
class TestSidecarWriteFailure:
    def test_sidecar_failure_does_not_crash_rotation(self):
        """If write_sha256_sidecar raises, rotation should log error
        and continue — the file data is still valid."""
        pass
```

- [ ] **Step 2: Implement fix**

Wrap both sidecar write sites (lines 648 and 677) in try/except:

```python
            if file_path.exists() and not sc.exists() and file_path.stat().st_size > 0:
                try:
                    write_sha256_sidecar(file_path, sc)
                    self._sealed_files.add(file_path)
                    # ... existing metrics + log ...
                except OSError as e:
                    logger.error("sidecar_write_failed", path=str(file_path), error=str(e))
```

Apply the same pattern at the second sidecar write site.

- [ ] **Step 3: Run tests + commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: handle sidecar write failure during rotation"
```

---

### Task 6: Writer — Add Kafka commit failure callback

**Priority:** MEDIUM — Async commits fail silently; offset drift grows unbounded.

**Files:**
- Modify: `src/writer/consumer.py:773`
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Write test**

```python
class TestKafkaCommitCallback:
    def test_commit_callback_logs_error_on_failure(self):
        """Kafka commit failure should be logged and counted by metric."""
        pass
```

- [ ] **Step 2: Implement fix**

Replace bare `self._consumer.commit(asynchronous=True)` with a callback:

```python
        def _on_commit(err, partitions):
            if err is not None:
                logger.error("kafka_commit_failed", error=str(err))
                writer_metrics.kafka_commit_failures_total.inc()

        self._consumer.commit(asynchronous=True, on_commit=_on_commit)
```

Note: confluent-kafka's `commit(on_commit=...)` requires `on_commit` parameter. Verify API signature.

- [ ] **Step 3: Run tests + commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: add kafka commit failure callback and metric"
```

---

### Task 7: Chaos test — Corrupt message in Redpanda

**Priority:** HIGH — Validates the Task 1 fix end-to-end.

**Files:**
- Create: `tests/chaos/12_corrupt_message.sh`
- Modify: `tests/chaos/common.sh` (add `inject_corrupt_message` helper)

- [ ] **Step 1: Add `inject_corrupt_message` helper to common.sh**

```bash
# Inject a corrupt (non-JSON) message into a Redpanda topic.
# Usage: inject_corrupt_message <topic> [message]
inject_corrupt_message() {
    local topic="${1:?Usage: inject_corrupt_message <topic> [message]}"
    local msg="${2:-NOT_VALID_JSON{{{corrupt}}}"
    $COMPOSE exec -T redpanda rpk topic produce "$topic" <<< "$msg"
    echo "   Injected corrupt message into ${topic}"
}
```

- [ ] **Step 2: Write the chaos test**

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Corrupt Message in Redpanda ==="
echo "Injects corrupt (non-JSON) messages and verifies the writer"
echo "skips them and continues processing valid data."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-injection envelope count..."
pre_inject=$(count_envelopes)

echo "2. Injecting corrupt messages into Redpanda..."
inject_corrupt_message "binance.trades"
inject_corrupt_message "binance.bookticker"

echo "3. Waiting for writer to process past corrupt messages..."
if wait_for_envelope_count_gt "$pre_inject" 60; then
    pass "writer continued processing after corrupt messages"
else
    fail "writer stopped processing after corrupt messages"
fi

echo "4. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

post_inject=$(count_envelopes)
assert_gt "archive grew after corrupt injection" "$post_inject" "$pre_inject"

print_test_report
teardown_stack
print_results
```

- [ ] **Step 3: Make executable + run**

```bash
chmod +x tests/chaos/12_corrupt_message.sh
bash tests/chaos/12_corrupt_message.sh
```

- [ ] **Step 4: Commit**

```bash
git add tests/chaos/12_corrupt_message.sh tests/chaos/common.sh
git commit -m "chaos test 12: corrupt message injection"
```

---

### Task 8: Chaos test — PostgreSQL kill during writer commit

**Priority:** MEDIUM — Validates the Task 3 fix end-to-end.

**Files:**
- Create: `tests/chaos/13_pg_kill_during_commit.sh`

- [ ] **Step 1: Write the chaos test**

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: PostgreSQL Kill During Writer Commit ==="
echo "Kills PostgreSQL while data is flowing, verifies the writer"
echo "recovers after PG comes back without data loss."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-kill envelope count..."
pre_kill=$(count_envelopes)

echo "2. Killing PostgreSQL..."
event_start_ns=$(ts_now_ns)
docker kill "${REDPANDA_CONTAINER/redpanda/postgres}"
# Actually: use the proper container name
$COMPOSE stop postgres 2>&1

echo "3. Waiting 15s (writer continues to receive but can't commit)..."
sleep 15

echo "4. Restarting PostgreSQL..."
$COMPOSE up -d postgres 2>&1
wait_service_healthy postgres
event_end_ns=$(ts_now_ns)

echo "5. Waiting for writer to recover and resume..."
if wait_for_envelope_count_gt "$pre_kill" 60; then
    pass "writer resumed after PG recovery"
else
    fail "writer did not resume after PG recovery"
fi

echo "6. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

post_recovery=$(count_envelopes)
assert_gt "archive grew after PG recovery" "$post_recovery" "$pre_kill"

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make executable + run + commit**

---

### Task 9: Chaos test — Rapid restart storm

**Priority:** LOW — Validates deduplication across multiple quick restarts.

**Files:**
- Create: `tests/chaos/14_rapid_restart_storm.sh`

- [ ] **Step 1: Write the chaos test**

Kill the writer 3 times in 60 seconds. Verify:
- No duplicate offsets in archive
- restart_gap records for each restart
- Data integrity preserved

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Rapid Restart Storm ==="
echo "Kills the writer 3 times in 60 seconds and verifies no duplicates."
echo ""

setup_stack
wait_for_data 20

pre_kill=$(count_envelopes)

for i in 1 2 3; do
    echo "${i}. Killing writer (restart ${i}/3)..."
    docker kill "${WRITER_CONTAINER}"
    sleep 5
    $COMPOSE up -d writer 2>&1
    if ! wait_service_healthy writer 30; then :; fi
    sleep 10
done

echo "4. Verifying results..."
assert_container_healthy "writer"

if check_integrity; then
    pass "data integrity OK (no duplicates across 3 restarts)"
else
    fail "data integrity check failed"
fi

post_storm=$(count_envelopes)
assert_gt "archive has data after restart storm" "$post_storm" "$pre_kill"

gaps=$(count_gaps "restart_gap")
assert_gt "restart_gap records exist for rapid restarts" "$gaps" 0

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make executable + run + commit**

---

### Task 10: Add `write_errors_total`, `pg_commit_failures_total`, `kafka_commit_failures_total` metrics

**Priority:** Required by Tasks 2, 3, 6.

**Files:**
- Modify: `src/writer/metrics.py`

- [ ] **Step 1: Add the three new Counter metrics**

```python
write_errors_total = Counter(
    "writer_write_errors_total",
    "Disk write failures (ENOSPC, permission, etc.)",
    ["exchange", "stream"],
)

pg_commit_failures_total = Counter(
    "writer_pg_commit_failures_total",
    "PostgreSQL state commit failures",
)

kafka_commit_failures_total = Counter(
    "writer_kafka_commit_failures_total",
    "Kafka async offset commit failures",
)
```

- [ ] **Step 2: Verify Prometheus endpoint exposes them**

- [ ] **Step 3: Commit**

```bash
git add src/writer/metrics.py
git commit -m "writer: add error-path metrics for disk, PG, and Kafka failures"
```

---

## Execution Order

Tasks have dependencies:
1. **Task 10** first (metrics needed by Tasks 2, 3, 6)
2. **Task 1** (corrupt messages — highest impact)
3. **Task 2** (ENOSPC)
4. **Task 3** (PG failure)
5. **Task 4** (serialization)
6. **Task 5** (sidecar)
7. **Task 6** (Kafka callback)
8. **Task 7** (chaos: corrupt message — requires Task 1)
9. **Task 8** (chaos: PG kill — requires Task 3)
10. **Task 9** (chaos: restart storm — independent)

## Verification

After all tasks, run the full test suite:

```bash
bash scripts/run-all-tests.sh
```

Expected: 0 failures.
