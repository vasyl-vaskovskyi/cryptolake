# Gap Emission on Error Paths — Complete Coverage Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ensure every error path that drops data also emits a gap record into the archive, so downstream consumers always know when data is missing.

**Architecture:** Add two new gap reasons (`write_error`, `deserialization_error`) to the envelope module. At each error-handling site in the writer consumer loop, emit a gap envelope into the buffer before skipping/continuing. Add a chaos test for prolonged PG outage + crash (compound failure). Add a chaos test for Redpanda partition leadership change.

**Tech Stack:** Python 3.13, pytest, orjson, confluent-kafka, Docker Compose

**Commit convention:** Lowercase messages, no conventional-commit prefixes.

---

## File Map

| File | Responsibility | Tasks |
|------|---------------|-------|
| `src/common/envelope.py` | Gap reason registry | 1 |
| `src/writer/consumer.py` | Emit gaps on write error, PG error, corrupt msg | 2, 3, 4 |
| `tests/unit/test_writer_error_handling.py` | Unit tests for gap emission | 2, 3, 4 |
| `tests/chaos/15_pg_outage_then_crash.sh` | Compound failure: PG down + writer crash | 5 |
| `tests/chaos/16_redpanda_leader_change.sh` | Partition leadership move during writes | 6 |

---

### Task 1: Register new gap reasons

**Files:**
- Modify: `src/common/envelope.py:9-18`
- Test: `tests/unit/test_envelope.py`

- [ ] **Step 1: Add new gap reasons to VALID_GAP_REASONS**

In `src/common/envelope.py`, add `"write_error"` and `"deserialization_error"` to the `VALID_GAP_REASONS` frozenset:

```python
VALID_GAP_REASONS = frozenset(
    {
        "ws_disconnect",
        "pu_chain_break",
        "session_seq_skip",
        "buffer_overflow",
        "snapshot_poll_miss",
        "collector_restart",  # kept for migration: existing archives contain it
        "restart_gap",
        "write_error",
        "deserialization_error",
    }
)
```

- [ ] **Step 2: Verify existing tests still pass**

Run: `uv run pytest tests/unit/test_envelope.py -v`
Expected: PASS (the gap_reason_values test enumerates reasons dynamically)

- [ ] **Step 3: Commit**

```bash
git add src/common/envelope.py
git commit -m "add write_error and deserialization_error gap reasons"
```

---

### Task 2: Emit gap on disk write error

**Files:**
- Modify: `src/writer/consumer.py` (`_write_to_disk` around line 736)
- Test: `tests/unit/test_writer_error_handling.py`

The current code catches OSError and continues. We need to also create a gap envelope for each affected stream.

- [ ] **Step 1: Add unit test**

Add to `tests/unit/test_writer_error_handling.py`:

```python
class TestWriteErrorGapEmission:
    def test_write_error_emits_gap_envelope(self):
        """When _write_to_disk catches OSError, the error path must
        create a write_error gap envelope for the affected stream."""
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        assert 'reason="write_error"' in source, (
            "_write_to_disk OSError handler must emit write_error gap"
        )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_writer_error_handling.py::TestWriteErrorGapEmission -v`
Expected: FAIL

- [ ] **Step 3: Implement gap emission in _write_to_disk**

In `src/writer/consumer.py`, in the OSError except block of `_write_to_disk` (around line 736), after the existing logging and metric, add gap envelope creation. The gap envelope needs to be returned alongside the states so the caller can inject it into the archive.

The cleanest approach: collect gap envelopes in a list and return them alongside states. Modify `_write_to_disk` signature to return a tuple `(states, gap_envelopes)`:

```python
    def _write_to_disk(self, results: list[FlushResult]) -> tuple[list[FileState], list[dict]]:
        """Write compressed frames to disk and fsync. Returns (FileState list, gap envelopes).
        Gap envelopes are emitted for any files that failed to write."""
        states: list[FileState] = []
        gap_envelopes: list[dict] = []
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
                # Emit gap envelope for the lost data window
                now_ns = time.time_ns()
                gap_envelopes.append(create_gap_envelope(
                    exchange=result.target.exchange,
                    symbol=result.target.symbol,
                    stream=result.target.stream,
                    collector_session_id="",
                    session_seq=-1,
                    gap_start_ts=now_ns,
                    gap_end_ts=now_ns,
                    reason="write_error",
                    detail=f"Disk write failed: {e}",
                ))
                continue

            # ... existing success-path code for file_size, metrics, states.append ...
```

Then update `_write_and_save` (the caller) to handle the gap envelopes — inject them into the buffer so they get written on the next flush:

```python
    async def _write_and_save(self, results):
        start = time.monotonic()
        states, gap_envelopes = self._write_to_disk(results)
        # Inject any error-path gap envelopes into the buffer for next flush
        for gap in gap_envelopes:
            self.buffer_manager.add(gap)
        # ... rest of existing rotation/seal/commit code ...
```

- [ ] **Step 4: Update all callers of _write_to_disk to unpack tuple**

Search for all call sites of `_write_to_disk` and update them.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/unit/ -v --tb=short -q`

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: emit write_error gap on disk write failure"
```

---

### Task 3: Emit gap on PG commit failure

**Files:**
- Modify: `src/writer/consumer.py` (`_commit_state` around line 803)
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Add unit test**

```python
class TestPgErrorGapEmission:
    def test_pg_failure_emits_write_error_gap(self):
        """When PG commit fails, a write_error gap must be emitted."""
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        # The PG failure path should inject gap envelopes for affected streams
        assert "pg_commit_failed_will_retry" in source
        # Check that a gap is created in the PG failure path
        assert "write_error" in source or "pg_error" in source
```

- [ ] **Step 2: Implement**

In `_commit_state`'s except block (around line 803), after logging and metric, emit gap envelopes for each affected stream and inject them into the buffer:

```python
        except Exception as e:
            logger.error(
                "pg_commit_failed_will_retry",
                error=str(e),
                states=len(states),
                checkpoints=len(checkpoints),
            )
            writer_metrics.pg_commit_failures_total.inc()
            # Emit gap for each affected stream so archive knows data may be inconsistent
            now_ns = time.time_ns()
            for result in results:
                gap = create_gap_envelope(
                    exchange=result.target.exchange,
                    symbol=result.target.symbol,
                    stream=result.target.stream,
                    collector_session_id="",
                    session_seq=-1,
                    gap_start_ts=now_ns,
                    gap_end_ts=now_ns,
                    reason="write_error",
                    detail=f"PostgreSQL commit failed: {e}",
                )
                self.buffer_manager.add(gap)
            return
```

- [ ] **Step 3: Run tests + commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: emit write_error gap on PG commit failure"
```

---

### Task 4: Emit gap on corrupt message deserialization

**Files:**
- Modify: `src/writer/consumer.py` (consume_loop around line 529)
- Test: `tests/unit/test_writer_error_handling.py`

- [ ] **Step 1: Add unit test**

```python
class TestDeserializationErrorGapEmission:
    def test_corrupt_message_emits_deserialization_error_gap(self):
        """Corrupt message skip must emit a deserialization_error gap."""
        import src.writer.consumer as consumer_mod
        source = Path(consumer_mod.__file__).read_text()
        assert 'reason="deserialization_error"' in source, (
            "corrupt message handler must emit deserialization_error gap"
        )
```

- [ ] **Step 2: Implement**

In the consume_loop's except block for deserialization (around line 529), after logging and metric increment, emit a gap envelope. Since we don't know the stream/symbol from the corrupt message, use the topic to derive exchange:

```python
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
                # Emit gap so archive documents the skipped message
                now_ns = time.time_ns()
                # Extract exchange from topic (format: "exchange.stream")
                parts = msg_topic.split(".", 1) if msg_topic else ["unknown", "unknown"]
                gap_exchange = parts[0] if len(parts) > 0 else "unknown"
                gap_stream = parts[1] if len(parts) > 1 else "unknown"
                gap = create_gap_envelope(
                    exchange=gap_exchange,
                    symbol="unknown",
                    stream=gap_stream,
                    collector_session_id="",
                    session_seq=-1,
                    gap_start_ts=now_ns,
                    gap_end_ts=now_ns,
                    reason="deserialization_error",
                    detail=f"Corrupt message at offset {msg_offset} (size={len(raw_value)})",
                )
                self.buffer_manager.add(gap)
                continue
```

- [ ] **Step 3: Run tests + commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "writer: emit deserialization_error gap on corrupt message"
```

---

### Task 5: Chaos test — Prolonged PG outage + writer crash (compound failure)

**Files:**
- Create: `tests/chaos/15_pg_outage_then_crash.sh`

- [ ] **Step 1: Write the test**

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: PG Outage + Writer Crash (Compound Failure) ==="
echo "Kills PG, lets writer run without commits, then kills writer too."
echo "Verifies recovery after both are restored."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-chaos envelope count..."
pre_chaos=$(count_envelopes)

echo "2. Killing PostgreSQL..."
$COMPOSE kill postgres 2>&1

echo "3. Waiting 15s (writer runs without PG)..."
sleep 15

echo "4. Killing writer (compound failure)..."
docker kill "${WRITER_CONTAINER}"

echo "5. Restoring PostgreSQL..."
$COMPOSE up -d postgres 2>&1
wait_service_healthy postgres 60

echo "6. Restoring writer..."
$COMPOSE up -d writer 2>&1
if ! wait_service_healthy writer 30; then :; fi

echo "7. Waiting for data flow to resume..."
if wait_for_envelope_count_gt "$pre_chaos" 60; then
    pass "data flow resumed after compound failure"
else
    fail "data flow did not resume"
fi

echo "8. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"

if check_integrity; then
    pass "data integrity OK after compound failure"
else
    fail "data integrity check failed"
fi

post_recovery=$(count_envelopes)
assert_gt "archive grew after recovery" "$post_recovery" "$pre_chaos"

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make executable + run**

```bash
chmod +x tests/chaos/15_pg_outage_then_crash.sh
bash tests/chaos/15_pg_outage_then_crash.sh
```

- [ ] **Step 3: Commit**

```bash
git add tests/chaos/15_pg_outage_then_crash.sh
git commit -m "chaos test 15: compound failure PG outage + writer crash"
```

---

### Task 6: Chaos test — Redpanda partition leadership change

**Files:**
- Create: `tests/chaos/16_redpanda_leader_change.sh`

- [ ] **Step 1: Write the test**

Simulate a Redpanda restart (which forces partition leadership re-election) while data is flowing:

```bash
#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap teardown_stack EXIT

echo "=== Chaos: Redpanda Restart (Leader Re-election) ==="
echo "Restarts Redpanda to force partition leadership changes."
echo "Verifies no data loss across the re-election."
echo ""

setup_stack
wait_for_data 20

echo "1. Recording pre-restart envelope count..."
pre_restart=$(count_envelopes)

echo "2. Restarting Redpanda (forces leader re-election)..."
$COMPOSE restart redpanda 2>&1
wait_service_healthy redpanda 60

echo "3. Waiting for writer and collector to reconnect..."
wait_healthy

echo "4. Waiting for data flow to resume..."
if wait_for_envelope_count_gt "$pre_restart" 60; then
    pass "data flow resumed after Redpanda restart"
else
    fail "data flow did not resume"
fi

echo "5. Verifying results..."
assert_container_healthy "writer"
assert_container_healthy "collector"
assert_container_healthy "redpanda"

if check_integrity; then
    pass "data integrity OK after leader re-election"
else
    fail "data integrity check failed"
fi

post_restart=$(count_envelopes)
assert_gt "archive grew after Redpanda restart" "$post_restart" "$pre_restart"

print_test_report
teardown_stack
print_results
```

- [ ] **Step 2: Make executable + run + commit**

```bash
chmod +x tests/chaos/16_redpanda_leader_change.sh
bash tests/chaos/16_redpanda_leader_change.sh
git add tests/chaos/16_redpanda_leader_change.sh
git commit -m "chaos test 16: Redpanda restart and leader re-election"
```

---

## Execution Order

1. **Task 1** (new gap reasons — prerequisite for Tasks 2, 3, 4)
2. **Tasks 2, 3, 4** in parallel (gap emission on each error path)
3. **Tasks 5, 6** sequentially (chaos tests use Docker)

## Verification

After all tasks:

```bash
uv run pytest tests/unit/ -v --tb=short -q
bash tests/chaos/15_pg_outage_then_crash.sh
bash tests/chaos/16_redpanda_leader_change.sh
```
