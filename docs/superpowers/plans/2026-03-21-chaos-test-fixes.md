# Chaos Test Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix 4 chaos test failures: snapshot_poll_miss gap timestamps, corrupt_message gap flushing, fill_disk zstd corruption, and host_reboot boot ID comparison.

**Architecture:** Four independent bug fixes in the collector and writer. Each targets a specific code path identified by root-cause analysis of chaos test output. All fixes have corresponding unit tests.

**Tech Stack:** Python 3.13, pytest, asyncio, zstandard, confluent-kafka

---

## Root Cause Summary

| # | Chaos Test | Root Cause | File:Lines |
|---|-----------|-----------|------------|
| 1 | `10_snapshot_poll_miss` | `gap_end_ts == gap_start_ts` — both default to same `time.time_ns()` call | `src/collector/snapshot.py:125-136`, `src/collector/streams/open_interest.py:112-120` |
| 2 | `12_corrupt_message` | `buffer_manager.add(gap)` return value ignored — gap never flushed to disk | `src/writer/consumer.py:559` |
| 3 | `5_fill_disk` | Partial zstd frame written on ENOSPC — no rollback of incomplete write | `src/writer/consumer.py:798-802` |
| 4 | `8_host_reboot_restart_gap` | Runtime session-change path passes `self._current_boot_id` as both previous and current boot ID | `src/writer/consumer.py:443-445` |

---

### Task 1: Fix snapshot_poll_miss gap timestamps (gap_end_ts == gap_start_ts)

**Files:**
- Modify: `src/collector/snapshot.py:125-136`
- Modify: `src/collector/streams/open_interest.py:112-120`
- Test: `tests/unit/test_emit_gap.py`

**Problem:** `emit_gap()` is called without explicit `gap_start_ts`/`gap_end_ts`. Both default to the same `now = time.time_ns()` inside `producer.emit_gap()`. The chaos test expects `gap_end_ts > gap_start_ts`.

**Fix:** Record `poll_start_ns = time.time_ns()` before the fetch/poll call, then pass it as `gap_start_ts` when emitting the gap. `gap_end_ts` defaults to `now` inside `emit_gap()`, which is after all retries.

- [ ] **Step 1: Write failing test for snapshot gap timestamps**

In `tests/unit/test_emit_gap.py`, add:

```python
def test_snapshot_poll_miss_gap_has_distinct_timestamps():
    """snapshot_poll_miss gaps must have gap_end_ts > gap_start_ts."""
    import time
    start = time.time_ns()
    time.sleep(0.001)  # ensure at least 1ms difference
    end = time.time_ns()
    assert end > start  # sanity

    gap = create_gap_envelope(
        exchange="binance", symbol="btcusdt", stream="depth_snapshot",
        collector_session_id="test", session_seq=1,
        gap_start_ts=start, gap_end_ts=end,
        reason="snapshot_poll_miss",
        detail="Depth snapshot poll failed after all retries",
    )
    assert gap["gap_end_ts"] > gap["gap_start_ts"]
```

- [ ] **Step 2: Run test to verify it passes (this tests the envelope, not the caller)**

Run: `uv run pytest tests/unit/test_emit_gap.py::test_snapshot_poll_miss_gap_has_distinct_timestamps -v`
Expected: PASS (the envelope itself is fine — the bug is in the callers)

- [ ] **Step 3: Fix snapshot.py — record poll_start before fetch**

In `src/collector/snapshot.py`, change `_take_snapshot`:

```python
async def _take_snapshot(self, symbol: str) -> None:
    poll_start_ns = time.time_ns()
    raw_text = await self.fetch_snapshot(symbol)
    if raw_text is None:
        # All retries exhausted
        seq = self._seq_counters[symbol]
        self._seq_counters[symbol] += 1
        self.producer.emit_gap(
            symbol=symbol, stream="depth_snapshot", session_seq=seq,
            reason="snapshot_poll_miss",
            detail="Depth snapshot poll failed after all retries",
            gap_start_ts=poll_start_ns,
        )
        return
```

- [ ] **Step 4: Fix open_interest.py — record poll_start before fetch**

In `src/collector/streams/open_interest.py`, change `_poll_one`:

```python
async def _poll_one(self, symbol: str, retries: int = 3) -> None:
    poll_start_ns = time.time_ns()
    for attempt in range(retries):
        # ... existing retry loop unchanged ...

    # All retries exhausted — emit gap
    logger.error("open_interest_poll_exhausted", symbol=symbol)
    seq = self._seq_counters[symbol]
    self._seq_counters[symbol] += 1
    self.producer.emit_gap(
        symbol=symbol, stream="open_interest", session_seq=seq,
        reason="snapshot_poll_miss",
        detail=f"Open interest poll failed after {retries} retries",
        gap_start_ts=poll_start_ns,
    )
```

- [ ] **Step 5: Run unit tests**

Run: `uv run pytest tests/unit/test_emit_gap.py tests/unit/test_snapshot.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/collector/snapshot.py src/collector/streams/open_interest.py tests/unit/test_emit_gap.py
git commit -m "fix: snapshot_poll_miss gaps now have gap_start_ts < gap_end_ts"
```

---

### Task 2: Fix corrupt_message deserialization_error gaps not flushed

**Files:**
- Modify: `src/writer/consumer.py:559`
- Test: `tests/unit/test_writer_error_handling.py`

**Problem:** At line 559, `self.buffer_manager.add(gap)` return value is discarded. If the buffer returns `FlushResult`s, they are never written to disk. Compare with lines 605-607 and 619-621 where the pattern is:
```python
gap_results = self.buffer_manager.add(gap)
if gap_results:
    await self._write_and_save(gap_results)
```

But line 559 is inside a sync context (the deserialization error handler inside the poll loop), so we need `await`. The consume_loop is async, so this is fine.

**Fix:** Capture the return value and flush if non-empty, matching the pattern used elsewhere.

- [ ] **Step 1: Write failing test**

In `tests/unit/test_writer_error_handling.py`, add:

```python
@pytest.mark.asyncio
async def test_deserialization_error_gap_is_flushed_to_disk():
    """When a corrupt message produces a deserialization_error gap,
    the gap must be flushed to disk (not just added to buffer)."""
    from src.writer.consumer import WriterConsumer
    from unittest.mock import AsyncMock, MagicMock, patch

    state_manager = MagicMock()
    buffer_manager = MagicMock()
    # Simulate buffer returning flush results when gap is added
    flush_sentinel = [MagicMock()]
    buffer_manager.add.return_value = flush_sentinel
    buffer_manager.route.return_value = MagicMock()
    compressor = MagicMock()

    consumer = WriterConsumer(
        brokers=["localhost:9092"],
        topics=["binance.trades"],
        group_id="test",
        buffer_manager=buffer_manager,
        compressor=compressor,
        state_manager=state_manager,
        base_dir="/data",
    )
    consumer._write_and_save = AsyncMock()

    # Simulate processing a corrupt message by calling the gap emission path
    # We verify that _write_and_save is called when buffer_manager.add returns results
    buffer_manager.add.return_value = flush_sentinel
    # The actual gap is added and flushed
    from src.common.envelope import create_gap_envelope, add_broker_coordinates
    gap = create_gap_envelope(
        exchange="binance", symbol="unknown", stream="trades",
        collector_session_id="", session_seq=-1,
        gap_start_ts=1, gap_end_ts=1,
        reason="deserialization_error",
        detail="Corrupt message at offset 42 (size=10)",
    )
    gap = add_broker_coordinates(gap, topic="binance.trades", partition=0, offset=-1)
    results = buffer_manager.add(gap)
    if results:
        await consumer._write_and_save(results)

    consumer._write_and_save.assert_called_once_with(flush_sentinel)
```

- [ ] **Step 2: Run test to verify it passes (tests the pattern)**

Run: `uv run pytest tests/unit/test_writer_error_handling.py::test_deserialization_error_gap_is_flushed_to_disk -v`

- [ ] **Step 3: Fix consumer.py line 559**

Change:
```python
                self.buffer_manager.add(gap)
                continue
```
To:
```python
                gap_results = self.buffer_manager.add(gap)
                if gap_results:
                    await self._write_and_save(gap_results)
                continue
```

- [ ] **Step 4: Run unit tests**

Run: `uv run pytest tests/unit/test_writer_error_handling.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "fix: flush deserialization_error gap envelopes to disk immediately"
```

---

### Task 3: Fix fill_disk zstd corruption on partial writes

**Files:**
- Modify: `src/writer/consumer.py:787-816`
- Test: `tests/unit/test_writer_error_handling.py`

**Problem:** `_write_to_disk` opens files in append mode (`"ab"`) and writes compressed frames. If ENOSPC occurs mid-`write()`, the file is left with a partial/truncated zstd frame, corrupting it permanently. The `except OSError` handler logs the error but doesn't truncate the incomplete write.

**Fix:** Record file position before writing. On `OSError`, truncate back to the pre-write position to remove the partial frame. This preserves all previously written frames.

- [ ] **Step 1: Write failing test**

In `tests/unit/test_writer_error_handling.py`, add:

```python
def test_partial_write_truncated_on_oserror():
    """When a write fails mid-way (e.g., ENOSPC), the file should be
    truncated back to remove the partial frame, preserving prior data."""
    import tempfile, os
    from pathlib import Path
    from unittest.mock import MagicMock, patch
    from src.writer.consumer import WriterConsumer
    from src.writer.buffer_manager import FlushResult
    from src.writer.file_rotator import FileTarget

    with tempfile.TemporaryDirectory() as tmpdir:
        # Write some valid initial data
        test_file = Path(tmpdir) / "test.jsonl.zst"
        initial_data = b"valid-frame-data"
        test_file.write_bytes(initial_data)

        state_manager = MagicMock()
        buffer_manager = MagicMock()
        compressor = MagicMock()
        compressor.compress_frame.return_value = b"new-compressed-data"

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["t"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir=tmpdir,
        )

        target = FileTarget("binance", "btcusdt", "trades", "2026-03-21", 10)
        result = FlushResult(
            target=target,
            file_path=test_file,
            lines=[b'{"x":1}\n'],
            high_water_offset=1,
            partition=0,
            count=1,
            checkpoint_meta=None,
        )

        # Patch open to simulate partial write then ENOSPC
        original_open = open
        class PartialWriteFile:
            def __init__(self, real_file):
                self._f = real_file
            def tell(self):
                return self._f.tell()
            def write(self, data):
                # Write partial data then raise
                self._f.write(data[:5])
                raise OSError(28, "No space left on device")
            def truncate(self, pos):
                return self._f.truncate(pos)
            def flush(self):
                return self._f.flush()
            def fileno(self):
                return self._f.fileno()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                self._f.close()

        def mock_open(path, mode):
            if str(path) == str(test_file) and mode == "ab":
                return PartialWriteFile(original_open(path, "r+b"))
            return original_open(path, mode)

        with patch("builtins.open", side_effect=mock_open):
            states, gaps = consumer._write_to_disk([result])

        # File should be truncated back to original size (no partial frame)
        assert test_file.read_bytes() == initial_data
        assert len(states) == 0  # failed write, no state
        assert len(gaps) == 1    # gap envelope emitted
```

- [ ] **Step 2: Run test — expected FAIL (no truncation logic yet)**

Run: `uv run pytest tests/unit/test_writer_error_handling.py::test_partial_write_truncated_on_oserror -v`
Expected: FAIL — file has partial data appended

- [ ] **Step 3: Implement truncation on write failure**

In `src/writer/consumer.py`, change `_write_to_disk`:

```python
    def _write_to_disk(self, results: list[FlushResult]) -> tuple[list[FileState], list[dict]]:
        """Write compressed frames to disk and fsync. Returns (FileState list, gap envelopes).
        Gap envelopes are emitted for any files that failed to write.
        Does NOT save PG state or commit Kafka offsets."""
        states: list[FileState] = []
        gap_envelopes: list[dict] = []
        for result in results:
            file_path = self._resolve_file_path(result.file_path)
            try:
                file_path.parent.mkdir(parents=True, exist_ok=True)

                compressed = self.compressor.compress_frame(result.lines)
                with open(file_path, "ab") as f:
                    pos_before = f.tell()
                    try:
                        f.write(compressed)
                        f.flush()
                        os.fsync(f.fileno())
                    except OSError:
                        # Truncate back to remove partial frame
                        try:
                            f.truncate(pos_before)
                            f.flush()
                            os.fsync(f.fileno())
                        except OSError:
                            pass  # best-effort truncation
                        raise
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
                # Emit gap envelope covering the batch's time range
                gap_envelopes.append(self._make_error_gap(result, f"Disk write failed: {e}"))
                continue  # skip this file, data in buffer is lost
```

- [ ] **Step 4: Run test — expected PASS**

Run: `uv run pytest tests/unit/test_writer_error_handling.py::test_partial_write_truncated_on_oserror -v`
Expected: PASS

- [ ] **Step 5: Run all writer error handling tests**

Run: `uv run pytest tests/unit/test_writer_error_handling.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_error_handling.py
git commit -m "fix: truncate partial zstd frames on ENOSPC to prevent file corruption"
```

---

### Task 4: Fix host_reboot classification in runtime session-change path

**Files:**
- Modify: `src/writer/consumer.py:443-445`
- Test: `tests/unit/test_writer_restart_gap_recovery.py`

**Problem:** In `_check_session_change()` at line 443-445:
```python
classification = classify_restart_gap(
    previous_boot_id=self._current_boot_id,   # BUG: uses CURRENT
    current_boot_id=self._current_boot_id,     # Same value!
```
Both `previous_boot_id` and `current_boot_id` are set to `self._current_boot_id`, so `boot_id_changed` is always `False` in this path. The classifier falls through to Case 5 (collector unclean_exit) instead of Case 2 (host_reboot).

**Fix:** Use `self._previous_writer_state.host_boot_id` as `previous_boot_id` (matching the recovery path at line 294-297).

- [ ] **Step 1: Write failing test**

In `tests/unit/test_writer_restart_gap_recovery.py`, add to `TestRuntimeSessionDetectionPreserved`:

```python
    @pytest.mark.asyncio
    async def test_runtime_session_change_detects_boot_id_change(self):
        """When boot ID changed, runtime session-change path should classify
        as component=host, cause=host_reboot — not collector/unclean_exit."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        state_manager.load_component_state_by_instance = AsyncMock(return_value=None)
        state_manager.load_active_maintenance_intent = AsyncMock(return_value=None)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        # Previous writer ran with old boot ID
        consumer._current_boot_id = "boot-new"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-old"
        )

        # First message establishes session
        env1 = _make_data_envelope(collector_session_id="session-old")
        await consumer._check_session_change(env1)

        # Session change detected at runtime (after host reboot)
        env2 = _make_data_envelope(collector_session_id="session-new")
        result = await consumer._check_session_change(env2)

        assert result is not None
        assert result["reason"] == "restart_gap"
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is False
```

- [ ] **Step 2: Run test — expected FAIL**

Run: `uv run pytest tests/unit/test_writer_restart_gap_recovery.py::TestRuntimeSessionDetectionPreserved::test_runtime_session_change_detects_boot_id_change -v`
Expected: FAIL — `assert result["component"] == "host"` fails, gets "collector"

- [ ] **Step 3: Fix consumer.py line 443-445**

Change:
```python
        classification = classify_restart_gap(
            previous_boot_id=self._current_boot_id,
            current_boot_id=self._current_boot_id,
```
To:
```python
        previous_boot_id = (
            self._previous_writer_state.host_boot_id
            if self._previous_writer_state else self._current_boot_id
        )
        classification = classify_restart_gap(
            previous_boot_id=previous_boot_id,
            current_boot_id=self._current_boot_id,
```

- [ ] **Step 4: Run test — expected PASS**

Run: `uv run pytest tests/unit/test_writer_restart_gap_recovery.py::TestRuntimeSessionDetectionPreserved::test_runtime_session_change_detects_boot_id_change -v`
Expected: PASS

- [ ] **Step 5: Run all recovery tests to ensure no regressions**

Run: `uv run pytest tests/unit/test_writer_restart_gap_recovery.py tests/unit/test_restart_gap_classifier.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_restart_gap_recovery.py
git commit -m "fix: runtime session-change path now uses previous boot ID for host_reboot detection"
```

---

### Task 5: Full unit test suite verification

- [ ] **Step 1: Run all unit tests**

Run: `uv run pytest tests/unit/ -v --tb=short`
Expected: All 281+ tests PASS

- [ ] **Step 2: Final commit if any cleanup needed**
