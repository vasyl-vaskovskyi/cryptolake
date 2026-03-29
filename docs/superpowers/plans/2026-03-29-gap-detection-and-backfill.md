# Gap Detection, Completeness Monitoring & Historical Backfill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate silent data loss by fixing gap detection bugs, adding completeness monitoring, and building a historical backfill CLI.

**Architecture:** Three independent layers — (1) fix detection bugs in writer/collector, (2) add completeness metrics + alert + dashboard, (3) build gap analyzer + Binance historical backfill CLI. Each layer is testable and deployable on its own.

**Tech Stack:** Python 3, prometheus_client, click, aiohttp, zstandard, orjson, pytest

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/common/envelope.py` | Add `"checkpoint_lost"` to VALID_GAP_REASONS |
| `src/collector/producer.py` | Fix missing metric increment in `_emit_overflow_gap()` |
| `src/writer/metrics.py` | Add 3 new metrics: `gap_records_written_total`, `hours_sealed_today`, `hours_sealed_previous_day` |
| `src/writer/consumer.py` | Patch `_check_recovery_gap()` + increment `gap_records_written_total` on gap flush + hours sealed tracking |
| `src/writer/file_rotator.py` | Add `build_backfill_file_path()` helper |
| `src/exchanges/binance.py` | Add `build_historical_trades_url()`, `build_historical_funding_url()`, `build_historical_liquidations_url()`, `build_historical_open_interest_url()` |
| `src/cli/gaps.py` | New: `analyze` and `backfill` CLI commands |
| `src/cli/verify.py` | Extend `generate_manifest()` to include backfill files |
| `infra/prometheus/alert_rules.yml` | Add `IncompleteDay` alert |
| `infra/sampler/sampler.yml` | Add "Hours Sealed Today" widget |
| `tests/unit/test_envelope.py` | Test new gap reason |
| `tests/unit/test_overflow_gap_metric.py` | Test metric fix |
| `tests/unit/test_writer_restart_gap_recovery.py` | Add test for checkpoint-lost path |
| `tests/unit/test_gap_records_written_metric.py` | Test new writer metric |
| `tests/unit/test_hours_sealed_metrics.py` | Test hours sealed metrics |
| `tests/unit/test_gap_analyzer.py` | Test analyze command |
| `tests/unit/test_backfill.py` | Test backfill command |
| `tests/unit/test_binance_historical_urls.py` | Test URL builders |

---

### Task 1: Add `checkpoint_lost` to VALID_GAP_REASONS

**Files:**
- Modify: `src/common/envelope.py:9-21`
- Test: `tests/unit/test_envelope.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_envelope.py` (or add to existing):

```python
from src.common.envelope import create_gap_envelope, VALID_GAP_REASONS

def test_checkpoint_lost_is_valid_gap_reason():
    assert "checkpoint_lost" in VALID_GAP_REASONS

def test_create_gap_envelope_with_checkpoint_lost():
    env = create_gap_envelope(
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        collector_session_id="test-session",
        session_seq=-1,
        gap_start_ts=1000,
        gap_end_ts=2000,
        reason="checkpoint_lost",
        detail="No durable checkpoint; recovered gap bounds from archive",
    )
    assert env["reason"] == "checkpoint_lost"
    assert env["type"] == "gap"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_envelope.py -v -k checkpoint_lost`
Expected: FAIL with `ValueError: Invalid gap reason 'checkpoint_lost'`

- [ ] **Step 3: Add `checkpoint_lost` to VALID_GAP_REASONS**

In `src/common/envelope.py`, add `"checkpoint_lost"` to the `VALID_GAP_REASONS` frozenset (line 9-21):

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
        "checkpoint_lost",
    }
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_envelope.py -v -k checkpoint_lost`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/common/envelope.py tests/unit/test_envelope.py
git commit -m "feat: add checkpoint_lost to valid gap reasons"
```

---

### Task 2: Fix buffer overflow gap metric in collector

**Files:**
- Modify: `src/collector/producer.py:146-171`
- Test: `tests/unit/test_emit_gap.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_emit_gap.py`:

```python
def test_emit_overflow_gap_increments_metric(producer, mock_metrics):
    """_emit_overflow_gap must increment gaps_detected_total like emit_gap does."""
    producer._emit_overflow_gap(symbol="btcusdt", stream="trades", start_ts=1000)
    mock_metrics.gaps_detected_total.labels.assert_called_with(
        exchange=producer.exchange, symbol="btcusdt", stream="trades", reason="buffer_overflow",
    )
    mock_metrics.gaps_detected_total.labels().inc.assert_called_once()
```

Note: adapt fixtures to match the existing test file's patterns (check `tests/unit/test_emit_gap.py` for existing `producer` and `mock_metrics` fixtures).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_emit_gap.py -v -k overflow_gap_increments`
Expected: FAIL — `inc()` not called

- [ ] **Step 3: Add metric increment to `_emit_overflow_gap()`**

In `src/collector/producer.py`, add the metric call at line 147 (after method signature, before `gap = create_gap_envelope`):

```python
def _emit_overflow_gap(self, symbol: str, stream: str, start_ts: int) -> None:
    """Emit a buffer_overflow gap record when recovering from overflow."""
    collector_metrics.gaps_detected_total.labels(
        exchange=self.exchange, symbol=symbol, stream=stream, reason="buffer_overflow",
    ).inc()
    gap = create_gap_envelope(
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_emit_gap.py -v -k overflow_gap_increments`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/collector/producer.py tests/unit/test_emit_gap.py
git commit -m "fix: increment gaps_detected_total in _emit_overflow_gap"
```

---

### Task 3: Patch `_check_recovery_gap()` for missing checkpoint

**Files:**
- Modify: `src/writer/consumer.py:283-286`
- Test: `tests/unit/test_writer_restart_gap_recovery.py`

This is the critical bug fix. When `checkpoint is None` but archived files exist, emit a `checkpoint_lost` gap instead of silently returning.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_writer_restart_gap_recovery.py`:

```python
def test_no_checkpoint_with_existing_archive_emits_gap(tmp_path):
    """When PG has no checkpoint but archive files exist, emit checkpoint_lost gap."""
    consumer = _make_consumer(base_dir=str(tmp_path))
    # Simulate: no durable checkpoint loaded
    consumer._durable_checkpoints = {}
    consumer._recovery_done = set()

    # Create a fake archive file with one data envelope
    archive_dir = tmp_path / "binance" / "btcusdt" / "trades" / "2026-03-28"
    archive_dir.mkdir(parents=True)
    import zstandard, orjson
    cctx = zstandard.ZstdCompressor()
    last_envelope = {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1774712068000000000,
        "exchange_ts": 1774712067890, "collector_session_id": "old-session",
        "session_seq": 100, "raw_text": "{}", "raw_sha256": "abc",
        "_topic": "binance.trades", "_partition": 0, "_offset": 100,
    }
    compressed = cctx.compress(orjson.dumps(last_envelope) + b"\n")
    (archive_dir / "hour-15.jsonl.zst").write_bytes(compressed)

    # Incoming envelope from new session
    envelope = _make_data_envelope(
        exchange="binance", symbol="btcusdt", stream="trades",
        session_id="new-session", seq=0,
        received_at=1774722068000000000,
    )

    gap = consumer._check_recovery_gap(envelope)
    assert gap is not None
    assert gap["reason"] == "checkpoint_lost"
    assert gap["gap_start_ts"] == 1774712068000000000
    assert gap["gap_end_ts"] == 1774722068000000000


def test_no_checkpoint_no_archive_returns_none(tmp_path):
    """First-ever run: no checkpoint and no archive files = no gap."""
    consumer = _make_consumer(base_dir=str(tmp_path))
    consumer._durable_checkpoints = {}
    consumer._recovery_done = set()

    envelope = _make_data_envelope(
        exchange="binance", symbol="btcusdt", stream="trades",
        session_id="first-session", seq=0,
    )

    gap = consumer._check_recovery_gap(envelope)
    assert gap is None
```

Note: `_make_consumer` and `_make_data_envelope` are existing helpers in the test file. Add `base_dir` parameter support if not already present — check the existing `_make_consumer` helper.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_writer_restart_gap_recovery.py -v -k "no_checkpoint"`
Expected: FAIL — current code returns `None` for both cases

- [ ] **Step 3: Implement the fix**

In `src/writer/consumer.py`, replace lines 283-286:

```python
        # No durable checkpoint = first-ever run, no gap to detect
        checkpoint = self._durable_checkpoints.get(stream_key)
        if checkpoint is None:
            return None
```

With:

```python
        checkpoint = self._durable_checkpoints.get(stream_key)
        if checkpoint is None:
            # Check if archive files exist for this stream — if so, the
            # checkpoint was lost (e.g. writer killed before PG commit)
            exchange, symbol, stream = stream_key
            stream_dir = Path(self.base_dir) / exchange / symbol / stream
            if not stream_dir.exists() or not any(stream_dir.rglob("hour-*.jsonl.zst")):
                logger.info("first_run_no_checkpoint",
                            exchange=exchange, symbol=symbol, stream=stream)
                return None

            # Find the most recent hour file and read its last envelope
            hour_files = sorted(stream_dir.rglob("hour-*.jsonl.zst"))
            gap_start_ts = 0
            for hf in reversed(hour_files):
                try:
                    dctx = zstd.ZstdDecompressor()
                    with open(hf, "rb") as f:
                        data = dctx.stream_reader(f).read()
                    lines = [l for l in data.strip().split(b"\n") if l]
                    if lines:
                        last_env = orjson.loads(lines[-1])
                        gap_start_ts = last_env.get("received_at", 0)
                        break
                except Exception:
                    continue  # try the next file

            current_received_at = envelope.get("received_at", 0)
            current_session_id = envelope.get("collector_session_id", "")
            logger.warning("recovery_gap_no_checkpoint",
                           exchange=exchange, symbol=symbol, stream=stream,
                           gap_start_ts=gap_start_ts, gap_end_ts=current_received_at)

            writer_metrics.session_gaps_detected_total.labels(
                exchange=exchange, symbol=symbol, stream=stream,
            ).inc()
            self._recovery_gap_emitted.add(stream_key)

            return create_gap_envelope(
                exchange=exchange,
                symbol=symbol,
                stream=stream,
                collector_session_id=current_session_id,
                session_seq=-1,
                gap_start_ts=gap_start_ts,
                gap_end_ts=current_received_at,
                reason="checkpoint_lost",
                detail="No durable checkpoint; recovered gap bounds from archive",
                received_at=current_received_at,
            )
```

Add imports at the top of `consumer.py` if not already present:

```python
import zstandard as zstd
import orjson
from pathlib import Path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_writer_restart_gap_recovery.py -v -k "no_checkpoint"`
Expected: PASS

- [ ] **Step 5: Run full recovery gap test suite**

Run: `pytest tests/unit/test_writer_restart_gap_recovery.py -v`
Expected: All existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py tests/unit/test_writer_restart_gap_recovery.py
git commit -m "fix: emit checkpoint_lost gap when PG checkpoint missing but archive exists"
```

---

### Task 4: Add `writer_gap_records_written_total` metric

**Files:**
- Modify: `src/writer/metrics.py`
- Modify: `src/writer/consumer.py` (in `_write_to_disk` and consume loop)
- Test: `tests/unit/test_gap_records_written_metric.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_gap_records_written_metric.py`:

```python
from unittest.mock import patch
from src.writer import metrics as writer_metrics

def test_gap_records_written_metric_exists():
    assert hasattr(writer_metrics, "gap_records_written_total")

def test_gap_records_written_has_correct_labels():
    label_names = writer_metrics.gap_records_written_total._labelnames
    assert set(label_names) == {"exchange", "symbol", "stream", "reason"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_gap_records_written_metric.py -v`
Expected: FAIL — `AttributeError: module has no attribute 'gap_records_written_total'`

- [ ] **Step 3: Add the metric to `src/writer/metrics.py`**

Add after the existing `session_gaps_detected_total` definition:

```python
gap_records_written_total = Counter(
    "writer_gap_records_written_total",
    "Gap envelopes persisted to archive",
    ["exchange", "symbol", "stream", "reason"],
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_gap_records_written_metric.py -v`
Expected: PASS

- [ ] **Step 5: Increment the metric when gap envelopes are written**

In `src/writer/consumer.py`, in the consume loop where gap envelopes are written to disk (around lines 632-658), add metric increments. The gap envelopes go through `buffer_manager.add()` then `_write_and_save()`. The cleanest place is in `_write_to_disk()` — add counting for gap-type lines inside each `result.lines`:

In `_write_to_disk()` method, after the successful write (after line 872, before `states.append`), add:

```python
            # Count gap envelopes written to disk
            for line in result.lines:
                try:
                    env = orjson.loads(line)
                    if env.get("type") == "gap":
                        writer_metrics.gap_records_written_total.labels(
                            exchange=result.target.exchange,
                            symbol=result.target.symbol,
                            stream=result.target.stream,
                            reason=env.get("reason", "unknown"),
                        ).inc()
                except Exception:
                    pass
```

- [ ] **Step 6: Run all writer tests**

Run: `pytest tests/unit/test_writer*.py -v`
Expected: All pass

- [ ] **Step 7: Commit**

```bash
git add src/writer/metrics.py src/writer/consumer.py tests/unit/test_gap_records_written_metric.py
git commit -m "feat: add writer_gap_records_written_total metric for gap envelope persistence"
```

---

### Task 5: Add hours sealed metrics

**Files:**
- Modify: `src/writer/metrics.py`
- Modify: `src/writer/consumer.py` (sealing + midnight rotation logic)
- Test: `tests/unit/test_hours_sealed_metrics.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_hours_sealed_metrics.py`:

```python
from src.writer import metrics as writer_metrics

def test_hours_sealed_today_metric_exists():
    assert hasattr(writer_metrics, "hours_sealed_today")

def test_hours_sealed_previous_day_metric_exists():
    assert hasattr(writer_metrics, "hours_sealed_previous_day")

def test_hours_sealed_today_labels():
    assert set(writer_metrics.hours_sealed_today._labelnames) == {
        "exchange", "symbol", "stream",
    }

def test_hours_sealed_previous_day_labels():
    assert set(writer_metrics.hours_sealed_previous_day._labelnames) == {
        "exchange", "symbol", "stream",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_hours_sealed_metrics.py -v`
Expected: FAIL

- [ ] **Step 3: Add metrics to `src/writer/metrics.py`**

```python
hours_sealed_today = Gauge(
    "writer_hours_sealed_today",
    "Hours sealed so far for the current UTC date",
    ["exchange", "symbol", "stream"],
)

hours_sealed_previous_day = Gauge(
    "writer_hours_sealed_previous_day",
    "Hours sealed for the previous UTC date",
    ["exchange", "symbol", "stream"],
)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_hours_sealed_metrics.py -v`
Expected: PASS

- [ ] **Step 5: Increment `hours_sealed_today` when sealing files**

In `src/writer/consumer.py`, in the sealing block (around line 733, inside the `_rotate_file` method), after `write_sha256_sidecar(file_path, sc)` and `self._sealed_files.add(file_path)`:

```python
                    write_sha256_sidecar(file_path, sc)
                    self._sealed_files.add(file_path)
                    writer_metrics.files_rotated_total.labels(
                        exchange=exchange, symbol=symbol, stream=stream,
                    ).inc()
                    writer_metrics.hours_sealed_today.labels(
                        exchange=exchange, symbol=symbol, stream=stream,
                    ).inc()
                    logger.info("file_sealed", path=str(file_path))
```

- [ ] **Step 6: Track sealed hours internally and snapshot on date change**

Add an internal tracking dict to the `WriterConsumer.__init__()`:

```python
self._hours_sealed_count: dict[tuple[str, str, str], int] = {}  # (exchange, symbol, stream) -> count
```

Then in the sealing block (same place as Step 5), also increment the internal counter:

```python
                    key = (exchange, symbol, stream)
                    self._hours_sealed_count[key] = self._hours_sealed_count.get(key, 0) + 1
```

In the consume loop date-change detection (around line 673), when `current_date != prev_date`, snapshot the internal counter to `hours_sealed_previous_day` and reset:

```python
                if current_hour != prev_hour or current_date != prev_date:
                    if current_date != prev_date:
                        # Midnight rotation: snapshot today's sealed count to previous_day
                        ex, sym, st = file_key
                        key = (ex, sym, st)
                        today_val = self._hours_sealed_count.get(key, 0)
                        writer_metrics.hours_sealed_previous_day.labels(
                            exchange=ex, symbol=sym, stream=st,
                        ).set(today_val)
                        writer_metrics.hours_sealed_today.labels(
                            exchange=ex, symbol=sym, stream=st,
                        ).set(0)
                        self._hours_sealed_count[key] = 0
                    await self._rotate_file(file_key, prev_date, prev_hour)
```

- [ ] **Step 7: Run all writer tests**

Run: `pytest tests/unit/test_writer*.py -v`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add src/writer/metrics.py src/writer/consumer.py tests/unit/test_hours_sealed_metrics.py
git commit -m "feat: add hours_sealed_today and hours_sealed_previous_day metrics"
```

---

### Task 6: Add `IncompleteDay` alert and dashboard widget

**Files:**
- Modify: `infra/prometheus/alert_rules.yml`
- Modify: `infra/sampler/sampler.yml`

- [ ] **Step 1: Add the alert rule**

In `infra/prometheus/alert_rules.yml`, add after the existing `KafkaCommitFailing` alert (at the end of the rules list):

```yaml
      - alert: IncompleteDay
        expr: writer_hours_sealed_previous_day < 24
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "Incomplete day: {{ $labels.exchange }}/{{ $labels.symbol }}/{{ $labels.stream }}"
          description: "Only {{ $value }} of 24 hours archived for the previous UTC day."
```

- [ ] **Step 2: Add the dashboard widget**

In `infra/sampler/sampler.yml`, add after the "Write Errors /12h" sparkline (Tier 1 section), before the Tier 2 comment:

```yaml
  - title: Hours Sealed Today
    position: [[60, 32], [20, 8]]
    rate-ms: 60000
    sample: |
        curl -sg "http://localhost:9090/api/v1/query?query=min(writer_hours_sealed_today{exchange=~\"${EXCHANGE:-.*}\",symbol=~\"${SYMBOL:-.*}\"})" | jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'
```

- [ ] **Step 3: Commit**

```bash
git add infra/prometheus/alert_rules.yml infra/sampler/sampler.yml
git commit -m "feat: add IncompleteDay alert and Hours Sealed Today dashboard widget"
```

---

### Task 7: Add Binance historical URL builders

**Files:**
- Modify: `src/exchanges/binance.py`
- Test: `tests/unit/test_binance_historical_urls.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_binance_historical_urls.py`:

```python
from src.exchanges.binance import BinanceAdapter

def test_build_historical_trades_url():
    adapter = BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
        symbols=["btcusdt"],
    )
    url = adapter.build_historical_trades_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/aggTrades?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"

def test_build_historical_trades_url_with_from_id():
    adapter = BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
        symbols=["btcusdt"],
    )
    url = adapter.build_historical_trades_url("btcusdt", from_id=12345)
    assert "fromId=12345" in url
    assert "startTime" not in url

def test_build_historical_funding_url():
    adapter = BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
        symbols=["btcusdt"],
    )
    url = adapter.build_historical_funding_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/fundingRate?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"

def test_build_historical_liquidations_url():
    adapter = BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
        symbols=["btcusdt"],
    )
    url = adapter.build_historical_liquidations_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/allForceOrders?symbol=BTCUSDT&startTime=1000&endTime=2000&limit=1000"

def test_build_historical_open_interest_url():
    adapter = BinanceAdapter(
        ws_base="wss://fstream.binance.com",
        rest_base="https://fapi.binance.com",
        symbols=["btcusdt"],
    )
    url = adapter.build_historical_open_interest_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/futures/data/openInterestHist?symbol=BTCUSDT&period=5m&startTime=1000&endTime=2000&limit=500"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_binance_historical_urls.py -v`
Expected: FAIL — methods don't exist

- [ ] **Step 3: Implement the URL builders**

In `src/exchanges/binance.py`, add after the existing `build_open_interest_url()` method (line 89):

```python
    def build_historical_trades_url(
        self, symbol: str, *,
        start_time: int | None = None,
        end_time: int | None = None,
        from_id: int | None = None,
        limit: int = 1000,
    ) -> str:
        base = f"{self.rest_base}/fapi/v1/aggTrades?symbol={symbol.upper()}"
        if from_id is not None:
            return f"{base}&fromId={from_id}&limit={limit}"
        return f"{base}&startTime={start_time}&endTime={end_time}&limit={limit}"

    def build_historical_funding_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/fundingRate"
            f"?symbol={symbol.upper()}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_historical_liquidations_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/allForceOrders"
            f"?symbol={symbol.upper()}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_historical_open_interest_url(
        self, symbol: str, *, start_time: int, end_time: int, limit: int = 500,
    ) -> str:
        return (
            f"{self.rest_base}/futures/data/openInterestHist"
            f"?symbol={symbol.upper()}&period=5m&startTime={start_time}&endTime={end_time}&limit={limit}"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_binance_historical_urls.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/exchanges/binance.py tests/unit/test_binance_historical_urls.py
git commit -m "feat: add Binance historical REST API URL builders for backfill"
```

---

### Task 8: Add `build_backfill_file_path()` to file_rotator

**Files:**
- Modify: `src/writer/file_rotator.py`
- Modify: `tests/unit/test_file_rotator.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_file_rotator.py`:

```python
from src.writer.file_rotator import build_backfill_file_path

def test_build_backfill_file_path_basic():
    path = build_backfill_file_path("/data", "binance", "btcusdt", "trades", "2026-03-28", 16, 1)
    assert str(path) == "/data/binance/btcusdt/trades/2026-03-28/hour-16.backfill-1.jsonl.zst"

def test_build_backfill_file_path_increments():
    path = build_backfill_file_path("/data", "binance", "btcusdt", "trades", "2026-03-28", 16, 3)
    assert "backfill-3" in str(path)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_file_rotator.py -v -k backfill`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement**

In `src/writer/file_rotator.py`, add after `build_file_path()`:

```python
def build_backfill_file_path(
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    backfill_seq: int,
) -> Path:
    symbol = symbol.lower()
    name = f"hour-{hour}.backfill-{backfill_seq}.jsonl.zst"
    return Path(base_dir) / exchange / symbol / stream / date / name
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/unit/test_file_rotator.py -v`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/writer/file_rotator.py tests/unit/test_file_rotator.py
git commit -m "feat: add build_backfill_file_path helper"
```

---

### Task 9: Build gap analyzer CLI

**Files:**
- Create: `src/cli/gaps.py`
- Test: `tests/unit/test_gap_analyzer.py`

This is the largest task. The analyzer scans archive directories and produces a gap coverage report.

- [ ] **Step 1: Write the failing test for hour inventory**

Create `tests/unit/test_gap_analyzer.py`:

```python
import json
from pathlib import Path
import zstandard
import orjson
import pytest
from click.testing import CliRunner
from src.cli.gaps import cli

def _write_hour_file(base: Path, exchange: str, symbol: str, stream: str,
                     date: str, hour: int, envelopes: list[dict],
                     suffix: str = "") -> Path:
    """Write a zstd-compressed hour file with given envelopes."""
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    name = f"hour-{hour}{suffix}.jsonl.zst"
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / name).write_bytes(cctx.compress(data))
    return dir_path / name

def _make_data_env(exchange="binance", symbol="btcusdt", stream="trades",
                   received_at=1000, exchange_ts=999):
    return {
        "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
        "stream": stream, "received_at": received_at, "exchange_ts": exchange_ts,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": 0,
    }

def _make_gap_env(exchange="binance", symbol="btcusdt", stream="trades",
                  reason="restart_gap", gap_start_ts=1000, gap_end_ts=2000):
    return {
        "v": 1, "type": "gap", "exchange": exchange, "symbol": symbol,
        "stream": stream, "received_at": gap_end_ts,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": gap_start_ts, "gap_end_ts": gap_end_ts,
        "reason": reason, "detail": "test gap",
        "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": -1,
    }

def test_analyze_full_day_no_gaps(tmp_path):
    """A complete day with 24 hours should report 100% coverage."""
    for h in range(24):
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                        "2026-03-27", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-27"])
    assert result.exit_code == 0
    assert "24/24" in result.output

def test_analyze_missing_hours(tmp_path):
    """Missing hours 16 and 17 should be reported."""
    for h in range(24):
        if h in (16, 17):
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                        "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28"])
    assert result.exit_code == 0
    assert "MISSING" in result.output
    assert "22/24" in result.output

def test_analyze_backfill_files_count_as_covered(tmp_path):
    """Hours with backfill files should count as covered."""
    for h in range(24):
        if h == 16:
            _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                            "2026-03-28", h, [_make_data_env()],
                            suffix=".backfill-1")
        else:
            _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                            "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28"])
    assert result.exit_code == 0
    assert "24/24" in result.output

def test_analyze_json_output(tmp_path):
    """--json flag should produce valid JSON."""
    for h in range(24):
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                        "2026-03-27", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path),
                                 "--json", "--date", "2026-03-27"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "binance" in data
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_gap_analyzer.py -v`
Expected: FAIL — module not found

- [ ] **Step 3: Implement `src/cli/gaps.py` — analyze command**

Create `src/cli/gaps.py`:

```python
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import click
import orjson
import zstandard as zstd

from src.common.config import default_archive_dir

BACKFILLABLE_STREAMS = frozenset({"trades", "funding_rate", "liquidations", "open_interest"})
NON_BACKFILLABLE_STREAMS = frozenset({"depth", "depth_snapshot", "bookticker"})

DEFAULT_ARCHIVE_DIR = default_archive_dir()


def _decompress_and_parse(file_path: Path) -> list[dict]:
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as f:
        data = dctx.stream_reader(f).read()
    return [orjson.loads(line) for line in data.strip().split(b"\n") if line]


def _scan_hours(date_dir: Path) -> dict[int, str]:
    """Scan a date directory and classify each hour 0-23.

    Returns dict mapping hour -> status: 'present', 'backfilled', 'late', 'missing'.
    """
    hours: dict[int, str] = {}

    for f in date_dir.glob("hour-*.jsonl.zst"):
        name = f.name
        # Parse hour number from filename
        hour_part = name.split(".")[0]  # "hour-16" or "hour-16"
        try:
            hour_num = int(hour_part.replace("hour-", ""))
        except ValueError:
            continue

        if ".backfill-" in name:
            if hours.get(hour_num) not in ("present", "late"):
                hours[hour_num] = "backfilled"
        elif ".late-" in name:
            if hours.get(hour_num) != "present":
                hours[hour_num] = "late"
        else:
            hours[hour_num] = "present"

    # Mark missing hours
    for h in range(24):
        if h not in hours:
            hours[h] = "missing"

    return hours


def _scan_gaps(date_dir: Path) -> list[dict]:
    """Read all zst files in a date directory and extract gap envelopes."""
    gaps: list[dict] = []
    for f in sorted(date_dir.glob("hour-*.jsonl.zst")):
        try:
            envs = _decompress_and_parse(f)
            for env in envs:
                if env.get("type") == "gap":
                    gaps.append(env)
        except Exception:
            continue
    return gaps


def analyze_archive(
    base_dir: Path,
    exchange_filter: str | None = None,
    symbol_filter: str | None = None,
    stream_filter: str | None = None,
    date_filter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Analyze archive for gaps and coverage. Returns structured report."""
    report: dict = {}

    for exchange_dir in sorted(base_dir.iterdir()):
        if not exchange_dir.is_dir():
            continue
        exchange = exchange_dir.name
        if exchange_filter and exchange != exchange_filter:
            continue

        report[exchange] = {}
        for symbol_dir in sorted(exchange_dir.iterdir()):
            if not symbol_dir.is_dir():
                continue
            symbol = symbol_dir.name
            if symbol_filter and symbol != symbol_filter:
                continue

            report[exchange][symbol] = {}
            for stream_dir in sorted(symbol_dir.iterdir()):
                if not stream_dir.is_dir():
                    continue
                stream = stream_dir.name
                if stream_filter and stream != stream_filter:
                    continue

                report[exchange][symbol][stream] = {}
                for date_dir in sorted(stream_dir.iterdir()):
                    if not date_dir.is_dir():
                        continue
                    date = date_dir.name
                    if date_filter and date != date_filter:
                        continue
                    if date_from and date < date_from:
                        continue
                    if date_to and date > date_to:
                        continue

                    hours = _scan_hours(date_dir)
                    gaps = _scan_gaps(date_dir)

                    missing = [h for h, s in sorted(hours.items()) if s == "missing"]
                    covered = sum(1 for s in hours.values() if s != "missing")
                    backfillable = stream in BACKFILLABLE_STREAMS

                    report[exchange][symbol][stream][date] = {
                        "hours": hours,
                        "gaps": gaps,
                        "missing_hours": missing,
                        "covered": covered,
                        "expected": 24,
                        "backfillable": backfillable,
                    }

    return report


def _format_hours_line(hours: dict[int, str]) -> str:
    """Format hour statuses into a compact line like '0-15 ✓  16 MISSING  17 MISSING  18-23 ✓'."""
    parts: list[str] = []
    run_start: int | None = None
    run_status: str | None = None

    def _flush_run(start: int, end: int, status: str) -> None:
        if status == "present":
            label = f"{start}-{end} OK" if end > start else f"{start} OK"
        elif status == "backfilled":
            label = f"{start}-{end} BACKFILLED" if end > start else f"{start} BACKFILLED"
        elif status == "late":
            label = f"{start}-{end} LATE" if end > start else f"{start} LATE"
        else:
            label = f"{start}-{end} MISSING" if end > start else f"{start} MISSING"
        parts.append(label)

    for h in range(24):
        status = hours.get(h, "missing")
        if run_status is None:
            run_start = h
            run_status = status
        elif status != run_status:
            _flush_run(run_start, h - 1, run_status)
            run_start = h
            run_status = status
    if run_status is not None:
        _flush_run(run_start, 23, run_status)

    return "  ".join(parts)


def _print_report(report: dict) -> None:
    """Print human-readable report to stdout."""
    total_expected = 0
    total_covered = 0
    recoverable_gaps = 0
    unrecoverable_gaps = 0

    for exchange, symbols in report.items():
        for symbol, streams in symbols.items():
            click.echo(f"\n{exchange} / {symbol}")
            click.echo("=" * 60)

            for stream, dates in streams.items():
                for date, info in dates.items():
                    total_expected += info["expected"]
                    total_covered += info["covered"]

                    if info["missing_hours"]:
                        click.echo(f"\n  {stream} / {date}")
                        click.echo(f"  Hours: {_format_hours_line(info['hours'])}")
                        if info["gaps"]:
                            for g in info["gaps"]:
                                click.echo(f"  Gap: {g.get('reason')} ({g.get('detail', '')})")
                        if info["backfillable"]:
                            recoverable_gaps += len(info["missing_hours"])
                            click.echo(f"  Backfill: recoverable")
                        else:
                            unrecoverable_gaps += len(info["missing_hours"])
                            click.echo(f"  Backfill: unrecoverable (no historical API)")
                        click.echo(f"  Coverage: {info['covered']}/{info['expected']} ({info['covered']/info['expected']*100:.1f}%)")

    if total_expected > 0:
        click.echo(f"\nSummary:")
        click.echo(f"  Total coverage: {total_covered}/{total_expected} hours ({total_covered/total_expected*100:.1f}%)")
        if recoverable_gaps:
            click.echo(f"  Recoverable gaps: {recoverable_gaps} hours")
        if unrecoverable_gaps:
            click.echo(f"  Unrecoverable gaps: {unrecoverable_gaps} hours")
    else:
        click.echo("No archive data found.")


@click.group()
def cli():
    """CryptoLake gap analysis and backfill CLI."""


@cli.command()
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream")
@click.option("--date", default=None, help="Single date YYYY-MM-DD")
@click.option("--date-from", default=None, help="Start of date range")
@click.option("--date-to", default=None, help="End of date range")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.option("--base-dir", default=DEFAULT_ARCHIVE_DIR, help="Archive base directory")
def analyze(exchange, symbol, stream, date, date_from, date_to, as_json, base_dir):
    """Analyze archive for gaps and missing hours."""
    report = analyze_archive(
        Path(base_dir),
        exchange_filter=exchange,
        symbol_filter=symbol,
        stream_filter=stream,
        date_filter=date,
        date_from=date_from,
        date_to=date_to,
    )
    if as_json:
        # Serialize gaps properly (they contain non-string keys)
        click.echo(json.dumps(report, indent=2, default=str))
    else:
        _print_report(report)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_gap_analyzer.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_gap_analyzer.py
git commit -m "feat: add gap analyzer CLI with hour inventory and coverage reporting"
```

---

### Task 10: Build backfill runner CLI

**Files:**
- Modify: `src/cli/gaps.py`
- Test: `tests/unit/test_backfill.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_backfill.py`:

```python
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
import zstandard
import orjson
import pytest
from click.testing import CliRunner
from src.cli.gaps import cli

def _write_hour_file(base, exchange, symbol, stream, date, hour, envelopes):
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_data_env():
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1000, "exchange_ts": 999,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": "binance.trades", "_partition": 0, "_offset": 0,
    }

def test_backfill_dry_run_shows_plan(tmp_path):
    """Dry run should show what would be backfilled without writing."""
    for h in range(24):
        if h == 16:
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                        "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28", "--dry-run"])
    assert result.exit_code == 0
    assert "hour 16" in result.output.lower() or "16" in result.output

def test_backfill_skips_non_backfillable_streams(tmp_path):
    """Depth, depth_snapshot, bookticker should be skipped."""
    for h in range(24):
        if h == 16:
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "depth",
                        "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--stream", "depth", "--dry-run"])
    assert result.exit_code == 0
    assert "unrecoverable" in result.output.lower() or "skip" in result.output.lower()

def test_backfill_skips_already_backfilled(tmp_path):
    """Hours with existing backfill files should be skipped."""
    for h in range(24):
        if h == 16:
            # Write a backfill file for hour 16
            dir_path = tmp_path / "binance" / "btcusdt" / "trades" / "2026-03-28"
            dir_path.mkdir(parents=True, exist_ok=True)
            cctx = zstandard.ZstdCompressor()
            data = orjson.dumps(_make_data_env())
            (dir_path / "hour-16.backfill-1.jsonl.zst").write_bytes(cctx.compress(data))
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades",
                        "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28", "--dry-run"])
    assert result.exit_code == 0
    # Should report 0 gaps to fill (already backfilled)
    assert "0" in result.output or "nothing" in result.output.lower() or "skip" in result.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_backfill.py -v`
Expected: FAIL — `backfill` command not defined

- [ ] **Step 3: Implement the backfill command**

Add to `src/cli/gaps.py`, after the `analyze` command:

```python
import asyncio
import hashlib
import time

import aiohttp

from src.exchanges.binance import BinanceAdapter
from src.writer.file_rotator import build_backfill_file_path, compute_sha256, sidecar_path


STREAM_TO_ENDPOINT = {
    "trades": "historical_trades",
    "funding_rate": "historical_funding",
    "liquidations": "historical_liquidations",
    "open_interest": "historical_open_interest",
}

# Binance REST API weights per request
ENDPOINT_WEIGHTS = {
    "trades": 20,
    "funding_rate": 1,
    "liquidations": 21,
    "open_interest": 1,
}


def _hour_to_ms_range(date: str, hour: int) -> tuple[int, int]:
    """Convert a date + hour to start/end millisecond timestamps."""
    from datetime import datetime, timezone, timedelta
    dt_start = datetime.strptime(f"{date} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc)
    dt_end = dt_start + timedelta(hours=1) - timedelta(milliseconds=1)
    return int(dt_start.timestamp() * 1000), int(dt_end.timestamp() * 1000)


def _wrap_backfill_envelope(
    raw_record: dict,
    *,
    exchange: str,
    symbol: str,
    stream: str,
    session_id: str,
    seq: int,
    exchange_ts_key: str,
) -> dict:
    """Wrap a raw Binance REST response record into a standard envelope."""
    raw_text = orjson.dumps(raw_record).decode()
    exchange_ts = raw_record.get(exchange_ts_key, 0)
    return {
        "v": 1,
        "type": "data",
        "source": "backfill",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": time.time_ns(),
        "exchange_ts": exchange_ts,
        "collector_session_id": session_id,
        "session_seq": seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "backfill",
        "_partition": 0,
        "_offset": seq,
    }


# Map stream -> exchange_ts field name in Binance REST response
STREAM_TS_KEYS = {
    "trades": "T",
    "funding_rate": "fundingTime",
    "liquidations": "time",
    "open_interest": "timestamp",
}


async def _fetch_historical_data(
    session: aiohttp.ClientSession,
    adapter: BinanceAdapter,
    symbol: str,
    stream: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """Fetch all pages of historical data from Binance for a time window."""
    all_records: list[dict] = []
    current_start = start_ms

    while current_start < end_ms:
        if stream == "trades":
            url = adapter.build_historical_trades_url(
                symbol, start_time=current_start, end_time=end_ms)
        elif stream == "funding_rate":
            url = adapter.build_historical_funding_url(
                symbol, start_time=current_start, end_time=end_ms)
        elif stream == "liquidations":
            url = adapter.build_historical_liquidations_url(
                symbol, start_time=current_start, end_time=end_ms)
        elif stream == "open_interest":
            url = adapter.build_historical_open_interest_url(
                symbol, start_time=current_start, end_time=end_ms)
        else:
            break

        async with session.get(url) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                click.echo(f"  Rate limited, sleeping {retry_after}s...")
                await asyncio.sleep(retry_after)
                continue
            resp.raise_for_status()
            records = await resp.json()

        if not records:
            break

        all_records.extend(records)

        # Advance pagination
        ts_key = STREAM_TS_KEYS[stream]
        last_ts = records[-1].get(ts_key, 0)
        if stream == "trades":
            # For trades, use fromId pagination for exactness
            last_id = records[-1].get("a", 0)
            url = adapter.build_historical_trades_url(symbol, from_id=last_id + 1)
            # Re-fetch from the next ID
            current_start = end_ms  # break outer loop
            async with session.get(url) as resp2:
                if resp2.status == 200:
                    more = await resp2.json()
                    # Filter to within our time window
                    more = [r for r in more if r.get("T", 0) <= end_ms]
                    if more:
                        all_records.extend(more)
                        # Continue paginating...
                        while len(more) == 1000:
                            last_id = more[-1].get("a", 0)
                            url = adapter.build_historical_trades_url(
                                symbol, from_id=last_id + 1)
                            async with session.get(url) as resp3:
                                if resp3.status != 200:
                                    break
                                more = await resp3.json()
                                more = [r for r in more if r.get("T", 0) <= end_ms]
                                if more:
                                    all_records.extend(more)
                                else:
                                    break
            break
        else:
            if last_ts <= current_start:
                break  # no progress
            current_start = last_ts + 1

    return all_records


def _write_backfill_files(
    records: list[dict],
    *,
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    session_id: str,
) -> int:
    """Group records by hour, wrap in envelopes, write backfill files. Returns count written."""
    from collections import defaultdict
    by_hour: dict[int, list[dict]] = defaultdict(list)
    ts_key = STREAM_TS_KEYS[stream]

    for rec in records:
        ts_ms = rec.get(ts_key, 0)
        hour = (ts_ms // 3_600_000) % 24
        by_hour[hour].append(rec)

    written = 0
    for hour, hour_records in sorted(by_hour.items()):
        # Find next available backfill sequence number
        date_dir = Path(base_dir) / exchange / symbol.lower() / stream / date
        seq = 1
        while (date_dir / f"hour-{hour}.backfill-{seq}.jsonl.zst").exists():
            seq += 1

        file_path = build_backfill_file_path(base_dir, exchange, symbol, stream, date, hour, seq)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        envelopes = []
        for i, rec in enumerate(hour_records):
            env = _wrap_backfill_envelope(
                rec,
                exchange=exchange,
                symbol=symbol,
                stream=stream,
                session_id=session_id,
                seq=i,
                exchange_ts_key=ts_key,
            )
            envelopes.append(env)

        cctx = zstd.ZstdCompressor()
        data = b"\n".join(orjson.dumps(e) for e in envelopes)
        file_path.write_bytes(cctx.compress(data))

        # Write SHA256 sidecar
        sc = sidecar_path(file_path)
        digest = compute_sha256(file_path)
        sc.write_text(f"{digest}  {file_path.name}\n")

        written += len(envelopes)
        click.echo(f"  Wrote {len(envelopes)} records to {file_path.name}")

    return written


@cli.command()
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream (default: all backfillable)")
@click.option("--date", default=None, help="Single date YYYY-MM-DD")
@click.option("--date-from", default=None, help="Start of date range")
@click.option("--date-to", default=None, help="End of date range")
@click.option("--dry-run", is_flag=True, help="Show plan without writing")
@click.option("--base-dir", default=DEFAULT_ARCHIVE_DIR, help="Archive base directory")
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, base_dir):
    """Backfill missing hours from Binance historical API."""
    report = analyze_archive(
        Path(base_dir),
        exchange_filter=exchange,
        symbol_filter=symbol,
        stream_filter=stream,
        date_filter=date,
        date_from=date_from,
        date_to=date_to,
    )

    # Collect backfill tasks
    tasks: list[dict] = []
    for ex, symbols in report.items():
        for sym, streams in symbols.items():
            for st, dates in streams.items():
                if st not in BACKFILLABLE_STREAMS:
                    click.echo(f"Skipping {ex}/{sym}/{st}: unrecoverable (no historical API)")
                    continue
                for dt, info in dates.items():
                    for hour in info["missing_hours"]:
                        tasks.append({
                            "exchange": ex, "symbol": sym, "stream": st,
                            "date": dt, "hour": hour,
                        })

    if not tasks:
        click.echo("Nothing to backfill — all hours covered or skipped.")
        return

    click.echo(f"\nBackfill plan: {len(tasks)} missing hours")
    for t in tasks:
        click.echo(f"  {t['exchange']}/{t['symbol']}/{t['stream']}/{t['date']} hour {t['hour']}")

    if dry_run:
        click.echo("\nDry run — no files written.")
        return

    # Group tasks by (exchange, symbol, stream, date) to batch API calls
    from collections import defaultdict
    grouped: dict[tuple, list[int]] = defaultdict(list)
    for t in tasks:
        key = (t["exchange"], t["symbol"], t["stream"], t["date"])
        grouped[key].append(t["hour"])

    async def _run_backfill():
        # Use default Binance REST base
        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
            symbols=[],
        )
        session_id = f"backfill-{datetime.now(timezone.utc).isoformat()}"
        total_written = 0

        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for (ex, sym, st, dt), hours in grouped.items():
                click.echo(f"\nBackfilling {ex}/{sym}/{st}/{dt} hours {hours}...")
                # Compute time range spanning all missing hours
                all_starts = []
                all_ends = []
                for h in hours:
                    s, e = _hour_to_ms_range(dt, h)
                    all_starts.append(s)
                    all_ends.append(e)

                start_ms = min(all_starts)
                end_ms = max(all_ends)

                try:
                    records = await _fetch_historical_data(
                        session, adapter, sym, st, start_ms, end_ms)
                    if records:
                        written = _write_backfill_files(
                            records,
                            base_dir=base_dir,
                            exchange=ex,
                            symbol=sym,
                            stream=st,
                            date=dt,
                            session_id=session_id,
                        )
                        total_written += written
                    else:
                        click.echo(f"  No historical data returned for {st}")
                except Exception as e:
                    click.echo(f"  Error: {e}")

        click.echo(f"\nBackfill complete: {total_written} records written.")

    asyncio.run(_run_backfill())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_backfill.py -v`
Expected: All PASS

- [ ] **Step 5: Run analyzer tests too**

Run: `pytest tests/unit/test_gap_analyzer.py tests/unit/test_backfill.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_backfill.py
git commit -m "feat: add backfill CLI command with Binance historical API integration"
```

---

### Task 11: Extend verify CLI for backfill file awareness

**Files:**
- Modify: `src/cli/verify.py:182-233`
- Modify: `tests/unit/test_verify.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_verify.py`:

```python
def test_manifest_includes_backfill_hours(tmp_path):
    """generate_manifest should include backfill files in hour inventory."""
    # Create hour-15 (regular) and hour-16 (backfill)
    _make_archive(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", 15)
    # Create backfill file for hour 16
    date_dir = tmp_path / "binance" / "btcusdt" / "trades" / "2026-03-28"
    import zstandard, orjson
    cctx = zstandard.ZstdCompressor()
    env = {"v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
           "stream": "trades", "received_at": 1000, "exchange_ts": 999,
           "source": "backfill",
           "collector_session_id": "backfill-test", "session_seq": 0,
           "raw_text": "{}", "raw_sha256": hashlib.sha256(b"{}").hexdigest(),
           "_topic": "backfill", "_partition": 0, "_offset": 0}
    data = cctx.compress(orjson.dumps(env))
    (date_dir / "hour-16.backfill-1.jsonl.zst").write_bytes(data)

    from src.cli.verify import generate_manifest
    m = generate_manifest(tmp_path, "binance", "2026-03-28")
    hours = m["symbols"]["btcusdt"]["streams"]["trades"]["hours"]
    assert 15 in hours
    assert 16 in hours
```

Note: adapt to use the existing `_make_archive` helper in the test file.

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_verify.py -v -k backfill`
Expected: FAIL — hour 16 not in manifest (glob only matches `hour-*.jsonl.zst`, not `hour-*.backfill-*.jsonl.zst`)

- [ ] **Step 3: Fix the glob in `generate_manifest()`**

In `src/cli/verify.py`, line 203, the glob pattern `hour-*.jsonl.zst` does not match backfill files. Change:

```python
            for f in sorted(date_dir.glob("hour-*.jsonl.zst")):
```

to:

```python
            for f in sorted(date_dir.glob("hour-*jsonl.zst")):
```

Wait — that's too broad. Instead, add a second glob for backfill files. Replace the single glob loop (lines 203-231) with:

```python
            all_files = sorted(
                list(date_dir.glob("hour-*.jsonl.zst"))
                + list(date_dir.glob("hour-*.backfill-*.jsonl.zst"))
                + list(date_dir.glob("hour-*.late-*.jsonl.zst"))
            )
            for f in all_files:
                hour_str = f.name.split(".")[0].replace("hour-", "")
                try:
                    hours.append(int(hour_str))
                except ValueError:
                    pass
```

- [ ] **Step 4: Also update the verify command file glob (line 259)**

The verify command on line 259 uses `rglob("*/hour-*.jsonl.zst")`. Update to also match backfill files:

```python
    files = sorted(
        list(base.rglob(f"*/{date}/hour-*.jsonl.zst"))
        + list(base.rglob(f"*/{date}/hour-*.backfill-*.jsonl.zst"))
    )
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/unit/test_verify.py -v`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/cli/verify.py tests/unit/test_verify.py
git commit -m "feat: extend verify CLI to recognize backfill files"
```

---

### Task 12: Final integration test — run analyzer against real archive

- [ ] **Step 1: Run the analyzer against the actual local archive**

```bash
cd /Users/vasyl.vaskovskyi/data/cryptolake
python -m src.cli.gaps analyze --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt
```

Verify it correctly identifies the missing hours 16-17 on 2026-03-28.

- [ ] **Step 2: Run with --json flag**

```bash
python -m src.cli.gaps analyze --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt --json | python -m json.tool
```

Verify valid JSON output.

- [ ] **Step 3: Run backfill dry run**

```bash
python -m src.cli.gaps backfill --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt --date 2026-03-28 --dry-run
```

Verify it lists the missing hours for backfillable streams.

- [ ] **Step 4: Run full test suite**

```bash
pytest tests/unit/ -v --tb=short
```

Verify all tests pass.

- [ ] **Step 5: Commit all remaining changes**

```bash
git add -A
git commit -m "feat: complete gap detection, monitoring, and backfill implementation"
```
