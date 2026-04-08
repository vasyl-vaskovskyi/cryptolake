# Gap Filter via Backup Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop writing collector-emitted gap envelopes to the archive when the other collector already delivered data for the same window. Only persist a gap when neither collector had data.

**Architecture:** A new `CoverageFilter` in `src/writer/failover.py` tracks latest `received_at` per source (primary/backup) per `(exchange, symbol, stream)`. Incoming collector gap envelopes are either (a) dropped immediately if the other source already covers `gap_end_ts`, (b) parked in a pending queue keyed by `(source, stream_key, gap_start_ts)` that naturally coalesces stacked records, or (c) flushed after `grace_period_seconds` if still uncovered (true bilateral outage). Filter is bidirectional — works identically for primary and backup gap envelopes. Disabled by `grace_period_seconds: 0`.

**Tech Stack:** Python 3, pydantic config, prometheus_client metrics, pytest, confluent_kafka consumer.

**Spec:** `docs/superpowers/specs/2026-04-08-gap-filter-via-backup-coverage-design.md`

---

## File Structure

**Files to modify:**
- `src/common/config.py` — add `GapFilterConfig` submodel, wire into `WriterConfig`
- `config/config.test.yaml` — set `grace_period_seconds: 10` for tests
- `config/config.yaml` — default `grace_period_seconds: 10`
- `src/writer/metrics.py` — three new metrics
- `src/writer/failover.py` — new `CoverageFilter` class; `FailoverManager.activate()` uses `CoverageFilter.max_received()` for seek timestamps
- `src/writer/consumer.py` — instantiate `CoverageFilter`, route incoming envelopes through it (both primary and backup paths), skip silence-timer reset on gap envelopes, sweep expired pending gaps each iteration
- `src/writer/main.py` — pass `grace_period_seconds` from config into `WriterConsumer`
- `src/collector/connection.py` — coalesce `_emit_disconnect_gaps` (cleanup)
- `tests/unit/test_failover.py` — `TestCoverageFilter` suite
- `tests/unit/test_config.py` *(if exists)* or inline in test_failover.py — config parsing test
- `tests/chaos/10_snapshot_poll_miss.sh` — invert assertions

**No new files.** `CoverageFilter` lives next to `FailoverManager` because they collaborate on per-stream state.

---

## Task 1: Add `GapFilterConfig` to writer config

**Files:**
- Modify: `src/common/config.py`
- Modify: `config/config.test.yaml`
- Modify: `config/config.yaml`
- Test: `tests/unit/test_failover.py` (new `TestGapFilterConfig` class at bottom)

- [ ] **Step 1: Write the failing test**

Append to `tests/unit/test_failover.py`:

```python
from pathlib import Path
from src.common.config import load_config, CryptoLakeConfig


class TestGapFilterConfig:
    def test_default_grace_period_is_10s(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 10.0

    def test_grace_period_zero_is_allowed(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
writer:
  gap_filter:
    grace_period_seconds: 0
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 0.0

    def test_grace_period_custom_value(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
writer:
  gap_filter:
    grace_period_seconds: 25
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 25.0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_failover.py::TestGapFilterConfig -v
```

Expected: FAIL with `AttributeError: 'WriterConfig' object has no attribute 'gap_filter'`.

- [ ] **Step 3: Add GapFilterConfig to config.py**

In `src/common/config.py`, add this class above `WriterConfig` and add the field:

```python
class GapFilterConfig(BaseModel):
    grace_period_seconds: float = 10.0


class WriterConfig(BaseModel):
    base_dir: str = Field(default_factory=default_archive_dir)
    rotation: str = "hourly"
    compression: str = "zstd"
    compression_level: int = 3
    checksum: str = "sha256"
    flush_messages: int = 10000
    flush_interval_seconds: int = 30
    gap_filter: GapFilterConfig = Field(default_factory=GapFilterConfig)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/unit/test_failover.py::TestGapFilterConfig -v
```

Expected: 3 PASSED.

- [ ] **Step 5: Add to test and prod YAML files**

In `config/config.test.yaml`, modify the `writer:` block to add `gap_filter`:

```yaml
writer:
  rotation: "hourly"
  compression: "zstd"
  compression_level: 3
  checksum: "sha256"
  flush_messages: 100
  flush_interval_seconds: 10
  gap_filter:
    grace_period_seconds: 10
```

In `config/config.yaml`, find the `writer:` block (add if missing) and append the same `gap_filter` key with `grace_period_seconds: 10`.

- [ ] **Step 6: Commit**

```bash
git add src/common/config.py config/config.test.yaml config/config.yaml tests/unit/test_failover.py
git commit -m "feat(writer): add gap_filter config with grace_period_seconds"
```

---

## Task 2: Add new writer metrics

**Files:**
- Modify: `src/writer/metrics.py`

- [ ] **Step 1: Append three new metrics**

At the end of `src/writer/metrics.py`:

```python
# --- Gap coverage filter metrics ---
gap_envelopes_suppressed_total = Counter(
    "writer_gap_envelopes_suppressed_total",
    "Gap envelopes dropped because the other collector covered the window",
    ["source", "reason"],
)

gap_coalesced_total = Counter(
    "writer_gap_coalesced_total",
    "Gap envelopes merged into an existing pending entry (stacked reconnect gaps)",
    ["source"],
)

gap_pending_size = Gauge(
    "writer_gap_pending_size",
    "Current size of the coverage filter's pending gap queue",
)
```

- [ ] **Step 2: Sanity import check**

```bash
uv run python -c "from src.writer import metrics as m; print(m.gap_envelopes_suppressed_total, m.gap_coalesced_total, m.gap_pending_size)"
```

Expected: three prometheus_client object reprs, no errors.

- [ ] **Step 3: Commit**

```bash
git add src/writer/metrics.py
git commit -m "feat(writer): add coverage filter metrics"
```

---

## Task 3: `CoverageFilter` — init, coverage tracking, `handle_data` (no pending yet)

**Files:**
- Modify: `src/writer/failover.py`
- Test: `tests/unit/test_failover.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_failover.py`:

```python
from src.writer.failover import CoverageFilter


def _data_env(exchange="binance", symbol="btcusdt", stream="trades", received_at=1000, raw_text='{"a": 1}'):
    return {
        "type": "data",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": received_at,
        "raw_text": raw_text,
    }


class TestCoverageFilterInit:
    def test_enabled_when_grace_period_positive(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.enabled is True

    def test_disabled_when_grace_period_zero(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        assert cf.enabled is False

    def test_pending_starts_empty(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.pending_size == 0

    def test_last_received_starts_zero(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0

    def test_max_received_starts_zero(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.max_received(("binance", "btcusdt", "trades")) == 0


class TestCoverageFilterHandleData:
    def test_data_updates_last_received_for_source(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 5000

    def test_data_does_not_update_other_source(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("backup", ("binance", "btcusdt", "trades")) == 0

    def test_data_updates_max_received(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        cf.handle_data("backup", _data_env(received_at=7000))
        assert cf.max_received(("binance", "btcusdt", "trades")) == 7000

    def test_data_does_not_regress_last_received(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        cf.handle_data("primary", _data_env(received_at=3000))  # out-of-order
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 5000

    def test_data_ignored_when_disabled(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0

    def test_data_without_received_at_is_ignored(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        env = _data_env()
        del env["received_at"]
        cf.handle_data("primary", env)
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterInit tests/unit/test_failover.py::TestCoverageFilterHandleData -v
```

Expected: all FAIL with `ImportError` or `AttributeError`.

- [ ] **Step 3: Implement `CoverageFilter` skeleton in failover.py**

At the end of `src/writer/failover.py`, append:

```python
class CoverageFilter:
    """Tracks per-source data coverage and filters redundant gap envelopes.

    A gap belongs in the archive if and only if neither collector had data for
    that window. This filter drops gap envelopes whose window is already covered
    by data from the other source, and parks the rest briefly so backup data
    arriving late can still cover them.
    """

    def __init__(self, grace_period_seconds: float):
        self._grace_period = float(grace_period_seconds)
        # (exchange, symbol, stream) -> {"primary": received_at_ns, "backup": received_at_ns}
        self._last_received: dict[tuple[str, str, str], dict[str, int]] = {}
        # (source, stream_key, gap_start_ts) -> (envelope, first_seen_monotonic)
        self._pending: dict[tuple[str, tuple[str, str, str], int], tuple[dict, float]] = {}

    @property
    def enabled(self) -> bool:
        return self._grace_period > 0

    @property
    def pending_size(self) -> int:
        return len(self._pending)

    def last_received(self, source: str, stream_key: tuple[str, str, str]) -> int:
        return self._last_received.get(stream_key, {}).get(source, 0)

    def max_received(self, stream_key: tuple[str, str, str]) -> int:
        entry = self._last_received.get(stream_key, {})
        return max(entry.values(), default=0)

    def handle_data(self, source: str, envelope: dict) -> None:
        """Record a data envelope's arrival. Updates coverage and drops any
        newly-covered pending gaps (added in a later task)."""
        if not self.enabled:
            return
        received_at = envelope.get("received_at")
        if received_at is None:
            return
        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        coverage = self._last_received.setdefault(stream_key, {})
        if received_at > coverage.get(source, 0):
            coverage[source] = received_at
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterInit tests/unit/test_failover.py::TestCoverageFilterHandleData -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/failover.py tests/unit/test_failover.py
git commit -m "feat(writer): add CoverageFilter with per-source received_at tracking"
```

---

## Task 4: `CoverageFilter.handle_gap` — drop-if-covered path

**Files:**
- Modify: `src/writer/failover.py`
- Test: `tests/unit/test_failover.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_failover.py`:

```python
def _gap_env(exchange="binance", symbol="btcusdt", stream="trades",
             gap_start=1000, gap_end=2000, reason="ws_disconnect"):
    return {
        "type": "gap",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "gap_start_ts": gap_start,
        "gap_end_ts": gap_end,
        "reason": reason,
    }


class TestCoverageFilterHandleGap:
    def test_primary_gap_dropped_when_backup_covers(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("backup", _data_env(received_at=3000))
        handled = cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert handled is True
        assert cf.pending_size == 0

    def test_backup_gap_dropped_when_primary_covers(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=3000))
        handled = cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        assert handled is True
        assert cf.pending_size == 0

    def test_primary_gap_dropped_when_backup_equal_to_gap_end(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("backup", _data_env(received_at=2000))
        handled = cf.handle_gap("primary", _gap_env(gap_end=2000))
        assert handled is True

    def test_gap_handled_false_when_disabled(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        handled = cf.handle_gap("primary", _gap_env())
        assert handled is False

    def test_same_source_coverage_does_not_count(self):
        # primary data cannot cover primary's own gap — that would be circular
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=3000))
        handled = cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        # Parked (not dropped immediately) because other_source=backup has no data yet.
        assert handled is True
        assert cf.pending_size == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterHandleGap -v
```

Expected: FAIL with `AttributeError: 'CoverageFilter' object has no attribute 'handle_gap'`.

- [ ] **Step 3: Implement `handle_gap` with drop-if-covered + stub pending**

Add to `CoverageFilter` class in `src/writer/failover.py`:

```python
    def handle_gap(self, source: str, envelope: dict) -> bool:
        """Try to suppress or park a gap envelope.

        Returns True if the envelope was handled (dropped or parked) — caller
        must NOT write it. Returns False if the filter is disabled — caller
        should write as usual.
        """
        if not self.enabled:
            return False

        from src.writer import metrics as writer_metrics

        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        gap_start = envelope.get("gap_start_ts", 0)
        gap_end = envelope.get("gap_end_ts", 0)
        reason = envelope.get("reason", "unknown")

        other_source = "backup" if source == "primary" else "primary"
        other_received = self._last_received.get(stream_key, {}).get(other_source, 0)

        if other_received >= gap_end:
            writer_metrics.gap_envelopes_suppressed_total.labels(
                source=source, reason=reason,
            ).inc()
            return True

        # Park — full implementation in Task 5.
        self._pending[(source, stream_key, gap_start)] = (envelope, time.monotonic())
        return True
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterHandleGap -v
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/failover.py tests/unit/test_failover.py
git commit -m "feat(writer): CoverageFilter drops gap envelopes already covered by other source"
```

---

## Task 5: Pending queue with coalescing + data-envelope sweep

**Files:**
- Modify: `src/writer/failover.py`
- Test: `tests/unit/test_failover.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_failover.py`:

```python
class TestCoverageFilterPending:
    def test_gap_parked_when_not_yet_covered(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 1

    def test_stacked_gaps_coalesce_into_one_entry(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=3000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=4000))
        assert cf.pending_size == 1

    def test_coalesced_entry_has_latest_gap_end(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=5000))
        # Peek via internal state — acceptable in unit test
        key = ("primary", ("binance", "btcusdt", "trades"), 1000)
        assert cf._pending[key][0]["gap_end_ts"] == 5000

    def test_coalesce_does_not_reset_first_seen(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        key = ("primary", ("binance", "btcusdt", "trades"), 1000)
        original_first_seen = cf._pending[key][1]
        time.sleep(0.01)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=3000))
        assert cf._pending[key][1] == original_first_seen

    def test_different_gap_start_creates_separate_entries(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=5000, gap_end=6000))
        assert cf.pending_size == 2

    def test_gap_from_different_source_creates_separate_entry(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 2

    def test_data_sweep_drops_now_covered_pending_primary_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 1
        cf.handle_data("backup", _data_env(received_at=3000))
        assert cf.pending_size == 0

    def test_data_sweep_drops_now_covered_pending_backup_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_data("primary", _data_env(received_at=3000))
        assert cf.pending_size == 0

    def test_data_sweep_does_not_drop_uncovered_pending(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=5000))
        cf.handle_data("backup", _data_env(received_at=2000))  # not past gap_end
        assert cf.pending_size == 1

    def test_same_source_data_does_not_cover_own_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_data("primary", _data_env(received_at=3000))  # primary's own data
        assert cf.pending_size == 1

    def test_data_for_different_stream_does_not_drop(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(stream="trades", gap_end=2000))
        cf.handle_data("backup", _data_env(stream="depth", received_at=3000))
        assert cf.pending_size == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterPending -v
```

Expected: several FAIL — coalescing not yet implemented, data sweep not yet implemented.

- [ ] **Step 3: Implement coalescing in `handle_gap`**

Replace the stub "Park" block in `handle_gap` (at the bottom of the method) with:

```python
        # Park in pending queue, coalescing stacked records with the same gap_start.
        key = (source, stream_key, gap_start)
        existing = self._pending.get(key)
        if existing is not None:
            old_env, first_seen = existing
            if gap_end > old_env.get("gap_end_ts", 0):
                old_env["gap_end_ts"] = gap_end
                detail = envelope.get("detail")
                if detail:
                    old_env["detail"] = detail
            # Preserve first_seen so grace period counts from original arrival
            writer_metrics.gap_coalesced_total.labels(source=source).inc()
        else:
            self._pending[key] = (envelope, time.monotonic())

        writer_metrics.gap_pending_size.set(len(self._pending))
        return True
```

- [ ] **Step 4: Implement pending sweep in `handle_data`**

Replace the existing `handle_data` body in `CoverageFilter` with:

```python
    def handle_data(self, source: str, envelope: dict) -> None:
        """Record a data envelope's arrival and drop any newly-covered pending gaps."""
        if not self.enabled:
            return
        received_at = envelope.get("received_at")
        if received_at is None:
            return
        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        coverage = self._last_received.setdefault(stream_key, {})
        if received_at > coverage.get(source, 0):
            coverage[source] = received_at

        if not self._pending:
            return

        from src.writer import metrics as writer_metrics

        # Sweep pending gaps: drop any whose other-source coverage now reaches gap_end_ts
        to_remove: list[tuple[str, tuple[str, str, str], int]] = []
        for key, (gap_env, _first_seen) in self._pending.items():
            pending_source, pending_stream, _ = key
            if pending_stream != stream_key:
                continue
            other_source = "backup" if pending_source == "primary" else "primary"
            other_received = coverage.get(other_source, 0)
            if other_received >= gap_env.get("gap_end_ts", 0):
                to_remove.append(key)
                writer_metrics.gap_envelopes_suppressed_total.labels(
                    source=pending_source, reason=gap_env.get("reason", "unknown"),
                ).inc()

        for key in to_remove:
            del self._pending[key]

        if to_remove:
            writer_metrics.gap_pending_size.set(len(self._pending))
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterPending tests/unit/test_failover.py::TestCoverageFilterHandleData tests/unit/test_failover.py::TestCoverageFilterHandleGap -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/writer/failover.py tests/unit/test_failover.py
git commit -m "feat(writer): CoverageFilter pending queue with coalescing and data sweep"
```

---

## Task 6: `CoverageFilter.sweep_expired` — grace-period flush

**Files:**
- Modify: `src/writer/failover.py`
- Test: `tests/unit/test_failover.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/test_failover.py`:

```python
class TestCoverageFilterSweepExpired:
    def test_sweep_empty_when_no_pending(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.sweep_expired() == []

    def test_sweep_does_not_flush_within_grace_period(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env())
        assert cf.sweep_expired() == []
        assert cf.pending_size == 1

    def test_sweep_flushes_after_grace_period(self):
        cf = CoverageFilter(grace_period_seconds=0.01)  # 10ms
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        time.sleep(0.02)
        expired = cf.sweep_expired()
        assert len(expired) == 1
        assert expired[0]["gap_start_ts"] == 1000
        assert expired[0]["gap_end_ts"] == 2000
        assert cf.pending_size == 0

    def test_sweep_returns_empty_when_disabled(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        # Can't park while disabled, but should still return empty safely
        assert cf.sweep_expired() == []

    def test_sweep_partial_flush(self):
        cf = CoverageFilter(grace_period_seconds=0.05)  # 50ms
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        time.sleep(0.08)
        cf.handle_gap("primary", _gap_env(gap_start=5000, gap_end=6000))  # fresh
        expired = cf.sweep_expired()
        assert len(expired) == 1
        assert expired[0]["gap_start_ts"] == 1000
        assert cf.pending_size == 1  # the fresh one remains
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterSweepExpired -v
```

Expected: FAIL with `AttributeError: 'CoverageFilter' object has no attribute 'sweep_expired'`.

- [ ] **Step 3: Implement `sweep_expired`**

Add to `CoverageFilter` class:

```python
    def sweep_expired(self) -> list[dict]:
        """Return and remove pending gap envelopes whose grace period has elapsed.

        Caller must write these to the archive — they represent real bilateral outages.
        """
        if not self.enabled or not self._pending:
            return []
        now = time.monotonic()
        expired: list[dict] = []
        to_remove: list[tuple[str, tuple[str, str, str], int]] = []
        for key, (gap_env, first_seen) in self._pending.items():
            if (now - first_seen) >= self._grace_period:
                expired.append(gap_env)
                to_remove.append(key)
        for key in to_remove:
            del self._pending[key]
        if to_remove:
            from src.writer import metrics as writer_metrics
            writer_metrics.gap_pending_size.set(len(self._pending))
        return expired
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/unit/test_failover.py::TestCoverageFilterSweepExpired -v
```

Expected: all PASS.

- [ ] **Step 5: Run all CoverageFilter tests together**

```bash
uv run pytest tests/unit/test_failover.py -v -k "CoverageFilter"
```

Expected: every CoverageFilter test PASSES.

- [ ] **Step 6: Commit**

```bash
git add src/writer/failover.py tests/unit/test_failover.py
git commit -m "feat(writer): CoverageFilter grace-period flush for real bilateral outages"
```

---

## Task 7: Wire `CoverageFilter` into `WriterConsumer`

**Files:**
- Modify: `src/writer/consumer.py`
- Modify: `src/writer/main.py`
- Test: `tests/unit/test_consumer_failover_integration.py` (new test)

- [ ] **Step 1: Wire config → consumer constructor**

In `src/writer/main.py` line 61-70, pass the grace period into the consumer:

```python
        self.consumer = WriterConsumer(
            brokers=self.config.redpanda.brokers,
            topics=self.topics,
            group_id="cryptolake-writer",
            buffer_manager=self.buffer_manager,
            compressor=self.compressor,
            state_manager=self.state_manager,
            base_dir=self.config.writer.base_dir,
            host_evidence=host_evidence,
            gap_filter_grace_period_seconds=self.config.writer.gap_filter.grace_period_seconds,
        )
```

In `src/writer/consumer.py` `WriterConsumer.__init__` (lines 55-66), add the new parameter and instantiate `CoverageFilter`:

```python
    def __init__(
        self,
        brokers: list[str],
        topics: list[str],
        group_id: str,
        buffer_manager: BufferManager,
        compressor: ZstdFrameCompressor,
        state_manager: StateManager,
        base_dir: str,
        host_evidence: HostLifecycleEvidence | None = None,
        gap_filter_grace_period_seconds: float = 10.0,
    ):
```

And at line 101-106, right after the `FailoverManager` instantiation, add:

```python
        # Real-time failover manager
        self._failover = FailoverManager(
            brokers=brokers,
            primary_topics=topics,
            backup_prefix=os.environ.get("BACKUP_TOPIC_PREFIX", "backup."),
        )

        # Coverage filter — drops collector-emitted gap envelopes already covered
        # by data from the other source.
        from src.writer.failover import CoverageFilter
        self._coverage_filter = CoverageFilter(
            grace_period_seconds=gap_filter_grace_period_seconds,
        )
```

- [ ] **Step 2: Route primary envelopes through the filter**

In `src/writer/consumer.py`, replace lines 818-832 (the primary consume block inside the normal-path `if`) with:

```python
                envelope = self._deserialize_and_stamp(msg)
                if envelope is None:
                    continue

                if self._should_skip_recovery_dedup(envelope, msg):
                    continue

                self._count_consumed(envelope)

                env_type = envelope.get("type")
                if env_type == "gap":
                    if self._coverage_filter.handle_gap("primary", envelope):
                        # Suppressed or parked. Deliberately skip silence-timer
                        # reset: primary emitting gaps ≠ primary healthy, and we
                        # want failover to activate so backup data can flow.
                        # Do NOT use `continue` here — the post-if sweep/flush
                        # code below must still run this iteration.
                        pass
                    else:
                        # Filter disabled — fall through to normal write path
                        self._failover.track_record(envelope)
                        self._failover.reset_silence_timer()
                        await self._handle_rotation_and_buffer(envelope, active_hours)
                else:
                    self._coverage_filter.handle_data("primary", envelope)
                    self._failover.track_record(envelope)
                    self._failover.reset_silence_timer()
                    if env_type == "data":
                        await self._handle_gap_detection(envelope, msg)
                    await self._handle_rotation_and_buffer(envelope, active_hours)
```

- [ ] **Step 3: Route backup envelopes through the filter**

In `src/writer/consumer.py`, inside the failover path (lines 838-868), replace the backup-handling `if backup_msg is not None and not backup_msg.error():` block with:

```python
                    if backup_msg is not None and not backup_msg.error():
                        envelope = self._deserialize_backup_msg(backup_msg)
                        if envelope is not None and not self._failover.should_filter(envelope):
                            env_type = envelope.get("type")
                            backup_handled_by_filter = (
                                env_type == "gap"
                                and self._coverage_filter.handle_gap("backup", envelope)
                            )
                            if not backup_handled_by_filter:
                                if env_type != "gap":
                                    self._coverage_filter.handle_data("backup", envelope)

                                # Check for gap on first backup record per stream
                                if env_type == "data":
                                    stream_key = (envelope.get("exchange", ""),
                                                  envelope.get("symbol", ""),
                                                  envelope.get("stream", ""))
                                    nk = extract_natural_key(envelope)
                                    if stream_key not in self._failover._gap_checked and nk is not None:
                                        self._failover._gap_checked.add(stream_key)
                                        gap = self._failover.check_failover_gap(
                                            stream_key=stream_key,
                                            first_backup_key=nk,
                                            first_backup_received_at=envelope.get("received_at", 0),
                                        )
                                        if gap is not None:
                                            gap = add_broker_coordinates(
                                                gap, topic=backup_msg.topic(),
                                                partition=backup_msg.partition(), offset=-1)
                                            gap_results = self.buffer_manager.add(gap)
                                            if gap_results:
                                                await self._write_and_save(gap_results)

                                self._count_consumed(envelope)
                                self._failover.track_record(envelope)
                                writer_metrics.failover_records_total.inc()
                                await self._handle_rotation_and_buffer(envelope, active_hours)
```

No `continue` — the primary probe below must still run this iteration.

- [ ] **Step 4: Sweep expired pending gaps each loop iteration**

In `src/writer/consumer.py`, at the bottom of `consume_loop` (just before `if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:`), add:

```python
            # Sweep coverage-filter pending gaps whose grace period expired.
            # These are real bilateral outages — write them as usual.
            expired_gaps = self._coverage_filter.sweep_expired()
            for gap_env in expired_gaps:
                flush_results = self.buffer_manager.add(gap_env)
                if flush_results:
                    await self._write_and_save(flush_results)
```

- [ ] **Step 5: Run the full writer unit-test suite to check nothing regressed**

```bash
uv run pytest tests/unit/test_failover.py tests/unit/test_consumer_failover_integration.py tests/unit/test_emit_gap.py tests/unit/test_gap_records_written_metric.py -v
```

Expected: all PASS. If anything else broke, investigate before committing.

The silence-timer-on-gap behavior is verified end-to-end by chaos test 10 in Task 10 — that test requires the filter to activate failover during the blackout, which only happens if the gap envelopes from primary do not reset the silence timer.

- [ ] **Step 6: Commit**

```bash
git add src/writer/consumer.py src/writer/main.py
git commit -m "feat(writer): route envelopes through CoverageFilter, skip silence-timer reset on gap envelopes"
```

---

## Task 8: Update `FailoverManager.activate()` to use `max_received` for seek

**Files:**
- Modify: `src/writer/failover.py`
- Test: `tests/unit/test_failover.py`

**Why:** The spec says `FailoverManager._last_received` is the only caller that needs a single value, used to compute backup-consumer seek timestamps. We replaced the single value with per-source tracking in CoverageFilter. `FailoverManager` still has its own `_last_received` populated by `track_record`. Instead of duplicating state, `FailoverManager.activate()` should consult the CoverageFilter for the seek baseline, so the seek uses the most recent data seen from either side.

- [ ] **Step 1: Give `FailoverManager` a reference to `CoverageFilter`**

In `src/writer/failover.py`, change `FailoverManager.__init__` signature to accept an optional coverage filter:

```python
    def __init__(
        self,
        brokers: list[str],
        primary_topics: list[str],
        backup_prefix: str = "backup.",
        silence_timeout: float = 5.0,
        coverage_filter: "CoverageFilter | None" = None,
    ):
        self._brokers = brokers
        self._primary_topics = primary_topics
        self._backup_prefix = backup_prefix
        self._backup_topics = [f"{backup_prefix}{t}" for t in primary_topics]
        self._silence_timeout = silence_timeout
        self._coverage_filter = coverage_filter

        self._last_key: dict[tuple[str, str, str], int] = {}
        self._last_received: dict[tuple[str, str, str], int] = {}
        # ... rest unchanged
```

- [ ] **Step 2: Use `CoverageFilter.max_received` when computing seek**

In `src/writer/failover.py` `FailoverManager.activate()` method, find the loop that computes `seek_ts_ns` (around lines 150-158):

```python
                seek_ts_ns: int | None = None
                for stream_key, received_ns in self._last_received.items():
                    exchange, symbol, stream = stream_key
                    if f"{exchange}.{stream}" == primary_topic:
                        ts = received_ns - 10_000_000_000
                        if seek_ts_ns is None or ts < seek_ts_ns:
                            seek_ts_ns = ts
```

Replace with:

```python
                seek_ts_ns: int | None = None
                for stream_key in list(self._last_received.keys()):
                    exchange, symbol, stream = stream_key
                    if f"{exchange}.{stream}" != primary_topic:
                        continue
                    if self._coverage_filter is not None:
                        received_ns = self._coverage_filter.max_received(stream_key)
                    else:
                        received_ns = self._last_received.get(stream_key, 0)
                    if received_ns <= 0:
                        continue
                    ts = received_ns - 10_000_000_000
                    if seek_ts_ns is None or ts < seek_ts_ns:
                        seek_ts_ns = ts
```

- [ ] **Step 3: Wire coverage filter into FailoverManager in WriterConsumer**

In `src/writer/consumer.py`, update the order of instantiation so `CoverageFilter` is created before `FailoverManager`, then passed in:

```python
        # Coverage filter — drops collector-emitted gap envelopes already covered
        # by data from the other source.
        from src.writer.failover import CoverageFilter
        self._coverage_filter = CoverageFilter(
            grace_period_seconds=gap_filter_grace_period_seconds,
        )

        # Real-time failover manager
        self._failover = FailoverManager(
            brokers=brokers,
            primary_topics=topics,
            backup_prefix=os.environ.get("BACKUP_TOPIC_PREFIX", "backup."),
            coverage_filter=self._coverage_filter,
        )
```

- [ ] **Step 4: Write a regression test for seek with split coverage**

Append to `tests/unit/test_failover.py`:

```python
class TestFailoverManagerWithCoverageFilter:
    def test_seek_uses_max_across_sources(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=1_000_000_000_000_000_000))
        cf.handle_data("backup", _data_env(received_at=2_000_000_000_000_000_000))

        fm = FailoverManager(
            brokers=["localhost:9092"],
            primary_topics=["binance.trades"],
            coverage_filter=cf,
        )
        # Populate _last_received so activate() iterates the stream
        fm._last_received[("binance", "btcusdt", "trades")] = 1_000_000_000_000_000_000

        # We don't actually call activate() (would contact Kafka).
        # Instead verify CoverageFilter.max_received returns the max.
        assert cf.max_received(("binance", "btcusdt", "trades")) == 2_000_000_000_000_000_000
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/unit/test_failover.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/writer/failover.py src/writer/consumer.py tests/unit/test_failover.py
git commit -m "feat(writer): FailoverManager.activate uses CoverageFilter.max_received for seek"
```

---

## Task 9: Collector — coalesce `_emit_disconnect_gaps` (cleanup)

**Files:**
- Modify: `src/collector/connection.py`
- Test: `tests/unit/test_emit_gap.py` (or whatever existing test file covers connection.py — check first with `grep -l _emit_disconnect_gaps tests/unit/`)

- [ ] **Step 1: Find the existing test file**

```bash
grep -l "_emit_disconnect_gaps\|ws_disconnect" tests/unit/
```

If no test file exists that covers it, create a minimal new one: `tests/unit/test_disconnect_gap_coalescing.py`.

- [ ] **Step 2: Write the failing test**

Create `tests/unit/test_disconnect_gap_coalescing.py`:

```python
"""Tests for ws_disconnect gap coalescing on repeated reconnect attempts.

Relevant references:
- Class under test: src.collector.connection.WebSocketManager
- _PUBLIC_STREAMS = {"depth", "bookticker"} (src/exchanges/binance.py:15)
- _MARKET_STREAMS = {"trades", "funding_rate", "liquidations"} (src/exchanges/binance.py:16)
"""
from unittest.mock import MagicMock

from src.collector.connection import WebSocketManager


def _make_manager(enabled_streams):
    """Build a WebSocketManager with a mocked producer, skipping real WS setup."""
    producer = MagicMock()
    mgr = WebSocketManager.__new__(WebSocketManager)
    mgr.exchange = "binance"
    mgr.collector_session_id = "test_session"
    mgr.producer = producer
    mgr.symbols = ["btcusdt"]
    mgr.enabled_streams = list(enabled_streams)
    mgr.handlers = {}
    mgr._seq_counters = {}
    mgr._last_received_at = {}
    mgr._disconnect_gap_emitted = set()
    return mgr, producer


def test_first_disconnect_emits_one_gap_per_public_stream():
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # 1 symbol × 2 public streams (depth + bookticker)
    assert len(ws_calls) == 2


def test_repeated_disconnect_calls_coalesce():
    """Three consecutive _emit_disconnect_gaps calls (simulating 3 reconnect
    retries) should produce the SAME gaps as one call, not 3× more."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    mgr._emit_disconnect_gaps("public")
    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # Still only 2 (one per public stream), NOT 6.
    assert len(ws_calls) == 2


def test_data_arrival_clears_emitted_flag():
    """After data resumes on a stream, a subsequent disconnect should emit again."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    producer.emit_gap.reset_mock()

    # Simulate data arrival clearing the flag (happens in _receive_loop)
    mgr._disconnect_gap_emitted.discard(("btcusdt", "depth"))
    mgr._disconnect_gap_emitted.discard(("btcusdt", "bookticker"))

    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    assert len(ws_calls) == 2  # both streams re-emitted
```

- [ ] **Step 3: Run test to verify it fails**

```bash
uv run pytest tests/unit/test_disconnect_gap_coalescing.py -v
```

Expected: FAIL — current code emits 3× per stream.

- [ ] **Step 4: Implement coalescing**

In `src/collector/connection.py`, find the `WSConnection.__init__` method and add a new instance attribute (near other state initialization):

```python
        # Tracks (symbol, stream) pairs with a pending disconnect gap that
        # has already been emitted. Cleared on successful reconnect/data.
        self._disconnect_gap_emitted: set[tuple[str, str]] = set()
```

In `_emit_disconnect_gaps` (around lines 277-312), add a skip check at the top of the inner loop and mark the pair as emitted after emission. Replace the `for symbol in self.symbols:` block (lines 292-312) with:

```python
        for symbol in self.symbols:
            for stream in affected:
                if stream not in self.enabled_streams:
                    continue
                if (symbol, stream) in self._disconnect_gap_emitted:
                    continue  # already emitted on first disconnect for this outage
                gap_start = self._last_received_at.get((symbol, stream), now)
                seq = self._next_seq(symbol, stream)
                self.producer.emit_gap(
                    symbol=symbol, stream=stream,
                    session_seq=seq,
                    reason="ws_disconnect",
                    detail=f"WebSocket {socket_name} disconnected",
                    gap_start_ts=gap_start, gap_end_ts=now,
                )
                self._disconnect_gap_emitted.add((symbol, stream))
                # Advance the stream handler's seq tracker past the gap
                # envelope's seq so the next data message doesn't trigger
                # a spurious session_seq_skip.
                handler = self.handlers.get(stream)
                if handler is not None and hasattr(handler, "_seq_trackers"):
                    tracker = handler._seq_trackers.get(symbol)
                    if tracker is not None:
                        tracker._last_seq = seq
```

Also update the method docstring to reflect coalescing behavior:

```python
    def _emit_disconnect_gaps(self, socket_name: str) -> None:
        """Emit gap records for all symbol/stream combos on this socket.

        Only emits on the FIRST call after a disconnect. Subsequent calls
        during the reconnect-retry loop are coalesced (the flag is cleared
        in _receive_loop when data resumes). The writer's CoverageFilter
        still coalesces any leakage by gap_start_ts as a defense in depth.
        """
```

Find `_receive_loop` (around line 145) where incoming data is processed successfully. Just before `self._last_received_at[(symbol, stream_type)] = time.time_ns()` (around line 186), clear the emitted flag for that `(symbol, stream_type)`:

```python
            # Clear any pending disconnect-gap flag — data resumed
            self._disconnect_gap_emitted.discard((symbol, stream_type))

            self._last_received_at[(symbol, stream_type)] = time.time_ns()
            await handler.handle(symbol, raw_text, exchange_ts, seq)
```

- [ ] **Step 5: Run test to verify it passes**

```bash
uv run pytest tests/unit/test_disconnect_gap_coalescing.py -v
```

Expected: PASS.

- [ ] **Step 6: Run existing collector tests to check nothing broke**

```bash
uv run pytest tests/unit/test_emit_gap.py -v
```

Expected: all PASS (or clearly explainable failures — investigate before proceeding).

- [ ] **Step 7: Commit**

```bash
git add src/collector/connection.py tests/unit/test_disconnect_gap_coalescing.py
git commit -m "fix(collector): coalesce stacked ws_disconnect gaps across reconnect retries"
```

---

## Task 10: Chaos test 10 — invert assertions

**Files:**
- Modify: `tests/chaos/10_snapshot_poll_miss.sh`
- Modify: `tests/chaos/common.sh` (add helpers if needed)

- [ ] **Step 1: Add helpers to `common.sh`**

Append to `tests/chaos/common.sh`:

```bash
# Scrape a single collector metric value from the collector's Prometheus endpoint.
# Usage: get_collector_metric <container> <metric_line_prefix>  →  prints integer value or 0
get_collector_metric() {
    local container="${1:?Usage: get_collector_metric <container> <metric_prefix>}"
    local prefix="${2:?Usage: get_collector_metric <container> <metric_prefix>}"
    docker exec -i "${container}" python -c "
from urllib.request import urlopen
data = urlopen('http://localhost:8000/metrics', timeout=2).read().decode()
total = 0.0
for line in data.splitlines():
    if line.startswith('${prefix}') and not line.startswith('#'):
        try:
            total += float(line.split()[-1])
        except Exception:
            pass
print(int(total))
" 2>/dev/null || echo "0"
}

# Assert that a per-stream data record interval inside a time window is never
# larger than max_gap_seconds. Proves backup filled in the outage.
# Usage: assert_continuous_data <stream> <window_start_ns> <window_end_ns> <max_gap_seconds>
assert_continuous_data() {
    local stream="${1:?Usage: assert_continuous_data <stream> <start_ns> <end_ns> <max_gap_s>}"
    local start_ns="$2"
    local end_ns="$3"
    local max_gap_s="$4"
    uv run python -c "
import zstandard as zstd, orjson
from pathlib import Path
base = Path('${TEST_DATA_DIR}')
start_ns = ${start_ns}
end_ns = ${end_ns}
max_gap_ns = int(${max_gap_s}) * 1_000_000_000
stream = '${stream}'
errors = []
for f in sorted(base.rglob(f'*/{stream}/*.zst')):
    with open(f, 'rb') as fh:
        data = zstd.ZstdDecompressor().stream_reader(fh).read()
    prev_ts = None
    for line in data.strip().split(b'\n'):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get('type') != 'data':
            continue
        ts = env.get('received_at', 0)
        if ts < start_ns or ts > end_ns:
            continue
        if prev_ts is not None and (ts - prev_ts) > max_gap_ns:
            errors.append(f'{f.name}: {(ts - prev_ts)/1e9:.1f}s gap between {prev_ts} and {ts}')
        prev_ts = ts
if errors:
    for e in errors:
        print(f'ERROR: {e}')
    exit(1)
print(f'OK: {stream} continuous within window (max interval <= {${max_gap_s}}s)')
"
}
```

- [ ] **Step 2: Rewrite verification block in `10_snapshot_poll_miss.sh`**

Open `tests/chaos/10_snapshot_poll_miss.sh`. Replace everything from `--- Verification ---` through `print_test_report` (currently lines 39-111) with:

```bash
section "Verification"

assert_container_healthy "collector"
assert_container_healthy "writer"

# --- Chaos landed on primary ---
primary_reconnects=$(get_collector_metric "${COLLECTOR_CONTAINER}" "collector_ws_reconnects_total")
primary_ws_gaps=$(get_collector_metric "${COLLECTOR_CONTAINER}" 'collector_gaps_detected_total{exchange="binance",reason="ws_disconnect"')
assert_gt "primary collector saw reconnects (chaos landed)" "$primary_reconnects" 0
assert_gt "primary collector emitted ws_disconnect gaps internally" "$primary_ws_gaps" 0

# --- Backup was unaffected ---
backup_reconnects=$(get_collector_metric "${BACKUP_COLLECTOR_CONTAINER}" "collector_ws_reconnects_total")
assert_eq "backup collector had no reconnects (network untouched)" 0 "$backup_reconnects"

# --- Archive is clean — writer's CoverageFilter suppressed primary's gap envelopes ---
archive_ws_gaps=$(count_gaps "ws_disconnect")
archive_poll_miss_gaps=$(count_gaps "snapshot_poll_miss")
assert_eq "archive has 0 ws_disconnect gaps (backup covered)" 0 "$archive_ws_gaps"
assert_eq "archive has 0 snapshot_poll_miss gaps (backup covered)" 0 "$archive_poll_miss_gaps"

# --- Data is continuous through the blackout window — backup actually fed the writer ---
if assert_continuous_data "bookticker" "$event_start_ns" "$event_end_ns" 5; then
    pass "bookticker data continuous across blackout window (<=5s inter-record gaps)"
else
    fail "bookticker has data holes during blackout — backup did NOT cover"
fi

if assert_continuous_data "trades" "$event_start_ns" "$event_end_ns" 5; then
    pass "trades data continuous across blackout window"
else
    fail "trades has data holes during blackout"
fi

# --- Data integrity ---
if check_integrity; then
    pass "data integrity OK"
else
    fail "data integrity check failed"
fi

print_test_report
```

Also remove lines 50-92 (the `validate_poll_miss_gaps` inline function and its invocation) — they're no longer needed because we assert 0 poll_miss gaps exist.

- [ ] **Step 3: Update the test header comment**

At the top of `10_snapshot_poll_miss.sh`, replace:

```bash
echo "=== Chaos 10: Snapshot Poll Miss ==="
echo "Blocks collector HTTPS egress to trigger snapshot_poll_miss gaps"
echo "while WebSocket streams continue via existing connections."
```

with:

```bash
echo "=== Chaos 10: Primary Collector Isolation ==="
echo "Blocks primary collector HTTPS egress (including WebSocket). Backup"
echo "collector is unaffected. Asserts writer's CoverageFilter suppresses"
echo "primary's gap envelopes and backup data flows continuously through."
```

- [ ] **Step 4: Run chaos test 10**

```bash
scripts/run-all-tests.sh --chaos-test 10
```

Expected: PASS. All assertions green. Archive should show 0 collector gap records and continuous bookticker/trades data.

If FAIL: investigate which assertion failed. Common issues:
- `primary_ws_gaps == 0`: the primary collector may not have emitted any internal gap metric — check `collector_gaps_detected_total` label structure in `src/collector/metrics.py`.
- `archive_ws_gaps > 0`: filter is not suppressing — check Task 7 wiring, the `handle_gap` return value, and whether `continue` actually skips `_handle_rotation_and_buffer`.
- `assert_continuous_data` FAIL: backup is not reaching the writer — check failover activation, coverage-filter data handling.

- [ ] **Step 5: Commit**

```bash
git add tests/chaos/10_snapshot_poll_miss.sh tests/chaos/common.sh
git commit -m "test(chaos): invert test 10 to assert CoverageFilter suppresses primary gaps"
```

---

## Task 11: Final end-to-end verification

- [ ] **Step 1: Full chaos test suite**

```bash
scripts/run-all-tests.sh --chaos
```

Expected: all 17 chaos tests PASS. Watch especially for regressions in tests 1, 3, 7, 8, 17 — any test that restarts the collector or triggers failover.

- [ ] **Step 2: Full unit test suite**

```bash
uv run pytest tests/unit/ -v
```

Expected: all PASS.

- [ ] **Step 3: Spot-check metrics**

Run chaos test 10 once more and inspect the writer container's Prometheus metrics afterward (before teardown — tests teardown the stack, so you'll need to manually hold it up or add a pause before teardown). Look for:

```
writer_gap_envelopes_suppressed_total{source="primary",reason="ws_disconnect"} > 0
writer_gap_coalesced_total{source="primary"} > 0  # stacked records were coalesced
writer_gap_pending_size  # should be 0 after backup catches up
```

This isn't a hard assertion — it's observability confidence.

- [ ] **Step 4: Commit any final fixes and push**

If all tests pass, the implementation is complete. No final commit needed unless fixes were required.

---

## Rollback plan

If a regression is discovered in prod, set `writer.gap_filter.grace_period_seconds: 0` in `config/config.yaml` — this disables the filter entirely. No code changes required.
