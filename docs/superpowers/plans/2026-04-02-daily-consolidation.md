# Daily Consolidation Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a service that joins hourly archive files into single daily compressed files per exchange/symbol/stream, with verification and cleanup.

**Architecture:** Hour-sequential streaming pipeline. For each symbol (sequential) and stream (sequential), process hours 00-23 one at a time: decompress, merge, sort by `exchange_ts`, stream-write to a zstd-compressed daily file. Synthesize gap envelopes for missing hours. Verify output before removing hourly files. Standalone Docker service running daily at 02:30 UTC, with a CLI companion for manual runs.

**Tech Stack:** Python 3.12, zstandard, orjson, structlog, prometheus_client, click (CLI)

**Spec:** `docs/superpowers/specs/2026-04-02-daily-consolidation-design.md`

---

### Task 1: Add `missing_hour` to valid gap reasons

**Files:**
- Modify: `src/common/envelope.py:9-22`
- Test: `tests/unit/test_consolidate.py` (create)

- [ ] **Step 1: Create test file with first test**

Create `tests/unit/test_consolidate.py`:

```python
from src.common.envelope import VALID_GAP_REASONS, create_gap_envelope


def test_missing_hour_is_valid_gap_reason():
    assert "missing_hour" in VALID_GAP_REASONS


def test_create_gap_envelope_with_missing_hour():
    env = create_gap_envelope(
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        collector_session_id="consolidation-2026-03-29T02:30:00Z",
        session_seq=-1,
        gap_start_ts=1711670400_000_000_000,
        gap_end_ts=1711673999_999_999_999,
        reason="missing_hour",
        detail="No data files found for hour 14; not recoverable via backfill",
    )
    assert env["type"] == "gap"
    assert env["reason"] == "missing_hour"
    assert env["gap_start_ts"] == 1711670400_000_000_000
    assert env["gap_end_ts"] == 1711673999_999_999_999
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_consolidate.py::test_missing_hour_is_valid_gap_reason -v`
Expected: FAIL — `"missing_hour"` not in `VALID_GAP_REASONS`

- [ ] **Step 3: Add `missing_hour` to valid reasons**

In `src/common/envelope.py`, add `"missing_hour"` to `VALID_GAP_REASONS`:

```python
VALID_GAP_REASONS = frozenset(
    {
        "ws_disconnect",
        "pu_chain_break",
        "session_seq_skip",
        "buffer_overflow",
        "snapshot_poll_miss",
        "collector_restart",
        "restart_gap",
        "write_error",
        "deserialization_error",
        "checkpoint_lost",
        "missing_hour",
    }
)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/common/envelope.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add missing_hour gap reason"
```

---

### Task 2: File discovery and classification

**Files:**
- Create: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests for file discovery**

Append to `tests/unit/test_consolidate.py`:

```python
import zstandard
import orjson
from pathlib import Path
from src.cli.consolidate import discover_hour_files


def _make_data_env(exchange_ts=1000):
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1000, "exchange_ts": exchange_ts,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": "binance.trades", "_partition": 0, "_offset": 0,
    }


def _write_zst_file(path: Path, envelopes: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    path.write_bytes(cctx.compress(data))


def test_discover_hour_files_groups_correctly(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()

    # base file for hour 0
    _write_zst_file(date_dir / "hour-0.jsonl.zst", [_make_data_env()])
    # base + late + backfill for hour 1
    _write_zst_file(date_dir / "hour-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.late-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.backfill-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.backfill-2.jsonl.zst", [_make_data_env()])
    # sidecar files should be ignored
    (date_dir / "hour-0.jsonl.zst.sha256").write_text("abc  hour-0.jsonl.zst\n")

    result = discover_hour_files(date_dir)

    assert 0 in result
    assert result[0]["base"] == date_dir / "hour-0.jsonl.zst"
    assert result[0]["late"] == []
    assert result[0]["backfill"] == []

    assert 1 in result
    assert result[1]["base"] == date_dir / "hour-1.jsonl.zst"
    assert result[1]["late"] == [date_dir / "hour-1.late-1.jsonl.zst"]
    assert len(result[1]["backfill"]) == 2

    # Hour 2-23 should not be in result
    assert 2 not in result


def test_discover_hour_files_empty_dir(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    result = discover_hour_files(date_dir)
    assert result == {}


def test_discover_hour_files_backfill_only(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    _write_zst_file(date_dir / "hour-5.backfill-1.jsonl.zst", [_make_data_env()])

    result = discover_hour_files(date_dir)
    assert 5 in result
    assert result[5]["base"] is None
    assert result[5]["backfill"] == [date_dir / "hour-5.backfill-1.jsonl.zst"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_discover_hour_files_groups_correctly -v`
Expected: FAIL — `ImportError: cannot import name 'discover_hour_files'`

- [ ] **Step 3: Implement `discover_hour_files`**

Create `src/cli/consolidate.py`:

```python
"""Daily consolidation: merge hourly archive files into single daily files."""
from __future__ import annotations

import re
from pathlib import Path

import structlog

logger = structlog.get_logger()

# Regex patterns for hour file classification
_RE_BASE = re.compile(r"^hour-(\d{1,2})\.jsonl\.zst$")
_RE_LATE = re.compile(r"^hour-(\d{1,2})\.late-(\d+)\.jsonl\.zst$")
_RE_BACKFILL = re.compile(r"^hour-(\d{1,2})\.backfill-(\d+)\.jsonl\.zst$")


def discover_hour_files(date_dir: Path) -> dict[int, dict]:
    """Scan a date directory and classify files by hour.

    Returns dict keyed by hour (0-23), each value is:
        {"base": Path | None, "late": [Path, ...], "backfill": [Path, ...]}
    Late and backfill lists are sorted by sequence number.
    """
    groups: dict[int, dict] = {}

    for f in sorted(date_dir.iterdir()):
        if not f.is_file() or not f.name.endswith(".jsonl.zst"):
            continue

        name = f.name
        m = _RE_BACKFILL.match(name)
        if m:
            hour = int(m.group(1))
            groups.setdefault(hour, {"base": None, "late": [], "backfill": []})
            groups[hour]["backfill"].append(f)
            continue

        m = _RE_LATE.match(name)
        if m:
            hour = int(m.group(1))
            groups.setdefault(hour, {"base": None, "late": [], "backfill": []})
            groups[hour]["late"].append(f)
            continue

        m = _RE_BASE.match(name)
        if m:
            hour = int(m.group(1))
            groups.setdefault(hour, {"base": None, "late": [], "backfill": []})
            groups[hour]["base"] = f
            continue

    # Sort late and backfill lists by sequence number
    for hour_data in groups.values():
        hour_data["late"].sort(key=lambda p: int(_RE_LATE.match(p.name).group(2)))
        hour_data["backfill"].sort(key=lambda p: int(_RE_BACKFILL.match(p.name).group(2)))

    return groups
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k discover`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add file discovery and classification"
```

---

### Task 3: Hour merging — decompress, merge, sort

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests for merge_hour**

Append to `tests/unit/test_consolidate.py`:

```python
from src.cli.consolidate import merge_hour


def test_merge_hour_base_only(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    envs = [_make_data_env(exchange_ts=300), _make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)]
    _write_zst_file(date_dir / "hour-5.jsonl.zst", envs)

    file_group = {"base": date_dir / "hour-5.jsonl.zst", "late": [], "backfill": []}
    records = merge_hour(5, file_group)

    assert len(records) == 3
    # Should be sorted by exchange_ts
    assert records[0]["exchange_ts"] == 100
    assert records[1]["exchange_ts"] == 200
    assert records[2]["exchange_ts"] == 300


def test_merge_hour_with_late_and_backfill(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()

    _write_zst_file(date_dir / "hour-5.jsonl.zst", [_make_data_env(exchange_ts=100)])
    _write_zst_file(date_dir / "hour-5.late-1.jsonl.zst", [_make_data_env(exchange_ts=50)])
    _write_zst_file(date_dir / "hour-5.backfill-1.jsonl.zst", [_make_data_env(exchange_ts=75)])

    file_group = {
        "base": date_dir / "hour-5.jsonl.zst",
        "late": [date_dir / "hour-5.late-1.jsonl.zst"],
        "backfill": [date_dir / "hour-5.backfill-1.jsonl.zst"],
    }
    records = merge_hour(5, file_group)

    assert len(records) == 3
    assert records[0]["exchange_ts"] == 50
    assert records[1]["exchange_ts"] == 75
    assert records[2]["exchange_ts"] == 100


def test_merge_hour_preserves_gap_envelopes(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()

    gap_env = {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 500,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 200_000_000_000, "gap_end_ts": 300_000_000_000,
        "reason": "ws_disconnect", "detail": "test gap",
    }
    data_env = _make_data_env(exchange_ts=100)
    _write_zst_file(date_dir / "hour-5.jsonl.zst", [data_env, gap_env])

    file_group = {"base": date_dir / "hour-5.jsonl.zst", "late": [], "backfill": []}
    records = merge_hour(5, file_group)

    assert len(records) == 2
    types = [r["type"] for r in records]
    assert "gap" in types
    assert "data" in types
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_merge_hour_base_only -v`
Expected: FAIL — `ImportError: cannot import name 'merge_hour'`

- [ ] **Step 3: Implement `merge_hour`**

Add to `src/cli/consolidate.py`:

```python
import orjson
import zstandard as zstd


def _decompress_and_parse(file_path: Path) -> list[dict]:
    """Decompress a .jsonl.zst file and parse all JSONL records."""
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as fh:
        data = dctx.stream_reader(fh).read()
    result = []
    for line in data.strip().split(b"\n"):
        if line:
            result.append(orjson.loads(line))
    return result


def _sort_key(record: dict) -> int:
    """Extract sort key from a record: exchange_ts for data, gap_start_ts for gaps."""
    if record.get("type") == "gap":
        return record["gap_start_ts"]
    return record["exchange_ts"]


def merge_hour(hour: int, file_group: dict) -> list[dict]:
    """Decompress all files for one hour, merge, and sort by exchange_ts.

    file_group: {"base": Path | None, "late": [Path, ...], "backfill": [Path, ...]}
    Returns sorted list of record dicts.
    """
    all_records: list[dict] = []

    # Read base file first
    if file_group["base"] is not None:
        all_records.extend(_decompress_and_parse(file_group["base"]))

    # Then late files (already sorted by seq)
    for path in file_group["late"]:
        all_records.extend(_decompress_and_parse(path))

    # Then backfill files (already sorted by seq)
    for path in file_group["backfill"]:
        all_records.extend(_decompress_and_parse(path))

    # Sort all records by exchange_ts (or gap_start_ts for gap envelopes)
    all_records.sort(key=_sort_key)

    return all_records
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k merge`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add hour merging with sort"
```

---

### Task 4: Gap envelope synthesis for missing hours

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from datetime import datetime, timezone
from src.cli.consolidate import synthesize_missing_hour_gap


def test_synthesize_missing_hour_gap_structure():
    env = synthesize_missing_hour_gap(
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
        hour=14,
        session_id="consolidation-2026-03-29T02:30:00Z",
    )
    assert env["v"] == 1
    assert env["type"] == "gap"
    assert env["exchange"] == "binance"
    assert env["symbol"] == "btcusdt"
    assert env["stream"] == "trades"
    assert env["reason"] == "missing_hour"
    assert env["session_seq"] == -1
    assert "hour 14" in env["detail"]

    # gap_start_ts = 2026-03-28 14:00:00 UTC in nanoseconds
    expected_start = int(datetime(2026, 3, 28, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    assert env["gap_start_ts"] == expected_start

    # gap_end_ts = 2026-03-28 14:59:59.999999999 UTC
    expected_end = int(datetime(2026, 3, 28, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000) - 1
    assert env["gap_end_ts"] == expected_end


def test_synthesize_missing_hour_gap_hour_boundaries():
    # Hour 0 of the day
    env = synthesize_missing_hour_gap(
        exchange="binance", symbol="btcusdt", stream="depth",
        date="2026-03-28", hour=0,
        session_id="consolidation-test",
    )
    expected_start = int(datetime(2026, 3, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    assert env["gap_start_ts"] == expected_start

    # Hour 23 of the day
    env = synthesize_missing_hour_gap(
        exchange="binance", symbol="btcusdt", stream="depth",
        date="2026-03-28", hour=23,
        session_id="consolidation-test",
    )
    expected_end = int(datetime(2026, 3, 29, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000) - 1
    assert env["gap_end_ts"] == expected_end
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_synthesize_missing_hour_gap_structure -v`
Expected: FAIL — `ImportError: cannot import name 'synthesize_missing_hour_gap'`

- [ ] **Step 3: Implement `synthesize_missing_hour_gap`**

Add to `src/cli/consolidate.py`:

```python
from datetime import datetime, timezone

from src.common.envelope import create_gap_envelope


def synthesize_missing_hour_gap(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    session_id: str,
) -> dict:
    """Create a gap envelope for a completely missing hour."""
    # Parse date and compute hour boundaries in nanoseconds
    year, month, day = (int(x) for x in date.split("-"))
    hour_start = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)
    hour_end_exclusive = datetime(
        year, month, day + (1 if hour == 23 else 0),
        (hour + 1) % 24, 0, 0,
        tzinfo=timezone.utc,
    ) if not (hour == 23 and day == 31) else datetime(
        year, month + 1, 1, 0, 0, 0, tzinfo=timezone.utc,
    )

    gap_start_ns = int(hour_start.timestamp() * 1_000_000_000)
    gap_end_ns = int(hour_end_exclusive.timestamp() * 1_000_000_000) - 1

    return create_gap_envelope(
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        collector_session_id=session_id,
        session_seq=-1,
        gap_start_ts=gap_start_ns,
        gap_end_ts=gap_end_ns,
        reason="missing_hour",
        detail=f"No data files found for hour {hour}; not recoverable via backfill",
    )
```

Wait — the `datetime` math for day/month overflow is fragile. Use `timedelta` instead:

```python
from datetime import datetime, timedelta, timezone

from src.common.envelope import create_gap_envelope


def synthesize_missing_hour_gap(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    session_id: str,
) -> dict:
    """Create a gap envelope for a completely missing hour."""
    year, month, day = (int(x) for x in date.split("-"))
    hour_start = datetime(year, month, day, hour, 0, 0, tzinfo=timezone.utc)
    hour_end_exclusive = hour_start + timedelta(hours=1)

    gap_start_ns = int(hour_start.timestamp() * 1_000_000_000)
    gap_end_ns = int(hour_end_exclusive.timestamp() * 1_000_000_000) - 1

    return create_gap_envelope(
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        collector_session_id=session_id,
        session_seq=-1,
        gap_start_ts=gap_start_ns,
        gap_end_ts=gap_end_ns,
        reason="missing_hour",
        detail=f"No data files found for hour {hour}; not recoverable via backfill",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k synthesize`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add missing hour gap synthesis"
```

---

### Task 5: Streaming daily file writer

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from src.cli.consolidate import write_daily_file


def test_write_daily_file_creates_compressed_output(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    records_by_hour = {
        0: [_make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)],
        1: [_make_data_env(exchange_ts=300)],
    }

    def hour_iterator():
        for h in range(24):
            if h in records_by_hour:
                yield h, records_by_hour[h]

    stats = write_daily_file(output_path, hour_iterator())

    assert output_path.exists()
    assert stats["total_records"] == 3
    assert stats["data_records"] == 3
    assert stats["gap_records"] == 0

    # Verify the file can be decompressed and read back
    dctx = zstandard.ZstdDecompressor()
    with open(output_path, "rb") as fh:
        data = dctx.stream_reader(fh).read()
    lines = [l for l in data.strip().split(b"\n") if l]
    assert len(lines) == 3
    first = orjson.loads(lines[0])
    assert first["exchange_ts"] == 100


def test_write_daily_file_with_gap_envelopes(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    gap_env = {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 500,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 200_000_000_000, "gap_end_ts": 300_000_000_000,
        "reason": "missing_hour", "detail": "test",
    }

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]
        yield 1, [gap_env]

    stats = write_daily_file(output_path, hour_iterator())
    assert stats["total_records"] == 2
    assert stats["data_records"] == 1
    assert stats["gap_records"] == 1
    assert stats["hours"][0]["data_records"] == 1
    assert stats["hours"][1]["data_records"] == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_write_daily_file_creates_compressed_output -v`
Expected: FAIL — `ImportError: cannot import name 'write_daily_file'`

- [ ] **Step 3: Implement `write_daily_file`**

Add to `src/cli/consolidate.py`:

```python
from typing import Iterator


def write_daily_file(
    output_path: Path,
    hour_records: Iterator[tuple[int, list[dict]]],
) -> dict:
    """Stream-write records to a zstd-compressed JSONL daily file.

    hour_records: iterator yielding (hour, records_list) tuples.
    Returns stats dict with record counts per hour and totals.
    """
    cctx = zstd.ZstdCompressor(level=3)
    stats = {
        "total_records": 0,
        "data_records": 0,
        "gap_records": 0,
        "hours": {},
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as fh:
        with cctx.stream_writer(fh) as writer:
            for hour, records in hour_records:
                hour_data = 0
                hour_gaps = 0
                for record in records:
                    line = orjson.dumps(record) + b"\n"
                    writer.write(line)
                    if record.get("type") == "gap":
                        hour_gaps += 1
                    else:
                        hour_data += 1
                stats["hours"][hour] = {
                    "data_records": hour_data,
                    "gap_records": hour_gaps,
                }
                stats["total_records"] += hour_data + hour_gaps
                stats["data_records"] += hour_data
                stats["gap_records"] += hour_gaps

    return stats
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k write_daily`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add streaming daily file writer"
```

---

### Task 6: Verification pass

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from src.cli.consolidate import verify_daily_file
from src.writer.file_rotator import compute_sha256, write_sha256_sidecar, sidecar_path


def test_verify_daily_file_passes_valid_file(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    records = [_make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)]

    def hour_iterator():
        yield 0, records

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=2, sha256_path=sc)
    assert ok is True
    assert error is None


def test_verify_daily_file_fails_on_wrong_count(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=999, sha256_path=sc)
    assert ok is False
    assert "count" in error.lower()


def test_verify_daily_file_fails_on_wrong_sha256(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    sc.write_text("0000bad  2026-03-28.jsonl.zst\n")

    ok, error = verify_daily_file(output_path, expected_count=1, sha256_path=sc)
    assert ok is False
    assert "sha256" in error.lower()


def test_verify_daily_file_fails_on_decreasing_ts(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    # Manually write a file with out-of-order timestamps
    cctx = zstandard.ZstdCompressor(level=3)
    records = [_make_data_env(exchange_ts=200), _make_data_env(exchange_ts=100)]
    data = b"\n".join(orjson.dumps(r) for r in records) + b"\n"
    output_path.write_bytes(cctx.compress(data))

    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=2, sha256_path=sc)
    assert ok is False
    assert "order" in error.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_verify_daily_file_passes_valid_file -v`
Expected: FAIL — `ImportError: cannot import name 'verify_daily_file'`

- [ ] **Step 3: Implement `verify_daily_file`**

Add to `src/cli/consolidate.py`:

```python
from src.writer.file_rotator import compute_sha256


def verify_daily_file(
    daily_path: Path,
    expected_count: int,
    sha256_path: Path,
) -> tuple[bool, str | None]:
    """Verify a daily file: record count, ordering, and SHA256.

    Returns (True, None) on success, (False, error_message) on failure.
    """
    # Check SHA256 first (cheapest — reads compressed file)
    actual_sha = compute_sha256(daily_path)
    expected_sha = sha256_path.read_text().strip().split()[0]
    if actual_sha != expected_sha:
        return False, f"SHA256 mismatch: expected {expected_sha}, got {actual_sha}"

    # Stream-read and verify count + ordering
    dctx = zstd.ZstdDecompressor()
    count = 0
    prev_ts = -1

    with open(daily_path, "rb") as fh:
        reader = dctx.stream_reader(fh)
        buf = b""
        while True:
            chunk = reader.read(65536)
            if not chunk:
                break
            buf += chunk
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line:
                    continue
                record = orjson.loads(line)
                count += 1
                if record.get("type") == "gap":
                    ts = record["gap_start_ts"]
                else:
                    ts = record["exchange_ts"]
                if ts < prev_ts:
                    return False, (
                        f"Order violation at record {count}: "
                        f"ts {ts} < previous {prev_ts}"
                    )
                prev_ts = ts

        # Process remaining buffer
        if buf.strip():
            record = orjson.loads(buf.strip())
            count += 1
            if record.get("type") == "gap":
                ts = record["gap_start_ts"]
            else:
                ts = record["exchange_ts"]
            if ts < prev_ts:
                return False, (
                    f"Order violation at record {count}: "
                    f"ts {ts} < previous {prev_ts}"
                )

    if count != expected_count:
        return False, f"Record count mismatch: expected {expected_count}, got {count}"

    return True, None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k verify`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add daily file verification"
```

---

### Task 7: Manifest writer

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
import json
from src.cli.consolidate import write_manifest


def test_write_manifest_structure(tmp_path):
    manifest_path = tmp_path / "2026-03-28.manifest.json"
    write_manifest(
        manifest_path=manifest_path,
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
        daily_file_name="2026-03-28.jsonl.zst",
        daily_file_sha256="abc123",
        stats={
            "total_records": 100,
            "data_records": 98,
            "gap_records": 2,
            "hours": {
                0: {"data_records": 50, "gap_records": 0},
                1: {"data_records": 48, "gap_records": 0},
                14: {"data_records": 0, "gap_records": 2},
            },
        },
        hour_details={
            0: {"status": "present", "sources": ["hour-0.jsonl.zst"]},
            1: {"status": "present", "sources": ["hour-1.jsonl.zst", "hour-1.late-1.jsonl.zst"]},
            14: {"status": "missing", "synthesized_gap": True},
        },
        source_files=["hour-0.jsonl.zst", "hour-1.jsonl.zst", "hour-1.late-1.jsonl.zst"],
        missing_hours=[14],
    )

    assert manifest_path.exists()
    m = json.loads(manifest_path.read_text())
    assert m["version"] == 1
    assert m["exchange"] == "binance"
    assert m["symbol"] == "btcusdt"
    assert m["stream"] == "trades"
    assert m["date"] == "2026-03-28"
    assert m["daily_file"] == "2026-03-28.jsonl.zst"
    assert m["daily_file_sha256"] == "abc123"
    assert m["total_records"] == 100
    assert m["data_records"] == 98
    assert m["gap_records"] == 2
    assert m["missing_hours"] == [14]
    assert "14" in m["hours"]
    assert m["hours"]["14"]["status"] == "missing"
    assert "consolidated_at" in m
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_write_manifest_structure -v`
Expected: FAIL — `ImportError: cannot import name 'write_manifest'`

- [ ] **Step 3: Implement `write_manifest`**

Add to `src/cli/consolidate.py`:

```python
import json


def write_manifest(
    *,
    manifest_path: Path,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    daily_file_name: str,
    daily_file_sha256: str,
    stats: dict,
    hour_details: dict[int, dict],
    source_files: list[str],
    missing_hours: list[int],
) -> None:
    """Write a manifest JSON file summarizing the consolidation."""
    manifest = {
        "version": 1,
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "date": date,
        "consolidated_at": datetime.now(timezone.utc).isoformat(),
        "daily_file": daily_file_name,
        "daily_file_sha256": daily_file_sha256,
        "total_records": stats["total_records"],
        "data_records": stats["data_records"],
        "gap_records": stats["gap_records"],
        "hours": {},
        "missing_hours": missing_hours,
        "source_files": source_files,
    }

    # Build hours section: merge stats with hour_details
    all_hours = set(stats.get("hours", {}).keys()) | set(hour_details.keys())
    for h in sorted(all_hours):
        hour_key = str(h)
        entry = dict(hour_details.get(h, {}))
        if h in stats.get("hours", {}):
            entry.update(stats["hours"][h])
        manifest["hours"][hour_key] = entry

    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k manifest`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add manifest writer"
```

---

### Task 8: Cleanup function

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from src.cli.consolidate import cleanup_hourly_files


def test_cleanup_removes_zst_keeps_sha256(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()

    zst_file = date_dir / "hour-0.jsonl.zst"
    sha_file = date_dir / "hour-0.jsonl.zst.sha256"
    zst_file.write_bytes(b"data")
    sha_file.write_text("abc  hour-0.jsonl.zst\n")

    late_zst = date_dir / "hour-0.late-1.jsonl.zst"
    late_sha = date_dir / "hour-0.late-1.jsonl.zst.sha256"
    late_zst.write_bytes(b"data")
    late_sha.write_text("abc  hour-0.late-1.jsonl.zst\n")

    consolidated = [zst_file, late_zst]
    cleanup_hourly_files(date_dir, consolidated)

    assert not zst_file.exists()
    assert not late_zst.exists()
    assert sha_file.exists()
    assert late_sha.exists()


def test_cleanup_does_not_remove_unrelated_files(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()

    unrelated = date_dir / "something_else.txt"
    unrelated.write_text("keep me")

    zst_file = date_dir / "hour-0.jsonl.zst"
    zst_file.write_bytes(b"data")

    cleanup_hourly_files(date_dir, [zst_file])

    assert not zst_file.exists()
    assert unrelated.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_cleanup_removes_zst_keeps_sha256 -v`
Expected: FAIL — `ImportError: cannot import name 'cleanup_hourly_files'`

- [ ] **Step 3: Implement `cleanup_hourly_files`**

Add to `src/cli/consolidate.py`:

```python
def cleanup_hourly_files(date_dir: Path, consolidated_files: list[Path]) -> int:
    """Remove consolidated .jsonl.zst files, keep .sha256 sidecars.

    Returns number of files removed.
    """
    removed = 0
    for f in consolidated_files:
        if f.exists():
            f.unlink()
            removed += 1
            logger.info("cleanup_removed", file=f.name)
    return removed
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k cleanup`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add hourly file cleanup"
```

---

### Task 9: Main orchestrator — `consolidate_day`

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from src.cli.consolidate import consolidate_day


def _setup_full_day(tmp_path, hours=range(24), stream="trades"):
    """Create a full day of hourly files for testing."""
    base_dir = tmp_path
    date_dir = base_dir / "binance" / "btcusdt" / stream / "2026-03-28"
    for h in hours:
        envs = [_make_data_env(exchange_ts=h * 3600_000 + i) for i in range(3)]
        _write_zst_file(date_dir / f"hour-{h}.jsonl.zst", envs)
        # Write sha256 sidecars
        from src.writer.file_rotator import write_sha256_sidecar, sidecar_path as sc_path
        data_path = date_dir / f"hour-{h}.jsonl.zst"
        write_sha256_sidecar(data_path, sc_path(data_path))
    return base_dir


def test_consolidate_day_full_day(tmp_path):
    base_dir = _setup_full_day(tmp_path)
    result = consolidate_day(
        base_dir=str(base_dir),
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
    )

    assert result["success"] is True
    assert result["total_records"] == 72  # 24 hours * 3 records

    # Daily file should exist
    daily = base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28.jsonl.zst"
    assert daily.exists()
    assert daily.with_suffix(".zst.sha256").exists()

    # Manifest should exist
    manifest = base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28.manifest.json"
    assert manifest.exists()

    # Hourly .jsonl.zst files should be removed
    date_dir = base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28"
    zst_files = list(date_dir.glob("hour-*.jsonl.zst"))
    assert len(zst_files) == 0

    # But .sha256 sidecars should remain
    sha_files = list(date_dir.glob("*.sha256"))
    assert len(sha_files) == 24


def test_consolidate_day_with_missing_hours(tmp_path):
    # Create hours 0-22 but skip hour 14
    hours = [h for h in range(23) if h != 14]
    base_dir = _setup_full_day(tmp_path, hours=hours)

    result = consolidate_day(
        base_dir=str(base_dir),
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
    )

    assert result["success"] is True
    assert result["missing_hours"] == [14, 23]
    # 22 hours * 3 records + 2 gap envelopes
    assert result["total_records"] == 68

    m = json.loads(
        (base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28.manifest.json").read_text()
    )
    assert 14 in m["missing_hours"]


def test_consolidate_day_skips_already_consolidated(tmp_path):
    base_dir = _setup_full_day(tmp_path)

    # Create a fake daily file to simulate already consolidated
    daily = base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28.jsonl.zst"
    daily.parent.mkdir(parents=True, exist_ok=True)
    daily.write_bytes(b"fake")

    result = consolidate_day(
        base_dir=str(base_dir),
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
    )

    assert result["skipped"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_consolidate_day_full_day -v`
Expected: FAIL — `ImportError: cannot import name 'consolidate_day'`

- [ ] **Step 3: Implement `consolidate_day`**

Add to `src/cli/consolidate.py`:

```python
from src.writer.file_rotator import compute_sha256 as _compute_sha256, write_sha256_sidecar, sidecar_path


def consolidate_day(
    *,
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
) -> dict:
    """Consolidate all hourly files for one exchange/symbol/stream/date into a daily file.

    Returns result dict with keys: success, skipped, total_records, missing_hours, error.
    """
    session_id = f"consolidation-{datetime.now(timezone.utc).isoformat()}"
    base = Path(base_dir)
    stream_dir = base / exchange / symbol.lower() / stream
    date_dir = stream_dir / date
    daily_path = stream_dir / f"{date}.jsonl.zst"
    sha_path = sidecar_path(daily_path)
    manifest_path = stream_dir / f"{date}.manifest.json"

    # Idempotency check
    if daily_path.exists():
        logger.info("consolidation_skipped", exchange=exchange, symbol=symbol,
                     stream=stream, date=date, reason="daily file already exists")
        return {"skipped": True, "success": True}

    # Check if date directory exists
    if not date_dir.is_dir():
        logger.warning("consolidation_no_date_dir", exchange=exchange, symbol=symbol,
                       stream=stream, date=date)
        return {"skipped": True, "success": True}

    # Discover and classify files
    hour_files = discover_hour_files(date_dir)

    # Identify missing hours and collect source files
    missing_hours = []
    source_files = []
    hour_details: dict[int, dict] = {}

    for h in range(24):
        if h not in hour_files:
            missing_hours.append(h)
            hour_details[h] = {"status": "missing", "synthesized_gap": True}
        else:
            fg = hour_files[h]
            sources = []
            if fg["base"]:
                sources.append(fg["base"].name)
            sources.extend(f.name for f in fg["late"])
            sources.extend(f.name for f in fg["backfill"])
            source_files.extend(sources)

            if fg["base"]:
                status = "present"
            elif fg["backfill"]:
                status = "backfilled"
            else:
                status = "late"
            hour_details[h] = {"status": status, "sources": sources}

    logger.info("consolidation_starting", exchange=exchange, symbol=symbol,
                stream=stream, date=date, present_hours=24 - len(missing_hours),
                missing_hours=len(missing_hours))

    # Build hour records iterator
    def hour_iterator():
        for h in range(24):
            if h in hour_files:
                records = merge_hour(h, hour_files[h])
                yield h, records
            else:
                gap = synthesize_missing_hour_gap(
                    exchange=exchange, symbol=symbol, stream=stream,
                    date=date, hour=h, session_id=session_id,
                )
                yield h, [gap]

    # Write daily file
    stats = write_daily_file(daily_path, hour_iterator())

    # Write SHA256 sidecar
    write_sha256_sidecar(daily_path, sha_path)
    daily_sha = _compute_sha256(daily_path)

    # Write manifest
    write_manifest(
        manifest_path=manifest_path,
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        date=date,
        daily_file_name=daily_path.name,
        daily_file_sha256=daily_sha,
        stats=stats,
        hour_details=hour_details,
        source_files=source_files,
        missing_hours=missing_hours,
    )

    # Verify
    ok, error = verify_daily_file(daily_path, stats["total_records"], sha_path)
    if not ok:
        logger.error("consolidation_verification_failed", exchange=exchange,
                     symbol=symbol, stream=stream, date=date, error=error)
        # Clean up partial output
        daily_path.unlink(missing_ok=True)
        sha_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        return {"success": False, "error": error}

    # Cleanup hourly files
    consolidated_files = []
    for h, fg in hour_files.items():
        if fg["base"]:
            consolidated_files.append(fg["base"])
        consolidated_files.extend(fg["late"])
        consolidated_files.extend(fg["backfill"])
    cleanup_hourly_files(date_dir, consolidated_files)

    logger.info("consolidation_complete", exchange=exchange, symbol=symbol,
                stream=stream, date=date, total_records=stats["total_records"],
                missing_hours=len(missing_hours))

    return {
        "success": True,
        "skipped": False,
        "total_records": stats["total_records"],
        "missing_hours": missing_hours,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k consolidate_day`
Expected: 3 passed

- [ ] **Step 5: Run all tests to verify nothing is broken**

Run: `python -m pytest tests/unit/test_consolidate.py -v`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add consolidate_day orchestrator"
```

---

### Task 10: CLI entry point

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_consolidate.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/unit/test_consolidate.py`:

```python
from click.testing import CliRunner
from src.cli.consolidate import cli


def test_cli_consolidate_full_day(tmp_path):
    base_dir = _setup_full_day(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, [
        "run", "--base-dir", str(base_dir),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-03-28",
    ])
    assert result.exit_code == 0
    assert "complete" in result.output.lower() or "success" in result.output.lower()

    daily = base_dir / "binance" / "btcusdt" / "trades" / "2026-03-28.jsonl.zst"
    assert daily.exists()


def test_cli_consolidate_specific_stream(tmp_path):
    base_dir = _setup_full_day(tmp_path, stream="depth")
    runner = CliRunner()
    result = runner.invoke(cli, [
        "run", "--base-dir", str(base_dir),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--stream", "depth", "--date", "2026-03-28",
    ])
    assert result.exit_code == 0
    daily = base_dir / "binance" / "btcusdt" / "depth" / "2026-03-28.jsonl.zst"
    assert daily.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/unit/test_consolidate.py::test_cli_consolidate_full_day -v`
Expected: FAIL — `ImportError: cannot import name 'cli'`

- [ ] **Step 3: Implement CLI**

Add to `src/cli/consolidate.py`:

```python
import click


ALL_STREAMS = [
    "trades", "depth", "depth_snapshot", "bookticker",
    "funding_rate", "liquidations", "open_interest",
]


@click.group()
def cli():
    """Daily consolidation CLI."""
    pass


@cli.command()
@click.option("--base-dir", default=None, help="Archive base directory")
@click.option("--exchange", default="binance", help="Exchange name")
@click.option("--symbol", required=True, help="Trading symbol (e.g. btcusdt)")
@click.option("--stream", default=None, help="Specific stream to consolidate (default: all)")
@click.option("--date", "target_date", required=True, help="Date to consolidate (YYYY-MM-DD)")
def run(base_dir, exchange, symbol, stream, target_date):
    """Consolidate hourly files into a daily file."""
    from src.common.config import default_archive_dir
    from src.common.logging import setup_logging

    setup_logging()

    if base_dir is None:
        base_dir = default_archive_dir()

    streams = [stream] if stream else ALL_STREAMS

    for s in streams:
        date_dir = Path(base_dir) / exchange / symbol.lower() / s / target_date
        if not date_dir.is_dir():
            continue

        result = consolidate_day(
            base_dir=base_dir,
            exchange=exchange,
            symbol=symbol,
            stream=s,
            date=target_date,
        )

        if result.get("skipped"):
            click.echo(f"[{s}] Skipped (already consolidated or no data)")
        elif result.get("success"):
            click.echo(
                f"[{s}] Complete: {result['total_records']} records, "
                f"{len(result.get('missing_hours', []))} missing hours"
            )
        else:
            click.echo(f"[{s}] FAILED: {result.get('error', 'unknown error')}")

    click.echo("Consolidation finished.")


if __name__ == "__main__":
    cli()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidate.py -v -k cli`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_consolidate.py
git commit -m "feat(consolidation): add CLI entry point"
```

---

### Task 11: Consolidation scheduler service

**Files:**
- Create: `src/cli/consolidation_scheduler.py`
- Test: `tests/unit/test_consolidation_scheduler.py`

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_consolidation_scheduler.py`:

```python
from unittest.mock import patch, MagicMock
from datetime import date

from src.cli.consolidation_scheduler import _get_target_date, _get_all_streams


def test_get_target_date_returns_yesterday():
    with patch("src.cli.consolidation_scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 3, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        result = _get_target_date()
        assert result == "2026-03-28"


def test_get_all_streams_from_config():
    streams = _get_all_streams()
    assert "trades" in streams
    assert "depth" in streams
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/unit/test_consolidation_scheduler.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement the scheduler**

Create `src/cli/consolidation_scheduler.py`:

```python
"""Consolidation scheduler: runs daily file consolidation and exposes Prometheus metrics.

Merges hourly archive files into single daily files per exchange/symbol/stream.
Runs once daily at a configurable hour (default 02:30 UTC).
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import structlog
from prometheus_client import Counter, Gauge, start_http_server

from src.cli.consolidate import ALL_STREAMS, consolidate_day
from src.common.config import default_archive_dir

logger = structlog.get_logger()

METRICS_PORT = 8003
DEFAULT_START_HOUR = 2
DEFAULT_START_MINUTE = 30

# ── Prometheus metrics ─────────────────────────────────────────────
consolidation_last_run_ts = Gauge(
    "consolidation_last_run_timestamp_seconds",
    "Unix timestamp of the last consolidation run",
)
consolidation_last_duration = Gauge(
    "consolidation_last_run_duration_seconds",
    "Duration of the last consolidation run in seconds",
)
consolidation_last_success = Gauge(
    "consolidation_last_run_success",
    "1 if last consolidation run succeeded, 0 if it failed",
)
consolidation_runs_total = Counter(
    "consolidation_runs_total",
    "Total number of consolidation runs since process start",
)
consolidation_days_processed = Counter(
    "consolidation_days_processed",
    "Days successfully consolidated",
)
consolidation_files_consolidated = Counter(
    "consolidation_files_consolidated",
    "Hourly files consolidated into daily files",
)
consolidation_verification_failures = Counter(
    "consolidation_verification_failures_total",
    "Verification failures during consolidation",
)
consolidation_missing_hours = Counter(
    "consolidation_missing_hours_total",
    "Synthesized missing-hour gaps",
)


def _get_target_date() -> str:
    """Return yesterday's date as YYYY-MM-DD string."""
    yesterday = date.today() - timedelta(days=1)
    return yesterday.isoformat()


def _get_all_streams() -> list[str]:
    """Return all stream types to consolidate."""
    return list(ALL_STREAMS)


def _get_symbols(base_dir: str, exchange: str) -> list[str]:
    """Discover symbols from the archive directory structure."""
    exchange_dir = Path(base_dir) / exchange
    if not exchange_dir.is_dir():
        return []
    return sorted(
        d.name for d in exchange_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )


def _seconds_until_next_run(start_hour: int, start_minute: int) -> float:
    """Calculate seconds until the next scheduled run time."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _run_consolidation_cycle(base_dir: str) -> None:
    """Run one consolidation cycle for all symbols and streams."""
    start = time.monotonic()
    run_ts = time.time()
    success = True
    total_files = 0

    try:
        target_date = _get_target_date()
        exchange = "binance"
        symbols = _get_symbols(base_dir, exchange)
        streams = _get_all_streams()

        logger.info("consolidation_cycle_starting", date=target_date,
                     symbols=len(symbols), streams=len(streams))

        for symbol in symbols:
            for stream in streams:
                date_dir = Path(base_dir) / exchange / symbol / stream / target_date
                if not date_dir.is_dir():
                    continue

                try:
                    result = consolidate_day(
                        base_dir=base_dir,
                        exchange=exchange,
                        symbol=symbol,
                        stream=stream,
                        date=target_date,
                    )

                    if result.get("skipped"):
                        logger.info("consolidation_stream_skipped",
                                    symbol=symbol, stream=stream, date=target_date)
                    elif result.get("success"):
                        n_missing = len(result.get("missing_hours", []))
                        consolidation_missing_hours.inc(n_missing)
                        consolidation_days_processed.inc()
                        # Count source files from hourly dir
                        total_files += result.get("total_records", 0)
                        logger.info("consolidation_stream_done",
                                    symbol=symbol, stream=stream,
                                    date=target_date,
                                    records=result["total_records"],
                                    missing_hours=n_missing)
                    else:
                        consolidation_verification_failures.inc()
                        success = False
                        logger.error("consolidation_stream_failed",
                                     symbol=symbol, stream=stream,
                                     date=target_date,
                                     error=result.get("error"))

                except Exception as e:
                    logger.error("consolidation_stream_error",
                                 symbol=symbol, stream=stream,
                                 date=target_date, error=str(e))
                    success = False

    except Exception as e:
        logger.error("consolidation_cycle_failed", error=str(e))
        success = False

    elapsed = time.monotonic() - start
    consolidation_last_run_ts.set(run_ts)
    consolidation_last_duration.set(elapsed)
    consolidation_last_success.set(1 if success else 0)
    consolidation_runs_total.inc()

    logger.info("consolidation_cycle_complete",
                elapsed_s=round(elapsed, 1), success=success)


async def main() -> None:
    base_dir = default_archive_dir()
    start_hour = DEFAULT_START_HOUR
    start_minute = DEFAULT_START_MINUTE

    logger.info("consolidation_scheduler_starting",
                metrics_port=METRICS_PORT,
                base_dir=base_dir,
                run_at=f"{start_hour:02d}:{start_minute:02d} UTC")

    start_http_server(METRICS_PORT)

    run_count = 0
    while True:
        sleep_secs = _seconds_until_next_run(start_hour, start_minute)
        logger.info("consolidation_sleeping",
                     next_run_in_h=round(sleep_secs / 3600, 1))
        await asyncio.sleep(sleep_secs)

        run_count += 1
        logger.info("consolidation_run_starting", run=run_count)
        await _run_consolidation_cycle(base_dir)


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/unit/test_consolidation_scheduler.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidation_scheduler.py tests/unit/test_consolidation_scheduler.py
git commit -m "feat(consolidation): add consolidation scheduler service"
```

---

### Task 12: Dockerfile and Docker Compose integration

**Files:**
- Create: `Dockerfile.consolidation`
- Modify: `docker-compose.yml`
- Modify: `infra/prometheus/prometheus.yml`

- [ ] **Step 1: Create Dockerfile**

Create `Dockerfile.consolidation`:

```dockerfile
FROM python:3.12.7-slim AS base

WORKDIR /app

RUN groupadd -r cryptolake && useradd -r -g cryptolake cryptolake
RUN pip install uv

COPY pyproject.toml uv.lock ./
COPY src/ src/
RUN uv sync --frozen --no-dev

RUN mkdir -p /data /home/cryptolake/.cache/uv && \
    chown -R cryptolake:cryptolake /data /home/cryptolake

USER cryptolake

EXPOSE 8003

CMD ["/app/.venv/bin/python", "-m", "src.cli.consolidation_scheduler"]
```

- [ ] **Step 2: Add consolidation service to docker-compose.yml**

Add after the `backfill` service block (after line 148):

```yaml
  consolidation:
    build:
      context: .
      dockerfile: Dockerfile.consolidation
    depends_on:
      writer:
        condition: service_healthy
    networks:
      - cryptolake_internal
    volumes:
      - ${HOST_DATA_DIR:-/data}:${HOST_DATA_DIR:-/data}
    environment:
      - HOST_DATA_DIR=${HOST_DATA_DIR:-/data}
    healthcheck:
      test: ["CMD", "python", "-c", "from urllib.request import urlopen; raise SystemExit(0 if urlopen('http://127.0.0.1:8003/', timeout=5).status == 200 else 1)"]
      interval: 30s
      timeout: 5s
      retries: 3
    restart: unless-stopped
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

- [ ] **Step 3: Add Prometheus scrape target**

Add to `infra/prometheus/prometheus.yml` after the backfill job:

```yaml
  - job_name: consolidation
    static_configs:
      - targets: ["consolidation:8003"]
```

- [ ] **Step 4: Verify docker-compose config is valid**

Run: `docker compose config --quiet`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add Dockerfile.consolidation docker-compose.yml infra/prometheus/prometheus.yml
git commit -m "feat(consolidation): add Docker service and Prometheus scraping"
```

---

### Task 13: Run full test suite and verify

**Files:** None (verification only)

- [ ] **Step 1: Run all consolidation tests**

Run: `python -m pytest tests/unit/test_consolidate.py tests/unit/test_consolidation_scheduler.py -v`
Expected: All tests pass

- [ ] **Step 2: Run the entire test suite to check for regressions**

Run: `python -m pytest tests/ -v`
Expected: All tests pass, no regressions

- [ ] **Step 3: Test CLI manually with dry data**

Run:
```bash
# Create a temp test directory and run CLI
TMPDIR=$(mktemp -d)
python -c "
import zstandard, orjson
from pathlib import Path
base = Path('$TMPDIR')
for h in range(24):
    d = base / 'binance' / 'btcusdt' / 'trades' / '2026-03-28'
    d.mkdir(parents=True, exist_ok=True)
    env = {'v':1,'type':'data','exchange':'binance','symbol':'btcusdt','stream':'trades',
           'received_at':1000,'exchange_ts':h*3600000+1,'collector_session_id':'test',
           'session_seq':0,'raw_text':'{}','raw_sha256':'abc',
           '_topic':'t','_partition':0,'_offset':0}
    cctx = zstandard.ZstdCompressor()
    (d / f'hour-{h}.jsonl.zst').write_bytes(cctx.compress(orjson.dumps(env)))
"
python -m src.cli.consolidate run --base-dir "$TMPDIR" --symbol btcusdt --date 2026-03-28
ls -la "$TMPDIR/binance/btcusdt/trades/"
rm -rf "$TMPDIR"
```

Expected: Daily file created, hourly files removed, manifest present

- [ ] **Step 4: Commit (if any fixes were needed)**

```bash
git add -u
git commit -m "fix(consolidation): address issues found during verification"
```
