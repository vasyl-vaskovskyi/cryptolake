# Sealed Daily Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After consolidation, package all per-stream daily files into a single sealed tar.zst archive per date. Scripts default to today's date with a 3-day max range, and skip sealed dates.

**Architecture:** Consolidation gains a sealing step that creates `{symbol}/{date}.tar.zst` from uncompressed JSONL + manifests + checksums, then removes per-stream files. Analyze reads sealed archives via tarfile, integrity/backfill skip them. All three CLI scripts default to today (UTC) and enforce 3-day range limits.

**Tech Stack:** Python 3.12, tarfile, zstandard, click

**Spec:** `docs/superpowers/specs/2026-04-03-sealed-daily-archive-design.md`

---

### Task 1: Seal daily archive function

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_seal_archive.py` (create)

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_seal_archive.py`:

```python
import json
import tarfile
import zstandard
import orjson
from pathlib import Path
from src.cli.consolidate import seal_daily_archive


def _create_daily_files(base_dir, exchange, symbol, stream, date, records=3):
    """Create fake consolidated daily files for testing."""
    stream_dir = base_dir / exchange / symbol / stream
    stream_dir.mkdir(parents=True, exist_ok=True)

    # Create .jsonl.zst
    cctx = zstandard.ZstdCompressor(level=3)
    lines = []
    for i in range(records):
        env = {
            "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
            "stream": stream, "received_at": 1000 + i, "exchange_ts": 1000 + i,
            "collector_session_id": "test", "session_seq": i,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        lines.append(orjson.dumps(env))
    data = b"\n".join(lines) + b"\n"
    zst_path = stream_dir / f"{date}.jsonl.zst"
    zst_path.write_bytes(cctx.compress(data))

    # Create .jsonl.zst.sha256
    from src.writer.file_rotator import compute_sha256
    sha = compute_sha256(zst_path)
    (stream_dir / f"{date}.jsonl.zst.sha256").write_text(f"{sha}  {date}.jsonl.zst\n")

    # Create .manifest.json
    manifest = {
        "version": 1, "exchange": exchange, "symbol": symbol,
        "stream": stream, "date": date, "total_records": records,
        "data_records": records, "gap_records": 0,
        "hours": {}, "missing_hours": [], "source_files": [],
    }
    (stream_dir / f"{date}.manifest.json").write_text(json.dumps(manifest))


def test_seal_creates_tar_zst(tmp_path):
    date = "2026-04-02"
    for stream in ["trades", "depth"]:
        _create_daily_files(tmp_path, "binance", "btcusdt", stream, date)

    result = seal_daily_archive(
        base_dir=str(tmp_path),
        exchange="binance",
        symbol="btcusdt",
        date=date,
    )

    assert result["success"] is True
    archive_path = tmp_path / "binance" / "btcusdt" / f"{date}.tar.zst"
    assert archive_path.exists()
    sha_path = tmp_path / "binance" / "btcusdt" / f"{date}.tar.zst.sha256"
    assert sha_path.exists()


def test_seal_archive_contains_uncompressed_jsonl(tmp_path):
    date = "2026-04-02"
    _create_daily_files(tmp_path, "binance", "btcusdt", "trades", date, records=2)

    seal_daily_archive(
        base_dir=str(tmp_path), exchange="binance", symbol="btcusdt", date=date,
    )

    archive_path = tmp_path / "binance" / "btcusdt" / f"{date}.tar.zst"
    dctx = zstandard.ZstdDecompressor()
    with open(archive_path, "rb") as f:
        with dctx.stream_reader(f) as reader:
            with tarfile.open(fileobj=reader, mode="r|") as tar:
                names = [m.name for m in tar]
    assert f"trades-{date}.jsonl" in names
    assert f"trades-{date}.manifest.json" in names
    assert f"trades-{date}.sha256" in names


def test_seal_removes_per_stream_files(tmp_path):
    date = "2026-04-02"
    _create_daily_files(tmp_path, "binance", "btcusdt", "trades", date)

    seal_daily_archive(
        base_dir=str(tmp_path), exchange="binance", symbol="btcusdt", date=date,
    )

    stream_dir = tmp_path / "binance" / "btcusdt" / "trades"
    assert not (stream_dir / f"{date}.jsonl.zst").exists()
    assert not (stream_dir / f"{date}.manifest.json").exists()
    assert not (stream_dir / f"{date}.jsonl.zst.sha256").exists()


def test_seal_skips_if_already_sealed(tmp_path):
    date = "2026-04-02"
    symbol_dir = tmp_path / "binance" / "btcusdt"
    symbol_dir.mkdir(parents=True, exist_ok=True)
    (symbol_dir / f"{date}.tar.zst").write_bytes(b"fake")

    result = seal_daily_archive(
        base_dir=str(tmp_path), exchange="binance", symbol="btcusdt", date=date,
    )
    assert result["skipped"] is True
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_seal_archive.py -v`
Expected: FAIL — `cannot import name 'seal_daily_archive'`

- [ ] **Step 3: Implement `seal_daily_archive`**

Add to `src/cli/consolidate.py`, before the `click` imports:

```python
import tarfile


def seal_daily_archive(
    *,
    base_dir: str,
    exchange: str,
    symbol: str,
    date: str,
) -> dict:
    """Package all per-stream daily files into a single sealed tar.zst archive.

    Creates {base_dir}/{exchange}/{symbol}/{date}.tar.zst containing
    uncompressed JSONL, manifests, and SHA256 files for all streams.
    Removes per-stream daily files after successful archive creation.
    """
    base = Path(base_dir)
    symbol_dir = base / exchange / symbol.lower()
    archive_path = symbol_dir / f"{date}.tar.zst"
    sha_path = symbol_dir / f"{date}.tar.zst.sha256"

    if archive_path.exists():
        logger.info("seal_skipped", exchange=exchange, symbol=symbol,
                     date=date, reason="already sealed")
        return {"skipped": True, "success": True}

    # Collect per-stream files to archive
    files_to_archive: list[tuple[str, Path]] = []  # (archive_name, source_path)
    streams_found: list[str] = []

    for stream_dir in sorted(symbol_dir.iterdir()):
        if not stream_dir.is_dir():
            continue
        stream_name = stream_dir.name
        zst_file = stream_dir / f"{date}.jsonl.zst"
        manifest_file = stream_dir / f"{date}.manifest.json"
        sha_file = stream_dir / f"{date}.jsonl.zst.sha256"

        if not zst_file.exists():
            continue

        streams_found.append(stream_name)
        files_to_archive.append((f"{stream_name}-{date}.jsonl", zst_file))
        if manifest_file.exists():
            files_to_archive.append((f"{stream_name}-{date}.manifest.json", manifest_file))
        if sha_file.exists():
            files_to_archive.append((f"{stream_name}-{date}.sha256", sha_file))

    if not files_to_archive:
        logger.warning("seal_no_files", exchange=exchange, symbol=symbol, date=date)
        return {"skipped": True, "success": True}

    logger.info("seal_starting", exchange=exchange, symbol=symbol,
                date=date, streams=streams_found)

    # Build tar.zst archive
    cctx = zstd.ZstdCompressor(level=3)
    symbol_dir.mkdir(parents=True, exist_ok=True)

    with open(archive_path, "wb") as fh:
        with cctx.stream_writer(fh) as zst_writer:
            with tarfile.open(fileobj=zst_writer, mode="w|") as tar:
                for archive_name, source_path in files_to_archive:
                    if archive_name.endswith(".jsonl"):
                        # Decompress .jsonl.zst -> raw JSONL for the tar
                        dctx = zstd.ZstdDecompressor()
                        with open(source_path, "rb") as src:
                            raw_data = dctx.stream_reader(src).read()
                        info = tarfile.TarInfo(name=archive_name)
                        info.size = len(raw_data)
                        import io
                        tar.addfile(info, io.BytesIO(raw_data))
                    else:
                        # Manifests and SHA256 files — add directly
                        tar.add(source_path, arcname=archive_name)

    # Write SHA256 sidecar for the archive
    from src.writer.file_rotator import compute_sha256 as _compute_sha256
    archive_sha = _compute_sha256(archive_path)
    sha_path.write_text(f"{archive_sha}  {archive_path.name}\n")

    # Remove per-stream daily files
    for _archive_name, source_path in files_to_archive:
        source_path.unlink(missing_ok=True)

    logger.info("seal_complete", exchange=exchange, symbol=symbol,
                date=date, streams=streams_found, archive=str(archive_path))

    return {"success": True, "skipped": False, "streams": streams_found}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_seal_archive.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_seal_archive.py
git commit -m "feat(seal): add seal_daily_archive function"
```

---

### Task 2: Hook sealing into consolidation CLI

**Files:**
- Modify: `src/cli/consolidate.py`
- Test: `tests/unit/test_seal_archive.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/test_seal_archive.py`:

```python
from click.testing import CliRunner
from src.cli.consolidate import cli


def _setup_hourly_for_seal(tmp_path, date="2026-04-02"):
    """Create hourly files for one stream so consolidation + seal can run."""
    from datetime import datetime, timezone
    base_dir = tmp_path
    stream = "trades"
    date_dir = base_dir / "binance" / "btcusdt" / stream / date
    day_start_ms = int(datetime(2026, 4, 2, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    for h in range(24):
        envs = [{
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": stream, "received_at": (day_start_ms + h * 3600000) * 1000000,
            "exchange_ts": day_start_ms + h * 3600000,
            "collector_session_id": "test", "session_seq": i,
            "raw_text": f'{{"a": {h * 100 + i}}}', "raw_sha256": "abc",
        } for i in range(3)]
        from tests.unit.test_consolidate import _write_zst_file
        _write_zst_file(date_dir / f"hour-{h}.jsonl.zst", envs)
        from src.writer.file_rotator import write_sha256_sidecar, sidecar_path
        write_sha256_sidecar(date_dir / f"hour-{h}.jsonl.zst", sidecar_path(date_dir / f"hour-{h}.jsonl.zst"))
    return base_dir


def test_consolidate_run_seals_archive(tmp_path):
    base_dir = _setup_hourly_for_seal(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, [
        "run", "--base-dir", str(base_dir),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "sealed" in result.output.lower() or "Sealed" in result.output

    # Archive should exist
    archive = base_dir / "binance" / "btcusdt" / "2026-04-02.tar.zst"
    assert archive.exists()

    # Per-stream files should be removed
    assert not (base_dir / "binance" / "btcusdt" / "trades" / "2026-04-02.jsonl.zst").exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_seal_archive.py::test_consolidate_run_seals_archive -v`
Expected: FAIL — no sealing happens

- [ ] **Step 3: Add sealing to the consolidation `run` command**

In `src/cli/consolidate.py`, update the `run()` function to call `seal_daily_archive` after all streams are consolidated:

```python
@cli.command()
@click.option("--base-dir", default=None, help="Archive base directory")
@click.option("--exchange", default="binance", help="Exchange name")
@click.option("--symbol", required=True, help="Trading symbol (e.g. btcusdt)")
@click.option("--stream", default=None, help="Specific stream to consolidate (default: all)")
@click.option("--date", "target_date", required=True, help="Date to consolidate (YYYY-MM-DD)")
def run(base_dir, exchange, symbol, stream, target_date):
    """Consolidate hourly files into a daily file, then seal into archive."""
    from src.common.config import default_archive_dir
    from src.common.logging import setup_logging

    setup_logging()

    if base_dir is None:
        base_dir = default_archive_dir()

    # Check if already sealed
    symbol_dir = Path(base_dir) / exchange / symbol.lower()
    archive_path = symbol_dir / f"{target_date}.tar.zst"
    if archive_path.exists():
        click.echo(f"Date {target_date} is already sealed. Skipping.")
        return

    streams = [stream] if stream else ALL_STREAMS
    any_consolidated = False

    for s in streams:
        date_dir = Path(base_dir) / exchange / symbol.lower() / s / target_date
        daily_file = Path(base_dir) / exchange / symbol.lower() / s / f"{target_date}.jsonl.zst"
        if not date_dir.is_dir() and not daily_file.exists():
            continue

        result = consolidate_day(
            base_dir=base_dir,
            exchange=exchange,
            symbol=symbol,
            stream=s,
            date=target_date,
        )

        if result.get("skipped"):
            if daily_file.exists():
                any_consolidated = True  # already consolidated, count for sealing
            else:
                click.echo(f"[{s}] Skipped (already consolidated or no data)")
        elif result.get("success"):
            any_consolidated = True
            click.echo(
                f"[{s}] Complete: {result['total_records']} records, "
                f"{len(result.get('missing_hours', []))} missing hours"
            )
        else:
            click.echo(f"[{s}] FAILED: {result.get('error', 'unknown error')}")

    # Seal all consolidated streams into a single archive
    if any_consolidated:
        seal_result = seal_daily_archive(
            base_dir=base_dir,
            exchange=exchange,
            symbol=symbol,
            date=target_date,
        )
        if seal_result.get("success") and not seal_result.get("skipped"):
            click.echo(f"Sealed {target_date} -> {exchange}/{symbol}/{target_date}.tar.zst")

    click.echo("Consolidation finished.")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_seal_archive.py -v`
Expected: 5 passed

- [ ] **Step 5: Run all tests**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/unit/test_infrastructure_assets.py -q`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/cli/consolidate.py tests/unit/test_seal_archive.py
git commit -m "feat(seal): hook sealing into consolidation CLI"
```

---

### Task 3: Default date and 3-day range limit for all scripts

**Files:**
- Modify: `src/cli/gaps.py` (analyze + backfill CLIs)
- Modify: `src/cli/integrity.py` (check CLI)
- Test: `tests/unit/test_cli_date_defaults.py` (create)

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_cli_date_defaults.py`:

```python
from datetime import datetime, timezone
from src.cli.gaps import _resolve_date_args
from src.cli.integrity import _resolve_date_args as _resolve_date_args_integrity


def test_resolve_date_defaults_to_today():
    result = _resolve_date_args(None, None, None)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert result == (today, None, None)


def test_resolve_date_passes_through_explicit_date():
    result = _resolve_date_args("2026-04-01", None, None)
    assert result == ("2026-04-01", None, None)


def test_resolve_date_passes_through_range():
    result = _resolve_date_args(None, "2026-04-01", "2026-04-03")
    assert result == (None, "2026-04-01", "2026-04-03")


def test_resolve_date_rejects_range_over_3_days():
    import pytest
    with pytest.raises(click.UsageError, match="3 days"):
        _resolve_date_args(None, "2026-04-01", "2026-04-05")


def test_resolve_date_integrity_defaults_to_today():
    result = _resolve_date_args_integrity(None, None, None)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert result == (today, None, None)


import click
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_cli_date_defaults.py -v`
Expected: FAIL — `cannot import name '_resolve_date_args'`

- [ ] **Step 3: Implement `_resolve_date_args` in gaps.py**

Add to `src/cli/gaps.py`, before the CLI commands:

```python
def _resolve_date_args(
    date: str | None,
    date_from: str | None,
    date_to: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Apply default date (today UTC) and enforce 3-day range limit."""
    from datetime import datetime, timezone, timedelta

    # Default to today if nothing specified
    if date is None and date_from is None and date_to is None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return today, None, None

    # Enforce 3-day range limit
    if date_from is not None and date_to is not None:
        try:
            d_from = datetime.strptime(date_from, "%Y-%m-%d")
            d_to = datetime.strptime(date_to, "%Y-%m-%d")
            if (d_to - d_from).days > 3:
                raise click.UsageError("Date range cannot exceed 3 days. Use a narrower range.")
        except ValueError:
            pass  # Let downstream handle invalid dates

    return date, date_from, date_to
```

- [ ] **Step 4: Implement the same function in integrity.py**

Add the same `_resolve_date_args` function to `src/cli/integrity.py` (before the `check` command). Same code.

- [ ] **Step 5: Wire into the analyze CLI**

In `src/cli/gaps.py`, update the `analyze` command function:

```python
def analyze(exchange, symbol, stream, date, date_from, date_to, output_json, base_dir):
    """Analyze archive coverage and report gaps."""
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)
    base = Path(base_dir)
    # ... rest unchanged
```

- [ ] **Step 6: Wire into the backfill CLI**

In `src/cli/gaps.py`, update the `backfill` command function:

```python
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, deep, full_day, base_dir):
    """Find gaps and backfill missing hours from Binance historical REST API."""
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)
    base = Path(base_dir)
    # ... rest unchanged
```

- [ ] **Step 7: Wire into the integrity CLI**

In `src/cli/integrity.py`, update the `check` command function:

```python
def check(exchange, symbol, stream, date, date_from, date_to, as_json, base_dir):
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)
    report = check_integrity(
        Path(base_dir),
        # ... rest unchanged
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_cli_date_defaults.py -v`
Expected: 5 passed

- [ ] **Step 9: Run all tests**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/unit/test_infrastructure_assets.py -q`
Expected: All pass

- [ ] **Step 10: Commit**

```bash
git add src/cli/gaps.py src/cli/integrity.py tests/unit/test_cli_date_defaults.py
git commit -m "feat: default date to today (UTC) with 3-day range limit"
```

---

### Task 4: Sealed date skipping in integrity and backfill

**Files:**
- Modify: `src/cli/integrity.py`
- Modify: `src/cli/gaps.py`
- Test: `tests/unit/test_sealed_skip.py` (create)

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_sealed_skip.py`:

```python
from pathlib import Path
from click.testing import CliRunner


def _create_sealed_archive(base_dir, exchange, symbol, date):
    """Create a fake sealed archive."""
    symbol_dir = base_dir / exchange / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    (symbol_dir / f"{date}.tar.zst").write_bytes(b"fake-archive")
    (symbol_dir / f"{date}.tar.zst.sha256").write_text("abc  fake\n")


def test_integrity_skips_sealed_date(tmp_path):
    from src.cli.integrity import check
    _create_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(check, [
        "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "sealed" in result.output.lower()


def test_backfill_skips_sealed_date(tmp_path):
    from src.cli.gaps import cli
    _create_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(cli, [
        "backfill", "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "sealed" in result.output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_sealed_skip.py -v`
Expected: FAIL — sealed dates not detected

- [ ] **Step 3: Add sealed check to integrity CLI**

In `src/cli/integrity.py`, at the start of the `check` command, after `_resolve_date_args`:

```python
def check(exchange, symbol, stream, date, date_from, date_to, as_json, base_dir):
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)

    # Check if date is sealed
    if date is not None:
        base = Path(base_dir)
        for exch_dir in sorted(base.iterdir()) if base.exists() else []:
            if not exch_dir.is_dir():
                continue
            if exchange and exch_dir.name != exchange:
                continue
            for sym_dir in sorted(exch_dir.iterdir()):
                if not sym_dir.is_dir():
                    continue
                if symbol and sym_dir.name != symbol:
                    continue
                archive = sym_dir / f"{date}.tar.zst"
                if archive.exists():
                    click.echo(f"{date} is sealed. Skipping.")
                    return

    report = check_integrity(
        # ... rest unchanged
```

- [ ] **Step 4: Add sealed check to backfill CLI**

In `src/cli/gaps.py`, at the start of the `backfill` command, after `_resolve_date_args`:

```python
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, deep, full_day, base_dir):
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)
    base = Path(base_dir)

    # Check if date is sealed
    if date is not None:
        for exch_dir in sorted(base.iterdir()) if base.exists() else []:
            if not exch_dir.is_dir():
                continue
            if exchange and exch_dir.name != exchange:
                continue
            for sym_dir in sorted(exch_dir.iterdir()):
                if not sym_dir.is_dir():
                    continue
                if symbol and sym_dir.name != symbol:
                    continue
                archive = sym_dir / f"{date}.tar.zst"
                if archive.exists():
                    click.echo(f"{date} is sealed. Skipping.")
                    return

    # ... rest of backfill unchanged
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_sealed_skip.py -v`
Expected: 2 passed

- [ ] **Step 6: Commit**

```bash
git add src/cli/integrity.py src/cli/gaps.py tests/unit/test_sealed_skip.py
git commit -m "feat(seal): integrity and backfill skip sealed dates"
```

---

### Task 5: Analyze reads from sealed archives

**Files:**
- Modify: `src/cli/gaps.py`
- Test: `tests/unit/test_sealed_skip.py`

- [ ] **Step 1: Write failing test**

Append to `tests/unit/test_sealed_skip.py`:

```python
import json
import tarfile
import zstandard
import orjson
import io


def _create_real_sealed_archive(base_dir, exchange, symbol, date):
    """Create a sealed archive with actual JSONL data and manifests."""
    symbol_dir = base_dir / exchange / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    archive_path = symbol_dir / f"{date}.tar.zst"
    cctx = zstandard.ZstdCompressor(level=3)

    # Create gap envelope for testing
    gap_env = {
        "v": 1, "type": "gap", "exchange": exchange, "symbol": symbol,
        "stream": "trades", "received_at": 1000,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 1775132494000000000, "gap_end_ts": 1775132523000000000,
        "reason": "ws_disconnect", "detail": "test gap",
    }
    data_env = {
        "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
        "stream": "trades", "received_at": 2000, "exchange_ts": 2000,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
    }
    jsonl_data = orjson.dumps(data_env) + b"\n" + orjson.dumps(gap_env) + b"\n"

    manifest = {
        "version": 1, "exchange": exchange, "symbol": symbol,
        "stream": "trades", "date": date, "total_records": 2,
        "data_records": 1, "gap_records": 1,
        "hours": {"12": {"status": "present", "data_records": 1}},
        "missing_hours": [], "source_files": [],
    }
    manifest_bytes = json.dumps(manifest).encode()

    with open(archive_path, "wb") as f:
        with cctx.stream_writer(f) as zst:
            with tarfile.open(fileobj=zst, mode="w|") as tar:
                info = tarfile.TarInfo(name=f"trades-{date}.jsonl")
                info.size = len(jsonl_data)
                tar.addfile(info, io.BytesIO(jsonl_data))

                info = tarfile.TarInfo(name=f"trades-{date}.manifest.json")
                info.size = len(manifest_bytes)
                tar.addfile(info, io.BytesIO(manifest_bytes))


def test_analyze_reads_sealed_archive(tmp_path):
    from src.cli.gaps import cli
    _create_real_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(cli, [
        "analyze", "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "ws_disconnect" in result.output
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/python -m pytest tests/unit/test_sealed_skip.py::test_analyze_reads_sealed_archive -v`
Expected: FAIL — analyze doesn't find sealed data

- [ ] **Step 3: Add sealed archive reading to analyze_archive**

In `src/cli/gaps.py`, add a function to read from sealed archives:

```python
def _read_sealed_archive(archive_path: Path, date: str) -> list[tuple[str, dict, list[dict]]]:
    """Read manifests and gap envelopes from a sealed tar.zst archive.

    Returns list of (stream_name, hour_map, gap_envelopes) tuples.
    """
    results: list[tuple[str, dict, list[dict]]] = []
    manifests: dict[str, dict] = {}
    gap_envelopes: dict[str, list[dict]] = {}

    dctx = zstd.ZstdDecompressor()
    try:
        with open(archive_path, "rb") as f:
            with dctx.stream_reader(f) as reader:
                with tarfile.open(fileobj=reader, mode="r|") as tar:
                    for member in tar:
                        if member.name.endswith(f"-{date}.manifest.json"):
                            stream_name = member.name.split(f"-{date}.manifest.json")[0]
                            data = tar.extractfile(member).read()
                            manifests[stream_name] = json.loads(data)
                        elif member.name.endswith(f"-{date}.jsonl"):
                            stream_name = member.name.split(f"-{date}.jsonl")[0]
                            fobj = tar.extractfile(member)
                            gaps = []
                            for line in fobj:
                                if b'"gap"' in line:
                                    env = orjson.loads(line)
                                    if env.get("type") == "gap":
                                        gaps.append(env)
                            gap_envelopes[stream_name] = gaps
    except Exception as exc:
        logger.warning("sealed_archive_read_failed", path=str(archive_path), error=str(exc))
        return []

    for stream_name, manifest in manifests.items():
        hour_map: dict[int, str] = {}
        for h_str, h_info in manifest.get("hours", {}).items():
            hour_map[int(h_str)] = h_info.get("status", "present")
        gaps = gap_envelopes.get(stream_name, [])
        results.append((stream_name, hour_map, gaps))

    return results
```

Then in `analyze_archive()`, add sealed archive detection in the stream-scanning loop. After the line that builds `all_date_names` from directories and consolidated files, also check for sealed archives at the symbol level:

In the section where dates are discovered (where we check for consolidated daily files and directories), add sealed archive detection. Before the `# Apply date filters` section, add:

```python
                # Sealed archives at symbol level: {symbol}/{date}.tar.zst
                for f in sym_dir.iterdir():
                    if f.is_file() and f.name.endswith(".tar.zst"):
                        name = f.name[:-len(".tar.zst")]
                        if len(name) == 10 and name[4] == "-" and name[7] == "-":
                            sealed_dates.add(name)
```

And after date filtering, handle sealed dates by reading from the archive:

```python
                # Process sealed dates
                for date_name in sorted(sealed_dates):
                    if date and date_name != date:
                        continue
                    if date_from and date_name < date_from:
                        continue
                    if date_to and date_name > date_to:
                        continue
                    archive_path = sym_dir / f"{date_name}.tar.zst"
                    sealed_data = _read_sealed_archive(archive_path, date_name)
                    for s_stream, s_hours, s_gaps in sealed_data:
                        if stream and s_stream != stream:
                            continue
                        covered = len(s_hours)
                        report.setdefault(exch_name, {})
                        report[exch_name].setdefault(sym_name, {})
                        report[exch_name][sym_name].setdefault(s_stream, {})
                        report[exch_name][sym_name][s_stream][date_name] = {
                            "hours": s_hours,
                            "gaps": s_gaps,
                            "covered": covered,
                            "total": 24,
                            "expect_from": min(s_hours.keys()) if s_hours else 0,
                            "expect_to": max(s_hours.keys()) if s_hours else 23,
                        }
```

NOTE: The exact integration point in `analyze_archive` needs careful placement. The sealed date handling should be done at the **symbol level** (before the stream loop), since sealed archives contain ALL streams. Read the sealed archive, then inject its data into the report for each stream found inside.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_sealed_skip.py -v`
Expected: 3 passed

- [ ] **Step 5: Run all tests**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/unit/test_infrastructure_assets.py -q`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_sealed_skip.py
git commit -m "feat(seal): analyze reads manifests and gaps from sealed archives"
```

---

### Task 6: Full verification

- [ ] **Step 1: Run all tests**

Run: `.venv/bin/python -m pytest tests/ --ignore=tests/unit/test_infrastructure_assets.py -v`
Expected: All pass

- [ ] **Step 2: Test the full pipeline manually**

```bash
# Create hourly test data, consolidate, and verify sealing
TMPDIR=$(mktemp -d)
.venv/bin/python -c "
import zstandard, orjson
from pathlib import Path
from datetime import datetime, timezone
from src.writer.file_rotator import write_sha256_sidecar, sidecar_path

base = Path('$TMPDIR')
day_start = int(datetime(2026, 4, 2, tzinfo=timezone.utc).timestamp() * 1000)
for stream in ['trades', 'depth']:
    for h in range(24):
        d = base / 'binance' / 'btcusdt' / stream / '2026-04-02'
        d.mkdir(parents=True, exist_ok=True)
        env = {'v':1,'type':'data','exchange':'binance','symbol':'btcusdt','stream':stream,
               'received_at':(day_start+h*3600000)*1000000,'exchange_ts':day_start+h*3600000,
               'collector_session_id':'test','session_seq':0,
               'raw_text':'{\"a\":'+str(h)+'}','raw_sha256':'abc',
               '_topic':'t','_partition':0,'_offset':0}
        cctx = zstandard.ZstdCompressor()
        p = d / f'hour-{h}.jsonl.zst'
        p.write_bytes(cctx.compress(orjson.dumps(env)))
        write_sha256_sidecar(p, sidecar_path(p))
"

# Consolidate + seal
.venv/bin/python -m src.cli.consolidate run --base-dir "$TMPDIR" --symbol btcusdt --date 2026-04-02

# Verify archive exists
ls -lh "$TMPDIR/binance/btcusdt/2026-04-02.tar.zst"

# Verify analyze works on sealed date
.venv/bin/python -m src.cli.gaps analyze --base-dir "$TMPDIR" --exchange binance --symbol btcusdt --date 2026-04-02

# Verify integrity skips sealed date
.venv/bin/python -m src.cli.integrity --base-dir "$TMPDIR" --exchange binance --symbol btcusdt --date 2026-04-02

rm -rf "$TMPDIR"
```

- [ ] **Step 3: Test default date behavior**

```bash
# Should default to today
.venv/bin/python -m src.cli.gaps analyze --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt

# Should fail with range error
.venv/bin/python -m src.cli.gaps analyze --base-dir /Users/vasyl.vaskovskyi/data/archive --date-from 2026-04-01 --date-to 2026-04-10
```
