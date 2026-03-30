# In-File Gap Backfill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend backfill to fill gaps within existing archive files using Binance historical API — ID-based for trades, time-based for funding_rate/liquidations/open_interest.

**Architecture:** The integrity checker produces structured gap ranges (trade ID gaps). Gap envelope scanning produces time windows. The backfill command's `--deep` flag runs both, fetches missing data, and writes backfill files.

**Tech Stack:** Python 3, click, aiohttp, zstandard, orjson, pytest

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/cli/integrity.py` | Add `find_backfillable_gaps()` — returns structured trade ID gap ranges |
| `src/cli/gaps.py` | Add `find_time_based_gaps()`, `_fetch_by_id()`, extend `backfill` with `--deep` |
| `src/cli/backfill_scheduler.py` | Pass `deep=True` to backfill cycle |
| `tests/unit/test_integrity_gaps.py` | Tests for `find_backfillable_gaps()` |
| `tests/unit/test_deep_backfill.py` | Tests for `--deep` flag and time-based gaps |

---

### Task 1: Add `find_backfillable_gaps()` to integrity checker

**Files:**
- Modify: `src/cli/integrity.py`
- Create: `tests/unit/test_integrity_gaps.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_integrity_gaps.py`:

```python
from pathlib import Path
import zstandard
import orjson
from src.cli.integrity import find_backfillable_gaps

def _write_hour_file(base: Path, exchange: str, symbol: str, stream: str,
                     date: str, hour: int, envelopes: list[dict]) -> None:
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_trade_env(a: int, session_seq: int = 0) -> dict:
    raw = {"e": "aggTrade", "a": a, "s": "BTCUSDT", "p": "67000", "q": "0.1",
           "T": 1774900000000, "m": True}
    raw_text = orjson.dumps(raw).decode()
    import hashlib
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1774900000000000000,
        "exchange_ts": 1774900000000,
        "collector_session_id": "test", "session_seq": session_seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "binance.trades", "_partition": 0, "_offset": session_seq,
    }

def test_find_backfillable_gaps_detects_trade_id_gap(tmp_path):
    # Records with a gap: trade IDs 100,101,102 then 110,111
    envs = [_make_trade_env(a=100, session_seq=0),
            _make_trade_env(a=101, session_seq=1),
            _make_trade_env(a=102, session_seq=2),
            _make_trade_env(a=110, session_seq=3),
            _make_trade_env(a=111, session_seq=4)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 1
    assert gaps[0]["type"] == "id_gap"
    assert gaps[0]["stream"] == "trades"
    assert gaps[0]["from_id"] == 103
    assert gaps[0]["to_id"] == 109
    assert gaps[0]["missing"] == 7

def test_find_backfillable_gaps_no_gaps(tmp_path):
    envs = [_make_trade_env(a=100+i, session_seq=i) for i in range(5)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0

def test_find_backfillable_gaps_ignores_depth(tmp_path):
    # Depth breaks should not be returned (not backfillable)
    envs = [_make_trade_env(a=100, session_seq=0)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "depth", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_integrity_gaps.py -v`
Expected: FAIL — `ImportError: cannot import name 'find_backfillable_gaps'`

- [ ] **Step 3: Implement `find_backfillable_gaps()`**

Add to `src/cli/integrity.py` after `check_integrity()`:

```python
def find_backfillable_gaps(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Find trade ID gaps suitable for backfill via fromId API.

    Runs the trades integrity checker and converts breaks into structured
    gap ranges with from_id/to_id for exact API fetching.
    Only returns gaps for the 'trades' stream (depth/bookticker are not backfillable).
    """
    report = check_integrity(
        base_dir,
        exchange=exchange,
        symbol=symbol,
        stream="trades",
        date=date,
        date_from=date_from,
        date_to=date_to,
    )
    gaps: list[dict] = []
    for (exch, sym, stream_name, date_name), info in report.items():
        for b in info["breaks"]:
            if b["field"] != "a" or b["missing"] is None or b["missing"] <= 0:
                continue
            # Determine which hour this gap falls in from received_at
            hour = 0
            if b["at_received"]:
                from datetime import datetime, timezone
                try:
                    dt = datetime.fromtimestamp(b["at_received"] / 1_000_000_000, tz=timezone.utc)
                    hour = dt.hour
                except (ValueError, OSError):
                    pass
            gaps.append({
                "type": "id_gap",
                "exchange": exch,
                "symbol": sym,
                "stream": stream_name,
                "date": date_name,
                "hour": hour,
                "from_id": b["expected"],        # first missing ID
                "to_id": b["actual"] - 1,        # last missing ID
                "missing": b["missing"],
            })
    return gaps
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_integrity_gaps.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cli/integrity.py tests/unit/test_integrity_gaps.py
git commit -m "feat: add find_backfillable_gaps() for trade ID gap detection

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Add `find_time_based_gaps()` to gaps.py

**Files:**
- Modify: `src/cli/gaps.py`
- Create: `tests/unit/test_deep_backfill.py`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_deep_backfill.py`:

```python
from pathlib import Path
import hashlib
import zstandard
import orjson
from src.cli.gaps import find_time_based_gaps

def _write_hour_file(base: Path, exchange: str, symbol: str, stream: str,
                     date: str, hour: int, envelopes: list[dict]) -> None:
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_gap_env(stream: str, gap_start_ns: int, gap_end_ns: int) -> dict:
    return {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": stream, "received_at": gap_end_ns,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": gap_start_ns, "gap_end_ts": gap_end_ns,
        "reason": "ws_disconnect", "detail": "WebSocket disconnected",
        "_topic": f"binance.{stream}", "_partition": 0, "_offset": -1,
    }

def _make_data_env(stream: str) -> dict:
    raw_text = "{}"
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": stream, "received_at": 1000, "exchange_ts": 999,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": f"binance.{stream}", "_partition": 0, "_offset": 0,
    }

def test_find_time_based_gaps_funding_rate(tmp_path):
    gap = _make_gap_env("funding_rate", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("funding_rate")
    _write_hour_file(tmp_path, "binance", "btcusdt", "funding_rate", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 1
    assert gaps[0]["type"] == "time_gap"
    assert gaps[0]["stream"] == "funding_rate"
    assert gaps[0]["start_ms"] == 1774900000000  # ns -> ms
    assert gaps[0]["end_ms"] == 1774900060000

def test_find_time_based_gaps_ignores_trades(tmp_path):
    # Trades use ID-based backfill, not time-based
    gap = _make_gap_env("trades", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("trades")
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0

def test_find_time_based_gaps_ignores_depth(tmp_path):
    gap = _make_gap_env("depth", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("depth")
    _write_hour_file(tmp_path, "binance", "btcusdt", "depth", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_deep_backfill.py -v`
Expected: FAIL — `ImportError: cannot import name 'find_time_based_gaps'`

- [ ] **Step 3: Implement `find_time_based_gaps()`**

Add to `src/cli/gaps.py` after `_scan_gaps()`:

```python
TIME_BASED_BACKFILL_STREAMS = frozenset({"funding_rate", "liquidations", "open_interest"})


def find_time_based_gaps(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Find ws_disconnect gap envelopes in time-based backfillable streams.

    Scans funding_rate, liquidations, open_interest for gap records and
    returns time windows suitable for startTime/endTime API fetching.
    Trades are excluded (use find_backfillable_gaps for ID-based backfill).
    Depth/bookticker are excluded (no historical API).
    """
    gaps: list[dict] = []

    for exch_dir in sorted(base_dir.iterdir()):
        if not exch_dir.is_dir():
            continue
        if exchange and exch_dir.name != exchange:
            continue
        for sym_dir in sorted(exch_dir.iterdir()):
            if not sym_dir.is_dir():
                continue
            if symbol and sym_dir.name != symbol:
                continue
            for stream_dir in sorted(sym_dir.iterdir()):
                if not stream_dir.is_dir():
                    continue
                if stream_dir.name not in TIME_BASED_BACKFILL_STREAMS:
                    continue
                for date_dir in sorted(stream_dir.iterdir()):
                    if not date_dir.is_dir():
                        continue
                    date_name = date_dir.name
                    if date and date_name != date:
                        continue
                    if date_from and date_name < date_from:
                        continue
                    if date_to and date_name > date_to:
                        continue

                    gap_envs = _scan_gaps(date_dir)
                    for g in gap_envs:
                        start_ns = g.get("gap_start_ts", 0)
                        end_ns = g.get("gap_end_ts", 0)
                        if end_ns <= start_ns:
                            continue
                        from datetime import datetime, timezone
                        try:
                            dt = datetime.fromtimestamp(
                                start_ns / 1_000_000_000, tz=timezone.utc)
                            hour = dt.hour
                        except (ValueError, OSError):
                            hour = 0
                        gaps.append({
                            "type": "time_gap",
                            "exchange": exch_dir.name,
                            "symbol": sym_dir.name,
                            "stream": stream_dir.name,
                            "date": date_name,
                            "hour": hour,
                            "start_ms": start_ns // 1_000_000,
                            "end_ms": end_ns // 1_000_000,
                        })
    return gaps
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_deep_backfill.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_deep_backfill.py
git commit -m "feat: add find_time_based_gaps() for funding/liquidations/OI gap detection

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Add ID-based fetch function and `--deep` flag to backfill

**Files:**
- Modify: `src/cli/gaps.py`
- Modify: `tests/unit/test_deep_backfill.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_deep_backfill.py`:

```python
from click.testing import CliRunner
from src.cli.gaps import cli

def test_deep_dry_run_shows_id_gaps(tmp_path):
    # Create trades with ID gap
    envs = [_make_trade_env(100, 0), _make_trade_env(101, 1),
            _make_trade_env(110, 2), _make_trade_env(111, 3)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--deep", "--dry-run"])
    assert result.exit_code == 0
    assert "id_gap" in result.output or "trades" in result.output
    assert "102" in result.output or "109" in result.output

def _make_trade_env(a: int, seq: int) -> dict:
    raw = {"e": "aggTrade", "a": a, "s": "BTCUSDT", "p": "67000", "q": "0.1",
           "T": 1774900000000, "m": True}
    raw_text = orjson.dumps(raw).decode()
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1774900000000000000,
        "exchange_ts": 1774900000000,
        "collector_session_id": "test", "session_seq": seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "binance.trades", "_partition": 0, "_offset": seq,
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_deep_backfill.py::test_deep_dry_run_shows_id_gaps -v`
Expected: FAIL — `--deep` option not recognized

- [ ] **Step 3: Add `_fetch_by_id()` function**

Add to `src/cli/gaps.py` after `_fetch_historical_all()`:

```python
async def _fetch_by_id(
    adapter: BinanceAdapter,
    symbol: str,
    from_id: int,
    to_id: int,
) -> list[dict]:
    """Fetch trades by aggregate trade ID range using fromId pagination."""
    all_records: list[dict] = []
    current_id = from_id

    async with aiohttp.ClientSession() as session:
        while current_id <= to_id:
            url = adapter.build_historical_trades_url(symbol, from_id=current_id, limit=1000)
            page = await _fetch_historical_page(session, url)
            if not page:
                break

            for rec in page:
                if rec.get("a", 0) > to_id:
                    break
                all_records.append(rec)
            else:
                # All records in page were within range, continue
                last_a = page[-1].get("a", 0)
                if last_a >= to_id or len(page) < 1000:
                    break
                current_id = last_a + 1
                continue
            break  # Broke out of inner loop (hit to_id)

    return all_records
```

- [ ] **Step 4: Add `--deep` flag to backfill command**

In `src/cli/gaps.py`, find the `@cli.command()` decorator for `backfill` and add the option. Then extend the backfill function body.

Add the click option after existing options:
```python
@click.option("--deep", is_flag=True, help="Also backfill gaps within existing files (runs integrity checker)")
```

Add to the backfill function signature:
```python
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, base_dir, deep):
```

After the existing missing-hours backfill section (after the `total_written` loop), add:

```python
    # ── Deep backfill: gaps within existing files ──
    if deep:
        from src.cli.integrity import find_backfillable_gaps

        # 1. ID-based gaps (trades)
        id_gaps = find_backfillable_gaps(
            base, exchange=exchange, symbol=symbol,
            date=date, date_from=date_from, date_to=date_to,
        )

        # 2. Time-based gaps (funding_rate, liquidations, open_interest)
        time_gaps = find_time_based_gaps(
            base, exchange=exchange, symbol=symbol,
            date=date, date_from=date_from, date_to=date_to,
        )

        all_deep_gaps = id_gaps + time_gaps

        if all_deep_gaps and dry_run:
            click.echo(f"\nDeep scan: {len(id_gaps)} ID gap(s), {len(time_gaps)} time gap(s):")
            for g in all_deep_gaps:
                if g["type"] == "id_gap":
                    click.echo(f"  {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']} "
                               f"hour={g['hour']} fromId={g['from_id']} toId={g['to_id']} missing={g['missing']}")
                else:
                    click.echo(f"  {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']} "
                               f"hour={g['hour']} {g['start_ms']}-{g['end_ms']}ms")
            return

        if all_deep_gaps and not dry_run:
            if not adapter:
                adapter = BinanceAdapter(ws_base="wss://fstream.binance.com", rest_base=BINANCE_REST_BASE)
            if not session_id:
                session_id = str(uuid.uuid4())

            for g in id_gaps:
                click.echo(f"Fetching {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']} "
                           f"fromId={g['from_id']} toId={g['to_id']}...")
                try:
                    records = asyncio.run(_fetch_by_id(adapter, g["symbol"], g["from_id"], g["to_id"]))
                except Exception as exc:
                    click.echo(f"  ERROR: {exc}")
                    continue
                if records:
                    backfill_seq = _next_backfill_seq(base, g["exchange"], g["symbol"],
                                                      g["stream"], g["date"], g["hour"])
                    n, _ = _write_backfill_files(
                        records, base_dir=str(base), exchange=g["exchange"],
                        symbol=g["symbol"], stream=g["stream"], date=g["date"],
                        session_id=session_id, seq_offset=total_written,
                        backfill_seq=backfill_seq,
                    )
                    total_written += n
                    click.echo(f"  Wrote {n} records")

            for g in time_gaps:
                click.echo(f"Fetching {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']} "
                           f"{g['start_ms']}-{g['end_ms']}ms...")
                try:
                    records = asyncio.run(
                        _fetch_historical_all(adapter, g["symbol"], g["stream"],
                                              g["start_ms"], g["end_ms"]))
                except Exception as exc:
                    click.echo(f"  ERROR: {exc}")
                    continue
                if records:
                    backfill_seq = _next_backfill_seq(base, g["exchange"], g["symbol"],
                                                      g["stream"], g["date"], g["hour"])
                    n, _ = _write_backfill_files(
                        records, base_dir=str(base), exchange=g["exchange"],
                        symbol=g["symbol"], stream=g["stream"], date=g["date"],
                        session_id=session_id, seq_offset=total_written,
                        backfill_seq=backfill_seq,
                    )
                    total_written += n
                    click.echo(f"  Wrote {n} records")

        elif not all_deep_gaps:
            click.echo("Deep scan: no in-file gaps found.")

    if total_written > 0:
        click.echo(f"\nBackfill complete: {total_written} total records written.")
```

Note: you'll need to hoist `adapter`, `session_id`, and `total_written` to be accessible in both the missing-hours section and the deep section. Check the existing code structure and adjust variable scoping.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/unit/test_deep_backfill.py -v`
Expected: All PASS

- [ ] **Step 5: Run full test suite**

Run: `pytest tests/unit/ -v --tb=short`
Expected: All pass

- [ ] **Step 6: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_deep_backfill.py
git commit -m "feat: add --deep flag to backfill for in-file gap recovery

ID-based backfill for trades (fromId), time-based for funding_rate,
liquidations, open_interest. Runs integrity checker + gap envelope
scan to find gaps within existing files.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Update backfill scheduler to use `--deep`

**Files:**
- Modify: `src/cli/backfill_scheduler.py`

- [ ] **Step 1: Add deep backfill to the scheduler cycle**

In `src/cli/backfill_scheduler.py`, update `_run_backfill_cycle()` to also run deep backfill after the missing-hours pass. Add after the existing missing-hours loop:

```python
        # ── Deep backfill: in-file gaps ──
        from src.cli.integrity import find_backfillable_gaps
        from src.cli.gaps import find_time_based_gaps, _fetch_by_id

        id_gaps = find_backfillable_gaps(Path(base_dir))
        time_gaps = find_time_based_gaps(Path(base_dir))
        deep_gaps = id_gaps + time_gaps
        backfill_gaps_found.set(len(missing) + len(deep_gaps))

        if deep_gaps:
            logger.info("backfill_deep_starting", id_gaps=len(id_gaps), time_gaps=len(time_gaps))

            if not adapter:
                adapter = BinanceAdapter(
                    ws_base="wss://fstream.binance.com",
                    rest_base="https://fapi.binance.com",
                    symbols=[],
                )
            if not session_id:
                session_id = f"backfill-{datetime.now(timezone.utc).isoformat()}"

            for g in id_gaps:
                try:
                    records = await _fetch_by_id(adapter, g["symbol"], g["from_id"], g["to_id"])
                    if records:
                        backfill_seq = _next_backfill_seq(
                            Path(base_dir), g["exchange"], g["symbol"],
                            g["stream"], g["date"], g["hour"])
                        n, _ = _write_backfill_files(
                            records, base_dir=base_dir, exchange=g["exchange"],
                            symbol=g["symbol"], stream=g["stream"], date=g["date"],
                            session_id=session_id, seq_offset=total_written,
                            backfill_seq=backfill_seq,
                        )
                        total_written += n
                        logger.info("backfill_id_gap_done",
                                    exchange=g["exchange"], symbol=g["symbol"],
                                    stream=g["stream"], records=n)
                except Exception as e:
                    logger.error("backfill_id_gap_failed", error=str(e),
                                 exchange=g["exchange"], symbol=g["symbol"])
                    success = False

            for g in time_gaps:
                try:
                    records = await _fetch_historical_all(
                        adapter, g["symbol"], g["stream"], g["start_ms"], g["end_ms"])
                    if records:
                        backfill_seq = _next_backfill_seq(
                            Path(base_dir), g["exchange"], g["symbol"],
                            g["stream"], g["date"], g["hour"])
                        n, _ = _write_backfill_files(
                            records, base_dir=base_dir, exchange=g["exchange"],
                            symbol=g["symbol"], stream=g["stream"], date=g["date"],
                            session_id=session_id, seq_offset=total_written,
                            backfill_seq=backfill_seq,
                        )
                        total_written += n
                        logger.info("backfill_time_gap_done",
                                    exchange=g["exchange"], symbol=g["symbol"],
                                    stream=g["stream"], records=n)
                except Exception as e:
                    logger.error("backfill_time_gap_failed", error=str(e),
                                 exchange=g["exchange"], symbol=g["symbol"])
                    success = False
```

Note: ensure `adapter` and `session_id` variables are hoisted so they're shared between the missing-hours section and the deep section. Initialize them as `None` at the top and create on first use.

- [ ] **Step 2: Run full test suite**

Run: `pytest tests/unit/ -v --tb=short`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add src/cli/backfill_scheduler.py
git commit -m "feat: backfill scheduler runs deep backfill by default

Scheduler now also runs integrity checker for trade ID gaps and scans
gap envelopes for time-based gaps after the missing-hours pass.

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Integration test against real archive

- [ ] **Step 1: Run deep backfill dry run against real archive**

```bash
.venv/bin/python -m src.cli.gaps backfill --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt --deep --dry-run
```

Verify it finds the 19,025 missing trades from the integrity checker.

- [ ] **Step 2: Run full test suite**

```bash
.venv/bin/python -m pytest tests/unit/ -v --tb=short
```

All tests must pass.

- [ ] **Step 3: Commit any remaining fixes**

```bash
git add -A
git commit -m "test: verify deep backfill against real archive

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```
