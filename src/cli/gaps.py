from __future__ import annotations

import asyncio
import hashlib
import io
import json
import time
import uuid
from pathlib import Path

import aiohttp
import click
import orjson
import zstandard as zstd

from src.common.config import default_archive_dir
from src.exchanges.binance import BinanceAdapter
from src.writer.file_rotator import build_backfill_file_path, compute_sha256, sidecar_path

DEFAULT_ARCHIVE_DIR = default_archive_dir()

BACKFILLABLE_STREAMS = frozenset({"trades", "funding_rate", "liquidations", "open_interest"})
NON_BACKFILLABLE_STREAMS = frozenset({"depth", "depth_snapshot", "bookticker"})

STREAM_TS_KEYS = {
    "trades": "T",
    "funding_rate": "E",
    "liquidations": "time",
    "open_interest": "timestamp",
}

ENDPOINT_WEIGHTS = {
    "trades": 20,
    "funding_rate": 1,
    "liquidations": 21,
    "open_interest": 1,
}

BINANCE_REST_BASE = "https://fapi.binance.com"


def _hour_to_ms_range(date: str, hour: int) -> tuple[int, int]:
    from datetime import datetime, timezone, timedelta
    dt_start = datetime.strptime(f"{date} {hour:02d}:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
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
    raw_text = orjson.dumps(raw_record).decode()
    return {
        "v": 1,
        "type": "data",
        "source": "backfill",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": time.time_ns(),
        "exchange_ts": raw_record.get(exchange_ts_key, 0),
        "collector_session_id": session_id,
        "session_seq": seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "backfill",
        "_partition": 0,
        "_offset": seq,
    }


async def _fetch_historical_page(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int = 5,
) -> list[dict]:
    for attempt in range(max_retries + 1):
        async with session.get(url) as resp:
            if resp.status in (429, 418) and attempt < max_retries:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                await asyncio.sleep(retry_after)
                continue
            resp.raise_for_status()
            return await resp.json(content_type=None)


def _compute_next_funding_time(event_time_ms: int) -> int:
    from datetime import datetime, timezone, timedelta
    dt = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    if hour < 8:
        next_dt = dt.replace(hour=8, minute=0, second=0, microsecond=0)
    elif hour < 16:
        next_dt = dt.replace(hour=16, minute=0, second=0, microsecond=0)
    else:
        next_day = (dt + timedelta(days=1))
        next_dt = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(next_dt.timestamp() * 1000)


def _build_mark_price_update(symbol, mark_kline, index_kline, premium_kline, funding_rate):
    event_time = mark_kline[6] + 1
    return {
        "e": "markPriceUpdate",
        "E": event_time,
        "s": symbol,
        "p": mark_kline[4],
        "ap": mark_kline[4],
        "P": premium_kline[4],
        "i": index_kline[4],
        "r": funding_rate,
        "T": _compute_next_funding_time(event_time),
    }


async def _fetch_funding_rate_composite(adapter, symbol, start_ms, end_ms):
    async with aiohttp.ClientSession() as session:
        mark_klines = []
        index_klines = []
        premium_klines = []

        current = start_ms
        while current <= end_ms:
            mark_url = adapter.build_mark_price_klines_url(symbol, start_time=current, end_time=end_ms)
            index_url = adapter.build_index_price_klines_url(symbol, start_time=current, end_time=end_ms)
            premium_url = adapter.build_premium_index_klines_url(symbol, start_time=current, end_time=end_ms)

            mark_page, index_page, premium_page = await asyncio.gather(
                _fetch_historical_page(session, mark_url),
                _fetch_historical_page(session, index_url),
                _fetch_historical_page(session, premium_url),
            )

            if not mark_page:
                break

            mark_klines.extend(mark_page)
            index_klines.extend(index_page or [])
            premium_klines.extend(premium_page or [])

            last_open = mark_page[-1][0]
            if last_open >= end_ms or len(mark_page) < 1000:
                break
            current = last_open + 60000

        if not mark_klines:
            return []

        funding_url = adapter.build_historical_funding_url(symbol, start_time=start_ms, end_time=end_ms)
        funding_records = await _fetch_historical_page(session, funding_url) or []

        funding_rates = sorted([(fr["fundingTime"], fr["fundingRate"]) for fr in funding_records])

        def _get_rate(event_ms):
            rate = "0"
            for ft, fr in funding_rates:
                if ft <= event_ms:
                    rate = fr
                else:
                    break
            return rate

        index_by_time = {k[0]: k for k in index_klines}
        premium_by_time = {k[0]: k for k in premium_klines}

        records = []
        for mk in mark_klines:
            open_time = mk[0]
            ik = index_by_time.get(open_time, mk)
            pk = premium_by_time.get(open_time, [open_time, "0", "0", "0", "0"])
            rate = _get_rate(mk[6] + 1)
            records.append(_build_mark_price_update(symbol.upper(), mk, ik, pk, rate))

    return records


async def _fetch_historical_all(
    adapter: BinanceAdapter,
    symbol: str,
    stream: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """Fetch all records for the given stream/symbol in [start_ms, end_ms] with pagination."""
    if stream == "funding_rate":
        return await _fetch_funding_rate_composite(adapter, symbol, start_ms, end_ms)

    ts_key = STREAM_TS_KEYS[stream]
    all_records: list[dict] = []

    async with aiohttp.ClientSession() as session:
        current_start = start_ms
        while current_start <= end_ms:
            if stream == "trades":
                url = adapter.build_historical_trades_url(
                    symbol, start_time=current_start, end_time=end_ms, limit=1000
                )
            elif stream == "liquidations":
                url = adapter.build_historical_liquidations_url(
                    symbol, start_time=current_start, end_time=end_ms, limit=1000
                )
            elif stream == "open_interest":
                url = adapter.build_historical_open_interest_url(
                    symbol, start_time=current_start, end_time=end_ms, limit=500
                )
            else:
                break

            page = await _fetch_historical_page(session, url)
            if not page:
                break

            all_records.extend(page)

            # Advance pagination cursor using the last record's timestamp
            last_ts = page[-1].get(ts_key, 0)
            if last_ts is None or last_ts >= end_ms or len(page) < (500 if stream == "open_interest" else 1000):
                break
            current_start = last_ts + 1

    return all_records


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
            last_a = page[-1].get("a", 0)
            if last_a >= to_id or len(page) < 1000:
                break
            current_id = last_a + 1

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
    seq_offset: int,
    backfill_seq: int,
) -> tuple[Path, int]:
    """Wrap records as envelopes and write a backfill file. Returns (file_path, records_written)."""
    ts_key = STREAM_TS_KEYS[stream]
    envelopes = []
    for i, record in enumerate(records):
        env = _wrap_backfill_envelope(
            record,
            exchange=exchange,
            symbol=symbol,
            stream=stream,
            session_id=session_id,
            seq=seq_offset + i,
            exchange_ts_key=ts_key,
        )
        envelopes.append(orjson.dumps(env))

    if not envelopes:
        return None, 0

    out_path = build_backfill_file_path(base_dir, exchange, symbol, stream, date, _parse_hour_from_date(date, records[0].get(ts_key, 0)), backfill_seq)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    cctx = zstd.ZstdCompressor()
    raw = b"\n".join(envelopes)
    out_path.write_bytes(cctx.compress(raw))

    sc = sidecar_path(out_path)
    sc.write_text(f"{compute_sha256(out_path)}  {out_path.name}\n")

    return out_path, len(envelopes)


def _parse_hour_from_date(date: str, ts_ms: int) -> int:
    """Derive the hour from a millisecond timestamp."""
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.hour


def _decompress_and_parse(file_path: Path) -> list[dict]:
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as fh:
        data = dctx.stream_reader(fh).read()
    result = []
    for line in data.strip().split(b"\n"):
        if line:
            result.append(orjson.loads(line))
    return result


def _collect_missing_hours(report: dict) -> list[tuple[str, str, str, str, int]]:
    """Extract (exchange, symbol, stream, date, hour) tuples for missing hours in backfillable streams."""
    missing = []
    for exch_name, symbols in report.items():
        for sym_name, streams in symbols.items():
            for stream_name, dates in streams.items():
                if stream_name not in BACKFILLABLE_STREAMS:
                    continue
                for date_name, info in dates.items():
                    hour_map = info["hours"]
                    for h in range(24):
                        if h not in hour_map:
                            missing.append((exch_name, sym_name, stream_name, date_name, h))
    return missing


def _next_backfill_seq(base_dir: Path, exchange: str, symbol: str, stream: str, date: str, hour: int) -> int:
    """Find the next available backfill sequence number for a given hour."""
    date_dir = base_dir / exchange / symbol / stream / date
    if not date_dir.exists():
        return 1
    existing = []
    for f in date_dir.iterdir():
        parsed = _classify_hour_file(f.name)
        if parsed is None:
            continue
        h, status = parsed
        if h == hour and status == "backfilled":
            # Extract the seq number from the filename
            stem = f.name[: -len(".jsonl.zst")]
            parts = stem.split(".", 1)
            if len(parts) == 2 and parts[1].startswith("backfill-"):
                try:
                    seq = int(parts[1][len("backfill-"):])
                    existing.append(seq)
                except ValueError:
                    pass
    return max(existing, default=0) + 1


def _classify_hour_file(filename: str) -> tuple[int, str] | None:
    """Parse a filename and return (hour, status) or None if not an hour file.

    Status is one of: 'present', 'backfilled', 'late'.
    """
    # Expected patterns:
    #   hour-N.jsonl.zst          -> present
    #   hour-N.backfill-M.jsonl.zst -> backfilled
    #   hour-N.late-M.jsonl.zst    -> late
    if not filename.endswith(".jsonl.zst"):
        return None
    # Strip the .jsonl.zst suffix — we work on the stem parts
    stem = filename[: -len(".jsonl.zst")]  # e.g. "hour-5" or "hour-5.backfill-1"
    parts = stem.split(".", 1)  # split at first dot only
    hour_part = parts[0]  # "hour-N"
    if not hour_part.startswith("hour-"):
        return None
    try:
        hour = int(hour_part[len("hour-"):])
    except ValueError:
        return None

    if len(parts) == 1:
        status = "present"
    elif parts[1].startswith("backfill-"):
        status = "backfilled"
    elif parts[1].startswith("late-"):
        status = "late"
    else:
        return None

    return hour, status


def _scan_hours(date_dir: Path) -> dict[int, str]:
    """Return a mapping of hour -> status for all hour files in date_dir.

    Status precedence when multiple files exist for the same hour:
    present > backfilled > late
    (first-seen wins within same priority level)
    """
    priority = {"present": 0, "backfilled": 1, "late": 2}
    result: dict[int, str] = {}
    if not date_dir.exists():
        return result
    for f in date_dir.iterdir():
        parsed = _classify_hour_file(f.name)
        if parsed is None:
            continue
        hour, status = parsed
        if hour not in result or priority[status] < priority[result[hour]]:
            result[hour] = status
    return result


def _scan_gaps(date_dir: Path) -> list[dict]:
    """Read .jsonl.zst files in date_dir and return gap envelopes only.

    Lightweight scan — uses streaming decompression and skips lines that
    don't contain '"gap"'. Does NOT compute _records_missed (that's the
    integrity checker's job).
    """
    if not date_dir.exists():
        return []

    gaps: list[dict] = []
    for f in sorted(date_dir.glob("*.jsonl.zst")):
        try:
            dctx = zstd.ZstdDecompressor()
            with open(f, "rb") as fh:
                reader = dctx.stream_reader(fh)
                text_reader = io.TextIOWrapper(reader, encoding="utf-8")
                for line in text_reader:
                    if '"gap"' not in line:
                        continue
                    env = orjson.loads(line)
                    if env.get("type") == "gap":
                        gaps.append(env)
        except Exception:
            pass

    return gaps


TIME_BASED_BACKFILL_STREAMS = frozenset({"funding_rate", "liquidations", "open_interest"})


def find_time_based_gaps(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Find ws_disconnect gap envelopes in time-based backfillable streams."""
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
                            dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
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


def _format_duration(ns: int) -> str:
    """Format a nanosecond duration into human-readable form.

    <60s → "42s", <60m → "3m12s", <24h → "2h14m3s", >=24h → "1d2h14m3s".
    Zero or negative → "instant".
    """
    if ns <= 0:
        return "0ms"
    total_seconds = int(ns / 1_000_000_000)
    if total_seconds == 0:
        ms = int(ns / 1_000_000)
        return f"{ms}ms"
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days > 0:
        return f"{days}d{hours}h{minutes}m{seconds}s"
    if hours > 0:
        return f"{hours}h{minutes}m{seconds}s"
    if minutes > 0:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"


def _ns_to_time(ns: int) -> str:
    """Format nanosecond timestamp to HH:mm:ss UTC."""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return "??:??:??"


def _records_missed(gap: dict) -> str:
    """Return records missed count as a string.

    Parses the count from the gap envelope's detail field. Shows "?" for
    missing hour entries and gaps where the count cannot be determined.
    For exact record counts, use the integrity checker instead.
    """
    # Parse from detail
    import re
    detail = gap.get("detail", "")
    # session_seq_skip: "expected 100, got 105" → 5
    m = re.search(r"expected (\d+), got (\d+)", detail)
    if m:
        return str(int(m.group(2)) - int(m.group(1)))
    # buffer_overflow: "42 messages dropped" or "42 diffs dropped"
    m = re.search(r"(\d+) (?:messages|diffs) dropped", detail)
    if m:
        return m.group(1)
    # restart_gap/checkpoint_lost: "(3 records missed)"
    m = re.search(r"\((\d+) records missed\)", detail)
    if m:
        return m.group(1)
    return "?"


def _gap_status(gap: dict, stream_name: str) -> str:
    """Determine the status of a gap record."""
    if stream_name in NON_BACKFILLABLE_STREAMS:
        return "UNRECOVERABLE"
    # TODO: check for IN PROGRESS state when backfill tracking is added
    return "MISSING"


def _file_status_for_hour(hour: int, hour_map: dict[int, str], stream_name: str) -> str:
    """Determine file-level status for an hour."""
    status = hour_map.get(hour)
    if status is None:
        if stream_name in NON_BACKFILLABLE_STREAMS:
            return "UNRECOVERABLE"
        return "MISSING"
    if status == "backfilled":
        return "RECOVERED"
    # present or late — file exists
    return "OK"


def _format_hours_line(hours: dict[int, str], expect_from: int = 0, expect_to: int = 23) -> str:
    """Produce a compact single-line representation of hour coverage.

    Only shows hours within the expected recording window [expect_from, expect_to].
    Example: "15-23 OK" or "0-15 OK  16 MISSING  17 MISSING  18-23 OK"
    """
    STATUS_LABEL = {
        "present": "OK",
        "backfilled": "RECOVERED",
        "late": "OK",
        "missing": "MISSING",
    }

    hour_range = range(expect_from, expect_to + 1)
    if not hour_range:
        return "no hours expected"

    all_hours = [(h, hours.get(h, "missing")) for h in hour_range]

    segments: list[tuple[int, int, str]] = []
    run_start = all_hours[0][0]
    run_status = all_hours[0][1]
    for h, status in all_hours[1:]:
        if status != run_status:
            segments.append((run_start, h - 1, run_status))
            run_start = h
            run_status = status
    segments.append((run_start, expect_to, run_status))

    parts: list[str] = []
    for start, end, status in segments:
        label = STATUS_LABEL.get(status, status.upper())
        if start == end:
            parts.append(f"{start} {label}")
        else:
            parts.append(f"{start}-{end} {label}")
    return "  ".join(parts)


def analyze_archive(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    stream: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Scan base_dir and return a nested report dict.

    Structure: report[exchange][symbol][stream][date] = {
        "hours": {hour: status, ...},
        "gaps": [...],
        "covered": int,
        "total": 24,
    }
    """
    report: dict = {}

    exchanges_to_scan: list[Path] = []
    if exchange:
        exch_dir = base_dir / exchange
        if exch_dir.exists():
            exchanges_to_scan = [exch_dir]
    else:
        if base_dir.exists():
            exchanges_to_scan = [d for d in sorted(base_dir.iterdir()) if d.is_dir()]

    for exch_dir in exchanges_to_scan:
        exch_name = exch_dir.name

        symbols_to_scan: list[Path] = []
        if symbol:
            sym_dir = exch_dir / symbol
            if sym_dir.exists():
                symbols_to_scan = [sym_dir]
        else:
            symbols_to_scan = [d for d in sorted(exch_dir.iterdir()) if d.is_dir()]

        for sym_dir in symbols_to_scan:
            sym_name = sym_dir.name

            streams_to_scan: list[Path] = []
            if stream:
                str_dir = sym_dir / stream
                if str_dir.exists():
                    streams_to_scan = [str_dir]
            else:
                streams_to_scan = [d for d in sorted(sym_dir.iterdir()) if d.is_dir()]

            for stream_dir in streams_to_scan:
                stream_name = stream_dir.name

                dates_to_scan: list[Path] = []
                if date:
                    date_dir = stream_dir / date
                    if date_dir.exists():
                        dates_to_scan = [date_dir]
                    else:
                        # Still include it so we can report missing
                        dates_to_scan = [date_dir]
                elif date_from or date_to:
                    for d in sorted(stream_dir.iterdir()):
                        if not d.is_dir():
                            continue
                        name = d.name
                        if date_from and name < date_from:
                            continue
                        if date_to and name > date_to:
                            continue
                        dates_to_scan.append(d)
                else:
                    dates_to_scan = [d for d in sorted(stream_dir.iterdir()) if d.is_dir()]

                # First pass: collect all date/hour data
                all_dates_data: list[tuple[str, dict[int, str], list[dict]]] = []
                for date_dir in dates_to_scan:
                    date_name = date_dir.name
                    hour_map = _scan_hours(date_dir)
                    gap_envs = _scan_gaps(date_dir) if date_dir.exists() else []
                    all_dates_data.append((date_name, hour_map, gap_envs))

                # Determine the recording window for this stream
                first_hour: int | None = None
                first_date: str | None = None
                last_hour: int | None = None
                last_date: str | None = None
                for date_name, hour_map, _ in all_dates_data:
                    if not hour_map:
                        continue
                    min_h = min(hour_map.keys())
                    max_h = max(hour_map.keys())
                    if first_date is None or date_name < first_date or (date_name == first_date and min_h < first_hour):
                        first_date = date_name
                        first_hour = min_h
                    if last_date is None or date_name > last_date or (date_name == last_date and max_h > last_hour):
                        last_date = date_name
                        last_hour = max_h

                # Second pass: build report with correct expected hours
                for date_name, hour_map, gaps in all_dates_data:
                    covered = len(hour_map)

                    # Determine expected hour range for this date
                    expect_from = 0
                    expect_to = 23
                    if first_date is not None and date_name == first_date:
                        expect_from = first_hour
                    if last_date is not None and date_name == last_date:
                        expect_to = last_hour

                    expected = expect_to - expect_from + 1

                    report.setdefault(exch_name, {})
                    report[exch_name].setdefault(sym_name, {})
                    report[exch_name][sym_name].setdefault(stream_name, {})
                    report[exch_name][sym_name][stream_name][date_name] = {
                        "hours": hour_map,
                        "gaps": gaps,
                        "covered": covered,
                        "total": expected,
                        "expect_from": expect_from,
                        "expect_to": expect_to,
                    }

    return report


def _ns_to_hour_str(ns: int) -> str:
    """Extract hour from nanosecond timestamp."""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return str(dt.hour)
    except (ValueError, OSError, OverflowError):
        return "?"


def _print_report(report: dict) -> None:
    """Print a single unified table of all gaps across all streams."""
    rows: list[dict] = []
    grand_total_gaps = 0
    grand_restored = 0
    grand_unrecoverable = 0
    grand_remaining = 0
    grand_total_dur = 0

    for exch_name, symbols in sorted(report.items()):
        for sym_name, streams in sorted(symbols.items()):
            for stream_name, dates in sorted(streams.items()):
                for date_name, info in sorted(dates.items()):
                    hour_map = info["hours"]
                    gaps = info["gaps"]
                    expect_from = info.get("expect_from", 0)
                    expect_to = info.get("expect_to", 23)

                    # Build unified gap list
                    all_entries: list[dict] = []

                    for h in range(expect_from, expect_to + 1):
                        if h not in hour_map:
                            from datetime import datetime, timezone, timedelta
                            dt_start = datetime.strptime(
                                f"{date_name} {h:02d}:00:00", "%Y-%m-%d %H:%M:%S"
                            ).replace(tzinfo=timezone.utc)
                            dt_end = dt_start + timedelta(hours=1) - timedelta(milliseconds=1)
                            start_ns = int(dt_start.timestamp() * 1_000_000_000)
                            end_ns = int(dt_end.timestamp() * 1_000_000_000)
                            file_st = _file_status_for_hour(h, hour_map, stream_name)
                            all_entries.append({
                                "reason": "missing_hour",
                                "gap_start_ts": start_ns,
                                "gap_end_ts": end_ns,
                                "_status": file_st,
                                "_hour": str(h),
                            })

                    for g in gaps:
                        all_entries.append({
                            **g,
                            "_status": _gap_status(g, stream_name),
                            "_hour": _ns_to_hour_str(g.get("gap_start_ts", 0)),
                        })

                    all_entries.sort(key=lambda e: (e.get("gap_start_ts", 0), e.get("gap_end_ts", 0)))

                    if not all_entries:
                        rows.append({
                            "entity": stream_name, "date": date_name, "hour": "",
                            "reason": "", "start": "", "end": "",
                            "duration": "", "missed": "", "status": "OK",
                        })
                        continue

                    # Compute effective durations
                    high_water = 0
                    for entry in all_entries:
                        start_ts = entry.get("gap_start_ts", 0)
                        end_ts = entry.get("gap_end_ts", 0)
                        effective_start = max(start_ts, high_water)
                        entry["_dur_ns"] = max(0, end_ts - effective_start)
                        if end_ts > high_water:
                            high_water = end_ts

                    for entry in all_entries:
                        status = entry["_status"]
                        dur_ns = entry["_dur_ns"]
                        grand_total_gaps += 1
                        grand_total_dur += dur_ns
                        if status == "RECOVERED":
                            grand_restored += 1
                        elif status == "UNRECOVERABLE":
                            grand_unrecoverable += 1
                        else:
                            grand_remaining += 1

                        rows.append({
                            "entity": stream_name, "date": date_name,
                            "hour": entry.get("_hour", ""),
                            "reason": entry.get("reason", "unknown"),
                            "start": _ns_to_time(entry.get("gap_start_ts", 0)),
                            "end": _ns_to_time(entry.get("gap_end_ts", 0)),
                            "duration": _format_duration(dur_ns),
                            "missed": _records_missed(entry),
                            "status": status,
                        })

    # Sort descending by entity, date, hour
    rows.sort(key=lambda r: (r["entity"], r["date"], r["hour"]), reverse=True)

    # Print table
    click.echo("")
    hdr = (
        f"  {'ENTITY':<16} {'DATE':<12} {'HOUR':>4}  {'REASON':<16} "
        f"{'START':>8}  {'END':>8}  {'DURATION':>10}  {'MISSED':>7}  {'STATUS':<14}"
    )
    click.echo(hdr)
    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*16} "
        f"{'-'*8}  {'-'*8}  {'-'*10}  {'-'*7}  {'-'*14}"
    )

    for row in rows:
        click.echo(
            f"  {row['entity']:<16} {row['date']:<12} {row['hour']:>4}  "
            f"{row['reason']:<16} "
            f"{row['start']:>8}  {row['end']:>8}  {row['duration']:>10}  "
            f"{row['missed']:>7}  {row['status']:<14}"
        )

    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*16} "
        f"{'-'*8}  {'-'*8}  {'-'*10}  {'-'*7}  {'-'*14}"
    )
    click.echo(
        f"  {'TOTAL':<16} {'':12} {'':>4}  "
        f"{grand_total_gaps} gaps{'':<11} {'':>8}  {'':>8}  "
        f"{_format_duration(grand_total_dur):>10}  {'':>7}  "
        f"R:{grand_restored} U:{grand_unrecoverable} M:{grand_remaining}"
    )


@click.group()
def cli():
    """CryptoLake gap analysis CLI."""


@cli.command()
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream")
@click.option("--date", default=None, help="Specific date (YYYY-MM-DD)")
@click.option("--date-from", default=None, help="Start date inclusive (YYYY-MM-DD)")
@click.option("--date-to", default=None, help="End date inclusive (YYYY-MM-DD)")
@click.option("--json", "output_json", is_flag=True, help="Output report as JSON")
@click.option(
    "--base-dir",
    default=DEFAULT_ARCHIVE_DIR,
    help="Archive base directory",
)
def analyze(exchange, symbol, stream, date, date_from, date_to, output_json, base_dir):
    """Analyze archive coverage and report gaps."""
    base = Path(base_dir)
    report = analyze_archive(
        base,
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        date=date,
        date_from=date_from,
        date_to=date_to,
    )
    if output_json:
        # Convert int keys in hours dict to str for JSON serialisation
        def _serialisable(obj):
            if isinstance(obj, dict):
                return {str(k) if isinstance(k, int) else k: _serialisable(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_serialisable(i) for i in obj]
            return obj

        click.echo(json.dumps(_serialisable(report)))
    else:
        _print_report(report)


@cli.command()
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream")
@click.option("--date", default=None, help="Specific date (YYYY-MM-DD)")
@click.option("--date-from", default=None, help="Start date inclusive (YYYY-MM-DD)")
@click.option("--date-to", default=None, help="End date inclusive (YYYY-MM-DD)")
@click.option("--dry-run", is_flag=True, help="Print backfill plan without fetching data")
@click.option("--deep", is_flag=True, help="Also backfill gaps within existing files")
@click.option(
    "--base-dir",
    default=DEFAULT_ARCHIVE_DIR,
    help="Archive base directory",
)
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, deep, base_dir):
    """Find gaps and backfill missing hours from Binance historical REST API."""
    base = Path(base_dir)

    # If a specific stream is given and it's not backfillable, report and exit early
    if stream and stream not in BACKFILLABLE_STREAMS:
        click.echo(f"Stream '{stream}' is not backfillable (unrecoverable via REST API). Skipping.")
        return

    report = analyze_archive(
        base,
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        date=date,
        date_from=date_from,
        date_to=date_to,
    )

    # Separate backfillable from non-backfillable streams in the report
    non_backfillable_found: list[str] = []
    for _exch, symbols in report.items():
        for _sym, streams in symbols.items():
            for stream_name in streams:
                if stream_name not in BACKFILLABLE_STREAMS and stream_name not in non_backfillable_found:
                    non_backfillable_found.append(stream_name)

    for s in non_backfillable_found:
        click.echo(f"Stream '{s}' is not backfillable. Skipping.")

    missing = _collect_missing_hours(report)

    # Initialize shared state
    adapter = BinanceAdapter(ws_base="wss://fstream.binance.com", rest_base=BINANCE_REST_BASE)
    session_id = str(uuid.uuid4())
    total_written = 0

    if not missing:
        click.echo("Nothing to backfill: all backfillable hours are covered.")
    elif dry_run:
        click.echo(f"Dry-run: {len(missing)} missing hour(s) to backfill:")
        for exch_name, sym_name, stream_name, date_name, h in missing:
            click.echo(f"  {exch_name}/{sym_name}/{stream_name}/{date_name}  hour={h}")
    else:
        for exch_name, sym_name, stream_name, date_name, hour in missing:
            start_ms, end_ms = _hour_to_ms_range(date_name, hour)
            click.echo(f"Fetching {exch_name}/{sym_name}/{stream_name}/{date_name} hour={hour}...")

            try:
                records = asyncio.run(
                    _fetch_historical_all(adapter, sym_name, stream_name, start_ms, end_ms)
                )
            except Exception as exc:
                click.echo(f"  ERROR fetching: {exc}")
                continue

            if not records:
                click.echo(f"  No records returned for hour {hour}.")
                continue

            backfill_seq = _next_backfill_seq(base, exch_name, sym_name, stream_name, date_name, hour)
            out_path, n = _write_backfill_files(
                records,
                base_dir=str(base),
                exchange=exch_name,
                symbol=sym_name,
                stream=stream_name,
                date=date_name,
                session_id=session_id,
                seq_offset=total_written,
                backfill_seq=backfill_seq,
            )
            total_written += n
            if out_path:
                click.echo(f"  Wrote {n} records -> {out_path}")

        click.echo(f"Backfill complete: {total_written} total records written.")

    # Deep backfill: fill gaps within existing files
    if deep:
        from src.cli.integrity import find_backfillable_gaps
        id_gaps = find_backfillable_gaps(
            base,
            exchange=exchange,
            symbol=symbol,
            date=date,
            date_from=date_from,
            date_to=date_to,
        )
        time_gaps = find_time_based_gaps(
            base,
            exchange=exchange,
            symbol=symbol,
            date=date,
            date_from=date_from,
            date_to=date_to,
        )
        all_deep_gaps = id_gaps + time_gaps

        if not all_deep_gaps:
            click.echo("Deep: no in-file gaps found.")
        elif dry_run:
            click.echo(f"Deep dry-run: {len(all_deep_gaps)} in-file gap(s) found:")
            for g in id_gaps:
                click.echo(
                    f"  id_gap  {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']}"
                    f"  from_id={g['from_id']} to_id={g['to_id']} missing={g['missing']}"
                )
            for g in time_gaps:
                click.echo(
                    f"  time_gap  {g['exchange']}/{g['symbol']}/{g['stream']}/{g['date']}"
                    f"  start_ms={g['start_ms']} end_ms={g['end_ms']}"
                )
        else:
            for g in id_gaps:
                exch_name = g["exchange"]
                sym_name = g["symbol"]
                stream_name = g["stream"]
                date_name = g["date"]
                hour = g["hour"]
                click.echo(
                    f"Deep fetching id_gap {exch_name}/{sym_name}/{stream_name}/{date_name}"
                    f"  from_id={g['from_id']} to_id={g['to_id']}..."
                )
                try:
                    records = asyncio.run(
                        _fetch_by_id(adapter, sym_name, g["from_id"], g["to_id"])
                    )
                except Exception as exc:
                    click.echo(f"  ERROR fetching: {exc}")
                    continue
                if not records:
                    click.echo("  No records returned.")
                    continue
                backfill_seq = _next_backfill_seq(base, exch_name, sym_name, stream_name, date_name, hour)
                out_path, n = _write_backfill_files(
                    records,
                    base_dir=str(base),
                    exchange=exch_name,
                    symbol=sym_name,
                    stream=stream_name,
                    date=date_name,
                    session_id=session_id,
                    seq_offset=total_written,
                    backfill_seq=backfill_seq,
                )
                total_written += n
                if out_path:
                    click.echo(f"  Wrote {n} records -> {out_path}")

            for g in time_gaps:
                exch_name = g["exchange"]
                sym_name = g["symbol"]
                stream_name = g["stream"]
                date_name = g["date"]
                hour = g["hour"]
                click.echo(
                    f"Deep fetching time_gap {exch_name}/{sym_name}/{stream_name}/{date_name}"
                    f"  start_ms={g['start_ms']} end_ms={g['end_ms']}..."
                )
                try:
                    records = asyncio.run(
                        _fetch_historical_all(adapter, sym_name, stream_name, g["start_ms"], g["end_ms"])
                    )
                except Exception as exc:
                    click.echo(f"  ERROR fetching: {exc}")
                    continue
                if not records:
                    click.echo("  No records returned.")
                    continue
                backfill_seq = _next_backfill_seq(base, exch_name, sym_name, stream_name, date_name, hour)
                out_path, n = _write_backfill_files(
                    records,
                    base_dir=str(base),
                    exchange=exch_name,
                    symbol=sym_name,
                    stream=stream_name,
                    date=date_name,
                    session_id=session_id,
                    seq_offset=total_written,
                    backfill_seq=backfill_seq,
                )
                total_written += n
                if out_path:
                    click.echo(f"  Wrote {n} records -> {out_path}")

            click.echo(f"Deep backfill complete: {total_written} total records written.")


if __name__ == "__main__":
    cli()
