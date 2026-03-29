from __future__ import annotations

import asyncio
import hashlib
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
    "funding_rate": "fundingTime",
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


async def _fetch_historical_page(session: aiohttp.ClientSession, url: str) -> list[dict]:
    async with session.get(url) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def _fetch_historical_all(
    adapter: BinanceAdapter,
    symbol: str,
    stream: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """Fetch all records for the given stream/symbol in [start_ms, end_ms] with pagination."""
    ts_key = STREAM_TS_KEYS[stream]
    all_records: list[dict] = []

    async with aiohttp.ClientSession() as session:
        current_start = start_ms
        while current_start <= end_ms:
            if stream == "trades":
                url = adapter.build_historical_trades_url(
                    symbol, start_time=current_start, end_time=end_ms, limit=1000
                )
            elif stream == "funding_rate":
                url = adapter.build_historical_funding_url(
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
    """Read all .jsonl.zst files in date_dir and return gap envelopes."""
    gaps: list[dict] = []
    if not date_dir.exists():
        return gaps
    dctx = zstd.ZstdDecompressor()
    for f in sorted(date_dir.glob("*.jsonl.zst")):
        try:
            with open(f, "rb") as fh:
                data = dctx.stream_reader(fh).read()
            for line in data.strip().split(b"\n"):
                if not line:
                    continue
                env = orjson.loads(line)
                if env.get("type") == "gap":
                    gaps.append(env)
        except Exception:
            pass  # Skip unreadable files
    return gaps


def _format_duration(ns: int) -> str:
    """Format a nanosecond duration into human-readable form.

    <60s → "42s", <60m → "3m12s", <24h → "2h14m3s", >=24h → "1d2h14m3s".
    Zero or negative → "instant".
    """
    if ns <= 0:
        return "instant"
    total_seconds = int(ns / 1_000_000_000)
    if total_seconds == 0:
        return "instant"
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
    return "-"


def _format_hours_line(hours: dict[int, str]) -> str:
    """Produce a compact single-line representation of hour coverage.

    Example: "0-15 OK  16 MISSING  17 MISSING  18-23 OK"
    """
    if not hours:
        return "all 24 hours MISSING"

    STATUS_LABEL = {
        "present": "OK",
        "backfilled": "RECOVERED",
        "late": "OK",
        "missing": "MISSING",
    }

    all_hours = [(h, hours.get(h, "missing")) for h in range(24)]

    segments: list[tuple[int, int, str]] = []
    run_start = 0
    run_status = all_hours[0][1]
    for i in range(1, 24):
        if all_hours[i][1] != run_status:
            segments.append((run_start, i - 1, run_status))
            run_start = i
            run_status = all_hours[i][1]
    segments.append((run_start, 23, run_status))

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

                for date_dir in dates_to_scan:
                    date_name = date_dir.name
                    hour_map = _scan_hours(date_dir)
                    gaps = _scan_gaps(date_dir)
                    covered = len(hour_map)

                    report.setdefault(exch_name, {})
                    report[exch_name].setdefault(sym_name, {})
                    report[exch_name][sym_name].setdefault(stream_name, {})
                    report[exch_name][sym_name][stream_name][date_name] = {
                        "hours": hour_map,
                        "gaps": gaps,
                        "covered": covered,
                        "total": 24,
                    }

    return report


def _print_report(report: dict) -> None:
    """Print a human-readable coverage report with gap table and summaries."""
    # Accumulators for grand total
    grand_total_gaps = 0
    grand_total_gaps_dur = 0
    grand_restored_gaps = 0
    grand_restored_gaps_dur = 0
    grand_unrecoverable_gaps = 0
    grand_unrecoverable_gaps_dur = 0
    grand_remaining_gaps = 0
    grand_remaining_gaps_dur = 0
    grand_recorded_dur = 0

    for exch_name, symbols in sorted(report.items()):
        for sym_name, streams in sorted(symbols.items()):
            click.echo(f"\n{exch_name} / {sym_name}")
            click.echo("=" * 80)

            for stream_name, dates in sorted(streams.items()):
                for date_name, info in sorted(dates.items()):
                    covered = info["covered"]
                    total = info["total"]
                    hours_line = _format_hours_line(info["hours"])
                    hour_map = info["hours"]
                    gaps = info["gaps"]

                    click.echo(f"\n  {stream_name} / {date_name}  {covered}/{total} hours")
                    click.echo(f"  Hours: {hours_line}")

                    # Build unified gap list: gap records + missing hours (as synthetic entries)
                    all_entries: list[dict] = []

                    # Add missing hours as synthetic gap entries
                    for h in range(24):
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
                                "_gap_status": file_st,  # MISSING, RECOVERED, UNRECOVERABLE
                                "_file_status": file_st,
                                "_is_hour_gap": True,
                            })

                    # Add actual gap records from files
                    for g in gaps:
                        g_status = _gap_status(g, stream_name)
                        all_entries.append({
                            **g,
                            "_gap_status": g_status,
                            "_file_status": "-",
                            "_is_hour_gap": False,
                        })

                    # Sort all entries by start timestamp, then by end
                    all_entries.sort(key=lambda e: (e.get("gap_start_ts", 0), e.get("gap_end_ts", 0)))

                    if not all_entries:
                        click.echo("  No gaps.")
                        continue

                    # Compute effective (non-overlapping) duration for each gap.
                    # Gaps from sparse streams (e.g. liquidations) record gap_start_ts
                    # as the last message received_at, which can be hours before the
                    # actual disconnect.  By tracking a high-water mark we count each
                    # moment of gap time only once.
                    high_water = 0
                    for entry in all_entries:
                        start_ts = entry.get("gap_start_ts", 0)
                        end_ts = entry.get("gap_end_ts", 0)
                        effective_start = max(start_ts, high_water)
                        effective_dur = max(0, end_ts - effective_start)
                        entry["_effective_dur_ns"] = effective_dur
                        if end_ts > high_water:
                            high_water = end_ts

                    # Print table header
                    click.echo("")
                    hdr = (
                        f"  {'Reason':<20} {'Start':>8}  {'End':>8}  "
                        f"{'Duration':>12}  {'Status':<15} {'File Status':<15}"
                    )
                    click.echo(hdr)
                    click.echo(f"  {'-'*20} {'-'*8}  {'-'*8}  {'-'*12}  {'-'*15} {'-'*15}")

                    # Per-stream/date accumulators
                    st_total = 0
                    st_total_dur = 0
                    st_restored = 0
                    st_restored_dur = 0
                    st_unrecoverable = 0
                    st_unrecoverable_dur = 0
                    st_remaining = 0
                    st_remaining_dur = 0

                    for entry in all_entries:
                        reason = entry.get("reason", "unknown")
                        start_ts = entry.get("gap_start_ts", 0)
                        end_ts = entry.get("gap_end_ts", 0)
                        dur_ns = entry["_effective_dur_ns"]
                        gap_status = entry["_gap_status"]
                        file_status = entry["_file_status"]

                        start_str = _ns_to_time(start_ts)
                        end_str = _ns_to_time(end_ts)
                        dur_str = _format_duration(dur_ns)

                        click.echo(
                            f"  {reason:<20} {start_str:>8}  {end_str:>8}  "
                            f"{dur_str:>12}  {gap_status:<15} {file_status:<15}"
                        )

                        # Accumulate
                        st_total += 1
                        st_total_dur += dur_ns
                        if gap_status == "RECOVERED" or file_status == "RECOVERED":
                            st_restored += 1
                            st_restored_dur += dur_ns
                        elif gap_status == "UNRECOVERABLE" or file_status == "UNRECOVERABLE":
                            st_unrecoverable += 1
                            st_unrecoverable_dur += dur_ns
                        else:
                            st_remaining += 1
                            st_remaining_dur += dur_ns

                    # Total recorded duration = covered hours × 1h in nanoseconds
                    recorded_dur_ns = covered * 3_600_000_000_000

                    # Per-stream/date summary
                    click.echo(f"  {'-'*20} {'-'*8}  {'-'*8}  {'-'*12}  {'-'*15} {'-'*15}")
                    click.echo(f"  Total recorded:      {_format_duration(recorded_dur_ns)}")
                    click.echo(f"  Total gaps:          {st_total:>4} / {_format_duration(st_total_dur)}")
                    click.echo(f"  Restored:            {st_restored:>4} / {_format_duration(st_restored_dur)}")
                    click.echo(f"  Unrecoverable:       {st_unrecoverable:>4} / {_format_duration(st_unrecoverable_dur)}")
                    click.echo(f"  Remaining to restore:{st_remaining:>4} / {_format_duration(st_remaining_dur)}")

                    grand_total_gaps += st_total
                    grand_total_gaps_dur += st_total_dur
                    grand_restored_gaps += st_restored
                    grand_restored_gaps_dur += st_restored_dur
                    grand_unrecoverable_gaps += st_unrecoverable
                    grand_unrecoverable_gaps_dur += st_unrecoverable_dur
                    grand_remaining_gaps += st_remaining
                    grand_remaining_gaps_dur += st_remaining_dur
                    grand_recorded_dur += recorded_dur_ns

    # Grand total
    click.echo(f"\n{'=' * 80}")
    click.echo("TOTAL SUMMARY")
    click.echo(f"{'=' * 80}")
    click.echo(f"  Total recorded:            {_format_duration(grand_recorded_dur)}")
    click.echo(f"  Total gaps:          {grand_total_gaps:>4} / {_format_duration(grand_total_gaps_dur)}")
    click.echo(f"  Restored:            {grand_restored_gaps:>4} / {_format_duration(grand_restored_gaps_dur)}")
    click.echo(f"  Unrecoverable:       {grand_unrecoverable_gaps:>4} / {_format_duration(grand_unrecoverable_gaps_dur)}")
    click.echo(f"  Remaining to restore:{grand_remaining_gaps:>4} / {_format_duration(grand_remaining_gaps_dur)}")


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
@click.option(
    "--base-dir",
    default=DEFAULT_ARCHIVE_DIR,
    help="Archive base directory",
)
def backfill(exchange, symbol, stream, date, date_from, date_to, dry_run, base_dir):
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

    if not missing:
        click.echo("Nothing to backfill: all backfillable hours are covered.")
        return

    if dry_run:
        click.echo(f"Dry-run: {len(missing)} missing hour(s) to backfill:")
        for exch_name, sym_name, stream_name, date_name, h in missing:
            click.echo(f"  {exch_name}/{sym_name}/{stream_name}/{date_name}  hour={h}")
        return

    # Actual backfill
    adapter = BinanceAdapter(ws_base="wss://fstream.binance.com", rest_base=BINANCE_REST_BASE)
    session_id = str(uuid.uuid4())
    total_written = 0

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


if __name__ == "__main__":
    cli()
