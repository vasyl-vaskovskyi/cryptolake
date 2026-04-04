"""Archive integrity checker: verifies ID continuity in raw Binance data.

Independent of gap detection — checks Binance's own sequential IDs in the
raw_text to find missing records that the collector/writer may have silently
dropped.

Streams checked:
  - trades:     aggregate trade ID ("a") must be consecutive
  - depth:      each diff's "pu" must equal previous diff's "u"
  - bookticker: update ID ("u") must be consecutive

Streams NOT checked (no sequential ID):
  - funding_rate, liquidations, open_interest, depth_snapshot
"""
from __future__ import annotations

import io
import json
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import click
import orjson
import zstandard as zstd

from src.common.config import default_archive_dir

DEFAULT_ARCHIVE_DIR = default_archive_dir()

CHECKABLE_STREAMS = {"trades", "depth", "bookticker"}

ALL_STREAMS = {
    "trades", "depth", "depth_snapshot", "bookticker",
    "funding_rate", "liquidations", "open_interest",
}


def _read_data_records_from_file(f: Path) -> list[tuple[bytes, int, int]]:
    """Read all data records from a single .jsonl.zst file.

    Returns list of (raw_text bytes, received_at, exchange_ts) tuples.
    """
    records = []
    try:
        dctx = zstd.ZstdDecompressor()
        with open(f, "rb") as fh:
            reader = dctx.stream_reader(fh)
            text_reader = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_reader:
                if '"data"' not in line:
                    continue
                env = orjson.loads(line)
                if env.get("type") == "data":
                    records.append((
                        env["raw_text"].encode(),
                        env.get("received_at", 0),
                        env.get("exchange_ts", 0),
                    ))
    except Exception:
        pass
    return records


def _sort_key_for_stream(raw_bytes: bytes, exchange_ts: int, stream: str) -> int:
    """Extract the correct sort key for a record based on stream type."""
    if stream == "trades":
        raw = orjson.loads(raw_bytes)
        return raw.get("a", 0)
    if stream == "depth":
        raw = orjson.loads(raw_bytes)
        return raw.get("u", 0)
    return exchange_ts


def _stream_data_records(files: list[Path], stream: str = "") -> Iterator[tuple[bytes, int]]:
    """Stream (raw_text bytes, received_at) from files, merging per-hour.

    Handles both consolidated daily files (single file) and unconsolidated
    hourly files (groups by hour, merges, sorts, deduplicates).
    """
    if not files:
        return

    # Single consolidated daily file — stream directly, already sorted
    if len(files) == 1 and not files[0].name.startswith("hour-"):
        for rec in _read_data_records_from_file(files[0]):
            yield rec[0], rec[1]
        return

    # Unconsolidated: group by hour, merge, sort, deduplicate
    from src.cli.consolidate import discover_hour_files

    date_dir = files[0].parent
    hour_groups = discover_hour_files(date_dir)

    for hour in sorted(hour_groups.keys()):
        fg = hour_groups[hour]
        hour_files = []
        if fg["base"]:
            hour_files.append(fg["base"])
        hour_files.extend(fg["late"])
        hour_files.extend(fg["backfill"])

        # Single file for this hour — stream directly
        if len(hour_files) == 1:
            for rec in _read_data_records_from_file(hour_files[0]):
                yield rec[0], rec[1]
            continue

        # Multiple files: read, merge, sort, deduplicate by natural key
        all_records: list[tuple[bytes, int, int]] = []
        for f in hour_files:
            all_records.extend(_read_data_records_from_file(f))

        all_records.sort(key=lambda r: _sort_key_for_stream(r[0], r[2], stream))

        prev_key: int | None = None
        for raw_bytes, received_at, exchange_ts in all_records:
            key = _sort_key_for_stream(raw_bytes, exchange_ts, stream)
            if stream in ("trades", "depth") and key == prev_key:
                continue
            prev_key = key
            yield raw_bytes, received_at


def _check_trades(files: list[Path]) -> tuple[int, list[dict]]:
    """Check aggregate trade ID ("a") continuity. Returns (record_count, breaks)."""
    breaks: list[dict] = []
    prev_a: int | None = None
    count = 0
    for raw_bytes, received_at in _stream_data_records(files, stream="trades"):
        count += 1
        raw = orjson.loads(raw_bytes)
        a = raw.get("a")
        if a is None:
            continue
        if prev_a is not None and a != prev_a + 1:
            breaks.append({
                "field": "a",
                "expected": prev_a + 1,
                "actual": a,
                "missing": a - prev_a - 1,
                "at_received": received_at,
            })
        prev_a = a
    return count, breaks


def _check_depth(files: list[Path]) -> tuple[int, list[dict]]:
    """Check depth pu-chain continuity. Returns (record_count, breaks)."""
    breaks: list[dict] = []
    prev_u: int | None = None
    count = 0
    for raw_bytes, received_at in _stream_data_records(files, stream="depth"):
        count += 1
        raw = orjson.loads(raw_bytes)
        u = raw.get("u", 0)
        pu = raw.get("pu", 0)
        if prev_u is not None and pu != prev_u:
            breaks.append({
                "field": "pu",
                "expected": prev_u,
                "actual": pu,
                "missing": None,
                "at_received": received_at,
            })
        prev_u = u
    return count, breaks


def _check_bookticker(files: list[Path]) -> tuple[int, list[dict]]:
    """Check bookticker update ID ("u") for backwards jumps. Returns (record_count, breaks).

    The "u" field is a GLOBAL order book update ID shared across all symbols.
    Forward jumps are normal (other symbols' updates consume IDs). Only
    backwards jumps indicate a real problem (duplicate or out-of-order data).
    """
    breaks: list[dict] = []
    prev_u: int | None = None
    count = 0
    for raw_bytes, received_at in _stream_data_records(files, stream="bookticker"):
        count += 1
        raw = orjson.loads(raw_bytes)
        u = raw.get("u")
        if u is None:
            continue
        if prev_u is not None and u <= prev_u:
            breaks.append({
                "field": "u",
                "expected": prev_u + 1,
                "actual": u,
                "missing": None,
                "at_received": received_at,
            })
        prev_u = u
    return count, breaks


CHECKERS = {
    "trades": _check_trades,
    "depth": _check_depth,
    "bookticker": _check_bookticker,
}


def _scan_gap_envelopes_from_file(file_path: Path) -> list[dict]:
    """Read gap envelopes from a single .jsonl.zst file."""
    gaps = []
    if not file_path.exists():
        return gaps
    try:
        dctx = zstd.ZstdDecompressor()
        with open(file_path, "rb") as fh:
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


def _scan_gap_envelopes(date_dir: Path) -> list[dict]:
    """Read gap envelopes from all .jsonl.zst files in a date directory."""
    gaps = []
    if not date_dir.exists():
        return gaps
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


def _ns_to_time(ns: int) -> str:
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return "??:??:??"


def _ns_to_hour(ns: int) -> str:
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return str(dt.hour)
    except (ValueError, OSError, OverflowError):
        return "?"


def check_integrity(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    stream: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """Check ID continuity across archive files. Returns structured report."""
    report: dict = {}

    if not base_dir.exists():
        return report

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
                stream_name = stream_dir.name
                if stream and stream_name != stream:
                    continue

                # Discover dates: unconsolidated (directories) + consolidated (daily files)
                all_date_names: set[str] = set()
                consolidated_dates: set[str] = set()

                for f in stream_dir.iterdir():
                    if f.is_file() and f.name.endswith(".jsonl.zst"):
                        name = f.name[:-len(".jsonl.zst")]
                        if len(name) == 10 and name[4] == "-" and name[7] == "-":
                            all_date_names.add(name)
                            consolidated_dates.add(name)
                for d in stream_dir.iterdir():
                    if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                        all_date_names.add(d.name)

                for date_name in sorted(all_date_names):
                    if date and date_name != date:
                        continue
                    if date_from and date_name < date_from:
                        continue
                    if date_to and date_name > date_to:
                        continue

                    breaks: list[dict] = []
                    count = 0
                    gap_envelopes: list[dict] = []

                    if date_name in consolidated_dates:
                        # Consolidated: scan the single daily file
                        daily_file = stream_dir / f"{date_name}.jsonl.zst"
                        files = [daily_file]

                        if stream_name in CHECKABLE_STREAMS:
                            checker = CHECKERS[stream_name]
                            count, breaks = checker(files)
                        else:
                            count = len(_read_data_records_from_file(daily_file))

                        gap_envelopes = _scan_gap_envelopes_from_file(daily_file)
                    else:
                        # Unconsolidated: scan hourly files
                        date_dir = stream_dir / date_name
                        files = sorted(date_dir.glob("hour-*.jsonl.zst"),
                                       key=lambda f: int(f.name.split(".")[0].replace("hour-", "")))
                        if not files:
                            continue

                        if stream_name in CHECKABLE_STREAMS:
                            checker = CHECKERS[stream_name]
                            count, breaks = checker(files)
                        else:
                            for f in files:
                                count += len(_read_data_records_from_file(f))

                        gap_envelopes = _scan_gap_envelopes(date_dir)

                    # Skip dates with no data UNLESS explicitly requested
                    if count == 0 and not gap_envelopes:
                        if date:
                            # Explicitly requested date has no data — report it
                            key = (exch_dir.name, sym_dir.name, stream_name, date_name)
                            report[key] = {
                                "records": 0,
                                "breaks": [],
                                "gaps": [{"type": "gap", "reason": "no_data",
                                          "gap_start_ts": 0, "gap_end_ts": 0,
                                          "detail": "No data files found for this date"}],
                            }
                        continue

                    key = (exch_dir.name, sym_dir.name, stream_name, date_name)
                    report[key] = {
                        "records": count,
                        "breaks": breaks,
                        "gaps": gap_envelopes,
                    }

    return report


def find_backfillable_gaps(
    base_dir: Path,
    exchange: str | None = None,
    symbol: str | None = None,
    date: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """Find trade ID gaps suitable for backfill via fromId API."""
    report = check_integrity(
        base_dir, exchange=exchange, symbol=symbol, stream="trades",
        date=date, date_from=date_from, date_to=date_to,
    )
    gaps: list[dict] = []
    for (exch, sym, stream_name, date_name), info in report.items():
        for b in info["breaks"]:
            if b["field"] != "a" or b["missing"] is None or b["missing"] <= 0:
                continue
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
                "from_id": b["expected"],
                "to_id": b["actual"] - 1,
                "missing": b["missing"],
            })
    return gaps


def _gap_envelope_status(gap: dict, stream_name: str, date_dir: Path | None) -> str:
    """Determine recovery status of a gap envelope."""
    from src.cli.gaps import NON_BACKFILLABLE_STREAMS
    if stream_name in NON_BACKFILLABLE_STREAMS:
        return "UNRECOVERABLE"
    if date_dir is not None and date_dir.exists():
        from datetime import datetime, timezone
        start_ns = gap.get("gap_start_ts", 0)
        try:
            dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc)
            hour = dt.hour
        except (ValueError, OSError):
            return "MISSING"
        if list(date_dir.glob(f"hour-{hour}.backfill-*.jsonl.zst")):
            return "RECOVERED"
    return "MISSING"


def _id_break_status(break_info: dict, stream_name: str, date_dir: Path | None) -> str:
    """Determine recovery status of an ID break."""
    from src.cli.gaps import NON_BACKFILLABLE_STREAMS
    if stream_name in NON_BACKFILLABLE_STREAMS:
        return "UNRECOVERABLE"
    # If integrity (with merge) found no gap, breaks list is empty.
    # If we're here, the break persists after merging backfill — it's unrecovered.
    # But for depth (chain breaks), there's nothing to recover.
    if break_info.get("missing") is None:
        return "UNRECOVERABLE"
    if date_dir is not None and date_dir.exists():
        hour_str = _ns_to_hour(break_info["at_received"]) if break_info["at_received"] else "0"
        try:
            hour = int(hour_str)
        except ValueError:
            return "MISSING"
        if list(date_dir.glob(f"hour-{hour}.backfill-*.jsonl.zst")):
            return "RECOVERED"
    return "MISSING"


def _print_report(report: dict, base_dir: Path | None = None) -> None:
    total_records = 0
    total_id_breaks = 0
    total_missing = 0
    total_gaps = 0
    status_counts: dict[str, int] = {"RECOVERED": 0, "UNRECOVERABLE": 0, "MISSING": 0}

    rows: list[dict] = []

    for (exch, sym, stream_name, date_name), info in report.items():
        records = info["records"]
        breaks = info.get("breaks", [])
        gap_envelopes = info.get("gaps", [])
        total_records += records

        date_dir = None
        if base_dir is not None:
            date_dir = base_dir / exch / sym / stream_name / date_name

        # Gap envelopes first — build time ranges to suppress duplicate ID breaks
        gap_ranges: list[tuple[int, int]] = []
        if gap_envelopes:
            total_gaps += len(gap_envelopes)
            for g in gap_envelopes:
                gap_start = g.get("gap_start_ts", 0)
                gap_end = g.get("gap_end_ts", 0)
                gap_ranges.append((gap_start, gap_end))
                hour = _ns_to_hour(gap_start)
                reason = g.get("reason", "unknown")
                start = _ns_to_time(gap_start)
                end = _ns_to_time(gap_end)
                status = _gap_envelope_status(g, stream_name, date_dir)
                status_counts[status] += 1
                rows.append({
                    "entity": stream_name, "date": date_name, "hour": hour,
                    "type": "GAP",
                    "detail": f"{reason} {start}-{end}",
                    "missing": "",
                    "time": start,
                    "status": status,
                })

        # ID breaks — skip if a gap envelope already covers the same time
        if breaks:
            for b in breaks:
                at_ns = b.get("at_received", 0)
                covered = any(gs <= at_ns <= ge + 5_000_000_000 for gs, ge in gap_ranges)
                if covered:
                    continue
                total_id_breaks += len(breaks)
                total_missing += b["missing"] or 0
                hour = _ns_to_hour(at_ns) if at_ns else ""
                status = _id_break_status(b, stream_name, date_dir)
                status_counts[status] += 1
                rows.append({
                    "entity": stream_name, "date": date_name, "hour": hour,
                    "type": "ID_BREAK",
                    "detail": f"{b['field']} exp={b['expected']} got={b['actual']}",
                    "missing": str(b["missing"]) if b["missing"] is not None else "chain",
                    "time": _ns_to_time(at_ns) if at_ns else "",
                    "status": status,
                })

    # Sort descending by entity, date, hour
    rows.sort(key=lambda r: (r["entity"], r["date"], int(r["hour"]) if r["hour"] else -1), reverse=True)

    # Print table
    click.echo("")
    hdr = (
        f"  {'ENTITY':<16} {'DATE':<12} {'HOUR':>4}  {'TYPE':<10} "
        f"{'DETAIL':<42} {'MISSING':>8}  {'TIME':>8}  {'STATUS':<14}"
    )
    click.echo(hdr)
    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*10} "
        f"{'-'*42} {'-'*8}  {'-'*8}  {'-'*14}"
    )

    for row in rows:
        click.echo(
            f"  {row['entity']:<16} {row['date']:<12} {row['hour']:>4}  "
            f"{row['type']:<10} {row['detail']:<42} "
            f"{row['missing']:>8}  {row['time']:>8}  {row['status']:<14}"
        )

    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*10} "
        f"{'-'*42} {'-'*8}  {'-'*8}  {'-'*14}"
    )
    click.echo(
        f"  {'TOTAL':<16} {'':12} {'':>4}  "
        f"{total_records} records, {total_id_breaks} ID breaks, {total_gaps} gaps  "
        f"R:{status_counts['RECOVERED']} U:{status_counts['UNRECOVERABLE']} M:{status_counts['MISSING']}"
    )


def _resolve_date_args(
    date: str | None,
    date_from: str | None,
    date_to: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Apply default date (today UTC) and enforce 3-day range limit."""
    from datetime import datetime, timezone

    if date is None and date_from is None and date_to is None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return today, None, None

    if date_from is not None and date_to is not None:
        try:
            d_from = datetime.strptime(date_from, "%Y-%m-%d")
            d_to = datetime.strptime(date_to, "%Y-%m-%d")
            if (d_to - d_from).days > 3:
                raise click.UsageError("Date range cannot exceed 3 days. Use a narrower range.")
        except ValueError:
            pass

    return date, date_from, date_to


@click.command()
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream (trades, depth, bookticker)")
@click.option("--date", default=None, help="Single date YYYY-MM-DD")
@click.option("--date-from", default=None, help="Start of date range")
@click.option("--date-to", default=None, help="End of date range")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.option("--base-dir", default=DEFAULT_ARCHIVE_DIR, help="Archive base directory")
def check(exchange, symbol, stream, date, date_from, date_to, as_json, base_dir):
    """Check Binance ID continuity in archived data.

    Verifies that sequential IDs in the raw data (trade IDs, depth update
    chains, bookticker update IDs) have no gaps. This catches data loss
    that the collector's gap detection may have missed.
    """
    date, date_from, date_to = _resolve_date_args(date, date_from, date_to)
    if date is not None:
        base = Path(base_dir)
        if base.exists():
            for exch_dir in sorted(base.iterdir()):
                if not exch_dir.is_dir():
                    continue
                if exchange and exch_dir.name != exchange:
                    continue
                for sym_dir in sorted(exch_dir.iterdir()):
                    if not sym_dir.is_dir():
                        continue
                    if symbol and sym_dir.name != symbol:
                        continue
                    if (sym_dir / f"{date}.tar.zst").exists():
                        click.echo(f"{date} is sealed. Skipping.")
                        return
    report = check_integrity(
        Path(base_dir),
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        date=date,
        date_from=date_from,
        date_to=date_to,
    )
    if as_json:
        serializable = {}
        for k, v in report.items():
            serializable["/".join(k)] = v
        click.echo(json.dumps(serializable, indent=2, default=str))
    else:
        _print_report(report, base_dir=Path(base_dir))


if __name__ == "__main__":
    check()
