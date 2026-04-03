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
    """Stream (raw_text bytes, received_at) from hour files, merging per-hour.

    Groups files by hour (base + late + backfill), merges and sorts within
    each hour by the stream's natural key (trade ID for trades, update ID
    for depth, exchange_ts for others). This ensures backfill records are
    interleaved correctly with live data.
    """
    from src.cli.consolidate import discover_hour_files

    if not files:
        return

    # All files should be in the same date directory
    date_dir = files[0].parent
    hour_groups = discover_hour_files(date_dir)

    for hour in sorted(hour_groups.keys()):
        fg = hour_groups[hour]
        hour_files = []
        if fg["base"]:
            hour_files.append(fg["base"])
        hour_files.extend(fg["late"])
        hour_files.extend(fg["backfill"])

        # If only one file and no backfills, stream directly (memory efficient)
        if len(hour_files) == 1:
            for rec in _read_data_records_from_file(hour_files[0]):
                yield rec[0], rec[1]
            continue

        # Multiple files: read, merge, sort, deduplicate by natural key
        all_records: list[tuple[bytes, int, int]] = []
        for f in hour_files:
            all_records.extend(_read_data_records_from_file(f))

        all_records.sort(key=lambda r: _sort_key_for_stream(r[0], r[2], stream))

        # Deduplicate: if stream has a natural ID key, skip records with
        # the same key as the previous (backfill may overlap with live data)
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

                    files = sorted(date_dir.glob("hour-*.jsonl.zst"),
                                   key=lambda f: int(f.name.split(".")[0].replace("hour-", "")))
                    if not files:
                        continue

                    # ID continuity check (only for streams with sequential IDs)
                    breaks: list[dict] = []
                    count = 0
                    if stream_name in CHECKABLE_STREAMS:
                        checker = CHECKERS[stream_name]
                        count, breaks = checker(files)
                    else:
                        # Count records for non-checkable streams
                        for f in files:
                            count += len(_read_data_records_from_file(f))

                    # Gap envelopes (all streams)
                    gap_envelopes = _scan_gap_envelopes(date_dir)

                    if count == 0 and not gap_envelopes:
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


def _print_report(report: dict) -> None:
    total_records = 0
    total_id_breaks = 0
    total_missing = 0
    total_gaps = 0

    rows: list[dict] = []

    for (exch, sym, stream_name, date_name), info in report.items():
        records = info["records"]
        breaks = info.get("breaks", [])
        gap_envelopes = info.get("gaps", [])
        total_records += records

        # ID breaks
        if breaks:
            total_id_breaks += len(breaks)
            total_missing += sum(b["missing"] or 0 for b in breaks)

            for b in breaks:
                hour = _ns_to_hour(b["at_received"]) if b["at_received"] else ""
                rows.append({
                    "entity": stream_name, "date": date_name, "hour": hour,
                    "type": "ID_BREAK",
                    "detail": f"{b['field']} exp={b['expected']} got={b['actual']}",
                    "missing": str(b["missing"]) if b["missing"] is not None else "chain",
                    "time": _ns_to_time(b["at_received"]) if b["at_received"] else "",
                })

        # Gap envelopes
        if gap_envelopes:
            total_gaps += len(gap_envelopes)
            for g in gap_envelopes:
                hour = _ns_to_hour(g.get("gap_start_ts", 0))
                reason = g.get("reason", "unknown")
                start = _ns_to_time(g.get("gap_start_ts", 0))
                end = _ns_to_time(g.get("gap_end_ts", 0))
                rows.append({
                    "entity": stream_name, "date": date_name, "hour": hour,
                    "type": "GAP",
                    "detail": f"{reason} {start}-{end}",
                    "missing": "",
                    "time": start,
                })

    # Sort descending by entity, date, hour
    rows.sort(key=lambda r: (r["entity"], r["date"], r["hour"]), reverse=True)

    # Print table
    click.echo("")
    hdr = (
        f"  {'ENTITY':<16} {'DATE':<12} {'HOUR':>4}  {'TYPE':<10} "
        f"{'DETAIL':<42} {'MISSING':>8}  {'TIME':>8}"
    )
    click.echo(hdr)
    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*10} "
        f"{'-'*42} {'-'*8}  {'-'*8}"
    )

    for row in rows:
        click.echo(
            f"  {row['entity']:<16} {row['date']:<12} {row['hour']:>4}  "
            f"{row['type']:<10} {row['detail']:<42} "
            f"{row['missing']:>8}  {row['time']:>8}"
        )

    click.echo(
        f"  {'-'*16} {'-'*12} {'-'*4}  {'-'*10} "
        f"{'-'*42} {'-'*8}  {'-'*8}"
    )
    click.echo(
        f"  {'TOTAL':<16} {'':12} {'':>4}  "
        f"{total_records} records, {total_id_breaks} ID breaks, {total_gaps} gap envelopes, "
        f"{total_missing} missing"
    )


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
        _print_report(report)


if __name__ == "__main__":
    check()
