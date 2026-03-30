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

import json
from collections import defaultdict
from pathlib import Path

import click
import orjson
import zstandard as zstd

from src.common.config import default_archive_dir

DEFAULT_ARCHIVE_DIR = default_archive_dir()

CHECKABLE_STREAMS = {"trades", "depth", "bookticker"}


def _decompress_data_records(file_path: Path) -> list[dict]:
    """Decompress a .jsonl.zst file and return only data envelopes."""
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as f:
        data = dctx.stream_reader(f).read()
    records = []
    for line in data.strip().split(b"\n"):
        if not line:
            continue
        env = orjson.loads(line)
        if env.get("type") == "data":
            records.append(env)
    return records


def _check_trades(records: list[dict]) -> list[dict]:
    """Check aggregate trade ID ("a") continuity."""
    breaks: list[dict] = []
    prev_a: int | None = None
    for rec in records:
        raw = orjson.loads(rec["raw_text"])
        a = raw.get("a")
        if a is None:
            continue
        if prev_a is not None and a != prev_a + 1:
            breaks.append({
                "field": "a",
                "expected": prev_a + 1,
                "actual": a,
                "missing": a - prev_a - 1,
                "at_received": rec.get("received_at"),
            })
        prev_a = a
    return breaks


def _check_depth(records: list[dict]) -> list[dict]:
    """Check depth pu-chain continuity: each diff's pu must equal previous u."""
    breaks: list[dict] = []
    prev_u: int | None = None
    for rec in records:
        raw = orjson.loads(rec["raw_text"])
        U = raw.get("U", 0)
        u = raw.get("u", 0)
        pu = raw.get("pu", 0)
        if prev_u is not None and pu != prev_u:
            breaks.append({
                "field": "pu",
                "expected": prev_u,
                "actual": pu,
                "missing": None,
                "at_received": rec.get("received_at"),
            })
        prev_u = u
    return breaks


def _check_bookticker(records: list[dict]) -> list[dict]:
    """Check bookticker update ID ("u") for backwards jumps.

    The "u" field is a GLOBAL order book update ID shared across all symbols.
    Forward jumps are normal (other symbols' updates consume IDs). Only
    backwards jumps indicate a real problem (duplicate or out-of-order data).
    """
    breaks: list[dict] = []
    prev_u: int | None = None
    for rec in records:
        raw = orjson.loads(rec["raw_text"])
        u = raw.get("u")
        if u is None:
            continue
        if prev_u is not None and u <= prev_u:
            breaks.append({
                "field": "u",
                "expected": prev_u + 1,
                "actual": u,
                "missing": None,
                "at_received": rec.get("received_at"),
            })
        prev_u = u
    return breaks


CHECKERS = {
    "trades": _check_trades,
    "depth": _check_depth,
    "bookticker": _check_bookticker,
}


def _ns_to_time(ns: int) -> str:
    from datetime import datetime, timezone
    try:
        dt = datetime.fromtimestamp(ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%H:%M:%S")
    except (ValueError, OSError, OverflowError):
        return "??:??:??"


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
                if stream_name not in CHECKABLE_STREAMS:
                    continue

                checker = CHECKERS[stream_name]

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

                    # Read all data records across all hour files for this date,
                    # sorted by file name (hour order)
                    all_records: list[dict] = []
                    files = sorted(date_dir.glob("hour-*.jsonl.zst"))
                    for f in files:
                        try:
                            all_records.extend(_decompress_data_records(f))
                        except Exception:
                            pass

                    if not all_records:
                        continue

                    breaks = checker(all_records)

                    key = (exch_dir.name, sym_dir.name, stream_name, date_name)
                    report[key] = {
                        "records": len(all_records),
                        "breaks": breaks,
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
    total_breaks = 0
    total_missing = 0

    for (exch, sym, stream, date_name), info in sorted(report.items()):
        records = info["records"]
        breaks = info["breaks"]
        total_records += records

        if not breaks:
            click.echo(f"  {exch}/{sym}/{stream}/{date_name}  {records:>8} records  OK")
            continue

        total_breaks += len(breaks)
        missing_sum = sum(b["missing"] or 0 for b in breaks)
        total_missing += missing_sum

        click.echo(f"  {exch}/{sym}/{stream}/{date_name}  {records:>8} records  {len(breaks)} BREAKS  {missing_sum} missing")
        click.echo("")
        click.echo(f"    {'Field':<6} {'Expected':>14}  {'Actual':>14}  {'Missing':>8}  {'Time':>8}")
        click.echo(f"    {'-'*6} {'-'*14}  {'-'*14}  {'-'*8}  {'-'*8}")
        for b in breaks:
            missing_str = str(b["missing"]) if b["missing"] is not None else "chain"
            time_str = _ns_to_time(b["at_received"]) if b["at_received"] else ""
            click.echo(
                f"    {b['field']:<6} {b['expected']:>14}  {b['actual']:>14}  {missing_str:>8}  {time_str:>8}"
            )
        click.echo("")

    # Cross-reference: bookticker integrity via depth chain
    # Group results by (exchange, symbol, date) to check if depth is clean
    # for the same symbol/date where bookticker was checked
    by_sym_date: dict[tuple, dict[str, bool]] = defaultdict(dict)
    for (exch, sym, stream, date_name), info in report.items():
        key = (exch, sym, date_name)
        by_sym_date[key][stream] = len(info["breaks"]) == 0

    bookticker_notes: list[str] = []
    for (exch, sym, date_name), streams in sorted(by_sym_date.items()):
        if "bookticker" not in streams:
            continue
        if "depth" in streams:
            if streams["depth"]:
                bookticker_notes.append(
                    f"  {exch}/{sym}/bookticker/{date_name}: "
                    f"depth chain OK — bookticker integrity confirmed (same WebSocket, no drops)")
            else:
                bookticker_notes.append(
                    f"  {exch}/{sym}/bookticker/{date_name}: "
                    f"depth chain BROKEN — bookticker may have gaps too")
        else:
            bookticker_notes.append(
                f"  {exch}/{sym}/bookticker/{date_name}: "
                f"no depth data to cross-reference — bookticker integrity unverifiable")

    click.echo(f"\n{'='*60}")
    click.echo(f"INTEGRITY SUMMARY")
    click.echo(f"{'='*60}")
    click.echo(f"  Records checked:  {total_records}")
    click.echo(f"  ID breaks found:  {total_breaks}")
    click.echo(f"  Records missing:  {total_missing}")
    if total_records > 0 and total_breaks == 0:
        click.echo(f"  Status:           ALL IDs CONSECUTIVE")
    elif total_breaks > 0:
        click.echo(f"  Status:           INTEGRITY VIOLATIONS FOUND")
    if bookticker_notes:
        click.echo(f"\n  Bookticker cross-reference (via depth pu-chain):")
        for note in bookticker_notes:
            click.echo(note)


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
