from __future__ import annotations

import json
from pathlib import Path

import click
import orjson
import zstandard as zstd

from src.common.config import default_archive_dir

DEFAULT_ARCHIVE_DIR = default_archive_dir()

BACKFILLABLE_STREAMS = frozenset({"trades", "funding_rate", "liquidations", "open_interest"})
NON_BACKFILLABLE_STREAMS = frozenset({"depth", "depth_snapshot", "bookticker"})


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


def _format_hours_line(hours: dict[int, str]) -> str:
    """Produce a compact single-line representation of hour coverage.

    Example: "0-15 OK  16 MISSING  17 MISSING  18-23 OK"
    """
    if not hours:
        return "all 24 hours MISSING"

    STATUS_LABEL = {
        "present": "OK",
        "backfilled": "BACKFILLED",
        "late": "LATE",
        "missing": "MISSING",
    }

    # Build sequence for all 24 hours
    all_hours = [(h, hours.get(h, "missing")) for h in range(24)]

    # Compress into runs
    segments: list[tuple[int, int, str]] = []  # (start, end, status)
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
    """Print a human-readable coverage report."""
    for exch_name, symbols in sorted(report.items()):
        click.echo(f"Exchange: {exch_name}")
        for sym_name, streams in sorted(symbols.items()):
            click.echo(f"  Symbol: {sym_name}")
            for stream_name, dates in sorted(streams.items()):
                click.echo(f"    Stream: {stream_name}")
                for date_name, info in sorted(dates.items()):
                    covered = info["covered"]
                    total = info["total"]
                    hours_line = _format_hours_line(info["hours"])
                    gaps = info["gaps"]
                    gap_str = f"  gaps={len(gaps)}" if gaps else ""
                    status = "OK" if covered == total else "MISSING"
                    click.echo(
                        f"      {date_name}  {covered}/{total}  [{status}]{gap_str}"
                    )
                    click.echo(f"        {hours_line}")
                    if gaps:
                        for g in gaps:
                            click.echo(
                                f"        GAP  reason={g.get('reason')}  "
                                f"start={g.get('gap_start_ts')}  "
                                f"end={g.get('gap_end_ts')}"
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
