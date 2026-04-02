"""Daily consolidation: merge hourly archive files into single daily files."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import orjson
import structlog
import zstandard as zstd

from src.common.envelope import create_gap_envelope
from src.writer.file_rotator import compute_sha256, write_sha256_sidecar, sidecar_path

logger = structlog.get_logger()

_RE_BASE = re.compile(r"^hour-(\d{1,2})\.jsonl\.zst$")
_RE_LATE = re.compile(r"^hour-(\d{1,2})\.late-(\d+)\.jsonl\.zst$")
_RE_BACKFILL = re.compile(r"^hour-(\d{1,2})\.backfill-(\d+)\.jsonl\.zst$")


def discover_hour_files(date_dir: Path) -> dict[int, dict]:
    """Scan a date directory and classify files by hour.

    Returns dict keyed by hour (0-23), each value is:
        {"base": Path | None, "late": [Path, ...], "backfill": [Path, ...]}
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

    for hour_data in groups.values():
        hour_data["late"].sort(key=lambda p: int(_RE_LATE.match(p.name).group(2)))
        hour_data["backfill"].sort(key=lambda p: int(_RE_BACKFILL.match(p.name).group(2)))

    return groups


def _decompress_and_parse(file_path: Path) -> list[dict]:
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as fh:
        data = dctx.stream_reader(fh).read()
    result = []
    for line in data.strip().split(b"\n"):
        if line:
            result.append(orjson.loads(line))
    return result


def _sort_key(record: dict) -> int:
    if record.get("type") == "gap":
        return record["gap_start_ts"]
    return record["exchange_ts"]


def merge_hour(hour: int, file_group: dict) -> list[dict]:
    all_records: list[dict] = []
    if file_group["base"] is not None:
        all_records.extend(_decompress_and_parse(file_group["base"]))
    for path in file_group["late"]:
        all_records.extend(_decompress_and_parse(path))
    for path in file_group["backfill"]:
        all_records.extend(_decompress_and_parse(path))
    all_records.sort(key=_sort_key)
    return all_records


def synthesize_missing_hour_gap(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    session_id: str,
) -> dict:
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


def write_daily_file(
    output_path: Path,
    hour_records: Iterator[tuple[int, list[dict]]],
) -> dict:
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


def verify_daily_file(
    daily_path: Path,
    expected_count: int,
    sha256_path: Path,
) -> tuple[bool, str | None]:
    """Verify a daily file: record count, ordering, and SHA256."""
    actual_sha = compute_sha256(daily_path)
    expected_sha = sha256_path.read_text().strip().split()[0]
    if actual_sha != expected_sha:
        return False, f"SHA256 mismatch: expected {expected_sha}, got {actual_sha}"

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

    all_hours = set(stats.get("hours", {}).keys()) | set(hour_details.keys())
    for h in sorted(all_hours):
        hour_key = str(h)
        entry = dict(hour_details.get(h, {}))
        if h in stats.get("hours", {}):
            entry.update(stats["hours"][h])
        manifest["hours"][hour_key] = entry

    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def cleanup_hourly_files(date_dir: Path, consolidated_files: list[Path]) -> int:
    """Remove consolidated .jsonl.zst files, keep .sha256 sidecars."""
    removed = 0
    for f in consolidated_files:
        if f.exists():
            f.unlink()
            removed += 1
            logger.info("cleanup_removed", file=f.name)
    return removed


def consolidate_day(
    *,
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
) -> dict:
    """Consolidate all hourly files for one exchange/symbol/stream/date into a daily file."""
    session_id = f"consolidation-{datetime.now(timezone.utc).isoformat()}"
    base = Path(base_dir)
    stream_dir = base / exchange / symbol.lower() / stream
    date_dir = stream_dir / date
    daily_path = stream_dir / f"{date}.jsonl.zst"
    sha_path = sidecar_path(daily_path)
    manifest_path = stream_dir / f"{date}.manifest.json"

    if daily_path.exists():
        logger.info("consolidation_skipped", exchange=exchange, symbol=symbol,
                     stream=stream, date=date, reason="daily file already exists")
        return {"skipped": True, "success": True}

    if not date_dir.is_dir():
        logger.warning("consolidation_no_date_dir", exchange=exchange, symbol=symbol,
                       stream=stream, date=date)
        return {"skipped": True, "success": True}

    hour_files = discover_hour_files(date_dir)

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

    stats = write_daily_file(daily_path, hour_iterator())
    write_sha256_sidecar(daily_path, sha_path)
    daily_sha = compute_sha256(daily_path)

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

    ok, error = verify_daily_file(daily_path, stats["total_records"], sha_path)
    if not ok:
        logger.error("consolidation_verification_failed", exchange=exchange,
                     symbol=symbol, stream=stream, date=date, error=error)
        daily_path.unlink(missing_ok=True)
        sha_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        return {"success": False, "error": error}

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
        "files_consolidated": len(consolidated_files),
    }


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
