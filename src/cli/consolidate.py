"""Daily consolidation: merge hourly archive files into single daily files."""
from __future__ import annotations

import io
import json
import re
import sys
import tarfile
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


def _data_sort_key(record: dict, stream: str) -> int:
    """Extract sort key for a DATA record based on stream type."""
    if stream == "trades":
        raw = orjson.loads(record.get("raw_text", "{}"))
        return raw.get("a", record["exchange_ts"])
    if stream == "depth":
        raw = orjson.loads(record.get("raw_text", "{}"))
        return raw.get("u", record["exchange_ts"])
    return record["exchange_ts"]


def _stream_hour_lines(file_path: Path) -> Iterator[bytes]:
    """Stream raw JSONL lines from a .jsonl.zst file without loading all into memory."""
    import io
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as fh:
        reader = dctx.stream_reader(fh)
        text_reader = io.TextIOWrapper(reader, encoding="utf-8")
        for line in text_reader:
            stripped = line.strip()
            if stripped:
                yield stripped.encode("utf-8")


def merge_hour(hour: int, file_group: dict, stream: str = "") -> list[dict]:
    """Merge all files for one hour, sort data records by natural key,
    then insert gap envelopes at the correct position by received_at."""
    # If single file with no late/backfill, stream directly (memory efficient)
    has_extra = bool(file_group["late"]) or bool(file_group["backfill"])
    if file_group["base"] is not None and not has_extra:
        records = _decompress_and_parse(file_group["base"])
        # Single file: already in order from the writer, just return as-is
        return records

    data_records: list[dict] = []
    gap_records: list[dict] = []

    for source in [file_group["base"]] + file_group["late"] + file_group["backfill"]:
        if source is None:
            continue
        for rec in _decompress_and_parse(source):
            if rec.get("type") == "gap":
                gap_records.append(rec)
            else:
                data_records.append(rec)

    # Sort data records by natural key
    data_records.sort(key=lambda r: _data_sort_key(r, stream))

    if not gap_records:
        return data_records

    # Insert gap envelopes at the correct position using gap_start_ts
    # matched against received_at of surrounding data records
    gap_records.sort(key=lambda g: g.get("gap_start_ts", 0))

    result: list[dict] = []
    gap_idx = 0
    for rec in data_records:
        received = rec.get("received_at", 0)
        while gap_idx < len(gap_records):
            gap_ts = gap_records[gap_idx].get("gap_start_ts", 0)
            if gap_ts <= received:
                result.append(gap_records[gap_idx])
                gap_idx += 1
            else:
                break
        result.append(rec)
    while gap_idx < len(gap_records):
        result.append(gap_records[gap_idx])
        gap_idx += 1

    return result


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
    hour_records: Iterator,
) -> dict:
    """Write records to a zstd-compressed JSONL daily file.

    hour_records yields tuples of (hour, records_or_none, raw_file_or_none):
    - (hour, list[dict], None) — write dicts as JSONL
    - (hour, None, Path) — stream raw lines from file (memory efficient)
    """
    cctx = zstd.ZstdCompressor(level=3)
    stats = {
        "total_records": 0,
        "data_records": 0,
        "gap_records": 0,
        "hours": {},
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fh = open(output_path, "wb")
    writer = cctx.stream_writer(fh, closefd=False)
    try:
        for item in hour_records:
            # Support both 2-tuple (legacy) and 3-tuple formats
            if len(item) == 2:
                hour, records = item
                raw_file = None
            else:
                hour, records, raw_file = item

            hour_data = 0
            hour_gaps = 0

            if raw_file is not None:
                # Stream raw chunks: decompress → recompress without parsing
                dctx = zstd.ZstdDecompressor()
                last_byte = b""
                buf_tail = b""
                with open(raw_file, "rb") as src:
                    reader = dctx.stream_reader(src)
                    while True:
                        chunk = reader.read(65536)
                        if not chunk:
                            break
                        writer.write(chunk)
                        last_byte = chunk[-1:]
                        # Count complete lines (each \n terminates a record)
                        # Also track partial line at end of chunk
                        combined = buf_tail + chunk
                        parts = combined.split(b"\n")
                        buf_tail = parts[-1]  # incomplete line (or empty if ends with \n)
                        for part in parts[:-1]:
                            if not part:
                                continue
                            if b'"gap"' in part:
                                hour_gaps += 1
                            else:
                                hour_data += 1
                # Last partial line (file doesn't end with \n)
                if buf_tail.strip():
                    if b'"gap"' in buf_tail:
                        hour_gaps += 1
                    else:
                        hour_data += 1
                # Ensure trailing newline between hours
                if last_byte and last_byte != b"\n":
                    writer.write(b"\n")
            else:
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
    finally:
        writer.close()  # flushes zstd frame
        fh.close()      # closes the file handle
    return stats


def verify_daily_file(
    daily_path: Path,
    expected_count: int,
    sha256_path: Path,
    stream: str = "",
) -> tuple[bool, str | None]:
    """Verify a daily file: record count, ordering, and SHA256.

    Checks that data records are in non-decreasing order by natural key.
    Gap envelopes are skipped in ordering checks (they are positioned
    by received_at during merge, not by natural key).
    """
    actual_sha = compute_sha256(daily_path)
    expected_sha = sha256_path.read_text().strip().split()[0]
    if actual_sha != expected_sha:
        return False, f"SHA256 mismatch: expected {expected_sha}, got {actual_sha}"

    dctx = zstd.ZstdDecompressor()
    count = 0
    prev_key = -1

    def _check_record(record: dict, count: int, prev_key: int) -> tuple[int, str | None]:
        if record.get("type") == "gap":
            return prev_key, None  # skip gap envelopes in ordering check
        key = _data_sort_key(record, stream)
        if key < prev_key:
            return prev_key, (
                f"Order violation at record {count}: "
                f"key {key} < previous {prev_key}"
            )
        return key, None

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
                prev_key, error = _check_record(record, count, prev_key)
                if error:
                    return False, error

        if buf.strip():
            record = orjson.loads(buf.strip())
            count += 1
            prev_key, error = _check_record(record, count, prev_key)
            if error:
                return False, error

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
    """Remove consolidated .jsonl.zst files and their .sha256 sidecars.

    After all files are removed, removes the date directory if empty.
    """
    removed = 0
    for f in consolidated_files:
        if f.exists():
            f.unlink()
            removed += 1
            logger.info("cleanup_removed", file=f.name)
        # Also remove the .sha256 sidecar
        sc = sidecar_path(f)
        if sc.exists():
            sc.unlink()

    # Remove date directory if empty
    if date_dir.exists() and not any(date_dir.iterdir()):
        date_dir.rmdir()
        logger.info("cleanup_removed_dir", dir=date_dir.name)

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

    def _read_sidecar_sha(data_path: Path) -> str:
        """Read SHA256 from a sidecar file, or return empty string."""
        sc = sidecar_path(data_path)
        if sc.exists():
            return sc.read_text().strip().split()[0]
        return ""

    for h in range(24):
        if h not in hour_files:
            missing_hours.append(h)
            hour_details[h] = {"status": "missing", "synthesized_gap": True}
        else:
            fg = hour_files[h]
            sources: dict[str, str] = {}
            if fg["base"]:
                sources[fg["base"].name] = _read_sidecar_sha(fg["base"])
            for f in fg["late"]:
                sources[f.name] = _read_sidecar_sha(f)
            for f in fg["backfill"]:
                sources[f.name] = _read_sidecar_sha(f)
            source_files.extend(sources.keys())

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
                fg = hour_files[h]
                has_extra = bool(fg["late"]) or bool(fg["backfill"])
                if fg["base"] is not None and not has_extra:
                    # Single file: stream raw lines (memory efficient)
                    yield h, None, fg["base"]
                else:
                    # Multiple files: merge in memory
                    records = merge_hour(h, fg, stream=stream)
                    yield h, records, None
            else:
                gap = synthesize_missing_hour_gap(
                    exchange=exchange, symbol=symbol, stream=stream,
                    date=date, hour=h, session_id=session_id,
                )
                yield h, [gap], None

    stats = write_daily_file(daily_path, hour_iterator())

    if not daily_path.exists():
        logger.error("consolidation_daily_file_missing", path=str(daily_path),
                     detail="write_daily_file completed but file not found on disk")
        return {"success": False, "error": f"Daily file not created: {daily_path}"}

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

    ok, error = verify_daily_file(daily_path, stats["total_records"], sha_path, stream=stream)
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


def seal_daily_archive(*, base_dir: str, exchange: str, symbol: str, date: str) -> dict:
    """Package all per-stream daily files for a date into a single tar.zst archive.

    The archive is placed at {symbol_dir}/{date}.tar.zst.  For each stream found:
      - .jsonl.zst  → decompressed to raw JSONL, stored as {stream}-{date}.jsonl
      - .manifest.json → stored as {stream}-{date}.manifest.json
      - .jsonl.zst.sha256 → stored as {stream}-{date}.sha256

    A SHA-256 sidecar (.tar.zst.sha256) is written and all archived per-stream
    files are removed after successful archiving.
    """
    symbol_dir = Path(base_dir) / exchange / symbol.lower()
    tar_path = symbol_dir / f"{date}.tar.zst"
    sha_path = tar_path.with_suffix(tar_path.suffix + ".sha256")

    if tar_path.exists():
        logger.info("seal_skipped", exchange=exchange, symbol=symbol, date=date,
                    reason="archive already exists")
        return {"success": True, "skipped": True, "streams": []}

    # Discover streams that have a daily .jsonl.zst for this date
    streams_found = []
    if symbol_dir.is_dir():
        for entry in sorted(symbol_dir.iterdir()):
            if not entry.is_dir():
                continue
            candidate = entry / f"{date}.jsonl.zst"
            if candidate.exists():
                streams_found.append(entry.name)

    if not streams_found:
        logger.warning("seal_no_streams", exchange=exchange, symbol=symbol, date=date)
        return {"success": True, "skipped": True, "streams": []}

    # Build the tar.zst archive using streaming to avoid loading large files into memory.
    # For JSONL files: decompress .jsonl.zst and compute size first (needed for tar header),
    # then stream into the tar. For small files (manifests, sha256): add directly.
    archived_files: list[Path] = []
    symbol_dir.mkdir(parents=True, exist_ok=True)

    cctx = zstd.ZstdCompressor(level=3)
    fh = open(tar_path, "wb")
    zst_writer = cctx.stream_writer(fh, closefd=False)
    tf = tarfile.open(fileobj=zst_writer, mode="w|")

    try:
        for stream_name in streams_found:
            s_dir = symbol_dir / stream_name

            # --- .jsonl.zst → decompress to temp file, then add to tar ---
            zst_file = s_dir / f"{date}.jsonl.zst"
            # Get decompressed size by reading through
            dctx = zstd.ZstdDecompressor()
            decompressed_size = 0
            with open(zst_file, "rb") as src:
                reader = dctx.stream_reader(src)
                while True:
                    chunk = reader.read(65536)
                    if not chunk:
                        break
                    decompressed_size += len(chunk)

            # Add tar header then stream decompressed data
            member_name = f"{stream_name}-{date}.jsonl"
            info = tarfile.TarInfo(name=member_name)
            info.size = decompressed_size
            # Use a pipe: create a wrapper that streams decompressed data
            dctx2 = zstd.ZstdDecompressor()
            with open(zst_file, "rb") as src:
                tf.addfile(info, dctx2.stream_reader(src))
            archived_files.append(zst_file)

            # --- .manifest.json (small) ---
            manifest_file = s_dir / f"{date}.manifest.json"
            if manifest_file.exists():
                tf.add(manifest_file, arcname=f"{stream_name}-{date}.manifest.json")
                archived_files.append(manifest_file)

            # --- .jsonl.zst.sha256 (small) ---
            sha256_file = s_dir / f"{date}.jsonl.zst.sha256"
            if sha256_file.exists():
                tf.add(sha256_file, arcname=f"{stream_name}-{date}.sha256")
                archived_files.append(sha256_file)
    finally:
        tf.close()
        zst_writer.close()
        fh.close()

    # Write sha256 sidecar
    digest = compute_sha256(tar_path)
    sha_path.write_text(f"{digest}  {tar_path.name}\n")

    # Remove per-stream daily files
    for f in archived_files:
        if f.exists():
            f.unlink()

    logger.info("seal_complete", exchange=exchange, symbol=symbol, date=date,
                streams=streams_found, archive=tar_path.name)
    return {"success": True, "skipped": False, "streams": streams_found}


import click


ALL_STREAMS = [
    "trades", "depth", "depth_snapshot", "bookticker",
    "funding_rate", "liquidations", "open_interest",
]


@click.group()
def cli():
    """Daily consolidation CLI."""
    pass


def _consolidate_stream_subprocess(base_dir: str, exchange: str, symbol: str, stream: str, date: str) -> dict:
    """Run consolidate_day in a subprocess to isolate memory.

    Large streams (bookticker: 17M records) can consume multi-GB of memory.
    Running each stream in its own subprocess ensures memory is fully released
    between streams.
    """
    import subprocess, json as _json
    result = subprocess.run(
        [
            sys.executable, "-c",
            f"import json; from src.cli.consolidate import consolidate_day; "
            f"r = consolidate_day(base_dir={base_dir!r}, exchange={exchange!r}, "
            f"symbol={symbol!r}, stream={stream!r}, date={date!r}); "
            f"print(json.dumps(r))"
        ],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip().split("\n")[-1] if result.stderr else "unknown"
        return {"success": False, "error": f"Subprocess failed: {stderr}"}
    # Parse the last line of stdout as JSON (skip structlog lines)
    for line in reversed(result.stdout.strip().split("\n")):
        try:
            return _json.loads(line)
        except _json.JSONDecodeError:
            continue
    return {"success": False, "error": "No JSON output from subprocess"}


@cli.command()
@click.option("--base-dir", default=None, help="Archive base directory")
@click.option("--exchange", default="binance", help="Exchange name")
@click.option("--symbol", required=True, help="Trading symbol (e.g. btcusdt)")
@click.option("--stream", default=None, help="Specific stream to consolidate (default: all)")
@click.option("--date", "target_date", required=True, help="Date to consolidate (YYYY-MM-DD)")
def run(base_dir, exchange, symbol, stream, target_date):
    """Consolidate hourly files into daily files, then seal into archive.

    Each stream is consolidated in a separate subprocess to isolate memory.
    """
    from src.common.config import default_archive_dir
    from src.common.logging import setup_logging

    setup_logging()

    if base_dir is None:
        base_dir = default_archive_dir()

    # Skip everything if the symbol-level archive already exists
    symbol_dir = Path(base_dir) / exchange / symbol.lower()
    tar_path = symbol_dir / f"{target_date}.tar.zst"
    if tar_path.exists():
        click.echo(f"Already sealed: {tar_path.name} — skipping consolidation.")
        return

    # Run backfill before consolidation to recover missing data
    click.echo(f"Backfilling {target_date}...")
    import subprocess as _sp
    backfill_cmd = [
        sys.executable, "-m", "src.cli.gaps", "backfill",
        "--base-dir", base_dir,
        "--exchange", exchange,
        "--symbol", symbol.lower(),
        "--date", target_date,
        "--deep",
    ]
    bp = _sp.run(backfill_cmd, capture_output=True, text=True, timeout=600)
    if bp.returncode == 0:
        # Print non-empty lines that aren't JSON logs
        for line in bp.stdout.strip().split("\n"):
            if line and not line.startswith("{"):
                click.echo(f"  {line}")
    else:
        click.echo(f"  Backfill warning: {bp.stderr.strip().split(chr(10))[-1] if bp.stderr else 'failed'}")

    streams = [stream] if stream else ALL_STREAMS
    any_consolidated = False

    for s in streams:
        stream_dir = Path(base_dir) / exchange / symbol.lower() / s
        date_dir = stream_dir / target_date
        daily_file = stream_dir / f"{target_date}.jsonl.zst"

        if daily_file.exists():
            any_consolidated = True
            click.echo(f"[{s}] Skipped (already consolidated)")
            continue

        if not date_dir.is_dir():
            continue

        click.echo(f"[{s}] Consolidating...", nl=False)
        result = _consolidate_stream_subprocess(base_dir, exchange, symbol, s, target_date)

        if result.get("skipped"):
            click.echo(" skipped")
        elif result.get("success"):
            any_consolidated = True
            click.echo(
                f" {result['total_records']} records, "
                f"{len(result.get('missing_hours', []))} missing hours"
            )
        else:
            click.echo(f" FAILED: {result.get('error', 'unknown')}")

    click.echo("Consolidation finished.")

    if any_consolidated:
        seal_result = seal_daily_archive(
            base_dir=base_dir,
            exchange=exchange,
            symbol=symbol,
            date=target_date,
        )
        if seal_result.get("skipped"):
            click.echo(f"Seal skipped (already sealed).")
        elif seal_result.get("success"):
            click.echo(
                f"Sealed: {target_date}.tar.zst "
                f"({len(seal_result.get('streams', []))} streams)"
            )
        else:
            click.echo(f"Seal FAILED.")


if __name__ == "__main__":
    cli()
