"""Daily consolidation: merge hourly archive files into single daily files."""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import orjson
import structlog
import zstandard as zstd

from src.common.envelope import create_gap_envelope

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
