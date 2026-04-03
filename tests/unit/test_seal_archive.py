"""Tests for seal_daily_archive — Task 1 & 2."""
from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path

import orjson
import zstandard
import pytest

from click.testing import CliRunner


def _create_daily_files(base_dir, exchange, symbol, stream, date, records=3):
    """Create fake per-stream daily files (jsonl.zst + manifest + sha256 sidecar)."""
    stream_dir = base_dir / exchange / symbol / stream
    stream_dir.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor(level=3)
    lines = []
    for i in range(records):
        env = {
            "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
            "stream": stream, "received_at": 1000 + i, "exchange_ts": 1000 + i,
            "collector_session_id": "test", "session_seq": i,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        lines.append(orjson.dumps(env))
    (stream_dir / f"{date}.jsonl.zst").write_bytes(cctx.compress(b"\n".join(lines) + b"\n"))
    from src.writer.file_rotator import compute_sha256
    sha = compute_sha256(stream_dir / f"{date}.jsonl.zst")
    (stream_dir / f"{date}.jsonl.zst.sha256").write_text(f"{sha}  {date}.jsonl.zst\n")
    manifest = {
        "version": 1, "exchange": exchange, "symbol": symbol, "stream": stream,
        "date": date, "total_records": records, "data_records": records,
        "gap_records": 0, "hours": {}, "missing_hours": [], "source_files": [],
    }
    (stream_dir / f"{date}.manifest.json").write_text(json.dumps(manifest))


# ---------------------------------------------------------------------------
# Test 1: seal creates tar.zst + sha256 sidecar
# ---------------------------------------------------------------------------

def test_seal_creates_tar_zst(tmp_path):
    date = "2026-04-02"
    exchange = "binance"
    symbol = "btcusdt"
    for stream in ("trades", "depth"):
        _create_daily_files(tmp_path, exchange, symbol, stream, date)

    from src.cli.consolidate import seal_daily_archive
    result = seal_daily_archive(
        base_dir=str(tmp_path),
        exchange=exchange,
        symbol=symbol,
        date=date,
    )

    assert result["success"] is True
    assert result["skipped"] is False

    symbol_dir = tmp_path / exchange / symbol
    tar_path = symbol_dir / f"{date}.tar.zst"
    sha_path = symbol_dir / f"{date}.tar.zst.sha256"
    assert tar_path.exists(), "tar.zst archive should be created"
    assert sha_path.exists(), "sha256 sidecar should be created"

    # Verify the sha256 sidecar content matches the actual file hash
    from src.writer.file_rotator import compute_sha256
    actual_sha = compute_sha256(tar_path)
    sidecar_content = sha_path.read_text().strip()
    assert sidecar_content.startswith(actual_sha)


# ---------------------------------------------------------------------------
# Test 2: archive contains uncompressed JSONL (not .jsonl.zst)
# ---------------------------------------------------------------------------

def test_seal_archive_contains_uncompressed_jsonl(tmp_path):
    date = "2026-04-02"
    exchange = "binance"
    symbol = "btcusdt"
    _create_daily_files(tmp_path, exchange, symbol, "trades", date, records=5)

    from src.cli.consolidate import seal_daily_archive
    seal_daily_archive(
        base_dir=str(tmp_path),
        exchange=exchange,
        symbol=symbol,
        date=date,
    )

    tar_path = tmp_path / exchange / symbol / f"{date}.tar.zst"
    dctx = zstandard.ZstdDecompressor()
    with open(tar_path, "rb") as fh:
        raw = dctx.stream_reader(fh).read()

    with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
        names = tf.getnames()
        # Should have uncompressed JSONL, not .jsonl.zst
        assert f"trades-{date}.jsonl" in names, f"Expected trades-{date}.jsonl in {names}"
        assert f"trades-{date}.jsonl.zst" not in names

        # The JSONL content should be valid (not zstd-compressed)
        member = tf.getmember(f"trades-{date}.jsonl")
        content = tf.extractfile(member).read()
        lines = [l for l in content.strip().split(b"\n") if l]
        assert len(lines) == 5
        first = orjson.loads(lines[0])
        assert first["stream"] == "trades"


# ---------------------------------------------------------------------------
# Test 3: seal removes per-stream daily files after archiving
# ---------------------------------------------------------------------------

def test_seal_removes_per_stream_files(tmp_path):
    date = "2026-04-02"
    exchange = "binance"
    symbol = "btcusdt"
    streams = ("trades", "depth")
    for stream in streams:
        _create_daily_files(tmp_path, exchange, symbol, stream, date)

    # Verify they exist before sealing
    for stream in streams:
        stream_dir = tmp_path / exchange / symbol / stream
        assert (stream_dir / f"{date}.jsonl.zst").exists()
        assert (stream_dir / f"{date}.jsonl.zst.sha256").exists()
        assert (stream_dir / f"{date}.manifest.json").exists()

    from src.cli.consolidate import seal_daily_archive
    seal_daily_archive(
        base_dir=str(tmp_path),
        exchange=exchange,
        symbol=symbol,
        date=date,
    )

    # All per-stream files should be gone
    for stream in streams:
        stream_dir = tmp_path / exchange / symbol / stream
        assert not (stream_dir / f"{date}.jsonl.zst").exists(), "jsonl.zst should be removed"
        assert not (stream_dir / f"{date}.jsonl.zst.sha256").exists(), "sha256 sidecar should be removed"
        assert not (stream_dir / f"{date}.manifest.json").exists(), "manifest should be removed"


# ---------------------------------------------------------------------------
# Test 4: seal skips if already sealed
# ---------------------------------------------------------------------------

def test_seal_skips_if_already_sealed(tmp_path):
    date = "2026-04-02"
    exchange = "binance"
    symbol = "btcusdt"
    symbol_dir = tmp_path / exchange / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    # Write a fake tar.zst to simulate already sealed
    tar_path = symbol_dir / f"{date}.tar.zst"
    tar_path.write_bytes(b"fake-sealed-content")

    from src.cli.consolidate import seal_daily_archive
    result = seal_daily_archive(
        base_dir=str(tmp_path),
        exchange=exchange,
        symbol=symbol,
        date=date,
    )

    assert result["skipped"] is True
    assert result["success"] is True
    # The fake file should be untouched
    assert tar_path.read_bytes() == b"fake-sealed-content"
