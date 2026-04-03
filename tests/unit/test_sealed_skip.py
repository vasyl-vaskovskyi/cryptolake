import json
import io
import tarfile
import zstandard
import orjson
from pathlib import Path
from click.testing import CliRunner


def _create_sealed_archive(base_dir, exchange, symbol, date):
    symbol_dir = base_dir / exchange / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    (symbol_dir / f"{date}.tar.zst").write_bytes(b"fake-archive")


def test_integrity_skips_sealed_date(tmp_path):
    from src.cli.integrity import check
    _create_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(check, [
        "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "sealed" in result.output.lower()


def test_backfill_skips_sealed_date(tmp_path):
    from src.cli.gaps import cli
    _create_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(cli, [
        "backfill", "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "sealed" in result.output.lower()


def _create_real_sealed_archive(base_dir, exchange, symbol, date):
    symbol_dir = base_dir / exchange / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)
    archive_path = symbol_dir / f"{date}.tar.zst"
    cctx = zstandard.ZstdCompressor(level=3)
    gap_env = {
        "v": 1, "type": "gap", "exchange": exchange, "symbol": symbol,
        "stream": "trades", "received_at": 1000,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 1775132494000000000, "gap_end_ts": 1775132523000000000,
        "reason": "ws_disconnect", "detail": "test gap",
    }
    data_env = {
        "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
        "stream": "trades", "received_at": 2000, "exchange_ts": 2000,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
    }
    jsonl_data = orjson.dumps(data_env) + b"\n" + orjson.dumps(gap_env) + b"\n"
    manifest = {
        "version": 1, "exchange": exchange, "symbol": symbol,
        "stream": "trades", "date": date, "total_records": 2,
        "data_records": 1, "gap_records": 1,
        "hours": {"12": {"status": "present", "data_records": 1}},
        "missing_hours": [], "source_files": [],
    }
    manifest_bytes = json.dumps(manifest).encode()
    with open(archive_path, "wb") as f:
        with cctx.stream_writer(f) as zst:
            with tarfile.open(fileobj=zst, mode="w|") as tar:
                info = tarfile.TarInfo(name=f"trades-{date}.jsonl")
                info.size = len(jsonl_data)
                tar.addfile(info, io.BytesIO(jsonl_data))
                info = tarfile.TarInfo(name=f"trades-{date}.manifest.json")
                info.size = len(manifest_bytes)
                tar.addfile(info, io.BytesIO(manifest_bytes))


def test_analyze_reads_sealed_archive(tmp_path):
    from src.cli.gaps import cli
    _create_real_sealed_archive(tmp_path, "binance", "btcusdt", "2026-04-02")
    runner = CliRunner()
    result = runner.invoke(cli, [
        "analyze", "--base-dir", str(tmp_path),
        "--exchange", "binance", "--symbol", "btcusdt",
        "--date", "2026-04-02",
    ])
    assert result.exit_code == 0
    assert "ws_disconnect" in result.output
