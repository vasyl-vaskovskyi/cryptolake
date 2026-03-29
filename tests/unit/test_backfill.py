from pathlib import Path
import zstandard
import orjson
from click.testing import CliRunner
from src.cli.gaps import cli

def _write_hour_file(base, exchange, symbol, stream, date, hour, envelopes):
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_data_env():
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1000, "exchange_ts": 999,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": "binance.trades", "_partition": 0, "_offset": 0,
    }

def test_backfill_dry_run_shows_plan(tmp_path):
    for h in range(24):
        if h == 16:
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28", "--dry-run"])
    assert result.exit_code == 0
    assert "16" in result.output

def test_backfill_skips_non_backfillable_streams(tmp_path):
    for h in range(24):
        if h == 16:
            continue
        dir_path = tmp_path / "binance" / "btcusdt" / "depth" / "2026-03-28"
        dir_path.mkdir(parents=True, exist_ok=True)
        cctx = zstandard.ZstdCompressor()
        data = orjson.dumps(_make_data_env())
        (dir_path / f"hour-{h}.jsonl.zst").write_bytes(cctx.compress(data))
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--stream", "depth", "--dry-run"])
    assert result.exit_code == 0
    # depth is not backfillable
    assert "skip" in result.output.lower() or "unrecoverable" in result.output.lower() or "nothing" in result.output.lower()

def test_backfill_skips_already_backfilled(tmp_path):
    for h in range(24):
        if h == 16:
            dir_path = tmp_path / "binance" / "btcusdt" / "trades" / "2026-03-28"
            dir_path.mkdir(parents=True, exist_ok=True)
            cctx = zstandard.ZstdCompressor()
            data = orjson.dumps(_make_data_env())
            (dir_path / "hour-16.backfill-1.jsonl.zst").write_bytes(cctx.compress(data))
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--date", "2026-03-28", "--dry-run"])
    assert result.exit_code == 0
    assert "nothing" in result.output.lower() or "0" in result.output or "covered" in result.output.lower()
