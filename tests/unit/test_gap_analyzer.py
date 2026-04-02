import json
from pathlib import Path
import zstandard
import orjson
from click.testing import CliRunner
from src.cli.gaps import cli

def _write_hour_file(base: Path, exchange: str, symbol: str, stream: str,
                     date: str, hour: int, envelopes: list[dict],
                     suffix: str = "") -> Path:
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    name = f"hour-{hour}{suffix}.jsonl.zst"
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / name).write_bytes(cctx.compress(data))
    return dir_path / name

def _make_data_env(exchange="binance", symbol="btcusdt", stream="trades",
                   received_at=1000, exchange_ts=999):
    return {
        "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
        "stream": stream, "received_at": received_at, "exchange_ts": exchange_ts,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": 0,
    }

def _make_gap_env(exchange="binance", symbol="btcusdt", stream="trades",
                  reason="restart_gap", gap_start_ts=1000, gap_end_ts=2000):
    return {
        "v": 1, "type": "gap", "exchange": exchange, "symbol": symbol,
        "stream": stream, "received_at": gap_end_ts,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": gap_start_ts, "gap_end_ts": gap_end_ts,
        "reason": reason, "detail": "test gap",
        "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": -1,
    }

def test_analyze_full_day_no_gaps(tmp_path):
    for h in range(24):
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-27", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path), "--exchange", "binance", "--symbol", "btcusdt", "--date", "2026-03-27"])
    assert result.exit_code == 0
    assert "0 gaps" in result.output

def test_analyze_missing_hours(tmp_path):
    for h in range(24):
        if h in (16, 17):
            continue
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path), "--exchange", "binance", "--symbol", "btcusdt", "--date", "2026-03-28"])
    assert result.exit_code == 0
    assert "MISSING" in result.output
    assert "missing_hour" in result.output

def test_analyze_backfill_files_count_as_covered(tmp_path):
    for h in range(24):
        if h == 16:
            _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", h, [_make_data_env()], suffix=".backfill-1")
        else:
            _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-28", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path), "--exchange", "binance", "--symbol", "btcusdt", "--date", "2026-03-28"])
    assert result.exit_code == 0
    assert "0 gaps" in result.output

def test_analyze_json_output(tmp_path):
    for h in range(24):
        _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-27", h, [_make_data_env()])
    runner = CliRunner()
    result = runner.invoke(cli, ["analyze", "--base-dir", str(tmp_path), "--json", "--date", "2026-03-27"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "binance" in data
