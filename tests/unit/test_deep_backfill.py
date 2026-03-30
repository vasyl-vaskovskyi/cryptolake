from pathlib import Path
import hashlib
import zstandard
import orjson
from click.testing import CliRunner
from src.cli.gaps import find_time_based_gaps, cli

def _write_hour_file(base, exchange, symbol, stream, date, hour, envelopes):
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_gap_env(stream, gap_start_ns, gap_end_ns):
    return {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": stream, "received_at": gap_end_ns,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": gap_start_ns, "gap_end_ts": gap_end_ns,
        "reason": "ws_disconnect", "detail": "WebSocket disconnected",
        "_topic": f"binance.{stream}", "_partition": 0, "_offset": -1,
    }

def _make_data_env(stream):
    raw_text = "{}"
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": stream, "received_at": 1000, "exchange_ts": 999,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": raw_text, "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": f"binance.{stream}", "_partition": 0, "_offset": 0,
    }

def test_find_time_based_gaps_funding_rate(tmp_path):
    gap = _make_gap_env("funding_rate", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("funding_rate")
    _write_hour_file(tmp_path, "binance", "btcusdt", "funding_rate", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 1
    assert gaps[0]["type"] == "time_gap"
    assert gaps[0]["stream"] == "funding_rate"
    assert gaps[0]["start_ms"] == 1774900000000
    assert gaps[0]["end_ms"] == 1774900060000

def test_find_time_based_gaps_ignores_trades(tmp_path):
    gap = _make_gap_env("trades", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("trades")
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0

def test_find_time_based_gaps_ignores_depth(tmp_path):
    gap = _make_gap_env("depth", 1774900000000000000, 1774900060000000000)
    data = _make_data_env("depth")
    _write_hour_file(tmp_path, "binance", "btcusdt", "depth", "2026-03-30", 20, [data, gap])
    gaps = find_time_based_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0


def _make_trade_env(a, seq):
    raw = {"e": "aggTrade", "a": a, "s": "BTCUSDT", "p": "67000", "q": "0.1", "T": 1774900000000, "m": True}
    raw_text = orjson.dumps(raw).decode()
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1774900000000000000, "exchange_ts": 1774900000000,
        "collector_session_id": "test", "session_seq": seq,
        "raw_text": raw_text, "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "binance.trades", "_partition": 0, "_offset": seq,
    }


def test_deep_dry_run_shows_id_gaps(tmp_path):
    envs = [_make_trade_env(100, 0), _make_trade_env(101, 1), _make_trade_env(110, 2), _make_trade_env(111, 3)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    runner = CliRunner()
    result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path),
                                 "--exchange", "binance", "--symbol", "btcusdt",
                                 "--deep", "--dry-run"])
    assert result.exit_code == 0
    assert "id_gap" in result.output or "102" in result.output or "109" in result.output
