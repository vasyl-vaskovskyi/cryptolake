from pathlib import Path
import hashlib
import zstandard
import orjson
from src.cli.integrity import find_backfillable_gaps

def _write_hour_file(base, exchange, symbol, stream, date, hour, envelopes):
    dir_path = base / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    (dir_path / f"hour-{hour}.jsonl.zst").write_bytes(cctx.compress(data))

def _make_trade_env(a, session_seq=0):
    raw = {"e": "aggTrade", "a": a, "s": "BTCUSDT", "p": "67000", "q": "0.1", "T": 1774900000000, "m": True}
    raw_text = orjson.dumps(raw).decode()
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1774900000000000000, "exchange_ts": 1774900000000,
        "collector_session_id": "test", "session_seq": session_seq,
        "raw_text": raw_text, "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
        "_topic": "binance.trades", "_partition": 0, "_offset": session_seq,
    }

def test_find_backfillable_gaps_detects_trade_id_gap(tmp_path):
    envs = [_make_trade_env(a=100+i, session_seq=i) for i in range(3)] + [_make_trade_env(a=110, session_seq=3), _make_trade_env(a=111, session_seq=4)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 1
    assert gaps[0]["type"] == "id_gap"
    assert gaps[0]["stream"] == "trades"
    assert gaps[0]["from_id"] == 103
    assert gaps[0]["to_id"] == 109
    assert gaps[0]["missing"] == 7

def test_find_backfillable_gaps_no_gaps(tmp_path):
    envs = [_make_trade_env(a=100+i, session_seq=i) for i in range(5)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "trades", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0

def test_find_backfillable_gaps_ignores_depth(tmp_path):
    envs = [_make_trade_env(a=100, session_seq=0)]
    _write_hour_file(tmp_path, "binance", "btcusdt", "depth", "2026-03-30", 20, envs)
    gaps = find_backfillable_gaps(tmp_path, exchange="binance", symbol="btcusdt")
    assert len(gaps) == 0
