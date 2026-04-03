import orjson
from src.writer.backup_recovery import _dedup_records, _determine_coverage, recover_from_backup


def test_recover_returns_empty_when_no_backup_configured():
    records, coverage = recover_from_backup(
        brokers=[], backup_topic_prefix="",
        stream="trades", symbol="btcusdt", exchange="binance",
        gap_start_ns=1000, gap_end_ns=2000,
    )
    assert records == []
    assert coverage == "none"


def test_dedup_trades_by_trade_id():
    records = [
        {"type": "data", "stream": "trades", "exchange_ts": 100, "raw_text": '{"a": 1}'},
        {"type": "data", "stream": "trades", "exchange_ts": 101, "raw_text": '{"a": 2}'},
        {"type": "data", "stream": "trades", "exchange_ts": 102, "raw_text": '{"a": 2}'},
        {"type": "data", "stream": "trades", "exchange_ts": 103, "raw_text": '{"a": 3}'},
    ]
    deduped = _dedup_records(records, "trades")
    assert len(deduped) == 3
    ids = [orjson.loads(r["raw_text"]).get("a") for r in deduped]
    assert ids == [1, 2, 3]


def test_dedup_depth_by_update_id():
    records = [
        {"type": "data", "stream": "depth", "exchange_ts": 100, "raw_text": '{"u": 10, "pu": 9}'},
        {"type": "data", "stream": "depth", "exchange_ts": 101, "raw_text": '{"u": 11, "pu": 10}'},
        {"type": "data", "stream": "depth", "exchange_ts": 102, "raw_text": '{"u": 11, "pu": 10}'},
    ]
    deduped = _dedup_records(records, "depth")
    assert len(deduped) == 2


def test_determine_coverage_full():
    gap_start = 1000_000_000_000
    gap_end = 2000_000_000_000
    records = [{"received_at": 1999_000_000_000}]
    coverage, new_start = _determine_coverage(records, gap_start, gap_end)
    assert coverage == "full"
    assert new_start is None


def test_determine_coverage_partial():
    gap_start = 1000_000_000_000
    gap_end = 3000_000_000_000
    records = [{"received_at": 1500_000_000_000}]
    coverage, new_start = _determine_coverage(records, gap_start, gap_end)
    assert coverage == "partial"
    assert new_start > gap_start


def test_determine_coverage_none():
    coverage, new_start = _determine_coverage([], 1000, 2000)
    assert coverage == "none"
    assert new_start is None
