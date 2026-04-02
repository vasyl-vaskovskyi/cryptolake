import zstandard
import orjson
from pathlib import Path

from src.common.envelope import VALID_GAP_REASONS, create_gap_envelope
from src.cli.consolidate import discover_hour_files


def _make_data_env(exchange_ts=1000):
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1000, "exchange_ts": exchange_ts,
        "collector_session_id": "test", "session_seq": 0,
        "raw_text": "{}", "raw_sha256": "abc",
        "_topic": "binance.trades", "_partition": 0, "_offset": 0,
    }


def _write_zst_file(path: Path, envelopes: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    cctx = zstandard.ZstdCompressor()
    data = b"\n".join(orjson.dumps(e) for e in envelopes)
    path.write_bytes(cctx.compress(data))


def test_missing_hour_is_valid_gap_reason():
    assert "missing_hour" in VALID_GAP_REASONS


def test_create_gap_envelope_with_missing_hour():
    env = create_gap_envelope(
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        collector_session_id="consolidation-2026-03-29T02:30:00Z",
        session_seq=-1,
        gap_start_ts=1711670400_000_000_000,
        gap_end_ts=1711673999_999_999_999,
        reason="missing_hour",
        detail="No data files found for hour 14; not recoverable via backfill",
    )
    assert env["type"] == "gap"
    assert env["reason"] == "missing_hour"
    assert env["gap_start_ts"] == 1711670400_000_000_000
    assert env["gap_end_ts"] == 1711673999_999_999_999


# --- Task 2: File discovery and classification ---

def test_discover_hour_files_groups_correctly(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    _write_zst_file(date_dir / "hour-0.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.late-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.backfill-1.jsonl.zst", [_make_data_env()])
    _write_zst_file(date_dir / "hour-1.backfill-2.jsonl.zst", [_make_data_env()])
    (date_dir / "hour-0.jsonl.zst.sha256").write_text("abc  hour-0.jsonl.zst\n")

    result = discover_hour_files(date_dir)
    assert 0 in result
    assert result[0]["base"] == date_dir / "hour-0.jsonl.zst"
    assert result[0]["late"] == []
    assert result[0]["backfill"] == []
    assert 1 in result
    assert result[1]["base"] == date_dir / "hour-1.jsonl.zst"
    assert result[1]["late"] == [date_dir / "hour-1.late-1.jsonl.zst"]
    assert len(result[1]["backfill"]) == 2
    assert 2 not in result


def test_discover_hour_files_empty_dir(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    result = discover_hour_files(date_dir)
    assert result == {}


def test_discover_hour_files_backfill_only(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    _write_zst_file(date_dir / "hour-5.backfill-1.jsonl.zst", [_make_data_env()])
    result = discover_hour_files(date_dir)
    assert 5 in result
    assert result[5]["base"] is None
    assert result[5]["backfill"] == [date_dir / "hour-5.backfill-1.jsonl.zst"]
