import json
import zstandard
import orjson
from pathlib import Path

from datetime import datetime, timezone

from src.common.envelope import VALID_GAP_REASONS, create_gap_envelope
from src.cli.consolidate import (
    discover_hour_files,
    merge_hour,
    synthesize_missing_hour_gap,
    write_daily_file,
    verify_daily_file,
    write_manifest,
)
from src.writer.file_rotator import compute_sha256, write_sha256_sidecar, sidecar_path


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


# --- Task 3: Hour merging ---

def test_merge_hour_base_only(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    envs = [_make_data_env(exchange_ts=300), _make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)]
    _write_zst_file(date_dir / "hour-5.jsonl.zst", envs)
    file_group = {"base": date_dir / "hour-5.jsonl.zst", "late": [], "backfill": []}
    records = merge_hour(5, file_group)
    assert len(records) == 3
    assert records[0]["exchange_ts"] == 100
    assert records[1]["exchange_ts"] == 200
    assert records[2]["exchange_ts"] == 300


def test_merge_hour_with_late_and_backfill(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    _write_zst_file(date_dir / "hour-5.jsonl.zst", [_make_data_env(exchange_ts=100)])
    _write_zst_file(date_dir / "hour-5.late-1.jsonl.zst", [_make_data_env(exchange_ts=50)])
    _write_zst_file(date_dir / "hour-5.backfill-1.jsonl.zst", [_make_data_env(exchange_ts=75)])
    file_group = {
        "base": date_dir / "hour-5.jsonl.zst",
        "late": [date_dir / "hour-5.late-1.jsonl.zst"],
        "backfill": [date_dir / "hour-5.backfill-1.jsonl.zst"],
    }
    records = merge_hour(5, file_group)
    assert len(records) == 3
    assert records[0]["exchange_ts"] == 50
    assert records[1]["exchange_ts"] == 75
    assert records[2]["exchange_ts"] == 100


def test_merge_hour_preserves_gap_envelopes(tmp_path):
    date_dir = tmp_path / "2026-03-28"
    date_dir.mkdir()
    gap_env = {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 500,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 200_000_000_000, "gap_end_ts": 300_000_000_000,
        "reason": "ws_disconnect", "detail": "test gap",
    }
    data_env = _make_data_env(exchange_ts=100)
    _write_zst_file(date_dir / "hour-5.jsonl.zst", [data_env, gap_env])
    file_group = {"base": date_dir / "hour-5.jsonl.zst", "late": [], "backfill": []}
    records = merge_hour(5, file_group)
    assert len(records) == 2
    types = [r["type"] for r in records]
    assert "gap" in types
    assert "data" in types


# --- Task 4: Gap envelope synthesis for missing hours ---

def test_synthesize_missing_hour_gap_structure():
    env = synthesize_missing_hour_gap(
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
        hour=14,
        session_id="consolidation-2026-03-29T02:30:00Z",
    )
    assert env["v"] == 1
    assert env["type"] == "gap"
    assert env["exchange"] == "binance"
    assert env["symbol"] == "btcusdt"
    assert env["stream"] == "trades"
    assert env["reason"] == "missing_hour"
    assert env["session_seq"] == -1
    assert "hour 14" in env["detail"]
    expected_start = int(datetime(2026, 3, 28, 14, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    assert env["gap_start_ts"] == expected_start
    expected_end = int(datetime(2026, 3, 28, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000) - 1
    assert env["gap_end_ts"] == expected_end


def test_synthesize_missing_hour_gap_hour_boundaries():
    env = synthesize_missing_hour_gap(
        exchange="binance", symbol="btcusdt", stream="depth",
        date="2026-03-28", hour=0, session_id="consolidation-test",
    )
    expected_start = int(datetime(2026, 3, 28, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
    assert env["gap_start_ts"] == expected_start

    env = synthesize_missing_hour_gap(
        exchange="binance", symbol="btcusdt", stream="depth",
        date="2026-03-28", hour=23, session_id="consolidation-test",
    )
    expected_end = int(datetime(2026, 3, 29, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1_000_000_000) - 1
    assert env["gap_end_ts"] == expected_end


# --- Task 5: Streaming daily file writer ---

def test_write_daily_file_creates_compressed_output(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    records_by_hour = {
        0: [_make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)],
        1: [_make_data_env(exchange_ts=300)],
    }

    def hour_iterator():
        for h in range(24):
            if h in records_by_hour:
                yield h, records_by_hour[h]

    stats = write_daily_file(output_path, hour_iterator())
    assert output_path.exists()
    assert stats["total_records"] == 3
    assert stats["data_records"] == 3
    assert stats["gap_records"] == 0

    dctx = zstandard.ZstdDecompressor()
    with open(output_path, "rb") as fh:
        data = dctx.stream_reader(fh).read()
    lines = [l for l in data.strip().split(b"\n") if l]
    assert len(lines) == 3
    first = orjson.loads(lines[0])
    assert first["exchange_ts"] == 100


def test_write_daily_file_with_gap_envelopes(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    gap_env = {
        "v": 1, "type": "gap", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 500,
        "collector_session_id": "test", "session_seq": -1,
        "gap_start_ts": 200_000_000_000, "gap_end_ts": 300_000_000_000,
        "reason": "missing_hour", "detail": "test",
    }

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]
        yield 1, [gap_env]

    stats = write_daily_file(output_path, hour_iterator())
    assert stats["total_records"] == 2
    assert stats["data_records"] == 1
    assert stats["gap_records"] == 1
    assert stats["hours"][0]["data_records"] == 1
    assert stats["hours"][1]["data_records"] == 0


# --- Task 6: Daily file verification ---

def test_verify_daily_file_passes_valid_file(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"
    records = [_make_data_env(exchange_ts=100), _make_data_env(exchange_ts=200)]

    def hour_iterator():
        yield 0, records

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=2, sha256_path=sc)
    assert ok is True
    assert error is None


def test_verify_daily_file_fails_on_wrong_count(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=999, sha256_path=sc)
    assert ok is False
    assert "count" in error.lower()


def test_verify_daily_file_fails_on_wrong_sha256(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    def hour_iterator():
        yield 0, [_make_data_env(exchange_ts=100)]

    write_daily_file(output_path, hour_iterator())
    sc = sidecar_path(output_path)
    sc.write_text("0000bad  2026-03-28.jsonl.zst\n")

    ok, error = verify_daily_file(output_path, expected_count=1, sha256_path=sc)
    assert ok is False
    assert "sha256" in error.lower()


def test_verify_daily_file_fails_on_decreasing_ts(tmp_path):
    output_path = tmp_path / "2026-03-28.jsonl.zst"

    cctx = zstandard.ZstdCompressor(level=3)
    records = [_make_data_env(exchange_ts=200), _make_data_env(exchange_ts=100)]
    data = b"\n".join(orjson.dumps(r) for r in records) + b"\n"
    output_path.write_bytes(cctx.compress(data))

    sc = sidecar_path(output_path)
    write_sha256_sidecar(output_path, sc)

    ok, error = verify_daily_file(output_path, expected_count=2, sha256_path=sc)
    assert ok is False
    assert "order" in error.lower()


# --- Task 7: Manifest writer ---

def test_write_manifest_structure(tmp_path):
    manifest_path = tmp_path / "2026-03-28.manifest.json"
    write_manifest(
        manifest_path=manifest_path,
        exchange="binance",
        symbol="btcusdt",
        stream="trades",
        date="2026-03-28",
        daily_file_name="2026-03-28.jsonl.zst",
        daily_file_sha256="abc123",
        stats={
            "total_records": 100,
            "data_records": 98,
            "gap_records": 2,
            "hours": {
                0: {"data_records": 50, "gap_records": 0},
                1: {"data_records": 48, "gap_records": 0},
                14: {"data_records": 0, "gap_records": 2},
            },
        },
        hour_details={
            0: {"status": "present", "sources": ["hour-0.jsonl.zst"]},
            1: {"status": "present", "sources": ["hour-1.jsonl.zst", "hour-1.late-1.jsonl.zst"]},
            14: {"status": "missing", "synthesized_gap": True},
        },
        source_files=["hour-0.jsonl.zst", "hour-1.jsonl.zst", "hour-1.late-1.jsonl.zst"],
        missing_hours=[14],
    )

    assert manifest_path.exists()
    m = json.loads(manifest_path.read_text())
    assert m["version"] == 1
    assert m["exchange"] == "binance"
    assert m["symbol"] == "btcusdt"
    assert m["stream"] == "trades"
    assert m["date"] == "2026-03-28"
    assert m["daily_file"] == "2026-03-28.jsonl.zst"
    assert m["daily_file_sha256"] == "abc123"
    assert m["total_records"] == 100
    assert m["data_records"] == 98
    assert m["gap_records"] == 2
    assert m["missing_hours"] == [14]
    assert "14" in m["hours"]
    assert m["hours"]["14"]["status"] == "missing"
    assert "consolidated_at" in m
