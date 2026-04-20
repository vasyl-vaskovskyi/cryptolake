# tests/unit/collector/test_tap.py
import json
from pathlib import Path

import pytest

from src.collector.tap import FrameTap


def test_tap_writes_raw_bytes_and_envelope(tmp_path: Path):
    tap = FrameTap(output_dir=tmp_path, stream="depth")
    raw = b'{"e":"depthUpdate","s":"BTCUSDT"}'
    envelope = {
        "v": 1,
        "type": "depth",
        "exchange": "binance",
        "symbol": "btcusdt",
        "stream": "depth",
        "received_at": "2026-04-18T00:00:00Z",
        "raw_text": raw.decode(),
        "raw_sha256": "deadbeef",
    }

    tap.write(raw, envelope)

    files = sorted(tmp_path.iterdir())
    assert len(files) == 2
    raw_file = next(f for f in files if f.suffix == ".raw")
    env_file = next(f for f in files if f.suffix == ".json")

    assert raw_file.read_bytes() == raw
    loaded = json.loads(env_file.read_text())
    assert loaded == envelope


def test_tap_filenames_are_time_ordered(tmp_path: Path):
    tap = FrameTap(output_dir=tmp_path, stream="trades")
    tap.write(b"a", {"n": 1})
    tap.write(b"b", {"n": 2})
    raws = sorted(p.name for p in tmp_path.glob("*.raw"))
    assert raws[0] < raws[1]


def test_tap_disabled_when_output_dir_none():
    tap = FrameTap(output_dir=None, stream="depth")
    # Must not raise; must be a no-op.
    tap.write(b"x", {"k": "v"})
