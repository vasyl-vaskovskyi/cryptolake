from __future__ import annotations

import datetime
import hashlib

import orjson
import pytest
import zstandard as zstd

from src.writer.buffer_manager import BufferManager
from src.writer.compressor import ZstdFrameCompressor
from src.writer.file_rotator import compute_sha256, sidecar_path, write_sha256_sidecar


@pytest.mark.integration
class TestWriterFileOutput:
    def test_full_write_cycle(self, tmp_path) -> None:
        buffer_manager = BufferManager(
            base_dir=str(tmp_path),
            flush_messages=3,
            flush_interval_seconds=9999,
        )
        compressor = ZstdFrameCompressor(level=3)

        dt = datetime.datetime(2026, 3, 11, 14, 30, tzinfo=datetime.timezone.utc)
        ns = int(dt.timestamp() * 1_000_000_000)

        for offset in range(3):
            raw_text = f'{{"seq":{offset}}}'
            envelope = {
                "v": 1,
                "type": "data",
                "exchange": "binance",
                "symbol": "btcusdt",
                "stream": "trades",
                "received_at": ns + offset,
                "exchange_ts": 100,
                "collector_session_id": "s",
                "session_seq": offset,
                "raw_text": raw_text,
                "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
                "_topic": "binance.trades",
                "_partition": 0,
                "_offset": offset,
            }
            result = buffer_manager.add(envelope)

        assert result is not None
        assert len(result) == 1

        flush_result = result[0]
        flush_result.file_path.parent.mkdir(parents=True, exist_ok=True)
        compressed = compressor.compress_frame(flush_result.lines)
        flush_result.file_path.write_bytes(compressed)

        checksum_path = sidecar_path(flush_result.file_path)
        write_sha256_sidecar(flush_result.file_path, checksum_path)

        assert flush_result.file_path.exists()
        assert checksum_path.exists()
        assert compute_sha256(flush_result.file_path) == checksum_path.read_text().split()[0]

        lines = zstd.ZstdDecompressor().decompress(flush_result.file_path.read_bytes()).strip().split(b"\n")
        assert len(lines) == 3
        for offset, line in enumerate(lines):
            parsed = orjson.loads(line)
            assert parsed["_offset"] == offset
