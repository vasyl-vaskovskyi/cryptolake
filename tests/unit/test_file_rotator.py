import hashlib
from pathlib import Path


class TestFilePathGeneration:
    def test_data_file_path(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-11",
            hour=14,
        )
        assert path == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst")

    def test_late_file_path(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-11",
            hour=14,
            late_seq=1,
        )
        assert path == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.late-1.jsonl.zst")

    def test_path_all_lowercase(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="BTCUSDT",
            stream="depth_snapshot",
            date="2026-03-11",
            hour=0,
        )
        assert "BTCUSDT" not in str(path)
        assert "btcusdt" in str(path)

    def test_build_backfill_file_path_basic(self):
        from src.writer.file_rotator import build_backfill_file_path

        path = build_backfill_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-28",
            hour=16,
            backfill_seq=1,
        )
        assert str(path) == "/data/binance/btcusdt/trades/2026-03-28/hour-16.backfill-1.jsonl.zst"

    def test_build_backfill_file_path_increments(self):
        from src.writer.file_rotator import build_backfill_file_path

        path = build_backfill_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-28",
            hour=16,
            backfill_seq=3,
        )
        assert "backfill-3" in str(path)

    def test_sha256_sidecar_path(self):
        from src.writer.file_rotator import build_file_path, sidecar_path

        data_path = build_file_path("/data", "binance", "btcusdt", "trades", "2026-03-11", 14)
        sc = sidecar_path(data_path)
        assert sc == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst.sha256")


class TestFileTarget:
    def test_target_key(self):
        from src.writer.file_rotator import FileTarget

        t = FileTarget(exchange="binance", symbol="btcusdt", stream="trades",
                       date="2026-03-11", hour=14)
        assert t.key == ("binance", "btcusdt", "trades", "2026-03-11", 14)


class TestSHA256Sidecar:
    def test_compute_and_write_sha256(self, tmp_path):
        from src.writer.file_rotator import compute_sha256, write_sha256_sidecar

        # Write a test file
        test_file = tmp_path / "test.jsonl.zst"
        test_file.write_bytes(b"test data content")

        digest = compute_sha256(test_file)
        expected = hashlib.sha256(b"test data content").hexdigest()
        assert digest == expected

        sc_path = tmp_path / "test.jsonl.zst.sha256"
        write_sha256_sidecar(test_file, sc_path)
        assert sc_path.read_text().strip().startswith(expected)

    def test_sidecar_format(self, tmp_path):
        from src.writer.file_rotator import write_sha256_sidecar

        test_file = tmp_path / "hour-14.jsonl.zst"
        test_file.write_bytes(b"content")
        sc_path = tmp_path / "hour-14.jsonl.zst.sha256"
        write_sha256_sidecar(test_file, sc_path)

        content = sc_path.read_text().strip()
        # Format: "<hash>  <filename>"
        parts = content.split("  ")
        assert len(parts) == 2
        assert parts[1] == "hour-14.jsonl.zst"
