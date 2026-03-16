import pytest


class TestWriterState:
    def test_state_record_creation(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=1000,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=52428,
        )
        assert state.topic == "binance.trades"
        assert state.partition == 0
        assert state.high_water_offset == 1000
        assert state.file_byte_size == 52428

    def test_state_key(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades", partition=0,
            high_water_offset=100,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=0,
        )
        assert state.state_key == ("binance.trades", 0, "/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst")

    def test_multiple_files_same_topic_partition(self):
        """Multiple files for same (topic, partition) must have distinct state keys."""
        from src.writer.state_manager import FileState

        s1 = FileState(topic="binance.trades", partition=0, high_water_offset=100,
                       file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=1000)
        s2 = FileState(topic="binance.trades", partition=0, high_water_offset=200,
                       file_path="/data/binance/ethusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=2000)
        assert s1.state_key != s2.state_key


class TestStateManagerSQL:
    """These tests verify SQL generation. Full integration is in test_writer_rotation.py."""

    def test_create_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_TABLE_SQL
        assert "writer_file_state" in sql
        assert "topic" in sql
        assert "partition" in sql
        assert "file_path" in sql
        assert "high_water_offset" in sql
        assert "file_byte_size" in sql

    def test_upsert_sql_file_path_in_pk(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_SQL
        assert "INSERT" in sql
        assert "ON CONFLICT" in sql
        assert "file_path" in sql
