class TestBufferManager:
    def _make_envelope(self, exchange="binance", symbol="btcusdt", stream="trades",
                       received_at=1741689600_000_000_000, offset=0):
        """Helper to create a minimal envelope dict."""
        return {
            "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
            "stream": stream, "received_at": received_at,
            "exchange_ts": 100, "collector_session_id": "s", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
            "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": offset,
        }

    def test_add_message_to_buffer(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=30)
        env = self._make_envelope()
        bm.add(env)
        assert bm.total_buffered == 1

    def test_flush_threshold_triggers(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=2, flush_interval_seconds=9999)
        env1 = self._make_envelope(offset=0)
        env2 = self._make_envelope(offset=1)
        result1 = bm.add(env1)
        assert result1 is None  # no flush yet
        result2 = bm.add(env2)
        assert result2 is not None  # flush triggered
        assert len(result2) > 0

    def test_routing_by_received_at(self):
        """Messages route to file targets based on received_at timestamp, not wall clock."""
        from src.writer.buffer_manager import BufferManager
        import datetime

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        # Hour 14
        dt14 = datetime.datetime(2026, 3, 11, 14, 30, tzinfo=datetime.timezone.utc)
        ns14 = int(dt14.timestamp() * 1_000_000_000)
        # Hour 15
        dt15 = datetime.datetime(2026, 3, 11, 15, 10, tzinfo=datetime.timezone.utc)
        ns15 = int(dt15.timestamp() * 1_000_000_000)

        env14 = self._make_envelope(received_at=ns14, offset=0)
        env15 = self._make_envelope(received_at=ns15, offset=1)
        bm.add(env14)
        bm.add(env15)
        # Two different file targets
        assert len(bm._buffers) == 2

    def test_flush_all(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        for i in range(5):
            bm.add(self._make_envelope(offset=i))
        results = bm.flush_all()
        assert len(results) == 1  # all same target
        assert bm.total_buffered == 0


class TestFlushResultBackupSource:
    """FlushResult must expose whether the flush's last envelope came from a
    backup source, so the checkpoint writer can avoid poisoning the stream
    checkpoint's last_collector_session_id with a backup-collector session id
    during a failover window."""

    def _make_envelope(self, *, source=None, offset=0, session_id="primary-session"):
        env = {
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": "trades", "received_at": 1741689600_000_000_000,
            "exchange_ts": 100, "collector_session_id": session_id, "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
            "_topic": "binance.trades", "_partition": 0, "_offset": offset,
        }
        if source is not None:
            env["_source"] = source
        return env

    def test_flush_of_primary_only_has_backup_source_false(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        bm.add(self._make_envelope(offset=0))
        bm.add(self._make_envelope(offset=1))
        results = bm.flush_all()
        assert len(results) == 1
        assert results[0].has_backup_source is False

    def test_flush_last_envelope_backup_has_backup_source_true(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        bm.add(self._make_envelope(offset=0))
        bm.add(self._make_envelope(source="backup", offset=1, session_id="backup-session"))
        results = bm.flush_all()
        assert len(results) == 1
        assert results[0].has_backup_source is True

    def test_flush_last_envelope_primary_overrides_earlier_backup(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        bm.add(self._make_envelope(source="backup", offset=0, session_id="backup-session"))
        bm.add(self._make_envelope(offset=1))
        results = bm.flush_all()
        assert len(results) == 1
        assert results[0].has_backup_source is False
