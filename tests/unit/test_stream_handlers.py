import pytest
from unittest.mock import MagicMock


class TestTradesHandler:
    @pytest.mark.asyncio
    async def test_produces_data_envelope(self):
        from src.collector.streams.simple import SimpleStreamHandler

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        handler = SimpleStreamHandler("binance", "session_1", producer, "trades")

        await handler.handle("btcusdt", '{"e":"aggTrade"}', 100, 0)
        producer.produce.assert_called_once()
        env = producer.produce.call_args[0][0]
        assert env["stream"] == "trades"
        assert env["symbol"] == "btcusdt"
        assert env["raw_text"] == '{"e":"aggTrade"}'


class TestDepthHandler:
    @pytest.mark.asyncio
    async def test_buffers_before_sync(self):
        """Diffs arriving before sync point should be buffered, not dropped."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_not_called()
        # But the diff should be in the pending buffer
        assert len(handler._pending_diffs["btcusdt"]) == 1

    @pytest.mark.asyncio
    async def test_accepts_after_sync(self):
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])
        handler.set_sync_point("btcusdt", 1000)

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_called_once()

    @pytest.mark.asyncio
    async def test_drops_stale_diffs(self):
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])
        handler.set_sync_point("btcusdt", 1000)

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":990,"u":995,"pu":989,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_pending_diffs_replayed_after_sync(self):
        """Buffered diffs should be replayed when sync point is set."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])

        # Buffer diffs while unsynced
        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1003,"u":1005,"pu":1002,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw1, 100, 0)
        await handler.handle("btcusdt", raw2, 101, 1)
        assert producer.produce.call_count == 0
        assert len(handler._pending_diffs["btcusdt"]) == 2

        # Sync point triggers replay
        handler.set_sync_point("btcusdt", 1000)
        # First diff syncs (U<=1001, u>=1001), second chains (pu==1002)
        assert producer.produce.call_count == 2
        assert len(handler._pending_diffs["btcusdt"]) == 0

    @pytest.mark.asyncio
    async def test_pu_chain_break_calls_callback(self):
        """pu_chain_break should trigger resync callback."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        resync_called = []
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"],
                               on_pu_chain_break=lambda sym: resync_called.append(sym))
        handler.set_sync_point("btcusdt", 1000)

        # First valid diff
        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw1, 100, 0)
        # Broken chain
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1010,"u":1015,"pu":1008,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw2, 101, 1)
        assert resync_called == ["btcusdt"]


class TestWriterSessionChangeDetection:
    """Writer-side detection of collector session changes (crash/SIGKILL coverage)."""

    def _make_consumer(self):
        from unittest.mock import AsyncMock
        from src.writer.consumer import WriterConsumer

        consumer = WriterConsumer.__new__(WriterConsumer)
        consumer._last_session = {}
        consumer._recovery_gap_emitted = set()
        consumer._current_boot_id = "boot-1"
        consumer._previous_writer_state = None
        consumer._host_evidence = None
        # Mock state_manager for DB queries in _check_session_change
        consumer.state_manager = AsyncMock()
        consumer.state_manager.load_component_state_by_instance = AsyncMock(return_value=None)
        consumer.state_manager.load_active_maintenance_intent = AsyncMock(return_value=None)
        return consumer

    def _make_envelope(self, session_id="s1", received_at=1000, stream="trades"):
        return {
            "type": "data",
            "exchange": "binance",
            "symbol": "btcusdt",
            "stream": stream,
            "collector_session_id": session_id,
            "received_at": received_at,
        }

    @pytest.mark.asyncio
    async def test_first_message_no_gap(self):
        consumer = self._make_consumer()
        env = self._make_envelope()
        result = await consumer._check_session_change(env)
        assert result is None

    @pytest.mark.asyncio
    async def test_same_session_no_gap(self):
        consumer = self._make_consumer()
        await consumer._check_session_change(self._make_envelope(session_id="s1", received_at=1000))
        result = await consumer._check_session_change(self._make_envelope(session_id="s1", received_at=2000))
        assert result is None

    @pytest.mark.asyncio
    async def test_session_change_emits_gap(self):
        consumer = self._make_consumer()
        await consumer._check_session_change(self._make_envelope(session_id="s1", received_at=1000))
        result = await consumer._check_session_change(self._make_envelope(session_id="s2", received_at=5000))
        assert result is not None
        assert result["type"] == "gap"
        assert result["reason"] == "restart_gap"
        assert result["gap_start_ts"] == 1000
        assert result["gap_end_ts"] == 5000
        assert "s1" in result["detail"]
        assert "s2" in result["detail"]

    @pytest.mark.asyncio
    async def test_session_change_per_stream(self):
        """Session tracking is per (exchange, symbol, stream) — different streams are independent."""
        consumer = self._make_consumer()
        await consumer._check_session_change(self._make_envelope(session_id="s1", stream="trades"))
        await consumer._check_session_change(self._make_envelope(session_id="s1", stream="depth"))
        # Change session for trades only
        result_trades = await consumer._check_session_change(
            self._make_envelope(session_id="s2", stream="trades", received_at=2000)
        )
        result_depth = await consumer._check_session_change(
            self._make_envelope(session_id="s1", stream="depth", received_at=2000)
        )
        assert result_trades is not None
        assert result_trades["stream"] == "trades"
        assert result_depth is None


class TestProducerOverflow:
    def test_buffer_error_increments_dropped(self):
        """When confluent_kafka raises BufferError, produce returns False and tracks overflow window."""
        import threading
        from unittest.mock import MagicMock
        from src.collector.producer import CryptoLakeProducer
        from src.common.envelope import create_data_envelope

        mock_instance = MagicMock()
        mock_instance.produce.side_effect = BufferError("queue full")

        producer = CryptoLakeProducer.__new__(CryptoLakeProducer)
        producer.exchange = "binance"
        producer.collector_session_id = "s"
        producer._producer = mock_instance
        producer._on_overflow = None
        producer._overflow_windows = {}
        producer._overflow_seq = 0
        producer._buffer_counts = {}
        producer._lock = threading.Lock()
        producer.buffer_caps = {"depth": 80_000, "trades": 10_000}
        producer.other_cap = 10_000
        producer.max_buffer = 100_000

        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text="{}", exchange_ts=0,
            collector_session_id="s", session_seq=0,
        )
        result = producer.produce(env)
        assert result is False
        # Overflow window should be tracked with drop count
        assert ("btcusdt", "trades") in producer._overflow_windows
        assert producer._overflow_windows[("btcusdt", "trades")]["dropped"] == 1
