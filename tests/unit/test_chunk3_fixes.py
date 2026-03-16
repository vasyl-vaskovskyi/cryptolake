"""Tests for Chunk 3 review findings fixes."""
import asyncio

import pytest
from unittest.mock import MagicMock, AsyncMock, patch


class TestSnapshotSchedulerGracefulShutdown:
    """Finding 1: SnapshotScheduler._poll_loop blocks in asyncio.sleep()."""

    @pytest.mark.asyncio
    async def test_stop_interrupts_initial_delay(self):
        """stop() should wake up tasks blocked in initial delay sleep."""
        from src.collector.snapshot import SnapshotScheduler

        scheduler = SnapshotScheduler(
            exchange="binance",
            collector_session_id="s1",
            producer=MagicMock(),
            adapter=MagicMock(),
            depth_handler=MagicMock(),
            symbols=["btcusdt"],
            default_interval="5m",
        )
        scheduler._session = AsyncMock()
        scheduler._running = True
        scheduler._stop_event = asyncio.Event()
        scheduler._tasks = []

        # Start a poll loop with a very long initial delay
        task = asyncio.create_task(scheduler._poll_loop("btcusdt", 300, 9999))
        scheduler._tasks.append(task)

        # Give it a moment to enter the sleep
        await asyncio.sleep(0.05)

        # Stop should return quickly (not wait 9999s)
        await scheduler.stop()
        assert task.done()

    @pytest.mark.asyncio
    async def test_stop_interrupts_poll_interval(self):
        """stop() should wake up tasks blocked in poll interval sleep."""
        from src.collector.snapshot import SnapshotScheduler

        scheduler = SnapshotScheduler(
            exchange="binance",
            collector_session_id="s1",
            producer=MagicMock(),
            adapter=MagicMock(),
            depth_handler=MagicMock(),
            symbols=["btcusdt"],
            default_interval="5m",
        )
        scheduler._session = AsyncMock()
        scheduler._running = True
        scheduler._stop_event = asyncio.Event()
        scheduler._tasks = []

        # Mock _take_snapshot so the loop runs but doesn't do real HTTP
        scheduler._take_snapshot = AsyncMock()

        # Start a poll loop with no initial delay but 9999s interval
        task = asyncio.create_task(scheduler._poll_loop("btcusdt", 9999, 0))
        scheduler._tasks.append(task)

        # Wait for the first snapshot to complete
        await asyncio.sleep(0.05)
        assert scheduler._take_snapshot.called

        # Stop should return quickly
        await scheduler.stop()
        assert task.done()


class TestOpenInterestPollerGracefulShutdown:
    """Finding 1: OpenInterestPoller._poll_loop blocks in asyncio.sleep()."""

    @pytest.mark.asyncio
    async def test_stop_interrupts_poll_loop(self):
        """stop() should wake up tasks blocked in poll interval sleep."""
        from src.collector.streams.open_interest import OpenInterestPoller

        poller = OpenInterestPoller(
            exchange="binance",
            collector_session_id="s1",
            producer=MagicMock(),
            adapter=MagicMock(),
            symbols=["btcusdt"],
            poll_interval_seconds=9999,
        )
        poller._session = AsyncMock()
        poller._running = True
        poller._stop_event = asyncio.Event()
        poller._tasks = []

        # Mock _poll_once so we skip real HTTP
        poller._poll_once = AsyncMock()

        task = asyncio.create_task(poller._poll_loop("btcusdt", initial_delay=0))
        poller._tasks.append(task)

        await asyncio.sleep(0.05)
        assert poller._poll_once.called

        # Stop should return quickly
        await poller.stop()
        assert task.done()


class TestWebSocketManagerGracefulShutdown:
    """Finding 1: WebSocketManager.stop() only sets _running = False."""

    @pytest.mark.asyncio
    async def test_stop_cancels_tasks(self):
        """stop() should cancel connection loop tasks."""
        from src.collector.connection import WebSocketManager

        manager = WebSocketManager(
            exchange="binance",
            collector_session_id="s1",
            adapter=MagicMock(),
            producer=MagicMock(),
            handlers={},
            symbols=["btcusdt"],
            enabled_streams=["trades"],
        )
        manager._running = True

        # Create a fake long-running task
        async def _fake_loop():
            while True:
                await asyncio.sleep(9999)

        task = asyncio.create_task(_fake_loop())
        manager._tasks = [task]

        await manager.stop()
        assert task.done()
        assert manager._running is False


class TestDepthReplayGapDetection:
    """Finding 2: set_sync_point() replay silently swallows pu chain breaks."""

    def test_replay_detects_pu_chain_break(self):
        """Replayed diffs with pu chain break should emit gap and trigger callback."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        resync_called = []
        handler = DepthHandler(
            "binance", "session_1", producer, adapter, ["btcusdt"],
            on_pu_chain_break=lambda sym: resync_called.append(sym),
        )

        # Buffer diffs: first is valid sync, second has broken pu chain
        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1010,"u":1015,"pu":1008,"b":[],"a":[]}'
        handler._pending_diffs["btcusdt"] = [
            (raw1, 100, 0),
            (raw2, 101, 1),
        ]

        handler.set_sync_point("btcusdt", 1000)

        # First diff should be replayed (valid sync diff)
        # Second diff has pu=1008 but expected pu=1002 → gap
        calls = producer.produce.call_args_list
        assert len(calls) == 2  # one data envelope + one gap envelope

        # Second call should be a gap envelope
        gap_env = calls[1][0][0]
        assert gap_env["type"] == "gap"
        assert gap_env["reason"] == "pu_chain_break"
        assert "replay" in gap_env["detail"]

        # on_pu_chain_break should have been called
        assert resync_called == ["btcusdt"]

    def test_replay_no_gap_when_chain_valid(self):
        """Valid pu chain during replay should not emit gaps."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])

        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1003,"u":1005,"pu":1002,"b":[],"a":[]}'
        handler._pending_diffs["btcusdt"] = [
            (raw1, 100, 0),
            (raw2, 101, 1),
        ]

        handler.set_sync_point("btcusdt", 1000)

        # Both diffs valid, no gaps
        calls = producer.produce.call_args_list
        assert len(calls) == 2
        assert all(c[0][0]["type"] == "data" for c in calls)


class TestReadyEndpointNonBlocking:
    """Finding 3: /ready blocks the event loop with synchronous list_topics()."""

    @pytest.mark.asyncio
    async def test_ready_uses_executor(self):
        """_ready() should call is_connected() via run_in_executor."""
        from src.collector.main import Collector

        # Avoid full __init__ — just test the _ready method
        collector = Collector.__new__(Collector)
        collector.ws_manager = MagicMock()
        collector.ws_manager.is_connected.return_value = True
        collector.producer = MagicMock()
        collector.producer.is_connected.return_value = True

        request = MagicMock()

        # Patch run_in_executor to verify it's used
        loop = asyncio.get_running_loop()
        original_run_in_executor = loop.run_in_executor
        executor_calls = []

        async def tracking_executor(executor, fn, *args):
            executor_calls.append(fn)
            return await original_run_in_executor(executor, fn, *args)

        with patch.object(loop, 'run_in_executor', side_effect=tracking_executor):
            resp = await collector._ready(request)

        # Verify run_in_executor was called with producer.is_connected
        assert len(executor_calls) == 1
        assert executor_calls[0] == collector.producer.is_connected

        # Verify response is correct
        import json
        body = json.loads(resp.body)
        assert body["ws_connected"] is True
        assert body["producer_connected"] is True
        assert resp.status == 200
