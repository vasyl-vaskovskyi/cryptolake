"""Tests for collector lifecycle state recording.

Verifies that the Collector:
- upserts a ComponentRuntimeState row on startup (with host boot ID)
- sends periodic heartbeats
- marks clean shutdown on graceful stop
- attaches active maintenance intent to the clean shutdown marker
- handles PG unavailability gracefully (best-effort, non-blocking)
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


def _make_collector_with_mocked_deps():
    """Create a Collector instance with all external dependencies mocked out.

    Returns (collector, state_manager_mock).
    """
    with patch("src.collector.main.load_config") as mock_load_config, \
         patch("src.collector.main.CryptoLakeProducer"), \
         patch("src.collector.main.BinanceAdapter"), \
         patch("src.collector.main.WebSocketManager"):

        cfg = MagicMock()
        cfg.exchanges.binance.collector_id = "binance-collector-01"
        cfg.exchanges.binance.ws_base = "wss://fake"
        cfg.exchanges.binance.rest_base = "https://fake"
        cfg.exchanges.binance.symbols = ["btcusdt"]
        cfg.exchanges.binance.get_enabled_streams.return_value = ["trades"]
        cfg.exchanges.binance.depth.snapshot_interval = "5m"
        cfg.exchanges.binance.depth.snapshot_overrides = {}
        cfg.exchanges.binance.open_interest.poll_interval = "5m"
        cfg.redpanda.brokers = ["localhost:9092"]
        cfg.redpanda.producer.max_buffer = 100_000
        cfg.redpanda.producer.buffer_caps = {}
        cfg.redpanda.producer.default_stream_cap = 10_000
        cfg.monitoring.prometheus_port = 8000
        cfg.database.url = "postgresql://fake:fake@localhost/fake"
        mock_load_config.return_value = cfg

        from src.collector.main import Collector
        collector = Collector("config/config.yaml")

    # Create mock StateManager
    sm_mock = AsyncMock()
    sm_mock.connect = AsyncMock()
    sm_mock.close = AsyncMock()
    sm_mock.upsert_component_runtime = AsyncMock()
    sm_mock.mark_component_clean_shutdown = AsyncMock()
    sm_mock.create_maintenance_intent = AsyncMock()
    sm_mock.consume_maintenance_intent = AsyncMock()
    sm_mock.load_active_maintenance_intent = AsyncMock(return_value=None)

    collector._state_manager = sm_mock

    return collector, sm_mock


class TestCollectorStartupRegistration:
    """Collector startup should upsert a ComponentRuntimeState row."""

    @pytest.mark.asyncio
    async def test_startup_upserts_component_runtime(self):
        """On start, the collector should upsert a runtime state row with host boot ID."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        sm_mock.upsert_component_runtime.assert_called_once()
        call_args = sm_mock.upsert_component_runtime.call_args
        state = call_args[0][0] if call_args[0] else call_args[1].get("state")
        assert state.component == "collector"
        assert state.instance_id == collector.session_id
        assert state.host_boot_id == "boot-id-abc123"
        assert state.started_at is not None
        assert state.last_heartbeat_at is not None
        assert state.clean_shutdown_at is None

    @pytest.mark.asyncio
    async def test_startup_pg_failure_does_not_block(self):
        """If PG is unreachable on startup, collector should log warning and continue."""
        collector, sm_mock = _make_collector_with_mocked_deps()
        sm_mock.upsert_component_runtime.side_effect = Exception("PG down")

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            # Should not raise
            await collector._register_lifecycle_start()

        sm_mock.upsert_component_runtime.assert_called_once()


class TestCollectorHeartbeat:
    """Collector should periodically update last_heartbeat_at."""

    @pytest.mark.asyncio
    async def test_heartbeat_updates_runtime_state(self):
        """Heartbeat should call upsert_component_runtime with updated timestamp."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        # Reset to track heartbeat call
        sm_mock.upsert_component_runtime.reset_mock()

        await collector._send_heartbeat()

        sm_mock.upsert_component_runtime.assert_called_once()
        state = sm_mock.upsert_component_runtime.call_args[0][0]
        assert state.component == "collector"
        assert state.instance_id == collector.session_id

    @pytest.mark.asyncio
    async def test_heartbeat_pg_failure_logs_warning_and_continues(self):
        """If PG is unreachable during heartbeat, log warning and skip."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        sm_mock.upsert_component_runtime.reset_mock()
        sm_mock.upsert_component_runtime.side_effect = Exception("PG down")

        # Should not raise
        await collector._send_heartbeat()
        sm_mock.upsert_component_runtime.assert_called_once()

    @pytest.mark.asyncio
    async def test_heartbeat_loop_runs_periodically(self):
        """The heartbeat loop should call _send_heartbeat at the configured interval."""
        collector, sm_mock = _make_collector_with_mocked_deps()
        collector._heartbeat_interval = 0.05  # 50ms for test speed

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        sm_mock.upsert_component_runtime.reset_mock()

        # Start heartbeat loop, let it run for a short time, then cancel
        task = asyncio.create_task(collector._heartbeat_loop())
        await asyncio.sleep(0.15)  # Should get ~2-3 heartbeats
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert sm_mock.upsert_component_runtime.call_count >= 2


class TestCollectorCleanShutdown:
    """Collector shutdown should mark clean shutdown."""

    @pytest.mark.asyncio
    async def test_clean_shutdown_marks_component(self):
        """On graceful shutdown, collector should mark clean shutdown."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        await collector._mark_lifecycle_shutdown()

        sm_mock.mark_component_clean_shutdown.assert_called_once()
        call_kwargs = sm_mock.mark_component_clean_shutdown.call_args[1] if sm_mock.mark_component_clean_shutdown.call_args[1] else {}
        call_args = sm_mock.mark_component_clean_shutdown.call_args[0] if sm_mock.mark_component_clean_shutdown.call_args[0] else ()

        # Verify component and instance_id are passed
        if call_kwargs:
            assert call_kwargs["component"] == "collector"
            assert call_kwargs["instance_id"] == collector.session_id
        elif len(call_args) >= 2:
            assert call_args[0] == "collector"
            assert call_args[1] == collector.session_id

    @pytest.mark.asyncio
    async def test_clean_shutdown_with_maintenance_intent(self):
        """If a maintenance intent is active, attach it to the clean shutdown."""
        collector, sm_mock = _make_collector_with_mocked_deps()
        collector._active_maintenance_id = "deploy-2026-03-18T21-00Z"

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        await collector._mark_lifecycle_shutdown()

        sm_mock.mark_component_clean_shutdown.assert_called_once()
        call_kwargs = sm_mock.mark_component_clean_shutdown.call_args[1]
        assert call_kwargs.get("planned_shutdown") is True
        assert call_kwargs.get("maintenance_id") == "deploy-2026-03-18T21-00Z"

    @pytest.mark.asyncio
    async def test_shutdown_without_maintenance_is_unplanned(self):
        """Without active maintenance, shutdown is not marked as planned."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        await collector._mark_lifecycle_shutdown()

        call_kwargs = sm_mock.mark_component_clean_shutdown.call_args[1]
        assert call_kwargs.get("planned_shutdown") is False
        assert call_kwargs.get("maintenance_id") is None

    @pytest.mark.asyncio
    async def test_shutdown_pg_failure_does_not_block(self):
        """If PG is unreachable during shutdown, log warning and continue."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        sm_mock.mark_component_clean_shutdown.side_effect = Exception("PG down")

        # Should not raise
        await collector._mark_lifecycle_shutdown()
        sm_mock.mark_component_clean_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_absence_of_clean_shutdown_leaves_restart_unplanned(self):
        """If collector does not call _mark_lifecycle_shutdown, there is no clean shutdown record.

        This means the restart-gap classifier will classify it as unplanned.
        """
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        # Simulate crash: no _mark_lifecycle_shutdown called
        # The state should show no clean_shutdown_at
        state = sm_mock.upsert_component_runtime.call_args[0][0]
        assert state.clean_shutdown_at is None

        # And mark_component_clean_shutdown should NOT have been called
        sm_mock.mark_component_clean_shutdown.assert_not_called()


class TestCollectorLifecycleIntegration:
    """Integration-style tests verifying the full lifecycle is wired into start/shutdown."""

    @pytest.mark.asyncio
    async def test_shutdown_calls_lifecycle_methods(self):
        """Collector.shutdown() should call _mark_lifecycle_shutdown."""
        collector, sm_mock = _make_collector_with_mocked_deps()

        with patch("src.collector.main.get_host_boot_id", return_value="boot-id-abc123"):
            await collector._register_lifecycle_start()

        # Mock all the other shutdown deps
        collector.ws_manager = AsyncMock()
        collector.ws_manager.stop = AsyncMock()
        collector.snapshot_scheduler = None
        collector.oi_poller = None
        collector.producer = MagicMock()
        collector.producer.flush = MagicMock()
        collector._tasks = []

        # Cancel the heartbeat task if running
        collector._heartbeat_task = None

        await collector.shutdown()

        sm_mock.mark_component_clean_shutdown.assert_called_once()

    @pytest.mark.asyncio
    async def test_state_manager_initialization(self):
        """Collector should create a StateManager from config database.url."""
        with patch("src.collector.main.load_config") as mock_load_config, \
             patch("src.collector.main.CryptoLakeProducer"), \
             patch("src.collector.main.BinanceAdapter"), \
             patch("src.collector.main.WebSocketManager"):

            cfg = MagicMock()
            cfg.exchanges.binance.collector_id = "binance-collector-01"
            cfg.exchanges.binance.ws_base = "wss://fake"
            cfg.exchanges.binance.rest_base = "https://fake"
            cfg.exchanges.binance.symbols = ["btcusdt"]
            cfg.exchanges.binance.get_enabled_streams.return_value = ["trades"]
            cfg.exchanges.binance.depth.snapshot_interval = "5m"
            cfg.exchanges.binance.depth.snapshot_overrides = {}
            cfg.exchanges.binance.open_interest.poll_interval = "5m"
            cfg.redpanda.brokers = ["localhost:9092"]
            cfg.redpanda.producer.max_buffer = 100_000
            cfg.redpanda.producer.buffer_caps = {}
            cfg.redpanda.producer.default_stream_cap = 10_000
            cfg.monitoring.prometheus_port = 8000
            cfg.database.url = "postgresql://mydb:pass@host/db"
            mock_load_config.return_value = cfg

            from src.collector.main import Collector
            collector = Collector("config/config.yaml")

        assert collector._db_url == "postgresql://mydb:pass@host/db"
