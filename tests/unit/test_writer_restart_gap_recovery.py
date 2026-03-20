"""Unit tests for writer restart-gap recovery using durable stream checkpoints.

Tests prove:
- Collector session change after writer restart emits restart_gap (not collector_restart)
- Boot ID change emits component=host, cause=host_reboot
- Valid maintenance intent emits planned=true, cause=operator_shutdown
- First post-recovery record sets gap_end_ts
- Checkpoint updates happen only after durable write/commit
- REST-polled streams detect gaps via time delta when session_id unchanged
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.envelope import create_data_envelope
from src.writer.buffer_manager import BufferManager, CheckpointMeta, FlushResult
from src.writer.state_manager import (
    ComponentRuntimeState,
    MaintenanceIntent,
    StreamCheckpoint,
    StateManager,
)


def _make_data_envelope(
    *,
    exchange: str = "binance",
    symbol: str = "btcusdt",
    stream: str = "trades",
    collector_session_id: str = "session-new",
    session_seq: int = 1,
    received_at: int | None = None,
    offset: int = 100,
    partition: int = 0,
) -> dict:
    """Create a data envelope with broker coordinates already stamped."""
    if received_at is None:
        received_at = time.time_ns()
    return {
        "v": 1,
        "type": "data",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": received_at,
        "exchange_ts": 100,
        "collector_session_id": collector_session_id,
        "session_seq": session_seq,
        "raw_text": "{}",
        "raw_sha256": "abc123",
        "_topic": f"{exchange}.{stream}",
        "_partition": partition,
        "_offset": offset,
    }


def _make_checkpoint(
    *,
    exchange: str = "binance",
    symbol: str = "btcusdt",
    stream: str = "trades",
    last_received_at: str = "2026-03-18T10:00:00+00:00",
    last_collector_session_id: str = "session-old",
    last_gap_reason: str | None = None,
) -> StreamCheckpoint:
    return StreamCheckpoint(
        exchange=exchange,
        symbol=symbol,
        stream=stream,
        last_received_at=last_received_at,
        last_collector_session_id=last_collector_session_id,
        last_gap_reason=last_gap_reason,
    )


def _make_intent(
    *,
    expires_in_minutes: int = 30,
    scope: str = "full_stack",
) -> MaintenanceIntent:
    now = datetime.now(timezone.utc)
    return MaintenanceIntent(
        maintenance_id="maint-001",
        scope=scope,
        planned_by="operator",
        reason="scheduled update",
        created_at=(now - timedelta(minutes=5)).isoformat(),
        expires_at=(now + timedelta(minutes=expires_in_minutes)).isoformat(),
        consumed_at=None,
    )


def _make_component_state(
    *,
    component: str = "writer",
    host_boot_id: str = "boot-old",
    clean_shutdown_at: str | None = None,
    planned_shutdown: bool = False,
    maintenance_id: str | None = None,
) -> ComponentRuntimeState:
    now = datetime.now(timezone.utc)
    return ComponentRuntimeState(
        component=component,
        instance_id=f"{component}_2026-03-18T10:00:00Z",
        host_boot_id=host_boot_id,
        started_at=(now - timedelta(hours=1)).isoformat(),
        last_heartbeat_at=now.isoformat(),
        clean_shutdown_at=clean_shutdown_at,
        planned_shutdown=planned_shutdown,
        maintenance_id=maintenance_id,
    )


class TestRecoveryGapClassification:
    """After writer restart, the first envelope per stream should produce a
    restart_gap based on durable checkpoint comparison, NOT collector_restart."""

    def test_session_change_emits_restart_gap_not_collector_restart(self):
        """Core requirement: after writer restart, session change produces
        reason=restart_gap with structured metadata, NOT the old collector_restart."""
        from src.writer.consumer import WriterConsumer

        # Set up consumer with mocked dependencies
        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        # Load durable checkpoints (simulating a previous run)
        checkpoint = _make_checkpoint(last_collector_session_id="session-old")
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): checkpoint,
        }
        consumer._recovery_done = set()  # no streams have been recovered yet

        # Set boot ID for classification
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-aaa"
        )

        # Create incoming envelope with different session
        envelope = _make_data_envelope(collector_session_id="session-new")

        # Call recovery check
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["reason"] == "restart_gap"
        assert gap["reason"] != "collector_restart"
        assert "component" in gap
        assert "cause" in gap
        assert "planned" in gap
        assert "classifier" in gap

    def test_boot_id_change_emits_host_reboot(self):
        """When boot ID changed between runs, gap should have component=host, cause=host_reboot."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        checkpoint = _make_checkpoint(last_collector_session_id="session-old")
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-bbb"  # Different from previous
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-old"
        )

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["component"] == "host"
        assert gap["cause"] == "host_reboot"
        assert gap["planned"] is False

    def test_maintenance_intent_emits_planned_shutdown(self):
        """Valid maintenance intent should produce planned=true, cause=operator_shutdown."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        checkpoint = _make_checkpoint(last_collector_session_id="session-old")
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer",
            host_boot_id="boot-aaa",
            clean_shutdown_at=datetime.now(timezone.utc).isoformat(),
            planned_shutdown=True,
        )
        consumer._previous_collector_state = _make_component_state(
            component="collector",
            host_boot_id="boot-aaa",
            clean_shutdown_at=datetime.now(timezone.utc).isoformat(),
            planned_shutdown=True,
        )
        consumer._maintenance_intent = _make_intent()

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["planned"] is True
        assert gap["cause"] == "operator_shutdown"
        assert gap["component"] == "system"

    def test_first_post_recovery_record_sets_gap_end_ts(self):
        """gap_end_ts should be set to the received_at of the first post-recovery envelope."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        now_ns = time.time_ns()
        checkpoint = _make_checkpoint(
            last_collector_session_id="session-old",
            last_received_at="2026-03-18T10:00:00+00:00",
        )
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-aaa"
        )

        envelope = _make_data_envelope(
            collector_session_id="session-new",
            received_at=now_ns,
        )
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["gap_end_ts"] == now_ns

    def test_recovery_runs_once_per_stream(self):
        """After the first envelope is processed for a stream, recovery should not
        run again for that same stream."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        checkpoint = _make_checkpoint(last_collector_session_id="session-old")
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-aaa"
        )

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap1 = consumer._check_recovery_gap(envelope)
        assert gap1 is not None  # first call produces gap

        gap2 = consumer._check_recovery_gap(envelope)
        assert gap2 is None  # second call: recovery already done

    def test_no_durable_checkpoint_no_recovery_gap(self):
        """If there is no durable checkpoint for a stream (first-ever run),
        no recovery gap should be emitted."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        consumer._durable_checkpoints = {}  # no checkpoints
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = None

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap = consumer._check_recovery_gap(envelope)

        assert gap is None


class TestRestPolledStreamRecovery:
    """REST-polled streams (e.g., open_interest) may not have session_id changes
    when only the writer restarts. Detect gaps via time delta."""

    def test_time_delta_gap_detection_for_rest_polled_stream(self):
        """When session_id is unchanged but the time delta exceeds the expected
        poll interval, a restart_gap should be emitted."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.open_interest"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        # Checkpoint from 30 minutes ago -- same session
        old_ts = datetime.now(timezone.utc) - timedelta(minutes=30)
        checkpoint = _make_checkpoint(
            stream="open_interest",
            last_collector_session_id="session-same",
            last_received_at=old_ts.isoformat(),
        )
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "open_interest"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-aaa"
        )
        # Configure poll interval for open_interest
        consumer._rest_poll_interval_ns = 5 * 60 * 1_000_000_000  # 5 minutes in ns

        envelope = _make_data_envelope(
            stream="open_interest",
            collector_session_id="session-same",  # same session
            received_at=time.time_ns(),
        )
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["reason"] == "restart_gap"

    def test_no_gap_when_within_poll_interval(self):
        """When time delta is within the expected poll interval, no gap should be emitted
        even if session_id is the same (normal REST poll behavior)."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.open_interest"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        # Checkpoint from 3 minutes ago -- same session, within 5m poll interval
        recent_ts = datetime.now(timezone.utc) - timedelta(minutes=3)
        checkpoint = _make_checkpoint(
            stream="open_interest",
            last_collector_session_id="session-same",
            last_received_at=recent_ts.isoformat(),
        )
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "open_interest"): checkpoint,
        }
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-aaa"
        )
        consumer._rest_poll_interval_ns = 5 * 60 * 1_000_000_000

        envelope = _make_data_envelope(
            stream="open_interest",
            collector_session_id="session-same",
            received_at=time.time_ns(),
        )
        gap = consumer._check_recovery_gap(envelope)

        assert gap is None


class TestCheckpointMetaInFlushResult:
    """FlushResult should carry CheckpointMeta extracted from the last envelope."""

    def test_flush_result_has_checkpoint_meta(self):
        """After flush, FlushResult should contain CheckpointMeta from the last envelope."""
        bm = BufferManager(base_dir="/data", flush_messages=2)
        env1 = _make_data_envelope(
            collector_session_id="session-A",
            session_seq=1,
            received_at=1000000000_000_000_000,
            offset=0,
        )
        env2 = _make_data_envelope(
            collector_session_id="session-A",
            session_seq=2,
            received_at=1000000001_000_000_000,
            offset=1,
        )
        bm.add(env1)
        results = bm.add(env2)

        assert results is not None
        assert len(results) == 1
        result = results[0]
        assert result.checkpoint_meta is not None
        assert isinstance(result.checkpoint_meta, CheckpointMeta)
        assert result.checkpoint_meta.last_received_at == 1000000001_000_000_000
        assert result.checkpoint_meta.last_collector_session_id == "session-A"
        assert result.checkpoint_meta.last_session_seq == 2
        assert result.checkpoint_meta.stream_key == ("binance", "btcusdt", "trades")

    def test_flush_all_has_checkpoint_meta(self):
        """flush_all() should also produce FlushResult with CheckpointMeta."""
        bm = BufferManager(base_dir="/data", flush_messages=10_000)
        env = _make_data_envelope(
            collector_session_id="session-B",
            session_seq=5,
            received_at=1000000002_000_000_000,
            offset=10,
        )
        bm.add(env)
        results = bm.flush_all()

        assert len(results) == 1
        assert results[0].checkpoint_meta.last_collector_session_id == "session-B"
        assert results[0].checkpoint_meta.last_session_seq == 5


class TestCheckpointPersistenceAfterCommit:
    """Checkpoint updates should happen only after durable write/commit."""

    @pytest.mark.asyncio
    async def test_checkpoint_saved_in_commit_state(self):
        """save_stream_checkpoints should be called inside _commit_state,
        not before the durable write."""
        from src.writer.consumer import WriterConsumer

        state_manager = AsyncMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )
        consumer._consumer = MagicMock()  # mock Kafka consumer
        consumer._durable_checkpoints = {}
        consumer._recovery_done = set()

        # Create a FlushResult with checkpoint meta
        from src.writer.file_rotator import FileTarget
        from pathlib import Path

        meta = CheckpointMeta(
            last_received_at=1000000000_000_000_000,
            last_collector_session_id="session-A",
            last_session_seq=10,
            stream_key=("binance", "btcusdt", "trades"),
        )
        flush_result = FlushResult(
            target=FileTarget("binance", "btcusdt", "trades", "2026-03-18", 10),
            file_path=Path("/data/binance/btcusdt/trades/2026-03-18/hour-10.jsonl.zst"),
            lines=[b'{"test": 1}\n'],
            high_water_offset=100,
            partition=0,
            count=1,
            checkpoint_meta=meta,
        )

        from src.writer.state_manager import FileState
        states = [FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=100,
            file_path="/data/binance/btcusdt/trades/2026-03-18/hour-10.jsonl.zst",
            file_byte_size=512,
        )]

        await consumer._commit_state(states, [flush_result], time.monotonic())

        # save_states_and_checkpoints should have been called (atomic transaction)
        state_manager.save_states_and_checkpoints.assert_called_once()
        saved_checkpoints = state_manager.save_states_and_checkpoints.call_args[0][1]
        assert len(saved_checkpoints) == 1
        assert saved_checkpoints[0].exchange == "binance"
        assert saved_checkpoints[0].symbol == "btcusdt"
        assert saved_checkpoints[0].stream == "trades"

    @pytest.mark.asyncio
    async def test_in_memory_checkpoint_updated_after_commit(self):
        """The in-memory _durable_checkpoints cache should update only after
        successful commit."""
        from src.writer.consumer import WriterConsumer

        state_manager = AsyncMock(spec=StateManager)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )
        consumer._consumer = MagicMock()
        consumer._durable_checkpoints = {}
        consumer._recovery_done = set()

        from src.writer.file_rotator import FileTarget
        from pathlib import Path

        meta = CheckpointMeta(
            last_received_at=1000000000_000_000_000,
            last_collector_session_id="session-A",
            last_session_seq=10,
            stream_key=("binance", "btcusdt", "trades"),
        )
        flush_result = FlushResult(
            target=FileTarget("binance", "btcusdt", "trades", "2026-03-18", 10),
            file_path=Path("/data/binance/btcusdt/trades/2026-03-18/hour-10.jsonl.zst"),
            lines=[b'{"test": 1}\n'],
            high_water_offset=100,
            partition=0,
            count=1,
            checkpoint_meta=meta,
        )

        from src.writer.state_manager import FileState
        states = [FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=100,
            file_path="/data/binance/btcusdt/trades/2026-03-18/hour-10.jsonl.zst",
            file_byte_size=512,
        )]

        await consumer._commit_state(states, [flush_result], time.monotonic())

        # In-memory cache should now have the checkpoint
        key = ("binance", "btcusdt", "trades")
        assert key in consumer._durable_checkpoints
        assert consumer._durable_checkpoints[key].last_collector_session_id == "session-A"


class TestRuntimeSessionDetectionPreserved:
    """The existing _check_session_change() should still work for detecting
    session changes during normal (non-recovery) operation."""

    @pytest.mark.asyncio
    async def test_runtime_session_change_still_works(self):
        """After recovery, _check_session_change should still detect collector
        session changes during normal runtime."""
        from src.writer.consumer import WriterConsumer

        state_manager = MagicMock(spec=StateManager)
        # Mock async DB calls used by the runtime path
        state_manager.load_component_state_by_instance = AsyncMock(return_value=None)
        state_manager.load_active_maintenance_intent = AsyncMock(return_value=None)
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        # Simulate normal runtime -- first envelope sets session
        env1 = _make_data_envelope(collector_session_id="session-A")
        result1 = await consumer._check_session_change(env1)
        assert result1 is None  # first message, no prior session

        # Same session -- no gap
        env2 = _make_data_envelope(collector_session_id="session-A")
        result2 = await consumer._check_session_change(env2)
        assert result2 is None

        # Session change during runtime (no clean shutdown, no intent → unclean_exit)
        env3 = _make_data_envelope(collector_session_id="session-B")
        result3 = await consumer._check_session_change(env3)
        assert result3 is not None
        assert result3["reason"] == "restart_gap"
        assert result3["component"] == "collector"
        assert result3["cause"] == "unclean_exit"

    @pytest.mark.asyncio
    async def test_runtime_planned_collector_restart(self):
        """When collector does a graceful shutdown with maintenance intent,
        runtime session change should be classified as planned."""
        from src.writer.consumer import WriterConsumer

        intent = MaintenanceIntent(
            maintenance_id="maint-1",
            scope="collector",
            planned_by="cli",
            reason="chaos test",
            created_at=datetime.now(timezone.utc).isoformat(),
            expires_at=(datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
            consumed_at=None,
        )
        collector_state = ComponentRuntimeState(
            component="collector",
            instance_id="collector-A",
            host_boot_id="boot-1",
            started_at=datetime.now(timezone.utc).isoformat(),
            last_heartbeat_at=datetime.now(timezone.utc).isoformat(),
            clean_shutdown_at=datetime.now(timezone.utc).isoformat(),
            planned_shutdown=True,
            maintenance_id="maint-1",
        )

        state_manager = MagicMock(spec=StateManager)
        state_manager.load_component_state_by_instance = AsyncMock(
            return_value=collector_state,
        )
        state_manager.load_active_maintenance_intent = AsyncMock(
            return_value=intent,
        )
        buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
        compressor = MagicMock()

        consumer = WriterConsumer(
            brokers=["localhost:9092"],
            topics=["binance.trades"],
            group_id="test",
            buffer_manager=buffer_manager,
            compressor=compressor,
            state_manager=state_manager,
            base_dir="/data",
        )

        env1 = _make_data_envelope(collector_session_id="session-A")
        await consumer._check_session_change(env1)

        env2 = _make_data_envelope(collector_session_id="session-B")
        result = await consumer._check_session_change(env2)

        assert result is not None
        assert result["reason"] == "restart_gap"
        assert result["component"] == "collector"
        assert result["cause"] == "operator_shutdown"
        assert result["planned"] is True
        assert result["maintenance_id"] == "maint-1"
