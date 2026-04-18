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

from tests.helpers import make_intent

from src.common.envelope import create_data_envelope
from src.writer.buffer_manager import BufferManager, CheckpointMeta, FlushResult
from src.writer.consumer import WriterConsumer
from src.writer.state_manager import (
    ComponentRuntimeState,
    MaintenanceIntent,
    StreamCheckpoint,
    StateManager,
)


def _make_consumer(*, topics=None, async_state=False):
    """Create a WriterConsumer with standard mocked dependencies."""
    state_manager = AsyncMock(spec=StateManager) if async_state else MagicMock(spec=StateManager)
    buffer_manager = BufferManager(base_dir="/data", flush_messages=10_000)
    compressor = MagicMock()

    return WriterConsumer(
        brokers=["localhost:9092"],
        topics=topics or ["binance.trades"],
        group_id="test",
        buffer_manager=buffer_manager,
        compressor=compressor,
        state_manager=state_manager,
        base_dir="/data",
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
        consumer = _make_consumer()

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
        consumer = _make_consumer()

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
        consumer = _make_consumer()

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
        consumer._maintenance_intent = make_intent()

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["planned"] is True
        assert gap["cause"] == "operator_shutdown"
        assert gap["component"] == "system"

    def test_first_post_recovery_record_sets_gap_end_ts(self):
        """gap_end_ts should be set to the received_at of the first post-recovery envelope."""
        consumer = _make_consumer()

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
        consumer = _make_consumer()

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
        consumer = _make_consumer()

        consumer._durable_checkpoints = {}  # no checkpoints
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = None

        envelope = _make_data_envelope(collector_session_id="session-new")
        gap = consumer._check_recovery_gap(envelope)

        assert gap is None


class TestCheckpointLostGapDetection:
    """When PostgreSQL has no durable checkpoint but archive files exist on disk,
    emit a checkpoint_lost gap (writer was killed after disk write but before PG commit)."""

    def test_no_checkpoint_with_archive_files_emits_checkpoint_lost(self, tmp_path):
        """No PG checkpoint + archive files exist → emit checkpoint_lost gap."""
        import zstandard as zstd
        import orjson

        consumer = _make_consumer()
        consumer.base_dir = str(tmp_path)
        consumer._durable_checkpoints = {}
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = None

        # Create archive file: tmp_path/binance/btcusdt/trades/2026-03-28/hour-15.jsonl.zst
        archive_dir = tmp_path / "binance" / "btcusdt" / "trades" / "2026-03-28"
        archive_dir.mkdir(parents=True)
        archive_file = archive_dir / "hour-15.jsonl.zst"

        archive_received_at = 1774712068000000000
        data_env = {"v": 1, "type": "data", "received_at": archive_received_at, "exchange": "binance"}
        line = orjson.dumps(data_env)

        cctx = zstd.ZstdCompressor()
        with open(archive_file, "wb") as f:
            f.write(cctx.compress(line + b"\n"))

        # New-session envelope with a later received_at
        new_received_at = 1774722068000000000
        envelope = _make_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            collector_session_id="session-new",
            received_at=new_received_at,
        )

        gap = consumer._check_recovery_gap(envelope)

        assert gap is not None
        assert gap["reason"] == "checkpoint_lost"
        assert gap["gap_start_ts"] == archive_received_at
        assert gap["gap_end_ts"] == new_received_at

    def test_no_checkpoint_no_archive_files_returns_none(self, tmp_path):
        """No PG checkpoint + no archive files → first-ever run, return None."""
        consumer = _make_consumer()
        consumer.base_dir = str(tmp_path)
        consumer._durable_checkpoints = {}
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = None

        envelope = _make_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            collector_session_id="session-new",
            received_at=1774722068000000000,
        )

        gap = consumer._check_recovery_gap(envelope)

        assert gap is None


class TestRestPolledStreamRecovery:
    """REST-polled streams (e.g., open_interest) may not have session_id changes
    when only the writer restarts. Detect gaps via time delta."""

    def test_time_delta_gap_detection_for_rest_polled_stream(self):
        """When session_id is unchanged but the time delta exceeds the expected
        poll interval, a restart_gap should be emitted."""
        consumer = _make_consumer(topics=["binance.open_interest"])

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

    def test_gap_emitted_after_writer_restart_even_within_poll_interval(self):
        """After a writer restart, a recovery gap should ALWAYS be emitted —
        even for REST-polled streams within the normal poll interval.
        A writer restart means potential data loss; silent gaps violate the
        system invariant 'no data lost silently'."""
        consumer = _make_consumer(topics=["binance.open_interest"])

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

        # Writer restarted → gap must be recorded regardless of poll interval
        assert gap is not None
        assert gap["reason"] == "restart_gap"


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
        consumer = _make_consumer(async_state=True)
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
        consumer.state_manager.save_states_and_checkpoints.assert_called_once()
        saved_checkpoints = consumer.state_manager.save_states_and_checkpoints.call_args[0][1]
        assert len(saved_checkpoints) == 1
        assert saved_checkpoints[0].exchange == "binance"
        assert saved_checkpoints[0].symbol == "btcusdt"
        assert saved_checkpoints[0].stream == "trades"

    @pytest.mark.asyncio
    async def test_in_memory_checkpoint_updated_after_commit(self):
        """The in-memory _durable_checkpoints cache should update only after
        successful commit."""
        consumer = _make_consumer(async_state=True)
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
        consumer = _make_consumer()
        # Mock async DB calls used by the runtime path
        consumer.state_manager.load_component_state_by_instance = AsyncMock(return_value=None)
        consumer.state_manager.load_active_maintenance_intent = AsyncMock(return_value=None)

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

        consumer = _make_consumer()
        consumer.state_manager.load_component_state_by_instance = AsyncMock(
            return_value=collector_state,
        )
        consumer.state_manager.load_active_maintenance_intent = AsyncMock(
            return_value=intent,
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

    @pytest.mark.asyncio
    async def test_runtime_session_change_detects_boot_id_change(self):
        """When boot ID changed, runtime session-change path should classify
        as component=host, cause=host_reboot — not collector/unclean_exit."""
        consumer = _make_consumer()
        consumer.state_manager.load_component_state_by_instance = AsyncMock(return_value=None)
        consumer.state_manager.load_active_maintenance_intent = AsyncMock(return_value=None)

        # Previous writer ran with old boot ID
        consumer._current_boot_id = "boot-new"
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-old"
        )

        # First message establishes session
        env1 = _make_data_envelope(collector_session_id="session-old")
        await consumer._check_session_change(env1)

        # Session change detected at runtime (after host reboot)
        env2 = _make_data_envelope(collector_session_id="session-new")
        result = await consumer._check_session_change(env2)

        assert result is not None
        assert result["reason"] == "restart_gap"
        assert result["component"] == "host"
        assert result["cause"] == "host_reboot"
        assert result["planned"] is False


class TestRecoveryGapTimestampClamping:
    """When a checkpoint's last_received_at was set from a gap envelope with
    wall-clock received_at, it can be newer than messages Kafka re-delivers
    after restart.  The recovery gap must clamp gap_start_ts <= gap_end_ts."""

    def test_inverted_timestamps_clamped_to_zero_duration(self):
        """If checkpoint timestamp > first post-recovery message, clamp to avoid
        negative-duration gap (regression from buffer_overflow_recovery chaos test)."""
        consumer = _make_consumer()

        # Checkpoint has a NEWER timestamp (e.g., from a wall-clock error-gap envelope)
        future_ts = "2026-03-18T10:05:00+00:00"  # 10:05
        consumer._durable_checkpoints = {
            ("binance", "btcusdt", "trades"): _make_checkpoint(
                last_received_at=future_ts,
                last_collector_session_id="session-old",
            ),
        }
        consumer._previous_writer_state = _make_component_state(
            component="writer", host_boot_id="boot-A"
        )
        consumer._current_boot_id = "boot-A"

        # First post-recovery message has an OLDER received_at (re-delivered from Kafka)
        older_ns = int(datetime(2026, 3, 18, 10, 3, 0, tzinfo=timezone.utc).timestamp() * 1e9)
        envelope = _make_data_envelope(
            collector_session_id="session-old",
            received_at=older_ns,
        )

        result = consumer._check_recovery_gap(envelope)
        assert result is not None
        assert result["reason"] == "restart_gap"
        # gap_start_ts must be clamped to gap_end_ts (no negative duration)
        assert result["gap_start_ts"] <= result["gap_end_ts"]
        assert result["gap_end_ts"] == older_ns


class TestDepthRecoveryAnchorGap:
    """After a writer-recovery restart_gap on a depth stream, diffs arriving
    before the next depth_snapshot are unanchored: verify.py's "first diff
    must span a sync point" check fails.  The writer emits a supplementary
    gap closing the window [first post-recovery depth diff, first snapshot]
    so verify tolerates the unavoidable chain break."""

    def test_depth_recovery_records_pending_anchor(self):
        consumer = _make_consumer(topics=["binance.depth"])
        checkpoint = _make_checkpoint(
            stream="depth",
            last_collector_session_id="session-old",
        )
        consumer._durable_checkpoints = {("binance", "btcusdt", "depth"): checkpoint}
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(host_boot_id="boot-aaa")

        first_diff_ts = time.time_ns()
        envelope = _make_data_envelope(
            stream="depth",
            collector_session_id="session-new",
            received_at=first_diff_ts,
        )
        gap = consumer._check_recovery_gap(envelope)
        assert gap is not None
        assert gap["reason"] == "restart_gap"
        # The pending anchor for this (exchange, symbol) must be recorded.
        assert ("binance", "btcusdt") in consumer._depth_recovery_pending
        assert consumer._depth_recovery_pending[("binance", "btcusdt")] == first_diff_ts

    def test_snapshot_arrival_closes_depth_recovery_window(self):
        consumer = _make_consumer(topics=["binance.depth"])

        first_diff_ts = time.time_ns()
        snapshot_ts = first_diff_ts + 30_000_000_000  # +30s
        consumer._depth_recovery_pending = {("binance", "btcusdt"): first_diff_ts}

        snapshot_envelope = _make_data_envelope(
            stream="depth_snapshot",
            received_at=snapshot_ts,
        )
        supplementary = consumer._maybe_close_depth_recovery_gap(snapshot_envelope)

        assert supplementary is not None
        assert supplementary["reason"] == "recovery_depth_anchor"
        assert supplementary["stream"] == "depth"
        assert supplementary["symbol"] == "btcusdt"
        assert supplementary["gap_start_ts"] == first_diff_ts
        assert supplementary["gap_end_ts"] == snapshot_ts
        # Pending entry must be cleared.
        assert ("binance", "btcusdt") not in consumer._depth_recovery_pending

    def test_non_snapshot_envelopes_do_not_close_window(self):
        consumer = _make_consumer(topics=["binance.depth"])
        first_diff_ts = time.time_ns()
        consumer._depth_recovery_pending = {("binance", "btcusdt"): first_diff_ts}

        diff_envelope = _make_data_envelope(
            stream="depth",
            received_at=first_diff_ts + 1_000_000_000,
        )
        result = consumer._maybe_close_depth_recovery_gap(diff_envelope)

        assert result is None
        assert ("binance", "btcusdt") in consumer._depth_recovery_pending

    def test_no_pending_entry_returns_none(self):
        consumer = _make_consumer(topics=["binance.depth"])
        # No _depth_recovery_pending entries set.
        snapshot_envelope = _make_data_envelope(
            stream="depth_snapshot",
            received_at=time.time_ns(),
        )
        result = consumer._maybe_close_depth_recovery_gap(snapshot_envelope)
        assert result is None

    def test_non_depth_stream_recovery_does_not_record_pending(self):
        consumer = _make_consumer()
        checkpoint = _make_checkpoint(stream="trades", last_collector_session_id="session-old")
        consumer._durable_checkpoints = {("binance", "btcusdt", "trades"): checkpoint}
        consumer._recovery_done = set()
        consumer._current_boot_id = "boot-aaa"
        consumer._previous_writer_state = _make_component_state(host_boot_id="boot-aaa")

        envelope = _make_data_envelope(
            stream="trades",
            collector_session_id="session-new",
        )
        gap = consumer._check_recovery_gap(envelope)
        assert gap is not None
        # Only depth streams get a pending anchor; trades do not.
        assert ("binance", "btcusdt") not in consumer._depth_recovery_pending
