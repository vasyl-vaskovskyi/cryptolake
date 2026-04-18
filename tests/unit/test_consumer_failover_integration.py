"""Tests for failover integration in the WriterConsumer consume loop."""
from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from src.writer.buffer_manager import BufferManager
from src.writer.consumer import WriterConsumer
from src.writer.failover import FailoverManager
from src.writer import metrics as writer_metrics


def _make_consumer(*, topics=None):
    state_manager = MagicMock()
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
    *, exchange="binance", symbol="btcusdt", stream="trades",
    collector_session_id="session-1", session_seq=1, received_at=None,
    offset=100, partition=0, raw_text='{"a": 1}',
):
    if received_at is None:
        received_at = time.time_ns()
    return {
        "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
        "stream": stream, "received_at": received_at, "exchange_ts": 100,
        "collector_session_id": collector_session_id, "session_seq": session_seq,
        "raw_text": raw_text, "raw_sha256": "abc123",
        "_topic": f"{exchange}.{stream}", "_partition": partition, "_offset": offset,
    }


class TestWriterConsumerHasFailoverManager:
    def test_failover_manager_created(self):
        consumer = _make_consumer()
        assert isinstance(consumer._failover, FailoverManager)

    def test_failover_manager_has_correct_topics(self):
        consumer = _make_consumer(topics=["binance.trades", "binance.depth"])
        assert consumer._failover._primary_topics == ["binance.trades", "binance.depth"]

    def test_failover_not_active_initially(self):
        consumer = _make_consumer()
        assert consumer._failover.is_active is False


class TestConsumeLoopKeyTracking:
    @pytest.mark.asyncio
    async def test_natural_key_tracked_after_message(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}

        envelope = _make_data_envelope(raw_text='{"a": 42}')
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(envelope)
        mock_msg.topic.return_value = "binance.trades"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100

        call_count = 0
        def poll_side_effect(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_msg
            consumer._running = False
            return None

        consumer._consumer.poll.side_effect = poll_side_effect
        consumer._consumer.assignment.return_value = []
        await consumer.consume_loop()

        assert consumer._failover._last_key[("binance", "btcusdt", "trades")] == 42

    @pytest.mark.asyncio
    async def test_silence_timer_reset_on_message(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}

        envelope = _make_data_envelope(raw_text='{"a": 1}')
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(envelope)
        mock_msg.topic.return_value = "binance.trades"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 100

        call_count = 0
        def poll_side_effect(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_msg
            consumer._running = False
            return None

        consumer._consumer.poll.side_effect = poll_side_effect
        consumer._consumer.assignment.return_value = []
        await consumer.consume_loop()

        assert consumer._failover._no_message_since is not None


class TestConsumeLoopFailoverPath:
    @pytest.mark.asyncio
    async def test_backup_messages_consumed_during_failover(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}
        consumer._last_session = {("binance", "btcusdt", "trades"): ("session-1", time.time_ns())}

        consumer._failover._is_active = True
        consumer._failover._last_key[("binance", "btcusdt", "trades")] = 100

        backup_envelope = _make_data_envelope(raw_text='{"a": 101}', collector_session_id="session-1")
        mock_backup_msg = MagicMock()
        mock_backup_msg.error.return_value = None
        mock_backup_msg.value.return_value = orjson.dumps(backup_envelope)
        mock_backup_msg.topic.return_value = "backup.binance.trades"
        mock_backup_msg.partition.return_value = 0
        mock_backup_msg.offset.return_value = 200

        mock_backup_consumer = MagicMock()
        consumer._failover._backup_consumer = mock_backup_consumer

        backup_call = 0
        def backup_poll(timeout):
            nonlocal backup_call
            backup_call += 1
            return mock_backup_msg if backup_call == 1 else None
        mock_backup_consumer.poll.side_effect = backup_poll

        poll_calls = 0
        def primary_poll(timeout):
            nonlocal poll_calls
            poll_calls += 1
            if poll_calls >= 2:
                consumer._running = False
            return None
        consumer._consumer.poll.side_effect = primary_poll
        consumer._consumer.assignment.return_value = []

        await consumer.consume_loop()
        assert mock_backup_consumer.poll.called

    @pytest.mark.asyncio
    async def test_primary_return_triggers_switchback(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}
        consumer._last_session = {("binance", "btcusdt", "trades"): ("session-1", time.time_ns())}

        consumer._failover._is_active = True
        consumer._failover._last_key[("binance", "btcusdt", "trades")] = 100
        consumer._failover._failover_start_time = time.monotonic()

        mock_backup_consumer = MagicMock()
        mock_backup_consumer.poll.return_value = None
        consumer._failover._backup_consumer = mock_backup_consumer

        primary_envelope = _make_data_envelope(raw_text='{"a": 101}', collector_session_id="session-1")
        mock_primary_msg = MagicMock()
        mock_primary_msg.error.return_value = None
        mock_primary_msg.value.return_value = orjson.dumps(primary_envelope)
        mock_primary_msg.topic.return_value = "binance.trades"
        mock_primary_msg.partition.return_value = 0
        mock_primary_msg.offset.return_value = 300

        call_count = 0
        def primary_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_primary_msg
            consumer._running = False
            return None
        consumer._consumer.poll.side_effect = primary_poll
        consumer._consumer.assignment.return_value = []

        await consumer.consume_loop()
        assert consumer._failover.is_active is False
        mock_backup_consumer.close.assert_called()


class TestFailoverRoundTrip:
    @pytest.mark.asyncio
    async def test_full_failover_and_switchback_cycle(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}
        consumer._last_session = {("binance", "btcusdt", "trades"): ("session-1", time.time_ns())}
        consumer._consumer.assignment.return_value = []
        consumer._failover._silence_timeout = 0.01

        primary_1 = _make_data_envelope(raw_text='{"a": 100}', collector_session_id="session-1", session_seq=1)
        backup = _make_data_envelope(raw_text='{"a": 101}', collector_session_id="session-1", session_seq=2)
        primary_2 = _make_data_envelope(raw_text='{"a": 102}', collector_session_id="session-1", session_seq=3)

        mock_msg_1 = MagicMock()
        mock_msg_1.error.return_value = None
        mock_msg_1.value.return_value = orjson.dumps(primary_1)
        mock_msg_1.topic.return_value = "binance.trades"
        mock_msg_1.partition.return_value = 0
        mock_msg_1.offset.return_value = 1

        mock_msg_2 = MagicMock()
        mock_msg_2.error.return_value = None
        mock_msg_2.value.return_value = orjson.dumps(primary_2)
        mock_msg_2.topic.return_value = "binance.trades"
        mock_msg_2.partition.return_value = 0
        mock_msg_2.offset.return_value = 3

        mock_backup_msg = MagicMock()
        mock_backup_msg.error.return_value = None
        mock_backup_msg.value.return_value = orjson.dumps(backup)
        mock_backup_msg.topic.return_value = "backup.binance.trades"
        mock_backup_msg.partition.return_value = 0
        mock_backup_msg.offset.return_value = 50

        phase = {"current": 1}
        primary_n = {"n": 0}

        def primary_poll(timeout):
            primary_n["n"] += 1
            if phase["current"] == 1 and primary_n["n"] == 1:
                return mock_msg_1
            if phase["current"] == 1 and primary_n["n"] <= 3:
                return None
            if phase["current"] == 2:
                phase["current"] = 3
                return mock_msg_2
            if phase["current"] >= 3:
                consumer._running = False
                return None
            return None

        consumer._consumer.poll.side_effect = primary_poll

        mock_backup_consumer = MagicMock()
        backup_n = {"n": 0}
        def backup_poll(timeout):
            backup_n["n"] += 1
            if backup_n["n"] == 1:
                phase["current"] = 2
                return mock_backup_msg
            return None
        mock_backup_consumer.poll.side_effect = backup_poll

        def mock_activate():
            consumer._failover._is_active = True
            consumer._failover._failover_start_time = time.monotonic()
            consumer._failover._backup_consumer = mock_backup_consumer
            consumer._failover._gap_checked = set()
            consumer._failover._switchback_filtering = {}
            writer_metrics.failover_active.set(1)
            writer_metrics.failover_total.inc()
        consumer._failover.activate = mock_activate

        await consumer.consume_loop()

        assert consumer._failover.is_active is False
        assert consumer._failover._last_key[("binance", "btcusdt", "trades")] == 102


class TestCommitAfterSwitchback:
    """After deactivate() fires, the primary consumer's position must be
    explicitly committed so that any switchback-filtered duplicates do not
    leave phantom lag on the consumer group."""

    @pytest.mark.asyncio
    async def test_primary_commit_called_after_switchback(self):
        consumer = _make_consumer()
        consumer._consumer = MagicMock()
        consumer._running = True
        consumer._assigned = True
        consumer._recovery_done = {("binance", "btcusdt", "trades")}
        consumer._durable_checkpoints = {}
        consumer._recovery_high_water = {}
        consumer._last_session = {("binance", "btcusdt", "trades"): ("session-1", time.time_ns())}

        consumer._failover._is_active = True
        consumer._failover._last_key[("binance", "btcusdt", "trades")] = 100
        consumer._failover._failover_start_time = time.monotonic()

        mock_backup_consumer = MagicMock()
        mock_backup_consumer.poll.return_value = None
        consumer._failover._backup_consumer = mock_backup_consumer

        fresh_envelope = _make_data_envelope(raw_text='{"a": 150}', collector_session_id="session-1")
        mock_fresh_msg = MagicMock()
        mock_fresh_msg.error.return_value = None
        mock_fresh_msg.value.return_value = orjson.dumps(fresh_envelope)
        mock_fresh_msg.topic.return_value = "binance.trades"
        mock_fresh_msg.partition.return_value = 0
        mock_fresh_msg.offset.return_value = 300

        call_count = 0
        def primary_poll(timeout):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return mock_fresh_msg
            consumer._running = False
            return None
        consumer._consumer.poll.side_effect = primary_poll
        consumer._consumer.assignment.return_value = []

        await consumer.consume_loop()

        assert consumer._failover.is_active is False
        consumer._consumer.commit.assert_called()
