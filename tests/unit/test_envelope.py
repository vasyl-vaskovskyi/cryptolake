from __future__ import annotations

import hashlib
import time

import orjson
import pytest


class TestEnvelopeCreation:
    def test_create_data_envelope(self) -> None:
        from src.common.envelope import create_data_envelope

        raw_text = '{"e":"aggTrade","E":1741689600120}'
        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text=raw_text,
            exchange_ts=1741689600120,
            collector_session_id="test_2026-01-01T00:00:00Z",
            session_seq=1,
        )
        assert env["v"] == 1
        assert env["type"] == "data"
        assert env["exchange"] == "binance"
        assert env["symbol"] == "btcusdt"
        assert env["stream"] == "trades"
        assert env["raw_text"] == raw_text
        assert env["raw_sha256"] == hashlib.sha256(raw_text.encode()).hexdigest()
        assert env["collector_session_id"] == "test_2026-01-01T00:00:00Z"
        assert env["session_seq"] == 1
        assert isinstance(env["received_at"], int)
        assert env["exchange_ts"] == 1741689600120

    def test_create_gap_envelope(self) -> None:
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="test_2026-01-01T00:00:00Z",
            session_seq=100,
            gap_start_ts=1000000000000000000,
            gap_end_ts=1000000005000000000,
            reason="ws_disconnect",
            detail="WebSocket closed after 2h, reconnected in 1.2s",
        )
        assert env["v"] == 1
        assert env["type"] == "gap"
        assert env["exchange"] == "binance"
        assert env["symbol"] == "btcusdt"
        assert env["stream"] == "depth"
        assert env["gap_start_ts"] == 1000000000000000000
        assert env["gap_end_ts"] == 1000000005000000000
        assert env["reason"] == "ws_disconnect"
        assert env["detail"] == "WebSocket closed after 2h, reconnected in 1.2s"
        assert "raw_text" not in env
        assert "raw_sha256" not in env
        assert "exchange_ts" not in env

    def test_data_envelope_raw_sha256_integrity(self) -> None:
        from src.common.envelope import create_data_envelope

        raw = '{"key": "value", "num": 0.00100000}'
        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text=raw,
            exchange_ts=100,
            collector_session_id="s",
            session_seq=0,
        )
        expected_hash = hashlib.sha256(raw.encode()).hexdigest()
        assert env["raw_sha256"] == expected_hash

    def test_envelope_received_at_is_nanoseconds(self) -> None:
        from src.common.envelope import create_data_envelope

        before = time.time_ns()
        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text="{}",
            exchange_ts=100,
            collector_session_id="s",
            session_seq=0,
        )
        after = time.time_ns()
        assert before <= env["received_at"] <= after

    def test_gap_reason_values(self) -> None:
        from src.common.envelope import VALID_GAP_REASONS

        assert "ws_disconnect" in VALID_GAP_REASONS
        assert "pu_chain_break" in VALID_GAP_REASONS
        assert "session_seq_skip" in VALID_GAP_REASONS
        assert "buffer_overflow" in VALID_GAP_REASONS
        assert "snapshot_poll_miss" in VALID_GAP_REASONS
        assert "collector_restart" in VALID_GAP_REASONS
        assert "restart_gap" in VALID_GAP_REASONS
        assert "recovery_depth_anchor" in VALID_GAP_REASONS

    def test_create_restart_gap_envelope(self) -> None:
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="test_2026-01-01T00:00:00Z",
            session_seq=100,
            gap_start_ts=1000000000000000000,
            gap_end_ts=1000000005000000000,
            reason="restart_gap",
            detail="collector restarted for upgrade",
            component="ws_collector",
            cause="upgrade",
            planned=True,
            classifier="rule:scheduled_restart",
            evidence={"exit_code": 0, "uptime_seconds": 86400},
            maintenance_id="maint-2026-01-15-001",
        )
        assert env["reason"] == "restart_gap"
        assert env["component"] == "ws_collector"
        assert env["cause"] == "upgrade"
        assert env["planned"] is True
        assert env["classifier"] == "rule:scheduled_restart"
        assert env["evidence"] == {"exit_code": 0, "uptime_seconds": 86400}
        assert env["maintenance_id"] == "maint-2026-01-15-001"

    def test_restart_gap_envelope_optional_fields_omitted(self) -> None:
        """When optional restart metadata is not provided, those keys must not appear."""
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="s",
            session_seq=0,
            gap_start_ts=0,
            gap_end_ts=1,
            reason="restart_gap",
            detail="bare restart gap",
        )
        assert env["reason"] == "restart_gap"
        for key in ("component", "cause", "planned", "classifier", "evidence", "maintenance_id"):
            assert key not in env

    def test_restart_gap_envelope_partial_optional_fields(self) -> None:
        """Only provided optional fields appear in the envelope."""
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="s",
            session_seq=0,
            gap_start_ts=0,
            gap_end_ts=1,
            reason="restart_gap",
            detail="partial metadata",
            component="snapshot_poller",
            planned=False,
        )
        assert env["component"] == "snapshot_poller"
        assert env["planned"] is False
        assert "cause" not in env
        assert "classifier" not in env
        assert "evidence" not in env
        assert "maintenance_id" not in env

    def test_non_restart_gap_ignores_extra_fields(self) -> None:
        """Non-restart gaps should still work and not include restart metadata."""
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            collector_session_id="s",
            session_seq=0,
            gap_start_ts=0,
            gap_end_ts=1,
            reason="ws_disconnect",
            detail="normal disconnect",
        )
        assert env["reason"] == "ws_disconnect"
        for key in ("component", "cause", "planned", "classifier", "evidence", "maintenance_id"):
            assert key not in env

    def test_checkpoint_lost_is_valid_gap_reason(self) -> None:
        from src.common.envelope import VALID_GAP_REASONS

        assert "checkpoint_lost" in VALID_GAP_REASONS

    def test_create_gap_envelope_with_checkpoint_lost(self) -> None:
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            collector_session_id="test-session",
            session_seq=-1,
            gap_start_ts=1000,
            gap_end_ts=2000,
            reason="checkpoint_lost",
            detail="No durable checkpoint; recovered gap bounds from archive",
        )
        assert env["reason"] == "checkpoint_lost"
        assert env["type"] == "gap"

    def test_gap_invalid_reason_raises(self) -> None:
        from src.common.envelope import create_gap_envelope

        with pytest.raises(ValueError, match="reason"):
            create_gap_envelope(
                exchange="binance",
                symbol="btcusdt",
                stream="trades",
                collector_session_id="s",
                session_seq=0,
                gap_start_ts=0,
                gap_end_ts=1,
                reason="invalid_reason",
                detail="test",
            )


class TestEnvelopeSerialization:
    def test_serialize_data_envelope_to_bytes(self) -> None:
        from src.common.envelope import create_data_envelope, serialize_envelope

        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text='{"test": 1}',
            exchange_ts=100,
            collector_session_id="s",
            session_seq=0,
        )
        data = serialize_envelope(env)
        assert isinstance(data, bytes)
        parsed = orjson.loads(data)
        assert parsed["type"] == "data"
        assert parsed["raw_text"] == '{"test": 1}'

    def test_serialize_gap_envelope_to_bytes(self) -> None:
        from src.common.envelope import create_gap_envelope, serialize_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="s",
            session_seq=0,
            gap_start_ts=0,
            gap_end_ts=1,
            reason="ws_disconnect",
            detail="test",
        )
        data = serialize_envelope(env)
        parsed = orjson.loads(data)
        assert parsed["type"] == "gap"
        assert "raw_text" not in parsed

    def test_deserialize_envelope_round_trip(self) -> None:
        from src.common.envelope import (
            create_data_envelope,
            deserialize_envelope,
            serialize_envelope,
        )

        original = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text='{"e":"aggTrade","p":"0.00100000"}',
            exchange_ts=1741689600120,
            collector_session_id="test_session",
            session_seq=42,
        )
        roundtripped = deserialize_envelope(serialize_envelope(original))
        assert roundtripped == original

    def test_add_broker_coordinates(self) -> None:
        from src.common.envelope import add_broker_coordinates, create_data_envelope

        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text="{}",
            exchange_ts=100,
            collector_session_id="s",
            session_seq=0,
        )
        stamped = add_broker_coordinates(env, topic="binance.trades", partition=0, offset=42)
        assert stamped["_topic"] == "binance.trades"
        assert stamped["_partition"] == 0
        assert stamped["_offset"] == 42


class TestHeartbeatEnvelope:
    def test_heartbeat_envelope_alive_with_data(self):
        from src.common.envelope import create_heartbeat_envelope, HEARTBEAT_ENVELOPE_FIELDS

        env = create_heartbeat_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            collector_session_id="sess-1",
            emitted_at_ns=1_000_000_000_000_000_000,
            last_data_at_ns=999_999_999_000_000_000,
            last_session_seq=42,
            status="alive",
        )
        assert env["type"] == "heartbeat"
        assert env["v"] == 1
        assert env["status"] == "alive"
        assert env["last_data_at_ns"] == 999_999_999_000_000_000
        assert env["last_session_seq"] == 42
        # Schema completeness
        assert set(env.keys()) == HEARTBEAT_ENVELOPE_FIELDS

    def test_heartbeat_envelope_subscribed_silent_no_data(self):
        from src.common.envelope import create_heartbeat_envelope

        env = create_heartbeat_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="liquidations",
            collector_session_id="sess-1",
            emitted_at_ns=1_000_000_000_000_000_000,
            last_data_at_ns=None,  # never had data
            last_session_seq=0,
            status="subscribed_silent",
        )
        assert env["last_data_at_ns"] is None
        assert env["status"] == "subscribed_silent"

    def test_heartbeat_envelope_invalid_status_rejected(self):
        from src.common.envelope import create_heartbeat_envelope

        with pytest.raises(ValueError, match="Invalid heartbeat status"):
            create_heartbeat_envelope(
                exchange="binance", symbol="btcusdt", stream="trades",
                collector_session_id="s", emitted_at_ns=1, last_data_at_ns=None,
                last_session_seq=0, status="bogus",
            )
