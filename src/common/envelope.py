from __future__ import annotations

import hashlib
import time
from typing import Any

import orjson

VALID_GAP_REASONS = frozenset(
    {
        "ws_disconnect",
        "pu_chain_break",
        "session_seq_skip",
        "buffer_overflow",
        "snapshot_poll_miss",
        "collector_restart",  # kept for migration: existing archives contain it
        "restart_gap",
        "recovery_depth_anchor",
        "write_error",
        "deserialization_error",
        "checkpoint_lost",
        "missing_hour",
        "kafka_delivery_failed",  # producer.poll delivery callback received err
        "handler_error",          # handler.handle() raised — frame lost without disconnect
    }
)

DATA_ENVELOPE_FIELDS = frozenset({
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "exchange_ts", "collector_session_id", "session_seq", "raw_text", "raw_sha256",
})

GAP_ENVELOPE_FIELDS = frozenset({
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "collector_session_id", "session_seq", "gap_start_ts", "gap_end_ts",
    "reason", "detail",
})

# Per-(symbol, stream) liveness signal emitted on a fixed cadence regardless
# of whether the upstream is delivering data. Allows downstream consumers to
# distinguish "collector down" from "Binance silent on this stream" and to
# detect silent loss without relying on the collector to emit a gap.
HEARTBEAT_ENVELOPE_FIELDS = frozenset({
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "collector_session_id", "emitted_at_ns", "last_data_at_ns",
    "last_session_seq", "status",
})

# Valid heartbeat status values:
# - "alive"               — data flowing recently (last frame within heartbeat interval)
# - "subscribed_silent"   — SUBSCRIBE acked but stream is silent (Binance-side)
# - "disconnected"        — WebSocket is currently down (between connect attempts)
VALID_HEARTBEAT_STATUS = frozenset({"alive", "subscribed_silent", "disconnected"})

BROKER_COORD_FIELDS = frozenset({"_topic", "_partition", "_offset"})

_SENTINEL = object()  # used to distinguish "not provided" from None


def create_data_envelope(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    raw_text: str,
    exchange_ts: int,
    collector_session_id: str,
    session_seq: int,
    received_at: int | None = None,
) -> dict[str, Any]:
    return {
        "v": 1,
        "type": "data",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": time.time_ns() if received_at is None else received_at,
        "exchange_ts": exchange_ts,
        "collector_session_id": collector_session_id,
        "session_seq": session_seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
    }


def create_gap_envelope(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    collector_session_id: str,
    session_seq: int,
    gap_start_ts: int,
    gap_end_ts: int,
    reason: str,
    detail: str,
    received_at: int | None = None,
    # Optional restart metadata (keyword-only, included only when provided)
    component: Any = _SENTINEL,
    cause: Any = _SENTINEL,
    planned: Any = _SENTINEL,
    classifier: Any = _SENTINEL,
    evidence: Any = _SENTINEL,
    maintenance_id: Any = _SENTINEL,
) -> dict[str, Any]:
    if reason not in VALID_GAP_REASONS:
        raise ValueError(f"Invalid gap reason '{reason}'")

    env: dict[str, Any] = {
        "v": 1,
        "type": "gap",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": time.time_ns() if received_at is None else received_at,
        "collector_session_id": collector_session_id,
        "session_seq": session_seq,
        "gap_start_ts": gap_start_ts,
        "gap_end_ts": gap_end_ts,
        "reason": reason,
        "detail": detail,
    }

    # Include only provided optional restart metadata fields
    for key, value in (
        ("component", component),
        ("cause", cause),
        ("planned", planned),
        ("classifier", classifier),
        ("evidence", evidence),
        ("maintenance_id", maintenance_id),
    ):
        if value is not _SENTINEL:
            env[key] = value

    return env


def create_heartbeat_envelope(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    collector_session_id: str,
    emitted_at_ns: int,
    last_data_at_ns: int | None,
    last_session_seq: int,
    status: str,
) -> dict[str, Any]:
    """Per-(symbol, stream) liveness envelope.

    Allows the downstream consumer to:
      - Detect collector silence (missing heartbeats) without an upstream gap.
      - Distinguish "Binance dropped the subscription" (heartbeats keep arriving
        with status=subscribed_silent and stale last_data_at_ns) from "collector
        crashed" (heartbeats stop entirely).
      - Choose between primary and backup collectors using last_data_at_ns as
        the freshness indicator.
    """
    if status not in VALID_HEARTBEAT_STATUS:
        raise ValueError(f"Invalid heartbeat status '{status}'")
    return {
        "v": 1,
        "type": "heartbeat",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": emitted_at_ns,
        "collector_session_id": collector_session_id,
        "emitted_at_ns": emitted_at_ns,
        "last_data_at_ns": last_data_at_ns,
        "last_session_seq": last_session_seq,
        "status": status,
    }


def serialize_envelope(envelope: dict[str, Any]) -> bytes:
    return orjson.dumps(envelope)


def deserialize_envelope(data: bytes) -> dict[str, Any]:
    return orjson.loads(data)


def add_broker_coordinates(
    envelope: dict[str, Any],
    *,
    topic: str,
    partition: int,
    offset: int,
) -> dict[str, Any]:
    envelope["_topic"] = topic
    envelope["_partition"] = partition
    envelope["_offset"] = offset
    return envelope
