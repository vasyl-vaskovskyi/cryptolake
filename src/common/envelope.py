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
    }
)


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
) -> dict[str, Any]:
    if reason not in VALID_GAP_REASONS:
        raise ValueError(f"Invalid gap reason '{reason}'")

    return {
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
