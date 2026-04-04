"""Backup topic recovery: reads from backup.* Redpanda topics to fill gaps."""
from __future__ import annotations

import time
from typing import Any

import orjson
import structlog

logger = structlog.get_logger()


def _natural_key(record: dict, stream: str) -> Any:
    """Extract deduplication key for a record based on stream type."""
    raw = orjson.loads(record.get("raw_text", "{}"))
    if stream == "trades":
        return raw.get("a")
    if stream == "depth":
        return raw.get("u")
    if stream == "bookticker":
        return raw.get("u")
    return record.get("exchange_ts")


def _dedup_records(records: list[dict], stream: str) -> list[dict]:
    """Deduplicate records by their natural key."""
    seen: set = set()
    result: list[dict] = []
    for rec in records:
        key = _natural_key(rec, stream)
        if key is not None and key in seen:
            continue
        if key is not None:
            seen.add(key)
        result.append(rec)
    return result


def _determine_coverage(
    records: list[dict],
    gap_start_ns: int,
    gap_end_ns: int,
) -> tuple[str, int | None]:
    """Determine how much of the gap the backup records cover.

    Returns (coverage, new_gap_start_ns):
    - ("full", None) — records cover the entire gap
    - ("partial", new_start) — records cover up to new_start
    - ("none", None) — no records
    """
    if not records:
        return "none", None

    last_received = max(r.get("received_at", 0) for r in records)

    # If the last backup record is within 2 seconds of the gap end,
    # consider it full coverage (small margin for clock skew between
    # primary and backup collectors)
    gap_end_margin = gap_end_ns - 2_000_000_000
    if last_received >= gap_end_margin:
        return "full", None

    new_gap_start = last_received + 1
    return "partial", new_gap_start


def recover_from_backup(
    *,
    brokers: list[str],
    backup_topic_prefix: str,
    stream: str,
    symbol: str,
    exchange: str,
    gap_start_ns: int,
    gap_end_ns: int,
) -> tuple[list[dict], str]:
    """Attempt to recover missing records from backup Redpanda topics.

    Returns (records, coverage) where coverage is "full", "partial", or "none".
    """
    if not brokers or not backup_topic_prefix:
        return [], "none"

    backup_topic = f"{backup_topic_prefix}{exchange}.{stream}"

    try:
        from confluent_kafka import Consumer, TopicPartition

        consumer = Consumer({
            "bootstrap.servers": ",".join(brokers),
            "group.id": f"backup-recovery-{stream}-{int(time.time())}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
            "max.poll.interval.ms": 30000,
        })
    except Exception as exc:
        logger.warning("backup_recovery_consumer_failed", error=str(exc))
        return [], "none"

    try:
        md = consumer.list_topics(topic=backup_topic, timeout=5)
        topic_md = md.topics.get(backup_topic)
        if topic_md is None or topic_md.error is not None or not topic_md.partitions:
            logger.info("backup_topic_not_available", topic=backup_topic)
            return [], "none"

        partitions = [
            TopicPartition(backup_topic, p, gap_start_ns // 1_000_000)
            for p in topic_md.partitions
        ]
        offsets = consumer.offsets_for_times(partitions, timeout=5)
        consumer.assign(offsets)

        records: list[dict] = []
        deadline = time.monotonic() + 10

        while time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                continue

            try:
                envelope = orjson.loads(msg.value())
            except Exception:
                continue

            if envelope.get("symbol") != symbol:
                continue
            if envelope.get("type") != "data":
                continue

            received_ns = envelope.get("received_at", 0)
            if received_ns > gap_end_ns:
                break
            if gap_start_ns <= received_ns <= gap_end_ns:
                records.append(envelope)

        records = _dedup_records(records, stream)
        coverage, _ = _determine_coverage(records, gap_start_ns, gap_end_ns)

        logger.info("backup_recovery_result",
                     stream=stream, symbol=symbol,
                     records_recovered=len(records), coverage=coverage)

        return records, coverage

    except Exception as exc:
        logger.warning("backup_recovery_failed", stream=stream, error=str(exc))
        return [], "none"

    finally:
        try:
            consumer.close()
        except Exception:
            pass
