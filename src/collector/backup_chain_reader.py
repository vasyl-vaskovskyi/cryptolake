"""One-shot reader: get the last depth update ID from the other collector's topic."""
from __future__ import annotations

import time

import orjson
import structlog

logger = structlog.get_logger()

try:
    from confluent_kafka import Consumer, TopicPartition
except ImportError:  # pragma: no cover
    Consumer = None  # type: ignore[assignment,misc]
    TopicPartition = None  # type: ignore[assignment,misc]


def other_depth_topic(own_prefix: str, exchange: str) -> str:
    """Derive the other collector's depth topic from our own prefix.

    Primary (prefix="") checks backup.{exchange}.depth
    Backup (prefix="backup.") checks {exchange}.depth
    """
    if own_prefix == "backup.":
        return f"{exchange}.depth"
    return f"backup.{exchange}.depth"


def read_last_depth_update_id(
    *,
    brokers: list[str],
    topic: str,
    symbol: str,
    max_age_seconds: int = 30,
) -> int | None:
    """Read the last depth diff update ID from a Redpanda topic.

    Creates a temporary consumer, seeks to recent messages, finds the last
    depth diff for the given symbol, and returns its final update ID (u).

    Returns None if no recent matching message is found, or on any error.
    """
    try:
        consumer = Consumer({
            "bootstrap.servers": ",".join(brokers),
            "group.id": f"depth-chain-reader-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "session.timeout.ms": 10000,
        })
    except Exception as exc:
        logger.warning("backup_chain_reader_consumer_failed", error=str(exc))
        return None

    try:
        md = consumer.list_topics(topic=topic, timeout=5)
        topic_md = md.topics.get(topic)
        if topic_md is None or topic_md.error is not None or not topic_md.partitions:
            logger.info("backup_chain_reader_topic_not_available", topic=topic)
            return None

        # Seek to max_age_seconds ago
        seek_ms = int(time.time() * 1000) - (max_age_seconds * 1000)
        partitions = [
            TopicPartition(topic, pid, seek_ms)
            for pid in topic_md.partitions
        ]
        offsets = consumer.offsets_for_times(partitions, timeout=5)
        consumer.assign(offsets)

        # Poll for matching messages, keep the last one
        last_u: int | None = None
        deadline = time.monotonic() + 5  # max 5 seconds of polling

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

            # Check message age via Kafka timestamp
            ts_type, ts_ms = msg.timestamp()
            now_ms = int(time.time() * 1000)
            if (now_ms - ts_ms) > (max_age_seconds * 1000):
                continue

            # Extract u from raw_text
            raw_text = envelope.get("raw_text")
            if raw_text is None:
                continue
            try:
                raw = orjson.loads(raw_text) if isinstance(raw_text, str) else raw_text
                u = raw.get("u")
                if u is not None:
                    last_u = u
            except Exception:
                continue

        if last_u is not None:
            logger.info("backup_chain_reader_found", topic=topic, symbol=symbol, last_u=last_u)
        else:
            logger.info("backup_chain_reader_no_recent_data", topic=topic, symbol=symbol)

        return last_u

    except Exception as exc:
        logger.warning("backup_chain_reader_failed", topic=topic, error=str(exc))
        return None
    finally:
        try:
            consumer.close()
        except Exception:
            pass
