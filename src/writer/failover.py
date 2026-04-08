"""Writer real-time failover: switch to backup topics when primary goes silent."""
from __future__ import annotations

import time
from typing import Any

import orjson
import structlog
from confluent_kafka import Consumer as KafkaConsumer, TopicPartition

from src.common.envelope import create_gap_envelope
from src.writer import metrics as writer_metrics

logger = structlog.get_logger()

_RAW_KEY_STREAMS: dict[str, str] = {
    "trades": "a",
    "depth": "u",
    "bookticker": "u",
}


def extract_natural_key(envelope: dict[str, Any]) -> int | None:
    """Extract the monotonic natural key from a data envelope.

    Returns None for gap envelopes, missing data, or parse errors.
    """
    if envelope.get("type") != "data":
        return None

    stream = envelope.get("stream", "")

    if stream in _RAW_KEY_STREAMS:
        raw_text = envelope.get("raw_text")
        if raw_text is None:
            return None
        try:
            raw = orjson.loads(raw_text)
        except Exception:
            return None
        return raw.get(_RAW_KEY_STREAMS[stream])

    return envelope.get("exchange_ts")


class FailoverManager:
    """Manages real-time failover between primary and backup Kafka consumers."""

    def __init__(
        self,
        brokers: list[str],
        primary_topics: list[str],
        backup_prefix: str = "backup.",
        silence_timeout: float = 5.0,
    ):
        self._brokers = brokers
        self._primary_topics = primary_topics
        self._backup_prefix = backup_prefix
        self._backup_topics = [f"{backup_prefix}{t}" for t in primary_topics]
        self._silence_timeout = silence_timeout

        self._last_key: dict[tuple[str, str, str], int] = {}
        self._last_received: dict[tuple[str, str, str], int] = {}

        self._is_active: bool = False
        self._backup_consumer = None
        self._no_message_since: float | None = None
        self._failover_start_time: float = 0.0

        self._switchback_filtering: dict[tuple[str, str, str], bool] = {}
        self._gap_checked: set[tuple[str, str, str]] = set()

    @property
    def is_active(self) -> bool:
        return self._is_active

    @property
    def backup_consumer(self):
        return self._backup_consumer

    def reset_silence_timer(self) -> None:
        self._no_message_since = time.monotonic()

    def should_activate(self) -> bool:
        if self._is_active:
            return False
        if self._no_message_since is None:
            return False
        return (time.monotonic() - self._no_message_since) > self._silence_timeout

    def track_record(self, envelope: dict) -> None:
        key = extract_natural_key(envelope)
        if key is None:
            return
        stream_key = (envelope.get("exchange", ""), envelope.get("symbol", ""), envelope.get("stream", ""))
        self._last_key[stream_key] = key
        received_at = envelope.get("received_at")
        if received_at is not None:
            self._last_received[stream_key] = received_at

    def should_filter(self, envelope: dict) -> bool:
        key = extract_natural_key(envelope)
        if key is None:
            return False
        stream_key = (envelope.get("exchange", ""), envelope.get("symbol", ""), envelope.get("stream", ""))
        last = self._last_key.get(stream_key)
        if last is None:
            return False
        return key <= last

    def activate(self) -> None:
        if self._is_active:
            return

        self._failover_start_time = time.monotonic()
        self._is_active = True
        self._switchback_filtering = {}
        self._gap_checked = set()
        writer_metrics.failover_active.set(1)
        writer_metrics.failover_total.inc()

        try:
            consumer = KafkaConsumer({
                "bootstrap.servers": ",".join(self._brokers),
                "group.id": f"writer-failover-{int(time.time())}",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 30000,
            })
        except Exception as exc:
            logger.error("failover_consumer_creation_failed", error=str(exc))
            self._is_active = False
            writer_metrics.failover_active.set(0)
            return

        self._backup_consumer = consumer

        all_partitions: list[TopicPartition] = []
        for backup_topic in self._backup_topics:
            try:
                md = consumer.list_topics(topic=backup_topic, timeout=5)
                topic_md = md.topics.get(backup_topic)
                if topic_md is None or topic_md.error is not None or not topic_md.partitions:
                    logger.warning("failover_topic_not_available", topic=backup_topic)
                    continue

                primary_topic = backup_topic[len(self._backup_prefix):]

                seek_ts_ns: int | None = None
                for stream_key, received_ns in self._last_received.items():
                    exchange, symbol, stream = stream_key
                    if f"{exchange}.{stream}" == primary_topic:
                        ts = received_ns - 10_000_000_000
                        if seek_ts_ns is None or ts < seek_ts_ns:
                            seek_ts_ns = ts

                seek_ts_ms = (seek_ts_ns // 1_000_000) if seek_ts_ns is not None else (int(time.time() * 1000) - 10000)

                for pid in topic_md.partitions:
                    all_partitions.append(TopicPartition(backup_topic, pid, seek_ts_ms))
            except Exception as exc:
                logger.warning("failover_topic_seek_failed", topic=backup_topic, error=str(exc))

        if all_partitions:
            try:
                offsets = consumer.offsets_for_times(all_partitions, timeout=5)
                consumer.assign(offsets)
            except Exception as exc:
                logger.error("failover_seek_failed", error=str(exc))
        else:
            logger.warning("failover_no_partitions_found")

        logger.info("failover_activated", backup_topics=self._backup_topics, partitions=len(all_partitions))

    def deactivate(self) -> None:
        if not self._is_active:
            return

        duration = time.monotonic() - self._failover_start_time
        writer_metrics.failover_duration_seconds.observe(duration)
        writer_metrics.switchback_total.inc()
        writer_metrics.failover_active.set(0)

        self._is_active = False
        self._switchback_filtering = {}

        if self._backup_consumer is not None:
            try:
                self._backup_consumer.close()
            except Exception:
                pass
            self._backup_consumer = None

        logger.info("failover_deactivated", duration_seconds=round(duration, 2))

    def begin_switchback(self) -> None:
        for stream_key in self._last_key:
            self._switchback_filtering[stream_key] = True

    def check_switchback_filter(self, envelope: dict) -> bool:
        stream_key = (envelope.get("exchange", ""), envelope.get("symbol", ""), envelope.get("stream", ""))

        if not self._switchback_filtering.get(stream_key):
            return False

        key = extract_natural_key(envelope)
        if key is None:
            return False

        last = self._last_key.get(stream_key)
        if last is not None and key <= last:
            return True

        self._switchback_filtering[stream_key] = False
        return False

    def check_failover_gap(
        self,
        stream_key: tuple[str, str, str],
        first_backup_key: int,
        first_backup_received_at: int,
    ) -> dict | None:
        last_key = self._last_key.get(stream_key)
        if last_key is None:
            return None

        last_received = self._last_received.get(stream_key, 0)

        if first_backup_key <= last_key + 1:
            return None

        exchange, symbol, stream = stream_key
        return create_gap_envelope(
            exchange=exchange, symbol=symbol, stream=stream,
            collector_session_id="", session_seq=-1,
            gap_start_ts=last_received, gap_end_ts=first_backup_received_at,
            reason="restart_gap",
            detail=f"Failover gap: primary key {last_key} -> backup key {first_backup_key}",
            received_at=first_backup_received_at,
        )

    def cleanup(self) -> None:
        if self._backup_consumer is not None:
            try:
                self._backup_consumer.close()
            except Exception:
                pass
            self._backup_consumer = None
        self._is_active = False
        writer_metrics.failover_active.set(0)


class CoverageFilter:
    """Tracks per-source data coverage and filters redundant gap envelopes.

    A gap belongs in the archive if and only if neither collector had data for
    that window. This filter drops gap envelopes whose window is already covered
    by data from the other source, and parks the rest briefly so backup data
    arriving late can still cover them.
    """

    def __init__(self, grace_period_seconds: float):
        self._grace_period = float(grace_period_seconds)
        # (exchange, symbol, stream) -> {"primary": received_at_ns, "backup": received_at_ns}
        self._last_received: dict[tuple[str, str, str], dict[str, int]] = {}
        # (source, stream_key, gap_start_ts) -> (envelope, first_seen_monotonic)
        self._pending: dict[tuple[str, tuple[str, str, str], int], tuple[dict, float]] = {}

    @property
    def enabled(self) -> bool:
        return self._grace_period > 0

    @property
    def pending_size(self) -> int:
        return len(self._pending)

    def last_received(self, source: str, stream_key: tuple[str, str, str]) -> int:
        return self._last_received.get(stream_key, {}).get(source, 0)

    def max_received(self, stream_key: tuple[str, str, str]) -> int:
        entry = self._last_received.get(stream_key, {})
        return max(entry.values(), default=0)

    def handle_data(self, source: str, envelope: dict) -> None:
        """Record a data envelope's arrival and drop any newly-covered pending gaps."""
        if not self.enabled:
            return
        received_at = envelope.get("received_at")
        if received_at is None:
            return
        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        coverage = self._last_received.setdefault(stream_key, {})
        if received_at > coverage.get(source, 0):
            coverage[source] = received_at

        if not self._pending:
            return

        from src.writer import metrics as writer_metrics

        # Sweep pending gaps: drop any whose other-source coverage now reaches gap_end_ts
        to_remove: list[tuple[str, tuple[str, str, str], int]] = []
        for key, (gap_env, _first_seen) in self._pending.items():
            pending_source, pending_stream, _ = key
            if pending_stream != stream_key:
                continue
            other_source = "backup" if pending_source == "primary" else "primary"
            other_received = coverage.get(other_source, 0)
            if other_received >= gap_env.get("gap_end_ts", 0):
                to_remove.append(key)
                writer_metrics.gap_envelopes_suppressed_total.labels(
                    source=pending_source, reason=gap_env.get("reason", "unknown"),
                ).inc()

        for key in to_remove:
            del self._pending[key]

        if to_remove:
            writer_metrics.gap_pending_size.set(len(self._pending))

    def handle_gap(self, source: str, envelope: dict) -> bool:
        """Try to suppress or park a gap envelope.

        Returns True if the envelope was handled (dropped or parked) — caller
        must NOT write it. Returns False if the filter is disabled — caller
        should write as usual.
        """
        if not self.enabled:
            return False

        from src.writer import metrics as writer_metrics

        stream_key = (
            envelope.get("exchange", ""),
            envelope.get("symbol", ""),
            envelope.get("stream", ""),
        )
        gap_start = envelope.get("gap_start_ts", 0)
        gap_end = envelope.get("gap_end_ts", 0)
        reason = envelope.get("reason", "unknown")

        other_source = "backup" if source == "primary" else "primary"
        other_received = self._last_received.get(stream_key, {}).get(other_source, 0)

        if other_received >= gap_end:
            writer_metrics.gap_envelopes_suppressed_total.labels(
                source=source, reason=reason,
            ).inc()
            return True

        # Park in pending queue, coalescing stacked records with the same gap_start.
        key = (source, stream_key, gap_start)
        existing = self._pending.get(key)
        if existing is not None:
            old_env, first_seen = existing
            if gap_end > old_env.get("gap_end_ts", 0):
                old_env["gap_end_ts"] = gap_end
                detail = envelope.get("detail")
                if detail:
                    old_env["detail"] = detail
            # Preserve first_seen so grace period counts from original arrival
            writer_metrics.gap_coalesced_total.labels(source=source).inc()
        else:
            self._pending[key] = (envelope, time.monotonic())

        writer_metrics.gap_pending_size.set(len(self._pending))
        return True

    def sweep_expired(self) -> list[dict]:
        """Return and remove pending gap envelopes whose grace period has elapsed.

        Caller must write these to the archive — they represent real bilateral outages.
        """
        if not self.enabled or not self._pending:
            return []
        now = time.monotonic()
        expired: list[dict] = []
        to_remove: list[tuple[str, tuple[str, str, str], int]] = []
        for key, (gap_env, first_seen) in self._pending.items():
            if (now - first_seen) >= self._grace_period:
                expired.append(gap_env)
                to_remove.append(key)
        for key in to_remove:
            del self._pending[key]
        if to_remove:
            from src.writer import metrics as writer_metrics
            writer_metrics.gap_pending_size.set(len(self._pending))
        return expired
