# Backup Collector with Gap Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a redundant WebSocket collector with backup Redpanda topics and automatic gap recovery in the writer, eliminating most data loss from single-connection drops.

**Architecture:** A second collector container writes to `backup.*` Redpanda topics via a configurable topic prefix. When the writer detects a gap, it reads the backup topic for the missing time window, deduplicates by natural key, and writes recovered records — emitting either no gap envelope (full recovery), a narrowed gap (partial), or the original gap (no backup data).

**Tech Stack:** Python 3.12, confluent_kafka (Consumer + Producer), zstandard, orjson, structlog, Docker Compose

**Spec:** `docs/superpowers/specs/2026-04-03-backup-collector-design.md`

---

### Task 1: Add configurable `topic_prefix` to the producer

**Files:**
- Modify: `src/collector/producer.py`
- Modify: `src/collector/main.py`
- Test: `tests/unit/test_producer_topic_prefix.py` (create)

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_producer_topic_prefix.py`:

```python
from unittest.mock import patch, MagicMock
from src.collector.producer import CryptoLakeProducer


def test_producer_default_topic_prefix_is_empty():
    with patch("src.collector.producer.KafkaProducer"):
        producer = CryptoLakeProducer(
            brokers=["localhost:9092"],
            exchange="binance",
        )
        assert producer.topic_prefix == ""


def test_producer_custom_topic_prefix():
    with patch("src.collector.producer.KafkaProducer"):
        producer = CryptoLakeProducer(
            brokers=["localhost:9092"],
            exchange="binance",
            topic_prefix="backup.",
        )
        assert producer.topic_prefix == "backup."


def test_produce_uses_topic_prefix():
    with patch("src.collector.producer.KafkaProducer") as MockKafka:
        mock_instance = MagicMock()
        MockKafka.return_value = mock_instance
        mock_instance.__len__ = lambda self: 0

        producer = CryptoLakeProducer(
            brokers=["localhost:9092"],
            exchange="binance",
            topic_prefix="backup.",
        )
        envelope = {
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": "trades", "received_at": 1000, "exchange_ts": 999,
            "collector_session_id": "test", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        producer.produce(envelope)

        # Check the topic passed to Kafka produce
        call_args = mock_instance.produce.call_args
        assert call_args.kwargs["topic"] == "backup.binance.trades"


def test_produce_without_prefix_uses_default_topic():
    with patch("src.collector.producer.KafkaProducer") as MockKafka:
        mock_instance = MagicMock()
        MockKafka.return_value = mock_instance
        mock_instance.__len__ = lambda self: 0

        producer = CryptoLakeProducer(
            brokers=["localhost:9092"],
            exchange="binance",
        )
        envelope = {
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": "trades", "received_at": 1000, "exchange_ts": 999,
            "collector_session_id": "test", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        producer.produce(envelope)

        call_args = mock_instance.produce.call_args
        assert call_args.kwargs["topic"] == "binance.trades"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_producer_topic_prefix.py -v`
Expected: FAIL — `topic_prefix` not an attribute

- [ ] **Step 3: Implement topic_prefix in producer**

In `src/collector/producer.py`, add `topic_prefix` parameter to `__init__`:

```python
def __init__(
    self,
    brokers: list[str],
    exchange: str,
    collector_session_id: str = "",
    max_buffer: int = 100_000,
    buffer_caps: dict[str, int] | None = None,
    default_stream_cap: int = 10_000,
    on_overflow: Callable[[str, str, str], None] | None = None,
    topic_prefix: str = "",
):
    self.exchange = exchange
    self.topic_prefix = topic_prefix
    # ... rest unchanged
```

Change every `topic = f"{self.exchange}.{stream}"` to `topic = f"{self.topic_prefix}{self.exchange}.{stream}"`. There are 3 occurrences:

1. In `produce()` method (line 68): `topic = f"{self.topic_prefix}{self.exchange}.{stream}"`
2. In `_emit_overflow_gap()` method (line 168): `topic = f"{self.topic_prefix}{self.exchange}.{stream}"`
3. In `emit_gap()` → calls `self.produce()` which already uses the prefix, so no change needed

- [ ] **Step 4: Update collector main.py to pass topic_prefix from environment**

In `src/collector/main.py`, in the `Collector.__init__` method, add:

```python
self.producer = CryptoLakeProducer(
    brokers=self.config.redpanda.brokers,
    exchange="binance",
    collector_session_id=self.session_id,
    max_buffer=producer_cfg.max_buffer,
    buffer_caps=producer_cfg.buffer_caps,
    default_stream_cap=producer_cfg.default_stream_cap,
    topic_prefix=os.environ.get("TOPIC_PREFIX", ""),
)
```

Add `import os` at the top of main.py (if not already present).

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_producer_topic_prefix.py -v`
Expected: 4 passed

- [ ] **Step 6: Commit**

```bash
git add src/collector/producer.py src/collector/main.py tests/unit/test_producer_topic_prefix.py
git commit -m "feat(backup): add configurable topic_prefix to producer"
```

---

### Task 2: Add backup recovery module to the writer

**Files:**
- Create: `src/writer/backup_recovery.py`
- Test: `tests/unit/test_backup_recovery.py` (create)

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_backup_recovery.py`:

```python
import time
from unittest.mock import MagicMock, patch
from src.writer.backup_recovery import recover_from_backup


def _make_envelope(stream, symbol, exchange_ts, raw_text="{}", trade_id=None):
    """Create a test data envelope."""
    raw = raw_text
    if trade_id is not None:
        raw = f'{{"a": {trade_id}, "T": {exchange_ts}}}'
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": symbol,
        "stream": stream, "received_at": exchange_ts * 1_000_000,
        "exchange_ts": exchange_ts,
        "collector_session_id": "backup-test", "session_seq": 0,
        "raw_text": raw, "raw_sha256": "abc",
        "_topic": f"backup.binance.{stream}", "_partition": 0, "_offset": 0,
    }


def test_recover_returns_empty_when_no_backup_configured():
    records, coverage = recover_from_backup(
        brokers=[],
        backup_topic_prefix="",
        stream="trades",
        symbol="btcusdt",
        exchange="binance",
        gap_start_ns=1000,
        gap_end_ns=2000,
    )
    assert records == []
    assert coverage == "none"


def test_recover_returns_empty_when_consumer_fails():
    records, coverage = recover_from_backup(
        brokers=["localhost:99999"],
        backup_topic_prefix="backup.",
        stream="trades",
        symbol="btcusdt",
        exchange="binance",
        gap_start_ns=1000,
        gap_end_ns=2000,
    )
    assert records == []
    assert coverage == "none"


def test_dedup_trades_by_trade_id():
    from src.writer.backup_recovery import _dedup_records

    records = [
        _make_envelope("trades", "btcusdt", 100, trade_id=1),
        _make_envelope("trades", "btcusdt", 101, trade_id=2),
        _make_envelope("trades", "btcusdt", 102, trade_id=2),  # duplicate
        _make_envelope("trades", "btcusdt", 103, trade_id=3),
    ]
    deduped = _dedup_records(records, "trades")
    assert len(deduped) == 3
    # Check trade IDs are 1, 2, 3
    ids = []
    import orjson
    for r in deduped:
        raw = orjson.loads(r["raw_text"])
        ids.append(raw.get("a"))
    assert ids == [1, 2, 3]


def test_dedup_depth_by_update_id():
    from src.writer.backup_recovery import _dedup_records

    records = [
        {"type": "data", "stream": "depth", "exchange_ts": 100,
         "raw_text": '{"u": 10, "pu": 9}'},
        {"type": "data", "stream": "depth", "exchange_ts": 101,
         "raw_text": '{"u": 11, "pu": 10}'},
        {"type": "data", "stream": "depth", "exchange_ts": 102,
         "raw_text": '{"u": 11, "pu": 10}'},  # duplicate
    ]
    deduped = _dedup_records(records, "depth")
    assert len(deduped) == 2


def test_determine_coverage_full():
    from src.writer.backup_recovery import _determine_coverage

    gap_start = 1000_000_000_000
    gap_end = 2000_000_000_000
    records = [
        {"received_at": 1000_000_000_000},
        {"received_at": 1500_000_000_000},
        {"received_at": 1999_000_000_000},
    ]
    coverage, new_start = _determine_coverage(records, gap_start, gap_end)
    assert coverage == "full"
    assert new_start is None


def test_determine_coverage_partial():
    from src.writer.backup_recovery import _determine_coverage

    gap_start = 1000_000_000_000
    gap_end = 3000_000_000_000
    records = [
        {"received_at": 1000_000_000_000},
        {"received_at": 1500_000_000_000},
    ]
    coverage, new_start = _determine_coverage(records, gap_start, gap_end)
    assert coverage == "partial"
    assert new_start > gap_start


def test_determine_coverage_none():
    from src.writer.backup_recovery import _determine_coverage

    coverage, new_start = _determine_coverage([], 1000, 2000)
    assert coverage == "none"
    assert new_start is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_backup_recovery.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement backup_recovery.py**

Create `src/writer/backup_recovery.py`:

```python
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

    # If the last backup record is within 5 seconds of the gap end,
    # consider it full coverage (the backup may have reconnected
    # slightly before the primary)
    gap_end_margin = gap_end_ns - 5_000_000_000  # 5 seconds margin
    if last_received >= gap_end_margin:
        return "full", None

    # Partial coverage — return the adjusted gap start
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
    Records are deduplicated by natural key and filtered to the gap window.
    """
    if not brokers or not backup_topic_prefix:
        return [], "none"

    backup_topic = f"{backup_topic_prefix}{exchange}.{stream}"

    try:
        from confluent_kafka import Consumer, TopicPartition, OFFSET_END

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
        # Get partitions for the backup topic
        md = consumer.list_topics(topic=backup_topic, timeout=5)
        topic_md = md.topics.get(backup_topic)
        if topic_md is None or topic_md.error is not None or not topic_md.partitions:
            logger.info("backup_topic_not_available", topic=backup_topic)
            return [], "none"

        # Assign all partitions and seek by timestamp
        partitions = [
            TopicPartition(backup_topic, p, gap_start_ns // 1_000_000)
            for p in topic_md.partitions
        ]
        offsets = consumer.offsets_for_times(partitions, timeout=5)
        consumer.assign(offsets)

        # Read records until gap_end_ns
        records: list[dict] = []
        deadline = time.monotonic() + 10  # max 10 seconds reading
        gap_end_ms = gap_end_ns // 1_000_000

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

            # Filter to matching symbol
            if envelope.get("symbol") != symbol:
                continue

            # Filter to data records only
            if envelope.get("type") != "data":
                continue

            received_ns = envelope.get("received_at", 0)

            # Stop if past gap end
            if received_ns > gap_end_ns + 5_000_000_000:
                break

            # Only include records within the gap window
            if gap_start_ns <= received_ns <= gap_end_ns + 5_000_000_000:
                records.append(envelope)

        # Deduplicate
        records = _dedup_records(records, stream)

        # Determine coverage
        coverage, _ = _determine_coverage(records, gap_start_ns, gap_end_ns)

        logger.info("backup_recovery_result",
                     stream=stream, symbol=symbol,
                     records_recovered=len(records),
                     coverage=coverage)

        return records, coverage

    except Exception as exc:
        logger.warning("backup_recovery_failed", stream=stream, error=str(exc))
        return [], "none"

    finally:
        try:
            consumer.close()
        except Exception:
            pass
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_backup_recovery.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/writer/backup_recovery.py tests/unit/test_backup_recovery.py
git commit -m "feat(backup): add backup recovery module with dedup and coverage detection"
```

---

### Task 3: Hook backup recovery into the writer's gap emission

**Files:**
- Modify: `src/writer/consumer.py`
- Modify: `src/writer/main.py`
- Test: `tests/unit/test_writer_backup_integration.py` (create)

- [ ] **Step 1: Write failing test**

Create `tests/unit/test_writer_backup_integration.py`:

```python
from unittest.mock import patch, MagicMock
from src.writer.consumer import WriterConsumer


def test_writer_consumer_has_backup_config():
    """WriterConsumer accepts backup config."""
    consumer = WriterConsumer(
        brokers=["localhost:9092"],
        topics=["binance.trades"],
        group_id="test",
        buffer_manager=MagicMock(),
        compressor=MagicMock(),
        state_manager=MagicMock(),
        base_dir="/tmp",
        backup_brokers=["localhost:9092"],
        backup_topic_prefix="backup.",
    )
    assert consumer._backup_brokers == ["localhost:9092"]
    assert consumer._backup_topic_prefix == "backup."


def test_writer_consumer_backup_config_defaults_empty():
    """WriterConsumer defaults to no backup."""
    consumer = WriterConsumer(
        brokers=["localhost:9092"],
        topics=["binance.trades"],
        group_id="test",
        buffer_manager=MagicMock(),
        compressor=MagicMock(),
        state_manager=MagicMock(),
        base_dir="/tmp",
    )
    assert consumer._backup_brokers == []
    assert consumer._backup_topic_prefix == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_writer_backup_integration.py -v`
Expected: FAIL — `unexpected keyword argument 'backup_brokers'`

- [ ] **Step 3: Add backup config to WriterConsumer**

In `src/writer/consumer.py`, add parameters to `__init__`:

```python
def __init__(
    self,
    brokers: list[str],
    topics: list[str],
    group_id: str,
    buffer_manager: BufferManager,
    compressor: ZstdFrameCompressor,
    state_manager: StateManager,
    base_dir: str,
    host_evidence: HostLifecycleEvidence | None = None,
    backup_brokers: list[str] | None = None,
    backup_topic_prefix: str = "",
):
```

Add after existing `self._host_evidence` assignment:

```python
    self._backup_brokers = backup_brokers or []
    self._backup_topic_prefix = backup_topic_prefix
```

- [ ] **Step 4: Add `_try_backup_recovery` method to WriterConsumer**

In `src/writer/consumer.py`, add import at top:

```python
from src.writer.backup_recovery import recover_from_backup
```

Add method to the class:

```python
def _try_backup_recovery(
    self, gap_envelope: dict,
) -> tuple[list[dict], dict | None]:
    """Attempt to recover gap data from backup topics.

    Returns (recovered_records, adjusted_gap_or_None).
    If full recovery: returns (records, None) — no gap needed.
    If partial: returns (records, narrowed_gap_envelope).
    If none: returns ([], original_gap_envelope).
    """
    if not self._backup_brokers or not self._backup_topic_prefix:
        return [], gap_envelope

    stream = gap_envelope.get("stream", "")
    symbol = gap_envelope.get("symbol", "")
    exchange = gap_envelope.get("exchange", "")
    gap_start = gap_envelope.get("gap_start_ts", 0)
    gap_end = gap_envelope.get("gap_end_ts", 0)

    if gap_start <= 0 or gap_end <= 0:
        return [], gap_envelope

    records, coverage = recover_from_backup(
        brokers=self._backup_brokers,
        backup_topic_prefix=self._backup_topic_prefix,
        stream=stream,
        symbol=symbol,
        exchange=exchange,
        gap_start_ns=gap_start,
        gap_end_ns=gap_end,
    )

    from src.writer import metrics as writer_metrics
    writer_metrics.backup_recovery_attempts.inc()

    if coverage == "full":
        writer_metrics.backup_recovery_success.inc()
        return records, None
    elif coverage == "partial":
        writer_metrics.backup_recovery_partial.inc()
        # Narrow the gap envelope
        last_received = max(r.get("received_at", 0) for r in records)
        narrowed = dict(gap_envelope)
        narrowed["gap_start_ts"] = last_received + 1
        narrowed["detail"] = (
            f"{gap_envelope.get('detail', '')} "
            f"[partial backup recovery: {len(records)} records]"
        )
        return records, narrowed
    else:
        writer_metrics.backup_recovery_miss.inc()
        return [], gap_envelope
```

- [ ] **Step 5: Hook recovery into gap emission paths**

In `src/writer/consumer.py`, find the two places where gap envelopes are written to the buffer and add recovery before each.

**Location 1: Recovery gap (around line 753-763)**

Replace:
```python
            recovery_gap = self._check_recovery_gap(envelope)
            if recovery_gap is not None:
                recovery_gap = add_broker_coordinates(
                    recovery_gap,
                    topic=msg_topic,
                    partition=msg_partition,
                    offset=-1,
                )
                gap_results = self.buffer_manager.add(recovery_gap)
                if gap_results:
                    await self._write_and_save(gap_results)
```

With:
```python
            recovery_gap = self._check_recovery_gap(envelope)
            if recovery_gap is not None:
                recovered, adjusted_gap = self._try_backup_recovery(recovery_gap)
                for rec in recovered:
                    rec_results = self.buffer_manager.add(rec)
                    if rec_results:
                        await self._write_and_save(rec_results)
                if adjusted_gap is not None:
                    adjusted_gap = add_broker_coordinates(
                        adjusted_gap,
                        topic=msg_topic,
                        partition=msg_partition,
                        offset=-1,
                    )
                    gap_results = self.buffer_manager.add(adjusted_gap)
                    if gap_results:
                        await self._write_and_save(gap_results)
```

**Location 2: Runtime session change (around line 767-777)**

Replace:
```python
            gap_envelope = await self._check_session_change(envelope)
            if gap_envelope is not None:
                gap_envelope = add_broker_coordinates(
                    gap_envelope,
                    topic=msg_topic,
                    partition=msg_partition,
                    offset=-1,
                )
                gap_results = self.buffer_manager.add(gap_envelope)
                if gap_results:
                    await self._write_and_save(gap_results)
```

With:
```python
            gap_envelope = await self._check_session_change(envelope)
            if gap_envelope is not None:
                recovered, adjusted_gap = self._try_backup_recovery(gap_envelope)
                for rec in recovered:
                    rec_results = self.buffer_manager.add(rec)
                    if rec_results:
                        await self._write_and_save(rec_results)
                if adjusted_gap is not None:
                    adjusted_gap = add_broker_coordinates(
                        adjusted_gap,
                        topic=msg_topic,
                        partition=msg_partition,
                        offset=-1,
                    )
                    gap_results = self.buffer_manager.add(adjusted_gap)
                    if gap_results:
                        await self._write_and_save(gap_results)
```

- [ ] **Step 6: Update Writer main.py to pass backup config**

In `src/writer/main.py`, update the `WriterConsumer` construction:

```python
self.consumer = WriterConsumer(
    brokers=self.config.redpanda.brokers,
    topics=self.topics,
    group_id="cryptolake-writer",
    buffer_manager=self.buffer_manager,
    compressor=self.compressor,
    state_manager=self.state_manager,
    base_dir=self.config.writer.base_dir,
    host_evidence=host_evidence,
    backup_brokers=self.config.redpanda.brokers,
    backup_topic_prefix=os.environ.get("BACKUP_TOPIC_PREFIX", "backup."),
)
```

Add `import os` at the top if not present.

- [ ] **Step 7: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_writer_backup_integration.py tests/unit/test_backup_recovery.py -v`
Expected: All passed

- [ ] **Step 8: Commit**

```bash
git add src/writer/consumer.py src/writer/main.py src/writer/backup_recovery.py tests/unit/test_writer_backup_integration.py
git commit -m "feat(backup): hook backup recovery into writer gap emission"
```

---

### Task 4: Add Prometheus metrics for backup recovery

**Files:**
- Modify: `src/writer/metrics.py`

- [ ] **Step 1: Add metrics**

In `src/writer/metrics.py`, add:

```python
from prometheus_client import Counter

backup_recovery_attempts = Counter(
    "writer_backup_recovery_attempts_total",
    "Total backup recovery attempts",
)
backup_recovery_success = Counter(
    "writer_backup_recovery_success_total",
    "Full recoveries from backup (no gap envelope needed)",
)
backup_recovery_partial = Counter(
    "writer_backup_recovery_partial_total",
    "Partial recoveries from backup (narrowed gap)",
)
backup_recovery_miss = Counter(
    "writer_backup_recovery_miss_total",
    "Backup had no data for the gap",
)
```

- [ ] **Step 2: Commit**

```bash
git add src/writer/metrics.py
git commit -m "feat(backup): add Prometheus metrics for backup recovery"
```

---

### Task 5: Docker infrastructure — backup collector service + network

**Files:**
- Modify: `docker-compose.yml`
- Modify: `infra/prometheus/prometheus.yml`

- [ ] **Step 1: Add backup_egress network and collector-backup service to docker-compose.yml**

Add to the `networks` section:

```yaml
  backup_egress:
    driver: bridge
```

Add after the `collector` service:

```yaml
  collector-backup:
    build:
      context: .
      dockerfile: Dockerfile.collector
    depends_on:
      redpanda:
        condition: service_healthy
    ports:
      - "127.0.0.1:8004:8000"
    networks:
      - cryptolake_internal
      - backup_egress
    volumes:
      - ./config:/app/config:ro
    environment:
      - CONFIG_PATH=/app/config/config.yaml
      - DATABASE__URL=postgresql://cryptolake:${POSTGRES_PASSWORD:-postgres}@postgres:5432/cryptolake
      - MONITORING__WEBHOOK_URL=${WEBHOOK_URL:-}
      - COLLECTOR_ID=binance-collector-backup
      - TOPIC_PREFIX=backup.
    healthcheck:
      test: ["CMD", "python", "-c", "from urllib.request import urlopen; raise SystemExit(0 if urlopen('http://127.0.0.1:8000/health', timeout=5).status == 200 else 1)"]
      interval: 15s
      timeout: 5s
      retries: 5
    restart: unless-stopped
    stop_grace_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
```

- [ ] **Step 2: Add Prometheus scrape target**

In `infra/prometheus/prometheus.yml`, add:

```yaml
  - job_name: collector-backup
    static_configs:
      - targets: ["collector-backup:8000"]
```

- [ ] **Step 3: Update collector main.py to read COLLECTOR_ID from environment**

In `src/collector/main.py`, update session_id construction to use env var:

```python
collector_id = os.environ.get("COLLECTOR_ID", self.exchange_cfg.collector_id)
self.session_id = f"{collector_id}_{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
```

- [ ] **Step 4: Verify docker-compose config**

Run: `docker compose config --quiet`
Expected: No errors

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml infra/prometheus/prometheus.yml src/collector/main.py
git commit -m "feat(backup): add backup collector Docker service and network"
```

---

### Task 6: Backup topic retention configuration

**Files:**
- Create: `scripts/setup-backup-topics.sh`

- [ ] **Step 1: Create retention setup script**

Create `scripts/setup-backup-topics.sh`:

```bash
#!/usr/bin/env bash
# Set 30-minute retention on all backup.* Redpanda topics.
# Run after Redpanda and backup collector are up.
set -euo pipefail

BROKER="${1:-redpanda:9092}"
RETENTION_MS=1800000  # 30 minutes

echo "Waiting for backup topics to be created..."
sleep 10

for TOPIC in $(rpk topic list --brokers "$BROKER" 2>/dev/null | grep '^backup\.' | awk '{print $1}'); do
    echo "Setting retention ${RETENTION_MS}ms on ${TOPIC}"
    rpk topic alter-config "$TOPIC" --set "retention.ms=${RETENTION_MS}" --brokers "$BROKER"
done

echo "Done."
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x scripts/setup-backup-topics.sh
git add scripts/setup-backup-topics.sh
git commit -m "feat(backup): add backup topic retention setup script"
```

---

### Task 7: Update infrastructure tests

**Files:**
- Modify: `tests/unit/test_infrastructure_assets.py`

- [ ] **Step 1: Update expected services and networks**

In the test that checks docker-compose services, add `"collector-backup"` to the expected set.

In the test that checks Prometheus scrape targets, add `"collector-backup"` to the expected set.

In the test that checks networks, add `"backup_egress"` if there's a network check.

- [ ] **Step 2: Run tests**

Run: `.venv/bin/python -m pytest tests/unit/test_infrastructure_assets.py -v -k compose`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_infrastructure_assets.py
git commit -m "test: update infra tests for backup collector"
```

---

### Task 8: Full test suite verification

- [ ] **Step 1: Run all tests**

Run: `.venv/bin/python -m pytest tests/ -v`
Expected: All pass

- [ ] **Step 2: Verify docker-compose builds**

Run: `docker compose config --quiet`
Expected: No errors

- [ ] **Step 3: Manual verification (if Redpanda is running)**

Start both collectors and verify backup topics are created:

```bash
docker compose up -d redpanda collector collector-backup
rpk topic list --brokers localhost:9092
# Should show both binance.* and backup.binance.* topics
```
