# Smart Depth Resync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Skip the depth resync snapshot on collector startup when the other collector's depth topic has recent diffs — seed the pu chain from the backup's last update ID instead.

**Architecture:** New module `backup_chain_reader.py` reads the last depth diff from the other collector's topic in Redpanda. If recent (< 30s), extracts the final update ID `u` and returns it. `_depth_resync()` in `connection.py` calls this before fetching a snapshot — if it gets a valid `u`, it calls `set_sync_point(u)` and returns without the REST call.

**Tech Stack:** Python 3.12, confluent_kafka (Consumer), orjson, pytest

**Spec:** `docs/superpowers/specs/2026-04-06-smart-depth-resync-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/collector/backup_chain_reader.py` | Create | One-shot read of the other collector's depth topic to get last update ID |
| `src/collector/connection.py` | Modify | Call backup chain reader before snapshot fetch in `_depth_resync()` |
| `src/collector/main.py` | Modify | Pass `brokers` to `WebSocketManager` |
| `tests/unit/test_backup_chain_reader.py` | Create | Tests for backup chain reader |
| `tests/unit/test_depth_resync_skip.py` | Create | Tests for the skip-snapshot logic in `_depth_resync()` |

---

### Task 1: Create backup_chain_reader module with tests

**Files:**
- Create: `tests/unit/test_backup_chain_reader.py`
- Create: `src/collector/backup_chain_reader.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_backup_chain_reader.py`:

```python
"""Tests for backup_chain_reader: reads last depth update ID from the other collector's topic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import orjson
import pytest

from src.collector.backup_chain_reader import read_last_depth_update_id, other_depth_topic


class TestOtherDepthTopic:
    """Derive the other collector's depth topic from our own prefix."""

    def test_primary_checks_backup(self):
        assert other_depth_topic("", "binance") == "backup.binance.depth"

    def test_backup_checks_primary(self):
        assert other_depth_topic("backup.", "binance") == "binance.depth"

    def test_custom_prefix(self):
        assert other_depth_topic("staging.", "binance") == "backup.binance.depth"


class TestReadLastDepthUpdateId:
    """read_last_depth_update_id returns last u from recent backup depth diffs."""

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_u_from_recent_message(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        # Mock topic metadata
        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md = MagicMock()
        mock_md.topics = {"backup.binance.depth": mock_topic_md}
        mock_consumer.list_topics.return_value = mock_md

        # Mock message with recent timestamp and matching symbol
        import time
        now_ms = int(time.time() * 1000)
        envelope = {
            "type": "data",
            "symbol": "btcusdt",
            "stream": "depth",
            "received_at": time.time_ns(),
            "raw_text": orjson.dumps({"U": 100, "u": 200, "pu": 99}).decode(),
        }
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(envelope)
        mock_msg.timestamp.return_value = (1, now_ms)

        mock_consumer.poll.side_effect = [mock_msg, None]

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )
        assert result == 200
        mock_consumer.close.assert_called_once()

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_none_when_no_messages(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md = MagicMock()
        mock_md.topics = {"backup.binance.depth": mock_topic_md}
        mock_consumer.list_topics.return_value = mock_md

        mock_consumer.poll.return_value = None

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
        )
        assert result is None
        mock_consumer.close.assert_called_once()

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_none_when_topic_not_found(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        mock_md = MagicMock()
        mock_md.topics = {}
        mock_consumer.list_topics.return_value = mock_md

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
        )
        assert result is None

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_filters_by_symbol(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md = MagicMock()
        mock_md.topics = {"backup.binance.depth": mock_topic_md}
        mock_consumer.list_topics.return_value = mock_md

        import time
        # Message for wrong symbol
        wrong_symbol = {
            "type": "data", "symbol": "ethusdt", "stream": "depth",
            "received_at": time.time_ns(),
            "raw_text": orjson.dumps({"U": 100, "u": 200, "pu": 99}).decode(),
        }
        mock_wrong = MagicMock()
        mock_wrong.error.return_value = None
        mock_wrong.value.return_value = orjson.dumps(wrong_symbol)
        mock_wrong.timestamp.return_value = (1, int(time.time() * 1000))

        # Message for correct symbol
        right_symbol = {
            "type": "data", "symbol": "btcusdt", "stream": "depth",
            "received_at": time.time_ns(),
            "raw_text": orjson.dumps({"U": 300, "u": 400, "pu": 299}).decode(),
        }
        mock_right = MagicMock()
        mock_right.error.return_value = None
        mock_right.value.return_value = orjson.dumps(right_symbol)
        mock_right.timestamp.return_value = (1, int(time.time() * 1000))

        mock_consumer.poll.side_effect = [mock_wrong, mock_right, None]

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
        )
        assert result == 400

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_none_when_message_too_old(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md = MagicMock()
        mock_md.topics = {"backup.binance.depth": mock_topic_md}
        mock_consumer.list_topics.return_value = mock_md

        # Message from 60 seconds ago
        import time
        old_ms = int(time.time() * 1000) - 60_000
        envelope = {
            "type": "data", "symbol": "btcusdt", "stream": "depth",
            "received_at": (time.time_ns() - 60_000_000_000),
            "raw_text": orjson.dumps({"U": 100, "u": 200, "pu": 99}).decode(),
        }
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(envelope)
        mock_msg.timestamp.return_value = (1, old_ms)

        mock_consumer.poll.side_effect = [mock_msg, None]

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )
        assert result is None

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_none_on_consumer_error(self, MockConsumer):
        MockConsumer.side_effect = Exception("connection refused")

        result = read_last_depth_update_id(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
        )
        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_backup_chain_reader.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'src.collector.backup_chain_reader'`

- [ ] **Step 3: Write implementation**

Create `src/collector/backup_chain_reader.py`:

```python
"""One-shot reader: get the last depth update ID from the other collector's topic."""
from __future__ import annotations

import time

import orjson
import structlog

logger = structlog.get_logger()


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
        from confluent_kafka import Consumer, TopicPartition

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_backup_chain_reader.py -v`
Expected: All 7 tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/collector/backup_chain_reader.py tests/unit/test_backup_chain_reader.py
git commit -m "feat(depth): add backup_chain_reader for reading last depth update ID from other collector"
```

---

### Task 2: Pass brokers to WebSocketManager and integrate backup chain reader into _depth_resync

**Files:**
- Modify: `src/collector/main.py` (line 66-74)
- Modify: `src/collector/connection.py` (lines 29-47 and 186-256)
- Create: `tests/unit/test_depth_resync_skip.py`

- [ ] **Step 1: Write failing tests for the skip-snapshot logic**

Create `tests/unit/test_depth_resync_skip.py`:

```python
"""Tests for smart depth resync: skip snapshot when backup has recent depth diffs."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.collector.connection import WebSocketManager
from src.collector.streams.depth import DepthHandler


def _make_ws_manager(*, topic_prefix=""):
    """Create a WebSocketManager with mocked dependencies."""
    adapter = MagicMock()
    producer = MagicMock()
    producer.topic_prefix = topic_prefix
    producer.exchange = "binance"
    producer.is_healthy_for_resync.return_value = True

    depth_handler = MagicMock(spec=DepthHandler)
    depth_handler.detectors = {"btcusdt": MagicMock()}

    handlers = {"depth": depth_handler}

    ws = WebSocketManager(
        exchange="binance",
        collector_session_id="test-session",
        adapter=adapter,
        producer=producer,
        handlers=handlers,
        symbols=["btcusdt"],
        enabled_streams=["depth", "trades"],
        brokers=["localhost:9092"],
    )
    return ws, depth_handler, producer


class TestDepthResyncSkipsSnapshot:
    """When backup has recent depth diffs, _depth_resync skips the REST snapshot."""

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_skips_snapshot_when_backup_has_recent_data(self, mock_read):
        mock_read.return_value = 12345
        ws, depth_handler, producer = _make_ws_manager()

        await ws._depth_resync("btcusdt")

        # Should have called set_sync_point with backup's u
        depth_handler.set_sync_point.assert_called_once_with("btcusdt", 12345)
        # Should NOT have fetched a snapshot from REST
        ws.adapter.build_snapshot_url.assert_not_called()
        # Should NOT have produced a depth_snapshot envelope
        producer.produce.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_falls_through_to_snapshot_when_no_backup_data(self, mock_read):
        mock_read.return_value = None
        ws, depth_handler, producer = _make_ws_manager()

        # Mock the REST snapshot fetch
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.text = AsyncMock(return_value='{"lastUpdateId": 99999}')
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("src.collector.connection.aiohttp.ClientSession", return_value=mock_session):
            ws.adapter.build_snapshot_url.return_value = "https://example.com/depth"
            ws.adapter.parse_snapshot_last_update_id.return_value = 99999

            await ws._depth_resync("btcusdt")

        # Should have fetched snapshot from REST
        ws.adapter.build_snapshot_url.assert_called_once_with("btcusdt")
        # Should have called set_sync_point with snapshot's lastUpdateId
        depth_handler.set_sync_point.assert_called_once_with("btcusdt", 99999)

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_backup_check_uses_correct_topic_for_primary(self, mock_read):
        mock_read.return_value = 500
        ws, depth_handler, _ = _make_ws_manager(topic_prefix="")

        await ws._depth_resync("btcusdt")

        mock_read.assert_called_once_with(
            brokers=["localhost:9092"],
            topic="backup.binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_backup_check_uses_correct_topic_for_backup_collector(self, mock_read):
        mock_read.return_value = 500
        ws, depth_handler, _ = _make_ws_manager(topic_prefix="backup.")

        await ws._depth_resync("btcusdt")

        mock_read.assert_called_once_with(
            brokers=["localhost:9092"],
            topic="binance.depth",
            symbol="btcusdt",
            max_age_seconds=30,
        )

    @pytest.mark.asyncio
    @patch("src.collector.connection.read_last_depth_update_id")
    async def test_logs_skip_on_backup_hit(self, mock_read):
        mock_read.return_value = 12345
        ws, depth_handler, _ = _make_ws_manager()

        with patch("src.collector.connection.logger") as mock_logger:
            await ws._depth_resync("btcusdt")
            mock_logger.info.assert_any_call(
                "depth_resync_skipped_snapshot",
                symbol="btcusdt",
                backup_last_u=12345,
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/python -m pytest tests/unit/test_depth_resync_skip.py -v`
Expected: FAIL — `WebSocketManager.__init__()` doesn't accept `brokers` parameter

- [ ] **Step 3: Add `brokers` parameter to WebSocketManager**

In `src/collector/connection.py`, modify `__init__` (line 29-47):

Add `brokers: list[str]` parameter and store it:

```python
    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        adapter: BinanceAdapter,
        producer: CryptoLakeProducer,
        handlers: dict[str, StreamHandler],
        symbols: list[str],
        enabled_streams: list[str],
        brokers: list[str] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.adapter = adapter
        self.producer = producer
        self.handlers = handlers
        self.symbols = symbols
        self.enabled_streams = enabled_streams
        self._brokers = brokers or []
        self._seq_counters: dict[tuple[str, str], int] = {}
```

In `src/collector/main.py`, pass brokers to `WebSocketManager` (line 66-74):

```python
        self.ws_manager = WebSocketManager(
            exchange="binance",
            collector_session_id=self.session_id,
            adapter=self.adapter,
            producer=self.producer,
            handlers=self.handlers,
            symbols=self.symbols,
            enabled_streams=self.enabled_streams,
            brokers=self.config.redpanda.brokers,
        )
```

- [ ] **Step 4: Add backup chain reader call to `_depth_resync`**

In `src/collector/connection.py`, add the import at the top (after existing imports):

```python
from src.collector.backup_chain_reader import read_last_depth_update_id, other_depth_topic
```

Modify `_depth_resync()` — insert the backup check after the producer health wait (line 211) and `depth_handler.reset(symbol)` (line 213), but BEFORE the snapshot fetch retry loop (line 215):

```python
        depth_handler.reset(symbol)

        # Check if the other collector's depth topic has recent diffs.
        # If so, seed the pu chain from there and skip the REST snapshot.
        if self._brokers:
            backup_topic = other_depth_topic(self.producer.topic_prefix, self.exchange)
            backup_u = read_last_depth_update_id(
                brokers=self._brokers,
                topic=backup_topic,
                symbol=symbol,
                max_age_seconds=30,
            )
            if backup_u is not None:
                depth_handler.set_sync_point(symbol, backup_u)
                logger.info("depth_resync_skipped_snapshot",
                            symbol=symbol, backup_last_u=backup_u)
                return

        retries = 3
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/python -m pytest tests/unit/test_depth_resync_skip.py tests/unit/test_backup_chain_reader.py -v`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add src/collector/connection.py src/collector/main.py tests/unit/test_depth_resync_skip.py
git commit -m "feat(depth): skip resync snapshot when backup collector has recent depth diffs"
```

---

### Task 3: Update chaos test 17 to verify depth sync passes

**Files:**
- Modify: `tests/chaos/17_collector_failover_to_backup.sh`

- [ ] **Step 1: Add depth sync verification to chaos test 17**

After the existing `cryptolake verify` step in `tests/chaos/17_collector_failover_to_backup.sh`, the test should already pass since the collector now skips the unnecessary resync snapshot. No new assertions needed — the existing `cryptolake verify --full` will now pass because no false depth sync point errors are produced.

However, add a log-check assertion to verify the collector actually skipped the snapshot. After step 10 (writer switchback), add:

```bash
step 10.5 "Checking that restarted collector skipped depth snapshot..."
if $COMPOSE logs collector 2>&1 | grep -q "depth_resync_skipped_snapshot"; then
    pass "collector skipped depth resync snapshot (backup had recent data)"
else
    fail "collector did NOT skip depth resync snapshot"
fi
```

- [ ] **Step 2: Run chaos test 17**

Run: `bash tests/chaos/17_collector_failover_to_backup.sh`
Expected: All assertions PASS including `cryptolake verify`

- [ ] **Step 3: Commit**

```bash
git add tests/chaos/17_collector_failover_to_backup.sh
git commit -m "test(chaos): verify collector skips depth snapshot during failover in test 17"
```

---

### Task 4: Run all tests and verify

- [ ] **Step 1: Run unit tests**

Run: `.venv/bin/python -m pytest tests/unit/test_backup_chain_reader.py tests/unit/test_depth_resync_skip.py tests/unit/test_failover.py tests/unit/test_consumer_failover_integration.py -v`
Expected: All PASS

- [ ] **Step 2: Verify no import issues**

Run: `.venv/bin/python -c "from src.collector.backup_chain_reader import read_last_depth_update_id, other_depth_topic; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Final commit if any cleanup needed**

---

## Verification

1. **Unit tests:** `pytest tests/unit/test_backup_chain_reader.py tests/unit/test_depth_resync_skip.py -v`
2. **Import check:** `python -c "from src.collector.backup_chain_reader import read_last_depth_update_id"`
3. **Chaos test 17:** `bash tests/chaos/17_collector_failover_to_backup.sh` — `cryptolake verify` should now pass
4. **Chaos test 1:** `bash tests/chaos/1_collector_unclean_exit.sh` — should also benefit if backup collector is running
