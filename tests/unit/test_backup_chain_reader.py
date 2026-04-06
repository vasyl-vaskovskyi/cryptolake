"""Tests for backup_chain_reader: reads last depth update ID from the other collector's topic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import orjson
import pytest

from src.collector.backup_chain_reader import read_last_depth_update_id, other_depth_topic


class TestOtherDepthTopic:
    def test_primary_checks_backup(self):
        assert other_depth_topic("", "binance") == "backup.binance.depth"

    def test_backup_checks_primary(self):
        assert other_depth_topic("backup.", "binance") == "binance.depth"

    def test_custom_prefix(self):
        assert other_depth_topic("staging.", "binance") == "backup.binance.depth"


class TestReadLastDepthUpdateId:
    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_u_from_recent_message(self, MockConsumer):
        mock_consumer = MagicMock()
        MockConsumer.return_value = mock_consumer

        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md = MagicMock()
        mock_md.topics = {"backup.binance.depth": mock_topic_md}
        mock_consumer.list_topics.return_value = mock_md

        import time
        now_ms = int(time.time() * 1000)
        envelope = {
            "type": "data", "symbol": "btcusdt", "stream": "depth",
            "received_at": time.time_ns(),
            "raw_text": orjson.dumps({"U": 100, "u": 200, "pu": 99}).decode(),
        }
        mock_msg = MagicMock()
        mock_msg.error.return_value = None
        mock_msg.value.return_value = orjson.dumps(envelope)
        mock_msg.timestamp.return_value = (1, now_ms)

        mock_consumer.poll.side_effect = [mock_msg, None]

        result = read_last_depth_update_id(
            brokers=["localhost:9092"], topic="backup.binance.depth",
            symbol="btcusdt", max_age_seconds=30,
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
            brokers=["localhost:9092"], topic="backup.binance.depth", symbol="btcusdt",
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
            brokers=["localhost:9092"], topic="backup.binance.depth", symbol="btcusdt",
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
        wrong_symbol = {
            "type": "data", "symbol": "ethusdt", "stream": "depth",
            "received_at": time.time_ns(),
            "raw_text": orjson.dumps({"U": 100, "u": 200, "pu": 99}).decode(),
        }
        mock_wrong = MagicMock()
        mock_wrong.error.return_value = None
        mock_wrong.value.return_value = orjson.dumps(wrong_symbol)
        mock_wrong.timestamp.return_value = (1, int(time.time() * 1000))

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
            brokers=["localhost:9092"], topic="backup.binance.depth", symbol="btcusdt",
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
            brokers=["localhost:9092"], topic="backup.binance.depth",
            symbol="btcusdt", max_age_seconds=30,
        )
        assert result is None

    @patch("src.collector.backup_chain_reader.Consumer")
    def test_returns_none_on_consumer_error(self, MockConsumer):
        MockConsumer.side_effect = Exception("connection refused")

        result = read_last_depth_update_id(
            brokers=["localhost:9092"], topic="backup.binance.depth", symbol="btcusdt",
        )
        assert result is None
