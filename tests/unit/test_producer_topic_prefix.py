from unittest.mock import patch, MagicMock
from src.collector.producer import CryptoLakeProducer


def test_producer_default_topic_prefix_is_empty():
    with patch("src.collector.producer.KafkaProducer"):
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance")
        assert producer.topic_prefix == ""


def test_producer_custom_topic_prefix():
    with patch("src.collector.producer.KafkaProducer"):
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance", topic_prefix="backup.")
        assert producer.topic_prefix == "backup."


def test_produce_uses_topic_prefix():
    with patch("src.collector.producer.KafkaProducer") as MockKafka:
        mock_instance = MagicMock()
        MockKafka.return_value = mock_instance
        mock_instance.__len__ = lambda self: 0
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance", topic_prefix="backup.")
        envelope = {
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": "trades", "received_at": 1000, "exchange_ts": 999,
            "collector_session_id": "test", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        producer.produce(envelope)
        call_args = mock_instance.produce.call_args
        assert call_args.kwargs["topic"] == "backup.binance.trades"


def test_produce_without_prefix_uses_default_topic():
    with patch("src.collector.producer.KafkaProducer") as MockKafka:
        mock_instance = MagicMock()
        MockKafka.return_value = mock_instance
        mock_instance.__len__ = lambda self: 0
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance")
        envelope = {
            "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
            "stream": "trades", "received_at": 1000, "exchange_ts": 999,
            "collector_session_id": "test", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
        }
        producer.produce(envelope)
        call_args = mock_instance.produce.call_args
        assert call_args.kwargs["topic"] == "binance.trades"
