from unittest.mock import MagicMock
from src.writer.consumer import WriterConsumer


def test_writer_consumer_has_backup_config():
    consumer = WriterConsumer(
        brokers=["localhost:9092"], topics=["binance.trades"], group_id="test",
        buffer_manager=MagicMock(), compressor=MagicMock(),
        state_manager=MagicMock(), base_dir="/tmp",
        backup_brokers=["localhost:9092"], backup_topic_prefix="backup.",
    )
    assert consumer._backup_brokers == ["localhost:9092"]
    assert consumer._backup_topic_prefix == "backup."


def test_writer_consumer_backup_config_defaults_empty():
    consumer = WriterConsumer(
        brokers=["localhost:9092"], topics=["binance.trades"], group_id="test",
        buffer_manager=MagicMock(), compressor=MagicMock(),
        state_manager=MagicMock(), base_dir="/tmp",
    )
    assert consumer._backup_brokers == []
    assert consumer._backup_topic_prefix == ""
