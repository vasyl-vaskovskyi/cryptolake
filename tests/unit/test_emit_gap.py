from unittest.mock import MagicMock, patch
from src.collector.producer import CryptoLakeProducer

def test_emit_gap_produces_gap_envelope_and_increments_metric():
    with patch("confluent_kafka.Producer", autospec=True):
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance",
                                       collector_session_id="test-session")
    with patch.object(producer, "produce", return_value=True) as mock_produce, \
         patch("src.collector.producer.collector_metrics") as mock_metrics:
        producer.emit_gap(
            symbol="btcusdt", stream="depth", session_seq=42,
            reason="pu_chain_break", detail="test gap",
        )
        mock_produce.assert_called_once()
        gap = mock_produce.call_args[0][0]
        assert gap["type"] == "gap"
        assert gap["reason"] == "pu_chain_break"
        assert gap["exchange"] == "binance"
        assert gap["symbol"] == "btcusdt"
        assert gap["session_seq"] == 42
        mock_metrics.gaps_detected_total.labels.assert_called_once_with(
            exchange="binance", symbol="btcusdt", stream="depth", reason="pu_chain_break",
        )


def test_emit_gap_uses_custom_timestamps():
    with patch("confluent_kafka.Producer", autospec=True):
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance",
                                       collector_session_id="test-session")
    with patch.object(producer, "produce", return_value=True) as mock_produce, \
         patch("src.collector.producer.collector_metrics"):
        producer.emit_gap(
            symbol="ethusdt", stream="trades", session_seq=0,
            reason="ws_disconnect", detail="test",
            gap_start_ts=1000, gap_end_ts=2000,
        )
        gap = mock_produce.call_args[0][0]
        assert gap["gap_start_ts"] == 1000
        assert gap["gap_end_ts"] == 2000


def test_emit_overflow_gap_increments_gaps_detected_total():
    with patch("confluent_kafka.Producer", autospec=True):
        producer = CryptoLakeProducer(brokers=["localhost:9092"], exchange="binance",
                                       collector_session_id="test-session")
    with patch("src.collector.producer.collector_metrics") as mock_metrics:
        producer._emit_overflow_gap(symbol="btcusdt", stream="depth", window={"start_ts": 1000, "dropped": 5})
        mock_metrics.gaps_detected_total.labels.assert_called_once_with(
            exchange="binance", symbol="btcusdt", stream="depth", reason="buffer_overflow",
        )
