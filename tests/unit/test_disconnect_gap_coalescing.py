"""Tests for ws_disconnect gap coalescing on repeated reconnect attempts.

Relevant references:
- Class under test: src.collector.connection.WebSocketManager
- _PUBLIC_STREAMS = {"depth", "bookticker"} (src/exchanges/binance.py:15)
- _MARKET_STREAMS = {"trades", "funding_rate", "liquidations"} (src/exchanges/binance.py:16)
"""
from unittest.mock import MagicMock

from src.collector.connection import WebSocketManager


def _make_manager(enabled_streams):
    """Build a WebSocketManager with a mocked producer, skipping real WS setup."""
    producer = MagicMock()
    mgr = WebSocketManager.__new__(WebSocketManager)
    mgr.exchange = "binance"
    mgr.collector_session_id = "test_session"
    mgr.producer = producer
    mgr.symbols = ["btcusdt"]
    mgr.enabled_streams = list(enabled_streams)
    mgr.handlers = {}
    mgr._seq_counters = {}
    mgr._last_received_at = {}
    mgr._disconnect_gap_emitted = set()
    return mgr, producer


def test_first_disconnect_emits_one_gap_per_public_stream():
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # 1 symbol × 2 public streams (depth + bookticker)
    assert len(ws_calls) == 2


def test_repeated_disconnect_calls_coalesce():
    """Three consecutive _emit_disconnect_gaps calls (simulating 3 reconnect
    retries) should produce the SAME gaps as one call, not 3× more."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    mgr._emit_disconnect_gaps("public")
    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # Still only 2 (one per public stream), NOT 6.
    assert len(ws_calls) == 2


def test_data_arrival_clears_emitted_flag():
    """After data resumes on a stream, a subsequent disconnect should emit again."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("public")
    producer.emit_gap.reset_mock()

    # Simulate data arrival clearing the flag (happens in _receive_loop)
    mgr._disconnect_gap_emitted.discard(("btcusdt", "depth"))
    mgr._disconnect_gap_emitted.discard(("btcusdt", "bookticker"))

    mgr._emit_disconnect_gaps("public")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    assert len(ws_calls) == 2  # both streams re-emitted
