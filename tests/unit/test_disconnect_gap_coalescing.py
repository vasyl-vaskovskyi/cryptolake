"""Tests for ws_disconnect gap coalescing on repeated reconnect attempts.

Relevant references:
- Class under test: src.collector.connection.WebSocketManager
- _WS_STREAMS = {"depth", "bookticker", "trades", "funding_rate", "liquidations"}
  (src/exchanges/binance.py)
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
    mgr._subscribe_ack_at = {}
    mgr._disconnect_gap_emitted = set()
    return mgr, producer


def test_first_disconnect_emits_one_gap_per_enabled_stream():
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("ws")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # 1 symbol × 3 enabled WS streams (depth + bookticker + trades)
    assert len(ws_calls) == 3


def test_repeated_disconnect_calls_coalesce():
    """Three consecutive _emit_disconnect_gaps calls (simulating 3 reconnect
    retries) should produce the SAME gaps as one call, not 3× more."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("ws")
    mgr._emit_disconnect_gaps("ws")
    mgr._emit_disconnect_gaps("ws")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    # Still only 3 (one per enabled stream), NOT 9.
    assert len(ws_calls) == 3


def test_data_arrival_clears_emitted_flag():
    """After data resumes on a stream, a subsequent disconnect should emit again."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    mgr._emit_disconnect_gaps("ws")
    producer.emit_gap.reset_mock()

    # Simulate data arrival clearing the flag (happens in _receive_loop)
    mgr._disconnect_gap_emitted.discard(("btcusdt", "depth"))
    mgr._disconnect_gap_emitted.discard(("btcusdt", "bookticker"))
    mgr._disconnect_gap_emitted.discard(("btcusdt", "trades"))

    mgr._emit_disconnect_gaps("ws")
    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    assert len(ws_calls) == 3  # all three streams re-emitted


def test_gap_start_uses_ack_baseline_when_no_data_received():
    """A stream that had a SUBSCRIBE ack but never delivered a data frame
    must still produce a non-zero-width gap window. Without this, watchdog-
    triggered reconnects on cold-boot half-opens emit invisible zero-width
    gaps and downstream consumers see no marker for the missed period."""
    mgr, producer = _make_manager(["depth", "bookticker", "trades"])
    # Simulate SUBSCRIBE ack 5 seconds ago, but no data ever arrived
    ack_ns = 1_000_000_000_000_000_000
    for st in ("depth", "bookticker", "trades"):
        mgr._subscribe_ack_at[("btcusdt", st)] = ack_ns
    # _last_received_at remains empty — no frame ever delivered

    mgr._emit_disconnect_gaps("ws")

    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    assert len(ws_calls) == 3
    for call in ws_calls:
        assert call.kwargs["gap_start_ts"] == ack_ns, \
            "gap_start should fall back to subscribe_ack timestamp, not 'now'"
        assert call.kwargs["gap_end_ts"] > ack_ns, \
            "gap window must be non-zero when ack happened in the past"


def test_gap_start_prefers_last_received_over_ack():
    """When data has flowed, gap_start uses the most-recent data timestamp,
    not the older SUBSCRIBE ack."""
    mgr, producer = _make_manager(["depth"])
    ack_ns = 1_000_000_000_000_000_000
    last_data_ns = ack_ns + 5_000_000_000  # 5s after ack
    mgr._subscribe_ack_at[("btcusdt", "depth")] = ack_ns
    mgr._last_received_at[("btcusdt", "depth")] = last_data_ns

    mgr._emit_disconnect_gaps("ws")

    ws_calls = [c for c in producer.emit_gap.call_args_list
                if c.kwargs.get("reason") == "ws_disconnect"]
    assert len(ws_calls) == 1
    assert ws_calls[0].kwargs["gap_start_ts"] == last_data_ns
