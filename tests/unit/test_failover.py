"""Tests for writer real-time failover state machine."""
from __future__ import annotations

import pytest

from pathlib import Path

from src.common.config import load_config
from src.writer.failover import extract_natural_key


class TestExtractNaturalKey:
    def test_trades_uses_aggregate_trade_id(self):
        envelope = {"type": "data", "stream": "trades", "raw_text": '{"a": 12345, "p": "100.0"}'}
        assert extract_natural_key(envelope) == 12345

    def test_depth_uses_update_id(self):
        envelope = {"type": "data", "stream": "depth", "raw_text": '{"u": 9999, "pu": 9998}'}
        assert extract_natural_key(envelope) == 9999

    def test_bookticker_uses_update_id(self):
        envelope = {"type": "data", "stream": "bookticker", "raw_text": '{"u": 5555}'}
        assert extract_natural_key(envelope) == 5555

    def test_funding_rate_uses_exchange_ts(self):
        envelope = {"type": "data", "stream": "funding_rate", "exchange_ts": 1700000000000, "raw_text": "{}"}
        assert extract_natural_key(envelope) == 1700000000000

    def test_liquidations_uses_exchange_ts(self):
        envelope = {"type": "data", "stream": "liquidations", "exchange_ts": 1700000000001, "raw_text": "{}"}
        assert extract_natural_key(envelope) == 1700000000001

    def test_open_interest_uses_exchange_ts(self):
        envelope = {"type": "data", "stream": "open_interest", "exchange_ts": 1700000000002, "raw_text": "{}"}
        assert extract_natural_key(envelope) == 1700000000002

    def test_depth_snapshot_uses_exchange_ts(self):
        envelope = {"type": "data", "stream": "depth_snapshot", "exchange_ts": 1700000000003, "raw_text": "{}"}
        assert extract_natural_key(envelope) == 1700000000003

    def test_missing_raw_text_returns_none(self):
        assert extract_natural_key({"type": "data", "stream": "trades"}) is None

    def test_malformed_raw_text_returns_none(self):
        assert extract_natural_key({"type": "data", "stream": "trades", "raw_text": "not json"}) is None

    def test_missing_key_in_raw_returns_none(self):
        assert extract_natural_key({"type": "data", "stream": "trades", "raw_text": '{"p": "100.0"}'}) is None

    def test_gap_envelope_returns_none(self):
        assert extract_natural_key({"type": "gap", "stream": "trades", "raw_text": '{"a": 1}'}) is None


import time
from unittest.mock import MagicMock, patch

from src.writer.failover import CoverageFilter, FailoverManager
from src.writer import metrics as writer_metrics


class TestFailoverManagerInit:
    def test_initial_state_is_not_active(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        assert fm.is_active is False

    def test_initial_backup_consumer_is_none(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        assert fm._backup_consumer is None

    def test_last_key_starts_empty(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        assert fm._last_key == {}

    def test_backup_topics_derived_from_primary(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades", "binance.depth"])
        assert fm._backup_topics == ["backup.binance.trades", "backup.binance.depth"]

    def test_custom_backup_prefix(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"], backup_prefix="bk.")
        assert fm._backup_topics == ["bk.binance.trades"]


class TestFailoverManagerKeyTracking:
    def test_track_record_updates_last_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
                     "received_at": 1000_000_000_000_000_000, "raw_text": '{"a": 500}'}
        fm.track_record(envelope)
        assert fm._last_key[("binance", "btcusdt", "trades")] == 500

    def test_track_record_updates_last_received(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
                     "received_at": 1000_000_000_000_000_000, "raw_text": '{"a": 500}'}
        fm.track_record(envelope)
        assert fm._last_received[("binance", "btcusdt", "trades")] == 1000_000_000_000_000_000

    def test_track_record_ignores_gap_envelopes(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        envelope = {"type": "gap", "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
                     "received_at": 1000_000_000_000_000_000}
        fm.track_record(envelope)
        assert fm._last_key == {}

    def test_track_record_skips_if_no_natural_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
                     "raw_text": '{"p": "100.0"}', "received_at": 1000_000_000_000_000_000}
        fm.track_record(envelope)
        assert ("binance", "btcusdt", "trades") not in fm._last_key


class TestFailoverShouldActivate:
    def test_should_not_activate_before_timeout(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.reset_silence_timer()
        assert fm.should_activate() is False

    def test_should_activate_after_timeout(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"], silence_timeout=0.01)
        fm.reset_silence_timer()
        time.sleep(0.02)
        assert fm.should_activate() is True

    def test_should_not_activate_if_already_active(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"], silence_timeout=0.01)
        fm.reset_silence_timer()
        fm._is_active = True
        time.sleep(0.02)
        assert fm.should_activate() is False

    def test_should_not_activate_before_first_message(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        assert fm.should_activate() is False


class TestFailoverShouldFilter:
    def test_filter_backup_record_with_lower_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 499}'}
        assert fm.should_filter(envelope) is True

    def test_filter_backup_record_with_equal_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 500}'}
        assert fm.should_filter(envelope) is True

    def test_accept_backup_record_with_higher_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 501}'}
        assert fm.should_filter(envelope) is False

    def test_accept_when_no_last_key_for_stream(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 1}'}
        assert fm.should_filter(envelope) is False


class TestFailoverActivate:
    @patch("src.writer.failover.KafkaConsumer")
    def test_activate_creates_backup_consumer(self, MockConsumer):
        mock_instance = MagicMock()
        MockConsumer.return_value = mock_instance
        mock_md = MagicMock()
        mock_topic_md = MagicMock()
        mock_topic_md.error = None
        mock_topic_md.partitions = {0: MagicMock()}
        mock_md.topics = {"backup.binance.trades": mock_topic_md}
        mock_instance.list_topics.return_value = mock_md

        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_received[("binance", "btcusdt", "trades")] = 1000_000_000_000_000_000
        fm.reset_silence_timer()
        fm.activate()

        assert fm.is_active is True
        assert fm._backup_consumer is mock_instance
        MockConsumer.assert_called_once()

    @patch("src.writer.failover.KafkaConsumer")
    def test_activate_increments_failover_total(self, MockConsumer):
        mock_instance = MagicMock()
        MockConsumer.return_value = mock_instance
        mock_md = MagicMock()
        mock_md.topics = {}
        mock_instance.list_topics.return_value = mock_md

        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.reset_silence_timer()
        before_val = writer_metrics.failover_total._value.get()
        fm.activate()
        assert writer_metrics.failover_total._value.get() == before_val + 1


class TestFailoverDeactivate:
    @patch("src.writer.failover.KafkaConsumer")
    def test_deactivate_closes_backup_consumer(self, MockConsumer):
        mock_instance = MagicMock()
        MockConsumer.return_value = mock_instance
        mock_md = MagicMock()
        mock_md.topics = {}
        mock_instance.list_topics.return_value = mock_md

        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.reset_silence_timer()
        fm.activate()
        fm.deactivate()

        assert fm.is_active is False
        assert fm._backup_consumer is None
        mock_instance.close.assert_called_once()

    @patch("src.writer.failover.KafkaConsumer")
    def test_deactivate_increments_switchback_total(self, MockConsumer):
        mock_instance = MagicMock()
        MockConsumer.return_value = mock_instance
        mock_md = MagicMock()
        mock_md.topics = {}
        mock_instance.list_topics.return_value = mock_md

        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.reset_silence_timer()
        fm.activate()
        before_val = writer_metrics.switchback_total._value.get()
        fm.deactivate()
        assert writer_metrics.switchback_total._value.get() == before_val + 1

    def test_deactivate_when_not_active_is_noop(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.deactivate()
        assert fm.is_active is False


class TestSwitchbackFiltering:
    def test_switchback_filters_overlapping_primary_record(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        fm._switchback_filtering[("binance", "btcusdt", "trades")] = True
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 500}'}
        assert fm.check_switchback_filter(envelope) is True

    def test_switchback_stops_filtering_on_higher_key(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        fm._switchback_filtering[("binance", "btcusdt", "trades")] = True
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 501}'}
        assert fm.check_switchback_filter(envelope) is False
        assert fm._switchback_filtering[("binance", "btcusdt", "trades")] is False

    def test_switchback_no_filter_when_not_in_switchback_mode(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        envelope = {"type": "data", "exchange": "binance", "symbol": "btcusdt", "stream": "trades", "raw_text": '{"a": 500}'}
        assert fm.check_switchback_filter(envelope) is False

    def test_begin_switchback_enables_filtering_for_all_tracked_streams(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades", "binance.depth"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        fm._last_key[("binance", "btcusdt", "depth")] = 900
        fm.begin_switchback()
        assert fm._switchback_filtering[("binance", "btcusdt", "trades")] is True
        assert fm._switchback_filtering[("binance", "btcusdt", "depth")] is True


class TestFailoverGapEnvelope:
    def test_no_gap_when_backup_key_is_contiguous(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        fm._last_received[("binance", "btcusdt", "trades")] = 1000_000_000_000_000_000
        gap = fm.check_failover_gap(
            stream_key=("binance", "btcusdt", "trades"),
            first_backup_key=501, first_backup_received_at=1000_000_005_000_000_000,
        )
        assert gap is None

    def test_gap_when_backup_key_skips(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm._last_key[("binance", "btcusdt", "trades")] = 500
        fm._last_received[("binance", "btcusdt", "trades")] = 1000_000_000_000_000_000
        gap = fm.check_failover_gap(
            stream_key=("binance", "btcusdt", "trades"),
            first_backup_key=510, first_backup_received_at=1000_000_005_000_000_000,
        )
        assert gap is not None
        assert gap["type"] == "gap"
        assert gap["reason"] == "restart_gap"
        assert gap["gap_start_ts"] == 1000_000_000_000_000_000
        assert gap["gap_end_ts"] == 1000_000_005_000_000_000

    def test_gap_when_no_last_key_for_stream(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        gap = fm.check_failover_gap(
            stream_key=("binance", "btcusdt", "trades"),
            first_backup_key=1, first_backup_received_at=1000_000_000_000_000_000,
        )
        assert gap is None


class TestFailoverCleanup:
    @patch("src.writer.failover.KafkaConsumer")
    def test_cleanup_closes_active_backup_consumer(self, MockConsumer):
        mock_instance = MagicMock()
        MockConsumer.return_value = mock_instance
        mock_md = MagicMock()
        mock_md.topics = {}
        mock_instance.list_topics.return_value = mock_md
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.reset_silence_timer()
        fm.activate()
        fm.cleanup()
        assert fm._backup_consumer is None
        mock_instance.close.assert_called()

    def test_cleanup_when_no_consumer_is_noop(self):
        fm = FailoverManager(brokers=["localhost:9092"], primary_topics=["binance.trades"])
        fm.cleanup()



class TestGapFilterConfig:
    def test_default_grace_period_is_10s(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 10.0

    def test_grace_period_zero_is_allowed(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
writer:
  gap_filter:
    grace_period_seconds: 0
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 0.0

    def test_grace_period_custom_value(self, tmp_path: Path):
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
writer:
  gap_filter:
    grace_period_seconds: 25
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        cfg = load_config(p, env_overrides={})
        assert cfg.writer.gap_filter.grace_period_seconds == 25.0

    def test_negative_grace_period_rejected(self, tmp_path: Path):
        from src.common.config import ConfigValidationError
        cfg_yaml = """
database:
  url: "postgresql://u:p@localhost/db"
exchanges:
  binance:
    symbols: ["btcusdt"]
redpanda:
  brokers: ["localhost:9092"]
writer:
  gap_filter:
    grace_period_seconds: -1
"""
        p = tmp_path / "c.yaml"
        p.write_text(cfg_yaml)
        with pytest.raises(ConfigValidationError):
            load_config(p, env_overrides={})


def _data_env(exchange="binance", symbol="btcusdt", stream="trades", received_at=1000, raw_text='{"a": 1}'):
    return {
        "type": "data",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": received_at,
        "raw_text": raw_text,
    }


class TestCoverageFilterInit:
    def test_enabled_when_grace_period_positive(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.enabled is True

    def test_disabled_when_grace_period_zero(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        assert cf.enabled is False

    def test_pending_starts_empty(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.pending_size == 0

    def test_last_received_starts_zero(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0

    def test_max_received_starts_zero(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        assert cf.max_received(("binance", "btcusdt", "trades")) == 0


class TestCoverageFilterHandleData:
    def test_data_updates_last_received_for_source(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 5000

    def test_data_does_not_update_other_source(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("backup", ("binance", "btcusdt", "trades")) == 0

    def test_data_updates_max_received(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        cf.handle_data("backup", _data_env(received_at=7000))
        assert cf.max_received(("binance", "btcusdt", "trades")) == 7000

    def test_data_does_not_regress_last_received(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        cf.handle_data("primary", _data_env(received_at=3000))  # out-of-order
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 5000

    def test_data_ignored_when_disabled(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        cf.handle_data("primary", _data_env(received_at=5000))
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0

    def test_data_without_received_at_is_ignored(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        env = _data_env()
        del env["received_at"]
        cf.handle_data("primary", env)
        assert cf.last_received("primary", ("binance", "btcusdt", "trades")) == 0


def _gap_env(exchange="binance", symbol="btcusdt", stream="trades",
             gap_start=1000, gap_end=2000, reason="ws_disconnect"):
    return {
        "type": "gap",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "gap_start_ts": gap_start,
        "gap_end_ts": gap_end,
        "reason": reason,
    }


class TestCoverageFilterHandleGap:
    def test_primary_gap_dropped_when_backup_covers(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("backup", _data_env(received_at=3000))
        handled = cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert handled is True
        assert cf.pending_size == 0

    def test_backup_gap_dropped_when_primary_covers(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=3000))
        handled = cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        assert handled is True
        assert cf.pending_size == 0

    def test_primary_gap_dropped_when_backup_equal_to_gap_end(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("backup", _data_env(received_at=2000))
        handled = cf.handle_gap("primary", _gap_env(gap_end=2000))
        assert handled is True

    def test_gap_handled_false_when_disabled(self):
        cf = CoverageFilter(grace_period_seconds=0.0)
        handled = cf.handle_gap("primary", _gap_env())
        assert handled is False

    def test_same_source_coverage_does_not_count(self):
        # primary data cannot cover primary's own gap — that would be circular
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_data("primary", _data_env(received_at=3000))
        handled = cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        # Parked (not dropped immediately) because other_source=backup has no data yet.
        assert handled is True
        assert cf.pending_size == 1


class TestCoverageFilterPending:
    def test_gap_parked_when_not_yet_covered(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 1

    def test_stacked_gaps_coalesce_into_one_entry(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=3000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=4000))
        assert cf.pending_size == 1

    def test_coalesced_entry_has_latest_gap_end(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=5000))
        # Peek via internal state — acceptable in unit test
        key = ("primary", ("binance", "btcusdt", "trades"), 1000)
        assert cf._pending[key][0]["gap_end_ts"] == 5000

    def test_coalesce_does_not_reset_first_seen(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        key = ("primary", ("binance", "btcusdt", "trades"), 1000)
        original_first_seen = cf._pending[key][1]
        time.sleep(0.01)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=3000))
        assert cf._pending[key][1] == original_first_seen

    def test_different_gap_start_creates_separate_entries(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("primary", _gap_env(gap_start=5000, gap_end=6000))
        assert cf.pending_size == 2

    def test_gap_from_different_source_creates_separate_entry(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 2

    def test_data_sweep_drops_now_covered_pending_primary_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        assert cf.pending_size == 1
        cf.handle_data("backup", _data_env(received_at=3000))
        assert cf.pending_size == 0

    def test_data_sweep_drops_now_covered_pending_backup_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("backup", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_data("primary", _data_env(received_at=3000))
        assert cf.pending_size == 0

    def test_data_sweep_does_not_drop_uncovered_pending(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=5000))
        cf.handle_data("backup", _data_env(received_at=2000))  # not past gap_end
        assert cf.pending_size == 1

    def test_same_source_data_does_not_cover_own_gap(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(gap_start=1000, gap_end=2000))
        cf.handle_data("primary", _data_env(received_at=3000))  # primary's own data
        assert cf.pending_size == 1

    def test_data_for_different_stream_does_not_drop(self):
        cf = CoverageFilter(grace_period_seconds=10.0)
        cf.handle_gap("primary", _gap_env(stream="trades", gap_end=2000))
        cf.handle_data("backup", _data_env(stream="depth", received_at=3000))
        assert cf.pending_size == 1
