"""Tests for failover Prometheus metrics existence and types."""
from prometheus_client import Counter, Gauge, Histogram

from src.writer import metrics as m


def test_failover_active_gauge_exists():
    assert isinstance(m.failover_active, Gauge)

def test_failover_total_counter_exists():
    assert isinstance(m.failover_total, Counter)

def test_failover_duration_histogram_exists():
    assert isinstance(m.failover_duration_seconds, Histogram)

def test_failover_records_counter_exists():
    assert isinstance(m.failover_records_total, Counter)

def test_switchback_total_counter_exists():
    assert isinstance(m.switchback_total, Counter)

def test_old_backup_metrics_removed():
    assert not hasattr(m, "backup_recovery_attempts")
    assert not hasattr(m, "backup_recovery_success")
    assert not hasattr(m, "backup_recovery_partial")
    assert not hasattr(m, "backup_recovery_miss")
