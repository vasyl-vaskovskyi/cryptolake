import pytest


class TestParseIntervalSeconds:
    def test_minutes(self):
        from src.collector.snapshot import parse_interval_seconds
        assert parse_interval_seconds("5m") == 300
        assert parse_interval_seconds("1m") == 60

    def test_seconds(self):
        from src.collector.snapshot import parse_interval_seconds
        assert parse_interval_seconds("30s") == 30

    def test_invalid(self):
        from src.collector.snapshot import parse_interval_seconds
        with pytest.raises(ValueError):
            parse_interval_seconds("5h")
