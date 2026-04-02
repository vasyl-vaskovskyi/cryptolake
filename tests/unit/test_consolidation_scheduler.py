from datetime import datetime, timezone
from unittest.mock import patch

from src.cli.consolidation_scheduler import _get_target_date, _get_all_streams


def test_get_target_date_returns_yesterday():
    fake_now = datetime(2026, 3, 29, 3, 0, 0, tzinfo=timezone.utc)
    with patch("src.cli.consolidation_scheduler.datetime") as mock_dt:
        mock_dt.now.return_value = fake_now
        mock_dt.side_effect = lambda *args, **kw: datetime(*args, **kw)
        result = _get_target_date()
        assert result == "2026-03-28"


def test_get_all_streams_from_config():
    streams = _get_all_streams()
    assert "trades" in streams
    assert "depth" in streams
