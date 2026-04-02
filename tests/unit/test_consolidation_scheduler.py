from datetime import date, timedelta
from unittest.mock import patch

from src.cli.consolidation_scheduler import _get_target_date, _get_all_streams


def test_get_target_date_returns_yesterday():
    with patch("src.cli.consolidation_scheduler.date") as mock_date:
        mock_date.today.return_value = date(2026, 3, 29)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        result = _get_target_date()
        assert result == "2026-03-28"


def test_get_all_streams_from_config():
    streams = _get_all_streams()
    assert "trades" in streams
    assert "depth" in streams
