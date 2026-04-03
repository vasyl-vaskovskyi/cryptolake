import click
import pytest
from datetime import datetime, timezone
from src.cli.gaps import _resolve_date_args


def test_resolve_date_defaults_to_today():
    result = _resolve_date_args(None, None, None)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert result == (today, None, None)


def test_resolve_date_passes_through_explicit_date():
    result = _resolve_date_args("2026-04-01", None, None)
    assert result == ("2026-04-01", None, None)


def test_resolve_date_passes_through_range():
    result = _resolve_date_args(None, "2026-04-01", "2026-04-03")
    assert result == (None, "2026-04-01", "2026-04-03")


def test_resolve_date_rejects_range_over_3_days():
    with pytest.raises(click.UsageError, match="3 days"):
        _resolve_date_args(None, "2026-04-01", "2026-04-05")
