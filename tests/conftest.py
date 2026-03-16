from __future__ import annotations

from pathlib import Path
from typing import Any

import orjson
import pytest
import structlog

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Reset structlog between tests to avoid cached logger issues with capsys."""
    yield
    structlog.reset_defaults()


@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def load_fixture():
    def _load(name: str) -> dict[str, Any]:
        path = FIXTURES_DIR / name
        return orjson.loads(path.read_bytes())

    return _load
