# CryptoLake Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a raw-first crypto market data collection system that captures high-granularity trading data from Binance USD-M Futures via WebSocket/REST, buffers through Redpanda, and writes compressed archives with full observability.

**Architecture:** Bottom-up build. Common layer first (config, envelope, logging), then Binance exchange adapter, then collector (WS connections, stream handlers, gap detection, Redpanda producer), then writer (consumer, compressor, file rotation, PostgreSQL state), then verification CLI, then infrastructure/observability. Each phase is independently testable.

**Tech Stack:** Python 3.12, uv, uvloop, orjson, websockets, aiohttp, confluent-kafka-python, zstandard, psycopg, pydantic v2, prometheus-client, structlog, pytest, testcontainers-python. Infrastructure: Redpanda, PostgreSQL 16, Prometheus, Grafana, Alertmanager.

**Development Methodology:** Test-Driven Development (TDD) throughout. For every functional component: (1) write a failing test, (2) verify it fails, (3) write minimal implementation to pass, (4) verify it passes, (5) refactor if needed, (6) commit. Integration and chaos tests validate cross-component behavior.

**Spec Document:** `docs/2026-03-13-cryptolake-design.md`

---

## File Structure

```
cryptolake/
├── pyproject.toml
├── uv.lock
├── .gitignore
├── .env.example
├── docker-compose.yml
├── docker-compose.test.yml
├── Dockerfile.collector
├── Dockerfile.writer
├── config/
│   ├── config.yaml
│   └── config.example.yaml
├── src/
│   ├── __init__.py
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── connection.py
│   │   ├── streams/
│   │   │   ├── __init__.py
│   │   │   ├── base.py
│   │   │   ├── trades.py
│   │   │   ├── depth.py
│   │   │   ├── bookticker.py
│   │   │   ├── funding_rate.py
│   │   │   ├── liquidations.py
│   │   │   └── open_interest.py
│   │   ├── snapshot.py
│   │   ├── gap_detector.py
│   │   ├── producer.py
│   │   └── metrics.py
│   ├── writer/
│   │   ├── __init__.py
│   │   ├── main.py
│   │   ├── consumer.py
│   │   ├── buffer_manager.py
│   │   ├── compressor.py
│   │   ├── file_rotator.py
│   │   ├── state_manager.py
│   │   └── metrics.py
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── envelope.py
│   │   └── logging.py
│   ├── exchanges/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   └── binance.py
│   └── cli/
│       ├── __init__.py
│       └── verify.py
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_config.py
│   │   ├── test_envelope.py
│   │   ├── test_binance_adapter.py
│   │   ├── test_gap_detector.py
│   │   ├── test_compressor.py
│   │   ├── test_file_rotator.py
│   │   ├── test_buffer_manager.py
│   │   ├── test_state_manager.py
│   │   └── test_verify.py
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── test_binance_ws.py
│   │   ├── test_redpanda_roundtrip.py
│   │   └── test_writer_rotation.py
│   ├── chaos/
│   │   ├── kill_writer.sh
│   │   ├── kill_ws_connection.sh
│   │   ├── fill_disk.sh
│   │   ├── depth_reconnect_inflight.sh
│   │   ├── buffer_overflow_recovery.sh
│   │   └── writer_crash_before_commit.sh
│   └── fixtures/
│       ├── binance_aggtrade.json
│       ├── binance_depth_diff.json
│       ├── binance_depth_snapshot.json
│       ├── binance_bookticker.json
│       ├── binance_markprice.json
│       ├── binance_forceorder.json
│       ├── binance_openinterest.json
│       ├── binance_combined_frame.json
│       └── config_valid.yaml
├── infra/
│   ├── prometheus/
│   │   ├── prometheus.yml
│   │   └── alert_rules.yml
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources.yml
│   │   │   └── dashboards.yml
│   │   └── dashboards/
│   │       └── cryptolake.json
│   └── alertmanager/
│       └── alertmanager.yml
└── docs/
    ├── 2026-03-13-cryptolake-design.md
    └── plans/
        └── 2026-03-15-cryptolake-implementation.md
```

---

## Chunk 1: Project Bootstrap & Common Layer

### Task 1.1: Repository Scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/__init__.py`, `src/common/__init__.py`, `src/collector/__init__.py`, `src/collector/streams/__init__.py`, `src/writer/__init__.py`, `src/exchanges/__init__.py`, `src/cli/__init__.py`
- Create: `tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py`, `tests/conftest.py`
- Modify: `.gitignore`

- [ ] **Step 1: Create `pyproject.toml` with all dependencies**

```toml
[project]
name = "cryptolake"
version = "0.1.0"
description = "Raw-first crypto market data collection system"
requires-python = ">=3.12"
dependencies = [
    "uvloop>=0.19.0",
    "orjson>=3.10.0",
    "websockets>=13.0",
    "aiohttp>=3.9.0",
    "confluent-kafka>=2.5.0",
    "zstandard>=0.23.0",
    "psycopg[binary]>=3.2.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "prometheus-client>=0.21.0",
    "structlog>=24.4.0",
    "pyyaml>=6.0.0",
    "click>=8.1.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3.0",
    "pytest-asyncio>=0.24.0",
    "pytest-cov>=5.0.0",
    "testcontainers[kafka,postgres]>=4.7.0",
    "ruff>=0.5.0",
]

[project.scripts]
cryptolake = "src.cli.verify:cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
markers = [
    "integration: marks tests requiring external services",
    "chaos: marks chaos/failure-mode tests",
]

[tool.ruff]
target-version = "py312"
line-length = 120
```

- [ ] **Step 2: Update `.gitignore`**

Append to existing `.gitignore`:
```
__pycache__/
*.pyc
.venv/
*.egg-info/
dist/
.env
.coverage
htmlcov/
.pytest_cache/
/data/
```

- [ ] **Step 3: Create all `__init__.py` files and directory structure**

Create empty `__init__.py` in: `src/`, `src/common/`, `src/collector/`, `src/collector/streams/`, `src/writer/`, `src/exchanges/`, `src/cli/`, `tests/`, `tests/unit/`, `tests/integration/`.

- [ ] **Step 4: Create `tests/conftest.py` with shared fixtures**

```python
import pathlib
import pytest
import orjson

FIXTURES_DIR = pathlib.Path(__file__).parent / "fixtures"


@pytest.fixture
def fixtures_dir():
    return FIXTURES_DIR


@pytest.fixture
def load_fixture():
    def _load(name: str) -> dict:
        path = FIXTURES_DIR / name
        return orjson.loads(path.read_bytes())
    return _load
```

- [ ] **Step 5: Initialize uv and lock dependencies**

Run: `cd /Users/vasyl.vaskovskyi/data/cryptolake && uv sync --all-extras`
Expected: `uv.lock` created, `.venv/` created, all dependencies installed.

- [ ] **Step 6: Verify pytest runs (no tests yet)**

Run: `uv run pytest --co -q`
Expected: "no tests ran" or similar empty collection output, no import errors.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .gitignore src/ tests/conftest.py tests/__init__.py tests/unit/__init__.py tests/integration/__init__.py
git commit -m "feat: scaffold project structure with dependencies"
```

---

### Task 1.2: Test Fixtures (Binance Samples)

**Files:**
- Create: `tests/fixtures/binance_aggtrade.json`
- Create: `tests/fixtures/binance_depth_diff.json`
- Create: `tests/fixtures/binance_depth_snapshot.json`
- Create: `tests/fixtures/binance_bookticker.json`
- Create: `tests/fixtures/binance_markprice.json`
- Create: `tests/fixtures/binance_forceorder.json`
- Create: `tests/fixtures/binance_openinterest.json`
- Create: `tests/fixtures/binance_combined_frame.json`
- Create: `tests/fixtures/config_valid.yaml`

- [ ] **Step 1: Create fixture files with real Binance payload shapes**

`tests/fixtures/binance_aggtrade.json`:
```json
{
  "e": "aggTrade",
  "E": 1741689600120,
  "a": 123456,
  "s": "BTCUSDT",
  "p": "65432.10",
  "q": "0.001",
  "f": 200001,
  "l": 200005,
  "T": 1741689600119,
  "m": true
}
```

`tests/fixtures/binance_depth_diff.json`:
```json
{
  "e": "depthUpdate",
  "E": 1741689600200,
  "T": 1741689600195,
  "s": "BTCUSDT",
  "U": 1000,
  "u": 1005,
  "pu": 999,
  "b": [["65430.00", "1.500"], ["65429.00", "0.000"]],
  "a": [["65435.00", "2.300"], ["65436.00", "0.500"]]
}
```

`tests/fixtures/binance_depth_snapshot.json`:
```json
{
  "lastUpdateId": 1005,
  "E": 1741689600300,
  "T": 1741689600295,
  "bids": [["65430.00", "1.500"], ["65429.00", "3.200"]],
  "asks": [["65435.00", "2.300"], ["65436.00", "0.500"]]
}
```

`tests/fixtures/binance_bookticker.json`:
```json
{
  "e": "bookTicker",
  "u": 1006,
  "E": 1741689600400,
  "T": 1741689600395,
  "s": "BTCUSDT",
  "b": "65430.00",
  "B": "1.500",
  "a": "65435.00",
  "A": "2.300"
}
```

`tests/fixtures/binance_markprice.json`:
```json
{
  "e": "markPriceUpdate",
  "E": 1741689600500,
  "s": "BTCUSDT",
  "p": "65433.50",
  "i": "65432.00",
  "P": "65434.00",
  "r": "0.00010000",
  "T": 1741689600000
}
```

`tests/fixtures/binance_forceorder.json`:
```json
{
  "e": "forceOrder",
  "E": 1741689600600,
  "o": {
    "s": "BTCUSDT",
    "S": "SELL",
    "o": "LIMIT",
    "f": "IOC",
    "q": "0.100",
    "p": "65000.00",
    "ap": "65010.00",
    "X": "FILLED",
    "l": "0.100",
    "z": "0.100",
    "T": 1741689600590
  }
}
```

`tests/fixtures/binance_openinterest.json`:
```json
{
  "openInterest": "12345.678",
  "symbol": "BTCUSDT",
  "time": 1741689600700
}
```

`tests/fixtures/binance_combined_frame.json`:
```json
{
  "stream": "btcusdt@aggTrade",
  "data": {
    "e": "aggTrade",
    "E": 1741689600120,
    "a": 123456,
    "s": "BTCUSDT",
    "p": "65432.10",
    "q": "0.001",
    "f": 200001,
    "l": 200005,
    "T": 1741689600119,
    "m": true
  }
}
```

`tests/fixtures/config_valid.yaml`:
```yaml
database:
  url: "postgresql+psycopg://cryptolake:testpass@localhost:5432/cryptolake"

exchanges:
  binance:
    enabled: true
    market: "usdm_futures"
    ws_base: "wss://fstream.binance.com"
    rest_base: "https://fapi.binance.com"
    symbols:
      - btcusdt
      - ethusdt
      - solusdt
    streams:
      trades: true
      depth: true
      bookticker: true
      funding_rate: true
      liquidations: true
      open_interest: true
    depth:
      update_speed: "100ms"
      snapshot_interval: "5m"
      snapshot_overrides:
        btcusdt: "1m"
    open_interest:
      poll_interval: "5m"
    collector_id: "binance-collector-01"

redpanda:
  brokers:
    - "redpanda:9092"
  retention_hours: 48

writer:
  base_dir: "/data"
  rotation: "hourly"
  compression: "zstd"
  compression_level: 3
  checksum: "sha256"
  flush_messages: 10000
  flush_interval_seconds: 30

monitoring:
  prometheus_port: 8000
  alerting:
    webhook_url: ""
    rules:
      gap_detected: "critical"
      connection_lost: "critical"
      writer_lag_seconds: 30
      disk_usage_pct: 85
```

- [ ] **Step 2: Commit fixtures**

```bash
git add tests/fixtures/
git commit -m "feat: add Binance sample fixtures and test config"
```

---

### Task 1.3: Configuration System (TDD)

**Files:**
- Create: `src/common/config.py`
- Create: `tests/unit/test_config.py`

- [ ] **Step 1: Write failing tests for config parsing**

`tests/unit/test_config.py`:
```python
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestConfigLoading:
    def test_load_valid_config(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.exchanges.binance.enabled is True
        assert config.exchanges.binance.symbols == ["btcusdt", "ethusdt", "solusdt"]
        assert config.redpanda.retention_hours == 48

    def test_config_symbols_are_lowercase(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        for symbol in config.exchanges.binance.symbols:
            assert symbol == symbol.lower()

    def test_config_all_streams_enabled(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        streams = config.exchanges.binance.streams
        assert streams.trades is True
        assert streams.depth is True
        assert streams.bookticker is True
        assert streams.funding_rate is True
        assert streams.liquidations is True
        assert streams.open_interest is True

    def test_config_depth_snapshot_override(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.exchanges.binance.depth.snapshot_overrides["btcusdt"] == "1m"
        assert config.exchanges.binance.depth.snapshot_interval == "5m"

    def test_config_writer_defaults(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.writer.compression == "zstd"
        assert config.writer.compression_level == 3
        assert config.writer.rotation == "hourly"

    def test_config_retention_minimum_rejected(self, tmp_path):
        """retention_hours < 12 must be rejected"""
        from src.common.config import load_config, ConfigValidationError

        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text("""
database:
  url: "postgresql+psycopg://x:x@localhost:5432/x"
exchanges:
  binance:
    enabled: true
    market: usdm_futures
    ws_base: "wss://fstream.binance.com"
    rest_base: "https://fapi.binance.com"
    symbols: [btcusdt]
    streams:
      trades: true
      depth: false
      bookticker: false
      funding_rate: false
      liquidations: false
      open_interest: false
    depth:
      update_speed: "100ms"
      snapshot_interval: "5m"
    open_interest:
      poll_interval: "5m"
    collector_id: "test"
redpanda:
  brokers: ["localhost:9092"]
  retention_hours: 6
writer:
  base_dir: "/data"
  rotation: hourly
  compression: zstd
  compression_level: 3
  checksum: sha256
  flush_messages: 10000
  flush_interval_seconds: 30
monitoring:
  prometheus_port: 8000
  alerting:
    webhook_url: ""
    rules:
      gap_detected: critical
      connection_lost: critical
      writer_lag_seconds: 30
      disk_usage_pct: 85
""")
        with pytest.raises(ConfigValidationError, match="retention_hours"):
            load_config(bad_config)

    def test_config_env_override(self, monkeypatch, tmp_path):
        """Environment variables override YAML values"""
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml",
                             env_overrides={"REDPANDA__BROKERS": "broker1:9092,broker2:9092"})
        assert config.redpanda.brokers == ["broker1:9092", "broker2:9092"]

    def test_config_missing_file_raises(self):
        from src.common.config import load_config

        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_disabled_stream_no_depth_snapshot(self):
        """When depth is disabled, depth_snapshot is implicitly disabled too"""
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        # depth=true means both depth diffs and snapshots enabled
        enabled = config.exchanges.binance.get_enabled_streams()
        assert "depth" in enabled
        assert "depth_snapshot" in enabled

    def test_enabled_stream_list(self):
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        enabled = config.exchanges.binance.get_enabled_streams()
        assert enabled == ["trades", "depth", "depth_snapshot", "bookticker",
                           "funding_rate", "liquidations", "open_interest"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.config'`

- [ ] **Step 3: Implement configuration system**

`src/common/config.py`:
```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pydantic
import yaml


class ConfigValidationError(Exception):
    pass


class StreamsConfig(pydantic.BaseModel):
    trades: bool = True
    depth: bool = True
    bookticker: bool = True
    funding_rate: bool = True
    liquidations: bool = True
    open_interest: bool = True


class DepthConfig(pydantic.BaseModel):
    update_speed: str = "100ms"
    snapshot_interval: str = "5m"
    snapshot_overrides: dict[str, str] = pydantic.Field(default_factory=dict)


class OpenInterestConfig(pydantic.BaseModel):
    poll_interval: str = "5m"


class AlertRulesConfig(pydantic.BaseModel):
    gap_detected: str = "critical"
    connection_lost: str = "critical"
    writer_lag_seconds: int = 30
    disk_usage_pct: int = 85


class AlertingConfig(pydantic.BaseModel):
    webhook_url: str = ""
    rules: AlertRulesConfig = pydantic.Field(default_factory=AlertRulesConfig)


class MonitoringConfig(pydantic.BaseModel):
    prometheus_port: int = 8000
    alerting: AlertingConfig = pydantic.Field(default_factory=AlertingConfig)


class BinanceExchangeConfig(pydantic.BaseModel):
    enabled: bool = True
    market: str = "usdm_futures"
    ws_base: str = "wss://fstream.binance.com"
    rest_base: str = "https://fapi.binance.com"
    symbols: list[str]
    streams: StreamsConfig = pydantic.Field(default_factory=StreamsConfig)
    writer_streams_override: list[str] | None = None
    depth: DepthConfig = pydantic.Field(default_factory=DepthConfig)
    open_interest: OpenInterestConfig = pydantic.Field(default_factory=OpenInterestConfig)
    collector_id: str = "binance-collector-01"

    @pydantic.field_validator("symbols", mode="before")
    @classmethod
    def lowercase_symbols(cls, v: list[str]) -> list[str]:
        return [s.lower() for s in v]

    @pydantic.field_validator("writer_streams_override", mode="before")
    @classmethod
    def auto_include_depth_snapshot(cls, v: list[str] | None) -> list[str] | None:
        """If writer_streams_override includes 'depth', auto-include 'depth_snapshot'."""
        if v and "depth" in v and "depth_snapshot" not in v:
            v = list(v) + ["depth_snapshot"]
        return v

    def get_enabled_streams(self) -> list[str]:
        """Return ordered list of enabled stream names.
        depth=true enables both 'depth' and 'depth_snapshot'."""
        result: list[str] = []
        if self.streams.trades:
            result.append("trades")
        if self.streams.depth:
            result.append("depth")
            result.append("depth_snapshot")
        if self.streams.bookticker:
            result.append("bookticker")
        if self.streams.funding_rate:
            result.append("funding_rate")
        if self.streams.liquidations:
            result.append("liquidations")
        if self.streams.open_interest:
            result.append("open_interest")
        return result


class ExchangesConfig(pydantic.BaseModel):
    binance: BinanceExchangeConfig


class RedpandaConfig(pydantic.BaseModel):
    brokers: list[str]
    retention_hours: int = 48


class DatabaseConfig(pydantic.BaseModel):
    url: str


class WriterConfig(pydantic.BaseModel):
    base_dir: str = "/data"
    rotation: str = "hourly"
    compression: str = "zstd"
    compression_level: int = 3
    checksum: str = "sha256"
    flush_messages: int = 10000
    flush_interval_seconds: int = 30


class CryptoLakeConfig(pydantic.BaseModel):
    database: DatabaseConfig
    exchanges: ExchangesConfig
    redpanda: RedpandaConfig
    writer: WriterConfig = pydantic.Field(default_factory=WriterConfig)
    monitoring: MonitoringConfig = pydantic.Field(default_factory=MonitoringConfig)


def _apply_env_overrides(data: dict[str, Any], overrides: dict[str, str]) -> dict[str, Any]:
    """Apply EXCHANGES__BINANCE__SYMBOLS=x,y style overrides."""
    for key, value in overrides.items():
        parts = key.lower().split("__")
        target = data
        for part in parts[:-1]:
            if part not in target:
                target[part] = {}
            target = target[part]
        leaf = parts[-1]
        # Handle comma-separated lists
        if "," in value:
            target[leaf] = [v.strip() for v in value.split(",")]
        else:
            target[leaf] = value
    return data


def load_config(
    path: Path,
    env_overrides: dict[str, str] | None = None,
) -> CryptoLakeConfig:
    """Load and validate configuration from YAML file.

    Args:
        path: Path to YAML config file.
        env_overrides: Dict of KEY__NESTED=value overrides (for testing).
                       In production, reads from os.environ automatically.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        data = yaml.safe_load(f)

    # Collect env overrides
    overrides = env_overrides or {}
    if not env_overrides:
        # Auto-detect from environment
        for k, v in os.environ.items():
            if "__" in k and k.split("__")[0].lower() in (
                "database", "exchanges", "redpanda", "writer", "monitoring"
            ):
                overrides[k] = v

    if overrides:
        data = _apply_env_overrides(data, overrides)

    try:
        config = CryptoLakeConfig.model_validate(data)
    except pydantic.ValidationError as e:
        raise ConfigValidationError(str(e)) from e

    # Custom validations
    if config.redpanda.retention_hours < 12:
        raise ConfigValidationError(
            f"retention_hours must be >= 12, got {config.redpanda.retention_hours}"
        )

    return config
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_config.py -v`
Expected: All 10 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/common/config.py tests/unit/test_config.py
git commit -m "feat: configuration system with Pydantic validation and env overrides"
```

---

### Task 1.4: Message Envelope (TDD)

**Files:**
- Create: `src/common/envelope.py`
- Create: `tests/unit/test_envelope.py`

- [ ] **Step 1: Write failing tests for envelope**

`tests/unit/test_envelope.py`:
```python
import hashlib
import time
import orjson
import pytest


class TestEnvelopeCreation:
    def test_create_data_envelope(self):
        from src.common.envelope import create_data_envelope

        raw_text = '{"e":"aggTrade","E":1741689600120}'
        env = create_data_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            raw_text=raw_text,
            exchange_ts=1741689600120,
            collector_session_id="test_2026-01-01T00:00:00Z",
            session_seq=1,
        )
        assert env["v"] == 1
        assert env["type"] == "data"
        assert env["exchange"] == "binance"
        assert env["symbol"] == "btcusdt"
        assert env["stream"] == "trades"
        assert env["raw_text"] == raw_text
        assert env["raw_sha256"] == hashlib.sha256(raw_text.encode()).hexdigest()
        assert env["collector_session_id"] == "test_2026-01-01T00:00:00Z"
        assert env["session_seq"] == 1
        assert isinstance(env["received_at"], int)
        assert env["exchange_ts"] == 1741689600120

    def test_create_gap_envelope(self):
        from src.common.envelope import create_gap_envelope

        env = create_gap_envelope(
            exchange="binance",
            symbol="btcusdt",
            stream="depth",
            collector_session_id="test_2026-01-01T00:00:00Z",
            session_seq=100,
            gap_start_ts=1000000000000000000,
            gap_end_ts=1000000005000000000,
            reason="ws_disconnect",
            detail="WebSocket closed after 2h, reconnected in 1.2s",
        )
        assert env["v"] == 1
        assert env["type"] == "gap"
        assert env["exchange"] == "binance"
        assert env["symbol"] == "btcusdt"
        assert env["stream"] == "depth"
        assert env["gap_start_ts"] == 1000000000000000000
        assert env["gap_end_ts"] == 1000000005000000000
        assert env["reason"] == "ws_disconnect"
        assert env["detail"] == "WebSocket closed after 2h, reconnected in 1.2s"
        assert "raw_text" not in env
        assert "raw_sha256" not in env
        assert "exchange_ts" not in env

    def test_data_envelope_raw_sha256_integrity(self):
        from src.common.envelope import create_data_envelope

        raw = '{"key": "value", "num": 0.00100000}'
        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text=raw, exchange_ts=100,
            collector_session_id="s", session_seq=0,
        )
        expected_hash = hashlib.sha256(raw.encode()).hexdigest()
        assert env["raw_sha256"] == expected_hash

    def test_envelope_received_at_is_nanoseconds(self):
        from src.common.envelope import create_data_envelope

        before = time.time_ns()
        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text="{}", exchange_ts=100,
            collector_session_id="s", session_seq=0,
        )
        after = time.time_ns()
        assert before <= env["received_at"] <= after

    def test_gap_reason_values(self):
        from src.common.envelope import VALID_GAP_REASONS, create_gap_envelope

        assert "ws_disconnect" in VALID_GAP_REASONS
        assert "pu_chain_break" in VALID_GAP_REASONS
        assert "session_seq_skip" in VALID_GAP_REASONS
        assert "buffer_overflow" in VALID_GAP_REASONS
        assert "snapshot_poll_miss" in VALID_GAP_REASONS

    def test_gap_invalid_reason_raises(self):
        from src.common.envelope import create_gap_envelope

        with pytest.raises(ValueError, match="reason"):
            create_gap_envelope(
                exchange="binance", symbol="btcusdt", stream="trades",
                collector_session_id="s", session_seq=0,
                gap_start_ts=0, gap_end_ts=1,
                reason="invalid_reason", detail="test",
            )


class TestEnvelopeSerialization:
    def test_serialize_data_envelope_to_bytes(self):
        from src.common.envelope import create_data_envelope, serialize_envelope

        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text='{"test": 1}', exchange_ts=100,
            collector_session_id="s", session_seq=0,
        )
        data = serialize_envelope(env)
        assert isinstance(data, bytes)
        parsed = orjson.loads(data)
        assert parsed["type"] == "data"
        assert parsed["raw_text"] == '{"test": 1}'

    def test_serialize_gap_envelope_to_bytes(self):
        from src.common.envelope import create_gap_envelope, serialize_envelope

        env = create_gap_envelope(
            exchange="binance", symbol="btcusdt", stream="depth",
            collector_session_id="s", session_seq=0,
            gap_start_ts=0, gap_end_ts=1,
            reason="ws_disconnect", detail="test",
        )
        data = serialize_envelope(env)
        parsed = orjson.loads(data)
        assert parsed["type"] == "gap"
        assert "raw_text" not in parsed

    def test_add_broker_coordinates(self):
        from src.common.envelope import create_data_envelope, add_broker_coordinates

        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text="{}", exchange_ts=100,
            collector_session_id="s", session_seq=0,
        )
        stamped = add_broker_coordinates(env, topic="binance.trades", partition=0, offset=42)
        assert stamped["_topic"] == "binance.trades"
        assert stamped["_partition"] == 0
        assert stamped["_offset"] == 42
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_envelope.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.common.envelope'`

- [ ] **Step 3: Implement envelope module**

`src/common/envelope.py`:
```python
from __future__ import annotations

import hashlib
import time

import orjson

VALID_GAP_REASONS = frozenset({
    "ws_disconnect",
    "pu_chain_break",
    "session_seq_skip",
    "buffer_overflow",
    "snapshot_poll_miss",
})


def create_data_envelope(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    raw_text: str,
    exchange_ts: int,
    collector_session_id: str,
    session_seq: int,
    received_at: int | None = None,
) -> dict:
    if received_at is None:
        received_at = time.time_ns()
    return {
        "v": 1,
        "type": "data",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": received_at,
        "exchange_ts": exchange_ts,
        "collector_session_id": collector_session_id,
        "session_seq": session_seq,
        "raw_text": raw_text,
        "raw_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
    }


def create_gap_envelope(
    *,
    exchange: str,
    symbol: str,
    stream: str,
    collector_session_id: str,
    session_seq: int,
    gap_start_ts: int,
    gap_end_ts: int,
    reason: str,
    detail: str,
    received_at: int | None = None,
) -> dict:
    if reason not in VALID_GAP_REASONS:
        raise ValueError(
            f"Invalid gap reason '{reason}'. Must be one of: {sorted(VALID_GAP_REASONS)}"
        )
    if received_at is None:
        received_at = time.time_ns()
    return {
        "v": 1,
        "type": "gap",
        "exchange": exchange,
        "symbol": symbol,
        "stream": stream,
        "received_at": received_at,
        "collector_session_id": collector_session_id,
        "session_seq": session_seq,
        "gap_start_ts": gap_start_ts,
        "gap_end_ts": gap_end_ts,
        "reason": reason,
        "detail": detail,
    }


def serialize_envelope(envelope: dict) -> bytes:
    return orjson.dumps(envelope)


def deserialize_envelope(data: bytes) -> dict:
    return orjson.loads(data)


def add_broker_coordinates(
    envelope: dict,
    *,
    topic: str,
    partition: int,
    offset: int,
) -> dict:
    envelope["_topic"] = topic
    envelope["_partition"] = partition
    envelope["_offset"] = offset
    return envelope
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_envelope.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/common/envelope.py tests/unit/test_envelope.py
git commit -m "feat: message envelope with data/gap records, SHA-256, and broker coordinates"
```

---

### Task 1.5: Structured Logging

**Files:**
- Create: `src/common/logging.py`

- [ ] **Step 1: Implement structured logging setup**

`src/common/logging.py`:
```python
from __future__ import annotations

import logging
import sys

import structlog


def setup_logging(level: str = "INFO") -> None:
    """Configure structlog for JSON output to stdout."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(serializer=_json_dumps),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def _json_dumps(obj: dict, **kw) -> str:
    import orjson
    return orjson.dumps(obj).decode()


def get_logger(**initial_bindings) -> structlog.stdlib.BoundLogger:
    """Get a logger with initial context bindings (e.g., collector_session_id, symbol, stream)."""
    return structlog.get_logger(**initial_bindings)
```

- [ ] **Step 2: Write unit test for logging**

Add to `tests/unit/test_config.py` or create `tests/unit/test_logging.py`:
```python
import json
import io
import sys

class TestStructuredLogging:
    def test_json_output(self, capsys):
        from src.common.logging import setup_logging, get_logger
        setup_logging()
        log = get_logger(component="test", symbol="btcusdt")
        log.info("test_event", session_seq=42)
        output = capsys.readouterr().out
        parsed = json.loads(output.strip())
        assert parsed["event"] == "test_event"
        assert parsed["component"] == "test"
        assert parsed["symbol"] == "btcusdt"
        assert parsed["session_seq"] == 42
        assert "timestamp" in parsed
        assert parsed["level"] == "info"
```

- [ ] **Step 3: Run test**

Run: `uv run pytest tests/unit/test_logging.py -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/common/logging.py
git commit -m "feat: structured JSON logging with structlog and orjson"
```

---

### Manual Verification: Chunk 1

After completing all tasks in Chunk 1, verify the foundation works end-to-end:

1. **Run all unit tests:**
   ```bash
   uv run pytest tests/unit/ -v --tb=short
   ```
   **Check:** All tests pass. No import errors. Coverage covers config and envelope modules.

2. **Verify config loading manually:**
   ```bash
   uv run python -c "
   from src.common.config import load_config
   from pathlib import Path
   c = load_config(Path('tests/fixtures/config_valid.yaml'))
   print('Symbols:', c.exchanges.binance.symbols)
   print('Enabled streams:', c.exchanges.binance.get_enabled_streams())
   print('Writer base_dir:', c.writer.base_dir)
   print('Retention hours:', c.redpanda.retention_hours)
   "
   ```
   **Check:** Prints 3 symbols, 7 enabled streams, `/data` base dir, 48 retention hours.

3. **Verify envelope creation and integrity:**
   ```bash
   uv run python -c "
   from src.common.envelope import create_data_envelope, serialize_envelope, create_gap_envelope
   import orjson, hashlib
   env = create_data_envelope(exchange='binance', symbol='btcusdt', stream='trades',
       raw_text='{\"test\": 1}', exchange_ts=100, collector_session_id='s', session_seq=0)
   data = serialize_envelope(env)
   parsed = orjson.loads(data)
   print('Envelope type:', parsed['type'])
   print('SHA256 match:', parsed['raw_sha256'] == hashlib.sha256(parsed['raw_text'].encode()).hexdigest())
   print('received_at is nanoseconds:', len(str(parsed['received_at'])) >= 19)
   gap = create_gap_envelope(exchange='binance', symbol='btcusdt', stream='depth',
       collector_session_id='s', session_seq=1, gap_start_ts=0, gap_end_ts=1,
       reason='ws_disconnect', detail='test')
   print('Gap type:', gap['type'])
   print('Gap has no raw_text:', 'raw_text' not in gap)
   "
   ```
   **Check:** Envelope type is `data`, SHA256 matches, received_at has 19+ digits, gap type is `gap`, gap has no raw_text.

4. **Verify structured logging output:**
   ```bash
   uv run python -c "
   from src.common.logging import setup_logging, get_logger
   setup_logging()
   log = get_logger(collector_session_id='test_session', symbol='btcusdt', stream='trades')
   log.info('message_received', session_seq=42)
   "
   ```
   **Check:** Outputs a single JSON line with keys: `collector_session_id`, `symbol`, `stream`, `session_seq`, `event`, `level`, `timestamp`.

---

## Chunk 2: Binance Exchange Adapter

### Task 2.1: Base Exchange Adapter Interface

**Files:**
- Create: `src/exchanges/base.py`

- [ ] **Step 1: Define abstract base class for exchange adapters**

`src/exchanges/base.py`:
```python
from __future__ import annotations

from abc import ABC, abstractmethod


class ExchangeAdapter(ABC):
    """Base interface for exchange-specific logic.

    Each exchange adapter must implement URL building, stream routing,
    payload parsing, and raw_text extraction.
    """

    @abstractmethod
    def get_ws_urls(self, symbols: list[str], streams: list[str]) -> dict[str, str]:
        """Return dict of {socket_name: ws_url} for the given symbols and streams.
        May split across multiple sockets by traffic type."""

    @abstractmethod
    def route_stream(self, raw_frame: str) -> tuple[str, str, str]:
        """Parse the outer combined-stream frame and return (stream_type, symbol, raw_data_text).
        raw_data_text must be extracted via zero-copy string slicing."""

    @abstractmethod
    def extract_exchange_ts(self, stream: str, raw_text: str) -> int:
        """Extract exchange timestamp (milliseconds) from raw payload.
        Uses lightweight parsing via orjson."""

    @abstractmethod
    def build_snapshot_url(self, symbol: str, limit: int = 1000) -> str:
        """Build REST URL for depth snapshot."""

    @abstractmethod
    def build_open_interest_url(self, symbol: str) -> str:
        """Build REST URL for open interest."""

    @abstractmethod
    def parse_snapshot_last_update_id(self, raw_text: str) -> int:
        """Extract lastUpdateId from depth snapshot raw_text."""

    @abstractmethod
    def parse_depth_update_ids(self, raw_text: str) -> tuple[int, int, int]:
        """Extract (U, u, pu) from depth diff raw_text."""
```

- [ ] **Step 2: Commit**

```bash
git add src/exchanges/base.py
git commit -m "feat: abstract exchange adapter interface"
```

---

### Task 2.2: Binance Adapter — URL Building & Stream Routing (TDD)

**Files:**
- Create: `src/exchanges/binance.py`
- Create: `tests/unit/test_binance_adapter.py`

- [ ] **Step 1: Write failing tests for URL building**

`tests/unit/test_binance_adapter.py`:
```python
import pytest
from pathlib import Path

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestBinanceURLBuilding:
    def test_ws_urls_two_sockets(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        urls = adapter.get_ws_urls(
            symbols=["btcusdt", "ethusdt"],
            streams=["trades", "depth", "bookticker", "funding_rate", "liquidations"],
        )
        # Should produce two sockets: public and market
        assert "public" in urls
        assert "market" in urls

    def test_ws_public_socket_streams(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        urls = adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["depth", "bookticker"],
        )
        url = urls["public"]
        assert "btcusdt@depth@100ms" in url
        assert "btcusdt@bookTicker" in url
        assert url.startswith("wss://fstream.binance.com/stream?streams=")

    def test_ws_market_socket_streams(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        urls = adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["trades", "funding_rate", "liquidations"],
        )
        url = urls["market"]
        assert "btcusdt@aggTrade" in url
        assert "btcusdt@markPrice@1s" in url
        assert "btcusdt@forceOrder" in url

    def test_ws_no_public_streams_omits_socket(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        urls = adapter.get_ws_urls(
            symbols=["btcusdt"],
            streams=["trades"],  # only market streams
        )
        assert "public" not in urls
        assert "market" in urls

    def test_snapshot_url(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        url = adapter.build_snapshot_url("btcusdt", limit=1000)
        assert url == "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000"

    def test_open_interest_url(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        url = adapter.build_open_interest_url("btcusdt")
        assert url == "https://fapi.binance.com/fapi/v1/openInterest?symbol=BTCUSDT"


class TestBinanceStreamRouting:
    def test_route_aggtrade(self):
        from src.exchanges.binance import BinanceAdapter
        import orjson

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        frame = orjson.dumps({
            "stream": "btcusdt@aggTrade",
            "data": {"e": "aggTrade", "E": 100, "s": "BTCUSDT", "p": "1.0", "q": "1.0",
                     "a": 1, "f": 1, "l": 1, "T": 100, "m": True}
        }).decode()
        stream_type, symbol, raw_data = adapter.route_stream(frame)
        assert stream_type == "trades"
        assert symbol == "btcusdt"
        # raw_data should be valid JSON
        parsed = orjson.loads(raw_data)
        assert parsed["e"] == "aggTrade"

    def test_route_depth(self):
        from src.exchanges.binance import BinanceAdapter
        import orjson

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        frame = orjson.dumps({
            "stream": "btcusdt@depth@100ms",
            "data": {"e": "depthUpdate", "E": 100, "s": "BTCUSDT",
                     "U": 1, "u": 2, "pu": 0, "b": [], "a": []}
        }).decode()
        stream_type, symbol, raw_data = adapter.route_stream(frame)
        assert stream_type == "depth"
        assert symbol == "btcusdt"

    def test_route_bookticker(self):
        from src.exchanges.binance import BinanceAdapter
        import orjson

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        frame = orjson.dumps({
            "stream": "btcusdt@bookTicker",
            "data": {"e": "bookTicker", "s": "BTCUSDT", "b": "1.0", "a": "2.0",
                     "B": "1.0", "A": "1.0", "u": 1, "E": 100, "T": 100}
        }).decode()
        stream_type, symbol, raw_data = adapter.route_stream(frame)
        assert stream_type == "bookticker"
        assert symbol == "btcusdt"

    def test_route_markprice(self):
        from src.exchanges.binance import BinanceAdapter
        import orjson

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        frame = orjson.dumps({
            "stream": "btcusdt@markPrice@1s",
            "data": {"e": "markPriceUpdate", "E": 100, "s": "BTCUSDT",
                     "p": "1.0", "i": "1.0", "P": "1.0", "r": "0.0001", "T": 100}
        }).decode()
        stream_type, symbol, raw_data = adapter.route_stream(frame)
        assert stream_type == "funding_rate"
        assert symbol == "btcusdt"

    def test_route_forceorder(self):
        from src.exchanges.binance import BinanceAdapter
        import orjson

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        frame = orjson.dumps({
            "stream": "btcusdt@forceOrder",
            "data": {"e": "forceOrder", "E": 100, "o": {"s": "BTCUSDT", "S": "SELL",
                     "o": "LIMIT", "f": "IOC", "q": "1", "p": "1", "ap": "1",
                     "X": "FILLED", "l": "1", "z": "1", "T": 100}}
        }).decode()
        stream_type, symbol, raw_data = adapter.route_stream(frame)
        assert stream_type == "liquidations"
        assert symbol == "btcusdt"


class TestBinanceRawTextExtraction:
    def test_raw_text_preserves_original_bytes(self):
        """raw_text must be extracted via string slicing, not re-serialized.
        Key ordering, whitespace, and number formatting must be preserved."""
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        # Deliberately use non-canonical formatting to test fidelity
        inner = '{"e":"aggTrade","E":100,"p":"0.00100000","q":"1.000"}'
        frame = '{"stream":"btcusdt@aggTrade","data":' + inner + '}'
        _, _, raw_data = adapter.route_stream(frame)
        assert raw_data == inner

    def test_raw_text_unicode_escapes_preserved(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        inner = '{"e":"aggTrade","E":100,"note":"\\u0048ello"}'
        frame = '{"stream":"btcusdt@aggTrade","data":' + inner + '}'
        _, _, raw_data = adapter.route_stream(frame)
        assert raw_data == inner


class TestBinanceTimestampExtraction:
    def test_extract_ts_aggtrade(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"e":"aggTrade","E":1741689600120,"s":"BTCUSDT"}'
        ts = adapter.extract_exchange_ts("trades", raw)
        assert ts == 1741689600120

    def test_extract_ts_depth(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"e":"depthUpdate","E":1741689600200,"s":"BTCUSDT","U":1,"u":2,"pu":0}'
        ts = adapter.extract_exchange_ts("depth", raw)
        assert ts == 1741689600200

    def test_extract_ts_bookticker_missing_E_fallback(self):
        """If bookTicker has no E field, return None to signal fallback to received_at"""
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"e":"bookTicker","s":"BTCUSDT","b":"1.0","a":"2.0"}'
        ts = adapter.extract_exchange_ts("bookticker", raw)
        assert ts is None

    def test_extract_ts_depth_snapshot_returns_none(self):
        """depth_snapshot has no exchange_ts; caller must derive from received_at per spec 3.2.3"""
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"lastUpdateId":1005,"bids":[],"asks":[]}'
        ts = adapter.extract_exchange_ts("depth_snapshot", raw)
        assert ts is None

    def test_extract_ts_open_interest(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"openInterest":"12345","symbol":"BTCUSDT","time":1741689600700}'
        ts = adapter.extract_exchange_ts("open_interest", raw)
        assert ts == 1741689600700


class TestBinanceDepthParsing:
    def test_parse_depth_update_ids(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":1000,"u":1005,"pu":999,"b":[],"a":[]}'
        U, u, pu = adapter.parse_depth_update_ids(raw)
        assert U == 1000
        assert u == 1005
        assert pu == 999

    def test_parse_snapshot_last_update_id(self):
        from src.exchanges.binance import BinanceAdapter

        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
        )
        raw = '{"lastUpdateId":1005,"bids":[],"asks":[]}'
        assert adapter.parse_snapshot_last_update_id(raw) == 1005
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_binance_adapter.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'src.exchanges.binance'`

- [ ] **Step 3: Implement Binance adapter**

`src/exchanges/binance.py`:
```python
from __future__ import annotations

import orjson

from src.exchanges.base import ExchangeAdapter

# Stream name mapping from Binance subscription key to our stream type
_STREAM_KEY_MAP = {
    "aggTrade": "trades",
    "depth": "depth",
    "bookTicker": "bookticker",
    "markPrice": "funding_rate",
    "forceOrder": "liquidations",
}

# Which streams go on which socket
_PUBLIC_STREAMS = {"depth", "bookticker"}
_MARKET_STREAMS = {"trades", "funding_rate", "liquidations"}

# Binance subscription suffixes per our stream name
_SUBSCRIPTION_MAP = {
    "depth": "@depth@100ms",
    "bookticker": "@bookTicker",
    "trades": "@aggTrade",
    "funding_rate": "@markPrice@1s",
    "liquidations": "@forceOrder",
}


class BinanceAdapter(ExchangeAdapter):
    def __init__(self, ws_base: str, rest_base: str):
        self.ws_base = ws_base.rstrip("/")
        self.rest_base = rest_base.rstrip("/")

    def get_ws_urls(self, symbols: list[str], streams: list[str]) -> dict[str, str]:
        public_subs: list[str] = []
        market_subs: list[str] = []

        for symbol in symbols:
            s = symbol.lower()
            for stream in streams:
                suffix = _SUBSCRIPTION_MAP.get(stream)
                if suffix is None:
                    continue
                sub = f"{s}{suffix}"
                if stream in _PUBLIC_STREAMS:
                    public_subs.append(sub)
                elif stream in _MARKET_STREAMS:
                    market_subs.append(sub)

        urls: dict[str, str] = {}
        if public_subs:
            urls["public"] = f"{self.ws_base}/stream?streams={'/'.join(public_subs)}"
        if market_subs:
            urls["market"] = f"{self.ws_base}/stream?streams={'/'.join(market_subs)}"
        return urls

    def route_stream(self, raw_frame: str) -> tuple[str, str, str]:
        """Parse combined stream frame and extract (stream_type, symbol, raw_data_text).

        Uses balanced-brace string slicing for raw_data_text to preserve exact bytes.
        This is the "zero-copy equivalent" mechanism required by spec Section 4.1:
        the original data value's bytes are extracted without re-serialization.
        """
        # Locate the "data": key and extract the value via balanced brace counting
        data_key = '"data":'
        data_idx = raw_frame.index(data_key)
        data_start = data_idx + len(data_key)
        raw_data_text = _extract_data_value(raw_frame, data_start)

        # Parse outer frame for routing only (stream key)
        parsed = orjson.loads(raw_frame)
        stream_key = parsed["stream"]  # e.g., "btcusdt@aggTrade"
        symbol, stream_type = _parse_stream_key(stream_key)

        return stream_type, symbol, raw_data_text

    def extract_exchange_ts(self, stream: str, raw_text: str) -> int | None:
        parsed = orjson.loads(raw_text)
        if stream == "open_interest":
            return parsed.get("time")
        if stream == "depth_snapshot":
            # REST snapshot has no event timestamp
            return None
        ts = parsed.get("E")
        return ts if ts is not None else None

    def build_snapshot_url(self, symbol: str, limit: int = 1000) -> str:
        return f"{self.rest_base}/fapi/v1/depth?symbol={symbol.upper()}&limit={limit}"

    def build_open_interest_url(self, symbol: str) -> str:
        return f"{self.rest_base}/fapi/v1/openInterest?symbol={symbol.upper()}"

    def parse_snapshot_last_update_id(self, raw_text: str) -> int:
        parsed = orjson.loads(raw_text)
        return parsed["lastUpdateId"]

    def parse_depth_update_ids(self, raw_text: str) -> tuple[int, int, int]:
        parsed = orjson.loads(raw_text)
        return parsed["U"], parsed["u"], parsed["pu"]


def _extract_data_value(frame: str, search_start: int) -> str:
    """Extract the JSON value after "data": using balanced brace counting.
    This preserves the exact original bytes of the data field."""
    # Skip whitespace
    i = search_start
    while i < len(frame) and frame[i] in " \t\n\r":
        i += 1

    if frame[i] != "{":
        raise ValueError(f"Expected '{{' at position {i}, got '{frame[i]}'")

    depth = 0
    in_string = False
    escape_next = False
    start = i

    while i < len(frame):
        ch = frame[i]
        if escape_next:
            escape_next = False
        elif ch == "\\":
            if in_string:
                escape_next = True
        elif ch == '"':
            in_string = not in_string
        elif not in_string:
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return frame[start:i + 1]
        i += 1

    raise ValueError("Unbalanced braces in frame")


def _parse_stream_key(stream_key: str) -> tuple[str, str]:
    """Parse 'btcusdt@aggTrade' -> ('btcusdt', 'trades')"""
    parts = stream_key.split("@", 1)
    symbol = parts[0].lower()
    binance_stream = parts[1] if len(parts) > 1 else ""

    # Strip any parameters (e.g., @depth@100ms -> depth, @markPrice@1s -> markPrice)
    base_stream = binance_stream.split("@")[0]

    stream_type = _STREAM_KEY_MAP.get(base_stream)
    if stream_type is None:
        raise ValueError(f"Unknown Binance stream key: {stream_key}")

    return symbol, stream_type
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_binance_adapter.py -v`
Expected: All tests PASS (URL building, stream routing, raw text extraction, timestamp extraction, depth parsing).

- [ ] **Step 5: Commit**

```bash
git add src/exchanges/base.py src/exchanges/binance.py tests/unit/test_binance_adapter.py
git commit -m "feat: Binance exchange adapter with URL building, stream routing, and raw text extraction"
```

---

### Manual Verification: Chunk 2

1. **Run all unit tests so far:**
   ```bash
   uv run pytest tests/unit/ -v --tb=short
   ```
   **Check:** All tests pass — config (10), envelope (9), binance adapter (~18).

2. **Verify URL building manually:**
   ```bash
   uv run python -c "
   from src.exchanges.binance import BinanceAdapter
   adapter = BinanceAdapter(ws_base='wss://fstream.binance.com', rest_base='https://fapi.binance.com')
   urls = adapter.get_ws_urls(['btcusdt', 'ethusdt'], ['trades', 'depth', 'bookticker', 'funding_rate', 'liquidations'])
   for name, url in urls.items():
       print(f'{name}: {url}')
   print()
   print('Snapshot:', adapter.build_snapshot_url('btcusdt'))
   print('OI:', adapter.build_open_interest_url('ethusdt'))
   "
   ```
   **Check:** Two socket URLs (public + market) with correct Binance stream subscriptions. Snapshot and OI URLs point to correct REST endpoints with uppercase symbols.

3. **Verify raw text fidelity:**
   ```bash
   uv run python -c "
   from src.exchanges.binance import BinanceAdapter
   adapter = BinanceAdapter(ws_base='wss://x', rest_base='https://x')
   # Test with non-canonical number formatting
   inner = '{\"e\":\"aggTrade\",\"E\":100,\"p\":\"0.00100000\"}'
   frame = '{\"stream\":\"btcusdt@aggTrade\",\"data\":' + inner + '}'
   _, _, raw = adapter.route_stream(frame)
   print('Preserved:', raw == inner)
   print('Raw:', repr(raw))
   "
   ```
   **Check:** `Preserved: True` — the raw_text matches the original inner payload byte-for-byte.

---

## Chunk 3: Collector Core

### Task 3.1: Gap Detector (TDD)

**Files:**
- Create: `src/collector/gap_detector.py`
- Create: `tests/unit/test_gap_detector.py`

- [ ] **Step 1: Write failing tests for gap detector**

`tests/unit/test_gap_detector.py`:
```python
import pytest


class TestDepthPuChainValidator:
    def test_first_diff_after_sync_accepted(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        # First diff: U <= 1001 and u >= 1001
        result = det.validate_diff(U=999, u=1002, pu=998)
        assert result.valid is True
        assert result.gap is False

    def test_subsequent_diff_pu_chain_valid(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)  # sync
        result = det.validate_diff(U=1003, u=1005, pu=1002)
        assert result.valid is True

    def test_pu_chain_break_detected(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)  # sync
        result = det.validate_diff(U=1010, u=1015, pu=1008)  # gap: pu!=1002
        assert result.valid is False
        assert result.gap is True
        assert result.reason == "pu_chain_break"

    def test_stale_diff_before_sync_rejected(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        result = det.validate_diff(U=990, u=995, pu=989)  # u < lastUpdateId
        assert result.valid is False
        assert result.stale is True

    def test_no_sync_point_rejects(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        result = det.validate_diff(U=1, u=2, pu=0)
        assert result.valid is False
        assert result.reason == "no_sync_point"

    def test_reset_clears_state(self):
        from src.collector.gap_detector import DepthGapDetector

        det = DepthGapDetector(symbol="btcusdt")
        det.set_sync_point(last_update_id=1000)
        det.validate_diff(U=999, u=1002, pu=998)
        det.reset()
        result = det.validate_diff(U=1003, u=1005, pu=1002)
        assert result.valid is False
        assert result.reason == "no_sync_point"


class TestSessionSeqTracker:
    def test_sequential_no_gap(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        assert tracker.check(0) is None
        assert tracker.check(1) is None
        assert tracker.check(2) is None

    def test_gap_detected(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        tracker.check(0)
        tracker.check(1)
        gap = tracker.check(5)  # skipped 2, 3, 4
        assert gap is not None
        assert gap.expected == 2
        assert gap.actual == 5

    def test_first_message_no_gap(self):
        from src.collector.gap_detector import SessionSeqTracker

        tracker = SessionSeqTracker()
        assert tracker.check(42) is None  # first message, any seq ok
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_gap_detector.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement gap detector**

`src/collector/gap_detector.py`:
```python
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DiffValidationResult:
    valid: bool
    gap: bool = False
    stale: bool = False
    reason: str = ""


@dataclass
class SeqGap:
    expected: int
    actual: int


class DepthGapDetector:
    """Validates depth diff pu chain after synchronization with a snapshot."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._synced = False
        self._last_update_id: int | None = None
        self._last_u: int | None = None

    def set_sync_point(self, last_update_id: int) -> None:
        self._synced = True
        self._last_update_id = last_update_id
        self._last_u = None

    def validate_diff(self, U: int, u: int, pu: int) -> DiffValidationResult:
        if not self._synced:
            return DiffValidationResult(valid=False, reason="no_sync_point")

        # First diff after sync: find the sync point
        if self._last_u is None:
            lid = self._last_update_id
            if u < lid:
                return DiffValidationResult(valid=False, stale=True, reason="stale")
            if U <= lid + 1 and u >= lid + 1:
                self._last_u = u
                return DiffValidationResult(valid=True)
            return DiffValidationResult(valid=False, reason="no_sync_diff_found")

        # Subsequent diffs: pu must equal previous u
        if pu != self._last_u:
            return DiffValidationResult(valid=False, gap=True, reason="pu_chain_break")

        self._last_u = u
        return DiffValidationResult(valid=True)

    def reset(self) -> None:
        self._synced = False
        self._last_update_id = None
        self._last_u = None


class SessionSeqTracker:
    """Tracks session_seq for in-session gap detection."""

    def __init__(self):
        self._last_seq: int | None = None

    def check(self, seq: int) -> SeqGap | None:
        if self._last_seq is None:
            self._last_seq = seq
            return None
        expected = self._last_seq + 1
        self._last_seq = seq
        if seq != expected:
            return SeqGap(expected=expected, actual=seq)
        return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_gap_detector.py -v`
Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/collector/gap_detector.py tests/unit/test_gap_detector.py
git commit -m "feat: gap detector with depth pu-chain validation and session_seq tracking"
```

---

### Task 3.1b: Stream Handler & Scheduler Unit Tests (TDD)

**Files:**
- Create: `tests/unit/test_stream_handlers.py`

- [ ] **Step 1: Write tests for stream handlers and snapshot scheduler**

`tests/unit/test_stream_handlers.py`:
```python
import pytest
from unittest.mock import MagicMock, AsyncMock


class TestTradesHandler:
    @pytest.mark.asyncio
    async def test_produces_data_envelope(self):
        from src.collector.streams.trades import TradesHandler

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        handler = TradesHandler("binance", "session_1", producer)

        await handler.handle("btcusdt", '{"e":"aggTrade"}', 100, 0)
        producer.produce.assert_called_once()
        env = producer.produce.call_args[0][0]
        assert env["stream"] == "trades"
        assert env["symbol"] == "btcusdt"
        assert env["raw_text"] == '{"e":"aggTrade"}'


class TestDepthHandler:
    @pytest.mark.asyncio
    async def test_buffers_before_sync(self):
        """Diffs arriving before sync point should be buffered, not dropped."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":1,"u":2,"pu":0,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_not_called()
        # But the diff should be in the pending buffer
        assert len(handler._pending_diffs["btcusdt"]) == 1

    @pytest.mark.asyncio
    async def test_accepts_after_sync(self):
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])
        handler.set_sync_point("btcusdt", 1000)

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_called_once()

    @pytest.mark.asyncio
    async def test_drops_stale_diffs(self):
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])
        handler.set_sync_point("btcusdt", 1000)

        raw = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":990,"u":995,"pu":989,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw, 100, 0)
        producer.produce.assert_not_called()

    @pytest.mark.asyncio
    async def test_pending_diffs_replayed_after_sync(self):
        """Buffered diffs should be replayed when sync point is set."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"])

        # Buffer diffs while unsynced
        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1003,"u":1005,"pu":1002,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw1, 100, 0)
        await handler.handle("btcusdt", raw2, 101, 1)
        assert producer.produce.call_count == 0
        assert len(handler._pending_diffs["btcusdt"]) == 2

        # Sync point triggers replay
        handler.set_sync_point("btcusdt", 1000)
        # First diff syncs (U<=1001, u>=1001), second chains (pu==1002)
        assert producer.produce.call_count == 2
        assert len(handler._pending_diffs["btcusdt"]) == 0

    @pytest.mark.asyncio
    async def test_pu_chain_break_calls_callback(self):
        """pu_chain_break should trigger resync callback."""
        from src.collector.streams.depth import DepthHandler
        from src.exchanges.binance import BinanceAdapter

        producer = MagicMock()
        producer.produce = MagicMock(return_value=True)
        adapter = BinanceAdapter(ws_base="wss://x", rest_base="https://x")
        resync_called = []
        handler = DepthHandler("binance", "session_1", producer, adapter, ["btcusdt"],
                               on_pu_chain_break=lambda sym: resync_called.append(sym))
        handler.set_sync_point("btcusdt", 1000)

        # First valid diff
        raw1 = '{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":999,"u":1002,"pu":998,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw1, 100, 0)
        # Broken chain
        raw2 = '{"e":"depthUpdate","E":101,"s":"BTCUSDT","U":1010,"u":1015,"pu":1008,"b":[],"a":[]}'
        await handler.handle("btcusdt", raw2, 101, 1)
        assert resync_called == ["btcusdt"]


class TestProducerOverflow:
    def test_buffer_error_increments_dropped(self):
        """When confluent_kafka raises BufferError, produce returns False and tracks overflow window."""
        from unittest.mock import patch, MagicMock
        from src.collector.producer import CryptoLakeProducer
        from src.common.envelope import create_data_envelope

        with patch("src.collector.producer.KafkaProducer") as MockKafka:
            mock_instance = MagicMock()
            mock_instance.produce.side_effect = BufferError("queue full")
            MockKafka.return_value = mock_instance

            producer = CryptoLakeProducer.__new__(CryptoLakeProducer)
            producer.exchange = "binance"
            producer.collector_session_id = "s"
            producer._producer = mock_instance
            producer._on_overflow = None
            producer._overflow_start = {}
            producer._overflow_seq = 0

            env = create_data_envelope(
                exchange="binance", symbol="btcusdt", stream="trades",
                raw_text="{}", exchange_ts=0,
                collector_session_id="s", session_seq=0,
            )
            result = producer.produce(env)
            assert result is False
            # Overflow window should be tracked
            assert ("btcusdt", "trades") in producer._overflow_start
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_stream_handlers.py -v`
Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_stream_handlers.py
git commit -m "test: unit tests for stream handlers, scheduler, and producer overflow"
```

---

### Task 3.2: Collector Prometheus Metrics

**Files:**
- Create: `src/collector/metrics.py`

- [ ] **Step 1: Define all collector metrics**

`src/collector/metrics.py`:
```python
from prometheus_client import Counter, Gauge, Histogram

messages_produced_total = Counter(
    "collector_messages_produced_total",
    "Messages sent to Redpanda",
    ["exchange", "symbol", "stream"],
)

ws_connections_active = Gauge(
    "collector_ws_connections_active",
    "Current open WebSocket connections",
    ["exchange"],
)

ws_reconnects_total = Counter(
    "collector_ws_reconnects_total",
    "Reconnection count",
    ["exchange"],
)

gaps_detected_total = Counter(
    "collector_gaps_detected_total",
    "Gaps detected (sequence breaks, disconnects, drops)",
    ["exchange", "symbol", "stream"],
)

exchange_latency_ms = Histogram(
    "collector_exchange_latency_ms",
    "received_at - exchange_ts distribution (ms)",
    ["exchange", "symbol", "stream"],
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000],
)

snapshots_taken_total = Counter(
    "collector_snapshots_taken_total",
    "Successful REST snapshots",
    ["exchange", "symbol"],
)

snapshots_failed_total = Counter(
    "collector_snapshots_failed_total",
    "Failed snapshot attempts",
    ["exchange", "symbol"],
)

ntp_drift_ms = Gauge(
    "collector_ntp_drift_ms",
    "Estimated NTP clock drift in ms",
)

producer_buffer_size = Gauge(
    "collector_producer_buffer_size",
    "In-memory buffer size (messages) when Redpanda unavailable",
    ["exchange"],
)

messages_dropped_total = Counter(
    "collector_messages_dropped_total",
    "Messages dropped due to buffer overflow",
    ["exchange", "symbol", "stream"],
)
```

- [ ] **Step 2: Commit**

```bash
git add src/collector/metrics.py
git commit -m "feat: collector Prometheus metric definitions"
```

---

### Task 3.3: Redpanda Producer Wrapper (TDD)

**Files:**
- Create: `src/collector/producer.py`

This component wraps `confluent_kafka.Producer` with envelope serialization, topic routing, partitioned buffer management, and overflow policy. Full TDD is done via integration tests (Task 7.1) since it requires a real Redpanda broker. Here we define the interface and logic that can be unit-tested.

- [ ] **Step 1: Implement producer wrapper**

`src/collector/producer.py`:
```python
from __future__ import annotations

import threading
from typing import Callable

import structlog

from src.collector import metrics as collector_metrics
from src.common.envelope import serialize_envelope

logger = structlog.get_logger()

# Per-stream buffer caps (out of 100k global default)
_DEFAULT_BUFFER_CAPS = {
    "depth": 80_000,
    "trades": 10_000,
}
_DEFAULT_OTHER_CAP = 10_000


class CryptoLakeProducer:
    """Wraps confluent_kafka.Producer with envelope routing and overflow protection.

    Tracks overflow windows per (symbol, stream). When the first BufferError occurs,
    records the start timestamp. When produce succeeds after an overflow period,
    emits a buffer_overflow gap record covering the dropped interval.
    """

    def __init__(
        self,
        brokers: list[str],
        exchange: str,
        collector_session_id: str = "",
        max_buffer: int = 100_000,
        buffer_caps: dict[str, int] | None = None,
        on_overflow: Callable[[str, str, str], None] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.max_buffer = max_buffer
        self.buffer_caps = buffer_caps or _DEFAULT_BUFFER_CAPS
        self.other_cap = _DEFAULT_OTHER_CAP
        self._on_overflow = on_overflow
        self._buffer_counts: dict[str, int] = {}
        self._total_buffered = 0
        self._lock = threading.Lock()
        # Overflow window tracking: {(symbol, stream): start_ts_ns}
        self._overflow_start: dict[tuple[str, str], int] = {}
        self._overflow_seq: int = 0

        from confluent_kafka import Producer as KafkaProducer

        self._producer = KafkaProducer({
            "bootstrap.servers": ",".join(brokers),
            "acks": "all",
            "linger.ms": 5,
            "queue.buffering.max.messages": max_buffer,
            "queue.buffering.max.kbytes": 1048576,  # 1GB
        })

    def _get_stream_cap(self, stream: str) -> int:
        """Return the per-stream buffer cap for partitioned overflow protection."""
        return self.buffer_caps.get(stream, self.other_cap)

    def produce(self, envelope: dict) -> bool:
        """Produce an envelope to the appropriate Redpanda topic.

        Returns True if produced, False if dropped due to overflow.
        Enforces per-stream buffer caps to prevent high-volume streams (depth)
        from starving low-volume irreplaceable streams (liquidations, funding_rate).
        On recovery from overflow, emits a buffer_overflow gap record.
        """
        import time as _time
        stream = envelope["stream"]
        symbol = envelope["symbol"]
        topic = f"{self.exchange}.{stream}"
        key = symbol.encode()
        value = serialize_envelope(envelope)

        # Check per-stream cap and optimistically increment under a single lock
        # to avoid TOCTOU between cap check and count update (spec 7.5)
        with self._lock:
            current = self._buffer_counts.get(stream, 0)
            cap = self._get_stream_cap(stream)
            if current >= cap:
                # Per-stream cap exceeded — drop to protect other streams
                collector_metrics.messages_dropped_total.labels(
                    exchange=self.exchange, symbol=symbol, stream=stream,
                ).inc()
                overflow_key = (symbol, stream)
                if overflow_key not in self._overflow_start:
                    self._overflow_start[overflow_key] = _time.time_ns()
                if self._on_overflow:
                    self._on_overflow(self.exchange, symbol, stream)
                return False
            # Optimistically increment before produce; delivery callback decrements
            self._buffer_counts[stream] = current + 1

        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=self._make_delivery_cb(stream),
            )
            self._producer.poll(0)
            collector_metrics.messages_produced_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()

            # Check if we're recovering from overflow for this (symbol, stream)
            overflow_key = (symbol, stream)
            if overflow_key in self._overflow_start:
                self._emit_overflow_gap(symbol, stream, self._overflow_start.pop(overflow_key))

            return True
        except BufferError:
            # Roll back optimistic increment since produce failed
            with self._lock:
                self._buffer_counts[stream] = max(0, self._buffer_counts.get(stream, 1) - 1)
            collector_metrics.messages_dropped_total.labels(
                exchange=self.exchange, symbol=symbol, stream=stream,
            ).inc()
            # Track overflow window start
            overflow_key = (symbol, stream)
            if overflow_key not in self._overflow_start:
                self._overflow_start[overflow_key] = _time.time_ns()
            if self._on_overflow:
                self._on_overflow(self.exchange, symbol, stream)
            return False

    def _make_delivery_cb(self, stream: str):
        """Create a delivery callback that decrements per-stream buffer count."""
        def _cb(err, msg):
            with self._lock:
                count = self._buffer_counts.get(stream, 0)
                if count > 0:
                    self._buffer_counts[stream] = count - 1
            if err is not None:
                logger.error("producer_delivery_failed", error=str(err),
                             topic=msg.topic(), key=msg.key())
        return _cb

    def _emit_overflow_gap(self, symbol: str, stream: str, start_ts: int) -> None:
        """Emit a buffer_overflow gap record when recovering from overflow."""
        import time as _time
        from src.common.envelope import create_gap_envelope
        gap = create_gap_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream=stream,
            collector_session_id=self.collector_session_id,
            session_seq=self._overflow_seq,
            gap_start_ts=start_ts,
            gap_end_ts=_time.time_ns(),
            reason="buffer_overflow",
            detail=f"Producer buffer was full; messages dropped for {stream}/{symbol}",
        )
        self._overflow_seq += 1
        # Produce the gap record itself (best-effort — buffer just recovered so should succeed)
        topic = f"{self.exchange}.{stream}"
        try:
            self._producer.produce(
                topic=topic,
                key=symbol.encode(),
                value=serialize_envelope(gap),
                on_delivery=self._make_delivery_cb(stream),
            )
            logger.info("buffer_overflow_gap_emitted", symbol=symbol, stream=stream)
        except BufferError:
            logger.error("buffer_overflow_gap_emit_failed", symbol=symbol, stream=stream)

    def is_connected(self) -> bool:
        """Check if the producer can reach the broker via metadata query."""
        try:
            # list_topics() actually contacts the broker — timeout keeps it fast
            self._producer.list_topics(timeout=5)
            return True
        except Exception:
            return False

    def is_healthy_for_resync(self) -> bool:
        """True if producer is connected and not actively dropping messages.
        Used as precondition for depth resync (spec 7.2)."""
        if self._overflow_start:
            return False  # actively in overflow for at least one stream
        try:
            pending = len(self._producer)
            if pending > self.max_buffer * 0.8:
                return False  # buffer above 80% — too risky for resync
            self._producer.list_topics(timeout=5)
            return True
        except Exception:
            return False

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    def poll(self, timeout: float = 0) -> int:
        return self._producer.poll(timeout)

```

- [ ] **Step 2: Commit**

```bash
git add src/collector/producer.py
git commit -m "feat: Redpanda producer wrapper with overflow protection"
```

---

### Task 3.4: Stream Handler Base & Implementations

**Files:**
- Create: `src/collector/streams/base.py`
- Create: `src/collector/streams/trades.py`
- Create: `src/collector/streams/depth.py`
- Create: `src/collector/streams/bookticker.py`
- Create: `src/collector/streams/funding_rate.py`
- Create: `src/collector/streams/liquidations.py`
- Create: `src/collector/streams/open_interest.py`

- [ ] **Step 1: Define base stream handler**

`src/collector/streams/base.py`:
```python
from __future__ import annotations

from abc import ABC, abstractmethod

import structlog

from src.collector.gap_detector import SessionSeqTracker
from src.collector import metrics as collector_metrics
from src.common.envelope import create_gap_envelope

logger = structlog.get_logger()


class StreamHandler(ABC):
    """Processes a routed message for a specific stream type.

    Subclasses that want session_seq gap detection should call
    self._check_seq(symbol, session_seq) at the start of handle().
    """

    def _init_seq_tracking(self, exchange: str, collector_session_id: str,
                           producer, stream_name: str) -> None:
        """Call from subclass __init__ to enable session_seq gap detection."""
        self._seq_exchange = exchange
        self._seq_collector_session_id = collector_session_id
        self._seq_producer = producer
        self._seq_stream = stream_name
        self._seq_trackers: dict[str, SessionSeqTracker] = {}

    def _check_seq(self, symbol: str, session_seq: int) -> None:
        """Check session_seq continuity; emit gap record on skip (spec 7.4)."""
        tracker = self._seq_trackers.get(symbol)
        if tracker is None:
            tracker = SessionSeqTracker()
            self._seq_trackers[symbol] = tracker
        gap = tracker.check(session_seq)
        if gap is not None:
            import time as _time
            logger.warning("session_seq_skip", symbol=symbol,
                           stream=self._seq_stream,
                           expected=gap.expected, actual=gap.actual)
            collector_metrics.gaps_detected_total.labels(
                exchange=self._seq_exchange, symbol=symbol,
                stream=self._seq_stream,
            ).inc()
            now = _time.time_ns()
            gap_env = create_gap_envelope(
                exchange=self._seq_exchange, symbol=symbol,
                stream=self._seq_stream,
                collector_session_id=self._seq_collector_session_id,
                session_seq=session_seq,
                gap_start_ts=now, gap_end_ts=now,
                reason="session_seq_skip",
                detail=f"session_seq skip: expected {gap.expected}, got {gap.actual}",
            )
            self._seq_producer.produce(gap_env)

    @abstractmethod
    async def handle(
        self,
        symbol: str,
        raw_text: str,
        exchange_ts: int | None,
        session_seq: int,
    ) -> None:
        """Process a single stream message: wrap in envelope, produce to Redpanda."""
```

- [ ] **Step 2: Implement trade handler**

`src/collector/streams/trades.py`:
```python
from __future__ import annotations

from src.collector.streams.base import StreamHandler
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_data_envelope


class TradesHandler(StreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self._init_seq_tracking(exchange, collector_session_id, producer, "trades")

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="trades",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
```

- [ ] **Step 3: Implement depth handler (with gap detection integration)**

`src/collector/streams/depth.py`:
```python
from __future__ import annotations

from typing import Callable

import structlog

from src.collector.gap_detector import DepthGapDetector
from src.collector.producer import CryptoLakeProducer
from src.collector.streams.base import StreamHandler
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


_MAX_PENDING_DIFFS = 5000  # max buffered diffs per symbol during resync


class DepthHandler(StreamHandler):
    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        symbols: list[str],
        on_pu_chain_break: Callable[[str], None] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.detectors: dict[str, DepthGapDetector] = {
            s: DepthGapDetector(symbol=s) for s in symbols
        }
        self._pending_diffs: dict[str, list[tuple[str, int | None, int]]] = {
            s: [] for s in symbols
        }
        self._on_pu_chain_break = on_pu_chain_break

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        detector = self.detectors.get(symbol)
        if detector is None:
            return

        U, u, pu = self.adapter.parse_depth_update_ids(raw_text)
        result = detector.validate_diff(U=U, u=u, pu=pu)

        if result.stale:
            logger.debug("depth_stale_diff_dropped", symbol=symbol, u=u)
            return

        if result.gap:
            import time as _time
            logger.warning("depth_pu_chain_break", symbol=symbol,
                           expected_pu=detector._last_u, actual_pu=pu)
            collector_metrics.gaps_detected_total.labels(
                exchange=self.exchange, symbol=symbol, stream="depth",
            ).inc()
            # Emit gap record immediately when break is observed (spec 7.4)
            now = _time.time_ns()
            gap = create_gap_envelope(
                exchange=self.exchange, symbol=symbol, stream="depth",
                collector_session_id=self.collector_session_id,
                session_seq=session_seq,
                gap_start_ts=now, gap_end_ts=now,
                reason="pu_chain_break",
                detail=f"pu chain break: expected pu={detector._last_u}, got pu={pu}",
            )
            self.producer.produce(gap)
            # Trigger resync via callback (wired by Collector to WebSocketManager._depth_resync)
            if self._on_pu_chain_break:
                self._on_pu_chain_break(symbol)
            return

        if not result.valid:
            # Not synced yet — buffer live diffs for replay after snapshot arrives (spec 7.2 step 2)
            pending = self._pending_diffs.get(symbol)
            if pending is not None and len(pending) < _MAX_PENDING_DIFFS:
                pending.append((raw_text, exchange_ts, session_seq))
            else:
                import time as _time
                logger.warning("depth_pending_buffer_full", symbol=symbol)
                collector_metrics.gaps_detected_total.labels(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                ).inc()
                # Emit gap record for explicit data loss (spec invariant 1.4)
                now = _time.time_ns()
                gap = create_gap_envelope(
                    exchange=self.exchange, symbol=symbol, stream="depth",
                    collector_session_id=self.collector_session_id,
                    session_seq=session_seq,
                    gap_start_ts=now, gap_end_ts=now,
                    reason="buffer_overflow",
                    detail=f"Depth pending-diff buffer full ({_MAX_PENDING_DIFFS}), diffs dropped while awaiting snapshot",
                )
                self.producer.produce(gap)
                # Force resync — continued buffering is pointless
                detector.reset()
                if pending is not None:
                    pending.clear()
                if self._on_pu_chain_break:
                    self._on_pu_chain_break(symbol)
            return

        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="depth",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)

    def set_sync_point(self, symbol: str, last_update_id: int) -> None:
        """Set sync point from snapshot and replay buffered diffs (spec 7.2 steps 4-6)."""
        detector = self.detectors.get(symbol)
        if detector is None:
            return
        detector.set_sync_point(last_update_id)

        # Replay buffered diffs
        pending = self._pending_diffs.get(symbol, [])
        self._pending_diffs[symbol] = []
        replayed = 0
        for raw_text, exchange_ts, session_seq in pending:
            U, u, pu = self.adapter.parse_depth_update_ids(raw_text)
            result = detector.validate_diff(U=U, u=u, pu=pu)
            if result.stale:
                continue
            if not result.valid:
                continue
            envelope = create_data_envelope(
                exchange=self.exchange,
                symbol=symbol,
                stream="depth",
                raw_text=raw_text,
                exchange_ts=exchange_ts or 0,
                collector_session_id=self.collector_session_id,
                session_seq=session_seq,
            )
            self.producer.produce(envelope)
            replayed += 1
        if replayed > 0:
            logger.info("depth_pending_replayed", symbol=symbol, replayed=replayed,
                        total_buffered=len(pending))

    def reset(self, symbol: str) -> None:
        detector = self.detectors.get(symbol)
        if detector:
            detector.reset()
        # Keep pending diffs — they will be replayed after next sync point
```

- [ ] **Step 4: Implement remaining handlers (bookticker, funding_rate, liquidations)**

`src/collector/streams/bookticker.py`:
```python
from __future__ import annotations

from src.collector.streams.base import StreamHandler
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_data_envelope


class BookTickerHandler(StreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self._init_seq_tracking(exchange, collector_session_id, producer, "bookticker")

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="bookticker",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
```

`src/collector/streams/funding_rate.py`:
```python
from __future__ import annotations

from src.collector.streams.base import StreamHandler
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_data_envelope


class FundingRateHandler(StreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self._init_seq_tracking(exchange, collector_session_id, producer, "funding_rate")

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="funding_rate",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
```

`src/collector/streams/liquidations.py`:
```python
from __future__ import annotations

from src.collector.streams.base import StreamHandler
from src.collector.producer import CryptoLakeProducer
from src.common.envelope import create_data_envelope


class LiquidationsHandler(StreamHandler):
    def __init__(self, exchange: str, collector_session_id: str, producer: CryptoLakeProducer):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self._init_seq_tracking(exchange, collector_session_id, producer, "liquidations")

    async def handle(self, symbol: str, raw_text: str, exchange_ts: int | None, session_seq: int) -> None:
        self._check_seq(symbol, session_seq)
        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="liquidations",
            raw_text=raw_text,
            exchange_ts=exchange_ts or 0,
            collector_session_id=self.collector_session_id,
            session_seq=session_seq,
        )
        self.producer.produce(envelope)
```

- [ ] **Step 5: Implement open interest REST poller**

`src/collector/streams/open_interest.py`:
```python
from __future__ import annotations

import asyncio
import time

import aiohttp
import structlog

from src.collector.producer import CryptoLakeProducer
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


class OpenInterestPoller:
    """Periodically polls Binance REST API for open interest data."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        symbols: list[str],
        poll_interval_seconds: int = 300,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.symbols = symbols
        self.poll_interval = poll_interval_seconds
        self._session: aiohttp.ClientSession | None = None
        self._seq_counters: dict[str, int] = {s: 0 for s in symbols}
        self._running = False

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        self._running = True
        # Stagger initial requests
        tasks = []
        for i, symbol in enumerate(self.symbols):
            delay = (self.poll_interval / len(self.symbols)) * i
            tasks.append(asyncio.create_task(self._poll_loop(symbol, initial_delay=delay)))
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()

    async def _poll_loop(self, symbol: str, initial_delay: float = 0) -> None:
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        while self._running:
            await self._poll_once(symbol)
            await asyncio.sleep(self.poll_interval)

    async def _poll_once(self, symbol: str, retries: int = 3) -> None:
        url = self.adapter.build_open_interest_url(symbol)
        for attempt in range(retries):
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        logger.warning("open_interest_rate_limited", symbol=symbol,
                                       retry_after=retry_after)
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    raw_text = await resp.text()
                    exchange_ts = self.adapter.extract_exchange_ts("open_interest", raw_text)
                    seq = self._seq_counters[symbol]
                    self._seq_counters[symbol] += 1
                    envelope = create_data_envelope(
                        exchange=self.exchange,
                        symbol=symbol,
                        stream="open_interest",
                        raw_text=raw_text,
                        exchange_ts=exchange_ts or int(time.time_ns() / 1_000_000),
                        collector_session_id=self.collector_session_id,
                        session_seq=seq,
                    )
                    self.producer.produce(envelope)
                    return
            except Exception as e:
                logger.warning("open_interest_poll_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        # All retries exhausted — emit gap
        logger.error("open_interest_poll_exhausted", symbol=symbol)
        collector_metrics.gaps_detected_total.labels(
            exchange=self.exchange, symbol=symbol, stream="open_interest",
        ).inc()
        seq = self._seq_counters[symbol]
        self._seq_counters[symbol] += 1
        gap = create_gap_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="open_interest",
            collector_session_id=self.collector_session_id,
            session_seq=seq,
            gap_start_ts=time.time_ns(),
            gap_end_ts=time.time_ns(),
            reason="snapshot_poll_miss",
            detail=f"Open interest poll failed after {retries} retries",
        )
        self.producer.produce(gap)
```

- [ ] **Step 6: Commit**

```bash
git add src/collector/streams/
git commit -m "feat: stream handlers for trades, depth, bookticker, funding_rate, liquidations, open_interest"
```

---

### Task 3.5: Snapshot Scheduler (TDD)

**Files:**
- Create: `src/collector/snapshot.py`
- Create: `tests/unit/test_snapshot.py`

- [ ] **Step 1: Write failing tests for snapshot scheduler**

`tests/unit/test_snapshot.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_snapshot.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement snapshot scheduler**

`src/collector/snapshot.py`:
```python
from __future__ import annotations

import asyncio
import time

import aiohttp
import structlog

from src.collector.producer import CryptoLakeProducer
from src.collector.streams.depth import DepthHandler
from src.collector import metrics as collector_metrics
from src.common.envelope import create_data_envelope, create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


def parse_interval_seconds(interval: str) -> int:
    """Parse '5m', '1m', '30s' to seconds."""
    if interval.endswith("m"):
        return int(interval[:-1]) * 60
    if interval.endswith("s"):
        return int(interval[:-1])
    raise ValueError(f"Invalid interval: {interval}")


class SnapshotScheduler:
    """Periodically fetches depth snapshots via REST API."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        producer: CryptoLakeProducer,
        adapter: BinanceAdapter,
        depth_handler: DepthHandler,
        symbols: list[str],
        default_interval: str = "5m",
        overrides: dict[str, str] | None = None,
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.producer = producer
        self.adapter = adapter
        self.depth_handler = depth_handler
        self.symbols = symbols
        self.default_interval_s = parse_interval_seconds(default_interval)
        self.overrides = {
            s: parse_interval_seconds(v) for s, v in (overrides or {}).items()
        }
        self._session: aiohttp.ClientSession | None = None
        self._seq_counters: dict[str, int] = {s: 0 for s in symbols}
        self._running = False

    def _get_interval(self, symbol: str) -> int:
        return self.overrides.get(symbol, self.default_interval_s)

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        self._running = True
        tasks = []
        for i, symbol in enumerate(self.symbols):
            interval = self._get_interval(symbol)
            delay = (interval / len(self.symbols)) * i
            tasks.append(asyncio.create_task(self._poll_loop(symbol, interval, delay)))
        await asyncio.gather(*tasks)

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()

    async def fetch_snapshot(self, symbol: str, retries: int = 3) -> str | None:
        """Fetch a single depth snapshot. Returns raw_text or None on total failure."""
        url = self.adapter.build_snapshot_url(symbol)
        for attempt in range(retries):
            try:
                async with self._session.get(url) as resp:
                    if resp.status == 429:
                        retry_after = int(resp.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    resp.raise_for_status()
                    raw_text = await resp.text()
                    collector_metrics.snapshots_taken_total.labels(
                        exchange=self.exchange, symbol=symbol,
                    ).inc()
                    return raw_text
            except Exception as e:
                logger.warning("snapshot_fetch_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                collector_metrics.snapshots_failed_total.labels(
                    exchange=self.exchange, symbol=symbol,
                ).inc()
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
        return None

    async def _poll_loop(self, symbol: str, interval: int, initial_delay: float) -> None:
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        while self._running:
            await self._take_snapshot(symbol)
            await asyncio.sleep(interval)

    async def _take_snapshot(self, symbol: str) -> None:
        raw_text = await self.fetch_snapshot(symbol)
        if raw_text is None:
            # All retries exhausted
            collector_metrics.gaps_detected_total.labels(
                exchange=self.exchange, symbol=symbol, stream="depth_snapshot",
            ).inc()
            seq = self._seq_counters[symbol]
            self._seq_counters[symbol] += 1
            gap = create_gap_envelope(
                exchange=self.exchange,
                symbol=symbol,
                stream="depth_snapshot",
                collector_session_id=self.collector_session_id,
                session_seq=seq,
                gap_start_ts=time.time_ns(),
                gap_end_ts=time.time_ns(),
                reason="snapshot_poll_miss",
                detail=f"Depth snapshot poll failed after all retries",
            )
            self.producer.produce(gap)
            return

        received_at = time.time_ns()
        exchange_ts = int(received_at / 1_000_000)  # no E in REST snapshot
        seq = self._seq_counters[symbol]
        self._seq_counters[symbol] += 1

        envelope = create_data_envelope(
            exchange=self.exchange,
            symbol=symbol,
            stream="depth_snapshot",
            raw_text=raw_text,
            exchange_ts=exchange_ts,
            collector_session_id=self.collector_session_id,
            session_seq=seq,
            received_at=received_at,
        )
        self.producer.produce(envelope)
        # NOTE: Periodic snapshots are archived checkpoints for later reconstruction.
        # They must NOT call depth_handler.set_sync_point() — that would reset the
        # live pu chain validation mid-stream. Live sync is only reset during the
        # depth resync flow (reconnect or pu_chain_break, see Section 7.2).
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_snapshot.py -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/collector/snapshot.py tests/unit/test_snapshot.py
git commit -m "feat: depth snapshot scheduler with staggered polling and retry"
```

---

### Task 3.6: WebSocket Connection Manager

**Files:**
- Create: `src/collector/connection.py`

- [ ] **Step 1: Implement connection manager**

`src/collector/connection.py`:
```python
from __future__ import annotations

import asyncio
import time

import structlog
import websockets

from src.collector import metrics as collector_metrics
from src.collector.producer import CryptoLakeProducer
from src.collector.streams.base import StreamHandler
from src.common.envelope import create_gap_envelope
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()

# Backoff: 1s, 2s, 4s, 8s, 16s, 32s, max 60s
_MAX_BACKOFF = 60
_RECONNECT_BEFORE_24H = 23 * 3600 + 50 * 60  # 23h50m


class WebSocketManager:
    """Manages WebSocket connections to Binance with reconnect and demultiplexing."""

    def __init__(
        self,
        exchange: str,
        collector_session_id: str,
        adapter: BinanceAdapter,
        producer: CryptoLakeProducer,
        handlers: dict[str, StreamHandler],
        symbols: list[str],
        enabled_streams: list[str],
    ):
        self.exchange = exchange
        self.collector_session_id = collector_session_id
        self.adapter = adapter
        self.producer = producer
        self.handlers = handlers
        self.symbols = symbols
        self.enabled_streams = enabled_streams
        self._seq_counters: dict[tuple[str, str], int] = {}
        self._running = False
        self._ws_connected: dict[str, bool] = {}  # {socket_name: connected}
        self._last_received_at: dict[tuple[str, str], int] = {}
        self._consecutive_drops = 0
        self._backpressure_threshold = 10  # consecutive drops before pausing WS reads

    def _next_seq(self, symbol: str, stream: str) -> int:
        key = (symbol, stream)
        seq = self._seq_counters.get(key, 0)
        self._seq_counters[key] = seq + 1
        return seq

    async def start(self) -> None:
        self._running = True
        ws_streams = [s for s in self.enabled_streams
                      if s not in ("depth_snapshot", "open_interest")]
        urls = self.adapter.get_ws_urls(self.symbols, ws_streams)

        tasks = []
        for socket_name, url in urls.items():
            tasks.append(asyncio.create_task(
                self._connection_loop(socket_name, url)
            ))
        await asyncio.gather(*tasks)

    def has_ws_streams(self) -> bool:
        """True if this manager is expected to open any WebSocket connections."""
        ws_streams = [s for s in self.enabled_streams
                      if s not in ("depth_snapshot", "open_interest")]
        urls = self.adapter.get_ws_urls(self.symbols, ws_streams)
        return len(urls) > 0

    def is_connected(self) -> bool:
        """True if all expected WebSocket sockets are currently connected.
        Returns True if no WS sockets are expected (REST-only config, spec 11.3)."""
        if not self.has_ws_streams():
            return True  # no WS needed — skip check
        if not self._ws_connected:
            return False
        return all(self._ws_connected.values())

    async def stop(self) -> None:
        self._running = False

    async def _connection_loop(self, socket_name: str, url: str) -> None:
        backoff = 1
        while self._running:
            connect_time = time.monotonic()
            try:
                collector_metrics.ws_connections_active.labels(
                    exchange=self.exchange
                ).inc()
                async with websockets.connect(url, ping_interval=30, ping_timeout=10,
                                              close_timeout=5) as ws:
                    self._ws_connected[socket_name] = True
                    logger.info("ws_connected", socket=socket_name, url=url[:80])
                    backoff = 1  # reset on successful connect
                    await self._receive_loop(ws, socket_name, connect_time)
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                logger.warning("ws_disconnected", socket=socket_name, error=str(e))
            except Exception as e:
                logger.error("ws_unexpected_error", socket=socket_name, error=str(e))
            finally:
                self._ws_connected[socket_name] = False
                collector_metrics.ws_connections_active.labels(
                    exchange=self.exchange
                ).dec()
                collector_metrics.ws_reconnects_total.labels(
                    exchange=self.exchange
                ).inc()

            if not self._running:
                break

            # Emit gap records for all streams on this socket
            self._emit_disconnect_gaps(socket_name)

            # Trigger depth resync if public socket disconnected (spec 7.2)
            if socket_name == "public" and "depth" in self.enabled_streams:
                for symbol in self.symbols:
                    await self._depth_resync(symbol)

            await asyncio.sleep(min(backoff, _MAX_BACKOFF))
            backoff = min(backoff * 2, _MAX_BACKOFF)

    async def _receive_loop(self, ws, socket_name: str, connect_time: float) -> None:
        async for raw_frame in ws:
            if not self._running:
                break
            # Proactive 24h reconnect
            if time.monotonic() - connect_time > _RECONNECT_BEFORE_24H:
                logger.info("ws_proactive_reconnect", socket=socket_name)
                await ws.close()
                return

            # Backpressure: if producer is overwhelmed, pause WS reads
            # This creates a clean gap window instead of silently dropping thousands of msgs
            if self._consecutive_drops >= self._backpressure_threshold:
                logger.warning("ws_backpressure_active", socket=socket_name,
                               consecutive_drops=self._consecutive_drops)
                # Wait until producer drains, polling every 100ms
                while self._consecutive_drops >= self._backpressure_threshold and self._running:
                    self.producer.poll(0.1)
                    # Try a no-op produce to see if buffer has space
                    self._consecutive_drops = 0  # reset and re-evaluate on next produce
                    await asyncio.sleep(0.1)
                logger.info("ws_backpressure_released", socket=socket_name)

            try:
                stream_type, symbol, raw_text = self.adapter.route_stream(raw_frame)
            except Exception as e:
                logger.error("ws_route_failed", socket=socket_name, error=str(e))
                continue

            handler = self.handlers.get(stream_type)
            if handler is None:
                continue

            exchange_ts = self.adapter.extract_exchange_ts(stream_type, raw_text)
            seq = self._next_seq(symbol, stream_type)

            # Track latency
            if exchange_ts is not None:
                received_ms = time.time_ns() / 1_000_000
                latency = received_ms - exchange_ts
                collector_metrics.exchange_latency_ms.labels(
                    exchange=self.exchange, symbol=symbol, stream=stream_type,
                ).observe(latency)

            self._last_received_at[(symbol, stream_type)] = time.time_ns()
            await handler.handle(symbol, raw_text, exchange_ts, seq)

    async def _depth_resync(self, symbol: str) -> None:
        """Depth resync flow per spec Section 7.2.

        Called on reconnect or pu chain break for the public socket.
        1. Reset depth detector (diffs will be buffered by handler until sync)
        2. Fetch a fresh REST snapshot
        3. Set sync point from snapshot's lastUpdateId
        4. Depth handler resumes accepting diffs from the sync point
        """
        depth_handler = self.handlers.get("depth")
        if depth_handler is None:
            return

        logger.info("depth_resync_starting", symbol=symbol)

        # Precondition: wait until producer is healthy (spec 7.2)
        # Do not resync while producer is offline or dropping messages
        _max_wait = 60  # seconds
        _waited = 0
        while not self.producer.is_healthy_for_resync() and _waited < _max_wait:
            logger.warning("depth_resync_waiting_for_producer", symbol=symbol,
                           waited_s=_waited)
            await asyncio.sleep(2)
            _waited += 2
        if not self.producer.is_healthy_for_resync():
            logger.error("depth_resync_aborted_producer_unhealthy", symbol=symbol)
            import time as _time
            from src.common.envelope import create_gap_envelope
            gap = create_gap_envelope(
                exchange=self.exchange, symbol=symbol, stream="depth",
                collector_session_id=self.collector_session_id,
                session_seq=self._next_seq(symbol, "depth"),
                gap_start_ts=_time.time_ns(), gap_end_ts=_time.time_ns(),
                reason="pu_chain_break",
                detail=f"Depth resync aborted: producer unhealthy after {_max_wait}s wait",
            )
            # Best-effort gap emit — producer may still be unhealthy
            self.producer.produce(gap)
            return

        depth_handler.reset(symbol)

        # Import here to avoid circular dependency
        import aiohttp
        retries = 3
        for attempt in range(retries):
            try:
                url = self.adapter.build_snapshot_url(symbol)
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        resp.raise_for_status()
                        raw_text = await resp.text()

                last_update_id = self.adapter.parse_snapshot_last_update_id(raw_text)
                depth_handler.set_sync_point(symbol, last_update_id)

                # Produce the resync snapshot to the archive
                from src.common.envelope import create_data_envelope
                import time as _time
                received_at = _time.time_ns()
                env = create_data_envelope(
                    exchange=self.exchange, symbol=symbol, stream="depth_snapshot",
                    raw_text=raw_text, exchange_ts=int(received_at / 1_000_000),
                    collector_session_id=self.collector_session_id,
                    session_seq=self._next_seq(symbol, "depth_snapshot"),
                    received_at=received_at,
                )
                self.producer.produce(env)
                logger.info("depth_resync_complete", symbol=symbol,
                            last_update_id=last_update_id)
                return
            except Exception as e:
                logger.warning("depth_resync_snapshot_failed", symbol=symbol,
                               attempt=attempt + 1, error=str(e))
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)

        # All retries exhausted — emit gap and wait for next periodic snapshot
        logger.error("depth_resync_exhausted", symbol=symbol)
        from src.common.envelope import create_gap_envelope
        import time as _time
        gap = create_gap_envelope(
            exchange=self.exchange, symbol=symbol, stream="depth",
            collector_session_id=self.collector_session_id,
            session_seq=self._next_seq(symbol, "depth"),
            gap_start_ts=_time.time_ns(), gap_end_ts=_time.time_ns(),
            reason="pu_chain_break",
            detail=f"Depth resync failed after {retries} retries, waiting for periodic snapshot",
        )
        self.producer.produce(gap)

    def _emit_disconnect_gaps(self, socket_name: str) -> None:
        """Emit gap records for all symbol/stream combos on this socket."""
        now = time.time_ns()
        # Determine which streams were on this socket
        from src.exchanges.binance import _PUBLIC_STREAMS, _MARKET_STREAMS
        if socket_name == "public":
            affected = _PUBLIC_STREAMS
        elif socket_name == "market":
            affected = _MARKET_STREAMS
        else:
            return

        for symbol in self.symbols:
            for stream in affected:
                if stream not in self.enabled_streams:
                    continue
                gap_start = self._last_received_at.get((symbol, stream), now)
                seq = self._next_seq(symbol, stream)
                gap = create_gap_envelope(
                    exchange=self.exchange,
                    symbol=symbol,
                    stream=stream,
                    collector_session_id=self.collector_session_id,
                    session_seq=seq,
                    gap_start_ts=gap_start,
                    gap_end_ts=now,
                    reason="ws_disconnect",
                    detail=f"WebSocket {socket_name} disconnected",
                )
                self.producer.produce(gap)
                collector_metrics.gaps_detected_total.labels(
                    exchange=self.exchange, symbol=symbol, stream=stream,
                ).inc()
```

- [ ] **Step 2: Commit**

```bash
git add src/collector/connection.py
git commit -m "feat: WebSocket connection manager with reconnect, backoff, and gap emission"
```

---

### Task 3.7: Collector Main Entry Point

**Files:**
- Create: `src/collector/main.py`

- [ ] **Step 1: Implement collector main with health/ready endpoints**

`src/collector/main.py`:
```python
from __future__ import annotations

import asyncio
import signal
import time
from pathlib import Path

import structlog
import uvloop
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.common.config import load_config
from src.common.logging import setup_logging
from src.collector.connection import WebSocketManager
from src.collector.producer import CryptoLakeProducer
from src.collector.snapshot import SnapshotScheduler
from src.collector.streams.trades import TradesHandler
from src.collector.streams.depth import DepthHandler
from src.collector.streams.bookticker import BookTickerHandler
from src.collector.streams.funding_rate import FundingRateHandler
from src.collector.streams.liquidations import LiquidationsHandler
from src.collector.streams.open_interest import OpenInterestPoller
from src.exchanges.binance import BinanceAdapter

logger = structlog.get_logger()


class Collector:
    def __init__(self, config_path: str):
        self.config = load_config(Path(config_path))
        self.exchange_cfg = self.config.exchanges.binance
        self.session_id = f"{self.exchange_cfg.collector_id}_{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
        self.adapter = BinanceAdapter(
            ws_base=self.exchange_cfg.ws_base,
            rest_base=self.exchange_cfg.rest_base,
        )
        self.producer = CryptoLakeProducer(
            brokers=self.config.redpanda.brokers,
            exchange="binance",
            collector_session_id=self.session_id,
        )
        self.enabled_streams = self.exchange_cfg.get_enabled_streams()
        self.symbols = self.exchange_cfg.symbols

        # Build handlers
        self.handlers: dict[str, object] = {}
        if "trades" in self.enabled_streams:
            self.handlers["trades"] = TradesHandler("binance", self.session_id, self.producer)
        if "depth" in self.enabled_streams:
            self.handlers["depth"] = DepthHandler(
                "binance", self.session_id, self.producer, self.adapter, self.symbols,
                on_pu_chain_break=self._on_pu_chain_break)
        if "bookticker" in self.enabled_streams:
            self.handlers["bookticker"] = BookTickerHandler("binance", self.session_id, self.producer)
        if "funding_rate" in self.enabled_streams:
            self.handlers["funding_rate"] = FundingRateHandler("binance", self.session_id, self.producer)
        if "liquidations" in self.enabled_streams:
            self.handlers["liquidations"] = LiquidationsHandler("binance", self.session_id, self.producer)

        # Initialize optional components to None (created conditionally in start())
        self.snapshot_scheduler = None
        self.oi_poller = None

        self.ws_manager = WebSocketManager(
            exchange="binance",
            collector_session_id=self.session_id,
            adapter=self.adapter,
            producer=self.producer,
            handlers=self.handlers,
            symbols=self.symbols,
            enabled_streams=self.enabled_streams,
        )
        # Wire producer overflow to WS backpressure
        self.producer._on_overflow = lambda ex, sym, st: self._on_producer_overflow()

    def _on_producer_overflow(self) -> None:
        self.ws_manager._consecutive_drops += 1

    def _on_pu_chain_break(self, symbol: str) -> None:
        """Callback from DepthHandler when pu chain breaks — triggers depth resync."""
        import asyncio
        loop = asyncio.get_event_loop()
        loop.create_task(self.ws_manager._depth_resync(symbol))

    async def start(self) -> None:
        logger.info("collector_starting", session_id=self.session_id,
                     symbols=self.symbols, streams=self.enabled_streams)
        tasks = []

        # WebSocket streams
        ws_task = asyncio.create_task(self.ws_manager.start())
        tasks.append(ws_task)

        # Snapshot scheduler
        if "depth" in self.enabled_streams:
            depth_handler = self.handlers["depth"]
            self.snapshot_scheduler = SnapshotScheduler(
                exchange="binance",
                collector_session_id=self.session_id,
                producer=self.producer,
                adapter=self.adapter,
                depth_handler=depth_handler,
                symbols=self.symbols,
                default_interval=self.exchange_cfg.depth.snapshot_interval,
                overrides=self.exchange_cfg.depth.snapshot_overrides,
            )
            tasks.append(asyncio.create_task(self.snapshot_scheduler.start()))

        # Open interest poller
        if "open_interest" in self.enabled_streams:
            from src.collector.snapshot import parse_interval_seconds
            self.oi_poller = OpenInterestPoller(
                exchange="binance",
                collector_session_id=self.session_id,
                producer=self.producer,
                adapter=self.adapter,
                symbols=self.symbols,
                poll_interval_seconds=parse_interval_seconds(
                    self.exchange_cfg.open_interest.poll_interval),
            )
            tasks.append(asyncio.create_task(self.oi_poller.start()))

        # Health/metrics HTTP server
        tasks.append(asyncio.create_task(self._start_http()))

        await asyncio.gather(*tasks)

    async def shutdown(self) -> None:
        logger.info("collector_shutting_down")
        await self.ws_manager.stop()
        if self.snapshot_scheduler:
            await self.snapshot_scheduler.stop()
        if self.oi_poller:
            await self.oi_poller.stop()
        self.producer.flush(timeout=10.0)
        logger.info("collector_shutdown_complete")

    async def _start_http(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.config.monitoring.prometheus_port)
        await site.start()

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(self, request: web.Request) -> web.Response:
        checks = {
            "ws_connected": self.ws_manager.is_connected(),
            "producer_connected": self.producer.is_connected(),
        }
        status = 200 if all(checks.values()) else 503
        return web.json_response(checks, status=status)

    async def _metrics(self, request: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST)


def main():
    import os
    setup_logging()
    uvloop.install()

    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    collector = Collector(config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _signal_handler():
        loop.create_task(collector.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(collector.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add src/collector/main.py
git commit -m "feat: collector main entry with health/ready/metrics endpoints"
```

---

### Manual Verification: Chunk 3

1. **Run all unit tests:**
   ```bash
   uv run pytest tests/unit/ -v --tb=short
   ```
   **Check:** All tests pass including gap detector (9 tests), config, envelope, and binance adapter.

2. **Verify gap detector logic manually:**
   ```bash
   uv run python -c "
   from src.collector.gap_detector import DepthGapDetector
   det = DepthGapDetector('btcusdt')
   det.set_sync_point(1000)
   r1 = det.validate_diff(U=999, u=1002, pu=998)
   print('Sync diff valid:', r1.valid)
   r2 = det.validate_diff(U=1003, u=1005, pu=1002)
   print('Chain valid:', r2.valid)
   r3 = det.validate_diff(U=1010, u=1015, pu=1008)
   print('Break detected:', r3.gap, r3.reason)
   "
   ```
   **Check:** Sync diff valid: True, Chain valid: True, Break detected: True pu_chain_break.

3. **Verify all imports resolve (no circular deps):**
   ```bash
   uv run python -c "
   from src.collector.main import Collector
   from src.collector.connection import WebSocketManager
   from src.collector.snapshot import SnapshotScheduler, parse_interval_seconds
   from src.collector.streams.trades import TradesHandler
   from src.collector.streams.depth import DepthHandler
   from src.collector.streams.open_interest import OpenInterestPoller
   print('All collector imports OK')
   print('5m =', parse_interval_seconds('5m'), 'seconds')
   print('30s =', parse_interval_seconds('30s'), 'seconds')
   "
   ```
   **Check:** All imports succeed, 5m = 300 seconds, 30s = 30 seconds.

4. **Verify metrics are defined:**
   ```bash
   uv run python -c "
   from src.collector.metrics import (messages_produced_total, ws_connections_active,
       gaps_detected_total, exchange_latency_ms, snapshots_taken_total, messages_dropped_total)
   print('All collector metrics defined')
   "
   ```
   **Check:** No import errors.

---

## Chunk 4: Writer Core

### Task 4.1: Zstd Compressor (TDD)

**Files:**
- Create: `src/writer/compressor.py`
- Create: `tests/unit/test_compressor.py`

- [ ] **Step 1: Write failing tests for compressor**

`tests/unit/test_compressor.py`:
```python
import zstandard as zstd
import pytest


class TestZstdCompressor:
    def test_compress_single_frame_round_trip(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        lines = [b'{"test": 1}\n', b'{"test": 2}\n']
        frame = comp.compress_frame(lines)

        # Decompress and verify
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(frame)
        assert decompressed == b'{"test": 1}\n{"test": 2}\n'

    def test_compress_empty_input(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame = comp.compress_frame([])
        assert frame == b""  # No data = no frame

    def test_multiple_frames_concatenated(self):
        """Zstd supports concatenated frames — each flush produces a complete frame."""
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame1 = comp.compress_frame([b'{"batch": 1}\n'])
        frame2 = comp.compress_frame([b'{"batch": 2}\n'])
        combined = frame1 + frame2

        # Full decompression of concatenated frames
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(combined)
        decompressed = reader.read()
        assert decompressed == b'{"batch": 1}\n{"batch": 2}\n'

    def test_frame_is_complete_zstd(self):
        """Each frame must be independently decompressible."""
        from src.writer.compressor import ZstdFrameCompressor

        comp = ZstdFrameCompressor(level=3)
        frame = comp.compress_frame([b'{"x": 1}\n'])

        dctx = zstd.ZstdDecompressor()
        result = dctx.decompress(frame)
        assert result == b'{"x": 1}\n'

    def test_configurable_compression_level(self):
        from src.writer.compressor import ZstdFrameCompressor

        comp1 = ZstdFrameCompressor(level=1)
        comp9 = ZstdFrameCompressor(level=9)
        data = [b'{"key": "value"}\n'] * 100
        frame1 = comp1.compress_frame(data)
        frame9 = comp9.compress_frame(data)
        # Higher level should compress better (or at least equal)
        assert len(frame9) <= len(frame1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_compressor.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement compressor**

`src/writer/compressor.py`:
```python
from __future__ import annotations

import zstandard as zstd


class ZstdFrameCompressor:
    """Produces complete, independent zstd frames for crash-safe file writes.

    Each call to compress_frame() returns a self-contained zstd frame.
    Concatenating multiple frames produces a valid zstd file (per spec).
    On crash recovery, the file can be truncated at any frame boundary.
    """

    def __init__(self, level: int = 3):
        self._cctx = zstd.ZstdCompressor(level=level)

    def compress_frame(self, lines: list[bytes]) -> bytes:
        if not lines:
            return b""
        data = b"".join(lines)
        return self._cctx.compress(data)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_compressor.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/compressor.py tests/unit/test_compressor.py
git commit -m "feat: zstd frame compressor with crash-safe concatenation"
```

---

### Task 4.2: File Rotator (TDD)

**Files:**
- Create: `src/writer/file_rotator.py`
- Create: `tests/unit/test_file_rotator.py`

- [ ] **Step 1: Write failing tests for file rotator**

`tests/unit/test_file_rotator.py`:
```python
import hashlib
import pytest
from pathlib import Path


class TestFilePathGeneration:
    def test_data_file_path(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-11",
            hour=14,
        )
        assert path == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst")

    def test_late_file_path(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="btcusdt",
            stream="trades",
            date="2026-03-11",
            hour=14,
            late_seq=1,
        )
        assert path == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.late-1.jsonl.zst")

    def test_path_all_lowercase(self):
        from src.writer.file_rotator import build_file_path

        path = build_file_path(
            base_dir="/data",
            exchange="binance",
            symbol="BTCUSDT",
            stream="depth_snapshot",
            date="2026-03-11",
            hour=0,
        )
        assert "BTCUSDT" not in str(path)
        assert "btcusdt" in str(path)

    def test_sha256_sidecar_path(self):
        from src.writer.file_rotator import build_file_path, sidecar_path

        data_path = build_file_path("/data", "binance", "btcusdt", "trades", "2026-03-11", 14)
        sc = sidecar_path(data_path)
        assert sc == Path("/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst.sha256")


class TestFileTarget:
    def test_target_key(self):
        from src.writer.file_rotator import FileTarget

        t = FileTarget(exchange="binance", symbol="btcusdt", stream="trades",
                       date="2026-03-11", hour=14)
        assert t.key == ("binance", "btcusdt", "trades", "2026-03-11", 14)


class TestSHA256Sidecar:
    def test_compute_and_write_sha256(self, tmp_path):
        from src.writer.file_rotator import compute_sha256, write_sha256_sidecar

        # Write a test file
        test_file = tmp_path / "test.jsonl.zst"
        test_file.write_bytes(b"test data content")

        digest = compute_sha256(test_file)
        expected = hashlib.sha256(b"test data content").hexdigest()
        assert digest == expected

        sc_path = tmp_path / "test.jsonl.zst.sha256"
        write_sha256_sidecar(test_file, sc_path)
        assert sc_path.read_text().strip().startswith(expected)

    def test_sidecar_format(self, tmp_path):
        from src.writer.file_rotator import write_sha256_sidecar, compute_sha256

        test_file = tmp_path / "hour-14.jsonl.zst"
        test_file.write_bytes(b"content")
        sc_path = tmp_path / "hour-14.jsonl.zst.sha256"
        write_sha256_sidecar(test_file, sc_path)

        content = sc_path.read_text().strip()
        # Format: "<hash>  <filename>"
        parts = content.split("  ")
        assert len(parts) == 2
        assert parts[1] == "hour-14.jsonl.zst"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_file_rotator.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement file rotator**

`src/writer/file_rotator.py`:
```python
from __future__ import annotations

import datetime
import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FileTarget:
    exchange: str
    symbol: str
    stream: str
    date: str
    hour: int

    @property
    def key(self) -> tuple:
        return (self.exchange, self.symbol, self.stream, self.date, self.hour)


def build_file_path(
    base_dir: str,
    exchange: str,
    symbol: str,
    stream: str,
    date: str,
    hour: int,
    late_seq: int | None = None,
) -> Path:
    symbol = symbol.lower()
    name = f"hour-{hour:02d}"
    if late_seq is not None:
        name += f".late-{late_seq}"
    name += ".jsonl.zst"
    return Path(base_dir) / exchange / symbol / stream / date / name


def sidecar_path(data_path: Path) -> Path:
    return data_path.with_suffix(data_path.suffix + ".sha256")


def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def write_sha256_sidecar(data_path: Path, sc_path: Path) -> None:
    digest = compute_sha256(data_path)
    sc_path.write_text(f"{digest}  {data_path.name}\n")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_file_rotator.py -v`
Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/file_rotator.py tests/unit/test_file_rotator.py
git commit -m "feat: file rotator with path generation and SHA-256 sidecar"
```

---

### Task 4.3: PostgreSQL State Manager (TDD)

**Files:**
- Create: `src/writer/state_manager.py`
- Create: `tests/unit/test_state_manager.py`

- [ ] **Step 1: Write failing tests for state manager**

`tests/unit/test_state_manager.py`:
```python
import pytest


class TestWriterState:
    def test_state_record_creation(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades",
            partition=0,
            high_water_offset=1000,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=52428,
        )
        assert state.topic == "binance.trades"
        assert state.partition == 0
        assert state.high_water_offset == 1000
        assert state.file_byte_size == 52428

    def test_state_key(self):
        from src.writer.state_manager import FileState

        state = FileState(
            topic="binance.trades", partition=0,
            high_water_offset=100,
            file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
            file_byte_size=0,
        )
        assert state.state_key == ("binance.trades", 0, "/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst")

    def test_multiple_files_same_topic_partition(self):
        """Multiple files for same (topic, partition) must have distinct state keys."""
        from src.writer.state_manager import FileState

        s1 = FileState(topic="binance.trades", partition=0, high_water_offset=100,
                       file_path="/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=1000)
        s2 = FileState(topic="binance.trades", partition=0, high_water_offset=200,
                       file_path="/data/binance/ethusdt/trades/2026-03-11/hour-14.jsonl.zst",
                       file_byte_size=2000)
        assert s1.state_key != s2.state_key


class TestStateManagerSQL:
    """These tests verify SQL generation. Full integration is in test_writer_rotation.py."""

    def test_create_table_sql(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.CREATE_TABLE_SQL
        assert "writer_file_state" in sql
        assert "topic" in sql
        assert "partition" in sql
        assert "file_path" in sql
        assert "high_water_offset" in sql
        assert "file_byte_size" in sql

    def test_upsert_sql_file_path_in_pk(self):
        from src.writer.state_manager import StateManager

        sql = StateManager.UPSERT_SQL
        assert "INSERT" in sql
        assert "ON CONFLICT" in sql
        assert "file_path" in sql
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_state_manager.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement state manager**

`src/writer/state_manager.py`:
```python
from __future__ import annotations

from dataclasses import dataclass

import structlog

logger = structlog.get_logger()


@dataclass
class FileState:
    topic: str
    partition: int
    high_water_offset: int
    file_path: str
    file_byte_size: int

    @property
    def state_key(self) -> tuple[str, int, str]:
        return (self.topic, self.partition, self.file_path)


class StateManager:
    """Manages writer state in PostgreSQL for exactly-once archive semantics.

    Tracks the highest flushed Kafka offset and file byte size per (topic, partition, file_path).
    PK includes file_path so that multiple files for the same (topic, partition) — e.g.,
    different symbols or hours — each get independent state rows for correct truncation on restart.
    """

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS writer_file_state (
        topic TEXT NOT NULL,
        partition INTEGER NOT NULL,
        file_path TEXT NOT NULL,
        high_water_offset BIGINT NOT NULL,
        file_byte_size BIGINT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (topic, partition, file_path)
    );
    """

    UPSERT_SQL = """
    INSERT INTO writer_file_state (topic, partition, file_path, high_water_offset, file_byte_size, updated_at)
    VALUES (%(topic)s, %(partition)s, %(file_path)s, %(high_water_offset)s, %(file_byte_size)s, NOW())
    ON CONFLICT (topic, partition, file_path)
    DO UPDATE SET
        high_water_offset = EXCLUDED.high_water_offset,
        file_byte_size = EXCLUDED.file_byte_size,
        updated_at = NOW();
    """

    LOAD_ALL_SQL = """
    SELECT topic, partition, high_water_offset, file_path, file_byte_size
    FROM writer_file_state;
    """

    def __init__(self, db_url: str):
        self._db_url = db_url
        self._conn = None

    async def connect(self) -> None:
        import psycopg

        self._conn = await psycopg.AsyncConnection.connect(self._db_url)
        async with self._conn.cursor() as cur:
            await cur.execute(self.CREATE_TABLE_SQL)
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    async def load_all_states(self) -> dict[tuple[str, int, str], FileState]:
        states: dict[tuple[str, int, str], FileState] = {}
        async with self._conn.cursor() as cur:
            await cur.execute(self.LOAD_ALL_SQL)
            rows = await cur.fetchall()
            for row in rows:
                state = FileState(
                    topic=row[0],
                    partition=row[1],
                    high_water_offset=row[2],
                    file_path=row[3],
                    file_byte_size=row[4],
                )
                states[state.state_key] = state
        return states

    async def _ensure_connected(self) -> None:
        """Reconnect to PostgreSQL if the connection is closed or broken."""
        import psycopg
        try:
            # Lightweight check — execute a no-op
            async with self._conn.cursor() as cur:
                await cur.execute("SELECT 1")
        except (psycopg.OperationalError, psycopg.InterfaceError, Exception):
            logger.warning("pg_reconnecting")
            try:
                await self._conn.close()
            except Exception:
                pass
            self._conn = await psycopg.AsyncConnection.connect(self._db_url)

    async def save_states(self, states: list[FileState]) -> None:
        """Atomically save multiple file states in a single transaction.

        Retries up to 3 times on connection errors. If all retries fail,
        raises the exception — the writer will crash and resume from PG state on restart.
        """
        import asyncio
        max_retries = 3
        for attempt in range(max_retries):
            try:
                await self._ensure_connected()
                async with self._conn.transaction():
                    async with self._conn.cursor() as cur:
                        for state in states:
                            await cur.execute(self.UPSERT_SQL, {
                                "topic": state.topic,
                                "partition": state.partition,
                                "high_water_offset": state.high_water_offset,
                                "file_path": state.file_path,
                                "file_byte_size": state.file_byte_size,
                            })
                logger.debug("state_saved", count=len(states))
                return
            except Exception as e:
                logger.warning("pg_save_failed", attempt=attempt + 1, error=str(e))
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_state_manager.py -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/state_manager.py tests/unit/test_state_manager.py
git commit -m "feat: PostgreSQL state manager for writer exactly-once semantics"
```

---

### Task 4.4: Buffer Manager (TDD)

**Files:**
- Create: `src/writer/buffer_manager.py`
- Create: `tests/unit/test_buffer_manager.py`

- [ ] **Step 1: Write failing tests for buffer manager**

`tests/unit/test_buffer_manager.py`:
```python
import pytest
import orjson


class TestBufferManager:
    def _make_envelope(self, exchange="binance", symbol="btcusdt", stream="trades",
                       received_at=1741689600_000_000_000, offset=0):
        """Helper to create a minimal envelope dict."""
        return {
            "v": 1, "type": "data", "exchange": exchange, "symbol": symbol,
            "stream": stream, "received_at": received_at,
            "exchange_ts": 100, "collector_session_id": "s", "session_seq": 0,
            "raw_text": "{}", "raw_sha256": "abc",
            "_topic": f"{exchange}.{stream}", "_partition": 0, "_offset": offset,
        }

    def test_add_message_to_buffer(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=30)
        env = self._make_envelope()
        bm.add(env)
        assert bm.total_buffered == 1

    def test_flush_threshold_triggers(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=2, flush_interval_seconds=9999)
        env1 = self._make_envelope(offset=0)
        env2 = self._make_envelope(offset=1)
        result1 = bm.add(env1)
        assert result1 is None  # no flush yet
        result2 = bm.add(env2)
        assert result2 is not None  # flush triggered
        assert len(result2) > 0

    def test_routing_by_received_at(self):
        """Messages route to file targets based on received_at timestamp, not wall clock."""
        from src.writer.buffer_manager import BufferManager
        import datetime

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        # Hour 14
        dt14 = datetime.datetime(2026, 3, 11, 14, 30, tzinfo=datetime.timezone.utc)
        ns14 = int(dt14.timestamp() * 1_000_000_000)
        # Hour 15
        dt15 = datetime.datetime(2026, 3, 11, 15, 10, tzinfo=datetime.timezone.utc)
        ns15 = int(dt15.timestamp() * 1_000_000_000)

        env14 = self._make_envelope(received_at=ns14, offset=0)
        env15 = self._make_envelope(received_at=ns15, offset=1)
        bm.add(env14)
        bm.add(env15)
        # Two different file targets
        assert len(bm._buffers) == 2

    def test_flush_all(self):
        from src.writer.buffer_manager import BufferManager

        bm = BufferManager(base_dir="/data", flush_messages=100, flush_interval_seconds=9999)
        for i in range(5):
            bm.add(self._make_envelope(offset=i))
        results = bm.flush_all()
        assert len(results) == 1  # all same target
        assert bm.total_buffered == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_buffer_manager.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement buffer manager**

`src/writer/buffer_manager.py`:
```python
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from pathlib import Path

import orjson
import structlog

from src.writer.file_rotator import FileTarget, build_file_path

logger = structlog.get_logger()


@dataclass
class FlushResult:
    target: FileTarget
    file_path: Path
    lines: list[bytes]
    high_water_offset: int
    partition: int
    count: int


class BufferManager:
    """Routes incoming envelopes to per-file buffers and triggers flushes."""

    def __init__(self, base_dir: str, flush_messages: int = 10_000,
                 flush_interval_seconds: int = 30):
        self.base_dir = base_dir
        self.flush_messages = flush_messages
        self.flush_interval_seconds = flush_interval_seconds
        self._buffers: dict[tuple, list[dict]] = {}
        self.total_buffered = 0

    def add(self, envelope: dict) -> list[FlushResult] | None:
        """Add an envelope to the appropriate buffer. Returns flush results if threshold hit."""
        target = self._route(envelope)
        key = target.key
        if key not in self._buffers:
            self._buffers[key] = []
        self._buffers[key].append(envelope)
        self.total_buffered += 1

        if len(self._buffers[key]) >= self.flush_messages:
            return self._flush_buffer(key, target)
        return None

    def flush_key(self, file_key: tuple[str, str, str]) -> list[FlushResult]:
        """Flush buffers matching a (exchange, symbol, stream) prefix."""
        exchange, symbol, stream = file_key
        results: list[FlushResult] = []
        for key in list(self._buffers.keys()):
            if key[0] == exchange and key[1] == symbol and key[2] == stream:
                if self._buffers[key]:
                    target = FileTarget(*key)
                    results.extend(self._flush_buffer(key, target))
        return results

    def flush_all(self) -> list[FlushResult]:
        """Flush all non-empty buffers."""
        results: list[FlushResult] = []
        for key in list(self._buffers.keys()):
            if self._buffers[key]:
                target = FileTarget(*key)
                results.extend(self._flush_buffer(key, target))
        return results

    def _flush_buffer(self, key: tuple, target: FileTarget) -> list[FlushResult]:
        messages = self._buffers.pop(key, [])
        if not messages:
            return []
        self.total_buffered -= len(messages)

        lines = [orjson.dumps(env) + b"\n" for env in messages]
        high_water = max(m["_offset"] for m in messages)
        partition = messages[0]["_partition"]  # all messages in same buffer share partition
        file_path = build_file_path(
            self.base_dir, target.exchange, target.symbol,
            target.stream, target.date, target.hour,
        )
        return [FlushResult(
            target=target,
            file_path=file_path,
            lines=lines,
            high_water_offset=high_water,
            partition=partition,
            count=len(messages),
        )]

    def _route(self, envelope: dict) -> FileTarget:
        received_at_ns = envelope["received_at"]
        dt = datetime.datetime.fromtimestamp(
            received_at_ns / 1_000_000_000, tz=datetime.timezone.utc
        )
        return FileTarget(
            exchange=envelope["exchange"],
            symbol=envelope["symbol"],
            stream=envelope["stream"],
            date=dt.strftime("%Y-%m-%d"),
            hour=dt.hour,
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_buffer_manager.py -v`
Expected: All 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/writer/buffer_manager.py tests/unit/test_buffer_manager.py
git commit -m "feat: buffer manager with received_at routing and flush thresholds"
```

---

### Task 4.5: Writer Metrics

**Files:**
- Create: `src/writer/metrics.py`

- [ ] **Step 1: Define all writer metrics**

`src/writer/metrics.py`:
```python
from prometheus_client import Counter, Gauge, Histogram

messages_consumed_total = Counter(
    "writer_messages_consumed_total",
    "Messages read from Redpanda",
    ["exchange", "symbol", "stream"],
)

consumer_lag = Gauge(
    "writer_consumer_lag",
    "Messages behind head (gap proxy)",
    ["exchange", "stream"],
)

files_rotated_total = Counter(
    "writer_files_rotated_total",
    "File rotations completed",
    ["exchange", "symbol", "stream"],
)

bytes_written_total = Counter(
    "writer_bytes_written_total",
    "Compressed bytes to disk",
    ["exchange", "symbol", "stream"],
)

compression_ratio = Gauge(
    "writer_compression_ratio",
    "Raw / compressed size ratio",
    ["exchange", "stream"],
)

disk_usage_bytes = Gauge(
    "writer_disk_usage_bytes",
    "Total storage consumed",
)

disk_usage_pct = Gauge(
    "writer_disk_usage_pct",
    "Percentage of volume used",
)

flush_duration_ms = Histogram(
    "writer_flush_duration_ms",
    "Time to flush buffer to disk (ms)",
    ["exchange", "stream"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
)
```

- [ ] **Step 2: Commit**

```bash
git add src/writer/metrics.py
git commit -m "feat: writer Prometheus metric definitions"
```

---

### Task 4.6: Writer Consumer & Main Entry Point

**Files:**
- Create: `src/writer/consumer.py`
- Create: `src/writer/main.py`

- [ ] **Step 1: Implement Kafka consumer wrapper**

`src/writer/consumer.py`:
```python
from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

import structlog

from src.common.envelope import add_broker_coordinates, deserialize_envelope
from src.writer import metrics as writer_metrics
from src.writer.buffer_manager import BufferManager, FlushResult
from src.writer.compressor import ZstdFrameCompressor
from src.writer.file_rotator import (
    build_file_path, compute_sha256, sidecar_path, write_sha256_sidecar,
)
from src.writer.state_manager import FileState, StateManager

logger = structlog.get_logger()


class WriterConsumer:
    """Consumes from Redpanda, buffers, compresses, and writes archive files.

    Key design decisions per spec:
    - Consumer.poll() runs in run_in_executor to avoid blocking the async event loop (spec 8.2)
    - Hourly rotation seals files with SHA-256 sidecar (spec 8.4)
    - Sealed files are never reopened; late arrivals go to spillover files (spec 8.2)
    - Flush sequence: write → fsync → PG state → commit offsets (spec 8.3)
    """

    def __init__(
        self,
        brokers: list[str],
        topics: list[str],
        group_id: str,
        buffer_manager: BufferManager,
        compressor: ZstdFrameCompressor,
        state_manager: StateManager,
        base_dir: str,
    ):
        self.brokers = brokers
        self.topics = topics
        self.group_id = group_id
        self.buffer_manager = buffer_manager
        self.compressor = compressor
        self.state_manager = state_manager
        self.base_dir = base_dir
        self._consumer = None
        self._running = False
        self._assigned = False  # True once consumer receives partition assignment
        self._sealed_files: set[Path] = set()  # tracks sealed .jsonl.zst paths
        self._late_seq: dict[Path, int] = {}  # late-arrival sequence counters

    async def start(self) -> None:
        from confluent_kafka import Consumer as KafkaConsumer

        self._consumer = KafkaConsumer({
            "bootstrap.servers": ",".join(self.brokers),
            "group.id": self.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        })

        # Load state and seek to last known offsets
        states = await self.state_manager.load_all_states()
        self._recover_files(states)

        # Discover already-sealed files (those with .sha256 sidecar)
        self._discover_sealed_files()

        def _on_assign(consumer, partitions):
            self._assigned = True
            self._assigned_partitions = set(
                (tp.topic, tp.partition) for tp in partitions
            )
            logger.info("consumer_partitions_assigned", count=len(partitions))

        def _on_revoke(consumer, partitions):
            revoked = set((tp.topic, tp.partition) for tp in partitions)
            logger.critical("consumer_partitions_revoked",
                            revoked=list(revoked),
                            detail="Writer exclusivity violated — another consumer "
                                   "joined the group or rebalance occurred")
            # Flush any buffered data before losing ownership
            # (best-effort — we may lose the race)
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self._flush_and_commit())
            self._assigned = False
            # Crash the writer to prevent split-brain writes (spec 8.2)
            raise RuntimeError(
                f"Writer exclusivity violated: partitions {revoked} revoked. "
                "Another writer may have joined consumer group. Shutting down."
            )

        self._consumer.subscribe(self.topics, on_assign=_on_assign,
                                 on_revoke=_on_revoke)
        self._running = True

        # Collapse to max offset per (topic, partition) since state is now keyed per file
        seek_map: dict[tuple[str, int], int] = {}
        for s in states.values():
            key = (s.topic, s.partition)
            seek_map[key] = max(seek_map.get(key, 0), s.high_water_offset + 1)
        self._pending_seeks = seek_map

    def _recover_files(self, states: dict) -> None:
        """Truncate files to PostgreSQL-recorded byte sizes on startup."""
        for state in states.values():
            path = Path(state.file_path)
            if path.exists():
                actual_size = path.stat().st_size
                if actual_size > state.file_byte_size:
                    logger.warning("truncating_file", path=str(path),
                                   actual=actual_size, expected=state.file_byte_size)
                    with open(path, "r+b") as f:
                        f.truncate(state.file_byte_size)

    def _discover_sealed_files(self) -> None:
        """Scan base_dir for files that already have .sha256 sidecars."""
        base = Path(self.base_dir)
        if base.exists():
            for sc in base.rglob("*.jsonl.zst.sha256"):
                data_path = sc.with_suffix("")  # remove .sha256
                self._sealed_files.add(data_path)

    def _resolve_file_path(self, file_path: Path) -> Path:
        """If file_path is sealed (has sidecar), return a late-arrival spillover path."""
        if file_path in self._sealed_files or sidecar_path(file_path).exists():
            self._sealed_files.add(file_path)
            seq = self._late_seq.get(file_path, 0) + 1
            self._late_seq[file_path] = seq
            # Build late path: hour-14.late-1.jsonl.zst
            stem = file_path.stem.split(".")[0]  # "hour-14"
            late_name = f"{stem}.late-{seq}.jsonl.zst"
            return file_path.parent / late_name
        return file_path

    async def consume_loop(self) -> None:
        """Main consume loop. Polls in executor, buffers, flushes, and commits."""
        loop = asyncio.get_event_loop()
        last_flush_time = time.monotonic()
        # Track active (hour, date) per file key (exchange, symbol, stream).
        # Stores the previous message's hour and date so that at day boundaries
        # (23→00) we seal the correct previous-day file, not the new day's.
        active_hours: dict[tuple[str, str, str], tuple[int, str]] = {}

        while self._running:
            # Non-blocking poll via executor (spec 8.2: avoid blocking uvloop)
            msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)

            # Handle pending seeks on assignment
            if self._pending_seeks:
                assignments = self._consumer.assignment()
                for tp in assignments:
                    key = (tp.topic, tp.partition)
                    if key in self._pending_seeks:
                        from confluent_kafka import TopicPartition
                        self._consumer.seek(
                            TopicPartition(tp.topic, tp.partition, self._pending_seeks[key])
                        )
                        logger.info("consumer_seek", topic=tp.topic,
                                    partition=tp.partition,
                                    offset=self._pending_seeks[key])
                self._pending_seeks = {}

            if msg is None:
                if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:
                    await self._flush_and_commit()
                    last_flush_time = time.monotonic()
                continue

            if msg.error():
                logger.error("consumer_error", error=str(msg.error()))
                continue

            # Deserialize and stamp broker coordinates
            envelope = deserialize_envelope(msg.value())
            envelope = add_broker_coordinates(
                envelope,
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

            writer_metrics.messages_consumed_total.labels(
                exchange=envelope.get("exchange", ""),
                symbol=envelope.get("symbol", ""),
                stream=envelope.get("stream", ""),
            ).inc()

            # Per-file hourly rotation (spec 8.2: file routing by message received_at)
            import datetime
            msg_dt = datetime.datetime.fromtimestamp(
                envelope["received_at"] / 1_000_000_000,
                tz=datetime.timezone.utc,
            )
            current_hour = msg_dt.hour
            current_date = msg_dt.strftime("%Y-%m-%d")
            file_key = (envelope.get("exchange", ""),
                        envelope.get("symbol", ""),
                        envelope.get("stream", ""))
            prev = active_hours.get(file_key)
            if prev is not None:
                prev_hour, prev_date = prev
                if current_hour != prev_hour or current_date != prev_date:
                    # Seal previous file using the PREVIOUS date/hour,
                    # not the current message's date (critical at 23→00 day boundary)
                    await self._rotate_file(file_key, prev_date, prev_hour)
            active_hours[file_key] = (current_hour, current_date)

            # Add to buffer — may trigger flush
            flush_results = self.buffer_manager.add(envelope)
            if flush_results:
                await self._write_and_save(flush_results)
                last_flush_time = time.monotonic()

    async def _rotate_file(
        self,
        file_key: tuple[str, str, str],
        date_str: str,
        hour: int,
    ) -> None:
        """Seal files for a specific stream that has crossed an hour boundary.
        Order per spec 8.4: flush → write to disk → seal (.sha256 sidecar) →
        save PG state → commit Kafka offsets."""
        exchange, symbol, stream = file_key
        logger.info("hourly_rotation_triggered", exchange=exchange,
                     symbol=symbol, stream=stream, hour=hour)

        # 1. Flush buffer for this specific file key
        results = self.buffer_manager.flush_key(file_key)

        start = time.monotonic()
        states: list[FileState] = []

        # 2. Write to disk + fsync (no offset commit yet)
        if results:
            states = self._write_to_disk(results)

        # 3. Seal all flushed file paths BEFORE committing offsets
        files_to_seal = set()
        if results:
            for r in results:
                files_to_seal.add(r.file_path)
        # Always include the explicitly targeted file
        files_to_seal.add(build_file_path(
            self.base_dir, exchange, symbol, stream, date_str, hour))
        for file_path in files_to_seal:
            sc = sidecar_path(file_path)
            if file_path.exists() and not sc.exists() and file_path.stat().st_size > 0:
                write_sha256_sidecar(file_path, sc)
                self._sealed_files.add(file_path)
                writer_metrics.files_rotated_total.labels(
                    exchange=exchange, symbol=symbol, stream=stream,
                ).inc()
                logger.info("file_sealed", path=str(file_path))

        # 4. Now safe to commit — sidecar is durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _rotate_hour(self) -> None:
        """Seal all active files (used during shutdown). For normal operation,
        use _rotate_file() which seals per-stream files individually.
        Order: flush → write to disk → seal all → commit offsets (spec 8.4)."""
        logger.info("rotation_seal_all")
        start = time.monotonic()

        # 1. Flush all buffers and write to disk (no offset commit yet)
        results = self.buffer_manager.flush_all()
        states: list[FileState] = []
        if results:
            states = self._write_to_disk(results)

        # 2. Seal all active files BEFORE committing offsets
        base = Path(self.base_dir)
        for zst_file in base.rglob("*.jsonl.zst"):
            sc = sidecar_path(zst_file)
            if not sc.exists() and zst_file.stat().st_size > 0:
                write_sha256_sidecar(zst_file, sc)
                self._sealed_files.add(zst_file)
                writer_metrics.files_rotated_total.labels(
                    exchange="binance",
                    symbol=zst_file.parent.parent.parent.name,
                    stream=zst_file.parent.parent.name,
                ).inc()
                logger.info("file_sealed", path=str(zst_file))

        # 3. Now safe to commit — all sidecars are durable on disk
        if results:
            await self._commit_state(states, results, start)

    async def _flush_and_commit(self) -> None:
        results = self.buffer_manager.flush_all()
        if results:
            await self._write_and_save(results)

    def _write_to_disk(self, results: list[FlushResult]) -> list[FileState]:
        """Write compressed frames to disk and fsync. Returns FileState list
        for later commit. Does NOT save PG state or commit Kafka offsets."""
        states: list[FileState] = []
        for result in results:
            file_path = self._resolve_file_path(result.file_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)

            compressed = self.compressor.compress_frame(result.lines)
            with open(file_path, "ab") as f:
                f.write(compressed)
                f.flush()
                os.fsync(f.fileno())

            file_size = file_path.stat().st_size
            writer_metrics.bytes_written_total.labels(
                exchange=result.target.exchange,
                symbol=result.target.symbol,
                stream=result.target.stream,
            ).inc(len(compressed))

            states.append(FileState(
                topic=f"{result.target.exchange}.{result.target.stream}",
                partition=result.partition,
                high_water_offset=result.high_water_offset,
                file_path=str(file_path),
                file_byte_size=file_size,
            ))
        return states

    async def _commit_state(
        self,
        states: list[FileState],
        results: list[FlushResult],
        start: float,
    ) -> None:
        """Save file states to PG and commit Kafka offsets.
        Called after writes (and optional sealing) are complete."""
        await self.state_manager.save_states(states)
        self._consumer.commit(asynchronous=True)

        elapsed_ms = (time.monotonic() - start) * 1000
        for result in results:
            writer_metrics.flush_duration_ms.labels(
                exchange=result.target.exchange,
                stream=result.target.stream,
            ).observe(elapsed_ms)
        logger.debug("flush_complete", files=len(results), elapsed_ms=round(elapsed_ms, 1))

    async def _write_and_save(self, results: list[FlushResult]) -> None:
        """Normal flush: write files → fsync → save state to PG → commit offsets.
        For rotation (seal-before-commit), use _write_to_disk + seal + _commit_state."""
        start = time.monotonic()
        states = self._write_to_disk(results)
        await self._commit_state(states, results, start)

    def is_connected(self) -> bool:
        """True if consumer exists and has been assigned partitions."""
        return self._consumer is not None and self._assigned

    async def stop(self) -> None:
        self._running = False
        # _rotate_hour handles flush → write → seal → commit in correct order
        await self._rotate_hour()
        if self._consumer:
            self._consumer.close()
```

- [ ] **Step 2: Implement writer main entry**

`src/writer/main.py`:
```python
from __future__ import annotations

import asyncio
import os
import signal
from pathlib import Path

import structlog
import uvloop
from aiohttp import web
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from src.common.config import load_config
from src.common.logging import setup_logging
from src.writer.buffer_manager import BufferManager
from src.writer.compressor import ZstdFrameCompressor
from src.writer.consumer import WriterConsumer
from src.writer.state_manager import StateManager

logger = structlog.get_logger()


class Writer:
    def __init__(self, config_path: str):
        self.config = load_config(Path(config_path))
        exchange_cfg = self.config.exchanges.binance
        self.enabled_streams = exchange_cfg.get_enabled_streams()
        if exchange_cfg.writer_streams_override:
            self.enabled_streams = exchange_cfg.writer_streams_override

        self.topics = [f"binance.{s}" for s in self.enabled_streams]
        self.state_manager = StateManager(self.config.database.url)
        self.compressor = ZstdFrameCompressor(level=self.config.writer.compression_level)
        self.buffer_manager = BufferManager(
            base_dir=self.config.writer.base_dir,
            flush_messages=self.config.writer.flush_messages,
            flush_interval_seconds=self.config.writer.flush_interval_seconds,
        )
        self.consumer = WriterConsumer(
            brokers=self.config.redpanda.brokers,
            topics=self.topics,
            group_id="cryptolake-writer",
            buffer_manager=self.buffer_manager,
            compressor=self.compressor,
            state_manager=self.state_manager,
            base_dir=self.config.writer.base_dir,
        )

    async def start(self) -> None:
        logger.info("writer_starting", topics=self.topics)
        await self.state_manager.connect()
        await self.consumer.start()
        await asyncio.gather(
            self.consumer.consume_loop(),
            self._start_http(),
        )

    async def shutdown(self) -> None:
        logger.info("writer_shutting_down")
        await self.consumer.stop()
        await self.state_manager.close()
        logger.info("writer_shutdown_complete")

    async def _start_http(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        runner = web.AppRunner(app)
        await runner.setup()
        port = self.config.monitoring.prometheus_port + 1  # writer on 8001
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(self, request: web.Request) -> web.Response:
        checks = {"consumer_connected": self.consumer.is_connected(), "storage_writable": True}
        try:
            Path(self.config.writer.base_dir).mkdir(parents=True, exist_ok=True)
            test_file = Path(self.config.writer.base_dir) / ".write_test"
            test_file.write_text("ok")
            test_file.unlink()
        except Exception:
            checks["storage_writable"] = False
        status = 200 if all(checks.values()) else 503
        return web.json_response(checks, status=status)

    async def _metrics(self, request: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST)


def main():
    setup_logging()
    uvloop.install()

    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    writer = Writer(config_path)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _signal_handler():
        loop.create_task(writer.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        loop.run_until_complete(writer.start())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Commit**

```bash
git add src/writer/consumer.py src/writer/main.py src/writer/metrics.py
git commit -m "feat: writer consumer, main entry, and metrics"
```

---

### Manual Verification: Chunk 4

1. **Run all unit tests:**
   ```bash
   uv run pytest tests/unit/ -v --tb=short
   ```
   **Check:** All tests pass — compressor (5), file rotator (7), state manager (4), buffer manager (4), plus all prior.

2. **Verify compressor round-trip manually:**
   ```bash
   uv run python -c "
   from src.writer.compressor import ZstdFrameCompressor
   import zstandard as zstd
   comp = ZstdFrameCompressor(level=3)
   frame = comp.compress_frame([b'{\"trade\": 1}\n', b'{\"trade\": 2}\n'])
   print('Compressed size:', len(frame), 'bytes')
   dctx = zstd.ZstdDecompressor()
   decompressed = dctx.decompress(frame)
   print('Decompressed:', decompressed.decode())
   "
   ```
   **Check:** Compressed is smaller than raw, decompressed matches original JSON lines.

3. **Verify file path generation:**
   ```bash
   uv run python -c "
   from src.writer.file_rotator import build_file_path, sidecar_path
   p = build_file_path('/data', 'binance', 'btcusdt', 'trades', '2026-03-11', 14)
   print('Data path:', p)
   print('Sidecar:', sidecar_path(p))
   late = build_file_path('/data', 'binance', 'btcusdt', 'trades', '2026-03-11', 14, late_seq=1)
   print('Late file:', late)
   "
   ```
   **Check:** `/data/binance/btcusdt/trades/2026-03-11/hour-14.jsonl.zst`, sidecar has `.sha256` suffix, late file has `.late-1` in name.

4. **Verify buffer routing:**
   ```bash
   uv run python -c "
   from src.writer.buffer_manager import BufferManager
   import datetime
   bm = BufferManager(base_dir='/data', flush_messages=100, flush_interval_seconds=30)
   dt = datetime.datetime(2026, 3, 11, 14, 30, tzinfo=datetime.timezone.utc)
   ns = int(dt.timestamp() * 1_000_000_000)
   env = {'v': 1, 'type': 'data', 'exchange': 'binance', 'symbol': 'btcusdt',
          'stream': 'trades', 'received_at': ns, 'exchange_ts': 100,
          '_topic': 'binance.trades', '_partition': 0, '_offset': 0,
          'collector_session_id': 's', 'session_seq': 0,
          'raw_text': '{}', 'raw_sha256': 'x'}
   bm.add(env)
   print('Buffers:', len(bm._buffers))
   print('Target key:', list(bm._buffers.keys())[0])
   results = bm.flush_all()
   print('Flush results:', len(results))
   print('File path:', results[0].file_path)
   "
   ```
   **Check:** 1 buffer, target key includes date and hour 14, file path is correct.

5. **Verify all writer imports resolve:**
   ```bash
   uv run python -c "
   from src.writer.main import Writer
   from src.writer.consumer import WriterConsumer
   from src.writer.buffer_manager import BufferManager
   from src.writer.compressor import ZstdFrameCompressor
   from src.writer.state_manager import StateManager
   from src.writer.file_rotator import build_file_path
   print('All writer imports OK')
   "
   ```
   **Check:** No import errors.

---

## Chunk 5: Verification CLI

### Task 5.1: Verify CLI — Checksum, Envelope, Dedup, Gaps, Manifest (TDD)

**Files:**
- Create: `src/cli/verify.py`
- Create: `tests/unit/test_verify.py`

- [ ] **Step 1: Write failing tests**

`tests/unit/test_verify.py`:
```python
import hashlib
import pytest
import zstandard as zstd
import orjson
from pathlib import Path


def _make_data_envelope(offset, raw_text='{"test":1}'):
    raw_sha = hashlib.sha256(raw_text.encode()).hexdigest()
    return {
        "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
        "stream": "trades", "received_at": 1741689600_000_000_000,
        "exchange_ts": 1741689600120,
        "collector_session_id": "s", "session_seq": offset,
        "raw_text": raw_text, "raw_sha256": raw_sha,
        "_topic": "binance.trades", "_partition": 0, "_offset": offset,
    }


def _make_archive(tmp_path, envelopes, exchange="binance", symbol="btcusdt",
                  stream="trades", date="2026-03-11", hour=14):
    dir_path = tmp_path / exchange / symbol / stream / date
    dir_path.mkdir(parents=True, exist_ok=True)
    data_path = dir_path / f"hour-{hour:02d}.jsonl.zst"
    sc_path = dir_path / f"hour-{hour:02d}.jsonl.zst.sha256"
    cctx = zstd.ZstdCompressor(level=3)
    raw_lines = b"".join(orjson.dumps(e) + b"\n" for e in envelopes)
    compressed = cctx.compress(raw_lines)
    data_path.write_bytes(compressed)
    digest = hashlib.sha256(compressed).hexdigest()
    sc_path.write_text(f"{digest}  {data_path.name}\n")
    return data_path, sc_path


class TestChecksumVerification:
    def test_valid_checksum(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        assert verify_checksum(data_path, sc_path) == []

    def test_corrupted_file(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        with open(data_path, "r+b") as f:
            f.seek(5); f.write(b"\xff\xff")
        errors = verify_checksum(data_path, sc_path)
        assert len(errors) == 1 and "checksum mismatch" in errors[0].lower()

    def test_missing_sidecar(self, tmp_path):
        from src.cli.verify import verify_checksum
        data_path, sc_path = _make_archive(tmp_path, [_make_data_envelope(0)])
        sc_path.unlink()
        assert len(verify_checksum(data_path, sc_path)) == 1


class TestEnvelopeValidation:
    def test_valid(self):
        from src.cli.verify import verify_envelopes
        assert verify_envelopes([_make_data_envelope(i) for i in range(3)]) == []

    def test_sha256_mismatch(self):
        from src.cli.verify import verify_envelopes
        env = _make_data_envelope(0)
        env["raw_sha256"] = "dead" * 16
        assert len(verify_envelopes([env])) == 1

    def test_missing_field(self):
        from src.cli.verify import verify_envelopes
        env = _make_data_envelope(0)
        del env["raw_text"]
        assert len(verify_envelopes([env])) >= 1


class TestDuplicateOffsets:
    def test_no_duplicates(self):
        from src.cli.verify import check_duplicate_offsets
        assert check_duplicate_offsets([_make_data_envelope(i) for i in range(5)]) == []

    def test_duplicate(self):
        from src.cli.verify import check_duplicate_offsets
        errors = check_duplicate_offsets([_make_data_envelope(0), _make_data_envelope(0)])
        assert len(errors) == 1 and "duplicate" in errors[0].lower()


class TestGapReporting:
    def test_reports_gaps(self):
        from src.cli.verify import report_gaps
        gap = {"v": 1, "type": "gap", "reason": "ws_disconnect",
               "exchange": "binance", "symbol": "btcusdt", "stream": "trades",
               "received_at": 0, "collector_session_id": "s", "session_seq": 0,
               "gap_start_ts": 0, "gap_end_ts": 1, "detail": "test",
               "_topic": "t", "_partition": 0, "_offset": 1}
        assert len(report_gaps([_make_data_envelope(0), gap])) == 1

    def test_no_gaps(self):
        from src.cli.verify import report_gaps
        assert report_gaps([_make_data_envelope(i) for i in range(3)]) == []


class TestDepthReplay:
    """Tests use received_at for cross-topic ordering (not _offset, which is
    only meaningful within a single topic/partition — spec 4.1.1)."""

    _base_ts = 1741689600_000_000_000  # base timestamp for ordering

    def _make_depth_env(self, seq, U, u, pu):
        raw = f'{{"e":"depthUpdate","E":100,"s":"BTCUSDT","U":{U},"u":{u},"pu":{pu},"b":[],"a":[]}}'
        env = {**_make_data_envelope(seq, raw), "stream": "depth",
               "_topic": "binance.depth"}
        # Use seq to derive a monotonically increasing received_at
        env["received_at"] = self._base_ts + seq * 1_000_000
        return env

    def _make_snap_env(self, seq, last_update_id):
        raw = f'{{"lastUpdateId":{last_update_id},"bids":[],"asks":[]}}'
        env = {**_make_data_envelope(seq, raw), "stream": "depth_snapshot",
               "_topic": "binance.depth_snapshot"}
        env["received_at"] = self._base_ts + seq * 1_000_000
        return env

    def test_valid_chain_passes(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1003, 1005, 1002),
        ]
        errors = verify_depth_replay(diffs, [snap], [])
        assert errors == []

    def test_pu_break_without_gap_flagged(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),  # pu break
        ]
        errors = verify_depth_replay(diffs, [snap], [])
        assert len(errors) >= 1 and "pu chain break" in errors[0].lower()

    def test_pu_break_with_depth_gap_is_excused(self):
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        gap = {"type": "gap", "symbol": "btcusdt", "stream": "depth",
               "gap_start_ts": 0, "gap_end_ts": 9999999999_000_000_000,
               "received_at": 1741689600_000_000_000}
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),
        ]
        errors = verify_depth_replay(diffs, [snap], [gap])
        assert errors == []

    def test_non_depth_gap_does_not_excuse_depth_break(self):
        """A trades gap for the same symbol must NOT excuse a depth pu chain break."""
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        gap = {"type": "gap", "symbol": "btcusdt", "stream": "trades",
               "gap_start_ts": 0, "gap_end_ts": 9999999999_000_000_000,
               "received_at": 1741689600_000_000_000}
        diffs = [
            self._make_depth_env(1, 999, 1002, 998),
            self._make_depth_env(2, 1010, 1015, 1008),
        ]
        errors = verify_depth_replay(diffs, [snap], [gap])
        assert len(errors) >= 1 and "pu chain break" in errors[0].lower()

    def test_first_diff_must_span_sync_point(self):
        """First diff after snapshot must satisfy U <= lastUpdateId+1 <= u."""
        from src.cli.verify import verify_depth_replay
        snap = self._make_snap_env(0, 1000)
        # Diff that does NOT span lastUpdateId+1=1001: U=1010, u=1015
        diffs = [self._make_depth_env(1, 1010, 1015, 1009)]
        errors = verify_depth_replay(diffs, [snap], [])
        assert len(errors) >= 1 and "sync point" in errors[0].lower()

    def test_multi_symbol_isolated(self):
        """Each symbol's pu chain is validated independently."""
        from src.cli.verify import verify_depth_replay
        # btcusdt has valid chain
        snap_btc = {**self._make_snap_env(0, 1000), "symbol": "btcusdt"}
        diff_btc = {**self._make_depth_env(1, 999, 1002, 998), "symbol": "btcusdt"}
        # ethusdt has broken chain (no snapshot)
        diff_eth_raw = '{"e":"depthUpdate","E":100,"s":"ETHUSDT","U":500,"u":505,"pu":499,"b":[],"a":[]}'
        diff_eth = {**_make_data_envelope(2, diff_eth_raw), "symbol": "ethusdt",
                    "stream": "depth", "_topic": "binance.depth",
                    "received_at": self._base_ts + 2 * 1_000_000}
        errors = verify_depth_replay([diff_btc, diff_eth], [snap_btc], [])
        # btcusdt should pass, ethusdt should fail
        assert any("ethusdt" in e for e in errors)
        assert not any("btcusdt" in e for e in errors)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_verify.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement verification CLI**

`src/cli/verify.py` — Implements `verify_checksum()`, `verify_envelopes()`, `check_duplicate_offsets()`, `report_gaps()`, `decompress_and_parse()`, `verify_depth_replay()`, `generate_manifest()`, and Click CLI commands (`verify`, `manifest`). See spec Section 5.9 for full verification checks.

Key functions:
- `verify_checksum(data_path, sidecar_path) -> list[str]`: SHA-256 file hash vs sidecar
- `verify_envelopes(envelopes) -> list[str]`: required fields + raw_sha256 integrity
- `check_duplicate_offsets(envelopes) -> list[str]`: unique (_topic, _partition, _offset) tuples
- `report_gaps(envelopes) -> list[dict]`: filter type=="gap" records
- `verify_depth_replay(depth_envs, snap_envs, gap_envs) -> list[str]`: pu chain replay — sorts diffs and snapshots by received_at (cross-topic ordering), validates U/u/pu chain from each snapshot's lastUpdateId sync point, excuses breaks bounded by depth gap records
- `generate_manifest(base_dir, exchange, date) -> dict`: per-date manifest JSON
- CLI entry: `cryptolake verify --date YYYY-MM-DD --base-dir /data [--full] [--repair-checksums]`
- CLI entry: `cryptolake manifest --date YYYY-MM-DD --base-dir /data`

```python
from __future__ import annotations
import hashlib, json
from pathlib import Path
import click, orjson, zstandard as zstd

_REQUIRED_DATA_FIELDS = {"v","type","exchange","symbol","stream","received_at",
    "exchange_ts","collector_session_id","session_seq","raw_text","raw_sha256",
    "_topic","_partition","_offset"}
_REQUIRED_GAP_FIELDS = {"v","type","exchange","symbol","stream","received_at",
    "collector_session_id","session_seq","gap_start_ts","gap_end_ts","reason",
    "detail","_topic","_partition","_offset"}

def verify_checksum(data_path: Path, sidecar_path: Path) -> list[str]:
    if not sidecar_path.exists():
        return [f"Sidecar not found: {sidecar_path}"]
    expected = sidecar_path.read_text().strip().split()[0]
    h = hashlib.sha256()
    with open(data_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    if h.hexdigest() != expected:
        return [f"Checksum mismatch for {data_path.name}"]
    return []

def verify_envelopes(envelopes: list[dict]) -> list[str]:
    errors = []
    for i, env in enumerate(envelopes):
        req = _REQUIRED_GAP_FIELDS if env.get("type") == "gap" else _REQUIRED_DATA_FIELDS
        missing = req - set(env.keys())
        if missing:
            errors.append(f"Line {i}: missing fields: {sorted(missing)}")
            continue
        if env.get("type") == "data":
            actual = hashlib.sha256(env["raw_text"].encode()).hexdigest()
            if actual != env["raw_sha256"]:
                errors.append(f"Line {i}: raw_sha256 mismatch at offset {env.get('_offset')}")
    return errors

def check_duplicate_offsets(envelopes: list[dict]) -> list[str]:
    seen, errors = set(), []
    for env in envelopes:
        key = (env.get("_topic"), env.get("_partition"), env.get("_offset"))
        if key in seen:
            errors.append(f"Duplicate broker record: {key}")
        seen.add(key)
    return errors

def report_gaps(envelopes: list[dict]) -> list[dict]:
    return [e for e in envelopes if e.get("type") == "gap"]

def decompress_and_parse(file_path: Path) -> list[dict]:
    dctx = zstd.ZstdDecompressor()
    with open(file_path, "rb") as f:
        data = dctx.stream_reader(f).read()
    return [orjson.loads(line) for line in data.strip().split(b"\n") if line]

@click.group()
def cli():
    """CryptoLake data verification CLI."""

@cli.command()
@click.option("--date", required=True, help="Date to verify (YYYY-MM-DD)")
@click.option("--base-dir", default="/data", help="Archive base directory")
@click.option("--exchange", default=None, help="Filter by exchange")
@click.option("--symbol", default=None, help="Filter by symbol")
@click.option("--stream", default=None, help="Filter by stream")
@click.option("--full", is_flag=True, help="Full verification including cross-file dedup")
@click.option("--repair-checksums", is_flag=True, help="Generate missing .sha256 sidecars")
def verify(date, base_dir, exchange, symbol, stream, full, repair_checksums):
    """Verify archive integrity for a date."""
    base = Path(base_dir)
    all_errors: list[str] = []
    all_gaps: list[dict] = []
    all_envelopes: list[dict] = []

    # Find all .jsonl.zst files for the date
    files = sorted(base.rglob(f"*/{date}/hour-*.jsonl.zst"))
    if not files:
        click.echo(f"No archive files found for date {date} in {base_dir}")
        return

    for data_path in files:
        parts = data_path.relative_to(base).parts
        if len(parts) < 4:
            continue
        file_exchange, file_symbol, file_stream = parts[0], parts[1], parts[2]
        if exchange and file_exchange != exchange:
            continue
        if symbol and file_symbol != symbol:
            continue
        if stream and file_stream != stream:
            continue

        click.echo(f"Verifying: {data_path.relative_to(base)}")

        # 1. Checksum
        sc = Path(str(data_path) + ".sha256")
        if repair_checksums and not sc.exists():
            from src.writer.file_rotator import write_sha256_sidecar
            write_sha256_sidecar(data_path, sc)
            click.echo(f"  Repaired: {sc.name}")
        checksum_errors = verify_checksum(data_path, sc)
        all_errors.extend(checksum_errors)

        # 2. Decompress
        try:
            envelopes = decompress_and_parse(data_path)
        except Exception as e:
            all_errors.append(f"Decompression failed: {data_path} - {e}")
            continue

        # 3. Envelope validation
        all_errors.extend(verify_envelopes(envelopes))

        # 4. Intra-file duplicate offsets
        all_errors.extend(check_duplicate_offsets(envelopes))

        # 5. Gap reporting
        all_gaps.extend(report_gaps(envelopes))

        if full:
            all_envelopes.extend(envelopes)

    # Cross-file dedup check
    if full and all_envelopes:
        all_errors.extend(check_duplicate_offsets(all_envelopes))

    # Depth replay verification (when --full)
    if full and all_envelopes:
        depth_envs = [e for e in all_envelopes if e.get("stream") == "depth" and e.get("type") == "data"]
        snap_envs = [e for e in all_envelopes if e.get("stream") == "depth_snapshot" and e.get("type") == "data"]
        gap_envs = [e for e in all_envelopes if e.get("type") == "gap"]
        all_errors.extend(verify_depth_replay(depth_envs, snap_envs, gap_envs))

    # Report
    click.echo(f"\n{'='*60}")
    click.echo(f"Verification complete for {date}")
    click.echo(f"Files checked: {len(files)}")
    if all_errors:
        click.echo(f"\nERRORS ({len(all_errors)}):")
        for err in all_errors:
            click.echo(f"  - {err}")
    else:
        click.echo("Errors: 0")
    if all_gaps:
        click.echo(f"\nGAPS ({len(all_gaps)}):")
        for gap in all_gaps:
            click.echo(f"  - {gap.get('symbol')}/{gap.get('stream')}: "
                       f"{gap.get('reason')} ({gap.get('detail', '')})")
    else:
        click.echo("Gaps: 0")
    if all_errors:
        raise SystemExit(1)


@cli.command()
@click.option("--date", required=True, help="Date (YYYY-MM-DD)")
@click.option("--base-dir", default="/data", help="Archive base directory")
@click.option("--exchange", default="binance", help="Exchange name")
def manifest(date, base_dir, exchange):
    """Generate manifest.json for a date directory."""
    base = Path(base_dir)
    m = generate_manifest(base, exchange, date)
    # Write to each date directory that has data
    exchange_dir = base / exchange
    if exchange_dir.exists():
        for symbol_dir in exchange_dir.iterdir():
            if not symbol_dir.is_dir():
                continue
            for stream_dir in symbol_dir.iterdir():
                date_dir = stream_dir / date
                if date_dir.exists():
                    manifest_path = date_dir / "manifest.json"
                    manifest_path.write_text(json.dumps(m, indent=2) + "\n")
                    click.echo(f"Written: {manifest_path}")
    click.echo(json.dumps(m, indent=2))


def generate_manifest(base_dir: Path, exchange: str, date: str) -> dict:
    """Generate a manifest summarizing archive contents for a date."""
    manifest = {"date": date, "exchange": exchange, "symbols": {}}
    exchange_dir = base_dir / exchange
    if not exchange_dir.exists():
        return manifest
    for symbol_dir in sorted(exchange_dir.iterdir()):
        if not symbol_dir.is_dir():
            continue
        symbol = symbol_dir.name
        manifest["symbols"][symbol] = {"streams": {}}
        for stream_dir in sorted(symbol_dir.iterdir()):
            if not stream_dir.is_dir():
                continue
            date_dir = stream_dir / date
            if not date_dir.exists():
                continue
            hours, record_count, gaps_list = [], 0, []
            for f in sorted(date_dir.glob("hour-*.jsonl.zst")):
                hour_str = f.name.split(".")[0].replace("hour-", "").split(".")[0]
                try:
                    hours.append(int(hour_str))
                except ValueError:
                    pass
                try:
                    envs = decompress_and_parse(f)
                    record_count += len(envs)
                    for env in envs:
                        if env.get("type") == "gap":
                            gaps_list.append({"symbol": env.get("symbol"),
                                "reason": env.get("reason"),
                                "gap_start_ts": env.get("gap_start_ts"),
                                "gap_end_ts": env.get("gap_end_ts")})
                except Exception:
                    pass
            if hours:
                manifest["symbols"][symbol]["streams"][stream_dir.name] = {
                    "hours": sorted(set(hours)),
                    "record_count": record_count,
                    "gaps": gaps_list,
                }
    return manifest


def verify_depth_replay(depth_envelopes, snapshot_envelopes, gap_envelopes) -> list[str]:
    """Verify depth diffs are anchored by snapshots with valid pu chains.
    Gap records excuse breaks in the chain.

    Runs an independent pu-chain walk per symbol to avoid cross-symbol interference.
    """
    errors = []
    if not depth_envelopes:
        return errors

    # Group by symbol
    from collections import defaultdict
    depth_by_sym: dict[str, list] = defaultdict(list)
    snap_by_sym: dict[str, list] = defaultdict(list)

    for e in depth_envelopes:
        depth_by_sym[e["symbol"]].append(e)
    for e in snapshot_envelopes:
        snap_by_sym[e["symbol"]].append(e)
    # Scope gap exemptions by (symbol, stream) — only depth gaps excuse depth breaks
    gap_by_sym_stream: dict[tuple[str, str], list] = defaultdict(list)
    for g in gap_envelopes:
        key = (g.get("symbol", ""), g.get("stream", ""))
        gap_by_sym_stream[key].append(g)

    for symbol in depth_by_sym:
        # Sort by received_at (wall-clock timestamp), NOT _offset.
        # Diffs and snapshots are on separate topics (binance.depth vs binance.depth_snapshot),
        # so _offset values are incomparable across them (spec 4.1.1).
        sym_depths = sorted(depth_by_sym[symbol], key=lambda e: e["received_at"])
        sym_snaps = sorted(snap_by_sym.get(symbol, []), key=lambda e: e["received_at"])
        # Only depth and depth_snapshot gaps can excuse depth chain breaks
        sym_depth_gaps = gap_by_sym_stream.get((symbol, "depth"), []) + \
                         gap_by_sym_stream.get((symbol, "depth_snapshot"), [])
        gap_windows = [(g["gap_start_ts"], g["gap_end_ts"]) for g in sym_depth_gaps]

        def _in_gap(received_at):
            return any(s <= received_at <= e for s, e in gap_windows)

        snap_idx, synced, last_u, sync_lid = 0, False, None, None
        for env in sym_depths:
            raw = orjson.loads(env["raw_text"])
            U, u, pu = raw.get("U", 0), raw.get("u", 0), raw.get("pu", 0)
            # Advance past snapshots that arrived before this diff (by received_at)
            while snap_idx < len(sym_snaps) and sym_snaps[snap_idx]["received_at"] <= env["received_at"]:
                snap_raw = orjson.loads(sym_snaps[snap_idx]["raw_text"])
                sync_lid = snap_raw["lastUpdateId"]
                synced, last_u = True, None
                snap_idx += 1
            if not synced:
                if not _in_gap(env["received_at"]):
                    errors.append(f"[{symbol}] Depth diff at received_at {env['received_at']} has no preceding snapshot")
                continue
            if last_u is None:
                # First diff after snapshot: validate sync-point rule
                # U <= lastUpdateId+1 and u >= lastUpdateId+1
                if not (U <= sync_lid + 1 <= u):
                    if not _in_gap(env["received_at"]):
                        errors.append(
                            f"[{symbol}] First diff after snapshot does not span sync point: "
                            f"lastUpdateId={sync_lid}, U={U}, u={u} at received_at {env['received_at']}")
                    continue
                last_u = u
                continue
            if pu != last_u:
                if not _in_gap(env["received_at"]):
                    errors.append(f"[{symbol}] pu chain break at received_at {env['received_at']}: expected pu={last_u}, got pu={pu}")
                last_u = u
            else:
                last_u = u
    return errors
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_verify.py -v`
Expected: All 17 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cli/verify.py tests/unit/test_verify.py
git commit -m "feat: verification CLI with checksum, envelope, dedup, gap, and manifest checks"
```

---

### Manual Verification: Chunk 5

1. **Run all unit tests:**
   ```bash
   uv run pytest tests/unit/ -v --tb=short
   ```
   **Check:** All tests pass (~50+ across all modules).

2. **Create a test archive and verify it:**
   ```bash
   uv run python -c "
   import hashlib, orjson, zstandard as zstd
   from pathlib import Path
   base = Path('/tmp/cryptolake_test')
   d = base / 'binance' / 'btcusdt' / 'trades' / '2026-03-11'
   d.mkdir(parents=True, exist_ok=True)
   envs = []
   for i in range(5):
       raw = '{\"e\":\"aggTrade\",\"seq\":' + str(i) + '}'
       envs.append({'v':1,'type':'data','exchange':'binance','symbol':'btcusdt',
           'stream':'trades','received_at':1741689600_000_000_000+i,'exchange_ts':100,
           'collector_session_id':'s','session_seq':i,'raw_text':raw,
           'raw_sha256':hashlib.sha256(raw.encode()).hexdigest(),
           '_topic':'binance.trades','_partition':0,'_offset':i})
   raw_lines = b''.join(orjson.dumps(e)+b'\n' for e in envs)
   compressed = zstd.ZstdCompressor(level=3).compress(raw_lines)
   (d/'hour-14.jsonl.zst').write_bytes(compressed)
   digest = hashlib.sha256(compressed).hexdigest()
   (d/'hour-14.jsonl.zst.sha256').write_text(f'{digest}  hour-14.jsonl.zst\n')
   print('Created test archive')
   "
   ```

3. **Run verify CLI:**
   ```bash
   uv run cryptolake verify --date 2026-03-11 --base-dir /tmp/cryptolake_test --full
   ```
   **Check:** "Files checked: 1", "Errors: 0", "Gaps: 0". Exit code 0.

4. **Generate manifest:**
   ```bash
   uv run cryptolake manifest --date 2026-03-11 --base-dir /tmp/cryptolake_test
   ```
   **Check:** JSON with btcusdt/trades showing hour 14, 5 records, 0 gaps.

5. **Cleanup:** `rm -rf /tmp/cryptolake_test`

---

## Chunk 6: Infrastructure, Observability & Integration Tests

### Task 6.1: Docker & Compose Configuration

**Files:**
- Create: `Dockerfile.collector`
- Create: `Dockerfile.writer`
- Create: `docker-compose.yml`
- Create: `docker-compose.test.yml`
- Create: `config/config.yaml`
- Create: `config/config.example.yaml`
- Create: `.env.example`

- [ ] **Step 1: Create Dockerfile.collector**

```dockerfile
FROM python:3.12.7-slim AS base
WORKDIR /app
RUN groupadd -r cryptolake && useradd -r -g cryptolake cryptolake

# Install uv first (cacheable layer)
RUN pip install uv

# Copy dependency manifests and source (uv sync needs the project package)
COPY pyproject.toml uv.lock ./
COPY src/ src/
RUN uv sync --frozen --no-dev

RUN mkdir -p /data && chown cryptolake:cryptolake /data
USER cryptolake
EXPOSE 8000

CMD ["uv", "run", "python", "-m", "src.collector.main"]
```

- [ ] **Step 2: Create Dockerfile.writer**

```dockerfile
FROM python:3.12.7-slim AS base
WORKDIR /app
RUN groupadd -r cryptolake && useradd -r -g cryptolake cryptolake

# Install uv first (cacheable layer)
RUN pip install uv

# Copy dependency manifests and source (uv sync needs the project package)
COPY pyproject.toml uv.lock ./
COPY src/ src/
RUN uv sync --frozen --no-dev

RUN mkdir -p /data && chown cryptolake:cryptolake /data
USER cryptolake
EXPOSE 8001

CMD ["uv", "run", "python", "-m", "src.writer.main"]
```

- [ ] **Step 3: Create docker-compose.yml**

Full docker-compose.yml per spec Section 11.1 with services: postgres, redpanda, collector, writer, prometheus, grafana, alertmanager. Networks: cryptolake_internal (internal), collector_egress, alertmanager_egress. Volumes: postgres_data, redpanda_data, prometheus_data, data_volume.

Key points:
- Redpanda: `redpandadata/redpanda:v24.1.2`, retention 172800000ms, 1 partition default
- Postgres: `postgres:16-alpine`
- Collector depends_on redpanda (healthy), networks: internal + egress
- Writer depends_on redpanda + postgres (healthy), networks: internal only
- Prometheus: `prom/prometheus:v2.51.0`, internal only
- Grafana: `grafana/grafana:11.0.0`, port 3000
- Alertmanager: `prom/alertmanager:v0.27.0`, internal + alertmanager_egress

- [ ] **Step 4: Create config/config.yaml and config.example.yaml**

Use the fixture config as base, update with production defaults.

- [ ] **Step 5: Create .env.example**

```env
POSTGRES_PASSWORD=changeme
GF_ADMIN_PASSWORD=changeme
WEBHOOK_URL=https://hooks.slack.com/services/xxx
GRAFANA_BIND=127.0.0.1
```

- [ ] **Step 6: Commit**

```bash
git add Dockerfile.collector Dockerfile.writer docker-compose.yml docker-compose.test.yml \
    config/ .env.example
git commit -m "feat: Docker and Compose configuration for all services"
```

---

### Task 6.2: Prometheus, Grafana & Alertmanager Configuration

**Files:**
- Create: `infra/prometheus/prometheus.yml`
- Create: `infra/prometheus/alert_rules.yml`
- Create: `infra/grafana/provisioning/datasources.yml`
- Create: `infra/grafana/provisioning/dashboards.yml`
- Create: `infra/grafana/dashboards/cryptolake.json`
- Create: `infra/alertmanager/alertmanager.yml`

- [ ] **Step 1: Create Prometheus scrape config**

`infra/prometheus/prometheus.yml`:
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - alert_rules.yml

alerting:
  alertmanagers:
    - static_configs:
        - targets: ["alertmanager:9093"]

scrape_configs:
  - job_name: "collector"
    static_configs:
      - targets: ["collector:8000"]
  - job_name: "writer"
    static_configs:
      - targets: ["writer:8001"]
  - job_name: "redpanda"
    static_configs:
      - targets: ["redpanda:9644"]
```

- [ ] **Step 2: Create alert rules**

`infra/prometheus/alert_rules.yml` — defines alerts per spec Section 10.3: GapDetected, ConnectionLost, WriterLagging, WriterLagCritical, SnapshotsFailing, DiskAlmostFull, DiskCritical, HighLatency, NTPDrift, RedpandaBufferHigh, MessagesDropped.

- [ ] **Step 3: Create Grafana provisioning**

`infra/grafana/provisioning/datasources.yml`:
```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

`infra/grafana/provisioning/dashboards.yml`:
```yaml
apiVersion: 1
providers:
  - name: CryptoLake
    type: file
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: false
```

- [ ] **Step 4: Create Grafana dashboard JSON**

`infra/grafana/dashboards/cryptolake.json` — dashboard with panels per spec Section 10.4: message throughput (stacked area), latency heatmap, Redpanda -> Writer lag, connection status, gap timeline, disk usage, snapshot health, compression efficiency.

- [ ] **Step 5: Create Alertmanager config**

`infra/alertmanager/alertmanager.yml`:
```yaml
global:
  resolve_timeout: 5m

route:
  receiver: webhook
  group_by: [alertname, exchange, symbol, stream]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

receivers:
  - name: webhook
    webhook_configs:
      - url: "${WEBHOOK_URL}"
        send_resolved: true
```

- [ ] **Step 6: Commit**

```bash
git add infra/
git commit -m "feat: Prometheus, Grafana, and Alertmanager configuration"
```

---

### Task 6.3: Integration Tests

**Files:**
- Create: `tests/integration/test_redpanda_roundtrip.py`
- Create: `tests/integration/test_writer_rotation.py`
- Create: `tests/integration/test_binance_ws.py`

- [ ] **Step 1: Write Redpanda round-trip integration test**

`tests/integration/test_redpanda_roundtrip.py`:
```python
import pytest
import orjson
from testcontainers.kafka import KafkaContainer

from src.common.envelope import create_data_envelope, serialize_envelope, deserialize_envelope


@pytest.fixture(scope="module")
def redpanda():
    """Start a Redpanda container for testing."""
    with KafkaContainer(image="redpandadata/redpanda:v24.1.2") as container:
        yield container


@pytest.mark.integration
class TestRedpandaRoundTrip:
    def test_produce_consume_envelope(self, redpanda):
        from confluent_kafka import Producer, Consumer
        from confluent_kafka.admin import AdminClient, NewTopic

        broker = redpanda.get_bootstrap_server()

        # Create topic
        admin = AdminClient({"bootstrap.servers": broker})
        admin.create_topics([NewTopic("binance.trades", num_partitions=1)])

        # Produce
        producer = Producer({"bootstrap.servers": broker, "acks": "all"})
        env = create_data_envelope(
            exchange="binance", symbol="btcusdt", stream="trades",
            raw_text='{"e":"aggTrade","E":100}', exchange_ts=100,
            collector_session_id="test", session_seq=0,
        )
        producer.produce("binance.trades", key=b"btcusdt", value=serialize_envelope(env))
        producer.flush()

        # Consume
        consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": "test-group",
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe(["binance.trades"])
        msg = consumer.poll(timeout=10.0)
        assert msg is not None
        assert msg.error() is None

        received = deserialize_envelope(msg.value())
        assert received["exchange"] == "binance"
        assert received["symbol"] == "btcusdt"
        assert received["raw_text"] == '{"e":"aggTrade","E":100}'

        consumer.close()
```

- [ ] **Step 2: Write writer rotation integration test**

`tests/integration/test_writer_rotation.py`:
```python
import hashlib
import pytest
import orjson
import zstandard as zstd
from pathlib import Path

from src.writer.buffer_manager import BufferManager
from src.writer.compressor import ZstdFrameCompressor
from src.writer.file_rotator import build_file_path, sidecar_path, write_sha256_sidecar, compute_sha256


@pytest.mark.integration
class TestWriterFileOutput:
    def test_full_write_cycle(self, tmp_path):
        """Simulate: buffer messages -> flush -> compress -> write -> seal with SHA-256."""
        base_dir = str(tmp_path)
        bm = BufferManager(base_dir=base_dir, flush_messages=3, flush_interval_seconds=9999)
        comp = ZstdFrameCompressor(level=3)

        import datetime
        dt = datetime.datetime(2026, 3, 11, 14, 30, tzinfo=datetime.timezone.utc)
        ns = int(dt.timestamp() * 1_000_000_000)

        # Add 3 messages to trigger flush
        for i in range(3):
            raw = f'{{"seq":{i}}}'
            env = {
                "v": 1, "type": "data", "exchange": "binance", "symbol": "btcusdt",
                "stream": "trades", "received_at": ns + i, "exchange_ts": 100,
                "collector_session_id": "s", "session_seq": i,
                "raw_text": raw, "raw_sha256": hashlib.sha256(raw.encode()).hexdigest(),
                "_topic": "binance.trades", "_partition": 0, "_offset": i,
            }
            result = bm.add(env)

        # Last add should trigger flush
        assert result is not None
        assert len(result) == 1

        fr = result[0]
        fr.file_path.parent.mkdir(parents=True, exist_ok=True)
        compressed = comp.compress_frame(fr.lines)
        fr.file_path.write_bytes(compressed)

        # Seal with SHA-256
        sc = sidecar_path(fr.file_path)
        write_sha256_sidecar(fr.file_path, sc)

        # Verify
        assert fr.file_path.exists()
        assert sc.exists()
        assert compute_sha256(fr.file_path) == sc.read_text().split()[0]

        # Decompress and verify content
        dctx = zstd.ZstdDecompressor()
        decompressed = dctx.decompress(fr.file_path.read_bytes())
        lines = decompressed.strip().split(b"\n")
        assert len(lines) == 3
        for i, line in enumerate(lines):
            parsed = orjson.loads(line)
            assert parsed["_offset"] == i
```

- [ ] **Step 3: Write Binance WS integration test (fixture-based)**

`tests/integration/test_binance_ws.py`:
```python
import pytest
from pathlib import Path

from src.exchanges.binance import BinanceAdapter

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


@pytest.mark.integration
class TestBinanceWSFixtures:
    def test_route_all_fixture_types(self):
        """Route every fixture through the adapter and verify stream type extraction."""
        adapter = BinanceAdapter(ws_base="wss://fstream.binance.com",
                                 rest_base="https://fapi.binance.com")
        import orjson

        cases = [
            ("binance_aggtrade.json", "btcusdt@aggTrade", "trades"),
            ("binance_depth_diff.json", "btcusdt@depth@100ms", "depth"),
            ("binance_bookticker.json", "btcusdt@bookTicker", "bookticker"),
            ("binance_markprice.json", "btcusdt@markPrice@1s", "funding_rate"),
            ("binance_forceorder.json", "btcusdt@forceOrder", "liquidations"),
        ]

        for fixture_name, stream_key, expected_type in cases:
            inner = (FIXTURES_DIR / fixture_name).read_text()
            frame = orjson.dumps({"stream": stream_key, "data": orjson.loads(inner)}).decode()
            stream_type, symbol, raw_data = adapter.route_stream(frame)
            assert stream_type == expected_type, f"Failed for {fixture_name}"
            assert symbol == "btcusdt"
```

- [ ] **Step 4: Commit**

```bash
git add tests/integration/
git commit -m "feat: integration tests for Redpanda round-trip, writer rotation, and Binance WS fixtures"
```

---

### Task 6.4: Chaos Test Scripts

**Files:**
- Create: `tests/chaos/kill_writer.sh`
- Create: `tests/chaos/kill_ws_connection.sh`
- Create: `tests/chaos/fill_disk.sh`
- Create: `tests/chaos/depth_reconnect_inflight.sh`
- Create: `tests/chaos/buffer_overflow_recovery.sh`
- Create: `tests/chaos/writer_crash_before_commit.sh`

- [ ] **Step 1: Create chaos test scripts**

`tests/chaos/kill_writer.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
echo "=== Chaos: Kill Writer ==="
echo "1. Killing writer container..."
docker kill cryptolake-writer-1
echo "2. Waiting 120 seconds (data accumulates in Redpanda)..."
sleep 120
echo "3. Restarting writer..."
docker compose up -d writer
echo "4. Waiting 60 seconds for catch-up..."
sleep 60
echo "5. Running verification..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
echo "=== Check: Errors should be 0, writer should have caught up ==="
```

`tests/chaos/kill_ws_connection.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
echo "=== Chaos: Kill WebSocket Connection ==="
echo "1. Capturing pre-kill topic offsets..."
docker compose exec redpanda rpk topic consume binance.trades -n 1 --offset end
echo "2. Killing collector to simulate WS disconnect..."
docker kill cryptolake-collector-1
sleep 5
echo "3. Restarting collector..."
docker compose up -d collector
echo "4. Waiting 30 seconds for reconnect..."
sleep 30
echo "5. Checking for gap records..."
docker compose exec redpanda rpk topic consume binance.trades -n 20 --offset end | grep -c '"type":"gap"' || echo "No gap records found (check manually)"
echo "=== Check: Gap record with reason=ws_disconnect should exist ==="
```

`tests/chaos/fill_disk.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
echo "=== Chaos: Fill Disk ==="
echo "1. Creating large file in data volume..."
docker compose exec writer dd if=/dev/zero of=/data/fill_disk.tmp bs=1M count=500 2>/dev/null || true
echo "2. Checking disk usage..."
docker compose exec writer df -h /data
echo "3. Waiting 60 seconds..."
sleep 60
echo "4. Checking writer health..."
curl -s http://localhost:8001/health || echo "Writer health check failed"
echo "5. Cleaning up..."
docker compose exec writer rm -f /data/fill_disk.tmp
echo "6. Waiting 30 seconds for recovery..."
sleep 30
echo "=== Check: Writer should pause at 95%, resume after cleanup ==="
```

`tests/chaos/writer_crash_before_commit.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
echo "=== Chaos: Writer Crash Before Commit ==="
echo "1. Sending SIGKILL to writer (simulates crash mid-flush)..."
docker kill -s KILL cryptolake-writer-1
echo "2. Restarting writer..."
docker compose up -d writer
echo "3. Waiting 60 seconds for recovery..."
sleep 60
echo "4. Running full verification..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
echo "=== Check: No duplicates, no corrupt zstd frames, verification passes ==="
```

- [ ] **Step 2: Make scripts executable and commit**

```bash
chmod +x tests/chaos/*.sh
git add tests/chaos/
git commit -m "feat: chaos test scripts for writer kill, WS disconnect, disk fill, and crash recovery"
```

---

### Task 6.5: E2E Test Compose File

**Files:**
- Create: `docker-compose.test.yml`

- [ ] **Step 1: Create test compose with shorter intervals**

`docker-compose.test.yml` extends the main compose with:
- Shorter snapshot interval (30s instead of 5m)
- Shorter OI poll interval (30s)
- Only 1 symbol (btcusdt) for faster test
- Lower flush thresholds (100 messages, 10s interval)
- Auto-stop after configurable duration

- [ ] **Step 2: Commit**

```bash
git add docker-compose.test.yml
git commit -m "feat: E2E test docker-compose with shortened intervals"
```

---

### Manual Verification: Chunk 6

1. **Validate docker-compose configuration:**
   ```bash
   cd /path/to/cryptolake && docker compose config --quiet
   ```
   **Check:** No errors. All services, networks, and volumes resolve correctly.

2. **Build Docker images:**
   ```bash
   docker compose build collector writer
   ```
   **Check:** Both images build successfully with no errors.

3. **Start the full stack:**
   ```bash
   cp .env.example .env  # Edit with real values
   docker compose up -d
   ```
   **Check:** All containers are healthy within 60 seconds: `docker compose ps` shows all services "healthy" or "running".

4. **Verify data is flowing:**
   ```bash
   # After 2-3 minutes:
   docker compose exec redpanda rpk topic list
   docker compose exec redpanda rpk topic consume binance.trades -n 3
   ```
   **Check:** Topics exist with `binance.*` naming. Messages contain valid envelope JSON with `raw_text` field.

5. **Verify writer is creating files:**
   ```bash
   docker compose exec writer find /data -type f | head -20
   ```
   **Check:** Files appear under `/data/binance/{symbol}/{stream}/{date}/hour-{HH}.jsonl.zst`.

6. **Verify a file decompresses correctly:**
   ```bash
   docker compose exec writer sh -c 'zstd -d < $(find /data -name "*.jsonl.zst" | head -1) | head -3'
   ```
   **Check:** Valid JSON lines with envelope fields including `_topic`, `_partition`, `_offset`.

7. **Check health endpoints:**
   ```bash
   curl -s http://localhost:8000/health  # Collector
   curl -s http://localhost:8001/health  # Writer
   ```
   **Check:** Both return `{"status": "ok"}` with HTTP 200.

8. **Check Prometheus targets:**
   ```bash
   curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep -A2 '"health"'
   ```
   **Check:** collector, writer, and redpanda targets are "up".

9. **Open Grafana dashboard:**
   Open `http://localhost:3000` in browser, login with credentials from `.env`.
   **Check:** CryptoLake dashboard loads with populated panels: message throughput, Redpanda -> Writer lag, latency heatmap.

10. **Run integration tests:**
    ```bash
    uv run pytest tests/integration/ -v -m integration --tb=short
    ```
    **Check:** All integration tests pass (Redpanda round-trip, writer rotation, Binance WS fixtures).

11. **Run a chaos test:**
    ```bash
    bash tests/chaos/kill_writer.sh
    ```
    **Check:** Writer recovers, verification passes with 0 errors, no duplicate offsets.

12. **Run full verification CLI:**
    ```bash
    docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
    ```
    **Check:** "Errors: 0". Any gaps are reported with reason and time window.

13. **Test graceful shutdown:**
    ```bash
    docker compose down
    ```
    **Check:** All services exit within 30 seconds. Verify with `docker compose logs writer --tail=20` that the writer flushed buffers and committed offsets before exit.

14. **Run all unit tests one final time:**
    ```bash
    uv run pytest tests/unit/ -v --tb=short
    ```
    **Check:** All ~55+ tests pass.

---

## Summary: Complete Test Suite Commands

```bash
# Unit tests (fast, no external deps)
uv run pytest tests/unit/ -v

# Integration tests (requires Docker/testcontainers)
uv run pytest tests/integration/ -v -m integration

# E2E test (full stack)
docker compose -f docker-compose.test.yml up --build
# Wait 10 minutes, then:
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full

# Chaos tests (against running stack)
bash tests/chaos/kill_writer.sh
bash tests/chaos/kill_ws_connection.sh
bash tests/chaos/writer_crash_before_commit.sh
```

## Acceptance Criteria Mapping

| AC | Validated By |
|----|-------------|
| AC-1: E2E data collection | E2E test + manual check 4-6 |
| AC-2: Data integrity | `cryptolake verify --full` + unit tests |
| AC-3: Order book reconstruction | `verify_depth_replay()` + depth unit tests |
| AC-4: Auto reconnection | `kill_ws_connection.sh` + gap detector tests |
| AC-5: Writer crash resilience | `kill_writer.sh` + `writer_crash_before_commit.sh` |
| AC-6: Observability | Prometheus targets + Grafana check + alert rules |
| AC-7: Storage organization | `find /data` + file_rotator tests |
| AC-8: Config-driven behavior | config unit tests + E2E with overrides |
| AC-9: Rate limit compliance | Staggered scheduler + integration tests |
| AC-10: Graceful shutdown | `docker compose down` + log verification |
| AC-11: Writer deduplication | State manager tests + verify CLI dedup check |
