from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestConfigLoading:
    def test_load_valid_config(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.exchanges.binance.enabled is True
        assert config.exchanges.binance.symbols == ["btcusdt", "ethusdt", "solusdt"]
        assert config.redpanda.retention_hours == 48

    def test_config_symbols_are_lowercase(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        for symbol in config.exchanges.binance.symbols:
            assert symbol == symbol.lower()

    def test_config_all_streams_enabled(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        streams = config.exchanges.binance.streams
        assert streams.trades is True
        assert streams.depth is True
        assert streams.bookticker is True
        assert streams.funding_rate is True
        assert streams.liquidations is True
        assert streams.open_interest is True

    def test_config_depth_snapshot_override(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.exchanges.binance.depth.snapshot_overrides["btcusdt"] == "1m"
        assert config.exchanges.binance.depth.snapshot_interval == "5m"

    def test_config_writer_defaults(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        assert config.writer.compression == "zstd"
        assert config.writer.compression_level == 3
        assert config.writer.rotation == "hourly"

    def test_config_retention_minimum_rejected(self, tmp_path: Path) -> None:
        """retention_hours < 12 must be rejected."""
        from src.common.config import ConfigValidationError, load_config

        bad_config = tmp_path / "bad.yaml"
        bad_config.write_text(
            """
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
""".strip()
        )
        with pytest.raises(ConfigValidationError, match="retention_hours"):
            load_config(bad_config)

    def test_config_env_override(self) -> None:
        """Environment variables override YAML values."""
        from src.common.config import load_config

        config = load_config(
            FIXTURES_DIR / "config_valid.yaml",
            env_overrides={"REDPANDA__BROKERS": "broker1:9092,broker2:9092"},
        )
        assert config.redpanda.brokers == ["broker1:9092", "broker2:9092"]

    def test_config_missing_file_raises(self) -> None:
        from src.common.config import load_config

        with pytest.raises(FileNotFoundError):
            load_config(Path("/nonexistent/config.yaml"))

    def test_disabled_stream_no_depth_snapshot(self) -> None:
        """When depth is enabled, depth_snapshot is implicitly enabled too."""
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        enabled = config.exchanges.binance.get_enabled_streams()
        assert "depth" in enabled
        assert "depth_snapshot" in enabled

    def test_enabled_stream_list(self) -> None:
        from src.common.config import load_config

        config = load_config(FIXTURES_DIR / "config_valid.yaml")
        enabled = config.exchanges.binance.get_enabled_streams()
        assert enabled == [
            "trades",
            "depth",
            "depth_snapshot",
            "bookticker",
            "funding_rate",
            "liquidations",
            "open_interest",
        ]

    def test_writer_streams_override_auto_includes_depth_snapshot(self) -> None:
        """writer_streams_override with 'depth' must auto-add 'depth_snapshot'."""
        from src.common.config import BinanceExchangeConfig

        cfg = BinanceExchangeConfig(
            symbols=["btcusdt"],
            writer_streams_override=["trades", "depth"],
        )
        assert "depth_snapshot" in cfg.writer_streams_override
        assert cfg.writer_streams_override == ["trades", "depth", "depth_snapshot"]

    def test_writer_streams_override_no_duplicate_depth_snapshot(self) -> None:
        """If depth_snapshot is already listed, don't duplicate it."""
        from src.common.config import BinanceExchangeConfig

        cfg = BinanceExchangeConfig(
            symbols=["btcusdt"],
            writer_streams_override=["depth", "depth_snapshot"],
        )
        assert cfg.writer_streams_override.count("depth_snapshot") == 1

    def test_writer_streams_override_none_by_default(self) -> None:
        from src.common.config import BinanceExchangeConfig

        cfg = BinanceExchangeConfig(symbols=["btcusdt"])
        assert cfg.writer_streams_override is None

    def test_env_override_does_not_bleed_os_environ(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When explicit env_overrides are passed, os.environ should not bleed in."""
        from src.common.config import load_config

        monkeypatch.setenv("REDPANDA__BROKERS", "leaked:9092,leaked:9093")
        config = load_config(
            FIXTURES_DIR / "config_valid.yaml",
            env_overrides={"WRITER__COMPRESSION_LEVEL": "5"},
        )
        assert config.writer.compression_level == 5
        # os.environ REDPANDA__BROKERS should NOT have been applied
        assert config.redpanda.brokers == ["redpanda:9092"]
