from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator


class ConfigValidationError(Exception):
    """Raised when configuration data fails validation."""


class StreamsConfig(BaseModel):
    trades: bool = True
    depth: bool = True
    bookticker: bool = True
    funding_rate: bool = True
    liquidations: bool = True
    open_interest: bool = True


class DepthConfig(BaseModel):
    update_speed: str = "100ms"
    snapshot_interval: str = "5m"
    snapshot_overrides: dict[str, str] = Field(default_factory=dict)


class OpenInterestConfig(BaseModel):
    poll_interval: str = "5m"


class MonitoringConfig(BaseModel):
    prometheus_port: int = 8000
    webhook_url: str = ""


class BinanceExchangeConfig(BaseModel):
    enabled: bool = True
    market: str = "usdm_futures"
    ws_base: str = "wss://fstream.binance.com"
    rest_base: str = "https://fapi.binance.com"
    symbols: list[str]
    streams: StreamsConfig = Field(default_factory=StreamsConfig)
    writer_streams_override: list[str] | None = None
    depth: DepthConfig = Field(default_factory=DepthConfig)
    open_interest: OpenInterestConfig = Field(default_factory=OpenInterestConfig)
    collector_id: str = "binance-collector-01"

    @field_validator("symbols", mode="before")
    @classmethod
    def lowercase_symbols(cls, value: list[str]) -> list[str]:
        return [symbol.lower() for symbol in value]

    @field_validator("writer_streams_override", mode="before")
    @classmethod
    def auto_include_depth_snapshot(cls, value: list[str] | None) -> list[str] | None:
        if value and "depth" in value and "depth_snapshot" not in value:
            return [*value, "depth_snapshot"]
        return value

    def get_enabled_streams(self) -> list[str]:
        enabled: list[str] = []
        if self.streams.trades:
            enabled.append("trades")
        if self.streams.depth:
            enabled.extend(["depth", "depth_snapshot"])
        if self.streams.bookticker:
            enabled.append("bookticker")
        if self.streams.funding_rate:
            enabled.append("funding_rate")
        if self.streams.liquidations:
            enabled.append("liquidations")
        if self.streams.open_interest:
            enabled.append("open_interest")
        return enabled


class ExchangesConfig(BaseModel):
    binance: BinanceExchangeConfig


class ProducerConfig(BaseModel):
    max_buffer: int = 100_000
    buffer_caps: dict[str, int] = Field(default_factory=lambda: {"depth": 80_000, "trades": 10_000})
    default_stream_cap: int = 10_000


class RedpandaConfig(BaseModel):
    brokers: list[str]
    retention_hours: int = 48
    producer: ProducerConfig = Field(default_factory=ProducerConfig)

    @field_validator("retention_hours")
    @classmethod
    def validate_retention_hours(cls, value: int) -> int:
        if value < 12:
            raise ValueError("retention_hours must be >= 12")
        return value


class DatabaseConfig(BaseModel):
    url: str


def default_archive_dir() -> str:
    """Return the archive directory from HOST_DATA_DIR or default /data."""
    return os.environ.get("HOST_DATA_DIR", "/data")


class GapFilterConfig(BaseModel):
    grace_period_seconds: float = Field(default=10.0, ge=0.0)


class WriterConfig(BaseModel):
    base_dir: str = Field(default_factory=default_archive_dir)
    rotation: str = "hourly"
    compression: str = "zstd"
    compression_level: int = 3
    checksum: str = "sha256"
    flush_messages: int = 10000
    flush_interval_seconds: int = 30
    gap_filter: GapFilterConfig = Field(default_factory=GapFilterConfig)


class CryptoLakeConfig(BaseModel):
    database: DatabaseConfig
    exchanges: ExchangesConfig
    redpanda: RedpandaConfig
    writer: WriterConfig = Field(default_factory=WriterConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)


def _apply_env_overrides(data: dict[str, Any], overrides: dict[str, str]) -> dict[str, Any]:
    for key, raw_value in overrides.items():
        target = data
        parts = key.lower().split("__")
        for part in parts[:-1]:
            target = target.setdefault(part, {})

        value: Any = raw_value
        if "," in raw_value:
            value = [item.strip() for item in raw_value.split(",")]

        target[parts[-1]] = value

    return data


def _normalize_env_overrides(overrides: dict[str, str]) -> dict[str, str]:
    normalized = dict(overrides)
    host_data_dir = normalized.pop("HOST_DATA_DIR", None)
    if host_data_dir is not None and "WRITER__BASE_DIR" not in normalized:
        normalized["WRITER__BASE_DIR"] = host_data_dir
    return normalized


def load_config(path: Path, env_overrides: dict[str, str] | None = None) -> CryptoLakeConfig:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    data = yaml.safe_load(path.read_text()) or {}

    if env_overrides is not None:
        overrides = _normalize_env_overrides(env_overrides)
    else:
        overrides = {
            key: value
            for key, value in os.environ.items()
            if "__" in key
            and key.split("__", 1)[0].lower() in {"database", "exchanges", "redpanda", "writer", "monitoring"}
        }
        if "HOST_DATA_DIR" in os.environ:
            overrides["HOST_DATA_DIR"] = os.environ["HOST_DATA_DIR"]
        overrides = _normalize_env_overrides(overrides)
    if overrides:
        data = _apply_env_overrides(data, overrides)

    try:
        return CryptoLakeConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigValidationError(str(exc)) from exc
