"""Consolidation scheduler: runs daily file consolidation and exposes Prometheus metrics.

Merges hourly archive files into single daily files per exchange/symbol/stream.
Runs once daily at a configurable hour (default 02:30 UTC).
"""
from __future__ import annotations

import asyncio
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import structlog
from prometheus_client import Counter, Gauge, start_http_server

from src.cli.consolidate import ALL_STREAMS, consolidate_day
from src.common.config import default_archive_dir

logger = structlog.get_logger()

METRICS_PORT = 8003
DEFAULT_START_HOUR = 2
DEFAULT_START_MINUTE = 30

# ── Prometheus metrics ─────────────────────────────────────────────
consolidation_last_run_ts = Gauge(
    "consolidation_last_run_timestamp_seconds",
    "Unix timestamp of the last consolidation run",
)
consolidation_last_duration = Gauge(
    "consolidation_last_run_duration_seconds",
    "Duration of the last consolidation run in seconds",
)
consolidation_last_success = Gauge(
    "consolidation_last_run_success",
    "1 if last consolidation run succeeded, 0 if it failed",
)
consolidation_runs_total = Counter(
    "consolidation_runs_total",
    "Total number of consolidation runs since process start",
)
consolidation_days_processed = Counter(
    "consolidation_days_processed",
    "Days successfully consolidated",
)
consolidation_files_consolidated = Counter(
    "consolidation_files_consolidated",
    "Hourly files consolidated into daily files",
)
consolidation_verification_failures = Counter(
    "consolidation_verification_failures_total",
    "Verification failures during consolidation",
)
consolidation_missing_hours = Counter(
    "consolidation_missing_hours_total",
    "Synthesized missing-hour gaps",
)


def _get_target_date() -> str:
    """Return yesterday's date as YYYY-MM-DD string."""
    yesterday = date.today() - timedelta(days=1)
    return yesterday.isoformat()


def _get_all_streams() -> list[str]:
    """Return all stream types to consolidate."""
    return list(ALL_STREAMS)


def _get_symbols(base_dir: str, exchange: str) -> list[str]:
    """Discover symbols from the archive directory structure."""
    exchange_dir = Path(base_dir) / exchange
    if not exchange_dir.is_dir():
        return []
    return sorted(
        d.name for d in exchange_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )


def _seconds_until_next_run(start_hour: int, start_minute: int) -> float:
    """Calculate seconds until the next scheduled run time."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


async def _run_consolidation_cycle(base_dir: str) -> None:
    """Run one consolidation cycle for all symbols and streams."""
    start = time.monotonic()
    run_ts = time.time()
    success = True

    try:
        target_date = _get_target_date()
        exchange = "binance"
        symbols = _get_symbols(base_dir, exchange)
        streams = _get_all_streams()

        logger.info("consolidation_cycle_starting", date=target_date,
                     symbols=len(symbols), streams=len(streams))

        for symbol in symbols:
            for stream in streams:
                date_dir = Path(base_dir) / exchange / symbol / stream / target_date
                if not date_dir.is_dir():
                    continue

                try:
                    result = consolidate_day(
                        base_dir=base_dir,
                        exchange=exchange,
                        symbol=symbol,
                        stream=stream,
                        date=target_date,
                    )

                    if result.get("skipped"):
                        logger.info("consolidation_stream_skipped",
                                    symbol=symbol, stream=stream, date=target_date)
                    elif result.get("success"):
                        n_missing = len(result.get("missing_hours", []))
                        consolidation_missing_hours.inc(n_missing)
                        consolidation_days_processed.inc()
                        logger.info("consolidation_stream_done",
                                    symbol=symbol, stream=stream,
                                    date=target_date,
                                    records=result["total_records"],
                                    missing_hours=n_missing)
                    else:
                        consolidation_verification_failures.inc()
                        success = False
                        logger.error("consolidation_stream_failed",
                                     symbol=symbol, stream=stream,
                                     date=target_date,
                                     error=result.get("error"))

                except Exception as e:
                    logger.error("consolidation_stream_error",
                                 symbol=symbol, stream=stream,
                                 date=target_date, error=str(e))
                    success = False

    except Exception as e:
        logger.error("consolidation_cycle_failed", error=str(e))
        success = False

    elapsed = time.monotonic() - start
    consolidation_last_run_ts.set(run_ts)
    consolidation_last_duration.set(elapsed)
    consolidation_last_success.set(1 if success else 0)
    consolidation_runs_total.inc()

    logger.info("consolidation_cycle_complete",
                elapsed_s=round(elapsed, 1), success=success)


async def main() -> None:
    base_dir = default_archive_dir()
    start_hour = DEFAULT_START_HOUR
    start_minute = DEFAULT_START_MINUTE

    logger.info("consolidation_scheduler_starting",
                metrics_port=METRICS_PORT,
                base_dir=base_dir,
                run_at=f"{start_hour:02d}:{start_minute:02d} UTC")

    start_http_server(METRICS_PORT)

    run_count = 0
    while True:
        sleep_secs = _seconds_until_next_run(start_hour, start_minute)
        logger.info("consolidation_sleeping",
                     next_run_in_h=round(sleep_secs / 3600, 1))
        await asyncio.sleep(sleep_secs)

        run_count += 1
        logger.info("consolidation_run_starting", run=run_count)
        await _run_consolidation_cycle(base_dir)


if __name__ == "__main__":
    asyncio.run(main())
