"""Backfill scheduler: runs gap backfill every 6 hours and exposes Prometheus metrics.

Starts/stops with the Docker Compose project. Metrics are scraped by Prometheus
so operators can see last-run time, duration, gaps found, and records written
on the dashboard.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import structlog
from prometheus_client import Gauge, start_http_server

from src.cli.gaps import analyze_archive, BACKFILLABLE_STREAMS, _hour_to_ms_range
from src.common.config import default_archive_dir

logger = structlog.get_logger()

INTERVAL_SECONDS = 6 * 3600  # 6 hours
METRICS_PORT = 8002

# ── Prometheus metrics ─────────────────────────────────────────────
backfill_last_run_ts = Gauge(
    "backfill_last_run_timestamp_seconds",
    "Unix timestamp of the last backfill run",
)
backfill_last_duration = Gauge(
    "backfill_last_run_duration_seconds",
    "Duration of the last backfill run in seconds",
)
backfill_gaps_found = Gauge(
    "backfill_gaps_found",
    "Number of missing hours found in the last run",
)
backfill_records_written = Gauge(
    "backfill_records_written",
    "Number of records written in the last run",
)
backfill_last_success = Gauge(
    "backfill_last_run_success",
    "1 if last backfill run succeeded, 0 if it failed",
)
backfill_runs_total = Gauge(
    "backfill_runs_total",
    "Total number of backfill runs since process start",
)


def _collect_missing_hours(report: dict) -> list[tuple[str, str, str, str, int]]:
    """Extract (exchange, symbol, stream, date, hour) for missing hours in backfillable streams."""
    missing = []
    for exch, symbols in report.items():
        for sym, streams in symbols.items():
            for stream, dates in streams.items():
                if stream not in BACKFILLABLE_STREAMS:
                    continue
                for date_name, info in dates.items():
                    hour_map = info["hours"]
                    for h in range(info.get("expect_from", 0), info.get("expect_to", 23) + 1):
                        if h not in hour_map:
                            missing.append((exch, sym, stream, date_name, h))
    return missing


async def _run_backfill_cycle(base_dir: str) -> None:
    """Run one backfill cycle: analyze archive, fetch missing data, update metrics."""
    from src.exchanges.binance import BinanceAdapter
    from src.cli.gaps import (
        _fetch_historical_all, _write_backfill_files,
        _next_backfill_seq, find_time_based_gaps, _fetch_by_id,
    )
    from src.cli.integrity import find_backfillable_gaps

    start = time.monotonic()
    run_ts = time.time()
    total_written = 0
    success = True

    try:
        report = analyze_archive(Path(base_dir))
        missing = _collect_missing_hours(report)

        # Initialize adapter and session_id for both missing hours and deep backfill
        adapter = BinanceAdapter(
            ws_base="wss://fstream.binance.com",
            rest_base="https://fapi.binance.com",
            symbols=[],
        )
        session_id = f"backfill-{datetime.now(timezone.utc).isoformat()}"

        # ── Missing hours backfill ──
        from src.cli.gaps import EndpointUnavailableError
        if not missing:
            logger.info("backfill_no_gaps_found")
        else:
            logger.info("backfill_starting", gaps=len(missing))

            unavailable_streams: set[str] = set()
            for exch, sym, stream, date_name, hour in missing:
                if stream in unavailable_streams:
                    continue
                start_ms, end_ms = _hour_to_ms_range(date_name, hour)
                try:
                    records = await _fetch_historical_all(
                        adapter, sym, stream, start_ms, end_ms)
                    if records:
                        backfill_seq = _next_backfill_seq(
                            Path(base_dir), exch, sym, stream, date_name, hour)
                        n, _ = _write_backfill_files(
                            records,
                            base_dir=base_dir,
                            exchange=exch,
                            symbol=sym,
                            stream=stream,
                            date=date_name,
                            session_id=session_id,
                            seq_offset=total_written,
                            backfill_seq=backfill_seq,
                        )
                        total_written += n
                        logger.info("backfill_hour_done",
                                    exchange=exch, symbol=sym, stream=stream,
                                    date=date_name, hour=hour, records=n)
                except EndpointUnavailableError as e:
                    logger.warning("backfill_endpoint_unavailable",
                                   stream=stream, error=str(e))
                    unavailable_streams.add(stream)
                except Exception as e:
                    logger.error("backfill_hour_failed",
                                 exchange=exch, symbol=sym, stream=stream,
                                 date=date_name, hour=hour, error=str(e))
                    success = False

        # ── Deep backfill: in-file gaps ──
        id_gaps = find_backfillable_gaps(Path(base_dir))
        time_gaps = find_time_based_gaps(Path(base_dir))
        deep_gaps = id_gaps + time_gaps
        backfill_gaps_found.set(len(missing) + len(deep_gaps))

        if deep_gaps:
            logger.info("backfill_deep_starting", id_gaps=len(id_gaps), time_gaps=len(time_gaps))

            for g in id_gaps:
                try:
                    records = await _fetch_by_id(adapter, g["symbol"], g["from_id"], g["to_id"])
                    if records:
                        backfill_seq = _next_backfill_seq(
                            Path(base_dir), g["exchange"], g["symbol"],
                            g["stream"], g["date"], g["hour"])
                        n, _ = _write_backfill_files(
                            records, base_dir=base_dir, exchange=g["exchange"],
                            symbol=g["symbol"], stream=g["stream"], date=g["date"],
                            session_id=session_id, seq_offset=total_written,
                            backfill_seq=backfill_seq,
                        )
                        total_written += n
                        logger.info("backfill_id_gap_done",
                                    exchange=g["exchange"], symbol=g["symbol"],
                                    stream=g["stream"], records=n)
                except Exception as e:
                    logger.error("backfill_id_gap_failed", error=str(e),
                                 exchange=g["exchange"], symbol=g["symbol"])
                    success = False

            for g in time_gaps:
                try:
                    records = await _fetch_historical_all(
                        adapter, g["symbol"], g["stream"], g["start_ms"], g["end_ms"])
                    if records:
                        backfill_seq = _next_backfill_seq(
                            Path(base_dir), g["exchange"], g["symbol"],
                            g["stream"], g["date"], g["hour"])
                        n, _ = _write_backfill_files(
                            records, base_dir=base_dir, exchange=g["exchange"],
                            symbol=g["symbol"], stream=g["stream"], date=g["date"],
                            session_id=session_id, seq_offset=total_written,
                            backfill_seq=backfill_seq,
                        )
                        total_written += n
                        logger.info("backfill_time_gap_done",
                                    exchange=g["exchange"], symbol=g["symbol"],
                                    stream=g["stream"], records=n)
                except Exception as e:
                    logger.error("backfill_time_gap_failed", error=str(e),
                                 exchange=g["exchange"], symbol=g["symbol"])
                    success = False

    except Exception as e:
        logger.error("backfill_cycle_failed", error=str(e))
        success = False

    elapsed = time.monotonic() - start
    backfill_last_run_ts.set(run_ts)
    backfill_last_duration.set(elapsed)
    backfill_records_written.set(total_written)
    backfill_last_success.set(1 if success else 0)
    backfill_runs_total.inc()

    logger.info("backfill_cycle_complete",
                elapsed_s=round(elapsed, 1),
                records_written=total_written,
                success=success)


async def main() -> None:
    base_dir = default_archive_dir()
    logger.info("backfill_scheduler_starting",
                interval_h=INTERVAL_SECONDS // 3600,
                metrics_port=METRICS_PORT,
                base_dir=base_dir)

    # Expose Prometheus metrics
    start_http_server(METRICS_PORT)

    run_count = 0
    while True:
        run_count += 1
        logger.info("backfill_run_starting", run=run_count)
        await _run_backfill_cycle(base_dir)

        logger.info("backfill_sleeping", next_run_in_h=INTERVAL_SECONDS // 3600)
        await asyncio.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
