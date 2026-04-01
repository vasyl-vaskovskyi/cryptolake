# Daily Consolidation Service Design

**Date:** 2026-04-02
**Status:** Draft

## Overview

A service that joins all hourly files (including late-arrival and backfill files) into a single compressed daily file per exchange/symbol/stream. It processes hours sequentially in a streaming fashion to keep memory usage bounded, verifies the output, then removes the hourly files while retaining SHA256 sidecars as an audit trail.

## Goals

- Produce one `{date}.jsonl.zst` file per stream per symbol per day
- Maintain chronological ordering by `exchange_ts` within each hour
- Explicitly mark missing periods with synthesized gap envelopes
- Keep memory usage bounded (one hour at a time) to run safely on an 8GB VPS
- Full traceability via manifest files and retained sidecars

## Non-Goals

- S3/remote archival (future phase)
- Automatic catch-up of missed days (operator uses CLI for older days)
- Re-sorting records across hour boundaries

## Architecture

### Processing Order

```
for each symbol (sequential):
    for each stream (sequential):
        consolidate(symbol, stream, target_date)
```

Symbols are processed one by one to bound memory usage. Streams are processed sequentially within each symbol.

### Pipeline Steps

For each `consolidate(symbol, stream, date)`:

1. **Discover files** — scan the date directory, classify each file by regex:
   - `hour-(\d{2}).jsonl.zst` -> base hourly file
   - `hour-(\d{2}).late-(\d+).jsonl.zst` -> late arrival
   - `hour-(\d{2}).backfill-(\d+).jsonl.zst` -> backfill data
   - Group files by hour (0-23)

2. **Check idempotency** — if `{stream}/{date}.jsonl.zst` already exists, skip this day/stream

3. **Identify missing hours** — any hour 00-23 with no files at all

4. **Open streaming zstd writer** for `{stream}/{date}.jsonl.zst` (compression level 3)

5. **For each hour 00-23 in order:**
   - If hour has files: decompress all files for that hour, merge records into a single list, sort by `exchange_ts`, stream-write to daily file, release from memory
   - If hour is completely missing: synthesize a gap envelope covering the full hour, write it

6. **Finalize** — close zstd writer, compute SHA256 sidecar, write manifest

7. **Verify** — stream-read the daily file, confirm record count, non-decreasing `exchange_ts`, SHA256 match

8. **Cleanup** — remove hourly `.jsonl.zst` files, keep `.sha256` sidecars

### File Discovery & Classification

**Input:** Date directory, e.g. `binance/btcusdt/trades/2026-03-28/`

**Grouping result** — dict keyed by hour (0-23):

```python
{
    14: {
        "base": Path("hour-14.jsonl.zst"),
        "late": [Path("hour-14.late-1.jsonl.zst")],
        "backfill": [Path("hour-14.backfill-1.jsonl.zst"), Path("hour-14.backfill-2.jsonl.zst")]
    },
    15: {
        "base": Path("hour-15.jsonl.zst"),
        "late": [],
        "backfill": []
    }
    # hours with no entry -> missing
}
```

**Merge order within an hour:** base file first, then late files (sorted by N), then backfill files (sorted by N). All records merged together and sorted by `exchange_ts`.

## Gap Envelope Synthesis

When an hour is completely missing (no base, no late, no backfill files), the consolidator synthesizes a gap envelope:

```json
{
    "v": 1,
    "type": "gap",
    "exchange": "binance",
    "symbol": "btcusdt",
    "stream": "trades",
    "received_at": "<consolidation_run_time_ns>",
    "collector_session_id": "consolidation-2026-03-29T02:30:00Z",
    "session_seq": -1,
    "gap_start_ts": "<hour_start_ns>",
    "gap_end_ts": "<hour_end_ns>",
    "reason": "missing_hour",
    "detail": "No data files found for hour HH; not recoverable via backfill"
}
```

- Reuses the existing gap envelope schema from `envelope.py`
- New reason value `"missing_hour"` added to valid reasons set
- `gap_start_ts` / `gap_end_ts` span the full hour (e.g., 14:00:00.000000000 to 14:59:59.999999999)
- Existing gap envelopes already present in hourly files pass through as-is into the daily file

## Streaming Writer

1. Open output file with `zstd.ZstdCompressor` streaming writer (level 3)
2. For each hour 00-23:
   - If missing: serialize gap envelope, write line to compressor
   - If present: decompress all files for that hour, parse records, sort by `exchange_ts`, write each record as JSONL line, release from memory
3. Close compressor (finalizes zstd frame)
4. Compute SHA256 of finished file, write `.sha256` sidecar
5. Track record counts (data envelopes, gap envelopes) per hour for the manifest

Peak memory: one hour of one stream. Worst case ~500-700MB uncompressed for depth on busy hours, acceptable on 8GB.

## Output File Layout

**Daily file:** `{base_dir}/{exchange}/{symbol}/{stream}/{date}.jsonl.zst`
- One level above the date directory
- Example: `binance/btcusdt/trades/2026-03-28.jsonl.zst`

**SHA256 sidecar:** `{date}.jsonl.zst.sha256`
- Same directory as daily file

**Manifest:** `{date}.manifest.json`
- Same directory as daily file

### Manifest Structure

```json
{
    "version": 1,
    "exchange": "binance",
    "symbol": "btcusdt",
    "stream": "trades",
    "date": "2026-03-28",
    "consolidated_at": "2026-03-29T02:30:00Z",
    "daily_file": "2026-03-28.jsonl.zst",
    "daily_file_sha256": "abc123...",
    "total_records": 145230,
    "data_records": 145218,
    "gap_records": 12,
    "hours": {
        "00": {"status": "present", "sources": ["hour-00.jsonl.zst"], "data_records": 6100},
        "01": {"status": "present", "sources": ["hour-01.jsonl.zst", "hour-01.late-1.jsonl.zst"], "data_records": 5980},
        "14": {"status": "missing", "synthesized_gap": true},
        "15": {"status": "backfilled", "sources": ["hour-15.backfill-1.jsonl.zst"], "data_records": 5400}
    },
    "missing_hours": [14],
    "source_files": [
        "hour-00.jsonl.zst",
        "hour-01.jsonl.zst",
        "hour-01.late-1.jsonl.zst",
        "hour-15.backfill-1.jsonl.zst"
    ]
}
```

**Hour status values:**
- `"present"` — base file exists (may include late/backfill supplements)
- `"backfilled"` — no base file, but backfill files exist
- `"missing"` — no files at all, gap envelope synthesized

## Verification

After writing the daily file, before cleanup:

1. **Stream-read the daily file** — decompress line by line (no full load)
2. **Check record count** — total lines must match the count tracked during writing
3. **Check ordering** — `exchange_ts` must be non-decreasing across records; gap envelopes use `gap_start_ts` for ordering comparison
4. **Check SHA256** — recompute hash of daily `.jsonl.zst`, compare against sidecar

**On verification failure:**
- Leave hourly files untouched (no cleanup)
- Delete the partial daily file + sidecar + manifest
- Log error with details (which check failed, at what record)
- Increment `consolidation_verification_failures_total` Prometheus counter
- Continue to next stream/symbol — don't abort entire run

## Cleanup

**After successful verification:**

1. Remove all hourly `.jsonl.zst` files (base, late, backfill) that were consolidated
2. Keep `.sha256` sidecar files in the date directory as audit trail
3. Date directory persists with sidecar files

**Safety:**
- Only files that were part of this consolidation are removed
- If anything fails before cleanup, hourly files remain intact
- The day can be re-consolidated if needed

**Idempotency:** If the daily file already exists at `{stream}/{date}.jsonl.zst`, the consolidator skips that stream/day and logs an info message.

## Service Architecture

### Standalone Docker Service

Following the backfill scheduler pattern:

- **Entry point:** `src/cli/consolidation_scheduler.py`
- **Schedule:** Runs once daily at configurable time (default: 02:30 UTC)
- **Target date:** Yesterday (`UTC now - 1 day`)
- **Processing:** Symbols sequential, streams sequential

### Prometheus Metrics (port 8003)

| Metric | Type | Description |
|--------|------|-------------|
| `consolidation_last_run_timestamp_seconds` | Gauge | Timestamp of last run |
| `consolidation_last_run_duration_seconds` | Gauge | Duration of last run |
| `consolidation_last_run_success` | Gauge | 1 if last run succeeded, 0 otherwise |
| `consolidation_runs_total` | Counter | Total consolidation runs |
| `consolidation_days_processed` | Counter | Days successfully consolidated |
| `consolidation_files_consolidated` | Counter | Hourly files consolidated |
| `consolidation_verification_failures_total` | Counter | Verification failures |
| `consolidation_missing_hours_total` | Counter | Synthesized missing-hour gaps |

### Configuration

Reads from existing `config.yaml`. One new field:

```yaml
consolidation:
  start_hour_utc: 2  # hour of day to run (default: 2, runs at 02:30)
```

### Docker Compose

New service `consolidation` alongside `collector`, `writer`, `backfill`. Mounts the same data volume.

### CLI Companion

```bash
python -m src.cli.consolidate --date 2026-03-28
```

For manual runs and catch-up of older days. Same core logic as the scheduled service.

## Module Structure

### New Files

- `src/cli/consolidation_scheduler.py` — service entry point, scheduling loop, Prometheus metrics
- `src/cli/consolidate.py` — core consolidation logic + CLI entry point
- `tests/unit/test_consolidate.py` — unit tests

### Existing Files Modified

- `src/common/envelope.py` — add `"missing_hour"` to valid gap reasons
- `docker-compose.yml` — add `consolidation` service
- `config/config.yaml` — add `consolidation` section

### Reused Utilities

- `src/common/envelope.py` — `create_gap_envelope()`
- `src/writer/file_rotator.py` — `compute_sha256()`, `sidecar_path()`
- `src/common/config.py` — config loading
- `src/common/jsonl_io.py` — JSONL reading utilities

### Key Functions in `consolidate.py`

- `consolidate_day(base_dir, exchange, symbol, stream, date)` — orchestrator
- `discover_hour_files(date_dir)` — file classification & grouping
- `merge_hour(hour, file_group)` — decompress, merge, sort by `exchange_ts`
- `synthesize_missing_hour_gap(exchange, symbol, stream, date, hour)` — gap envelope for missing hours
- `write_daily_file(output_path, hours_data_iterator)` — streaming zstd writer
- `write_manifest(manifest_path, stats)` — manifest JSON
- `verify_daily_file(daily_path, expected_count, sha256_path)` — verification pass
- `cleanup_hourly_files(date_dir, consolidated_files)` — remove .jsonl.zst, keep .sha256
