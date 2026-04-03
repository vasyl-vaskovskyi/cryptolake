# Sealed Daily Archive Design

**Date:** 2026-04-03
**Status:** Draft

## Overview

After consolidation merges all hourly files into per-stream daily files, a sealing step packages all streams for a date into a single tar.zst archive at the symbol level. Sealed dates are immutable — analyze reads from the archive, integrity and backfill skip them. Scripts default to today's date with a max 3-day range.

## Goals

- One file per date per symbol: `binance/btcusdt/2026-04-02.tar.zst`
- Sealed dates cannot be modified by backfill or re-consolidated
- Analyze can still report on sealed dates by reading manifests from the archive
- Scripts default to today (UTC) when no date specified
- Date range limited to 3 days to prevent accidental full-archive scans

## Non-Goals

- S3/remote upload of archives (future phase)
- Unsealing archives (extract manually if needed)

## Sealed Archive Format

### Archive file

Location: `{base_dir}/{exchange}/{symbol}/{date}.tar.zst`
Sidecar: `{base_dir}/{exchange}/{symbol}/{date}.tar.zst.sha256`

A zstandard-compressed tar archive containing uncompressed JSONL files and metadata for all streams.

### Internal structure (flat)

```
trades-2026-04-02.jsonl
trades-2026-04-02.manifest.json
trades-2026-04-02.sha256
depth-2026-04-02.jsonl
depth-2026-04-02.manifest.json
depth-2026-04-02.sha256
bookticker-2026-04-02.jsonl
bookticker-2026-04-02.manifest.json
bookticker-2026-04-02.sha256
depth_snapshot-2026-04-02.jsonl
depth_snapshot-2026-04-02.manifest.json
depth_snapshot-2026-04-02.sha256
funding_rate-2026-04-02.jsonl
funding_rate-2026-04-02.manifest.json
funding_rate-2026-04-02.sha256
liquidations-2026-04-02.jsonl
liquidations-2026-04-02.manifest.json
liquidations-2026-04-02.sha256
open_interest-2026-04-02.jsonl
open_interest-2026-04-02.manifest.json
open_interest-2026-04-02.sha256
```

- `.jsonl` — uncompressed JSONL data (the tar.zst provides compression)
- `.manifest.json` — consolidation manifest with hour details, checksums, record counts
- `.sha256` — SHA256 hash of the original compressed `.jsonl.zst` file (audit trail)

## Sealing Process

Sealing is the final step of consolidation, after all streams are consolidated:

1. **Verify all streams are consolidated** — check that `{stream}/{date}.jsonl.zst` exists for each enabled stream
2. **Create tar.zst archive:**
   - For each stream's daily files (`{date}.jsonl.zst`, `{date}.manifest.json`, `{date}.jsonl.zst.sha256`):
     - Decompress the `.jsonl.zst` to raw JSONL
     - Add as `{stream}-{date}.jsonl` to the tar
     - Add manifest as `{stream}-{date}.manifest.json`
     - Add sha256 sidecar as `{stream}-{date}.sha256`
   - Compress the tar with zstandard
3. **Compute SHA256** of the archive, write `{date}.tar.zst.sha256` sidecar
4. **Remove per-stream daily files** — all `.jsonl.zst`, `.manifest.json`, `.jsonl.zst.sha256` files for this date across all stream directories

### Streaming approach

To avoid loading all data into memory, the tar is built incrementally:
- Open a zstd-compressed tar writer
- For each stream: decompress the `.jsonl.zst` and stream directly into the tar entry
- Close the tar and zstd writer

## Sealed Date Detection

A date is sealed when `{base_dir}/{exchange}/{symbol}/{date}.tar.zst` exists.

### Script behavior on sealed dates

| Script | Sealed date behavior |
|--------|---------------------|
| **analyze** | Read manifests from inside the archive. Show gap info from manifests and gap envelopes extracted from the archive. |
| **integrity** | Skip. Print: `"{date} is sealed. Skipping."` |
| **backfill** | Skip. Print: `"{date} is sealed. Skipping."` |
| **consolidation** | Skip. Already sealed. |

### Analyze on sealed dates

To read from a sealed archive without extracting:
1. Open the tar.zst for reading
2. Find manifest entries (`*-{date}.manifest.json`)
3. Parse manifests to get hour statuses and gap info
4. Find JSONL entries (`*-{date}.jsonl`) and scan for gap envelopes
5. Report as usual

## Script Default Date and Range Limits

### Default date

All three scripts (analyze, integrity, backfill) default to **today (UTC)** when no `--date`, `--date-from`, or `--date-to` is specified.

### Range limit

When `--date-from` and/or `--date-to` are specified, the range cannot exceed 3 days. If it does, the script exits with an error: `"Date range cannot exceed 3 days. Use a narrower range."`

`--date` (single day) is always allowed regardless of how old.

## Module Changes

### Consolidation (`src/cli/consolidate.py`)

- New function: `seal_daily_archive(base_dir, exchange, symbol, date, streams)` — creates the tar.zst and cleans up per-stream files
- `consolidate` CLI `run` command: after consolidating all streams, calls `seal_daily_archive`
- Consolidation scheduler: same — seals after consolidating

### Analyze (`src/cli/gaps.py`)

- `analyze_archive()`: detect sealed dates (`.tar.zst` at symbol level), read manifests and gap envelopes from archive
- CLI: default `--date` to today (UTC) when no date flags given
- CLI: enforce 3-day range limit

### Integrity (`src/cli/integrity.py`)

- `check_integrity()`: skip sealed dates with log message
- CLI: default `--date` to today (UTC) when no date flags given
- CLI: enforce 3-day range limit

### Backfill (`src/cli/gaps.py`)

- CLI: skip sealed dates
- CLI: default `--date` to today (UTC) when no date flags given
- CLI: enforce 3-day range limit
