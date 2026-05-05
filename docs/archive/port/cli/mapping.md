---
module: cli
status: complete
produced_by: analyst
python_files: [__init__.py, verify.py, consolidate.py, consolidation_scheduler.py, backfill_scheduler.py, gaps.py, integrity.py]
python_test_files: [test_verify.py, test_cli_date_defaults.py, test_consolidation_scheduler.py, test_deep_backfill.py, test_backfill.py, test_gap_analyzer.py, test_integrity_gaps.py]
---

## 1. Module summary

The CLI module provides verification, consolidation, backfill, gap analysis, and integrity-checking utilities for the CryptoLake archive. The primary entry point is the `verify` command, which gates gate 5 of all upstream modules by enforcing byte-identical archive output across the Python and Java implementations. The module also includes schedulers for daily consolidation (merges hourly files into daily archives) and 6-hourly gap backfill, utilities for analyzing missing data and interpolating funding-rate records, and integrity checkers that validate Binance sequence IDs within raw data. All commands are exposed via Click CLI groups, with optional Prometheus metrics exposed by the scheduler services.

## 2. File inventory

| File | Lines | Role | Public symbols |
|------|-------|------|-----------------|
| `__init__.py` | 2 | package init | none |
| `verify.py` | 430 | archive verification (gate 5 primary) | `cli` (Click group), `verify`, `manifest`, `mark_maintenance` (commands); `verify_checksum`, `verify_envelopes`, `check_duplicate_offsets`, `report_gaps`, `decompress_and_parse`, `verify_depth_replay`, `generate_manifest` (utilities) |
| `consolidate.py` | 987 | hourly-to-daily file merge | `consolidate_day`, `seal_daily_archive`, `consolidate_stream_to_tar` (CLI entry points); `discover_hour_files`, `merge_hour`, `write_daily_file`, `verify_daily_file`, `synthesize_missing_hour_gap`, `cleanup_hourly_files` (utilities) |
| `consolidation_scheduler.py` | 189 | daily consolidation daemon | `main` (async entry point for scheduling); Prometheus gauges/counters for metrics |
| `gaps.py` | 1634 | gap analysis and backfill | `cli` (Click group), `backfill`, `analyze` (commands); `_interpolate_funding_rate_gap`, `_fetch_historical_page`, `_fetch_by_id` (REST); `STREAM_TS_KEYS`, `ENDPOINT_WEIGHTS`, `BACKFILLABLE_STREAMS` (constants) |
| `backfill_scheduler.py` | 229 | 6-hourly backfill daemon | `main` (async entry point); Prometheus gauges/counters |
| `integrity.py` | 632 | archive ID continuity checker | `cli` (Click group), `check_trades`, `check_depth`, `check_bookticker` (commands); `_check_trades`, `_check_depth`, `_check_bookticker` (utilities) |

## 3. Public API surface

### `verify.py`

**`@click.group() def cli()`**  
Entry point for the verify CLI group. No parameters. Docstring: "CryptoLake data verification CLI."

**`@cli.command() def verify(date, base_dir, exchange, symbol, stream, full, repair_checksums)`**  
Signature: `verify(date: str, base_dir: str = DEFAULT_ARCHIVE_DIR, exchange: str = None, symbol: str = None, stream: str = None, full: bool = False, repair_checksums: bool = False) -> None`  
Semantics: Walks archive directory tree for date, verifies SHA-256 sidecars, deserializes and validates envelope fields, checks for duplicate (topic, partition, offset) tuples, optionally performs cross-file duplicate detection, and (if `full` flag) runs depth-diff pu-chain replay validation. Outputs human-readable errors and gap summary to stdout. Raises `SystemExit(1)` if any errors found. **CRITICAL**: This command's stdout is the SOURCE OF TRUTH for gate 5. The Java port must produce byte-identical output for the same archive set.

**`@cli.command() def manifest(date, base_dir, exchange)`**  
Signature: `manifest(date: str, base_dir: str = DEFAULT_ARCHIVE_DIR, exchange: str = "binance") -> None`  
Semantics: Generates a JSON manifest file (`{date}.manifest.json`) for each stream directory, summarizing hourly record counts, gaps, and hours found. Outputs JSON to stdout. Outputs JSON to stdout.

**`@cli.command("mark-maintenance") def mark_maintenance(db_url, scope, maintenance_id, reason, ttl_minutes)`**  
Signature: `mark_maintenance(db_url: str, scope: str, maintenance_id: str, reason: str, ttl_minutes: int = 60) -> None`  
Semantics: Records a planned maintenance intent to PostgreSQL without shutting down services. Constructs a `MaintenanceIntent` record with UTC timestamps and persists it. Outputs confirmation message to stdout.

**`verify_checksum(data_path: Path, sidecar_path: Path) -> list[str]`**  
Reads `sidecar_path` (format: `{hex}  {filename}\n`) and compares SHA-256 of `data_path` (a .jsonl.zst file) against the hex digest. Returns empty list if match, else list of error strings.

**`verify_envelopes(envelopes: list[dict]) -> list[str]`**  
For each envelope, checks required fields per type (gap or data). For data envelopes, re-computes raw_sha256 over `raw_text` as bytes and compares. Returns error strings for violations.

**`check_duplicate_offsets(envelopes: list[dict]) -> list[str]`**  
Collects (topic, partition, offset) tuples, skipping records with `_offset < 0`. Returns error strings for duplicates.

**`report_gaps(envelopes: list[dict]) -> list[dict]`**  
Returns all envelopes with `type == "gap"`, preserving all fields including optional restart metadata.

**`decompress_and_parse(file_path: Path) -> list[dict]`**  
Decompresses a .jsonl.zst file and parses each line as JSON. Returns list of dicts.

**`verify_depth_replay(depth_envelopes: list[dict], snapshot_envelopes: list[dict], gap_envelopes: list[dict]) -> list[str]`**  
Validates pu-chain continuity for depth diffs across snapshots, per symbol. Accepts gaps as pu-chain breaks. Returns error strings.

**`generate_manifest(base_dir: Path, exchange: str, date: str) -> dict`**  
Scans archive directory for files matching `{exchange}/{symbol}/{stream}/{date}/hour-*.jsonl.zst` and returns a manifest dict with symbol/stream/hour structure, record counts, and gap metadata.

### `consolidate.py`

**`consolidate_day(base_dir: str, exchange: str, symbol: str, stream: str, date: str) -> dict`**  
Merges all hour-*.jsonl.zst files (plus late-*.jsonl.zst and backfill-*.jsonl.zst variants) for one date/exchange/symbol/stream into a single {date}.jsonl.zst file. Sorts data records by natural key (trade ID, depth update ID, or exchange_ts), inserts gap envelopes by received_at, writes daily file, computes SHA-256 sidecar, writes manifest, verifies ordering, and cleans up hourly files. Returns dict with success/error, record counts, and missing hours.

**`seal_daily_archive(base_dir: str, exchange: str, symbol: str, date: str) -> dict`**  
Packages all per-stream daily files for a date into a single tar.zst archive. Decompresses each stream's .jsonl.zst into raw JSONL inside the tar, adds .manifest.json entries, and writes a .tar.zst.sha256 sidecar.

**`consolidate_stream_to_tar(tf: tarfile.TarFile, base_dir: str, exchange: str, symbol: str, stream: str, date: str) -> dict`**  
Consolidates one stream into an open tar file in a two-pass approach: pass 1 computes size and stats, pass 2 writes bytes into tar entries. Memory-bounded processing.

**`discover_hour_files(date_dir: Path) -> dict[int, dict]`**  
Scans a date directory and classifies files by hour (0-23). Returns dict with keys 0-23, each value a dict with "base", "late", "backfill" file lists.

**`merge_hour(hour: int, file_group: dict, stream: str = "") -> list[dict]`**  
Merges all files for one hour, sorts data records by natural key (per stream), and inserts gap envelopes by received_at. Returns list of dicts in order.

**`synthesize_missing_hour_gap(...) -> dict`**  
Creates a gap envelope for a missing hour with reason "missing_hour".

**`write_daily_file(output_path: Path, hour_records: Iterator) -> dict`**  
Writes hourly records (or raw file paths for streaming) to a zstd-compressed daily JSONL file. Returns stats dict with record counts per hour.

**`verify_daily_file(daily_path: Path, expected_count: int, sha256_path: Path, stream: str = "") -> tuple[bool, str | None]`**  
Verifies SHA-256, record count, and ordering (data records by natural key; gaps skipped in ordering checks). Returns (success, error_string).

### `gaps.py`

**`@click.group() def cli()`**  
Entry point for gaps CLI group.

**`@cli.command() def backfill(base_dir, exchange, symbol, stream, date, dry_run, json_output)`**  
Analyzes archive for missing hours in backfillable streams, fetches from Binance REST APIs (trades, funding_rate, liquidations, open_interest), writes backfill-*.jsonl.zst files. `--dry-run` shows plan without writing. Only handles backfillable streams; others reported as unrecoverable.

**`@cli.command() def analyze(...)`**  
Analyzes archive for missing hours per stream/date, reports gaps and backfillable status. Outputs human-readable or JSON.

**`_interpolate_funding_rate_gap(before_record: dict, after_record: dict, gap_start_ms: int, gap_end_ms: int, symbol: str) -> list[dict]`**  
Generates interpolated markPriceUpdate records (one per second) by linear interpolation of mark price, index price, and premium between boundary records. Outputs prices as strings with 8-decimal precision.

**`_fetch_historical_page(session: aiohttp.ClientSession, url: str, max_retries: int = 5) -> list[dict]`**  
Fetches a single REST page with retry logic for 429/418. Raises `EndpointUnavailableError` on 400/403/5xx.

**`_wrap_backfill_envelope(raw_record: dict, exchange: str, symbol: str, stream: str, session_id: str, seq: int, exchange_ts_key: str) -> dict`**  
Wraps a Binance raw record into a data envelope with backfill metadata.

**Constants**:  
`BACKFILLABLE_STREAMS = frozenset({"trades", "funding_rate", "open_interest"})`  
`NON_BACKFILLABLE_STREAMS = frozenset({"depth", "depth_snapshot", "bookticker", "liquidations"})`  
`STREAM_TS_KEYS = {"trades": "T", "funding_rate": "E", "liquidations": "time", "open_interest": "timestamp"}`  
`ENDPOINT_WEIGHTS = {...}` per stream  
`BINANCE_REST_BASE = "https://fapi.binance.com"`

### `consolidation_scheduler.py`

**`async def main() -> None`**  
Starts HTTP metrics server on port 8003, sleeps until next scheduled consolidation time (default 02:30 UTC, configurable via `CONSOLIDATION_START_HOUR_UTC` env var), runs consolidation cycle for all symbols and streams in yesterday's date directory, updates Prometheus metrics, and loops.

**Prometheus metrics**:  
`consolidation_last_run_timestamp_seconds`, `consolidation_last_run_duration_seconds`, `consolidation_last_run_success`, `consolidation_runs_total`, `consolidation_days_processed`, `consolidation_files_consolidated`, `consolidation_verification_failures_total`, `consolidation_missing_hours_total`.

### `backfill_scheduler.py`

**`async def main() -> None`**  
Starts HTTP metrics server on port 8002, runs backfill cycle every 6 hours, updates Prometheus gauges, and loops.

**Prometheus metrics**:  
`backfill_last_run_timestamp_seconds`, `backfill_last_run_duration_seconds`, `backfill_gaps_found`, `backfill_records_written`, `backfill_last_run_success`, `backfill_runs_total`.

### `integrity.py`

**`@click.group() def cli()`**  
Entry point for integrity CLI group.

**`@cli.command() def check_trades(base_dir, exchange, symbol, date, json_output)`**  
Checks aggregate trade ID ("a") continuity for trades stream. Reports any gaps as JSON or human-readable.

**`@cli.command() def check_depth(base_dir, exchange, symbol, date, json_output)`**  
Checks depth diff pu-chain continuity. Reports breaks.

**`@cli.command() def check_bookticker(base_dir, exchange, symbol, date, json_output)`**  
Checks bookticker update ID ("u") for backwards jumps (duplication/out-of-order). Forwards jumps OK (other symbols).

**`_check_trades(files: list[Path]) -> tuple[int, list[dict]]`**  
Returns (record_count, breaks) where breaks are dicts with "field", "expected", "actual", "missing", "at_received".

**`_check_depth(files: list[Path]) -> tuple[int, list[dict]]`**  
Returns (record_count, breaks) for pu-chain.

**`_check_bookticker(files: list[Path]) -> tuple[int, list[dict]]`**  
Returns (record_count, breaks) for backwards jumps only.

**Constants**:  
`CHECKABLE_STREAMS = {"trades", "depth", "bookticker"}`  
`ALL_STREAMS = {"trades", "depth", "depth_snapshot", "bookticker", "funding_rate", "liquidations", "open_interest"}`

## 4. Internal structure

### Key call graphs and dependencies

**`verify.py` call graph**:
- `cli()` → `verify()`, `manifest()`, `mark_maintenance()`
- `verify()` calls:
  - `decompress_and_parse()` (loads archives)
  - `verify_checksum()` (file integrity)
  - `verify_envelopes()` (envelope structure, raw_sha256)
  - `check_duplicate_offsets()` (dedup)
  - `verify_depth_replay()` (pu-chain validation) — central complex function
    - Builds symbol-based indexes of depth/snapshot/gap envelopes
    - Validates pu-chain per symbol, with gap window tolerance
    - ~110 lines; cyclomatic complexity ~8 (nested conditions for snapshot spanning and re-sync)
  - `report_gaps()` (gap extraction)
  - `generate_manifest()` (manifest generation)
- `manifest()` calls `generate_manifest()`

**`consolidate.py` call graph**:
- `consolidate_day()` is the primary CLI-facing function
  - Calls `discover_hour_files()`, `merge_hour()`, `write_daily_file()`, `verify_daily_file()`, `cleanup_hourly_files()`
- `merge_hour()` ~ 50 lines; merges data/gap records, sorts by natural key
- `write_daily_file()` ~ 70 lines; streams or serializes records to zstd JSONL
- `verify_daily_file()` ~ 60 lines; validates ordering and SHA-256
- `seal_daily_archive()` orchestrates tar.zst packing
- `consolidate_stream_to_tar()` packs stream into tar in two passes (memory-bounded)
- Dependencies: imports `consolidate` functions from gaps.py (`analyze_archive`, `BACKFILLABLE_STREAMS`)

**`gaps.py` call graph**:
- `cli()` → `backfill()`, `analyze()`
- `backfill()`:
  - Calls `analyze_archive()` (discovers missing hours)
  - Calls `_fetch_historical_all()` (REST fetch for missing hours)
  - Calls `_fetch_by_id()` (for deep ID-based backfill)
  - Calls `_write_backfill_files()` (writes backfill-*.jsonl.zst)
  - Calls `find_backfillable_gaps()` from integrity.py
  - ~500+ lines; orchestration logic
- `_interpolate_funding_rate_gap()` ~ 50 lines; interpolates funding rate records
- `_fetch_historical_page()` ~ 15 lines; REST wrapper with retry
- Dependencies: imports `BinanceAdapter` from exchanges.binance

**`consolidation_scheduler.py`**:
- `main()` → `_run_consolidation_cycle()`
- `_run_consolidation_cycle()` calls `consolidate_day()` for each symbol/stream/date
- Metrics tracking; async sleep loop

**`backfill_scheduler.py`**:
- `main()` → `_run_backfill_cycle()`
- `_run_backfill_cycle()` calls `analyze_archive()`, then backfill/interpolation functions
- Metrics tracking; async sleep loop

**`integrity.py` call graph**:
- `cli()` → `check_trades()`, `check_depth()`, `check_bookticker()`
- Each command calls corresponding `_check_*()` utility
- `_check_*()` utilities call `_stream_data_records()` which handles merging and deduplication

### God objects and complexity hotspots

- **`verify_depth_replay()` in verify.py** (~110 lines): validates pu-chain per symbol with complex snapshot/gap window logic. Nested loops over snapshots, diffs, gaps. Cyclomatic complexity ~8. This is the most complex function in the module and central to gate 5 correctness.
- **`consolidate_day()` in consolidate.py** (~140 lines): orchestrates merge, sort, write, verify, cleanup. Calls multiple helpers but no deeply nested logic.
- **`_run_consolidation_cycle()` in consolidation_scheduler.py** (~70 lines): loops over symbols/streams/dates, updates metrics. Straightforward orchestration.
- **`_run_backfill_cycle()` in backfill_scheduler.py** (~100 lines): analyzes archive, fetches missing data, handles unavailable streams, updates metrics.

### No truly god objects; functions are well-factored. Longest functions:
- `consolidate_day()` at 140 lines (orchestration, not monolithic)
- `_run_consolidation_cycle()` at 70 lines (simple loop)
- `_run_backfill_cycle()` at 100 lines (orchestration with error handling)

## 5. Concurrency surface

### Async/await patterns

**`consolidation_scheduler.py`**:
- `async def main()`: async context
- `await asyncio.sleep(sleep_secs)`: interruptible sleep for schedule delay
- `await _run_consolidation_cycle(base_dir)`: awaits consolidation cycle (which is NOT async internally but wrapped for symmetry)

**`backfill_scheduler.py`**:
- `async def main()`: async context
- `await asyncio.sleep(...)`: schedule sleep
- `await _run_backfill_cycle(base_dir)`: awaits cycle

**`gaps.py` (backfill command)**:
- `async with aiohttp.ClientSession()`: HTTP session
- `await _fetch_historical_page(session, url, ...)`: async REST fetch
- `await _fetch_historical_all(...)`: async wrapper for multi-page fetch
- `await asyncio.sleep(retry_after)`: rate-limit backoff
- No explicit task spawning; sequential REST calls only.

**`verify.py` (mark_maintenance command)**:
- `asyncio.run(_write_intent())`: runs async intent-write function
- `async def _write_intent()`: nested async function
- `await sm.connect()`: connects to StateManager
- `await sm.create_maintenance_intent(intent)`: persists to DB
- `await sm.close()`: closes connection

### No structural concurrency (TaskGroups, gather). Sequential processing dominated by:
- File system walks and reads (blocking, on virtual threads in Java)
- Zstd compression/decompression (blocking, on virtual threads)
- REST polling with sleeps (natural async in Python, blocks on virtual threads in Java)

### Backpressure: None explicit. Consolidation and backfill are batch-mode, not streaming.

### Key observations for Java port:
- No `asyncio.create_task()` or `gather()` fan-out — all operations are sequential
- `asyncio.run()` wraps small async blocks; translate to blocking calls on main thread or virtual thread executor
- `aiohttp.ClientSession` → `HttpClient` (one per service, Tier 2 rule 14)
- `asyncio.sleep()` → `Thread.sleep()` or `LockSupport.parkNanos()` (on virtual thread, no wrapper needed)

## 6. External I/O

### Kafka
**Outgoing**: None. The CLI module does not produce to Kafka. It consumes archives written by the collector/writer.

### HTTP / WebSocket
**Outgoing (gaps.py)**:
- Binance REST APIs:
  - `GET /fapi/v1/klines` for mark-price, index-price, premium-price klines (1-minute bars)
  - `GET /fapi/v1/fundingRate` for historical funding rates
  - `GET /fapi/v1/allAggTrades`, `GET /fapi/v1/forceOrders`, `GET /fapi/v1/openInterest`
- Base URL: `https://fapi.binance.com`
- No incoming WebSocket connections

### Filesystem
**Reading**:
- Archive directory structure: `{base_dir}/{exchange}/{symbol}/{stream}/{date}/hour-*.jsonl.zst`
- Sidecar files: `hour-*.jsonl.zst.sha256` (format: `{hex_digest}  {filename}\n`)
- Manifest files: `{date}.manifest.json`

**Writing**:
- Consolidated daily files: `{stream_dir}/{date}.jsonl.zst`
- Daily sidecars: `{date}.jsonl.zst.sha256`
- Manifest files: `{date}.manifest.json`
- Backfill files: `hour-{h}.backfill-{seq}.jsonl.zst`
- Sealed archive: `{symbol_dir}/{date}.tar.zst`

**Key paths**:
- Base archive dir: `default_archive_dir()` from config (typically `/data/cryptolake`)
- Subdirs: `exchange/symbol/stream/date/`

### Database (PostgreSQL)
**via StateManager** (`mark_maintenance` command):
- Connection: `db_url` parameter (connection string)
- Operation: `create_maintenance_intent(intent: MaintenanceIntent)`
- Schema: `component_runtime_state` table (implied by context)
- No transaction management exposed; async wrapper around blocking DB calls

### Metrics / Observability
**Prometheus**:
- Schedulers expose HTTP metrics on ports 8002 (backfill) and 8003 (consolidation)
- Metrics: Gauges (last_run_ts, duration, success) and Counters (runs_total, records_written, etc.)
- No metric labels (simple counters and gauges, not tagged per symbol/stream)

### Environment variables
- `CONSOLIDATION_START_HOUR_UTC`: hour to run consolidation (default 2, meaning 02:30 UTC)
- Config file: YAML at path from `default_archive_dir()` context

## 7. Data contracts

### Envelopes (from src/common/envelope.py, referenced in verify.py)

**Data envelope field set** (from BROKER_COORD_FIELDS + DATA_ENVELOPE_FIELDS):
```
v: int (version)
type: "data"
exchange: str
symbol: str (lowercase)
stream: str
received_at: int (nanoseconds, from time.time_ns())
exchange_ts: int (milliseconds)
collector_session_id: str
session_seq: int
raw_text: str (JSON string, exact bytes preserved)
raw_sha256: str (hex digest)
_topic: str (Kafka topic)
_partition: int
_offset: long (or -1 for synthetic records)
```

**Gap envelope field set** (from GAP_ENVELOPE_FIELDS + BROKER_COORD_FIELDS):
```
v: int
type: "gap"
exchange: str
symbol: str
stream: str
received_at: int (nanoseconds)
collector_session_id: str
session_seq: int (always -1 for gap envelopes)
gap_start_ts: long (nanoseconds)
gap_end_ts: long (nanoseconds)
reason: str (fixed vocabulary: "ws_disconnect", "pu_chain_break", "session_seq_skip", "buffer_overflow", "snapshot_poll_miss", "collector_restart", "restart_gap", "recovery_depth_anchor", "write_error", "deserialization_error", "checkpoint_lost", "missing_hour")
detail: str
component: str (optional, restart_gap only: "ws_collector", "snapshot_scheduler", etc.)
cause: str (optional: "oom_kill", "upgrade", "sigterm", etc.)
planned: bool (optional)
maintenance_id: str (optional: reference to maintenance intent)
_topic: str
_partition: int
_offset: long (-1 for synthetic records)
```

Note: Restart-gap envelopes have additional optional fields appended after required fields; Jackson serialization order matters (Tier 5 rule B1).

### Manifest JSON format
```
{
  "version": 1,
  "exchange": str,
  "symbol": str,
  "stream": str,
  "date": str (YYYY-MM-DD),
  "consolidated_at": str (ISO-8601 UTC timestamp),
  "daily_file": str (filename),
  "daily_file_sha256": str (hex digest),
  "total_records": int,
  "data_records": int,
  "gap_records": int,
  "hours": {
    "0": {"status": "present"|"backfilled"|"late"|"missing", "data_records": int, "gap_records": int, "sources": {...}},
    ...
  },
  "missing_hours": [int, ...],
  "source_files": [str, ...]
}
```

### Sort keys (per stream)
- `trades`: Binance aggregate trade ID ("a") field → int/long
- `depth`: Binance depth update ID ("u") field → int/long
- `bookticker`: Binance update ID ("u") field → int/long
- Other streams: exchange_ts (milliseconds) → int/long

Note: Tier 5 rule M5 — all sort keys are `long`, never `int`, to avoid overflow on Binance IDs that exceed 2^31.

### Timestamp formats
- `received_at`: nanoseconds since epoch, int64 (Python `time.time_ns()`)
- `exchange_ts`: milliseconds since epoch, int32 or int64 (depends on stream; verified as long in Java to avoid overflow)
- `gap_start_ts`, `gap_end_ts`: nanoseconds since epoch, int64
- ISO-8601 timestamps: `datetime.now(timezone.utc).isoformat()` → `"2026-04-18T12:34:56.123456+00:00"` (with fractional seconds)

### Binance number fields (in raw_text)
- Prices (`p`, `ap`, `P`, `r`): strings with 8-decimal precision (Tier 5 rule M4)
- IDs (`a`, `u`, `U`, `pu`): integers (large, may exceed int32)
- Timestamps (`E`, `T`, `timestamp`): integers (milliseconds)

## 8. Test catalog

### test_verify.py (24 tests)
- `TestChecksumVerification::test_valid_checksum`: checksum match
- `TestChecksumVerification::test_corrupted_file`: detects corruption
- `TestChecksumVerification::test_missing_sidecar`: detects missing sidecar
- `TestEnvelopeValidation::test_valid`: valid envelope passes
- `TestEnvelopeValidation::test_sha256_mismatch`: detects raw_sha256 mismatch
- `TestEnvelopeValidation::test_missing_field`: detects missing required field
- `TestDuplicateOffsets::test_no_duplicates`: clean dedup check
- `TestDuplicateOffsets::test_duplicate`: detects duplicate offset
- `TestGapReporting::test_reports_gaps`: extracts gap envelopes
- `TestGapReporting::test_no_gaps`: returns empty list
- `TestGapReporting::test_restart_gap_preserves_metadata`: preserves optional restart fields
- `TestRestartGapEnvelopeValidation`: verifies restart_gap envelopes with optional fields
- `TestDepthReplay`: complex pu-chain validation tests (15+ sub-tests for snapshot spanning, re-sync, chain breaks)

**Invariants tested**: Tier 1 #1 (raw_sha256), #4 (file durability — implicit via checksum), #5 (gap detection)

### test_backfill.py (3 tests)
- `test_backfill_dry_run_shows_plan`: dry-run mode
- `test_backfill_skips_non_backfillable_streams`: respects stream type
- `test_backfill_skips_already_backfilled`: avoids duplicate backfill

### test_gap_analyzer.py (4 tests)
- `test_analyze_full_day_no_gaps`: clean archive
- `test_analyze_missing_hours`: detects missing hours
- `test_analyze_backfill_files_count_as_covered`: counts backfill files
- `test_analyze_json_output`: JSON output format

### test_consolidation_scheduler.py (1 test)
- Minimal; mostly schedule calculation verification

### test_integrity_gaps.py (3 tests)
- Tests for trade ID continuity, depth pu-chain, bookticker update ID

### test_deep_backfill.py (integration-style tests)
- Tests ID-based gap detection and backfill reconstruction

## 9. Invariants touched (Tier 1 rules)

1. **`raw_text` is captured BEFORE any JSON parse** (Tier 1 #1)
   - Touched in `verify.py:38-50` where `verify_envelopes()` accepts raw_text from envelopes already deserialized from JSONL
   - Tier 1 #1 is a collector-side invariant; CLI does not capture raw_text (it reads pre-captured envelopes from archive)
   - But CLI validates that raw_sha256 matches re-hashed raw_text bytes (`verify_envelopes()` line 45)

2. **`raw_sha256` computed over exact bytes once at capture** (Tier 1 #2)
   - Touched in `verify.py:44-49` where `verify_envelopes()` re-computes SHA-256 over `raw_text.encode()` and compares
   - Validates that envelopes in archive have correct sha256; does NOT re-compute or regenerate

3. **Disabled streams emit zero artifacts** (Tier 1 #3)
   - Not applicable to this module (no stream disabling logic; all enabled streams in config are processed)

4. **Kafka consumer offsets committed only AFTER file flush** (Tier 1 #4)
   - Not applicable; CLI does not commit offsets (writer does)
   - But consolidation honors this indirectly: `cleanup_hourly_files()` (line 377) removes only AFTER daily file verified and flushed

5. **Every detected gap emits metric, log, AND archived gap record** (Tier 1 #5)
   - Touched in `consolidate.py:154-178` where `synthesize_missing_hour_gap()` creates a gap envelope for missing hours
   - Touched in `consolidation_scheduler.py:128,137` where gap counts and failures update metrics
   - Touched in `consolidation_scheduler.py:131,139` where gaps logged

6. **Recovery prefers replay from Kafka / exchange cursors over inferred reconstruction** (Tier 1 #6)
   - Touched in `gaps.py` backfill command which fetches from Binance REST (preferring historical fetch over reconstruction)
   - Touched in `consolidate.py:154-178` which CREATES missing-hour gaps (does NOT reconstruct; explicit gap emitted)

7. **JSON codec must not re-order, re-quote, re-format `raw_text`** (Tier 1 #7)
   - Touched in `consolidate.py:103-151` where `merge_hour()` reads raw_text as-is from deserialized envelopes and re-serializes via `orjson.dumps(record)`
   - Tier 1 #7 is a JSON round-trip guarantee: the exact `raw_text` string field is preserved on envelope output
   - `consolidate.py:249-251` writes `orjson.dumps(record)` to output; order preserved by orjson insertion-order guarantee

## 10. Port risks

### Gate 5 byte-identity requirement
**CRITICAL**: The `verify.py` CLI's stdout is the sole source of truth for gate 5. The Java port MUST produce byte-identical output (same error messages, same order, same format) for the same archive set. Any deviation will cause gate 5 re-runs with updated fixtures.

Risk areas:
- Error message formatting and ordering (currently emitted in order of file/record discovery)
- Floating-point formatting in interpolation (Tier 5 rule M4 — 8-decimal precision required)
- Timestamp serialization in manifests (ISO-8601 format with milliseconds)
- pu-chain validation logic (complex nested loop; must preserve exact error messages and order)

**Mitigation**: Establish fixture equivalence tests BEFORE porting to Java. Run Java `verify` against Python-generated archives and compare stdout diff.

### orjson insertion-order preservation
**Risk**: Jackson's default alphabetical property ordering breaks byte identity on envelope re-serialization.
**Mitigation**: Apply Tier 5 rule B1 (@JsonPropertyOrder annotation with exact field list) on data and gap envelope records.

### Zstd frame format
**Risk**: zstd-jni `Zstd.compress(bytes, level)` must produce single-frame output matching Python `zstandard.ZstdCompressor(level=3).compress(data)`.
**Mitigation**: Verify zstd-jni behavior in CI. Tier 5 rule I1 mandates single independent frame per batch (not ZstdOutputStream for whole file).

### Long vs int overflow on Binance IDs
**Risk**: Sort keys (trades "a", depth "u") exceed 2^31. Python hides unbounded int; Java must use `long`.
**Mitigation**: Tier 5 rule E1 — all ID fields are `long`. Consolidation sort keys must be `long`.

### Funding rate interpolation precision
**Risk**: 8-decimal format strings must be exact (`"65432.10000000"` not `"6.543210e+4"`).
**Mitigation**: Tier 5 rule M4 — use `String.format(Locale.ROOT, "%.8f", value)`.

### SHA-256 sidecar format
**Risk**: Sidecar format must be `{hex}  {filename}\n` (two spaces, exact).
**Mitigation**: Read/write logic hardcoded in `src/writer/file_rotator.py` (already ported); CLI just reads/compares.

### Manifest JSON schema compatibility
**Risk**: Manifest fields must remain in exact order for byte-identical output (test fixtures compare JSON string-form).
**Mitigation**: Use Jackson `@JsonPropertyOrder` or ordered Map on manifest record.

### Missing hour synthesis
**Risk**: Gap envelopes for missing hours must have exact `reason="missing_hour"` and computed gap window matching UTC hour boundaries.
**Mitigation**: Tier 5 rule F3 — always use UTC; consolidate.py line 154-178 shows exact timestamp computation.

### REST API retry and interpolation
**Risk**: Exponential backoff, retry-after header parsing, and interpolation math must match Python exactly.
**Mitigation**: Tier 5 rules D4, D7 — explicit retry and backoff; Tier 5 rule M4 — interpolation precision.

### Datetime round-trip via PostgreSQL
**Risk**: `mark_maintenance` stores ISO-8601 timestamps to PG; naive timestamp handling breaks round-trip.
**Mitigation**: Tier 5 rule F1 — always `Instant.now().toString()` for UTC; F2 for parsing (handle both +00:00 and Z).

### Click CLI compatibility
**Risk**: No Click equivalent in Java; must translate to picocli.
**Mitigation**: Tier 5 rule K1 (picocli groups/subcommands), K2 (System.out for CLI output, not logs), K3 (return int exit code, not System.exit).

## 11. Rule compliance

**Tier 1 #1** (raw_text capture): Touched in verify.py:44-49. Invariant is collector-side; CLI validates presence and integrity.

**Tier 1 #2** (raw_sha256 computation): Touched in verify.py:44-49. CLI re-validates sha256 over raw_text bytes.

**Tier 1 #3** (disabled streams): Not applicable to CLI.

**Tier 1 #4** (offset commit ordering): Not applicable to CLI (writer responsibility). Consolidation indirectly respects by verifying before cleanup.

**Tier 1 #5** (gap metrics + logs + records): Touched in consolidate.py:154-178 (synthesis), consolidation_scheduler.py:128-139 (metrics/logs).

**Tier 1 #6** (recovery prefers replay): Touched in gaps.py backfill command (fetches from REST), consolidate.py (emits explicit gaps, no reconstruction).

**Tier 1 #7** (JSON codec preservation): Touched in consolidate.py:249-251 (orjson.dumps preserves order).

All Tier 1 invariants are either touched or explicitly marked not applicable. The module is a verification and post-processing service; it does NOT violate invariants.

