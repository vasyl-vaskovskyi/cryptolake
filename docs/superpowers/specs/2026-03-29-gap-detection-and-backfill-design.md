# Gap Detection, Completeness Monitoring, and Historical Backfill

## Problem

CryptoLake silently loses data when the host shuts down (laptop close, reboot). The March 28 outage (hours 16–17 UTC) produced no gap envelope, no alert, and no backfill. Three failures compound:

1. **Silent gap detection bug**: `_check_recovery_gap()` in `consumer.py:284-286` returns `None` with no logging when PostgreSQL has no durable checkpoint for a stream (e.g. writer killed mid-flush before PG commit). The gap between the last archived record and the first post-restart message goes unrecorded.

2. **No completeness monitoring**: No metric, alert, or dashboard widget tracks whether all 24 hours exist for a stream/date. Missing hour files are invisible unless someone manually inspects the archive. The verify CLI lists hours found but never checks against the expected 0–23 range. Buffer overflow gaps in the collector (`_emit_overflow_gap`) don't increment `gaps_detected_total`.

3. **No backfill**: The design doc describes auto-backfill as "Phase 2" but it was never built. Binance provides historical REST endpoints for trades, funding rate, liquidations, and open interest — these can fill gaps retroactively.

**Policy**: NEVER LOSE DATA SILENTLY. Every gap must be visible via monitoring, and recoverable gaps must be filled from the historical API.

## Solution

Three independent layers, each deployable separately:

1. **Fix detection** — patch the silent return bug, add missing metrics, ensure every gap reaches Prometheus
2. **Add completeness monitoring** — missing-hour detection, Prometheus metric, alert, dashboard widget
3. **Build gap analyzer + backfill CLI** — scan archive for gaps, query Binance historical API, write backfill files

## Layer 1: Fix Silent Gap Detection

### 1.1 Patch `_check_recovery_gap()` (consumer.py:284-286)

**Current code (broken):**
```python
if checkpoint is None:
    return None  # silent — no log, no gap, no metric
```

**Fixed behavior:**
When no checkpoint exists but archived data exists for this stream on disk (hour files present), this is a gap — the writer lost its state. The fix:
1. Scan the archive directory for the stream's most recent hour file
2. Read the last envelope's `received_at` timestamp as `gap_start_ts`
3. Use the current message's `received_at` as `gap_end_ts`
4. Emit a gap envelope with `reason: "checkpoint_lost"` and `detail: "No durable checkpoint; recovered gap bounds from archive"`
5. Log a warning: `recovery_gap_no_checkpoint`

If no archived data exists either (truly first run), return `None` as before but log `info: first_run_no_checkpoint`.

### 1.2 Fix buffer overflow gap metric (producer.py)

`_emit_overflow_gap()` creates a gap envelope but does not call `gaps_detected_total.labels(...).inc()`. Add the missing increment so buffer overflow gaps appear in the `GapDetected` alert and dashboard.

### 1.3 New metric: `writer_gap_records_written_total`

Counter with labels `exchange, symbol, stream, reason`. Incremented in the writer every time a gap envelope is flushed to disk (in `_write_to_disk()` when processing gap-type envelopes). This gives Prometheus end-to-end visibility: a gap was not just detected but persisted to the archive.

## Layer 2: Completeness Monitoring

### 2.1 New metric: `writer_hours_sealed_today`

Gauge with labels `exchange, symbol, stream`. Value: count of distinct hours sealed so far for the current UTC date. Reset to 0 at midnight UTC rotation. This avoids unbounded cardinality from per-date/per-hour label combinations.

The writer increments this gauge each time a `.sha256` sidecar is written for the current date. On midnight rotation (when the writer starts writing to a new date directory), it snapshots the previous day's value into a second metric:

**`writer_hours_sealed_previous_day`**: Gauge with labels `exchange, symbol, stream`. Value: total hours sealed for the previous UTC date. Updated once at midnight rotation.

This enables simple PromQL:
- "How many hours were sealed yesterday for btcusdt/trades?" → `writer_hours_sealed_previous_day{exchange="binance", symbol="btcusdt", stream="trades"}`
- "Which streams are incomplete?" → `writer_hours_sealed_previous_day < 24`

### 2.2 New alert: `IncompleteDay`

```yaml
- alert: IncompleteDay
  expr: writer_hours_sealed_previous_day < 24
  for: 30m
  labels:
    severity: warning
  annotations:
    summary: "Incomplete day: {{ $labels.exchange }}/{{ $labels.symbol }}/{{ $labels.stream }}"
    description: "Only {{ $value }} of 24 hours archived for the previous UTC day."
```

The `for: 30m` delay ensures the alert only fires after the writer has had time to seal the last hours of the previous day during the midnight rotation window.

### 2.3 Dashboard widget

Add to sampler Tier 1:
```yaml
- title: Hours Sealed Today
  rate-ms: 60000
  sample: |
    curl -sg "http://localhost:9090/api/v1/query?query=min(writer_hours_sealed_today{exchange=~\"${EXCHANGE:-.*}\",symbol=~\"${SYMBOL:-.*}\"})" | jq '.data.result[0].value[1] // "0"' -r | xargs printf '%.0f'
```

Shows the minimum hours sealed across all streams for today (worst case). If one stream has only 5 hours sealed while others have 10, this shows 5 — the weakest link.

## Layer 3: Gap Analyzer and Backfill CLI

Both commands live under `src/cli/gaps.py` and operate per exchange/symbol pair with optional filters.

### 3.1 Gap Analyzer: `analyze`

```
python -m src.cli.gaps analyze [OPTIONS]

Options:
  --exchange TEXT     Filter by exchange (default: all)
  --symbol TEXT       Filter by symbol (default: all)
  --stream TEXT       Filter by stream (default: all)
  --date TEXT         Single date YYYY-MM-DD (default: all dates found)
  --date-from TEXT    Start of date range
  --date-to TEXT      End of date range
  --json             Output as JSON instead of table
  --base-dir PATH    Archive base directory (default: from config)
```

**What it does:**

For each exchange/symbol/stream/date combination found in the archive:

1. **Hour inventory**: Lists hours 0–23, marks each as `present`, `missing`, `backfilled`, or `late`
   - `present`: `hour-HH.jsonl.zst` exists
   - `missing`: no file at all
   - `backfilled`: `hour-HH.backfill-*.jsonl.zst` exists
   - `late`: `hour-HH.late-*.jsonl.zst` exists (spillover from restart)

2. **Gap record scan**: Reads all `.jsonl.zst` files and extracts `type: "gap"` envelopes. For each gap: reason, time window, whether a backfill file covers it.

3. **Coverage summary per stream/date**:
   - Hours expected: 24 (or fewer if the date is today/first day)
   - Hours covered: present + backfilled + late
   - Hours missing: not covered and no gap explains them
   - Gaps found: count, with breakdown by reason
   - Gaps recovered: count with backfill files
   - Gaps unrecoverable: order book streams (depth, depth_snapshot, bookticker)

4. **Overall summary**:
   - Total dates scanned
   - Total hours expected vs covered
   - Coverage percentage
   - Streams with outstanding gaps

**Example output (table mode):**

```
binance / btcusdt
═══════════════════════════════════════════════════════════════

  trades / 2026-03-28
  Hours: 0-15 ✓  16 MISSING  17 MISSING  18-23 ✓
  Gaps:  1 (checkpoint_lost 15:59→18:21 UTC)
  Backfill: none
  Coverage: 22/24 (91.7%)

  depth / 2026-03-28
  Hours: 0-15 ✓  16 MISSING  17 MISSING  18-23 ✓
  Gaps:  1 (checkpoint_lost 15:59→18:21 UTC)
  Backfill: unrecoverable (no historical API)
  Coverage: 22/24 (91.7%)

Summary: 2 dates, 7 streams
  Total coverage: 334/336 hours (99.4%)
  Recoverable gaps: 4 (trades, funding_rate, liquidations, open_interest)
  Unrecoverable gaps: 3 (depth, depth_snapshot, bookticker)
```

### 3.2 Backfill Runner: `backfill`

```
python -m src.cli.gaps backfill [OPTIONS]

Options:
  --exchange TEXT     Filter by exchange (default: all)
  --symbol TEXT       Filter by symbol (default: all)
  --stream TEXT       Filter by stream (default: all backfillable)
  --date TEXT         Single date YYYY-MM-DD (default: all dates with gaps)
  --date-from TEXT    Start of date range
  --date-to TEXT      End of date range
  --dry-run          Show what would be backfilled without writing
  --base-dir PATH    Archive base directory (default: from config)
```

**Flow:**

1. Runs the analyzer internally to find all unrecovered gaps
2. Filters to backfillable streams only: `trades`, `funding_rate`, `liquidations`, `open_interest`
3. For each gap:
   a. Determines the time window (`gap_start_ts` → `gap_end_ts`, or missing hour boundaries)
   b. Queries Binance historical REST API (paginated)
   c. Wraps each record in a standard envelope: same `v`, `type: "data"`, `exchange`, `symbol`, `stream` fields, plus `"source": "backfill"` marker
   d. Groups records by hour
   e. Writes to `hour-HH.backfill-N.jsonl.zst` (N increments if file exists)
   f. Creates `.sha256` sidecar
4. Skips gaps that already have backfill files (idempotent)
5. Prints summary: gaps found, gaps filled, gaps skipped

**Binance historical endpoints:**

| Stream | Endpoint | Key Parameters | Pagination | Weight |
|--------|----------|---------------|------------|--------|
| trades | `GET /fapi/v1/aggTrades` | `symbol`, `startTime`, `endTime` | `fromId` after last `a` in previous page, max 1000/req | 20 |
| funding_rate | `GET /fapi/v1/fundingRate` | `symbol`, `startTime`, `endTime`, `limit=1000` | Advance `startTime` past last result's `fundingTime` | 1 |
| liquidations | `GET /fapi/v1/allForceOrders` | `symbol`, `startTime`, `endTime`, `limit=1000` | Advance `startTime` past last result's `time` | 20+1 |
| open_interest | `GET /futures/data/openInterestHist` | `symbol`, `period=5m`, `startTime`, `endTime`, `limit=500` | Advance `startTime` past last result's `timestamp` | 1 |

**Rate limiting:**
- Binance allows 2400 weight/minute for REST
- Backfill runner tracks cumulative weight and sleeps when approaching 2000/min (safety margin)
- Respects `Retry-After` header on HTTP 429

**Non-backfillable streams:**
- `depth`, `depth_snapshot`, `bookticker`: logged as `unrecoverable` in analyzer output, skipped by backfill runner. Binance does not provide historical tick-level order book data via REST API.

### 3.3 Backfill file naming convention

Backfill files live alongside original hour files in the same date directory:

```
trades/2026-03-28/
  hour-15.jsonl.zst              # original
  hour-15.jsonl.zst.sha256
  hour-16.backfill-1.jsonl.zst   # recovered from /fapi/v1/aggTrades
  hour-16.backfill-1.jsonl.zst.sha256
  hour-17.backfill-1.jsonl.zst   # recovered
  hour-17.backfill-1.jsonl.zst.sha256
  hour-18.jsonl.zst              # original resumes
  hour-18.jsonl.zst.sha256
```

The pattern `hour-HH.backfill-N.jsonl.zst` mirrors the existing `hour-HH.late-N.jsonl.zst` convention. `N` increments from 1 if multiple backfill runs produce separate files for the same hour.

### 3.4 Envelope format for backfilled data

Backfill envelopes use the same schema as real-time data with one addition:

```json
{
  "v": 1,
  "type": "data",
  "source": "backfill",
  "exchange": "binance",
  "symbol": "btcusdt",
  "stream": "trades",
  "received_at": <backfill_run_timestamp_ns>,
  "exchange_ts": <original_event_time_ms>,
  "collector_session_id": "backfill-<ISO-timestamp>",
  "session_seq": <sequential within backfill run>,
  "raw_text": "<original JSON from Binance REST response>",
  "raw_sha256": "<sha256 of raw_text>",
  "_topic": "backfill",
  "_partition": 0,
  "_offset": <sequential within backfill run>
}
```

Key differences from real-time envelopes:
- `"source": "backfill"` — marks the record as retroactively filled (absent on real-time data)
- `collector_session_id` uses `backfill-` prefix
- `_topic` is `"backfill"` (not a real Kafka topic)
- `_offset` is sequential within the backfill run (not a real Kafka offset)
- `received_at` is the backfill run time, not original receipt time

### 3.5 Scheduling

The backfill runner can be scheduled via cron or systemd timer:

```bash
# Run every 6 hours, backfill all exchanges/symbols
0 */6 * * * cd /path/to/cryptolake && python -m src.cli.gaps backfill 2>&1 >> /var/log/cryptolake-backfill.log

# Or via systemd timer for the VPS deployment
```

Manual invocation for targeted recovery:
```bash
# Backfill a specific date
python -m src.cli.gaps backfill --exchange binance --symbol btcusdt --date 2026-03-28

# Dry run first
python -m src.cli.gaps backfill --exchange binance --symbol btcusdt --dry-run

# Backfill only trades
python -m src.cli.gaps backfill --exchange binance --symbol btcusdt --stream trades
```

### 3.6 Integration with existing verify CLI

The existing `verify` command (`src/cli/verify.py`) gains awareness of backfill files:
- `verify_checksum()` validates `.backfill-*.jsonl.zst` files and their sidecars
- `verify_envelopes()` accepts envelopes with `"source": "backfill"`
- `generate_manifest()` includes backfill files in the hour inventory
- `report_gaps()` cross-references gap envelopes against backfill file existence

## Changes Summary

| File | Change |
|------|--------|
| `src/writer/consumer.py` | Fix `_check_recovery_gap()` silent return; emit gap when checkpoint missing but archive exists |
| `src/collector/producer.py` | Add `gaps_detected_total.inc()` to `_emit_overflow_gap()` |
| `src/writer/metrics.py` | Add `writer_gap_records_written_total` counter, `writer_hours_sealed_today` and `writer_hours_sealed_previous_day` gauges |
| `src/writer/consumer.py` | Increment `gap_records_written_total` on gap flush; increment `hours_sealed_today` on file seal; snapshot to `hours_sealed_previous_day` on midnight rotation |
| `src/writer/file_rotator.py` | Emit `hours_sealed_today` metric when writing `.sha256` sidecar |
| `infra/prometheus/alert_rules.yml` | Add `IncompleteDay` alert |
| `infra/sampler/sampler.yml` | Add "Hour Coverage Yesterday %" widget |
| `src/cli/gaps.py` | New file: `analyze` and `backfill` CLI commands |
| `src/cli/verify.py` | Extend to recognize backfill files, check hour completeness |
| `src/exchanges/binance.py` | Add `build_historical_url()` methods for aggTrades, fundingRate, allForceOrders, openInterestHist |
| `src/common/envelope.py` | Support `source` field in envelope creation |

## Out of Scope

- **Historical order book data**: Binance does not provide tick-level depth diffs or bookticker via REST API. These gaps are documented by the analyzer as `unrecoverable`. Downstream consumers must handle depth gaps by waiting for the next snapshot to resync.
- **Automatic writer-integrated backfill**: The writer remains focused on real-time consumption. Backfill is a separate offline process.
- **Cross-exchange backfill**: Only Binance historical endpoints are implemented. Other exchanges would need their own adapter methods.
- **Backfill deduplication against live data**: If the writer was consuming while the gap occurred (e.g. partial coverage), the backfill may produce duplicate records for the overlap. Consumers should deduplicate by `exchange_ts` + trade ID / event key. This is acceptable for v1.
