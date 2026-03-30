# In-File Gap Backfill

## Problem

The backfill service only fills missing hour files. Gaps within existing files — such as 19,025 missing trades detected by the integrity checker inside `hour-20.jsonl.zst` — are not backfilled. The `--dry-run` reports "nothing to backfill" even though real data is missing.

## Solution

Extend the `backfill` command with a `--deep` flag that:
1. Runs the integrity checker to find exact ID gaps (trades)
2. Scans gap envelopes in archive files to find time-window gaps (funding_rate, liquidations, open_interest)
3. Fetches missing data from Binance historical API
4. Writes to separate backfill files (`hour-HH.backfill-N.jsonl.zst`)

## Backfill Strategies by Stream

| Stream | Gap detection method | Backfill API | Strategy |
|--------|---------------------|-------------|----------|
| trades | Integrity checker: aggregate trade ID (`a`) consecutive check | `GET /fapi/v1/aggTrades?fromId={from_id}` | ID-based (exact) |
| funding_rate | Gap envelope timestamps from archive | `GET /fapi/v1/fundingRate?startTime=&endTime=` | Time-based |
| liquidations | Gap envelope timestamps from archive | `GET /fapi/v1/allForceOrders?startTime=&endTime=` | Time-based |
| open_interest | Gap envelope timestamps from archive | `GET /futures/data/openInterestHist?startTime=&endTime=` | Time-based |
| depth | Integrity checker: pu-chain break | No historical API | Not backfillable |
| bookticker | Integrity checker: backwards u jump | No historical API | Not backfillable |
| depth_snapshot | N/A | N/A | Not backfillable |

## Changes

### 1. Extend integrity checker to output structured gap ranges

Add a function `find_backfillable_gaps()` to `src/cli/integrity.py` that returns:

```python
[
    {
        "type": "id_gap",          # ID-based gap (trades)
        "exchange": "binance",
        "symbol": "btcusdt",
        "stream": "trades",
        "date": "2026-03-30",
        "hour": 20,
        "from_id": 3229557097,     # first missing ID (expected)
        "to_id": 3229576120,       # last missing ID (actual - 1)
        "missing": 19025,
    },
]
```

For trades only — depth and bookticker breaks are returned with `"backfillable": False`.

### 2. Scan gap envelopes for time-window gaps

Add a function `find_time_based_gaps()` to `src/cli/gaps.py` that scans archive files for `ws_disconnect` gap envelopes in funding_rate, liquidations, and open_interest streams. Returns:

```python
[
    {
        "type": "time_gap",        # Time-based gap
        "exchange": "binance",
        "symbol": "btcusdt",
        "stream": "funding_rate",
        "date": "2026-03-30",
        "hour": 20,
        "start_ms": 1774900000000, # gap_start_ts converted to ms
        "end_ms": 1774903600000,   # gap_end_ts converted to ms
    },
]
```

### 3. Extend backfill command with `--deep` flag

```
python -m src.cli.gaps backfill --exchange binance --symbol btcusdt --deep
python -m src.cli.gaps backfill --exchange binance --symbol btcusdt --deep --dry-run
```

Flow when `--deep` is set:
1. Run existing missing-hour backfill (unchanged)
2. Run `find_backfillable_gaps()` from integrity checker → trades ID gaps
3. Run `find_time_based_gaps()` from gap envelope scan → funding_rate, liquidations, open_interest time gaps
4. For each trades ID gap:
   - Paginate `GET /fapi/v1/aggTrades?fromId={from_id}&limit=1000` until `a >= to_id`
   - Group records by hour, write to `hour-HH.backfill-N.jsonl.zst`
5. For each time-based gap:
   - Fetch with `startTime`/`endTime` from the appropriate endpoint
   - Group records by hour, write to `hour-HH.backfill-N.jsonl.zst`
6. Skip gaps that already have backfill files covering the range

### 4. Update backfill scheduler to use `--deep`

The scheduled backfill service (`src/cli/backfill_scheduler.py`) runs `--deep` by default since it has time. Manual runs can use either mode.

### 5. Idempotency

For ID-based gaps: before fetching, check if a backfill file for that hour already contains records covering the missing ID range. If so, skip.

For time-based gaps: check if a backfill file for that hour already exists for the gap's time window. If the file has records within the time range, skip.

## Files Changed

| File | Change |
|------|--------|
| `src/cli/integrity.py` | Add `find_backfillable_gaps()` function |
| `src/cli/gaps.py` | Add `find_time_based_gaps()`, extend `backfill` with `--deep` flag |
| `src/cli/backfill_scheduler.py` | Pass `deep=True` to backfill cycle |

## Out of Scope

- Backfilling depth diffs or bookticker — no historical API
- Modifying existing sealed hour files — always write separate backfill files
- Real-time backfill during collection — backfill is always an offline batch process
