# Funding Rate Composite Backfill

## Problem

The funding_rate backfill uses `GET /fapi/v1/fundingRate` which returns only 3 records per day (8-hourly settlement snapshots). The live `@markPrice@1s` stream captures 86,400 records/day with mark price, index price, premium, and funding rate every second. The backfill creates misleading files with 1 record per 8 hours — completely different data from the live stream.

## Solution

Replace the single-endpoint funding_rate fetch with a composite fetch from 4 Binance endpoints, reconstructing `markPriceUpdate` records at 1-minute granularity (1,440 records/day).

## Data Sources

| Endpoint | Returns | Maps to live field |
|----------|---------|-------------------|
| `GET /fapi/v1/markPriceKlines?interval=1m` | Mark price OHLC per minute | `p`, `ap` (close price) |
| `GET /fapi/v1/indexPriceKlines?interval=1m` | Index price OHLC per minute | `i` (close price) |
| `GET /fapi/v1/premiumIndexKlines?interval=1m` | Premium index OHLC per minute | `P` (close value) |
| `GET /fapi/v1/fundingRate` | Funding rate every 8h | `r` (applied to each minute in the window) |

## Constructed Record

For each minute, produce one record matching the live `markPriceUpdate` structure:

```json
{
  "e": "markPriceUpdate",
  "E": 1774828860000,
  "s": "BTCUSDT",
  "p": "66019.80000000",
  "ap": "66019.80000000",
  "P": "-0.00064353",
  "i": "66059.42586957",
  "r": "0.00000300",
  "T": 1774857600000
}
```

Field mapping:
- `e` — hardcode `"markPriceUpdate"`
- `E` — kline close time (kline[6] + 1, to get the close-of-minute timestamp)
- `s` — symbol in uppercase
- `p`, `ap` — `markPriceKlines` close (kline[4])
- `P` — `premiumIndexKlines` close (kline[4])
- `i` — `indexPriceKlines` close (kline[4])
- `r` — active funding rate at that minute (from `fundingRate` endpoint)
- `T` — next 8h boundary after `E`

## Funding Rate Assignment

Fetch all settlement records for the day. For each minute, apply the rate from the most recent settlement:
- 00:00–07:59 UTC → rate from 00:00 settlement
- 08:00–15:59 UTC → rate from 08:00 settlement
- 16:00–23:59 UTC → rate from 16:00 settlement

## Next Funding Time Computation

From `E` (event time in ms):
- If hour < 8 → T = same day 08:00 UTC
- If hour < 16 → T = same day 16:00 UTC
- Otherwise → T = next day 00:00 UTC

## Changes

| File | Change |
|------|--------|
| `src/cli/gaps.py` | Add `_fetch_funding_rate_composite(adapter, symbol, start_ms, end_ms)` function |
| `src/cli/gaps.py` | Update `_fetch_historical_all()` to route `funding_rate` stream to composite fetch |
| `src/exchanges/binance.py` | Add `build_mark_price_klines_url()`, `build_index_price_klines_url()`, `build_premium_index_klines_url()` |

## Rate Limiting

Per hour of missing data: 3 kline requests (each covers up to 1000 minutes = ~16 hours) + 1 fundingRate request = 4 requests total. Well within Binance's 2400 weight/minute limit.

## Limitations

- 1 record per minute vs 1 per second in the live stream (1,440/day vs 86,400/day)
- `P` field is premium index ratio, not the exact estimated settle price from the live stream
- `ap` is set equal to `p` (accumulated price is not available historically)
- `r` changes only every 8h, so 480 consecutive minutes share the same value

## Out of Scope

- Reconstructing 1-second granularity — no historical API supports it
- Backfilling `T` field with exact precision — computed from 8h boundaries
