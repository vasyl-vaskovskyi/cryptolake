# Funding Rate Composite Backfill — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-endpoint funding_rate backfill (3 records/day) with a composite fetch from 4 Binance endpoints that reconstructs markPriceUpdate records at 1-minute granularity (1,440 records/day).

**Architecture:** Add 3 new URL builders to BinanceAdapter (markPriceKlines, indexPriceKlines, premiumIndexKlines). Add `_fetch_funding_rate_composite()` to gaps.py that fetches all 4 endpoints, joins by timestamp, and constructs markPriceUpdate records. Route `_fetch_historical_all()` to use composite fetch when `stream == "funding_rate"`.

**Tech Stack:** Python 3, aiohttp, orjson, pytest

---

## File Structure

| File | Responsibility |
|------|---------------|
| `src/exchanges/binance.py` | Add 3 new kline URL builders |
| `src/cli/gaps.py` | Add `_fetch_funding_rate_composite()`, route funding_rate to it |
| `tests/unit/test_binance_historical_urls.py` | Test new URL builders |
| `tests/unit/test_funding_rate_composite.py` | Test composite record construction |

---

### Task 1: Add kline URL builders to BinanceAdapter

**Files:**
- Modify: `src/exchanges/binance.py`
- Modify: `tests/unit/test_binance_historical_urls.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_binance_historical_urls.py`:

```python
def test_build_mark_price_klines_url():
    adapter = _make_adapter()
    url = adapter.build_mark_price_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/markPriceKlines?symbol=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"

def test_build_index_price_klines_url():
    adapter = _make_adapter()
    url = adapter.build_index_price_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/indexPriceKlines?symbol=BTCUSDT&pair=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"

def test_build_premium_index_klines_url():
    adapter = _make_adapter()
    url = adapter.build_premium_index_klines_url("btcusdt", start_time=1000, end_time=2000)
    assert url == "https://fapi.binance.com/fapi/v1/premiumIndexKlines?symbol=BTCUSDT&interval=1m&startTime=1000&endTime=2000&limit=1000"
```

Note: `_make_adapter()` is already defined in this test file.

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_binance_historical_urls.py -v -k "klines"`
Expected: FAIL — methods don't exist

- [ ] **Step 3: Implement the URL builders**

Add to `src/exchanges/binance.py` after `build_historical_open_interest_url()`:

```python
    def build_mark_price_klines_url(
        self, symbol: str, *, start_time: int, end_time: int, interval: str = "1m", limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/markPriceKlines"
            f"?symbol={symbol.upper()}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_index_price_klines_url(
        self, symbol: str, *, start_time: int, end_time: int, interval: str = "1m", limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/indexPriceKlines"
            f"?symbol={symbol.upper()}&pair={symbol.upper()}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )

    def build_premium_index_klines_url(
        self, symbol: str, *, start_time: int, end_time: int, interval: str = "1m", limit: int = 1000,
    ) -> str:
        return (
            f"{self.rest_base}/fapi/v1/premiumIndexKlines"
            f"?symbol={symbol.upper()}&interval={interval}&startTime={start_time}&endTime={end_time}&limit={limit}"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_binance_historical_urls.py -v`
Expected: All pass

- [ ] **Step 5: Commit**

```bash
git add src/exchanges/binance.py tests/unit/test_binance_historical_urls.py
git commit -m "feat: add markPriceKlines, indexPriceKlines, premiumIndexKlines URL builders

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Implement `_fetch_funding_rate_composite()` and wire it up

**Files:**
- Modify: `src/cli/gaps.py`
- Create: `tests/unit/test_funding_rate_composite.py`

- [ ] **Step 1: Write the test for record construction**

Create `tests/unit/test_funding_rate_composite.py`:

```python
from src.cli.gaps import _build_mark_price_update, _compute_next_funding_time

def test_build_mark_price_update():
    mark_kline = [1774828800000, "65973.40", "66053.80", "65973.40", "66019.80", "0", 1774828859999, "0", 60, "0", "0", "0"]
    index_kline = [1774828800000, "66012.22", "66068.94", "66012.22", "66059.43", "0", 1774828859999, "0", 60, "0", "0", "0"]
    premium_kline = [1774828800000, "-0.00014", "-0.00014", "-0.00074", "-0.00064", "0", 1774828859999, "0", 12, "0", "0", "0"]
    funding_rate = "0.00000300"

    result = _build_mark_price_update("BTCUSDT", mark_kline, index_kline, premium_kline, funding_rate)

    assert result["e"] == "markPriceUpdate"
    assert result["s"] == "BTCUSDT"
    assert result["E"] == 1774828860000  # close time + 1
    assert result["p"] == "66019.80"     # mark close
    assert result["ap"] == "66019.80"    # same as p
    assert result["i"] == "66059.43"     # index close
    assert result["P"] == "-0.00064"     # premium close
    assert result["r"] == "0.00000300"   # funding rate
    assert result["T"] == 1774857600000  # next 8h = 08:00 UTC

def test_compute_next_funding_time_before_8():
    # 2026-03-30 03:00:00 UTC → next = 08:00 same day
    assert _compute_next_funding_time(1774839600000) == 1774857600000

def test_compute_next_funding_time_before_16():
    # 2026-03-30 10:00:00 UTC → next = 16:00 same day
    assert _compute_next_funding_time(1774864800000) == 1774886400000

def test_compute_next_funding_time_after_16():
    # 2026-03-30 20:00:00 UTC → next = 00:00 next day
    assert _compute_next_funding_time(1774900800000) == 1774915200000
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/unit/test_funding_rate_composite.py -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement helper functions in `src/cli/gaps.py`**

Add after `_fetch_historical_page()`:

```python
def _compute_next_funding_time(event_time_ms: int) -> int:
    """Compute next 8h funding boundary (00:00, 08:00, 16:00 UTC) after event_time_ms."""
    from datetime import datetime, timezone, timedelta
    dt = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    if hour < 8:
        next_dt = dt.replace(hour=8, minute=0, second=0, microsecond=0)
    elif hour < 16:
        next_dt = dt.replace(hour=16, minute=0, second=0, microsecond=0)
    else:
        next_day = (dt + timedelta(days=1))
        next_dt = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(next_dt.timestamp() * 1000)


def _build_mark_price_update(
    symbol: str,
    mark_kline: list,
    index_kline: list,
    premium_kline: list,
    funding_rate: str,
) -> dict:
    """Construct a markPriceUpdate record from kline data."""
    event_time = mark_kline[6] + 1  # close time + 1ms
    return {
        "e": "markPriceUpdate",
        "E": event_time,
        "s": symbol,
        "p": mark_kline[4],      # mark price close
        "ap": mark_kline[4],     # accumulated price = mark close
        "P": premium_kline[4],   # premium index close
        "i": index_kline[4],     # index price close
        "r": funding_rate,
        "T": _compute_next_funding_time(event_time),
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_funding_rate_composite.py -v`
Expected: All pass

- [ ] **Step 5: Implement `_fetch_funding_rate_composite()`**

Add to `src/cli/gaps.py` after the helper functions:

```python
async def _fetch_funding_rate_composite(
    adapter: BinanceAdapter,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """Fetch funding rate data by compositing 4 Binance endpoints.

    Reconstructs markPriceUpdate records at 1-minute granularity from:
    - markPriceKlines (mark price)
    - indexPriceKlines (index price)
    - premiumIndexKlines (premium index)
    - fundingRate (8-hourly settlement rate)
    """
    async with aiohttp.ClientSession() as session:
        # Fetch all 3 kline types with pagination
        mark_klines: list[list] = []
        index_klines: list[list] = []
        premium_klines: list[list] = []

        current = start_ms
        while current <= end_ms:
            mark_url = adapter.build_mark_price_klines_url(symbol, start_time=current, end_time=end_ms)
            index_url = adapter.build_index_price_klines_url(symbol, start_time=current, end_time=end_ms)
            premium_url = adapter.build_premium_index_klines_url(symbol, start_time=current, end_time=end_ms)

            mark_page, index_page, premium_page = await asyncio.gather(
                _fetch_historical_page(session, mark_url),
                _fetch_historical_page(session, index_url),
                _fetch_historical_page(session, premium_url),
            )

            if not mark_page:
                break

            mark_klines.extend(mark_page)
            index_klines.extend(index_page or [])
            premium_klines.extend(premium_page or [])

            last_open = mark_page[-1][0]
            if last_open >= end_ms or len(mark_page) < 1000:
                break
            current = last_open + 60000  # next minute

        if not mark_klines:
            return []

        # Fetch funding rate settlements for the period
        funding_url = adapter.build_historical_funding_url(symbol, start_time=start_ms, end_time=end_ms)
        funding_records = await _fetch_historical_page(session, funding_url) or []

        # Build funding rate lookup: sorted by time descending for bisect
        funding_rates: list[tuple[int, str]] = []
        for fr in funding_records:
            funding_rates.append((fr["fundingTime"], fr["fundingRate"]))
        funding_rates.sort()

        def _get_rate(event_ms: int) -> str:
            """Get the active funding rate at event_ms."""
            rate = "0"
            for ft, fr in funding_rates:
                if ft <= event_ms:
                    rate = fr
                else:
                    break
            return rate

        # Index klines by open_time for joining
        index_by_time = {k[0]: k for k in index_klines}
        premium_by_time = {k[0]: k for k in premium_klines}

        # Build composite records
        records: list[dict] = []
        for mk in mark_klines:
            open_time = mk[0]
            ik = index_by_time.get(open_time, mk)      # fallback to mark if missing
            pk = premium_by_time.get(open_time, [open_time, "0", "0", "0", "0"])
            rate = _get_rate(mk[6] + 1)  # rate at close time
            records.append(_build_mark_price_update(symbol.upper(), mk, ik, pk, rate))

    return records
```

- [ ] **Step 6: Route funding_rate to composite fetch in `_fetch_historical_all()`**

In `src/cli/gaps.py`, find the `elif stream == "funding_rate":` block inside `_fetch_historical_all()` (around line 103-106). Replace the entire function to route funding_rate to the composite:

Change the `elif stream == "funding_rate":` section to:

```python
            elif stream == "funding_rate":
                # Use composite fetch for funding_rate (reconstructs markPriceUpdate from 4 endpoints)
                return await _fetch_funding_rate_composite(adapter, symbol, start_ms, end_ms)
```

Note: this early-returns from the function, bypassing the pagination loop which doesn't apply to the composite fetch.

BUT: this `return` is inside `async with aiohttp.ClientSession()`. The composite function creates its own session. So instead, restructure: move the early return BEFORE the `async with` block:

```python
async def _fetch_historical_all(
    adapter: BinanceAdapter,
    symbol: str,
    stream: str,
    start_ms: int,
    end_ms: int,
) -> list[dict]:
    """Fetch all records for the given stream/symbol in [start_ms, end_ms] with pagination."""
    # Funding rate uses composite fetch from 4 endpoints
    if stream == "funding_rate":
        return await _fetch_funding_rate_composite(adapter, symbol, start_ms, end_ms)

    ts_key = STREAM_TS_KEYS[stream]
    all_records: list[dict] = []
    ...  # rest of existing code unchanged
```

Remove the old `elif stream == "funding_rate":` branch from inside the `while` loop.

- [ ] **Step 7: Run full test suite**

Run: `pytest tests/unit/ -v --tb=short`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add src/cli/gaps.py tests/unit/test_funding_rate_composite.py
git commit -m "feat: composite funding_rate backfill from 4 Binance endpoints

Replaces single fundingRate endpoint (3 records/day) with composite
fetch from markPriceKlines + indexPriceKlines + premiumIndexKlines +
fundingRate, reconstructing markPriceUpdate records at 1-minute
granularity (1,440 records/day).

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Integration test against real Binance API

- [ ] **Step 1: Test composite fetch for one hour**

```bash
.venv/bin/python -c "
import asyncio, json
from src.exchanges.binance import BinanceAdapter
from src.cli.gaps import _fetch_funding_rate_composite

async def test():
    adapter = BinanceAdapter(ws_base='wss://fstream.binance.com', rest_base='https://fapi.binance.com', symbols=['btcusdt'])
    # Fetch 1 hour: 2026-03-30 00:00-01:00 UTC
    records = await _fetch_funding_rate_composite(adapter, 'btcusdt', 1774828800000, 1774832400000)
    print(f'Records: {len(records)}')
    if records:
        print(f'First: {json.dumps(records[0], indent=2)}')
        print(f'Last:  {json.dumps(records[-1], indent=2)}')
        # Verify structure matches live stream
        r = records[0]
        for field in ['e', 'E', 's', 'p', 'ap', 'P', 'i', 'r', 'T']:
            assert field in r, f'Missing field: {field}'
        print('All fields present!')

asyncio.run(test())
"
```

Expected: ~60 records (1 per minute for 1 hour), each with all markPriceUpdate fields.

- [ ] **Step 2: Test dry-run backfill for funding_rate**

```bash
.venv/bin/python -m src.cli.gaps backfill --base-dir /Users/vasyl.vaskovskyi/data/archive --exchange binance --symbol btcusdt --stream funding_rate --date 2026-03-30 --dry-run
```

Verify it shows the missing hours.

- [ ] **Step 3: Run full test suite**

```bash
.venv/bin/python -m pytest tests/unit/ -v --tb=short
```

All must pass.
