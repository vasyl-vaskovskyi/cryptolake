# CryptoLake — Design Specification

**Version**: 1.0
**Date**: 2026-03-11
**Status**: Draft

---

## 1. Overview

### 1.1 Purpose

CryptoLake is a raw-first crypto market data collection system that captures high-granularity trading data from cryptocurrency exchanges. The original exchange payload is exactly recoverable from the archive — raw text is captured before JSON parsing to avoid any lossy transformation. These immutable compressed archives provide a reliable foundation for future backtesting engines and programmable trading bots.

### 1.2 Scope

- Collect USD-M Futures data from Binance (primary exchange, others to follow)
- Capture trades, order book depth (diffs + periodic snapshots), bookTicker, funding rate, liquidation orders, and open interest
- Store raw data in compressed JSON Lines archives with integrity checksums
- Provide full observability via Prometheus, Grafana, and Alertmanager
- Deploy as Docker containers designed for Kubernetes orchestration

### 1.3 Out of Scope

- Backtesting engine (future project, consumes archived data)
- Trading bot execution (future project, attaches as Redpanda consumer)
- Derived data layers (Parquet, database — built later from raw archives)
- Spot market data collection
- COIN-M Futures data collection
- UI or web dashboard beyond Grafana

### 1.4 Design Principles

- **Raw-first**: The original exchange payload is exactly recoverable from the archive. Raw text is captured before JSON parsing to prevent lossy transformations (key reordering, number precision loss, whitespace changes, duplicate key collapse). All derived formats are reproducible from raw data.
- **Never silently lose data**: Every gap, missed message, and failed snapshot must be logged and alerted. Data that cannot be recovered later (liquidation events, funding rates) is treated as irreplaceable.
- **Decoupled ingestion and I/O**: Collection and writing are separate processes connected via a durable message broker. I/O stalls never cause WebSocket backpressure.
- **Orchestration-ready**: Health check endpoints, graceful shutdown, externalized config, stateless collectors with external storage.

### 1.4.1 System Invariants

These rules are architectural invariants. If an implementation detail conflicts with any item below, the implementation is wrong.

- **Raw payload fidelity is preserved end-to-end**: `raw_text` is captured from the original WebSocket frame or HTTP body before parsing and remains the authoritative payload in the archive.
- **Disabled streams are fully absent**: a disabled stream is not subscribed, not polled, not archived, and does not emit gap records.
- **Durability ordering is strict**: the writer may commit Kafka offsets only after the corresponding records are durably flushed to disk.
- **Known data loss is explicit**: every recoverable or irrecoverable gap that is observed by the system is surfaced as a metric, alert, and archived gap record when applicable.
- **Recovery favors replay over reconstruction**: the system prefers replaying from Redpanda or re-syncing from exchange-native cursors instead of inventing inferred state.
- **Zero-GIL JSON handling**: All JSON parsing and serialization must use a fast C/Rust-backed library (`orjson`) to prevent the Python GIL from blocking the async event loop at high throughput.

### 1.5 Acceptance Criteria

Each criterion is independently testable. The system is considered complete when all pass.

**AC-1: End-to-end data collection**
Given the system is started with `docker-compose up` and a config containing 3 symbols (e.g., btcusdt, ethusdt, solusdt),
when the system runs for 1 hour,
then compressed `.jsonl.zst` files exist for all configured symbols across all enabled stream types (trades, depth, depth_snapshot, bookticker, funding_rate, liquidations, open_interest),
and each file decompresses to valid JSON Lines where every line matches the envelope schema (v, type, exchange, symbol, stream, received_at, exchange_ts, collector_session_id, session_seq, raw_text, raw_sha256, _topic, _partition, _offset for data records; gap records carry gap_start_ts, gap_end_ts, reason, detail instead of raw_text/raw_sha256),
and every `raw_text` field contains the original exchange payload verifiable via its `raw_sha256` digest.

**AC-2: Data integrity verification**
Given archived files exist for a completed hour,
when `cryptolake verify --date <date>` is run,
then all SHA-256 checksums match their sidecar files,
and no duplicate `(_topic, _partition, _offset)` tuples exist within or across hourly files,
and every depth diff sequence is anchored by a valid snapshot (depth bootstrap/replay check passes),
and all persisted gap events are reported with their reason and time window,
and no verification errors are reported.

**AC-3: Order book reconstruction**
Given archived depth diffs and depth snapshots for a symbol,
when depth diffs are replayed starting from the nearest snapshot,
then the reconstructed order book state matches the next periodic snapshot (within the snapshot's `lastUpdateId` window),
confirming zero data loss between snapshots.

**AC-4: Automatic reconnection and recovery**
Given the collector is running and actively collecting data,
when the WebSocket connection is forcibly dropped (simulating network failure),
then the collector reconnects within 60 seconds,
and the depth resync flow executes (stream reopened, diffs buffered during snapshot fetch, stale diffs dropped, sync point found via `lastUpdateId`, `pu` chain validated),
and no live depth diffs are lost during the snapshot round-trip,
and the gap is logged with timestamp window and symbol/stream detail,
and the `collector_gaps_detected_total` metric increments,
and data collection resumes without manual intervention.

**AC-5: Writer crash resilience**
Given the collector is producing messages to Redpanda and the writer is consuming,
when the writer process is killed (`docker kill writer`),
and the writer is restarted after 5 minutes,
then the writer resumes from its last committed Kafka offset,
and all messages produced during the downtime (within Redpanda's 48h retention) are written to disk,
and no messages are permanently lost (verified by comparing Redpanda topic high-water marks to the maximum `_offset` per topic/partition in the archive).

**AC-6: Observability operational**
Given the full docker-compose stack is running (including Prometheus, Grafana, Alertmanager),
then Prometheus is scraping metrics from collector, writer, and Redpanda,
and the Grafana dashboard loads with populated panels (message throughput, latency, consumer lag, disk usage),
and when a simulated gap event occurs, Alertmanager fires a `GapDetected` alert to the configured webhook within 2 minutes.

**AC-7: Storage organization**
Given the system has been running for at least 2 hours,
then files are organized as `/data/{exchange}/{symbol}/{stream}/{date}/hour-{HH}.jsonl.zst`,
and all directory and file names are lowercase,
and all timestamps within files use UTC,
and no empty files exist (hours with no data produce no file).

**AC-8: Configuration-driven behavior**
Given a config file with `depth.snapshot_interval: "1m"` for btcusdt and default `"5m"` for other symbols,
when the system runs for 10 minutes,
then btcusdt has ~10 depth snapshots archived and other symbols have ~2 each (within tolerance),
and disabling a stream (e.g., `bookticker: false`) results in no bookticker data collected or archived for any symbol.

**AC-9: Rate limit compliance**
Given the system is running with 5 symbols and all streams enabled,
then REST API calls (snapshots, open interest) stay within Binance's rate limits (2400 weight/minute),
and periodic REST requests are spread across their intervals (not bursted),
and HTTP 429 responses (if any) are handled with backoff and no crash.

**AC-10: Graceful shutdown**
Given the system is actively collecting and writing data,
when `docker-compose down` is issued,
then all services exit within 30 seconds,
and the writer flushes all buffered data to disk before exiting,
and Kafka offsets are committed,
and any unsealed file is detected and sealed on next startup.

**AC-11: Writer deduplication guarantee**
Given the writer is actively consuming and writing data,
when the writer process is killed (`docker kill writer`) immediately after a disk flush but before Kafka offset commit,
and the writer is restarted,
then the resulting archive contains no duplicate `(_topic, _partition, _offset)` tuples,
and `cryptolake verify --date <date>` reports zero duplicate broker records,
and downstream consumers can trust the archive without performing their own deduplication.

---

## 2. Architecture

### 2.1 High-Level Architecture

The system follows a **process-per-exchange** architecture with a **Redpanda message broker** as a durable buffer between collectors and writers.

```
                                          ┌─→ Raw File Writer (archival)
Binance WS/REST → Collector → Redpanda ──┤
                                          └─→ Future Consumer (trading bot, analytics)
                                                  ↑ zero pipeline changes to add

Prometheus ← scrapes /metrics from all services → Grafana → Alertmanager → webhooks
```

### 2.2 Services

The system consists of 6 Docker services:

| Service | Image | Purpose | Custom Code |
|---------|-------|---------|-------------|
| `collector` | `Dockerfile.collector` | Connects to exchange, produces raw messages to Redpanda | Yes |
| `writer` | `Dockerfile.writer` | Consumes from Redpanda, compresses, writes to disk | Yes |
| `redpanda` | `redpandadata/redpanda` | Durable message broker (Kafka-compatible) | No — config only |
| `prometheus` | `prom/prometheus` | Metrics collection and storage | No — config only |
| `grafana` | `grafana/grafana` | Dashboards and visualization | No — config + dashboard JSON |
| `alertmanager` | `prom/alertmanager` | Alert routing and notifications | No — config only |

### 2.3 Why Redpanda (Not Direct IPC)

| Concern | Without Broker | With Redpanda |
|---------|---------------|---------------|
| Writer crash | Messages in async queue lost | Messages buffered, writer resumes from offset |
| Collector-writer coupling | Custom IPC, backpressure, error handling | Independent services, produce/consume |
| Adding future consumers | Re-architect the pipeline | Add consumer group, zero changes to collector |
| Development complexity | Custom queue management | Each service is simple: produce or consume |
| Gap detection | Custom sequence tracking | Consumer lag metric built-in |
| Replay/reprocessing | Read archived files | Consume from any offset within retention |

Redpanda specifics:
- Single C++ binary, no JVM, no ZooKeeper
- Kafka API compatible — uses standard `aiokafka` Python library
- Sub-millisecond latency
- Single Docker container, ~256MB RAM at this scale
- Built-in admin console and schema registry

### 2.4 Data Flow

1. **Collector** connects to Binance USD-M Futures WebSocket streams utilizing **Combined Streams (multiplexing)** to reduce total network connections. Multiple stream types (trades, depth, liquidations) flow over the same physical WebSocket.
2. The Collector **demultiplexes** the incoming frames based on the exchange routing key (e.g., the `stream` field).
3. Each incoming message is wrapped in a **message envelope** with `received_at` nanosecond timestamp, session sequence, and collector session ID
4. Wrapped messages are produced to **Redpanda topics** (one topic per stream type)
5. **Writer** consumes from all topics, buffers messages, compresses with zstd, and writes hourly-rotated `.jsonl.zst` files
6. SHA-256 checksums are written as sidecar files for integrity verification
7. All services expose `/metrics` for Prometheus scraping

### 2.5 Future Trading Bot Integration

A trading bot attaches as a separate Redpanda consumer group. No changes to collector or writer.

```
Binance → Collector → Redpanda → Bot Consumer (real-time, sub-ms latency)
```

Latency breakdown:
- Binance → Collector: ~5-50ms (network, unavoidable)
- Collector → Redpanda: <1ms
- Redpanda → Bot consumer: <1ms
- Bottleneck is always the exchange network, not the internal pipeline

---

## 3. Exchange Integration

### 3.1 Binance USD-M Futures

| | Endpoint |
|---|---|
| WebSocket base | `wss://fstream.binance.com` |
| REST base | `https://fapi.binance.com` |
| Max streams per WS connection | 1024 (Binance limit) |
| Forced disconnect | Every 24 hours |

### 3.2 Data Streams

All streams are enabled by default. Each is individually toggleable via config.

#### 3.2.1 Trades (`trades` stream)

- **Source**: WebSocket `<symbol>@aggTrade` (via `/ws/market`)
- **Content**: Aggregated trades — price, quantity, buyer/seller maker flag, timestamp. Binance aggregates individual fills at the same price/time/side into a single aggTrade event.
- **Volume**: High (BTC/USDT: ~1-10 messages/sec average, spikes to 100+/sec)
- **Native cursor fields**:
  - `a` — aggregate trade ID (monotonically increasing per symbol). Gaps in `a` indicate missed aggTrade events.
  - `f` / `l` — first and last individual trade IDs within this aggregate. Enables cross-referencing with Binance's historical trade endpoint.
- **`exchange_ts` source**: `E` (event time, milliseconds)
- **Redpanda topic**: `binance.trades`
- **Key**: Symbol (e.g., `btcusdt`)

#### 3.2.2 Depth Diffs (`depth` stream)

- **Source**: WebSocket `<symbol>@depth@100ms` (via `/ws/public`)
- **Content**: Order book diff updates — changed price levels with new quantities, every 100ms
- **Volume**: Moderate-high (~10 messages/sec per symbol)
- **Sequence validation**: Each diff has `U` (first updateId), `u` (last updateId), and `pu` (previous last updateId). After synchronizing with a snapshot, the first usable diff is the one where `U <= lastUpdateId+1` and `u >= lastUpdateId+1`. All subsequent diffs must satisfy `pu == previous u` (the previous-update chain). Diffs where `u < lastUpdateId` are stale and discarded.
- **`exchange_ts` source**: `E` (event time, milliseconds)
- **Redpanda topic**: `binance.depth`
- **Key**: Symbol

#### 3.2.3 Depth Snapshots (`depth_snapshot`)

- **Source**: REST `GET /fapi/v1/depth?symbol=<SYMBOL>&limit=1000`
- **Content**: Full order book state — all bid and ask price levels with quantities
- **Schedule**: Configurable interval per symbol (default: every 5 minutes). Triggered by the snapshot scheduler in the collector.
- **Purpose**: Checkpoint for depth diff reconstruction. On reconnect or gap detection, the depth stream is reopened immediately and incoming diffs are buffered while a REST snapshot is fetched concurrently (see Section 7.2 for the full resync flow). **All snapshots are archived** — both periodic and resync-triggered snapshots are produced to `binance.depth_snapshot` and written to disk. There is no distinction in the archive; resync snapshots simply appear as additional snapshots between the periodic ones.
- **`exchange_ts` source**: None — REST depth snapshot response has no event timestamp. Set `exchange_ts` to `received_at / 1_000_000` (convert nanos to millis) as a best-effort approximation.
- **Redpanda topic**: `binance.depth_snapshot`
- **Key**: Symbol

#### 3.2.4 BookTicker (`bookTicker` stream)

- **Source**: WebSocket `<symbol>@bookTicker` (via `/ws/public`)
- **Content**: Real-time best bid/ask price and quantity updates
- **Volume**: High (updates on every top-of-book change)
- **`exchange_ts` source**: `E` (event time, milliseconds). Note: older Binance bookTicker payloads may omit `E`; if absent, fall back to `received_at / 1_000_000`.
- **Redpanda topic**: `binance.bookticker`
- **Key**: Symbol

#### 3.2.5 Funding Rate (`funding_rate`)

- **Source**: WebSocket `<symbol>@markPrice@1s` (via `/ws/market`)
- **Content**: Mark price, index price, estimated settle price, funding rate, next funding time
- **Volume**: 1 message/second per symbol
- **Significance**: Funding rates change every 8 hours. Funding rate arbitrage, long/short sentiment indicator. Cannot be derived from trade or order book data.
- **`exchange_ts` source**: `E` (event time, milliseconds)
- **Redpanda topic**: `binance.funding_rate`
- **Key**: Symbol

#### 3.2.6 Liquidation Orders (`liquidations`)

- **Source**: WebSocket `<symbol>@forceOrder` (via `/ws/market`)
- **Content**: Forced liquidation events — symbol, side, order type, time in force, quantity, price, average price, status, trade time
- **Volume**: Sporadic, spikes during volatile moves
- **Significance**: Liquidation cascades drive price moves. Clustering of liquidations indicates support/resistance. Cannot be reconstructed after the fact — irreplaceable data.
- **`exchange_ts` source**: `E` (event time, milliseconds) from the outer wrapper; the inner `o.T` is the trade time.
- **Redpanda topic**: `binance.liquidations`
- **Key**: Symbol

#### 3.2.7 Open Interest (`open_interest`)

- **Source**: REST `GET /fapi/v1/openInterest?symbol=<SYMBOL>` (polled, not WebSocket)
- **Content**: Total open interest (outstanding contracts) for a symbol
- **Schedule**: Polled every 5 minutes (configurable)
- **Significance**: Rising OI + rising price = strong trend confirmation. OI divergence from price = potential reversal signal. Not derivable from trades.
- **`exchange_ts` source**: `time` field in the REST response (milliseconds). If absent, fall back to `received_at / 1_000_000`.
- **Redpanda topic**: `binance.open_interest`
- **Key**: Symbol

### 3.3 Adding New Exchanges

Each exchange is implemented as an adapter in `src/exchanges/`. The adapter must:

1. Define WebSocket and REST base URLs
2. Implement stream subscription URL building
3. Implement response payload parsing (mapping exchange-specific fields to common envelope)
4. Implement exchange-native sequence validation logic (e.g., Binance depth `pu` chain)
5. Implement snapshot fetching via REST

The collector core, writer, Redpanda topics, file layout, and monitoring all work unchanged — only the exchange adapter is new.

---

## 4. Data Model

### 4.1 Message Envelope

Every message — regardless of stream type — is wrapped in a common envelope before being produced to Redpanda and archived. The `raw_text` field contains the original exchange payload captured as a string **before** JSON parsing to prevent lossy transformations (key reordering, number precision changes, whitespace normalization, duplicate key collapse).

**Data record example:**

```json
{
  "v": 1,
  "type": "data",
  "exchange": "binance",
  "symbol": "btcusdt",
  "stream": "trades",
  "received_at": 1741689600123456789,
  "exchange_ts": 1741689600120,
  "collector_session_id": "binance-collector-01_2026-03-11T00:00:00Z",
  "session_seq": 48291,
  "raw_text": "{\"e\":\"aggTrade\",\"E\":1741689600120,\"a\":123456,\"s\":\"BTCUSDT\",\"p\":\"65432.10\",\"q\":\"0.001\",\"f\":200001,\"l\":200005,\"T\":1741689600119,\"m\":true}",
  "raw_sha256": "a1b2c3d4e5f6..."
}
```

**Gap record example:**

```json
{
  "v": 1,
  "type": "gap",
  "exchange": "binance",
  "symbol": "btcusdt",
  "stream": "trades",
  "received_at": 1741689700000000000,
  "collector_session_id": "binance-collector-01_2026-03-11T00:00:00Z",
  "session_seq": 48350,
  "gap_start_ts": 1741689600123456789,
  "gap_end_ts": 1741689700000000000,
  "reason": "ws_disconnect",
  "detail": "WebSocket ConnectionClosed after 14h32m, reconnected in 2.3s"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `v` | int | Envelope schema version. Starts at `1`. Incremented only on breaking changes. Consumers must ignore unknown fields (forward-compatible). |
| `type` | string | Record type: `"data"` (exchange payload) or `"gap"` (gap event marker). Always present. Consumers filtering for exchange data should skip `type: "gap"` records. See gap record fields table below. |
| `exchange` | string | Exchange identifier (lowercase): `"binance"` |
| `symbol` | string | Trading pair (lowercase): `"btcusdt"` |
| `stream` | string | Stream type: `"trades"`, `"depth"`, `"depth_snapshot"`, `"bookticker"`, `"funding_rate"`, `"liquidations"`, `"open_interest"`. Always plural where applicable. Must match the Redpanda topic suffix and file directory name exactly (e.g., `stream="trades"` → topic `binance.trades` → directory `trades/`). |
| `received_at` | int64 | Local nanosecond timestamp (`time.time_ns()`). Server must run NTP (chrony). |
| `exchange_ts` | int64 | Exchange-provided timestamp (milliseconds for Binance). Extracted from the raw text via lightweight parsing. |
| `collector_session_id` | string | Unique identifier for this collector lifetime: `{collector_id}_{startup_timestamp_ISO}`. A new session starts on every collector restart. Used to scope `session_seq` — sequences are only meaningful within the same session. |
| `session_seq` | int64 | Monotonically increasing counter per (exchange, symbol, stream) tuple within a single collector session. Resets to 0 on collector restart. **Debugging-only** — useful for spotting in-session drops or reordering, but not a durable identity. Do not use for cross-session deduplication or gap detection in the archive. |
| `raw_text` | string | The original exchange payload as received from the WebSocket frame or HTTP response body, captured as a string **before** any JSON parsing. This is the authoritative raw data. Consumers who need structured access should parse this field themselves. |
| `raw_sha256` | string | SHA-256 hex digest of `raw_text`. Enables per-message integrity verification without parsing the payload. Can be used to detect corruption at any layer (Redpanda, compression, disk). |

When Binance combined streams are used, `raw_text` is the exact substring of the outer frame's `data` field. To ensure zero loss of fidelity (e.g. unicode escapes, nested keys), the extraction algorithm must parse the outer frame for routing, but extract the `data` value's raw byte range from the original frame string using string slicing (or an equivalent strict zero-copy mechanism), avoiding re-serialization entirely.

**Gap record fields** (present only when `type: "gap"`, replacing `raw_text` and `raw_sha256`):

| Field | Type | Description |
|-------|------|-------------|
| `gap_start_ts` | int64 | Nanosecond timestamp of the last successfully received message before the gap (same format as `received_at`). |
| `gap_end_ts` | int64 | Nanosecond timestamp when the gap ended (stream resumed or snapshot re-synced). |
| `reason` | string | Machine-readable gap cause. One of: `"ws_disconnect"` (WebSocket connection lost), `"pu_chain_break"` (depth diff `pu` validation failed), `"session_seq_skip"` (in-session sequence gap — collector internal drop), `"buffer_overflow"` (Redpanda unavailable, in-memory buffer full), `"snapshot_poll_miss"` (scheduled REST poll failed all retries). |
| `detail` | string | Human-readable context: duration, error message, reconnect time, number of messages affected. |

Gap records share the common envelope fields (`v`, `type`, `exchange`, `symbol`, `stream`, `received_at`, `collector_session_id`, `session_seq`) and receive broker coordinates (`_topic`, `_partition`, `_offset`) when archived by the writer — they flow through Redpanda like any other message. They do not carry `exchange_ts` (no exchange payload to extract it from).

**Why `raw_text` (string) instead of `raw` (object):** JSON parsing is a lossy transformation. A parsed-then-re-serialized JSON object may differ from the original in key ordering, numeric representation (`0.00100000` → `0.001`), whitespace, escape sequences, and duplicate keys. Storing the raw text as a string preserves the exact bytes received from the exchange, fulfilling the raw-first contract. The trade-off is slightly larger envelope size due to JSON string escaping of the embedded payload, but this is offset by zstd compression which handles repetitive escaped JSON efficiently.

### 4.1.1 Broker Coordinates (Writer-Stamped)

The writer appends broker coordinates to each envelope line before writing to disk. These fields are **not** set by the collector — they are added by the writer from the Kafka consumer record metadata.

| Field | Type | Description |
|-------|------|-------------|
| `_topic` | string | Redpanda topic the message was consumed from (e.g., `binance.trades`). |
| `_partition` | int | Partition number within the topic. |
| `_offset` | int64 | Kafka offset within the partition. Globally unique per (topic, partition). |

The tuple `(_topic, _partition, _offset)` is the **durable identity** of each message. It guarantees **intra-file ordering**: messages within a single archived file are strictly ordered by `_offset` (Kafka's partition guarantee). It is:
- **Globally unique**: Kafka guarantees exactly one message per (topic, partition, offset)
- **Stable**: Offsets do not change after write, regardless of collector restarts, session resets, or reprocessing
- **The basis for deduplication**: On writer restart with at-least-once semantics, duplicate records are detected by checking for repeated `(_topic, _partition, _offset)` tuples in the active file
- **The basis for archive verification**: The verify CLI checks for duplicate or missing offsets across hourly file boundaries

The `_` prefix convention signals these fields are infrastructure metadata added by the writer, not collector-produced data.

### 4.1.2 Schema Evolution

- The `v` field is always present and set to `1` for the initial version.
- New fields may be added in minor versions; consumers must ignore unknown fields.
- The `v` number increments only on breaking changes (field removal, type change, semantic change).
- All archived files contain the envelope version, so future consumers can determine the schema vintage without relying on file timestamps.

### 4.2 Time Synchronization

Accurate `received_at` timestamps require the host server to maintain clock synchronization:

- **Required**: `chrony` NTP daemon running on the host (or container with `NET_ADMIN` capability)
- **Target accuracy**: <1ms offset from UTC (chrony achieves this with good NTP servers)
- **Monitoring**: `collector_ntp_drift_ms` metric tracks estimated drift. Alert if >500ms sustained.
- **Future option**: PTP (Precision Time Protocol) for sub-microsecond accuracy if co-located with exchange servers

### 4.3 Redpanda Topics

Topic naming convention: `{exchange}.{stream}` where `{stream}` matches the envelope `stream` field exactly. This enables topic name derivation via simple string interpolation: `f"{exchange}.{stream}"`. Message key is always the symbol, enabling future partition-by-symbol scaling.

| Topic | Key | Partitions | Retention |
|-------|-----|------------|-----------|
| `binance.trades` | symbol | 1 (scale later) | 48h |
| `binance.depth` | symbol | 1 | 48h |
| `binance.depth_snapshot` | symbol | 1 | 48h |
| `binance.bookticker` | symbol | 1 | 48h |
| `binance.funding_rate` | symbol | 1 | 48h |
| `binance.liquidations` | symbol | 1 | 48h |
| `binance.open_interest` | symbol | 1 | 48h |

*Note on Topic Creation:* Relying on Kafka auto-creation is an anti-pattern as it uses cluster defaults (e.g., 7 days retention). The deployment must include a bootstrap script or init-container that uses the `rpk` CLI to explicitly create these topics with the configured 48h retention policy (`retention.ms=172800000`) before the Collector is allowed to start.

---

## 5. Storage

### 5.1 File Organization

```
/data/
└── binance/
    └── btcusdt/
        ├── trades/
        │   └── 2026-03-11/
        │       ├── hour-00.jsonl.zst
        │       ├── hour-00.jsonl.zst.sha256
        │       ├── hour-01.jsonl.zst
        │       ├── hour-01.jsonl.zst.sha256
        │       └── ...
        ├── depth/
        │   └── 2026-03-11/
        │       └── ...
        ├── depth_snapshot/
        │   └── 2026-03-11/
        │       └── ...
        ├── bookticker/
        │   └── 2026-03-11/
        │       └── ...
        ├── funding_rate/
        │   └── 2026-03-11/
        │       └── ...
        ├── liquidations/
        │   └── 2026-03-11/
        │       └── ...
        └── open_interest/
            └── 2026-03-11/
                └── ...
```

### 5.2 File Rules

- **One file per symbol per stream per hour**. Hourly rotation keeps files manageable (~10-50MB compressed for BTC/USDT trades).
- **SHA-256 checksum sidecar** (`.sha256`) for every compressed file. Written on rotation (file seal). Used to detect bit rot and verify integrity on read.
- **Date directories use UTC**. No timezone ambiguity.
- **Lowercase everything**. `btcusdt` not `BTCUSDT`. Avoids case-sensitivity issues across operating systems.
- **Files are append-only during the active hour**, then sealed and checksummed on rotation.
- **Format**: JSON Lines compressed with zstd. Each line is one message envelope (Section 4.1).

### 5.3 Compression

- **Algorithm**: Zstandard (zstd)
- **Level**: 3 (configurable, 1-19). Level 3 gives ~15-25x compression on repetitive JSON with fast compression speed.
- **Why zstd**: Better compression ratio than gzip at comparable speed. Supports streaming compression. Widely supported in data tools (pandas, polars, pyarrow).

### 5.4 Storage Estimates

Rough estimates for 5 symbols (BTC, ETH, SOL + 2 others), USD-M Futures:

| Stream | Raw JSON/day (est.) | Compressed/day (est.) |
|--------|--------------------|-----------------------|
| Trades | ~2-5 GB | ~150-300 MB |
| Depth diffs | ~5-10 GB | ~300-600 MB |
| Depth snapshots | ~500 MB | ~30-50 MB |
| BookTicker | ~3-6 GB | ~200-400 MB |
| Funding rate | ~50 MB | ~3-5 MB |
| Liquidations | ~10 MB | ~1 MB |
| Open interest | ~5 MB | ~0.5 MB |
| **Total** | **~10-22 GB/day** | **~700 MB - 1.4 GB/day** |

At ~1 GB/day compressed, a 1TB volume provides ~2.5 years of storage for 5 symbols.

### 5.5 Data Retention & Cleanup

- **Default policy**: Keep all archived data indefinitely. Raw data is the irreplaceable source of truth.
- **When disk reaches 85% (warning threshold)**: Operator must provision additional storage or migrate older data to cold/object storage (e.g., S3, GCS).
- **No automatic deletion**: The system never deletes archived files automatically. All cleanup is manual or via an external policy.
- **Future path — Archiver component**: A lightweight cron job or sidecar service (`archiver`) that automatically copies sealed hourly files (those with a `.sha256` sidecar) to object storage (S3, GCS, or MinIO).

### 5.6 Gap Auto-Healing (Backfilling)

While the system is designed to "never silently lose data" and explicitly marks missed messages with `type: "gap"` records, these gaps present problems for future backtesting and downstream DB loading.
- **Future Auto-Healing Worker:** A separate asynchronous worker should be implemented to scan the archive for `"type": "gap"` records.
- **Trades / AggTrades:** For missing trades, the worker automatically queries the exchange's REST API (e.g., Binance's `GET /fapi/v1/aggTrades` using `startTime` and `endTime` derived from the `gap_start_ts` and `gap_end_ts`), wraps the historical data in standard envelopes, and writes them to a specific backfill topic or file.
- **Order Book Depth (Out of Scope for v1):** Binance's REST API *does not* provide historical A2-level depth (100ms diffs). While Binance does publish daily CSV files of `depthUpdate` to their public data portal (`data.binance.vision`), automating this extraction is complex due to T+1 upload delays, CSV-to-JSON format translation, and massive file sizes. Therefore, **automatic order book gap backfilling is out of scope for v1**. Downstream DB loaders and backtesting engines must be designed to gracefully handle depth gaps by halting book updates and waiting for the next archived `depth_snapshot` record to reset their state. All recorded gaps, whether backfilled or not, must be prominently visualized on the Grafana dashboard so operators and analysts are immediately aware of data discontinuities.

### 5.7 Data Verification CLI

The project includes a CLI tool for verifying archive integrity:

```bash
# Verify all files for a specific date
cryptolake verify --date 2026-03-11

# Verify a specific symbol
cryptolake verify --exchange binance --symbol btcusdt --stream depth --date 2026-03-11

# Full verification: checksums + broker offset analysis across file boundaries
cryptolake verify --full --date 2026-03-11
```

Verification checks:
- SHA-256 checksum matches sidecar file (file-level integrity)
- Files are decompressible
- Each line is valid JSON matching the envelope schema
- Each `raw_sha256` matches `SHA-256(raw_text)` (per-message payload integrity)
- **Duplicate broker records**: No duplicate `(_topic, _partition, _offset)` tuples within or across hourly files. Duplicates indicate a writer dedup failure.
- **Depth bootstrap/replay**: For each symbol, verify that every depth diff sequence is anchored by a preceding `depth_snapshot` whose `lastUpdateId` satisfies the sync-point condition (`U <= lastUpdateId+1 && u >= lastUpdateId+1`). Replay the `pu` chain from that anchor and flag any break. This proves the archived depth data can reconstruct a valid order book. **Note**: This check requires parsing `raw_text` for `depth` and `depth_snapshot` records to extract `U`, `u`, `pu`, and `lastUpdateId`. This is the one place the verify CLI must parse `raw_text`, which is an exception to the general "consumers parse it themselves" guidance.
- **Persisted gap events**: Scan for `"type": "gap"` records in the archive. Report each gap with its `reason`, `symbol`, `stream`, and time window. No gap record should overlap with data records claiming to cover the same time range (i.e., a gap means data is genuinely missing, not just flagged).
- No missing hours (where data was expected based on stream activity and no gap record explains the absence)

### 5.8 Disaster Recovery

- **Archive volume loss:** Unrecoverable unless external backups exist. If loss occurs, restart the writer to at least recover the last 48h from Redpanda.
- **Redpanda volume loss:** No archive impact (data is already flushed to disk). The collector will recreate topics. Consumer offsets are lost, forcing the writer to rebuild its state file by scanning the archive before resuming.
- **Recommended backup cadence:** Daily `rsync` or object-storage sync of sealed files (`.sha256` present). *Note: Operators should avoid running backups while the writer is experiencing high consumer lag and catching up on past hours, as this may temporarily unseal and mutate files mid-backup.*

---

## 6. Configuration

### 6.1 Configuration File

```yaml
# config.yaml

server:
  time_sync: "chrony"

exchanges:
  binance:
    enabled: true
    market: "usdm_futures"
    ws_base: "wss://fstream.binance.com"
    rest_base: "https://fapi.binance.com"
    symbols:
      - btcusdt
      - ethusdt
      - solusdt
    streams:
      trades: true
      depth: true               # enables depth diffs AND depth snapshots together
      bookticker: true
      funding_rate: true
      liquidations: true
      open_interest: true
    depth:
      update_speed: "100ms"
      snapshot_interval: "5m"
      snapshot_overrides:
        btcusdt: "1m"
    open_interest:
      poll_interval: "5m"
    collector_id: "binance-collector-01"  # combined with startup timestamp to form collector_session_id

redpanda:
  brokers:
    - "redpanda:9092"
  retention_hours: 48
  # Config validator MUST reject retention_hours < 12 to ensure sufficient buffer for writer downtime.
  # topics_prefix is NOT used — topic names are derived from the envelope's
  # exchange and stream fields: f"{exchange}.{stream}" (see Section 4.3).

writer:
  base_dir: "/data"
  rotation: "hourly"
  compression: "zstd"
  compression_level: 3
  checksum: "sha256"
  flush_messages: 10000
  flush_interval_seconds: 30

monitoring:
  prometheus_port: 8000
  alerting:
    webhook_url: ""
    rules:
      gap_detected: "critical"
      connection_lost: "critical"
      writer_lag_seconds: 30
      disk_usage_pct: 85
```

### 6.2 Configuration Principles

- **Flat YAML, minimal nesting**: Easy to read, easy to override via environment variables in Docker/K8s (e.g., `EXCHANGES__BINANCE__SYMBOLS`).
- **Per-symbol snapshot overrides**: High-volume symbols can have more frequent snapshots.
- **Streams individually toggleable**: Add or disable stream types without code changes. Disabled streams are not subscribed, polled, archived, or assigned gap records. The `depth` toggle controls both depth diffs and depth snapshots as a unit — they are meaningless without each other.
- **Alert rules are declarative**: Thresholds, not logic.
- **Validated on startup**: Pydantic model validates the config before any connections are opened. Fail fast on misconfiguration. This includes validating that Redpanda `retention_hours` is safely above a minimum threshold (e.g., >= 12h) to prevent operator error from causing silent data expiry during writer maintenance.
- **Config changes require restart**: Adding/removing symbols or changing stream toggles requires a collector and/or writer restart. Hot-reload is not supported in v1. This is an intentional simplification — the system reconnects and re-syncs quickly on restart.

### 6.3 Environment Variable Overrides

For Docker/Kubernetes deployment, all config values can be overridden via environment variables using double-underscore nesting:

```
EXCHANGES__BINANCE__SYMBOLS=btcusdt,ethusdt
REDPANDA__BROKERS=redpanda-0:9092,redpanda-1:9092
WRITER__BASE_DIR=/mnt/data
MONITORING__ALERTING__WEBHOOK_URL=https://hooks.slack.com/...
```

---

## 7. Collector Design

### 7.1 Responsibilities

1. Connect to Binance USD-M Futures WebSocket streams
2. Manage WebSocket connections (two sockets per exchange split by traffic type, reconnection, 24h rotation)
3. Schedule periodic REST snapshot fetches
4. Poll REST endpoints for open interest
5. Wrap every incoming message in the common envelope (Section 4.1)
6. Produce enveloped messages to Redpanda topics
7. Detect in-session gaps (session_seq) and exchange-native sequence breaks (depth `pu` chain) and log/alert
8. Expose Prometheus metrics on `/metrics`
9. Respond to health checks on `/health`

### 7.2 Connection Manager

- Binance allows up to **1024 streams per WebSocket connection** and up to 5 connections per IP
- Connections are split by traffic type to isolate failure domains and match Binance endpoint semantics:
  - **`/ws/public`** — depth (`@depth@100ms`) + bookTicker (`@bookTicker`): 2 streams/symbol → **512 symbols/socket** theoretical, **500 operational cap**
  - **`/ws/market`** — aggTrade (`@aggTrade`) + markPrice (`@markPrice@1s`) + forceOrder (`@forceOrder`): 3 streams/symbol → **341 symbols/socket** theoretical, **300 operational cap**
- At 5 symbols this uses 10 + 15 = 25 total streams across 2 sockets (well within limits). A single `/ws/market` socket remains the bottleneck at scale: 300 symbols before needing a second connection.
- Handles Binance's mandatory 24-hour disconnect: proactively reconnects before the 24h mark
- Exponential backoff on disconnect: 1s, 2s, 4s, 8s, 16s, 32s, max 60s
- **Depth resync flow on reconnect or gap detection**:
  - *Pre-condition*: The system must not attempt a depth resync if the Redpanda producer is offline or actively dropping messages due to buffer overflow. It must wait for broker connectivity to be restored and the buffer to drain; otherwise, the resync snapshot will just fill the buffer or be immediately dropped.
  1. Reopen the depth WebSocket stream immediately
  2. Buffer all incoming depth diffs (do not discard — live updates arrive during the snapshot round-trip)
  3. Fetch a full REST snapshot concurrently (`GET /fapi/v1/depth?symbol=<SYMBOL>&limit=1000`)
  4. Once the snapshot arrives, drop all buffered diffs where `u < snapshot.lastUpdateId` (stale events)
  5. Find the first buffered diff where `U <= snapshot.lastUpdateId+1` AND `u >= snapshot.lastUpdateId+1` — this is the sync point
  6. Apply that diff and all subsequent diffs; from this point forward require `pu == previous u` for every event (the previous-update chain)
  7. If no buffered diff satisfies step 5, the buffer was too small or diffs arrived too late — retry the snapshot fetch up to 3 times with exponential backoff. If all retries fail (livelock prevention), emit a gap record, resume collecting unsynchronized diffs, and wait for the next scheduled periodic snapshot to act as the new anchor.

Reconnect / re-sync timeline:

```text
depth socket drops
    |
collector records disconnect_start_ts
    |
socket reconnect succeeds
    |
emit ws_disconnect gap window
    |
reopen depth stream immediately -----> buffer live diffs
    |                                   |
    +---- fetch REST snapshot ----------+
                                        |
snapshot arrives -> drop stale diffs -> find sync point -> validate pu chain
                                        |
release buffered diffs -> resume steady-state processing
```

### 7.3 Snapshot Scheduler

- Runs as an async task within the collector process
- Fetches full order book via `GET /fapi/v1/depth?symbol=<SYMBOL>&limit=1000`
- Default interval: 5 minutes, configurable per symbol
- Spreads requests across the interval to avoid bursting Binance rate limits. Periodic tasks begin with deterministic per-symbol offsets so the first cycle is staggered, not just subsequent ones. The same initial staggering pattern is applied to other periodic REST pollers such as open interest.
- On failure: retries 3x with backoff. If all fail, logs + alerts, skips cycle (next one in N minutes)
- If a scheduled poll exhausts all retries, the collector archives a `type: "gap"` record with `reason: "snapshot_poll_miss"` for the affected symbol/stream and time window.
- On WebSocket reconnect or gap detection: the depth resync flow (Section 7.2) triggers a snapshot fetch as part of its sync procedure

### 7.4 Gap Detector

- For depth diffs: validates the `pu` (previous last updateId) chain — each event's `pu` must equal the prior event's `u`. A break in this chain triggers the depth resync flow (Section 7.2).
- For other streams: monitors `session_seq` within the current `collector_session_id` for in-session drops or reordering. A session_seq gap indicates the collector dropped messages internally (e.g., buffer overflow). This is a debugging signal, not a durable gap — archive-level completeness is verified by the writer using broker offsets (Section 4.1.1).
- On gap detection:
  1. Logs gap event with symbol, stream, expected vs actual values, timestamp window
  2. Increments `collector_gaps_detected_total` metric
  3. For depth stream: triggers immediate snapshot re-sync
  4. Fires alert via Alertmanager
  5. Produces a **gap event record** to Redpanda (same topic as the affected stream) with `stream` set to the original stream name and a special envelope marker `"type": "gap"`. The gap record contains: `symbol`, `stream`, `gap_start_ts`, `gap_end_ts`, `reason` (e.g., `"ws_disconnect"`, `"pu_chain_break"`, `"session_seq_skip"`, `"snapshot_poll_miss"`), and `detail` (human-readable context). These records are archived alongside data, so the verify CLI and downstream consumers can discover known gaps without relying on external logs or metrics. Gap records are emitted only for streams that are currently enabled in config.

### 7.5 Producer

- Uses `aiokafka.AIOKafkaProducer`
- Serialization: JSON bytes (the envelope)
- Key: symbol (bytes) — enables future partition-by-symbol
- Acks: `acks=all` for durability. Note: with single-node Redpanda (development/small deployment), `acks=all` is equivalent to `acks=1` — true replication durability requires a multi-node Redpanda cluster. For single-node, data durability relies on Redpanda's fsync behavior.
- On Redpanda unavailability: buffers in bounded in-memory queue (configurable max, default 100,000 messages). Alerts if buffer exceeds 80% capacity.
- **Buffer overflow policy**: If the buffer fills completely, the collector drops the **newest** incoming messages to preserve chronological sequence. To prevent high-volume streams (`depth`) from starving low-volume irreplaceable streams (`liquidations`, `funding_rate`), the total in-memory buffer must be partitioned. E.g., for a 100,000 global max: `depth` gets max 80k, `trades` gets max 10k, and others share the remaining 10k (Note: these capacities should be tuned based on observed stream volumes).
- Linger: 5ms batch window for throughput optimization

Producer outage / overflow timeline:

```text
Redpanda unavailable
    |
producer retries and buffer grows
    |
80% full -> warning alert
    |
100% full -> newest messages dropped, counters increment, overflow window opens
    |
Redpanda recovers
    |
buffer drains to zero
    |
emit buffer_overflow gap record covering dropped interval
    |
resume normal publish path
```

---

## 8. Writer Design

### 8.1 Responsibilities

1. Consume messages from all Redpanda topics
2. Buffer messages per (exchange, symbol, stream) file target
3. Compress and flush to `.jsonl.zst` files
4. Rotate files hourly
5. Write SHA-256 checksums on file seal
6. Expose Prometheus metrics on `/metrics`
7. Respond to health checks on `/health`

### 8.2 Consumer

- Uses `aiokafka.AIOKafkaConsumer`
- Consumer group: `cryptolake-writer` (This group, combined with 1 partition per topic, enforces **Writer Exclusivity** — if two writers run accidentally, only one will receive messages. The writer should log a warning on startup if the group already has an active member).
- Subscribes to all `binance.*` topics
- **File Routing:** The writer assigns messages to hourly files based strictly on the message's internal `received_at` timestamp, *not* the writer's wall-clock time. If catching up on a past hour that has *already been sealed*, the writer will delete the old `.sha256` sidecar, reopen the `.jsonl.zst` file in append mode, stream the new data, and reseal with a new checksum when the buffer flushes.
- Manual offset commit after successful flush to disk (not auto-commit). If a commit batch spans multiple buffered files, the writer flushes all currently buffered records before committing that batch.
- On restart: resumes from last committed offset

**At-least-once delivery with writer-side deduplication**: If the writer crashes after flushing to disk but before committing the Kafka offset, it will re-consume some messages on restart. The writer guarantees duplicate-free archives:
- Every archived line carries broker coordinates `(_topic, _partition, _offset)` — the durable identity of the message (see Section 4.1.1)
- Since checking the tail of an incrementally compressed `.zst` file is extremely inefficient, the writer maintains a lightweight local state file (SQLite in WAL mode) tracking the highest flushed `_offset` per (topic, partition). This state file **must** live on the same volume as the archive data (`/data`). On startup, the writer must **first** scan for any unsealed files (missing `.sha256`), verify/seal them, and **then** load its state file. If the state file is lost, the writer must reconstruct it by scanning the tails of the newly-sealed/latest files for each topic/partition before consuming from Redpanda.
- **The archive is the contract**: downstream consumers (backtesting, analytics) can trust that archived files contain no duplicate broker records. Deduplication is the writer's responsibility, not pushed to consumers.
- `cryptolake verify` validates this guarantee by checking for duplicate `(_topic, _partition, _offset)` tuples (see Section 5.7)

Writer crash between flush and commit timeline:

```text
consume batch
    |
buffer by target file
    |
flush all active buffers to disk
    |
CRASH HERE? -------- yes ----------------------+
    |                                          |
    no                                         v
    |                                   restart writer
commit Kafka offsets                           |
    |                                   scan active/unsealed or latest sealed file
steady state                                |
                                            re-consume from last committed offset
                                            |
                                            skip already-archived broker offsets
                                            |
                                            flush -> commit -> steady state
```

### 8.3 Buffer Manager

- Maintains an in-memory buffer per target file `(exchange, symbol, stream, date, hour)`
- Flushes to disk when:
  - Buffer reaches size threshold (e.g., 10,000 messages or 10MB)
  - Flush timer expires (e.g., every 30 seconds)
  - Hour boundary triggers rotation
- Flushing is sequential per file to prevent interleaved writes
- Kafka offsets are committed only after successful disk flush

### 8.4 File Rotator

- File rotation is strictly synchronized with Kafka offset commits to prevent boundary edge cases. At each hour boundary (UTC), the current file is sealed:
  1. Stop consuming new messages temporarily
  2. Final flush of buffered data to disk
  3. Close the zstd compression stream
  4. Compute SHA-256 of the completed `.jsonl.zst` file
  5. Write `<filename>.sha256` sidecar
  6. Commit Kafka offsets for all flushed records and update local offset state file
  7. Increment `writer_files_rotated_total` metric
- New file opened for the next hour, and consumption resumes
- If no data arrived during an hour, no file is created (no empty files)

### 8.5 Compressor

- Uses `zstandard` Python library (C binding, fast)
- Streaming compression: messages are incrementally compressed into the active file
- Level 3 default (configurable)
- Each line in the compressed file is one JSON envelope, newline-terminated

---

## 9. Error Handling & Recovery

| Failure | Trigger / Symptom | Detection Signal | Automatic Recovery | Durable Artifact | Alert | Verification Method |
|---------|-------------------|------------------|--------------------|------------------|-------|---------------------|
| WebSocket disconnect | Socket closes unexpectedly | `websockets.ConnectionClosed`, socket state change | Exponential backoff reconnect, then depth re-sync if public socket | Archived `gap` record with `reason="ws_disconnect"` for enabled affected streams | `GapDetected`, `ConnectionLost` | Chaos test `kill_ws_connection.sh`, archive gap inspection, metrics timeline |
| Missed depth diffs | `pu` chain breaks | `pu != previous u` in depth handler | Depth re-sync flow: buffer diffs, fetch snapshot, find sync point, replay | Archived `gap` record with `reason="pu_chain_break"` | `GapDetected` | Depth replay in `cryptolake verify`, chaos test `depth_reconnect_inflight.sh` |
| Redpanda unavailable | Producer cannot publish | `KafkaError`, producer connectivity state | Buffer in memory, retry with backoff | No archive artifact unless overflow occurs; backlog remains in Redpanda/producer memory | `RedpandaBufferHigh` if buffer grows | Integration round-trip tests, manual broker outage drill |
| Producer buffer overflow | Buffer hits max capacity | buffer size threshold, dropped-message counter | Continue accepting exchange traffic, drop newest messages, close gap when backlog drains | Archived `gap` record with `reason="buffer_overflow"` plus dropped counters | `MessagesDropped` | Chaos test `buffer_overflow_recovery.sh`, verify CLI gap output |
| Writer crash | Process exits mid-consume | consumer lag rises, missing heartbeat | Restart writer, resume from last committed offset, dedup on broker coordinates | No duplicate archive records after restart; unsealed file may be sealed on boot | `WriterLagging` | Chaos tests `kill_writer.sh` and `writer_crash_before_commit.sh`, verify CLI dedup check |
| Scheduled REST poll miss | snapshot or open-interest poll fails all retries | retry exhaustion in periodic poller | Log, emit gap, wait until next interval | Archived `gap` record with `reason="snapshot_poll_miss"` | `SnapshotsFailing` | Forced poll-failure drill, verify CLI gap output |
| Disk almost full / full | storage usage rises above threshold | `writer_disk_usage_pct` metric | Warn at 85%, pause writer at 95% until space is freed | No immediate gap artifact; backlog remains recoverable in Redpanda while within retention | `DiskAlmostFull`, `DiskCritical` | Chaos test `fill_disk.sh`, lag recovery after freeing space |
| Binance rate limit | HTTP 429 | response status, `Retry-After` header | Back off per header; stagger future polls | None unless a poll fully exhausts retries, then `snapshot_poll_miss` gap record | `SnapshotsFailing` if retries keep failing | Manual rate-limit drill, poll spacing inspection |
| NTP drift | local clock diverges from exchange timestamps | sustained `received_at - exchange_ts` delta | Alert operator; fix host time sync | None | `NTPDrift` | Chrony check, Grafana latency/drift panels |
| Binance 24h forced disconnect | connection age approaches limit | proactive timer in connection manager | Reconnect before cutoff, then run depth re-sync on public socket | Archived `ws_disconnect` gap only if an actual downtime window occurs | `ConnectionLost` only if reconnect fails | Long-run soak test, reconnect metrics |

Key principle: **never silently lose data**. Every failure, gap, and anomaly is logged, metricked, and alerted.

---

## 10. Observability

### 10.1 Collector Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `collector_messages_produced_total` | counter | exchange, symbol, stream | Messages sent to Redpanda |
| `collector_ws_connections_active` | gauge | exchange | Current open WebSocket connections |
| `collector_ws_reconnects_total` | counter | exchange | Reconnection count |
| `collector_gaps_detected_total` | counter | exchange, symbol, stream | Sequence gaps found |
| `collector_exchange_latency_ms` | histogram | exchange, symbol, stream | `received_at - exchange_ts` distribution (Network Transit Time). Used to prove data represents reality vs lagged reality (latency arbitrage metric). |
| `collector_snapshots_taken_total` | counter | exchange, symbol | Successful REST snapshots |
| `collector_snapshots_failed_total` | counter | exchange, symbol | Failed snapshot attempts |
| `collector_ntp_drift_ms` | gauge | — | Estimated NTP clock drift |
| `collector_producer_buffer_size` | gauge | exchange | In-memory buffer size (messages) when Redpanda unavailable. The buffer is global per exchange (single `aiokafka` producer), so per-symbol breakdown is not available on this gauge — use `collector_messages_dropped_total` (which has symbol/stream labels) to identify which streams are affected during overflow. |
| `collector_messages_dropped_total` | counter | exchange, symbol, stream | Messages dropped due to buffer overflow |

### 10.2 Writer Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `writer_messages_consumed_total` | counter | exchange, symbol, stream | Messages read from Redpanda |
| `writer_consumer_lag` | gauge | exchange, stream | Messages behind head (gap proxy) |
| `writer_files_rotated_total` | counter | exchange, symbol, stream | File rotations completed |
| `writer_bytes_written_total` | counter | exchange, symbol, stream | Compressed bytes to disk |
| `writer_compression_ratio` | gauge | exchange, stream | Raw / compressed size ratio |
| `writer_disk_usage_bytes` | gauge | — | Total storage consumed |
| `writer_disk_usage_pct` | gauge | — | Percentage of volume used |
| `writer_flush_duration_ms` | histogram | exchange, stream | Time to flush buffer to disk |

All metrics with `symbol` label provide per-symbol granularity for dashboard filtering.

### 10.3 Alerting Rules

| Alert | Condition | Severity |
|-------|-----------|----------|
| `GapDetected` | `collector_gaps_detected_total` increases | critical |
| `ConnectionLost` | `collector_ws_connections_active == 0` for 30s | critical |
| `WriterLagging` | `writer_consumer_lag > 1000` for 2min | warning |
| `WriterLagCritical` | `writer_consumer_lag > 100000` (approx 5min, throughput-dependent) | critical |
| `SnapshotsFailing` | `collector_snapshots_failed_total` increases 3x in 15min | warning |
| `DiskAlmostFull` | `writer_disk_usage_pct > 85` | warning |
| `DiskCritical` | `writer_disk_usage_pct > 95` | critical |
| `HighLatency` | `collector_exchange_latency_ms` p99 > 500ms for 5min | warning |
| `NTPDrift` | `collector_ntp_drift_ms > 500` for 5min | warning |
| `RedpandaBufferHigh` | `collector_producer_buffer_size > 80000` | warning |
| `MessagesDropped` | `collector_messages_dropped_total` increases | critical |

### 10.4 Grafana Dashboard

Pre-built dashboard (`infra/grafana/dashboards/cryptolake.json`) with panels:

- **Message throughput**: messages/sec per symbol per stream (stacked area)
- **Exchange latency heatmap**: `received_at - exchange_ts` distribution over time
- **Consumer lag**: writer lag per topic (line chart, alerting threshold overlay)
- **Connection status**: WS connections active, reconnect events timeline
- **Gap timeline**: gap events plotted on time axis per symbol
- **Disk usage**: storage trend with projected full date
- **Snapshot health**: successful vs failed snapshots per symbol
- **Compression efficiency**: compression ratio over time

---

## 11. Deployment

### 11.1 Docker Compose Services

```yaml
services:
  redpanda:
    image: redpandadata/redpanda
    # Single-node, developer mode
    # Kafka API on 9092 (internal), admin on 9644 (internal only)
    ports:
      - "127.0.0.1:9092:9092"   # host-only for local debugging
    healthcheck:
      # Note: rpk path is stable in >= v24.x, but verify this healthcheck if pinning to major future versions.
      test: ["CMD-SHELL", "rpk cluster health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks: [cryptolake_internal]
    volumes:
      - redpanda_data:/var/lib/redpanda/data

  collector:
    build:
      dockerfile: Dockerfile.collector
    depends_on:
      redpanda:
        condition: service_healthy
    networks: [cryptolake_internal, collector_egress]
    volumes:
      - ./config:/app/config:ro
    environment:
      - CONFIG_PATH=/app/config/config.yaml
    healthcheck:
      test: ["CMD", "python", "-c", "from urllib.request import urlopen; raise SystemExit(0 if urlopen('http://127.0.0.1:8000/health', timeout=5).status == 200 else 1)"]
    restart: unless-stopped
    stop_grace_period: 30s
    logging:
      driver: "json-file"
      options:
        max-size: "50m"
        max-file: "5"

  writer:
    build:
      dockerfile: Dockerfile.writer
    depends_on:
      redpanda:
        condition: service_healthy
    networks: [cryptolake_internal]
    volumes:
      - ./config:/app/config:ro
      - data_volume:/data
    environment:
      - CONFIG_PATH=/app/config/config.yaml
    healthcheck:
      test: ["CMD", "python", "-c", "from urllib.request import urlopen; raise SystemExit(0 if urlopen('http://127.0.0.1:8001/health', timeout=5).status == 200 else 1)"]
    restart: unless-stopped
    stop_grace_period: 30s
    logging:
      driver: "json-file"
      options:
        max-size: "50m"
        max-file: "5"

  prometheus:
    image: prom/prometheus
    networks: [cryptolake_internal]
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus

  grafana:
    image: grafana/grafana
    ports:
      - "${GRAFANA_BIND:-127.0.0.1}:3000:3000"
    networks: [cryptolake_internal]
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GF_ADMIN_PASSWORD:?Set GF_ADMIN_PASSWORD in .env}
    volumes:
      - ./infra/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./infra/grafana/dashboards:/var/lib/grafana/dashboards:ro
    depends_on: [prometheus]

  alertmanager:
    image: prom/alertmanager
    command:
      - --config.file=/etc/alertmanager/alertmanager.yml
      - --config.expand-env
    environment:
      - WEBHOOK_URL=${WEBHOOK_URL:?Set WEBHOOK_URL in .env}
    networks: [cryptolake_internal, alertmanager_egress]
    volumes:
      - ./infra/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro

volumes:
  redpanda_data:
  prometheus_data:
  data_volume:

networks:
  cryptolake_internal:
    driver: bridge
    internal: true    # no outbound internet access
  collector_egress:
    driver: bridge    # collector needs internet for Binance API
  alertmanager_egress:
    driver: bridge    # alertmanager needs internet for webhook delivery (Slack, PagerDuty)
```

### 11.2 Orchestration Readiness

All custom services (collector, writer) are designed for Kubernetes:

- **Externalized config**: YAML file + environment variable overrides
- **Stateless collector**: No local state beyond in-memory buffers. Restartable at any time.
- **Writer state**: Consumer offset stored in Redpanda. File writes go to external volume. Restartable.
- **Resource limits**: Configurable CPU/memory limits per service
- **Logging**: Structured JSON to stdout. Docker/K8s log drivers handle collection.

### 11.3 Health & Readiness Endpoints

Both collector and writer expose HTTP endpoints for orchestrator probes:

**`/health` (liveness)**:
- Returns `200 {"status": "ok"}` if the process is alive and the event loop is responsive
- Returns `503` if the event loop is blocked or the process is in a bad state
- Used by Docker/K8s liveness probe — failure triggers container restart

**`/ready` (readiness)**:
- **Collector**: Returns `200` only if: (1) at least one WebSocket connection is established, AND (2) the Redpanda producer is connected
- **Writer**: Returns `200` only if: (1) the Redpanda consumer is connected and assigned partitions, AND (2) the storage volume is writable
- Returns `503` with a JSON body describing which checks failed
- Used by K8s readiness probe — failure removes the pod from service but does not restart

Container health checks must be executable in the base image as shipped. In slim Python images, prefer a Python-stdlib probe (`urllib.request`) or include an explicit probe binary; do not assume `curl` is present.

### 11.4 Graceful Shutdown

On SIGTERM, services execute a clean shutdown sequence within 30 seconds (controlled by Docker's `stop_grace_period` or K8s `terminationGracePeriodSeconds`, not application config):

**Collector shutdown sequence**:
1. Stop accepting new WebSocket messages (close subscriptions)
2. Flush any buffered messages to Redpanda producer
3. Wait for producer to drain (with timeout)
4. Close WebSocket connections
5. Close Redpanda producer
6. Exit

**Writer shutdown sequence**:
1. Stop consuming new messages from Redpanda
2. Flush all in-memory buffers to disk
3. Commit final Kafka offsets
4. Close active zstd compression streams (files remain valid but unsealed)
5. Exit

**Timeout behavior**: If shutdown exceeds the configured timeout, the process logs a warning and exits. Any active file that was not properly sealed (no checksum sidecar) is detected on next startup — the writer re-opens the file, verifies its content, computes the checksum, and seals it before starting normal operation.

### 11.5 Production Deployment

Production Kubernetes deployment specifics (replica counts, pod anti-affinity, PV provisioning, backup/restore procedures) are out of scope for v1 and will be documented in a separate operations runbook when the system moves beyond single-node Docker Compose deployment.

**Multi-node Redpanda upgrade path**: When the system outgrows single-node Redpanda (e.g., higher throughput, need for true replication durability), the upgrade path is:
1. Deploy a 3-node Redpanda cluster (e.g., via Redpanda Helm chart or `rpk` cluster setup)
2. Increase topic replication factor to 3 (`rpk topic alter-config binance.trades --set replication.factor=3` for each topic)
3. Update the collector's `REDPANDA__BROKERS` to list all 3 broker addresses
4. At this point `acks=all` provides true replication durability — a message is acknowledged only after all in-sync replicas confirm
5. No code changes required — only config and infrastructure. The collector and writer are already designed for multi-broker (the broker list is a config array).

### 11.6 Time Synchronization

The host running the collector must have accurate time:

```bash
# Install and enable chrony
sudo apt install chrony
sudo systemctl enable chrony

# Verify synchronization
chronyc tracking
# Offset should be < 1ms
```

For Docker: the container inherits the host clock. Ensure the Docker host runs chrony/ntpd.

### 11.7 Deployment Rollout

- **Writer update:** Stop writer → Redpanda buffers traffic → deploy new image → writer catches up from last committed offset. Zero data loss (within 48h retention).
- **Collector update:** Stop collector → connection drops, gap window opens → deploy new image → collector reconnects, re-syncs depth, and automatically emits a gap record.
- Zero-downtime collection is not a v1 goal; brief planned gaps (~30s) during restarts are acceptable and self-documenting.
- **Rollback:** Revert to the previous image tag and restart. No schema migrations to undo.
- **Schema Compatibility:** If an update adds fields to the envelope, writers and consumers are protected by the forward-compatibility rule (Section 4.1.2: ignore unknown fields).

---

## 12. Security

### 12.1 Network Boundaries

All internal services communicate over a **private Docker network** (`cryptolake_internal`). Only the operator-facing Grafana dashboard is exposed to the host:

| Service | Port | Binding | Rationale |
|---------|------|---------|-----------|
| Redpanda Kafka API | 9092 | `127.0.0.1:9092` (host-only) | Collector and writer connect via Docker network name. Host binding for local debugging only. |
| Redpanda Admin | 9644 | **not exposed** | Admin API bound to internal network only. Access via `docker exec` or SSH tunnel. |
| Prometheus | 9090 | **not exposed** | Grafana connects via internal network. Operators use SSH tunnel or `docker exec` for ad-hoc queries. |
| Alertmanager | 9093 | **not exposed** | Prometheus connects via internal network. Alertmanager joins `alertmanager_egress` network for outbound webhook delivery (Slack, PagerDuty, etc.). |
| Grafana | 3000 | `0.0.0.0:3000` (configurable) | Operator dashboard. Secured with credentials (see 12.2). Bind to `127.0.0.1:3000` in production and access via reverse proxy or SSH tunnel. |
| Collector metrics | 8000 | **not exposed** | Prometheus scrapes via internal network. |
| Writer metrics | 8001 | **not exposed** | Prometheus scrapes via internal network. |

The docker-compose `networks` block enforces this:

```yaml
networks:
  cryptolake_internal:
    driver: bridge
    internal: true   # no outbound internet — only collector needs egress
  collector_egress:
    driver: bridge   # collector connects to Binance via this network
```

The collector joins both `cryptolake_internal` (for Redpanda) and `collector_egress` (for Binance API). Alertmanager joins `cryptolake_internal` (for Prometheus) and `alertmanager_egress` (for webhook delivery to Slack/PagerDuty). All other services join only `cryptolake_internal`.

### 12.2 Credentials & Secrets

**v1 posture**: No exchange API keys are needed — Binance USD-M Futures public streams and REST endpoints are unauthenticated. No secrets are stored in config files.

Credentials that do exist:
- **Grafana admin password**: Set via `GF_SECURITY_ADMIN_PASSWORD` environment variable. Default `admin` must be changed before any network-accessible deployment. The docker-compose template sets this via a `.env` file (git-ignored) for local development. **Note: For production, `.env` files must be replaced with strict secret managers like Docker Secrets or K8s Secrets.**
- **Alertmanager webhook URL**: The Slack/Discord/PagerDuty webhook URL in `alertmanager.yml` is a secret. Store it via environment variable substitution (`$WEBHOOK_URL`), not hardcoded in the config file. The `.env` file is git-ignored.
- When running Alertmanager in Docker Compose, start it with config env expansion enabled (for example `--config.expand-env`) and pass `WEBHOOK_URL` into the container environment; otherwise the placeholder remains literal at runtime.

**Future (if authenticated exchange APIs are added)**: API keys and secrets must be injected via environment variables or a secrets manager (e.g., Docker secrets, K8s Secrets, Vault). Never stored in config files or images.

### 12.3 Data Security

- Archived `.jsonl.zst` files contain only public market data — no PII, no credentials.
- File permissions: Writer creates files with `0644` and directories with `0755`. The writer process runs as a non-root user in the container.
- The container image pre-creates `/data` owned by the application user so a first-use Docker named volume inherits writable permissions without running the writer as root.
- No encryption at rest in v1 (public data). If sensitive data streams are added later, volume-level encryption (LUKS, EBS encryption) is the recommended approach.

---

## 13. Project Structure

```
cryptolake/
├── docker-compose.yml
├── Dockerfile.collector
├── Dockerfile.writer
├── config/
│   ├── config.yaml
│   └── config.example.yaml
├── src/
│   ├── collector/
│   │   ├── __init__.py
│   │   ├── main.py                 # entry point, uvloop setup
│   │   ├── connection.py           # WS connection manager (mux, reconnect)
│   │   ├── streams/
│   │   │   ├── __init__.py
│   │   │   ├── base.py             # base stream handler interface
│   │   │   ├── trades.py
│   │   │   ├── depth.py
│   │   │   ├── bookticker.py
│   │   │   ├── funding_rate.py
│   │   │   ├── liquidations.py
│   │   │   └── open_interest.py    # REST poller (not WS)
│   │   ├── snapshot.py             # periodic REST snapshot scheduler
│   │   ├── gap_detector.py
│   │   ├── producer.py             # Redpanda/Kafka producer wrapper
│   │   └── metrics.py              # Prometheus metric definitions
│   ├── writer/
│   │   ├── __init__.py
│   │   ├── main.py                 # entry point
│   │   ├── consumer.py             # Redpanda/Kafka consumer
│   │   ├── compressor.py           # zstd streaming compression
│   │   ├── file_rotator.py         # hourly rotation + SHA-256 checksums
│   │   └── metrics.py              # Prometheus metric definitions
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py               # YAML config loader + Pydantic validation
│   │   ├── envelope.py             # message envelope schema (Pydantic model)
│   │   └── logging.py              # structured JSON logging setup
│   ├── exchanges/
│   │   ├── __init__.py
│   │   ├── base.py                 # base exchange adapter interface
│   │   └── binance.py              # Binance-specific: URL building, payload parsing
│   └── cli/
│       ├── __init__.py
│       └── verify.py               # data verification CLI tool
├── tests/
│   ├── unit/
│   │   ├── test_envelope.py
│   │   ├── test_gap_detector.py
│   │   ├── test_file_rotator.py
│   │   ├── test_compressor.py
│   │   └── test_config.py
│   ├── integration/
│   │   ├── test_binance_ws.py      # uses recorded fixtures in CI, live optional
│   │   ├── test_redpanda_roundtrip.py  # testcontainers
│   │   └── test_writer_rotation.py
│   ├── fixtures/
│   │   ├── binance_trade_sample.json
│   │   ├── binance_depth_diff_sample.json
│   │   ├── binance_depth_snapshot_sample.json
│   │   └── ...
│   ├── chaos/
│   │   ├── kill_writer.sh
│   │   ├── kill_ws_connection.sh
│   │   └── fill_disk.sh
│   └── conftest.py
├── infra/
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── provisioning/
│   │   │   ├── datasources.yml
│   │   │   └── dashboards.yml
│   │   └── dashboards/
│   │       └── cryptolake.json
│   ├── alertmanager/
│   │   └── alertmanager.yml
│   └── redpanda/
│       └── redpanda.yml
├── pyproject.toml
└── README.md
```

---

## 14. Testing Strategy

### 14.1 Unit Tests

- **Scope**: Pure functions with no external dependencies
- **Coverage**: Envelope schema validation, gap detector state machine, file rotator path generation and checksum computation, config parsing and validation, compression round-trip (compress → decompress → verify identical)
- **Framework**: pytest
- **Run**: `pytest tests/unit/`

### 14.2 Integration Tests

- **Scope**: Component interactions with real (containerized) dependencies
- **Coverage**:
  - Binance WebSocket connection + message parsing (recorded fixtures in CI, live connection for manual validation)
  - Redpanda produce/consume round-trip (testcontainers spins up Redpanda)
  - Writer file output: consume from Redpanda → verify correct file paths, decompressible content, valid checksums
- **Framework**: pytest + testcontainers-python
- **Run**: `pytest tests/integration/`

### 14.3 End-to-End Tests

- **Scope**: Full pipeline verification
- **Method**: `docker-compose -f docker-compose.test.yml up`
- **Verification**:
  1. All services start and pass health checks
  2. Collector connects to Binance and produces messages
  3. Writer creates files in expected directory structure
  4. Files are decompressible and contain valid JSON envelopes
  5. SHA-256 checksums match
  6. Prometheus metrics are populated
  7. No gap alerts fired during test window

### 14.4 Chaos Tests

- **Scope**: Failure mode validation
- **Scripts** in `tests/chaos/`:
  - Kill writer mid-stream → verify zero data loss on restart (consumer lag recovers, no duplicate files)
  - Kill WebSocket connection → verify auto-reconnect, snapshot re-sync, gap logged
  - Fill disk to 95% → verify writer pauses, alert fires, resumes after space freed
  - **Depth reconnect while snapshot is in flight**: Kill the `/ws/public` WebSocket while a periodic REST snapshot request is pending. Verify: (a) incoming diffs are buffered on the new connection, (b) the in-flight snapshot completes or is re-fetched, (c) the `pu` chain is re-established from the snapshot's `lastUpdateId`, (d) no diffs are silently dropped during the overlap, (e) a gap event record is written to the archive with `reason: "ws_disconnect"`.
  - **Writer crash after flush before offset commit**: Kill the writer process (`docker kill`) immediately after a disk flush but before the Kafka offset commit. Verify: (a) on restart, the writer re-consumes from the last committed offset, (b) duplicate `(_topic, _partition, _offset)` records are detected and skipped via the active-file tail scan, (c) the resulting archive contains no duplicate broker records, (d) `cryptolake verify` passes clean.
  - **Gap event written for disconnect / missed poll**: Force a WebSocket disconnect and separately skip a scheduled open_interest REST poll. Verify: (a) a `"type": "gap"` record appears in the archive for each incident, (b) the gap record's `reason` field matches the cause (`"ws_disconnect"` / `"snapshot_poll_miss"`), (c) the gap time window is accurate (within 1s of actual downtime), (d) `cryptolake verify` reports the gaps in its output.
  - **Buffer overflow at Redpanda recovery boundary**: Stop Redpanda while the collector is receiving high-frequency data (e.g., during a BTC volatility spike). Wait until the in-memory buffer fills to 100% and messages are being dropped. Then restart Redpanda. Verify: (a) the gap occurs at the moment of buffer overflow, not at the moment of Redpanda recovery — i.e., the newest messages dropped during the overflow period are the gap, and the collector resumes producing to Redpanda immediately on recovery without a second gap, (b) a `"type": "gap"` record is written with `reason: "buffer_overflow"` covering the exact time window of dropped messages, (c) `collector_messages_dropped_total` matches the count of messages in the gap window, (d) no messages are silently lost outside the reported gap window, (e) gap-filling logic (if implemented later) can use the gap record's timestamps to back-fill from Binance's REST historical endpoints.
- **Run**: Manual, against running docker-compose stack

### 14.5 Test Fixtures

Recorded real Binance WebSocket messages stored in `tests/fixtures/`. These provide deterministic input for unit and integration tests without network dependency. Fixtures include:

- Trade messages (single, burst)
- Depth diff messages (sequential, with gap)
- Depth snapshot (full book)
- BookTicker updates
- Funding rate updates
- Liquidation events
- Open interest responses

---

## 15. Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Python | 3.12 | Core application (pinned in Dockerfile) |
| Package Manager | uv (by Astral) | latest | Fast, deterministic dependency resolution and Docker builds |
| Event loop | uvloop | latest | High-performance asyncio replacement |
| JSON Parser | orjson | latest | C/Rust-backed JSON parsing to prevent GIL blocking |
| WebSocket | websockets | latest | Exchange WebSocket connections |
| HTTP client | aiohttp | latest | REST API calls (snapshots, open interest) |
| Kafka client | aiokafka | latest | Redpanda producer/consumer |
| Compression | zstandard | latest | zstd compression (C binding) |
| Validation | pydantic | v2 | Config and envelope schema validation |
| Metrics | prometheus-client | latest | Prometheus metric exposition |
| Logging | structlog | latest | Structured JSON logging. All loggers must bind `collector_session_id`, `symbol`, and `stream` to the context so every log line is easily filterable. |
| Config | pyyaml | latest | YAML config parsing |
| Message broker | Redpanda | ≥24.1 | Durable message buffer (Kafka-compatible). Pin to specific minor in docker-compose. |
| Metrics DB | Prometheus | ≥2.51 | Time-series metrics storage. Pin to specific minor in docker-compose. |
| Dashboards | Grafana | ≥11.0 | Visualization and dashboards. Pin to specific minor in docker-compose. |
| Alerting | Alertmanager | ≥0.27 | Alert routing and notifications. Pin to specific minor in docker-compose. |
| Testing | pytest | latest | Test framework |
| Test containers | testcontainers-python | latest | Containerized integration test dependencies |

### 15.1 Dependency Pinning

- All Python dependencies MUST be pinned via `uv.lock` (committed to repo).
- Docker base images MUST be pinned to a specific Python patch version (e.g., `python:3.12.7-slim`).
- Infrastructure components (Redpanda, Prometheus, Grafana, Alertmanager) MUST be pinned to specific minor versions in `docker-compose.yml`.
- Monthly dependency update cadence with full test suite validation.

---

## 16. Dependencies & Constraints

### 16.1 External Dependencies

- **Binance USD-M Futures API**: Subject to rate limits, maintenance windows, and API changes. The collector must handle all gracefully.
- **Network connectivity**: Stable, low-latency connection to Binance servers. To minimize Network Transit Time (`collector_exchange_latency_ms`) and remain competitive for future algorithmic trading layers, the deployment infrastructure should prioritize co-location or cloud regions geographically closest to Binance's matching engines (e.g., AWS `ap-northeast-1` / Tokyo).
- **NTP servers**: Reliable NTP sources for chrony. Use multiple sources for redundancy.

### 16.2 Binance API Limits

- WebSocket: Max 1024 streams per connection, max 5 connections per IP
- REST: 2400 request weight per minute (depth snapshot = weight 20 for limit=1000, open interest = weight 5)
- With 5 symbols: depth snapshot every 5 minutes = 60 requests/hour × weight 20 = 1200 weight/hour; open interest every 5 minutes = 60 requests/hour × weight 5 = 300 weight/hour. Combined = 1500 weight/hour = 25 weight/minute (well within the 2400/minute limit)

### 16.3 Resource Requirements (Estimated)

For 5 symbols, all streams enabled:

| Service | CPU | Memory | Disk |
|---------|-----|--------|------|
| Collector | 0.5 core | 256 MB | minimal |
| Writer | 0.5 core | 512 MB | ~1 GB/day compressed |
| Redpanda | 1 core | 512 MB | ~2 GB (48h retention) |
| Prometheus | 0.5 core | 512 MB | ~5 GB (2 week retention) |
| Grafana | 0.25 core | 256 MB | minimal |
| Alertmanager | 0.1 core | 64 MB | minimal |
| **Total** | **~3 cores** | **~2 GB** | **~1 GB/day growing** |

A single 4-core VPS with 4-8 GB RAM and a 500 GB volume handles this comfortably for months.

### 16.4 Capacity Planning Triggers

| Signal | Threshold | Action |
|---|---|---|
| Symbols count | > 50 | Add second `/ws/market` connection |
| Symbols count | > 300 | Add second `/ws/public` connection |
| REST rate weight | > 1500/min sustained | Implement shared token-bucket rate limiter |
| Compressed data/day | > 10 GB | Prioritize building external Archiver |
| Writer consumer lag | > 10 min sustained | Profile I/O, consider faster disk or parallel flushing |
| Redpanda disk | > 70% of retention window | Increase volume size or reduce retention hours |

---

## Appendix A. Acceptance Traceability

This table links the acceptance criteria to the implementation plan tasks and primary validation points so reviewers can audit coverage quickly.

| Acceptance Criterion | Primary Build Tasks in Plan | Primary Verification |
|----------------------|-----------------------------|----------------------|
| `AC-1` End-to-end data collection | Tasks 4, 10, 15, 21, 23, 25, 29 | E2E compose run, archive file existence, envelope schema validation |
| `AC-2` Data integrity verification | Tasks 18, 19, 21, 22, 31 | `cryptolake verify`, checksum sidecars, duplicate broker-coordinate scan |
| `AC-3` Order book reconstruction | Tasks 12, 13, 15, 22 | Depth bootstrap/replay check in verify CLI, depth chaos scenario |
| `AC-4` Automatic reconnection and recovery | Tasks 11, 12, 14, 15, 30 | WebSocket disconnect drill, gap records, reconnect metrics |
| `AC-5` Writer crash resilience | Tasks 19, 21, 27, 30 | Writer restart drill, lag catch-up, archive offset comparison |
| `AC-6` Observability operational | Tasks 8, 16, 23, 24, 29 | Prometheus scrape success, Grafana dashboard load, Alertmanager webhook firing |
| `AC-7` Storage organization | Tasks 18, 21, 28 | File path assertions, lowercase naming checks, no empty archive files |
| `AC-8` Configuration-driven behavior | Tasks 3, 6, 13, 15, 25 | Config unit tests, disabled-stream behavior, per-symbol snapshot overrides |
| `AC-9` Rate limit compliance | Tasks 13, 15, 24 | Staggered poll scheduling, 429 backoff behavior, metrics/alert review |
| `AC-10` Graceful shutdown | Tasks 14, 15, 21, 23, 29 | SIGTERM shutdown drill, final flush and offset commit verification |
| `AC-11` Writer deduplication guarantee | Tasks 19, 21, 22, 30 | Crash-after-flush chaos test, duplicate offset check in verify CLI |

## Appendix B. Architectural Decision Log

| ID | Decision | Alternatives Considered | Why This Decision Stands |
|----|----------|-------------------------|--------------------------|
| `ADR-001` | Put Redpanda between collector and writer | direct IPC queue, shared filesystem handoff | Broker-backed decoupling gives replay, lag visibility, and crash resilience without custom queue semantics. |
| `ADR-002` | Store exact `raw_text` in the archive and treat it as authoritative | parse-and-normalize JSON only, columnar-first storage | Raw-first storage preserves lossless payload fidelity and allows future re-parsing without schema regret. |
| `ADR-003` | Archive gap events on the same topic / stream namespace as the affected data | separate gap topic, logs-only gap handling | Same-stream gap records keep downstream verification and replay local to the data they qualify. |
| `ADR-004` | Split Binance traffic across `/ws/public` and `/ws/market` sockets | one combined socket for everything | Traffic-type split matches exchange semantics and narrows failure domains during reconnects and re-syncs. |
| `ADR-005` | Make the writer responsible for broker-coordinate stamping and deduplication | collector-stamped broker metadata, consumer-side dedup downstream | Broker offsets are only authoritative at consume time; centralizing dedup in the writer keeps the archive contract clean for all downstream consumers. |
| `ADR-006` | Use periodic REST polling for `depth_snapshot` and `open_interest` | WebSocket-only collection, derived snapshots | Binance exposes these data shapes naturally via REST; polling keeps implementation simple and verification explicit. |

## Appendix C. Operator Runbook

This appendix is a v1 operator quick-reference, not a full production SRE runbook.

### C.1 Startup Checks

1. Confirm the stack renders cleanly: `docker compose config`
2. Start services: `docker compose up -d`
3. Confirm containers are healthy: `docker compose ps`
4. Tail startup logs for the custom services: `docker compose logs collector writer --tail=100`
5. Confirm archives are being created after traffic arrives: `docker compose exec writer find /data -type f | head`

### C.2 Expected Shutdown Behavior

When `docker compose down` is issued, expect:
- collector to stop accepting new exchange messages and drain its producer buffer
- writer to flush all active file buffers, commit final offsets, and exit
- any active-but-unsealed file to be sealed on the next startup before normal consumption resumes

If shutdown was abrupt, verify recovery with:

```bash
docker compose up -d writer
docker compose logs writer --tail=100
docker compose exec writer python -m cli.verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
```

### C.3 Disk-Full Response

1. Check current usage in Grafana or via `docker compose exec writer df -h /data`
2. If usage exceeds 95%, expect the writer to pause consumption while Redpanda retains backlog
3. Free space or expand the volume
4. Confirm lag begins decreasing after remediation
5. Run `cryptolake verify` for the affected date to confirm archive integrity after recovery

### C.4 Validate a Gap Record

1. Run `docker compose exec writer python -m cli.verify --date <YYYY-MM-DD> --base-dir /data --full`
2. Confirm the reported gap includes `symbol`, `stream`, `reason`, `gap_start_ts`, and `gap_end_ts`
3. If needed, decompress the affected hourly file and inspect the `type: "gap"` line directly
4. Correlate the gap window with Prometheus/Grafana metrics and container logs

### C.5 Recover Unsealed Files

On startup, the writer automatically scans for `.jsonl.zst` files without `.sha256` sidecars, re-reads them, restores dedup state, and seals them. If an operator suspects this path failed:

1. Stop the writer
2. Inspect `/data` for `.jsonl.zst` files missing `.sha256`
3. Restart the writer and watch for scan/seal log entries
4. Re-run `cryptolake verify` after sealing completes

### C.6 Rotate Secrets

For v1, the relevant secrets are `GF_SECURITY_ADMIN_PASSWORD` and `WEBHOOK_URL`.

1. Update `.env` or the secret source used by the orchestrator
2. Recreate the affected service:
   - Grafana password change: `docker compose up -d --force-recreate grafana`
   - Alertmanager webhook change: `docker compose up -d --force-recreate alertmanager`
3. Confirm the rendered config and service logs show the new value was accepted without exposing the secret in plaintext output

