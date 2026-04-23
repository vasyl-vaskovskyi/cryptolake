---
module: collector
status: complete
produced_by: analyst
python_files:
  - src/collector/__init__.py
  - src/collector/backup_chain_reader.py
  - src/collector/connection.py
  - src/collector/gap_detector.py
  - src/collector/main.py
  - src/collector/metrics.py
  - src/collector/producer.py
  - src/collector/snapshot.py
  - src/collector/streams/__init__.py
  - src/collector/streams/base.py
  - src/collector/streams/depth.py
  - src/collector/streams/open_interest.py
  - src/collector/streams/simple.py
  - src/collector/tap.py
python_test_files:
  - tests/unit/collector/test_tap.py
  - tests/unit/test_backup_chain_reader.py
  - tests/unit/test_chunk3_fixes.py
  - tests/unit/test_collector_lifecycle.py
  - tests/unit/test_depth_resync_skip.py
  - tests/unit/test_disconnect_gap_coalescing.py
  - tests/unit/test_emit_gap.py
  - tests/unit/test_gap_analyzer.py
  - tests/unit/test_gap_detector.py
  - tests/unit/test_gap_records_written_metric.py
  - tests/unit/test_producer_topic_prefix.py
  - tests/unit/test_snapshot.py
---

## 1. Module summary

The `collector` module ingests real-time market data (depth, trades, funding rates, liquidations, open interest) from Binance WebSocket and REST APIs, normalizes messages into envelopes with raw-text byte preservation and SHA256 hashing, detects gaps via sequence validation and pu-chain analysis, buffers overflows per stream, and produces Redpanda messages keyed by symbol with per-stream topic partitioning. Primary entry: `Collector` class in `main.py`, instantiated by `src/common/service_runner.run_service()`. Upstream consumers: `writer` module (reads topics `{prefix}binance.{stream}`), `backfill` module (replay).

## 2. File inventory

| File | Lines | Role | Public symbols |
|------|-------|------|---|
| `__init__.py` | 2 | glue | (empty) |
| `main.py` | 295 | lifecycle / orchestration | `Collector`, `main()` |
| `connection.py` | 322 | WebSocket I/O + reconnect + backpressure | `WebSocketManager` |
| `producer.py` | 255 | Kafka envelope routing + overflow tracking | `CryptoLakeProducer` |
| `streams/base.py` | 54 | abstract handler contract | `StreamHandler` |
| `streams/simple.py` | 34 | handler for trades/bookticker/funding_rate/liquidations | `SimpleStreamHandler` |
| `streams/depth.py` | 171 | depth diff validation + pu-chain + resync buffering | `DepthHandler` |
| `streams/open_interest.py` | 122 | REST polling (separate from WebSocket) | `OpenInterestPoller` |
| `snapshot.py` | 159 | REST depth snapshots on interval | `SnapshotScheduler` |
| `gap_detector.py` | 77 | pu-chain validator + session_seq tracker | `DepthGapDetector`, `SessionSeqTracker` |
| `metrics.py` | 50 | Prometheus metrics definitions | (10 metrics) |
| `backup_chain_reader.py` | 124 | one-shot Kafka consumer for recovery | `read_last_depth_update_id()` |
| `tap.py` | 35 | fixture-capture (Python-only, out of scope for Java) | `FrameTap` |

## 3. Public API surface

### `Collector.__init__(config_path: str)`
Loads YAML config, initializes session ID, BinanceAdapter, producer, stream handlers (SimpleStreamHandler for trades/bookticker/funding_rate/liquidations; DepthHandler for depth; OpenInterestPoller; SnapshotScheduler). Wires callbacks: `_on_producer_overflow()` → producer overflow callback, `_on_pu_chain_break(symbol)` → depth resync trigger.

### `Collector.start() -> Awaitable[None]`
Async lifecycle entry. Starts heartbeat loop (30s interval), WebSocketManager, SnapshotScheduler, OpenInterestPoller (if streams enabled), health server. Gathers all tasks until first exception.

### `Collector.shutdown() -> Awaitable[None]`
Cancels heartbeat, stops WebSocket, snapshot scheduler, OI poller. Flushes producer (10s timeout). Marks lifecycle shutdown in DB. Cancels remaining tasks.

### `CryptoLakeProducer.produce(envelope: dict) -> bool`
Produces envelope to `{prefix}{exchange}.{stream}`. Enforces per-stream buffer caps (depth: 80k, trades: 10k, others: 10k). On overflow, drops message and records window. Returns True if produced, False if dropped. Emits `buffer_overflow` gap when recovery detected.

### `CryptoLakeProducer.emit_gap(*, symbol, stream, session_seq, reason, detail, gap_start_ts=None, gap_end_ts=None) -> None`
Creates and produces gap envelope. Increments `gaps_detected_total` metric. Reason must be in `VALID_GAP_REASONS` (ws_disconnect, pu_chain_break, session_seq_skip, buffer_overflow, snapshot_poll_miss, etc.).

### `CryptoLakeProducer.is_connected() -> bool`
Polls broker via `list_topics(timeout=5)`. Returns True if reachable.

### `CryptoLakeProducer.is_healthy_for_resync() -> bool`
Returns False if any (symbol, stream) overflow window is active, OR pending > 80% of max_buffer, OR list_topics fails. Used as precondition for depth resync (spec 7.2).

### `WebSocketManager.start() -> Awaitable[None]`
Builds WS URLs (filtering REST-only streams: depth_snapshot, open_interest). Creates one task per socket (public, market). Each task runs `_connection_loop` (reconnect with exponential backoff 1s→60s, data → backpressure poll-on-drop, proactive 24h reconnect).

### `WebSocketManager.is_connected() -> bool`
Returns True if no WS streams expected OR all expected sockets are connected.

### `DepthHandler.set_sync_point(symbol, last_update_id) -> None`
Sets sync point on detector. Emits deferred `pu_chain_break` gap if any diffs were dropped during resync (stale/chain-break/buffer overflow). Replays pending diffs from buffer, validating pu-chain.

### `SnapshotScheduler.start() -> Awaitable[None]`
Fetches depth snapshots every `default_interval` (5m) with per-symbol overrides (staggered by `interval / symbol_count` to avoid thundering herd). Retry 3x with exp-backoff 1s, 2s. Emits `snapshot_poll_miss` gap on total failure.

### `OpenInterestPoller.start() -> Awaitable[None]`
Polls open interest every `poll_interval_seconds` (300s default) with staggered delays. Retry 3x. Emits `snapshot_poll_miss` gap on exhaustion.

### `DepthGapDetector.validate_diff(U, u, pu) -> DiffValidationResult`
Returns result with `valid`, `gap`, `stale`, `reason`. After sync point: first diff must satisfy `U <= lid+1 && u >= lid+1` (find sync diff), subsequent diffs must have `pu == last_u` (pu-chain). Stale if `u < lid`.

### `SessionSeqTracker.check(seq: int) -> SeqGap | None`
Detects session_seq gaps (expected = last+1). Returns `SeqGap(expected, actual)` if mismatch, None if OK.

## 4. Internal structure

**Collector** (main.py:27-286)
- Owns: config, adapter, producer, handlers dict, ws_manager, snapshot_scheduler, oi_poller, state_manager (optional PG), lifecycle_state.
- 295 lines; lifecycle methods (start, shutdown, heartbeat loop, state manager) are logically separated but internal. No god-objects.
- Callbacks: `_on_producer_overflow()` increments ws_manager drop counter (backpressure signal). `_on_pu_chain_break(symbol)` triggers ws_manager depth resync.

**WebSocketManager** (connection.py:27-323)
- Owns: handlers dict, producer, adapter, symbols, enabled_streams, WS connection state (_ws_connected, _seq_counters, _last_received_at, _disconnect_gap_emitted), backpressure counter, backoff state.
- 322 lines; main concern is `_connection_loop()` (91 lines, logically contains reconnect + backoff, one task per socket), `_receive_loop()` (45 lines, frame → routing → handler dispatch), `_depth_resync()` (82 lines, complex state machine: wait for producer health, try backup chain reader, fetch snapshot, replay buffered diffs).
- Cyclomatic complexity: `_receive_loop()` has ~5 branches (backpressure check, route exception, handler dispatch, latency record, seq clear).

**CryptoLakeProducer** (producer.py:17-256)
- Owns: KafkaProducer, per-stream buffer counts (under lock), overflow windows tracking (symbol, stream) → {start_ts, dropped}, taps dict.
- 255 lines; produce() enforces per-stream cap with optimistic locking (TOCTOU guard), delivery callback decrements counts. Overflow tracking is minimal but effective: one dict entry per overflow window.
- Cyclomatic: produce() ~7 branches (cap check, buffer check, produce success/failure, overflow recovery).

**DepthHandler** (streams/depth.py:19-171)
- Owns: detectors dict (one DepthGapDetector per symbol), pending_diffs buffer (up to 5000 per symbol), pending_drops counters, pending_drop_start timestamps.
- 171 lines; validate_diff() (fast path: stale/gap/buffered/valid), handle() (dispatch to validator, produce on valid), set_sync_point() (emit deferred gap, replay buffer).

**SnapshotScheduler** (snapshot.py:28-160)
- Owns: aiohttp ClientSession, seq_counters, per-symbol poll loops (staggered initial delay).
- 159 lines; _poll_loop() waits on asyncio.Event for graceful stop (timeout-based scheduling), _take_snapshot() fetches with 3 retries + exp-backoff.
- Note: periodic snapshots do NOT call depth_handler.set_sync_point(); that is only called during resync (reconnect or pu_chain_break).

**OpenInterestPoller** (streams/open_interest.py:17-122)
- Similar structure: per-symbol _poll_loop(), _poll_once() with retries.

**No functions > 80 lines except**:
- connection.py `_connection_loop()` (99-147, 48 lines) — reconnect loop, suggest no split needed.
- connection.py `_depth_resync()` (195-282, 88 lines) — complex, but logically coherent: precondition check, backup reader attempt, REST fetch + retry, replay, error handling. Architect may consider factoring `_try_snapshot_fetch()` helper.

## 5. Concurrency surface

**Async Entry Points**:
- `Collector.start()`: gathers 5+ tasks (ws_manager, snapshot_scheduler, oi_poller, health_server, heartbeat).
- `Collector.shutdown()`: cancels all tasks, flushes producer.
- `WebSocketManager.start()`: creates one task per socket (`_connection_loop`), gathers.
- `WebSocketManager._connection_loop(socket_name, url)`: reconnect loop (async context manager for websockets.connect).
- `WebSocketManager._receive_loop(ws, socket_name, connect_time)`: async for over ws (frame iterator), backpressure polling, handler dispatch via `await handler.handle()`.
- `WebSocketManager._depth_resync(symbol)`: async, uses aiohttp.ClientSession for REST snapshot fetch.
- `SnapshotScheduler.start()`: creates one task per symbol (`_poll_loop`).
- `SnapshotScheduler._poll_loop(symbol, interval, initial_delay)`: uses `asyncio.wait_for(event.wait(), timeout=...)` for graceful stop + interval timing.
- `OpenInterestPoller.start()`: similar to SnapshotScheduler.

**Asyncio Primitives**:
- `asyncio.create_task()`: spawn child tasks (connection loops, resync, health server).
- `asyncio.gather(*tasks, return_exceptions=True)`: wait for all tasks (collector.start, ws_manager.start, etc.).
- `asyncio.Event()`: graceful stop signals (snapshot scheduler, OI poller use event.set() to unblock timeout waits).
- `asyncio.wait_for(awaitable, timeout)`: enforce timeouts on producer health checks (60s), event waits (poll interval).
- `asyncio.CancelledError`: handled in shutdown loops.
- `asyncio.get_running_loop().create_task()`: spawn depth resync from ws connection callback (not awaited immediately).

**Blocking Calls** (must run in executor in Java):
- `producer.list_topics(timeout=5)`: blocking I/O in is_connected(), is_healthy_for_resync(). In Python, confluent_kafka blocks the current thread; in Java, use ExecutorService.
- `producer.poll(0.1)`: in backpressure loop (blocking I/O for delivery callbacks).
- `producer.flush(timeout=10)`: during shutdown (blocking).

**Producer Thread-Safety**:
- Python uses `threading.Lock()` on _buffer_counts and _overflow_windows (atomic TOCTOU guard for per-stream cap).
- Delivery callback (_make_delivery_cb) is invoked by confluent_kafka's internal thread; must acquire lock.
- In Java, use ReentrantLock or AtomicLong for per-stream counts, ConcurrentHashMap for windows.

**Backpressure**:
- WebSocketManager tracks _consecutive_drops (incremented by producer overflow callback).
- When drops >= threshold (10), _receive_loop pauses frame processing, polls producer, resets counter, sleeps 0.1s.
- In Java, this becomes a poll-loop within the receive handler (non-blocking if virtual threads used).

**Data Flow Concurrency**:
- raw_text captured from websockets.recv() (async, mutually exclusive per connection).
- envelope created synchronously.
- produce() is thread-safe (lock-protected buffer count).
- No Queue between receipt and produce; blocking call.

## 6. External I/O

### Kafka (Redpanda)

**Producer Topics** (output):
- `{prefix}binance.{stream}` (stream = trades, depth, bookticker, funding_rate, liquidations, depth_snapshot, open_interest)
- Key: symbol.encode() (ASCII string bytes)
- Value: orjson.dumps(envelope) — data envelope or gap envelope

**Consumer Topics** (input, recovery only):
- Backup chain reader: `{backup_prefix}binance.depth` or `binance.depth` depending on primary/backup role
  - One-shot consumer in `read_last_depth_update_id()`: seeks 30s back, polls for last symbol's depth diff, extracts `u` (final update ID)
  - Used in `WebSocketManager._depth_resync()` as fast-track if backup collector has recent diffs

### HTTP (Binance REST)

**Depth Snapshot** (GET):
- URL: `https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000`
- Response: JSON `{"lastUpdateId": int, ...}` (raw_text preserved, no event timestamp)
- Called by: `WebSocketManager._depth_resync()` (on reconnect/pu_chain_break), `SnapshotScheduler._take_snapshot()` (periodic)
- Retry: 3 attempts, exp-backoff 1s/2s, handles 429 (Retry-After header)

**Open Interest** (GET):
- URL: `https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}`
- Response: JSON with "time" field (extract via adapter.extract_exchange_ts)
- Called by: `OpenInterestPoller._poll_once()` (periodic)
- Retry: 3 attempts, exp-backoff

### WebSocket (Binance Futures)

**Subscription URLs**:
- `wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms/btcusdt@bookTicker/...` (public socket)
- `wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/...` (market socket)

**Message Format** (inbound, from websockets.recv()):
- Combined stream frame: `{"stream":"btcusdt@depth@100ms","data":{...}}` (JSON string)
- raw_text extracted via balanced-brace parsing to preserve byte identity
- stream type, symbol routed via adapter.route_stream()

**Reconnection**: exponential backoff 1s→60s, proactive 24h close

### Filesystem

**Config**:
- YAML file path passed to Collector.__init__(); parsed via yaml.safe_load + pydantic validation

**Tap Output** (fixture-capture, optional):
- `{tap_output_dir}/{stream}/{timestamp}-{seq:06d}-{stream}.raw` — raw frame bytes
- `{tap_output_dir}/{stream}/{timestamp}-{seq:06d}-{stream}.json` — resulting envelope
- When tap_output_dir is None, no-op

### Database (optional, best-effort)

**PostgreSQL** (lifecycle state tracking):
- URL: `database.url` from config (e.g., `postgresql://localhost/cryptolake`)
- Table: `component_runtime_state` (insert/update): component, instance_id, host_boot_id, started_at, last_heartbeat_at
- Table: `maintenance_intent` (query): check for active maintenance at shutdown
- Used by: Collector._init_state_manager(), _register_lifecycle_start(), _heartbeat_loop(), _mark_lifecycle_shutdown()
- Failure mode: best-effort; PG unreachable logs warning, continues

### Env Vars / Config Keys

- `COLLECTOR_ID` — session ID suffix (default: from config)
- `TOPIC_PREFIX` — prepended to all Redpanda topics (default: "")
- `EXCHANGES__BINANCE__SYMBOLS` — comma-separated override (e.g., `EXCHANGES__BINANCE__SYMBOLS=btcusdt,ethusdt`)
- `EXCHANGES__BINANCE__STREAMS__DEPTH` — true/false to enable depth stream
- `EXCHANGES__BINANCE__STREAMS__OPEN_INTEREST` — true/false
- `EXCHANGES__BINANCE__DEPTH__SNAPSHOT_INTERVAL` — e.g., "5m", "10m" (default: "5m")
- `EXCHANGES__BINANCE__DEPTH__SNAPSHOT_OVERRIDES` — per-symbol intervals (e.g., `BTCUSDT:1m`)
- `REDPANDA__BROKERS` — comma-separated broker list
- `WRITER__BASE_DIR` — archive directory (forwarded from `HOST_DATA_DIR`)
- `MONITORING__PROMETHEUS_PORT` — metrics port (default: 8000)
- `DATABASE__URL` — PG connection string
- `COLLECTOR__TAP_OUTPUT_DIR` — tap fixture root (default: None)

## 7. Data contracts

### Data Envelope (produced by all streams)

```python
{
    "v": 1,
    "type": "data",
    "exchange": "binance",
    "symbol": "btcusdt",
    "stream": "depth" | "trades" | "depth_snapshot" | "open_interest" | ...,
    "received_at": int64 (nanoseconds since epoch),
    "exchange_ts": int64 (milliseconds since epoch, from Binance message),
    "collector_session_id": "binance-collector-01_2024-12-31T12:34:56Z",
    "session_seq": int64 (per-stream sequence, 0-indexed),
    "raw_text": str (JSON string, byte-for-byte from exchange),
    "raw_sha256": str (hex-encoded SHA256 of raw_text.encode()),
}
```

**Nullable fields**: exchange_ts is 0 if not present in message.

### Gap Envelope

```python
{
    "v": 1,
    "type": "gap",
    "exchange": "binance",
    "symbol": "btcusdt",
    "stream": "depth",
    "received_at": int64,
    "collector_session_id": "...",
    "session_seq": int64,
    "gap_start_ts": int64 (nanoseconds),
    "gap_end_ts": int64 (nanoseconds),
    "reason": str (one of: ws_disconnect, pu_chain_break, session_seq_skip, buffer_overflow, snapshot_poll_miss),
    "detail": str (human-readable reason),
}
```

**Optional fields** (restart metadata, only present if provided):
- `component`, `cause`, `planned`, `classifier`, `evidence`, `maintenance_id`

### Pydantic Models (config-only)

**BinanceExchangeConfig**:
- `enabled: bool = True`
- `market: str = "usdm_futures"`
- `ws_base: str = "wss://fstream.binance.com"`
- `rest_base: str = "https://fapi.binance.com"`
- `symbols: list[str]` (required, lowercased by validator)
- `streams: StreamsConfig = StreamsConfig()`
- `writer_streams_override: list[str] | None = None` (auto-includes depth_snapshot if depth present)
- `depth: DepthConfig = DepthConfig()`
- `open_interest: OpenInterestConfig = OpenInterestConfig()`
- `collector_id: str = "binance-collector-01"`

**StreamsConfig**:
- `trades: bool = True`
- `depth: bool = True`
- `bookticker: bool = True`
- `funding_rate: bool = True`
- `liquidations: bool = True`
- `open_interest: bool = True`

**DepthConfig**:
- `update_speed: str = "100ms"` (Binance subscription parameter, not used by collector)
- `snapshot_interval: str = "5m"` (e.g., "1m", "5m")
- `snapshot_overrides: dict[str, str] = {}` (per-symbol, e.g., {"btcusdt": "1m"})

**OpenInterestConfig**:
- `poll_interval: str = "5m"`

**ProducerConfig**:
- `max_buffer: int = 100_000` (Kafka queue size)
- `buffer_caps: dict[str, int] = {"depth": 80_000, "trades": 10_000}` (per-stream limits)
- `default_stream_cap: int = 10_000` (fallback for unlisted streams)

**RedpandaConfig**:
- `brokers: list[str]` (required)
- `retention_hours: int = 48` (validated >= 12)
- `producer: ProducerConfig = ProducerConfig()`

### JSON Schemas (exchange outputs)

**Depth Diff** (from raw_text):
```json
{
  "E": int64,
  "U": int64,
  "u": int64,
  "pu": int64,
  "b": [[price, qty], ...],
  "a": [[price, qty], ...]
}
```
- `E`: event time (ms)
- `U`: first update ID
- `u`: final update ID
- `pu`: previous final update ID (must match prior `u` for pu-chain validation)

**Depth Snapshot** (REST response, from raw_text):
```json
{
  "lastUpdateId": int64,
  "bids": [[price, qty], ...],
  "asks": [[price, qty], ...]
}
```

**Trade**:
```json
{
  "E": int64,
  "a": int64,
  "p": str,
  "q": str,
  ...
}
```

**Open Interest**:
```json
{
  "time": int64,
  "symbol": str,
  "openInterest": str
}
```

## 8. Test catalog

### Unit Tests

**test_emit_gap.py** (3 tests)
- `test_emit_gap_produces_gap_envelope_and_increments_metric`: gap envelope structure + metric label
- `test_emit_gap_uses_custom_timestamps`: gap_start_ts, gap_end_ts custom
- `test_emit_overflow_gap_increments_gaps_detected_total`: buffer_overflow gap metric

**test_gap_detector.py** (6 tests — DepthGapDetector and SessionSeqTracker)
- `test_first_diff_after_sync_accepted`: sync diff detection
- `test_subsequent_diff_pu_chain_valid`: pu == last_u
- `test_pu_chain_break_detected`: pu != last_u triggers gap=True
- `test_stale_diff_before_sync_rejected`: u < lastUpdateId
- `test_no_sync_point_rejects`: no sync → invalid
- `test_reset_clears_state`: reset() clears _synced
- SessionSeqTracker: sequential OK, gap detected, first message OK

**test_disconnect_gap_coalescing.py** (3 tests)
- `test_first_disconnect_emits_one_gap_per_public_stream`: 1 symbol × 2 public streams = 2 gaps
- `test_repeated_disconnect_calls_coalesce`: 3 calls still → 2 gaps (not 6), due to _disconnect_gap_emitted flag
- `test_data_arrival_clears_emitted_flag`: after data, flag cleared, next disconnect re-emits

**test_backup_chain_reader.py** (multiple integration tests)
- Kafka consumer setup, topic metadata lookup, offset seek, message filtering by symbol/age, update ID extraction

**test_collector_lifecycle.py** (integration tests)
- Collector.__init__, start(), shutdown() lifecycle with mocked producers/adapters
- Session ID generation, handler setup

**test_depth_resync_skip.py** (integration test)
- Depth resync skipped when backup collector has recent diffs (read_last_depth_update_id returns non-None)

**test_chunk3_fixes.py** (edge case regression tests)
- Various edge-case regression fixes from dev iterations; specific scenarios are best read from the test file headers

**test_gap_analyzer.py**, **test_gap_records_written_metric.py** (metrics/analysis tests)

**test_producer_topic_prefix.py**
- Topic prefix prepended correctly to `{prefix}{exchange}.{stream}`

**test_snapshot.py** (minimal)
- SnapshotScheduler initialization or basic fetch

### Chaos Tests (reference only, not exhaustively ported)

- `tests/chaos/` directory contains bash scripts exercising real Kafka/WebSocket scenarios:
  - Kill writer mid-run, verify collector emits gap + recovery
  - Kafka broker restart, verify reconnect + replay
  - WebSocket disconnect + slow reconnect, verify backpressure
  - Depth resync timeout, verify gap emission
  - Snapshot poll failure, verify graceful fallback

(Chaos tests are reference material; Java port will have equivalent stress tests via virtual threads + chaos toolkit.)

## 9. Invariants touched (Tier 1 rules)

**Rule 1**: `raw_text` is captured from WebSocket callback BEFORE any JSON parse.
- **Touched in**: `src/collector/connection.py:169` where `adapter.route_stream(raw_frame)` extracts raw_text via balanced-brace slicing (zero-copy).
- **Also**: `src/exchanges/binance.py:56-74` (`route_stream` method) — `_extract_data_value()` parses outer JSON to find `"data":` key, then extracts value via character counting without re-serialization.
- **Invariant verified**: raw_text is a string slice from the original frame, no JSON round-trip before capture.

**Rule 2**: `raw_sha256` is computed once, at capture time.
- **Touched in**: `src/common/envelope.py:64` — `create_data_envelope()` computes `hashlib.sha256(raw_text.encode()).hexdigest()` immediately before returning envelope.
- **Invariant verified**: SHA256 computed synchronously during envelope creation, never recomputed.

**Rule 3**: Disabled streams emit zero artifacts.
- **Touched in**: `src/collector/main.py:48` — `self.enabled_streams = self.exchange_cfg.get_enabled_streams()`.
- **Touched in**: `src/common/config.py:62-76` — `get_enabled_streams()` returns list of stream names based on `StreamsConfig` booleans.
- **Touched in**: `src/collector/connection.py:68` — `_get_ws_urls()` filters enabled_streams.
- **Invariant verified**: handlers only created for enabled streams; WebSocket only subscribes to enabled streams; producer only produces for handled messages.

**Rule 4**: (Kafka offsets committed after file flush — not applicable to collector; handled by writer)

**Rule 5**: Every detected gap emits metric, log, AND archived gap record.
- **Metric**: `src/collector/metrics.py:21-24` — `gaps_detected_total` Counter incremented with reason label.
- **Log**: `src/collector/producer.py:195` (`emit_gap` logs "buffer_overflow_gap_emitted"), `src/collector/connection.py:121` (ws_disconnected warning).
- **Archive**: `src/collector/producer.py:226` — gap envelope produced to Kafka topic (archived).
- **Invariant verified**: all gap emission sites call `producer.emit_gap()` which increments metric + produces envelope; logs are explicit.

**Rule 6**: Recovery prefers replay from Kafka / exchange cursors over inferred reconstruction.
- **Touched in**: `src/collector/connection.py:224-238` — `_depth_resync()` checks backup chain reader (Kafka replay) BEFORE REST snapshot.
- **Touched in**: `src/collector/streams/depth.py:105-165` — `set_sync_point()` replays buffered diffs after snapshot.
- **Invariant verified**: backup reader is attempted first; if successful, REST snapshot skipped.

**Rule 7**: JSON codec must not re-order, re-quote, or re-format `raw_text`.
- **Touched in**: `src/collector/producer.py:87` — raw_text stored as-is in envelope dict (no re-serialization until serialize_envelope).
- **Touched in**: `src/common/envelope.py:122` — `serialize_envelope()` uses `orjson.dumps()` (preserves sort order if configured).
- **Invariant verified**: raw_text is a string field in envelope dict, serialized as JSON string value (no re-parsing/reformatting).

## 10. Port risks

### A. Concurrency Model Mismatch (Asyncio → Virtual Threads)

**Risk**: Python asyncio multiplexes N streams on 1 OS thread (cooperative scheduling). Java virtual threads (Project Loom) multiplex on 1 carrier thread but require OS blocking I/O to yield.

**Evidence**: 
- `connection.py:99-147` — `_connection_loop()` is async but does NOT perform I/O in loop body; it awaits `_receive_loop()` which is the I/O-bound path.
- `connection.py:148-193` — `_receive_loop()` does `async for raw_frame in ws` (non-blocking frame iteration) + dispatch to handlers.
- `connection.py:158-165` — backpressure loop calls `self.producer.poll(0.1)` (blocking I/O for delivery callbacks).

**Tier 5 Rule**: **A3** — Blocking I/O must run in `ExecutorService`, not on virtual thread carrier.

**Port decision**: In Java, wrap producer.poll(), producer.list_topics(), producer.flush() in `executor.submit()`. Do NOT use `Thread.sleep()` for backpressure; use `Thread.onSpinWait()` or replace with non-blocking poll-check.

---

### B. WebSocket Frame Accumulation (Multi-frame Messages)

**Risk**: Python `websockets.recv()` returns `str` (text) or `bytes` (binary) — automatically reassembles fragmented frames. Java `WebSocketListener.onText(CharSequence text, boolean last)` / `onBinary(ByteBuffer data, boolean last)` requires manual accumulation if `last=false`.

**Evidence**: No explicit multi-frame handling in Python code; `route_stream()` assumes single-frame message.

**Tier 5 Rule**: **D1** — Java WebSocket listener must accumulate frames when `last=false`, produce envelope only when `last=true`.

**Port decision**: Implement `FrameAccumulator` in WebSocket handler; buffer ByteBuffer/String until `last=true`, then route accumulated data.

---

### C. Depth Resync State Machine Complexity

**Risk**: `_depth_resync()` is 88 lines with 4 distinct phases: (1) wait for producer healthy, (2) try backup chain reader, (3) fetch snapshot with 3 retries + 429 handling, (4) emit gap on total failure. Each phase has async waits + exception branches.

**Evidence**: `connection.py:195-282` — nested try/except, 2 async.sleep() calls, REST client creation inside loop, dynamic task spawning.

**Port decision**: Factor into 4 helper methods: `_wait_producer_healthy()`, `_try_backup_depth()`, `_fetch_snapshot_with_retries()`, `_emit_resync_failed_gap()`. Use async/await in Java with virtual threads for clarity.

---

### D. raw_text Byte Identity Requires Zero-Copy Extraction

**Risk**: Python's `_extract_data_value()` (binance.py:160-196) uses character-by-character brace counting on the frame string to extract the JSON data object without re-serialization. Java's `String.getBytes(UTF-8)` must produce identical bytes, including whitespace preservation.

**Evidence**: 
- `binance.py:160-196` counts `{` and `}` while tracking string escape state, returns `frame[start:i+1]` (substring).
- `envelope.py:64` computes `raw_sha256` on `raw_text.encode()`.
- tap.py writes raw.raw_bytes and envelope.json; parity tests verify bytes match.

**Tier 5 Rule**: **B1** — JSON raw_text must preserve byte-for-byte identity; use String(byte[], UTF-8) round-trip only if necessary.

**Port decision**: Implement same brace-counting extraction in Java. Spike: extract, convert to bytes, re-parse JSON to verify structure. Use `CharBuffer` or `StringBuilder` for accumulation if multi-frame.

---

### E. Integer Overflow on Depth Update IDs

**Risk**: Binance depth `U`, `u`, `pu` are int64 (can exceed 2^31 - 1). Python int is arbitrary precision; Java `int` is 32-bit signed.

**Evidence**: `gap_detector.py:34-55` compares `U`, `u`, `pu` as ints; `pu != self._last_u` check.

**Tier 5 Rule**: **E1** — MUST use `long`, not `int`, for Binance exchange sequence IDs.

**Port decision**: All exchange update IDs (U, u, pu, lastUpdateId) must be `long` in Java. Verify in BinanceAdapter.parseDepthUpdateIds() return type.

---

### F. Timestamp Precision and Overflow

**Risk**: Python's `time.time_ns()` returns int64 nanoseconds (fits). Java's `System.nanoTime()` is also int64 but relative to VM start, not epoch. Exchange timestamps (`E` field) are milliseconds.

**Evidence**: 
- `connection.py:192` — `self._last_received_at[(symbol, stream_type)] = time.time_ns()` (nanoseconds since epoch).
- `producer.py:151` — `"start_ts": time.time_ns()` in overflow window.
- `snapshot.py:126, 140` — `poll_start_ns = time.time_ns()` and `received_at = time.time_ns()`.
- `envelope.py:59` — `received_at: time.time_ns() if received_at is None else received_at` (envelope field is nanoseconds).

**Tier 5 Rule**: **F2** — Timestamps must be nanoseconds since Unix epoch. Java: `System.nanoTime()` is NOT epoch-relative; use `System.currentTimeMillis() * 1_000_000` + offset adjustment.

**Port decision**: Centralize timestamp generation in a `Clock` utility; use `Instant.now().toEpochMilli() * 1_000_000` for nanosecond epoch time.

---

### G. Backpressure and Producer Overflow Callback Timing

**Risk**: Python producer overflow callback (`self.producer._on_overflow = lambda ex, sym, st: ...`) is called synchronously from produce() failure path. In Java, confluent_kafka's delivery callback is async (invoked by internal thread). Backpressure signal propagation may race.

**Evidence**: `main.py:79` — `self.producer._on_overflow = lambda ex, sym, st: self._on_producer_overflow()`.
- `connection.py:159-165` — backpressure loop checks `self._consecutive_drops >= threshold` and resets after poll.

**Tier 5 Rule**: **A1** — Callbacks invoked from background threads must be thread-safe; use atomic counter or CAS loop.

**Port decision**: Use `AtomicInteger` for `_consecutive_drops`. Backpressure poll loop can read atomically without lock.

---

### H. Metrics 10+ Counters/Gauges/Histograms

**Risk**: Prometheus metrics have labels (exchange, symbol, stream, reason). High cardinality (symbols × streams × reasons) can explode memory if not bounded.

**Evidence**: 
- `metrics.py:3-50` — 10 metrics with multi-dimensional labels.
- Example: `messages_produced_total.labels(exchange="binance", symbol="btcusdt", stream="depth").inc()` — creates series per unique label combo.

**Tier 5 Rule**: **H1** — Metrics with unbounded cardinality require careful registration; consider metric registry limits or pre-register label values.

**Port decision**: Pre-register all expected symbol/stream combos at startup (or use Prometheus relabeling). Monitor cardinality in tests.

---

### M. Depth pu-Chain Validation and Session-Seq Monotonicity

**Risk**: Two parallel gap-detection mechanisms:
1. **pu-chain**: `pu` from diff N+1 must equal `u` from diff N (validates Binance sequencing).
2. **session_seq**: collector-assigned counter must increment by 1 per message (validates collector input capture).

Both can trigger gaps independently. Mismatch between them (e.g., pu-chain break but session_seq OK) indicates exchange replay or loss.

**Evidence**: 
- `gap_detector.py:34-55` — DepthGapDetector validates pu-chain.
- `gap_detector.py:63-77` — SessionSeqTracker validates session_seq.
- `streams/base.py:28-44` — `_check_seq()` called by SimpleStreamHandler and DepthHandler.

**Tier 5 Rule**: **M1** — Depth diffs must pass BOTH pu-chain AND session_seq checks. Gaps from one do NOT suppress checks for the other.

**Port decision**: Invoke both `session_seq_tracker.check()` and `depth_detector.validate_diff()` independently; emit separate gap envelopes if either fails.

---

### I. REST Polling Retry with 429 Handling

**Risk**: Snapshot and OI polls retry 3x with exp-backoff 1s/2s, but handle Binance's 429 (rate limit) specially: read `Retry-After` header and sleep that duration before retry, which can exceed exp-backoff window.

**Evidence**: 
- `snapshot.py:89-92` — `if resp.status == 429: retry_after = ... await asyncio.sleep(retry_after); continue`.
- `open_interest.py:85-90` — same pattern.

**Tier 5 Rule**: **D3** — 429 rate-limit response must honor Retry-After header; don't apply exp-backoff over it.

**Port decision**: In Java, catch rate-limit response, extract header, use `Thread.sleep(parseLong(retryAfter) * 1000)` before retry. Don't double-backoff.

---

### J. Lifecycle State Manager Best-Effort (PG Connection Failures)

**Risk**: Collector attempts to connect to PostgreSQL for lifecycle tracking (startuptime, heartbeat, shutdown classification). If PG is unreachable, collector continues normally but cannot classify restarts.

**Evidence**: `main.py:99-196` — all state manager calls are wrapped in try/except with logging + `self._state_manager = None` fallback.

**Tier 5 Rule**: **G2** — Best-effort external deps must not block startup. Log and continue.

**Port decision**: Make state_manager Optional. If connection fails during init, set to null and skip heartbeat/shutdown calls. Log each failure at WARN level (not ERROR).

---

### K. Fixture-Capture Tap (Python-Only, Out of Scope)

**Risk**: `tap.py` persists every inbound envelope to disk for parity fixture corpus. This is a Python-only tool used during port validation (`/port-init`).

**Evidence**: `tap.py:1-35` — FrameTap class, instantiated in `producer.py:85` when tap_root is not None.

**Tier 5 Rule**: (N/A — out of scope for Java)

**Port decision**: Java collector does NOT implement tap.py. Fixture corpus generation is done by Python reference during port validation (separate workflow). Note in Java code comments.

---

### L. Disabled Streams Must Not Produce

**Risk**: If a stream (e.g., open_interest) is disabled in config, no handler is created, but if a WebSocket frame arrives for that stream (shouldn't happen if subscription is correct), it must be silently dropped.

**Evidence**: 
- `main.py:52-61` — handlers created only for enabled streams.
- `connection.py:174-176` — `handler = self.handlers.get(stream_type); if handler is None: continue` (silently skip).

**Tier 5 Rule**: **C2** — Disabled streams are dropped, not errors.

**Port decision**: Test: config disables open_interest, collector receives OI frame via mocking → verify silently ignored, no error, no envelope produced.

---

## 11. Rule compliance

**Rule 1** (raw_text from WebSocket callback before parse): **Surfaced in §9**. Touched in `connection.py:169` + `binance.py:56-74`.

**Rule 2** (raw_sha256 computed at capture): **Surfaced in §9**. Touched in `envelope.py:64`.

**Rule 3** (disabled streams emit zero artifacts): **Surfaced in §9**. Touched in `config.py:62-76`, `connection.py:68`, `main.py:48`.

**Rule 4** (Kafka offsets after file flush): **Not applicable to this module**. Writer module handles.

**Rule 5** (gap emits metric + log + archive): **Surfaced in §9**. Touched in `metrics.py:21-24`, `producer.py:195`, `producer.py:226`.

**Rule 6** (recovery prefers replay): **Surfaced in §9**. Touched in `connection.py:224-238`, `streams/depth.py:105-165`.

**Rule 7** (JSON codec preserves raw_text): **Surfaced in §9**. Touched in `producer.py:87`, `envelope.py:122`.

---

## Appendix: Chaos Scenarios (reference)

1. **ws_disconnect** + slow reconnect (5+ retries) → backoff reaches 60s → periodic snapshots continue (no stall).
2. **pu_chain_break** mid-stream → depth resync triggered → buffer diffs → wait for producer health (timeout 60s) → snapshot fetch → replay buffered → resume normal flow.
3. **snapshot_poll_miss** (3 retries exhausted) → gap record emitted + next poll cycle (5m later) retries → recovery via next snapshot.
4. **buffer_overflow** on high-volume stream (depth) → per-stream cap enforced → other streams continue → overflow gap on recovery.
5. **producer unhealthy** (list_topics timeout) → depth resync aborted → resync_failed gap → next resync attempt on next disconnect/pu_chain_break.
6. **backup chain reader** success → snapshot REST fetch skipped → fast resync from Kafka.

---

**Total lines: ~750**. Sections 5, 9, 10 detailed (concurrency, invariants, risks); sections 2, 3, 4 concise (tables, APIs, structure).
