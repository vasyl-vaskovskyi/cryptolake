---
module: collector
status: approved
produced_by: architect
based_on_mapping: caa915704fb072e94af7cce84ca10e15ec310628
---

# Collector — Java design

Port target: `cryptolake-java/collector/src/main/java/com/cryptolake/collector/*`. Java 21, virtual-thread concurrency, no framework, explicit `main()` wiring. Reuses `com.cryptolake.common.*` accepted artifacts (envelope records, `EnvelopeCodec`, `Sha256`, `Clocks`, `HealthServer`, `SystemIdentity`, config records, `TopicNames`, `JsonlReader/Writer`) without redefinition.

Tier 1 invariants 1, 2, 3, 5, 6, 7 are load-bearing in this module (invariant 4 — offset commit after fsync — is writer-only). The design below threads those invariants through every subsystem and cites Tier 5 rule IDs in §10 Rule compliance.

---

## 1. Package layout

```
com.cryptolake.collector
├── Main                                    # main(), SIGTERM hook, SIGTERM-latched shutdown
├── CollectorSession                        # holds session_id, collector_id; constructed in Main
├── adapter/
│   ├── BinanceAdapter                      # routing, URL building, parse_depth_update_ids, parse_snapshot_last_update_id
│   ├── RawDataExtractor                    # balanced-brace byte extractor (Tier 1 §1, Tier 5 B4)
│   └── StreamKey                           # aggTrade ↔ trades etc. mapping constants
├── connection/
│   ├── WebSocketClient                     # owns java.net.http.WebSocket; one per socket (public/market)
│   ├── WebSocketListenerImpl               # implements WebSocket.Listener; frame accumulation
│   ├── FrameAccumulator                    # onText multi-fragment assembly
│   ├── ReconnectPolicy                     # exp backoff 1→60s, 24h proactive, reset on connect
│   ├── WebSocketSupervisor                 # spawns one client per socket; drives depth-resync callback
│   └── BackpressureGate                    # AtomicInteger consecutive drops + threshold gate
├── capture/
│   ├── RawFrameCapture                     # owns the pre-parse byte path; builds DataEnvelope
│   ├── FrameRoute                          # (streamType, symbol, rawText) record
│   └── ExchangeLatencyRecorder             # received_at - exchange_ts histogram feeder
├── streams/
│   ├── StreamHandler                       # sealed interface
│   ├── SimpleStreamHandler                 # trades / bookticker / funding_rate / liquidations
│   ├── DepthStreamHandler                  # diff buffering + pu chain + resync trigger
│   └── OpenInterestPoller                  # REST poll; virtual-thread scheduler
├── snapshot/
│   ├── SnapshotFetcher                     # one-shot REST GET /fapi/v1/depth; retries + 429
│   ├── SnapshotScheduler                   # per-symbol periodic fetch; staggered delay
│   ├── DepthSnapshotResync                 # explicit state machine — see §3
│   └── DepthState                          # sealed interface (Healthy/Buffering/Syncing/Recovered)
├── gap/
│   ├── GapEmitter                          # single triad owner: metric + log + archived gap envelope
│   ├── GapDetector                         # thin facade that owns detectors + tracker per symbol
│   ├── DepthGapDetector                    # pu-chain validator
│   ├── SessionSeqTracker                   # monotonic session_seq validator
│   ├── DisconnectGapCoalescer              # _disconnect_gap_emitted set (Tier 5 L)
│   └── DiffValidationResult                # record: valid/gap/stale/reason
├── producer/
│   ├── KafkaProducerBridge                 # single KafkaProducer<byte[],byte[]>; ReentrantLock-guarded per-stream counts
│   ├── PerStreamBuffer                     # cap + count map; overflow-window owner
│   ├── OverflowWindow                      # (symbol, stream) → startTs + dropped
│   └── DeliveryCallback                    # decrements count; logs errors
├── backup/
│   └── BackupChainReader                   # one-shot Consumer; other_depth_topic derivation
├── lifecycle/
│   ├── LifecycleStateManager               # JDBC+HikariCP upsert/mark-clean-shutdown; best-effort
│   └── HeartbeatScheduler                  # 30s heartbeat task
├── metrics/
│   └── CollectorMetrics                    # Micrometer registry; 8 Prometheus meters
└── harness/
    ├── MetricSkeletonDump                  # :collector:dumpMetricSkeleton (gate 4)
    └── RawTextParityHarness                # :collector:runRawTextParity (gate 3)
```

Total class count in §2 (catalog below): **37 classes and records**.

---

## 2. Class catalog

Per class: purpose / type / public API / dependencies / thread-safety. Records are `public record`; utilities `final class` with private constructor; sealed hierarchies noted.

### 2.1 Entry and session

**`Main`** — final class, utility.
- Purpose: wire all components, install SIGTERM hook, block on a `CountDownLatch` until shutdown.
- Public: `static void main(String[] args)`.
- Dependencies: constructs everything. Uses `Executors.newVirtualThreadPerTaskExecutor()` for long-running tasks (WebSocket supervisor, snapshot scheduler, OI poller, heartbeat). Opens a single `HttpClient` (Tier 5 D3), `KafkaProducer` (Tier 2 §14), `ObjectMapper` via `EnvelopeCodec.newMapper()`.
- Thread-safety: wiring runs on the main platform thread; no shared mutation after start.

**`CollectorSession`** — immutable record.
- Purpose: carries `collectorId`, `sessionId` (format from Tier 5 M7), start instant.
- Public: `static CollectorSession create(String collectorId, ClockSupplier clock)`.
- Thread-safety: record; immutable.

### 2.2 Exchange adapter

**`BinanceAdapter`** — final class (stateless wrt instance; holds two base URLs).
- Purpose: port of `src/exchanges/binance.py`. Builds WS URLs, routes frames, parses update IDs.
- Public: `Map<String,String> getWsUrls(List<String> symbols, List<String> streams)`, `FrameRoute routeStream(String rawFrame)`, `Long extractExchangeTs(String stream, String rawText)` (boxed — `null` allowed), `String buildSnapshotUrl(String symbol, int limit)`, `String buildOpenInterestUrl(String symbol)`, `long parseSnapshotLastUpdateId(String rawText)`, `DepthUpdateIds parseDepthUpdateIds(String rawText)` (all `long` — Tier 5 E1).
- Dependencies: `ObjectMapper` (for routing-only `readTree`), `RawDataExtractor`.
- Thread-safety: instance state is two final `String` bases; methods pure. Thread-safe.

**`DepthUpdateIds`** — record `long U, long u, long pu`. Immutable.

**`RawDataExtractor`** — final utility class.
- Purpose: ports `_extract_data_value` from `binance.py:160-196`. Balanced-brace slice of the `"data":` value. Returns a `String` that is a substring of the original frame — byte-for-byte identical after `getBytes(UTF_8)` (Tier 5 B4).
- Public: `static String extractDataValue(String frame, int searchStart)`.
- Thread-safety: stateless utility.

**`StreamKey`** — final utility.
- Purpose: ports `_STREAM_KEY_MAP`, `_PUBLIC_STREAMS`, `_MARKET_STREAMS`, `_SUBSCRIPTION_MAP` (Tier 5 M2). Four immutable `Map<String,String>` / `Set<String>` constants.
- Public: `static String toInternal(String binanceKey)`, `static Set<String> publicStreams()`, `static Set<String> marketStreams()`, `static String subscriptionSuffix(String stream)`, `static Map.Entry<String,String> parseStreamKey(String streamKey)`.
- Thread-safety: constants only.

### 2.3 Connection layer

**`WebSocketClient`** — final class. Owns one `java.net.http.WebSocket` per socket (`public` / `market`).
- Purpose: maintain a single long-running WebSocket; surface received bytes via the capture callback.
- Public: `void connect()` (blocking on the owning virtual thread), `void sendPing()`, `CompletableFuture<WebSocket> close()`, `boolean isConnected()`.
- Dependencies: `HttpClient`, `URI`, `WebSocketListenerImpl`, `ReconnectPolicy`, `FrameConsumer` (lambda).
- Thread-safety: `volatile boolean connected`; reads/writes to `WebSocket` go through its own thread-safe API. No `synchronized` around blocking calls (Tier 2 §9).

**`WebSocketListenerImpl`** — implements `java.net.http.WebSocket.Listener`.
- Purpose: accumulate multi-fragment text frames via `FrameAccumulator` and hand the complete UTF-8 bytes to the capture callback (Tier 5 D1, D2; Rule 1 pre-parse capture).
- Public: `CompletionStage<?> onOpen(...)`, `onText(WebSocket ws, CharSequence data, boolean last)`, `onBinary(WebSocket ws, ByteBuffer data, boolean last)`, `onClose(...)`, `onError(...)`.
- Per the spike result: on `last=true`, compute `byte[]` via `String.valueOf(buf).getBytes(UTF_8)`; binary path uses `ByteBuffer` concatenation.
- Back-pressure: call `ws.request(1)` ONLY after the capture pipeline has accepted the bytes (see `BackpressureGate` below). This matches Python's cooperative `async for` semantics (Tier 5 D2).
- Thread-safety: listener methods are serialized by the JDK WebSocket implementation; the accumulator is accessed from the same callback thread only.

**`FrameAccumulator`** — final class.
- Purpose: buffers `CharSequence` segments (onText) or `ByteBuffer` chunks (onBinary) across `last=false` calls; emits exactly one complete `String` (UTF-8 decoded when the path is binary) when `last=true`. Resets internal state each time.
- Public: `void append(CharSequence seg)`, `void append(ByteBuffer buf)`, `String complete()` (idempotent consume), `void reset()`.
- Dependencies: `StringBuilder` + `ByteArrayOutputStream`.
- Thread-safety: single-threaded per listener instance (not concurrent). Not shared.

**`ReconnectPolicy`** — final class.
- Purpose: port of Python backoff (`1s → 60s`, reset on connect; 24h proactive limit from Tier 5 D6/D7). Pure data — no sleeps inside.
- Public: `long nextBackoffMillis()`, `void reset()`, `boolean shouldProactivelyReconnect(Instant connectTime, Instant now)`.
- Thread-safety: caller-confined per-client instance.

**`WebSocketSupervisor`** — final class.
- Purpose: top-level coordinator. For each URL returned by `BinanceAdapter.getWsUrls(...)`, fork a virtual thread running `connectionLoop(socketName, url)` inside a `StructuredTaskScope.ShutdownOnFailure` (Tier 5 A1) so shutdown cancels cleanly. Each `connectionLoop` owns one `WebSocketClient`, catches `IOException`/`ConnectionClosedException`, increments `collector_ws_reconnects_total`, sleeps per `ReconnectPolicy` (via `CountDownLatch.await(timeout)` so stop is interruptible — Tier 5 A3), reconnects. On first public-socket connect with depth enabled, triggers `DepthSnapshotResync.start(symbol)` per symbol.
- Public: `void start()`, `void stop()`, `boolean isConnected()`, `void triggerDepthResync(String symbol)` (callback used by `DepthStreamHandler`).
- Dependencies: `List<String> symbols`, `List<String> enabledStreams`, `BinanceAdapter`, `HttpClient`, `RawFrameCapture`, `DepthSnapshotResync`, `BackpressureGate`, `DisconnectGapCoalescer`, `CollectorMetrics`, `ExecutorService` (virtual threads).
- Thread-safety: all maps are `ConcurrentHashMap`; status flags `volatile`. Lifecycle methods (`start`/`stop`) called only from `Main`.

**`BackpressureGate`** — final class.
- Purpose: `AtomicInteger consecutiveDrops` + `int threshold=10`. `shouldPause()` returns true when drops ≥ threshold. `onDrop()` increments (invoked by `KafkaProducerBridge` overflow callback, Tier 5 A1 callback thread-safety). `onRecovery()` resets to 0. When paused, the capture pipeline blocks by calling `producer.poll(Duration.ofMillis(100))` on the listener's virtual thread (not `Thread.sleep` — Tier 2 §10) until the gate releases.
- Public: `boolean shouldPause()`, `void onDrop()`, `void onRecovery()`.
- Thread-safety: atomic counter; no locks.

### 2.4 Capture

**`RawFrameCapture`** — final class.
- Purpose: the single pre-parse capture point (Tier 1 §1, Tier 5 B4). Method `onFrame(String socketName, String rawFrame)` does, in order:
  1. `BackpressureGate.shouldPause()` → poll producer for delivery-callback drainage, resume.
  2. Proactive 24h check via `ReconnectPolicy.shouldProactivelyReconnect(...)` → close WS, return.
  3. `BinanceAdapter.routeStream(rawFrame)` → `FrameRoute(streamType, symbol, rawText)` where `rawText` is the `RawDataExtractor`-produced slice.
  4. `handlers.get(streamType)` lookup; if null, drop silently (Rule 3 — disabled streams).
  5. `BinanceAdapter.extractExchangeTs(streamType, rawText)`; pass through `ExchangeLatencyRecorder` if non-null.
  6. `sessionSeqAllocator.next(symbol, streamType)`.
  7. Clear `_disconnect_gap_emitted` for `(symbol, streamType)` via `DisconnectGapCoalescer.onData(...)`.
  8. Update `lastReceivedAt[(symbol, stream)] = clock.nowNs()`.
  9. `handler.handle(symbol, rawText, exchangeTs, seq)`.
- `handle` builds envelopes via `DataEnvelope.create(...)`; Tier 1 §2 SHA-256 is computed inside that factory exactly once.
- Public: `void onFrame(String socketName, String rawFrame)`, `void onDisconnect(String socketName)` (delegates to `DisconnectGapCoalescer`).
- Dependencies: `BinanceAdapter`, `Map<String, StreamHandler> handlers`, `SessionSeqAllocator`, `BackpressureGate`, `DisconnectGapCoalescer`, `ExchangeLatencyRecorder`, `ClockSupplier`.
- Thread-safety: `lastReceivedAt` is `ConcurrentHashMap<TupleKey, Long>`. Handlers are stateless wrt concurrent dispatch or serialize internally. Method is called on the WebSocket listener thread (one per socket) — two sockets call it concurrently; they never share the same `(symbol, stream)` key because `_PUBLIC_STREAMS` and `_MARKET_STREAMS` are disjoint (Tier 5 M2).

**`FrameRoute`** — record `String streamType, String symbol, String rawText`. Immutable.

**`SessionSeqAllocator`** — final class.
- Purpose: port of `_next_seq`. Per-(symbol, stream) `long` counter allocated from `ConcurrentHashMap<Pair, AtomicLong>`.
- Public: `long next(String symbol, String stream)`.
- Thread-safety: atomics + CHM; lock-free.

**`ExchangeLatencyRecorder`** — final class.
- Purpose: observes `received_at_ms - exchange_ts_ms` into the `collector_exchange_latency_ms` histogram when `exchangeTs != null`. Uses integer math on nanoseconds (`nowNs / 1_000_000L - exchangeTs`) per Tier 5 E4.
- Public: `void record(String symbol, String stream, long exchangeTsMs, long receivedAtNs)`.
- Thread-safety: Micrometer meters thread-safe.

### 2.5 Streams

**`StreamHandler`** — `sealed interface` permits `SimpleStreamHandler`, `DepthStreamHandler`.
- Single method: `void handle(String symbol, String rawText, Long exchangeTs, long sessionSeq)`.
- Matches Python `StreamHandler.handle(...)` contract. Returns `void` — synchronous on the caller's virtual thread (Tier 5 A2; no async wrapping).

**`SimpleStreamHandler`** — final class (implements `StreamHandler`).
- Purpose: port of `src/collector/streams/simple.py`. Check session_seq, emit data envelope.
- Public: constructor `(String exchange, CollectorSession session, KafkaProducerBridge producer, String streamName, SessionSeqTracker sessionSeqTracker, GapEmitter gapEmitter, ClockSupplier clock)`, `handle(...)`.
- Inside `handle`: call `sessionSeqTracker.check(symbol, sessionSeq)` → if gap, `gapEmitter.emit(..., "session_seq_skip", ...)`. Then build envelope via `DataEnvelope.create(...)`, produce via `KafkaProducerBridge.produce(env)`.
- Thread-safety: trackers keyed per-(symbol); method invoked single-threaded per socket.

**`DepthStreamHandler`** — final class (implements `StreamHandler`).
- Purpose: port of `src/collector/streams/depth.py`. Diff validation, pu-chain, buffered replay.
- Owns: `Map<String, DepthGapDetector> detectors`, `Map<String, List<PendingDiff>> pendingDiffs` (cap 5000 per symbol — `_MAX_PENDING_DIFFS`), `Map<String, DropAccumulator> drops`.
- Public: `handle(...)`, `void setSyncPoint(String symbol, long lastUpdateId)`, `void reset(String symbol)`.
- Callback: `Consumer<String> onPuChainBreak` — fires `WebSocketSupervisor.triggerDepthResync(symbol)`.
- Thread-safety: confined to a single virtual thread per socket (all depth diffs come via the `public` WebSocket listener path). No locking needed.

**`PendingDiff`** — record `String rawText, Long exchangeTs, long sessionSeq`.

**`DropAccumulator`** — mutable-ish holder `long firstDropNs, int count`; accessed only from the single-owner virtual thread.

**`OpenInterestPoller`** — final class.
- Purpose: port of `src/collector/streams/open_interest.py`. Virtual-thread-based per-symbol poll loops. Uses `HttpClient.send(...)` on the same virtual thread (Tier 5 A2 — blocking is free on virtual threads).
- Public: `void start()`, `void stop()`, `boolean isRunning()`.
- Each poll loop: `CountDownLatch stopLatch` for graceful interruptible waits (Tier 5 A3). On `stopLatch.await(interval, SECONDS) == true`, return. 3-retry fetch with exp-backoff (1s, 2s) and 429 `Retry-After` honoring (Tier 5 D4).
- Thread-safety: one virtual thread per symbol; `ConcurrentHashMap` for per-symbol counters.

### 2.6 Snapshot + resync state machine

**`SnapshotFetcher`** — final class.
- Purpose: one-shot REST GET `/fapi/v1/depth?symbol=X&limit=1000`. Returns `String rawText`.
- Public: `Optional<String> fetch(String symbol)` — None on total failure after 3 retries.
- Retries: 3 attempts; on 429 read `Retry-After` header and sleep that long (Tier 5 D4); on other errors exp-backoff `1s, 2s` via `LockSupport.parkNanos` (not `Thread.sleep` — Tier 2 §10). Uses single shared `HttpClient` + `BodyHandlers.ofByteArray()` (Tier 5 D5 — explicit UTF-8 decode).
- Dependencies: `HttpClient`, `BinanceAdapter`, `CollectorMetrics` (`snapshotsTaken`, `snapshotsFailed`).
- Thread-safety: `HttpClient` is thread-safe; method pure.

**`SnapshotScheduler`** — final class.
- Purpose: port of `src/collector/snapshot.py`. Periodic per-symbol snapshots (5m default), staggered initial delays (`interval / symbol_count * i`).
- Public: `void start()`, `void stop()`.
- Each symbol runs in its own virtual thread: `initial wait → fetch → produce (as depth_snapshot envelope) → wait (stopLatch + interval)`.
- Important: periodic snapshots do NOT call `depthHandler.setSyncPoint(...)` — matches Python comment in `snapshot.py:156-159`. Only `DepthSnapshotResync` calls it.
- Emits `snapshot_poll_miss` gap on total failure via `GapEmitter`.
- Dependencies: `SnapshotFetcher`, `KafkaProducerBridge`, `GapEmitter`, `CollectorSession`, `ClockSupplier`, `List<String> symbols`, interval maps.
- Thread-safety: one virtual thread per symbol; stop coordinated via shared `CountDownLatch`.

**`DepthState`** — `sealed interface` with record variants:
- `Healthy(long lastU)` — pu-chain advancing normally.
- `Buffering(long firstBufferedU, List<PendingDiff> buffer)` — connected but no sync point yet; diffs accumulate up to `_MAX_PENDING_DIFFS=5000`.
- `Syncing(long snapshotRequestedAtNs, List<PendingDiff> buffer)` — snapshot REST call in flight OR awaiting backup-chain reader result.
- `Recovered(long lastUpdateId, Instant recoveredAt)` — transient state immediately after `setSyncPoint` (decays to `Healthy` after first live diff passes pu-chain).

States live per-symbol inside `DepthSnapshotResync`; transitions are explicit.

**`DepthSnapshotResync`** — final class.
- Purpose: the 88-line god-state-machine from `connection.py:195-282`, decomposed.
- Public: `void start(String symbol)` — invoked on: first public connect (per symbol), on public reconnect (per symbol), on pu-chain break (via `WebSocketSupervisor.triggerDepthResync`).
- Internal phases (each a separate private method):
  1. `waitProducerHealthy(symbol)` — loops 0..60s in 2s steps with `CountDownLatch.await(2, SECONDS)` checking `producer.isHealthyForResync()`. Timeout → emit `pu_chain_break` gap with detail "resync aborted, producer unhealthy" and abort.
  2. `tryBackupChainReader(symbol)` → `Optional<Long> lastU` via `BackupChainReader.readLastDepthUpdateId(brokers, topic, symbol, 30)`. On success, call `depthHandler.setSyncPoint(symbol, lastU)` (Rule 6 — replay preferred) and return.
  3. `fetchSnapshotWithRetries(symbol)` → `Optional<String> rawText` via `SnapshotFetcher.fetch(...)`. On empty, emit `pu_chain_break` gap and return.
  4. `applySnapshot(symbol, rawText)` → `long lastUpdateId = adapter.parseSnapshotLastUpdateId(rawText)`; `depthHandler.setSyncPoint(symbol, lastUpdateId)`; produce a `depth_snapshot` envelope.
- State table (per symbol; enforced by the `DepthState` sealed hierarchy inside `depthHandler`; the resync class merely triggers transitions):

  | Trigger                          | From state    | To state          | Side-effect                              |
  | -------------------------------- | ------------- | ----------------- | ---------------------------------------- |
  | first connect (public WS up)     | Healthy/∅     | Buffering         | `detector.reset()`                        |
  | first diff received              | Buffering     | Buffering         | append to pending buffer                  |
  | resync start (pu-break or connect)| any          | Syncing           | `detector.reset()`; keep pending buffer   |
  | backup-chain success             | Syncing       | Recovered→Healthy | `setSyncPoint(lastU)`, replay buffer       |
  | snapshot success                 | Syncing       | Recovered→Healthy | `setSyncPoint(lastUpdateId)`, replay buffer |
  | snapshot exhausted (3 retries)   | Syncing       | Buffering         | emit `pu_chain_break` gap; keep buffer     |
  | producer unhealthy > 60s         | Syncing       | Buffering         | emit `pu_chain_break` gap; keep buffer     |
  | live diff valid (after replay)   | Recovered     | Healthy           | no-op                                     |
  | pu-chain break on live diff       | Healthy       | Syncing           | `detector.reset()`, trigger resync        |
  | disconnect (public WS down)      | any           | Buffering→Syncing | emit ws_disconnect gap(s) per stream       |
  | diff arrives, buffer >= 5000     | Buffering     | Buffering         | drop; `_pending_drops++`; trigger resync   |
  | snapshot replay produces pu-break| Recovered     | Syncing           | emit pu_chain_break gap; re-trigger resync|

- Dependencies: `KafkaProducerBridge`, `BinanceAdapter`, `SnapshotFetcher`, `BackupChainReader`, `DepthStreamHandler`, `GapEmitter`, `CollectorSession`, `ClockSupplier`, `List<String> brokers`, `String topicPrefix`, `CollectorMetrics`.
- Thread-safety: `start(symbol)` may be invoked from multiple virtual threads (initial connect, reconnect, pu-chain-break callback). The per-symbol critical section is serialized by a `ConcurrentHashMap<String, ReentrantLock> perSymbolLock` (Tier 5 A5 — `ReentrantLock`, not `synchronized`), released before any blocking REST call (Tier 5 A5 watch-out).

### 2.7 Gap layer

**`GapEmitter`** — final class. Single-owner triad (Rule 5).
- Purpose: ALL gap emission paths in the module call one of `emit(...)` / `emitWithTimestamps(...)`. That method: (a) increments `collector_gaps_detected_total{exchange,symbol,stream,reason}`; (b) emits a structured log with `event=gap_emitted, reason, symbol, stream`; (c) builds a `GapEnvelope` via `GapEnvelope.create(...)` and passes it to `KafkaProducerBridge.produce(gap)`.
- Public: `void emit(String symbol, String stream, long sessionSeq, String reason, String detail)`, `void emitWithTimestamps(String symbol, String stream, long sessionSeq, String reason, String detail, long gapStartTs, long gapEndTs)`, `void emitOverflowRecovery(String symbol, String stream, OverflowWindow window)`.
- Dependencies: `CollectorMetrics`, `KafkaProducerBridge`, `CollectorSession`, `ClockSupplier`.
- Thread-safety: delegates to thread-safe collaborators; emits same-shape logs for identical call-sites.
- Matches writer's `com.cryptolake.writer.gap.GapEmitter` pattern.

**`GapDetector`** — thin facade class holding `Map<String, DepthGapDetector>` and `Map<(String,String), SessionSeqTracker>`.
- Public: `DiffValidationResult validateDepthDiff(String symbol, long U, long u, long pu)`, `Optional<SeqGap> checkSessionSeq(String symbol, String stream, long seq)`, `void resetDepth(String symbol)`, `void setDepthSyncPoint(String symbol, long lastUpdateId)`.
- Thread-safety: per-(symbol,stream) owner threads only. Not shared across sockets for the same stream (Tier 5 M2 disjoint sets).

**`DepthGapDetector`** — final class; exact port of `DepthGapDetector`.
- Fields: `boolean synced`, `long lastUpdateId`, `long lastU` (sentinel `Long.MIN_VALUE` for "unset"). All `long` (Tier 5 E1).
- Public: `void setSyncPoint(long lastUpdateId)`, `DiffValidationResult validateDiff(long U, long u, long pu)`, `void reset()`.

**`SessionSeqTracker`** — final class; per-(symbol, stream).
- Fields: `long lastSeq` (initialized to sentinel `-1L`).
- Public: `Optional<SeqGap> check(long seq)`.
- Thread-safety: confined to one owner thread.

**`SeqGap`** — record `long expected, long actual`.

**`DiffValidationResult`** — record `boolean valid, boolean gap, boolean stale, String reason`.

**`DisconnectGapCoalescer`** — final class.
- Purpose: ports `_disconnect_gap_emitted` set behavior plus the pre-bump seq-tracker fast-forward (Python `connection.py:283-322`).
- Public: `boolean tryMark(String symbol, String stream)` (returns false if already emitted; true on first), `void onData(String symbol, String stream)` (clears the flag).
- Backing store: `ConcurrentHashMap.newKeySet()`.
- Thread-safety: lock-free CHM.

### 2.8 Producer

**`KafkaProducerBridge`** — final class. The single `KafkaProducer<byte[],byte[]>` owner for the service (Tier 2 §14).
- Purpose: port of `src/collector/producer.py`. Per-stream buffer caps; overflow-window tracking; emit-gap convenience.
- Config (Tier 5 C1): `ACKS="all"`, `LINGER_MS=5`, `BUFFER_MEMORY=1_073_741_824L`, `ENABLE_IDEMPOTENCE=true`, `MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=1`, `bootstrap.servers = String.join(",", brokers)`, key/value serializers = `ByteArraySerializer`.
- Public: `boolean produce(DataEnvelope env)`, `boolean produce(GapEnvelope gap)` (internal — invoked by `GapEmitter`), `boolean isConnected()`, `boolean isHealthyForResync()`, `int flush(Duration timeout)`, `int poll(Duration timeout)`, `void setOverflowListener(OverflowListener listener)`.
- Internal state: `ReentrantLock lock` (Tier 5 A5) guarding `Map<String, Integer> bufferCounts`; `ConcurrentHashMap<(symbol,stream), OverflowWindow> overflowWindows`; `AtomicLong overflowSeq`.
- `produce(env)` sequence:
  1. `byte[] value = codec.toJsonBytes(env)`. Wrap Jackson exceptions → drop + `collector_messages_dropped_total++`.
  2. `String topic = TopicNames.forStream(topicPrefix, exchange, env.stream())`; `byte[] key = TopicNames.symbolKey(env.symbol())`.
  3. `lock.lock() → check/increment count under cap → lock.unlock()` (Tier 5 A5 watch-out: release before `send`).
  4. `producer.send(new ProducerRecord<>(topic, key, value), deliveryCb)`. On `BufferExhaustedException` / `TimeoutException` (Tier 5 C5): roll back count inside `lock`; record overflow; return false.
  5. On success: increment `collector_messages_produced_total`; if `(symbol, stream)` in `overflowWindows`, pop + `GapEmitter.emitOverflowRecovery(...)`.
- `isHealthyForResync()`: returns false if any overflow window active, OR if `overflowBufferBytes()` > 80% of `BUFFER_MEMORY`, OR if metadata probe fails. Metadata probe: `producer.partitionsFor("binance.depth")` with 5s timeout (replaces `list_topics(timeout=5)`; Tier 5 C6-ish).
- Thread-safety: `KafkaProducer` is thread-safe (documented by kafka-clients). Count-map under `ReentrantLock`. Delivery callback runs on kafka-clients' internal thread — also takes the lock (Tier 5 A5). `overflowSeq` atomic.

**`PerStreamBuffer`** — inner helper for count/cap lookups.
- Public: `int capFor(String stream)` (reads from `ProducerConfig.bufferCaps` with `default_stream_cap` fallback).

**`OverflowWindow`** — record `long startTsNs, int dropped` (mutable-ish via replacement in CHM `compute(...)`). Immutable record; `compute` replaces the whole record.

**`DeliveryCallback`** — implements `org.apache.kafka.clients.producer.Callback`.
- Purpose: decrement buffer count in a lock (Tier 5 A5). On `exception != null`, log `producer_delivery_failed` at ERROR. Does NOT throw (kafka-clients ignores thrown callbacks silently, but Tier 5 G1 says swallow-with-comment).
- Thread-safety: invoked from kafka-clients' IO thread; takes `KafkaProducerBridge.lock`.

**`OverflowListener`** — functional interface `void onOverflow(String exchange, String symbol, String stream)`. Implemented by `BackpressureGate::onDrop` wired into `WebSocketSupervisor`.

### 2.9 Backup

**`BackupChainReader`** — final class.
- Purpose: port of `src/collector/backup_chain_reader.py`. One-shot KafkaConsumer; `other_depth_topic` derivation.
- Public: `static String otherDepthTopic(String ownPrefix, String exchange)`, `Optional<Long> readLastDepthUpdateId(List<String> brokers, String topic, String symbol, Duration maxAge)`.
- Consumer config: `group.id="depth-chain-reader-<epochMs>"`, `auto.offset.reset=latest`, `enable.auto.commit=false`, `session.timeout.ms=10000`, key/value deserializers = `ByteArrayDeserializer`.
- Algorithm: `consumer.partitionsFor(topic)` → seek each partition to `offsetsForTimes(now - maxAge)` → poll 5s max → parse envelope via `codec.readTree(...)`, filter by `"symbol"`/`"type"=="data"`, check message timestamp age, extract `raw_text`.`u` (as `long` — Tier 5 E1 via `readTree(rawText).get("u").asLong()`). Return latest `u`.
- Closes consumer in `finally` (Tier 5 G1 best-effort swallow).
- Thread-safety: one consumer per call; never shared. Stateless.

### 2.10 Lifecycle

**`LifecycleStateManager`** — final class.
- Purpose: best-effort port of `Collector._init_state_manager`, `_register_lifecycle_start`, `_send_heartbeat`, `_mark_lifecycle_shutdown`, `_close_state_manager`. Uses JDBC + HikariCP on virtual threads.
- Public: `void connect()`, `void registerStart(ComponentRuntimeState state)`, `void heartbeat(ComponentRuntimeState state)`, `void markCleanShutdown(String component, String instanceId, boolean planned, String maintenanceId)`, `Optional<MaintenanceIntent> loadActiveMaintenanceIntent()`, `void close()`.
- All methods catch `SQLException` + `RuntimeException`, log WARN, and continue (Tier 5 J J / G2 — best-effort). `connect()` sets `this.dataSource=null` on failure; subsequent calls short-circuit.
- Dependencies: `HikariDataSource` or `null`; `ClockSupplier` for `started_at`.
- Thread-safety: `volatile HikariDataSource dataSource`; method-scoped connections. No shared mutable state.

**`ComponentRuntimeState`** — record `String component, String instanceId, String hostBootId, String startedAt, String lastHeartbeatAt` (ISO-8601 UTC — Tier 5 F1).

**`MaintenanceIntent`** — record `String maintenanceId, String reason, String createdAt`.

**`HeartbeatScheduler`** — final class.
- Purpose: schedules `lifecycleStateManager.heartbeat(...)` every 30s on a virtual-thread loop (Tier 5 A3 — `CountDownLatch.await(30, SECONDS)` returns false to continue, true on stop).
- Public: `void start()`, `void stop()`.

### 2.11 Metrics

**`CollectorMetrics`** — final class. Owns 8 Prometheus meters (§9).
- Follows writer's `WriterMetrics` pattern exactly: `NamingConvention.identity`; counters registered WITHOUT `_total` suffix (Tier 5 H4 watch-out); gauges via holder pattern (Tier 5 H6); histograms via `serviceLevelObjectives(...)` (Tier 5 H5).
- Public API enumerated in §9.
- Thread-safety: Micrometer meters are thread-safe; holders use `AtomicLong` / `AtomicReference` (no `synchronized`).

### 2.12 Harness

**`MetricSkeletonDump`** — final class. Mirror of writer's harness.
- Public: `static void main(String[] args)` — args[0] = output path.
- Instantiates `CollectorMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))`; exercises each meter with canonical label values (exchange=binance, symbol=btcusdt, stream=depth|trades|…); scrapes; canonicalises via the same regex/TreeSet logic as writer's; strips `_max` (Tier 5 H5 watch-out); writes output.
- Gradle task `:collector:dumpMetricSkeleton` runs it.

**`RawTextParityHarness`** — final class. THE load-bearing gate-3 harness.
- Public: `static void main(String[] args)` — `args[0]=fixturesDir` (= `parity-fixtures/websocket-frames`), optional `args[1]=reportPath`.
- Algorithm:
  1. Walk `fixturesDir` for every `*.raw` file; find the sibling `*.json` (Python-produced envelope).
  2. Read raw bytes: `byte[] rawBytes = Files.readAllBytes(rawPath)`; read envelope: `DataEnvelope expected = codec.readData(Files.readAllBytes(jsonPath))`.
  3. Simulate the capture path: decode rawBytes → `String rawFrame = new String(rawBytes, UTF_8)`; feed to `BinanceAdapter.routeStream(rawFrame)` → `FrameRoute(streamType, symbol, rawTextExtracted)`. For `depth_snapshot` and `open_interest` fixtures (no outer `"data":` wrapper) the fixture is the raw REST body; bypass `routeStream` and use the raw frame as `rawText` directly.
  4. Compute `javaBytes = rawTextExtracted.getBytes(UTF_8)`. Compare against `expected.rawText().getBytes(UTF_8)` byte-for-byte.
  5. Compute `Sha256.hexDigestUtf8(rawTextExtracted)`; compare to `expected.rawSha256()`.
  6. On mismatch: print hex diff (first 64 bytes) + file path; collect into failure list.
  7. After all fixtures: if failures non-empty, print summary + `System.exit(1)`. Else print `OK <N> fixtures` + `System.exit(0)`.
- Dependencies: `BinanceAdapter`, `EnvelopeCodec`, `ObjectMapper` (via codec). No Kafka, no HTTP.
- Gradle task `:collector:runRawTextParity` runs it with `baseDir=../parity-fixtures/websocket-frames`.

---

## 3. Concurrency design

Java 21 virtual threads + `StructuredTaskScope` where fan-out/cancellation semantics matter; `ConcurrentHashMap` + `AtomicInteger/AtomicLong` where possible; `ReentrantLock` (NEVER `synchronized`) where blocking sections cannot be broken up.

### 3.1 Thread topology

Single top-level `ExecutorService mainExec = Executors.newVirtualThreadPerTaskExecutor()` opened in `Main.main()` via try-with-resources. All long-running loops are `mainExec.submit(...)`:

- **T_ws_supervisor** (1 thread) — `WebSocketSupervisor.start()` forks one child per socket.
- **T_ws_public** (1 thread) — connection loop for the `public` WebSocket socket.
- **T_ws_market** (1 thread) — connection loop for the `market` WebSocket socket.
- **T_snapshot_scheduler** (1 + N threads) — one supervisor + one poll loop per symbol.
- **T_oi_poller** (1 + N threads) — one supervisor + one poll loop per symbol.
- **T_heartbeat** (1 thread) — runs `LifecycleStateManager.heartbeat(...)` every 30s.
- **T_resync** (ephemeral) — virtual thread spawned per `DepthSnapshotResync.start(symbol)` invocation; synchronizes on per-symbol `ReentrantLock`.
- **T_health** — `HealthServer` uses its own `jdk.httpserver` virtual-thread executor (from accepted common).
- **T_kafka_io** — internal kafka-clients thread pool; not owned by us. Delivery callbacks run here.
- **T_sigterm** — platform thread (JVM requirement) installed via `Runtime.getRuntime().addShutdownHook(new Thread(...))`.

`StructuredTaskScope.ShutdownOnFailure` is used inside `WebSocketSupervisor.start()`:
```text
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    for (Map.Entry<String,String> e : urls.entrySet()) {
        scope.fork(() -> connectionLoop(e.getKey(), e.getValue()));
    }
    scope.join();
    scope.throwIfFailed();
}
```
(Tier 5 A1: `ShutdownOnFailure` matches Python's `gather(..., return_exceptions=True)` semantics with explicit cancellation on first failure.)

### 3.2 Locking strategy

| Resource                                 | Primitive                         | Why                                                  |
| ---------------------------------------- | --------------------------------- | ---------------------------------------------------- |
| `KafkaProducerBridge.bufferCounts`       | `ReentrantLock`                   | Blocking `send` must NOT be inside (Tier 5 A5)       |
| `DepthSnapshotResync.perSymbolLock[s]`   | `ConcurrentHashMap<String, ReentrantLock>` | Serialize resync attempts for the same symbol (connect, pu-break, reconnect) without blocking other symbols |
| `overflowWindows`                         | `ConcurrentHashMap`              | Map mutations; `compute(...)` atomicity              |
| `lastReceivedAt`, `disconnectGapEmitted` | `ConcurrentHashMap.newKeySet()`   | Cross-socket reads                                   |
| `consecutiveDrops`                        | `AtomicInteger`                  | Backpressure callback is called from kafka-clients thread (Tier 5 A1) |
| `sessionSeq` allocator                    | `AtomicLong` per key             | Lock-free                                            |

No `synchronized` anywhere in the module (Tier 2 §9).

### 3.3 Cancellation and shutdown

1. SIGTERM hook counts down `shutdownLatch`.
2. `Main` blocks on `shutdownLatch.await()`.
3. After the latch: `shutdownAll()` sequentially stops (in order) — `HeartbeatScheduler`, `WebSocketSupervisor` (which `StructuredTaskScope.close()` cancels children), `SnapshotScheduler`, `OpenInterestPoller`, `KafkaProducerBridge.flush(Duration.ofSeconds(10))`, `LifecycleStateManager.markCleanShutdown(...)` + `close()`, `HealthServer.stop()`, `mainExec.close()` (awaits termination).
4. Interruptible waits use `CountDownLatch.await(timeout)` (Tier 5 A3). `InterruptedException` inside workers sets `Thread.currentThread().interrupt()` and exits the loop (Tier 5 A4).
5. Best-effort cleanup paths swallow via `try { ... } catch (Exception ignored) { // comment }` (Tier 5 G1). Never catches `Throwable`.

### 3.4 Mapping §5 concurrency items — addressed

| Python primitive (§5)                    | Java realization                                |
| ---------------------------------------- | ----------------------------------------------- |
| `asyncio.create_task` fan-out            | `StructuredTaskScope.fork` (Tier 5 A1)           |
| `asyncio.gather(..., return_exceptions=True)` | `ShutdownOnFailure` + `join()` + `throwIfFailed` |
| `asyncio.Event` for graceful stop         | `CountDownLatch` per lifecycle (Tier 5 A3)       |
| `asyncio.wait_for(ev.wait(), timeout)`    | `latch.await(timeout, unit)`                     |
| `asyncio.CancelledError` handling         | `InterruptedException` catch → restore interrupt flag |
| `asyncio.get_running_loop().create_task()` for best-effort resync | `scope.fork(() -> resync(symbol))` inside owning `WebSocketSupervisor` |
| `threading.Lock` on producer             | `ReentrantLock` (Tier 5 A5)                      |
| `producer.poll(0.1)` blocking I/O        | direct blocking call on virtual thread (Tier 5 A2) |
| `producer.list_topics(timeout=5)` blocking | direct blocking call on virtual thread; replaced with `producer.partitionsFor(...)` |
| `producer.flush(timeout=10)` blocking    | direct blocking call                             |
| `aiohttp.ClientSession()` per service     | single `HttpClient` (Tier 5 D3; Tier 2 §14)       |
| `async for raw_frame in ws`              | `WebSocket.Listener.onText` + `FrameAccumulator` + `ws.request(1)` (Tier 5 D1, D2) |
| backpressure `consecutive_drops`          | `AtomicInteger` + `BackpressureGate` (Tier 5 A1)  |

---

## 4. Python → Java mapping table

Every symbol from mapping §3 (Public API surface) and §4 (Internal structure) → Java class.method. Any symbol intentionally dropped is marked `DROPPED` with a reason.

### 4.1 `src/collector/main.py`

| Python symbol                                 | Java                                        | Notes                                        |
| --------------------------------------------- | ------------------------------------------- | -------------------------------------------- |
| `Collector.__init__(config_path)`             | `Main.main(args)` wiring block               | Direct wiring; no `Collector` class in Java  |
| `Collector.start()`                           | `Main.main(args)` body (before latch await) | All start() lines become explicit calls      |
| `Collector.shutdown()`                        | `Main.shutdownAll()` (private)               | Invoked from SIGTERM hook + latch              |
| `Collector._on_producer_overflow()`            | `BackpressureGate::onDrop`                   | Atomic counter bump                            |
| `Collector._on_pu_chain_break(symbol)`         | `WebSocketSupervisor.triggerDepthResync(symbol)` → `DepthSnapshotResync.start(symbol)` | Virtual-thread fork    |
| `Collector._init_state_manager`                | `LifecycleStateManager.connect()`            | Best-effort                                    |
| `Collector._register_lifecycle_start`          | `LifecycleStateManager.registerStart(...)`   |                                                |
| `Collector._send_heartbeat`                    | `LifecycleStateManager.heartbeat(...)`       |                                                |
| `Collector._heartbeat_loop`                    | `HeartbeatScheduler.start()`                 | Loop with `CountDownLatch.await(30s)`          |
| `Collector._mark_lifecycle_shutdown`           | `LifecycleStateManager.markCleanShutdown(...)` |                                              |
| `Collector._close_state_manager`               | `LifecycleStateManager.close()`              |                                                |
| `Collector._ready_checks`                      | `CollectorReadyCheck.get()` impl             | Used by `HealthServer`                         |
| `main()`                                       | `Main.main(String[])`                        |                                                |
| `src/common/service_runner.run_service`        | DROPPED                                      | Java replaces with `Main.main` + SIGTERM hook  |

### 4.2 `src/collector/connection.py` → `connection/`

| Python symbol                                  | Java                                                   | Notes                                |
| ---------------------------------------------- | ------------------------------------------------------ | ------------------------------------ |
| `WebSocketManager.__init__`                    | `WebSocketSupervisor` constructor                       |                                       |
| `WebSocketManager._next_seq`                    | `SessionSeqAllocator.next(symbol, stream)`              |                                       |
| `WebSocketManager._get_ws_urls`                | `BinanceAdapter.getWsUrls(...)` + `_REST_ONLY_STREAMS` filter in supervisor |             |
| `WebSocketManager.start`                       | `WebSocketSupervisor.start()`                            | Uses `StructuredTaskScope`             |
| `WebSocketManager.has_ws_streams`              | `WebSocketSupervisor.hasWsStreams()`                     |                                       |
| `WebSocketManager.is_connected`                 | `WebSocketSupervisor.isConnected()`                      |                                       |
| `WebSocketManager.stop`                         | `WebSocketSupervisor.stop()`                             |                                       |
| `WebSocketManager._connection_loop`            | `WebSocketSupervisor.connectionLoop(socketName, url)`    | Inside `StructuredTaskScope.fork`     |
| `WebSocketManager._receive_loop`               | `RawFrameCapture.onFrame(...)` (invoked from `WebSocketListenerImpl`) | Backpressure pause moves here |
| `WebSocketManager._depth_resync`               | `DepthSnapshotResync.start(symbol)` + private phase methods | State machine (§3)       |
| `WebSocketManager._emit_disconnect_gaps`       | `RawFrameCapture.onDisconnect(socketName)` → `DisconnectGapCoalescer` + `GapEmitter` |    |
| `_MAX_BACKOFF = 60`                             | `ReconnectPolicy.MAX_BACKOFF_SECONDS`                    |                                       |
| `_RECONNECT_BEFORE_24H`                        | `ReconnectPolicy.PROACTIVE_RECONNECT_DURATION`           | `Duration.ofHours(23).plusMinutes(50)` |
| `_REST_ONLY_STREAMS`                           | `StreamKey.REST_ONLY_STREAMS`                             |                                       |

### 4.3 `src/collector/producer.py` → `producer/`

| Python symbol                               | Java                                                  |
| ------------------------------------------- | ----------------------------------------------------- |
| `CryptoLakeProducer.__init__`               | `KafkaProducerBridge` constructor                      |
| `CryptoLakeProducer._get_stream_cap`        | `PerStreamBuffer.capFor(stream)`                       |
| `CryptoLakeProducer.produce(envelope)`      | `KafkaProducerBridge.produce(DataEnvelope)` / `produce(GapEnvelope)` |
| `CryptoLakeProducer._record_overflow`       | `KafkaProducerBridge.recordOverflow(symbol, stream)`   |
| `CryptoLakeProducer._make_delivery_cb`      | `DeliveryCallback`                                     |
| `CryptoLakeProducer._emit_overflow_gap`     | `GapEmitter.emitOverflowRecovery(...)`                 |
| `CryptoLakeProducer.emit_gap`               | `GapEmitter.emitWithTimestamps(...)`                   |
| `CryptoLakeProducer.is_connected`           | `KafkaProducerBridge.isConnected()`                    |
| `CryptoLakeProducer.is_healthy_for_resync`  | `KafkaProducerBridge.isHealthyForResync()`             |
| `CryptoLakeProducer.flush`, `.poll`         | `KafkaProducerBridge.flush(Duration)`, `.poll(Duration)`|
| `_tap_root` + `FrameTap`                     | DROPPED — tap is Python-only (mapping §10.K). Java collector MUST NOT ever write fixture files. |

### 4.4 `src/collector/streams/`

| Python                                           | Java                                    |
| ------------------------------------------------ | --------------------------------------- |
| `StreamHandler` (base class)                    | `streams.StreamHandler` sealed interface |
| `StreamHandler._init_seq_tracking`              | `SimpleStreamHandler` constructor (injects `SessionSeqTracker`) |
| `StreamHandler._check_seq`                      | `SimpleStreamHandler.checkSeq(symbol, seq)` (private) |
| `SimpleStreamHandler.__init__`                  | `SimpleStreamHandler` constructor       |
| `SimpleStreamHandler.handle`                    | `SimpleStreamHandler.handle`            |
| `DepthHandler.__init__`                         | `DepthStreamHandler` constructor        |
| `DepthHandler._record_drop`                     | `DepthStreamHandler.recordDrop`         |
| `DepthHandler.handle`                            | `DepthStreamHandler.handle`             |
| `DepthHandler.set_sync_point`                    | `DepthStreamHandler.setSyncPoint`       |
| `DepthHandler.reset`                             | `DepthStreamHandler.reset`              |
| `_MAX_PENDING_DIFFS=5000`                         | `DepthStreamHandler.MAX_PENDING_DIFFS`  |
| `OpenInterestPoller.__init__`                    | `OpenInterestPoller` constructor        |
| `OpenInterestPoller.start/stop`                  | same names                              |
| `OpenInterestPoller._poll_loop/_poll_once`       | private methods                          |

### 4.5 `src/collector/snapshot.py`

| Python                           | Java                                |
| --------------------------------- | ----------------------------------- |
| `parse_interval_seconds`          | `SnapshotScheduler.parseInterval(String)` (static) |
| `SnapshotScheduler.__init__`      | `SnapshotScheduler` constructor      |
| `SnapshotScheduler._get_interval` | `SnapshotScheduler.getInterval(symbol)` |
| `SnapshotScheduler.start/stop`    | same                                 |
| `SnapshotScheduler.fetch_snapshot` | `SnapshotFetcher.fetch(symbol)`      |
| `SnapshotScheduler._poll_loop`    | `SnapshotScheduler.pollLoop(symbol, intervalSec, delaySec)` |
| `SnapshotScheduler._take_snapshot` | `SnapshotScheduler.takeSnapshot(symbol)` |

### 4.6 `src/collector/gap_detector.py`

| Python                   | Java                          |
| ------------------------ | ----------------------------- |
| `DiffValidationResult`   | `gap.DiffValidationResult`     |
| `SeqGap`                 | `gap.SeqGap`                   |
| `DepthGapDetector`       | `gap.DepthGapDetector`         |
| `SessionSeqTracker`      | `gap.SessionSeqTracker`        |

### 4.7 `src/collector/backup_chain_reader.py`

| Python                             | Java                                              |
| ---------------------------------- | ------------------------------------------------- |
| `other_depth_topic`                | `backup.BackupChainReader.otherDepthTopic(...)`    |
| `read_last_depth_update_id`        | `backup.BackupChainReader.readLastDepthUpdateId(...)` |

### 4.8 `src/collector/metrics.py`

| Python metric            | Java meter (class `CollectorMetrics`) — see §9 |
| ------------------------ | ---------------------------------------------- |
| `messages_produced_total` | `messagesProduced(String, String, String)`    |
| `ws_connections_active`   | `setWsConnectionsActive(String, int)`          |
| `ws_reconnects_total`     | `wsReconnects(String)`                         |
| `gaps_detected_total`     | `gapsDetected(String, String, String, String)` |
| `exchange_latency_ms`     | `exchangeLatencyMs(String, String, String)`   |
| `snapshots_taken_total`   | `snapshotsTaken(String, String)`               |
| `snapshots_failed_total`  | `snapshotsFailed(String, String)`              |
| `messages_dropped_total`  | `messagesDropped(String, String, String)`      |

### 4.9 `src/collector/tap.py`

DROPPED — fixture-capture tap is Python-only (mapping §10.K). Java collector MUST NOT implement it. Java's gate 3 harness consumes the Python-produced fixtures.

### 4.10 `src/exchanges/binance.py`

| Python                             | Java                                    |
| ---------------------------------- | --------------------------------------- |
| `_STREAM_KEY_MAP`, `_SUBSCRIPTION_MAP`, `_PUBLIC_STREAMS`, `_MARKET_STREAMS` | `adapter.StreamKey` constants |
| `BinanceAdapter.__init__`          | `BinanceAdapter` constructor             |
| `BinanceAdapter.get_ws_urls`       | `BinanceAdapter.getWsUrls`                |
| `BinanceAdapter.route_stream`       | `BinanceAdapter.routeStream`              |
| `BinanceAdapter.extract_exchange_ts`| `BinanceAdapter.extractExchangeTs`        |
| `BinanceAdapter.build_*_url`        | `BinanceAdapter.build*Url(...)` (snapshot + open_interest only in scope; historical methods are CLI-module concerns — DROPPED from collector) |
| `BinanceAdapter.parse_snapshot_last_update_id` | `BinanceAdapter.parseSnapshotLastUpdateId` → `long` (Tier 5 E1) |
| `BinanceAdapter.parse_depth_update_ids` | `BinanceAdapter.parseDepthUpdateIds` → `DepthUpdateIds` record of three `long`s |
| `_extract_data_value`              | `adapter.RawDataExtractor.extractDataValue(frame, start)` |
| `_parse_stream_key`                 | `StreamKey.parseStreamKey(streamKey)`     |

---

## 5. Library mapping

| Python dep              | Java library                                                 | Version (from `gradle/libs.versions.toml`) | Notes                                                    |
| ----------------------- | ------------------------------------------------------------ | ------------------------------------------- | -------------------------------------------------------- |
| `asyncio` / `uvloop`    | Java 21 virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) | stdlib (Java 21)                       | Direct blocking on VT; no executor wrap (Tier 5 A2, A8)  |
| `websockets`            | `java.net.http.HttpClient.newWebSocketBuilder()`             | stdlib (Java 21)                           | Spike-proven byte-identity (Tier 5 D1; Rule 1)           |
| `aiohttp`               | `java.net.http.HttpClient`                                   | stdlib                                      | One per service (Tier 5 D3)                               |
| `confluent-kafka`       | `org.apache.kafka:kafka-clients`                             | `3.8.0`                                     | Producer + Consumer (Tier 5 C1, C5)                       |
| `orjson`                | `com.fasterxml.jackson.core:jackson-databind`                | `2.17.2`                                    | Via common's `EnvelopeCodec`                              |
| `pyyaml`                | `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml`   | `2.17.2`                                    | Via common's `YamlConfigLoader`                           |
| `pydantic` / `pydantic-settings` | `org.hibernate.validator:hibernate-validator`      | `8.0.1.Final`                                | For `@Min`, `@Size`, `@NotNull` on common config records; compact constructors for Python `field_validator` (Tier 5 J1–J3) |
| `prometheus-client`     | `io.micrometer:micrometer-registry-prometheus`               | `1.13.4`                                    | `NamingConvention.identity` (Tier 5 H4)                   |
| `structlog`             | `org.slf4j:slf4j-api` + `ch.qos.logback:logback-classic` + `net.logstash.logback:logstash-logback-encoder` | `2.0.13`, `1.5.8`, `7.4` | Via common's `StructuredLogger`; MDC for symbol/stream (Tier 5 H2, H3) |
| `psycopg[binary]`       | `org.postgresql:postgresql` + `com.zaxxer:HikariCP`          | `42.7.4`, `5.1.0`                            | Only used by `LifecycleStateManager` — best-effort         |
| `pytest` + `pytest-asyncio` | `org.junit.jupiter:junit-jupiter`                        | `5.11.0`                                    | All tests sync on VT (Tier 5 L3)                           |
| `testcontainers[kafka]`  | `org.testcontainers:kafka`                                   | `1.20.1`                                    | For `BackupChainReaderIT`                                  |
| — (spike code)          | `info.picocli:picocli`                                       | `4.7.6`                                     | Not used in collector module (CLI is a later module)       |
| — (built-in)             | `com.github.luben:zstd-jni`                                  | `1.5.6-4`                                   | Not used in collector module (zstd is writer-only)         |

No deviations from spec §1.3 library table. No reflective framework (Tier 2 §11).

---

## 6. Data contracts

All envelope records are reused from `com.cryptolake.common.envelope.*`. The collector does NOT redefine them.

### 6.1 `DataEnvelope` (reused)

Already defined in `com.cryptolake.common.envelope.DataEnvelope`. Field-by-field match with mapping §7:

| Python field             | Java field (JSON key)          | Java type  | Notes                                        |
| ------------------------ | ------------------------------ | ---------- | -------------------------------------------- |
| `v`                      | `v` (`v`)                      | `int`       | constant 1                                   |
| `type`                   | `type` (`type`)                | `String`    | constant `"data"`                             |
| `exchange`               | `exchange` (`exchange`)        | `String`    | `"binance"`                                   |
| `symbol`                 | `symbol` (`symbol`)            | `String`    | lowercase (Tier 5 M1)                          |
| `stream`                 | `stream` (`stream`)            | `String`    | one of `trades/depth/bookticker/...`           |
| `received_at`            | `receivedAt` (`received_at`)   | `long`      | ns (Tier 5 E2)                                 |
| `exchange_ts`            | `exchangeTs` (`exchange_ts`)   | `long`      | ms, `0` when missing                           |
| `collector_session_id`   | `collectorSessionId` (`collector_session_id`) | `String` | Tier 5 M7 format                     |
| `session_seq`            | `sessionSeq` (`session_seq`)   | `long`      | Tier 5 E1                                     |
| `raw_text`               | `rawText` (`raw_text`)         | `String`    | byte-for-byte passthrough (Tier 1 §1, §7)      |
| `raw_sha256`             | `rawSha256` (`raw_sha256`)     | `String`    | lowercase hex; computed ONCE in `create(...)` (Tier 1 §2) |

Jackson config: insertion order via `@JsonPropertyOrder` (Tier 5 B1), no naming strategy override, `writeValueAsBytes` (no indent, Tier 5 B2).

### 6.2 `GapEnvelope` (reused)

Already defined. Matches Python's `create_gap_envelope` field order with optional restart-metadata last. The collector sets `reason ∈ {ws_disconnect, pu_chain_break, session_seq_skip, buffer_overflow, snapshot_poll_miss}` only; restart-metadata fields are always `null` in collector-produced gaps (they are writer-only).

### 6.3 `DepthUpdateIds`

New record specific to collector:
```text
public record DepthUpdateIds(long U, long u, long pu) {}
```
All three `long` per Tier 5 E1. No JSON mapping — purely internal.

### 6.4 `FrameRoute`

```text
public record FrameRoute(String streamType, String symbol, String rawText) {}
```
Internal record; `rawText` is a substring of the original frame (Tier 1 §1 byte identity).

### 6.5 `PendingDiff`

```text
public record PendingDiff(String rawText, Long exchangeTs, long sessionSeq) {}
```
`exchangeTs` boxed to preserve Python's `None` possibility (distinguishes "absent" from `0`).

### 6.6 `OverflowWindow`

```text
public record OverflowWindow(long startTsNs, int dropped) {}
```
Replaced via CHM `compute(...)` — mutation is record-level replacement (Tier 2 §12).

### 6.7 `ComponentRuntimeState` / `MaintenanceIntent`

Already defined in writer's `state/` package but the collector needs its own copies in `com.cryptolake.collector.lifecycle.*` because writer's records may include writer-specific fields (`classifier`, `evidence`). The collector's records include only the fields Python's `Collector._register_lifecycle_start` / `_mark_lifecycle_shutdown` actually use: `component, instance_id, host_boot_id, started_at, last_heartbeat_at`.

### 6.8 Config records (reused)

`AppConfig`, `BinanceExchangeConfig`, `CollectorConfig`, `DepthConfig`, `OpenInterestConfig`, `ProducerConfig`, `RedpandaConfig`, `StreamsConfig`, `DatabaseConfig`, `MonitoringConfig` — ALL already defined in `com.cryptolake.common.config.*`. The collector's `Main` reads them via `YamlConfigLoader.load(Path)`.

### 6.9 ObjectMapper config

Single instance from `EnvelopeCodec.newMapper()` (already accepted). Settings verified in common: `SORT_PROPERTIES_ALPHABETICALLY=false`, `INDENT_OUTPUT=false`, `ORDER_MAP_ENTRIES_BY_KEYS=false`, `FAIL_ON_UNKNOWN_PROPERTIES=false`. No per-call allocations (Tier 5 B6).

---

## 7. Error model

### 7.1 Hierarchy

- `com.cryptolake.common.config.CryptoLakeConfigException extends RuntimeException` — already defined, fatal on config load errors (Tier 5 G2).
- `java.lang.IllegalArgumentException` — fail-fast in `GapReasons.requireValid` and config compact constructors (Tier 2 §16). Propagates up to `main()`, which logs and `System.exit(1)`.
- `java.io.UncheckedIOException` — wraps Jackson `IOException` (via `EnvelopeCodec`). Caught in `KafkaProducerBridge.produce` where appropriate; unexpected in hot paths means fail-fast.
- `java.io.IOException` — from `HttpClient.send` and `KafkaConsumer.poll` in `BackupChainReader`; caught at the call site, logged, and mapped to `Optional.empty()` (best-effort recovery).
- `java.nio.http.WebSocketHandshakeException` / `ConnectionClosedException` / `IOException` — caught in `WebSocketSupervisor.connectionLoop`; logged `ws_disconnected` at WARN; triggers reconnect via `ReconnectPolicy`.
- `org.apache.kafka.common.errors.BufferExhaustedException` + `TimeoutException` — caught in `KafkaProducerBridge.produce`; drop + record overflow (Tier 5 C5).
- `InterruptedException` — caught at every blocking call site; immediately sets `Thread.currentThread().interrupt()` and exits the loop (Tier 5 A4; Tier 2 §9).
- `java.sql.SQLException` — ONLY inside `LifecycleStateManager`; caught, logged WARN, swallowed (Tier 5 G2).

### 7.2 What retries, what kills

| Error                                                 | Policy                                                                    |
| ----------------------------------------------------- | ------------------------------------------------------------------------- |
| `CryptoLakeConfigException` at startup                | Fatal: log + `System.exit(1)` from `Main`                                  |
| WebSocket handshake fail / closed                     | Reconnect with exp backoff 1→60s (Tier 5 D7)                              |
| WebSocket 24h proactive window                        | Close + reconnect immediately (Tier 5 D6)                                  |
| HTTP 429 on snapshot / OI                              | Honor `Retry-After` then retry (Tier 5 D4)                                |
| HTTP 4xx (non-429) / 5xx                              | Log + retry up to 3 times, exp backoff 1s/2s; on exhaustion emit `snapshot_poll_miss` gap |
| `BufferExhaustedException` / `TimeoutException` on send | Drop message; `messages_dropped_total++`; record overflow window           |
| Per-stream cap exceeded                                | Drop; `messages_dropped_total++`; record overflow window                    |
| Jackson serialize/deserialize failure                 | Log + drop; `messages_dropped_total++` (envelope never reached Kafka)      |
| Depth diff validation fail (`stale`/`gap`/`not synced`) | Buffer / drop per state; emit `pu_chain_break` gap on break                |
| Session seq skip                                       | Emit `session_seq_skip` gap; continue                                       |
| PG unreachable                                         | Log WARN; `dataSource=null`; continue                                       |
| `InterruptedException`                                 | Restore interrupt flag + exit loop (graceful shutdown path)                 |
| Unexpected `RuntimeException` in a WS connection loop | Log `ws_unexpected_error`; reconnect (matches Python `connection.py:122`)   |
| Unexpected `RuntimeException` in a non-retryable caller path | Escape to `main()`; SIGTERM-style shutdown; `System.exit(1)`          |

### 7.3 Invariant-violation fail-fast

- Unknown gap `reason`: `GapReasons.requireValid` throws `IllegalArgumentException`. Caught nowhere; kills the JVM (Tier 2 §16 — correct behavior).
- Null `rawText`: `DataEnvelope.create` invokes `Sha256.hexDigestUtf8(rawText)` which throws NPE. Tier 2 §16 — intentional.
- Config with `retention_hours < 12` (Hibernate Validator): thrown at startup.

---

## 8. Test plan

One-to-one ports from mapping §8 (plus tier-5 coverage) to JUnit 5 classes. Every test method carries a `// ports: tests/unit/...::test_name` trace comment (Tier 3 §17).

All tests live in `cryptolake-java/collector/src/test/java/com/cryptolake/collector/`. No Docker in the unit layer. Chaos tests go in `src/test/java/.../CollectorChaosIT.java` with `@Tag("chaos") @Disabled("requires docker-compose stack — skeleton only")`.

### 8.1 Unit tests

| JUnit class                             | Method                                                 | Port of                                                                         |
| --------------------------------------- | ------------------------------------------------------ | -------------------------------------------------------------------------------- |
| `DepthGapDetectorTest`                  | `firstDiffAfterSyncAccepted`                           | `test_gap_detector.py::test_first_diff_after_sync_accepted`                      |
|                                         | `subsequentDiffPuChainValid`                           | `::test_subsequent_diff_pu_chain_valid`                                          |
|                                         | `puChainBreakDetected`                                 | `::test_pu_chain_break_detected`                                                 |
|                                         | `staleDiffBeforeSyncRejected`                          | `::test_stale_diff_before_sync_rejected`                                         |
|                                         | `noSyncPointRejects`                                   | `::test_no_sync_point_rejects`                                                   |
|                                         | `resetClearsState`                                     | `::test_reset_clears_state`                                                      |
| `SessionSeqTrackerTest`                 | `sequentialOk`                                         | `::test_session_seq_sequential_ok`                                               |
|                                         | `gapDetected`                                          | `::test_session_seq_gap_detected`                                                |
|                                         | `firstMessageOk`                                       | `::test_session_seq_first_message_ok`                                            |
| `GapEmitterTest`                        | `emitGapProducesEnvelopeAndIncrementsMetric`           | `test_emit_gap.py::test_emit_gap_produces_gap_envelope_and_increments_metric`    |
|                                         | `emitGapUsesCustomTimestamps`                          | `::test_emit_gap_uses_custom_timestamps`                                         |
|                                         | `emitOverflowGapIncrementsGapsDetectedTotal`           | `::test_emit_overflow_gap_increments_gaps_detected_total`                        |
| `DisconnectGapCoalescerTest`            | `firstDisconnectEmitsOneGapPerPublicStream`            | `test_disconnect_gap_coalescing.py::test_first_disconnect_emits_one_gap_per_public_stream` |
|                                         | `repeatedDisconnectCallsCoalesce`                      | `::test_repeated_disconnect_calls_coalesce`                                      |
|                                         | `dataArrivalClearsEmittedFlag`                         | `::test_data_arrival_clears_emitted_flag`                                        |
| `KafkaProducerBridgeTopicPrefixTest`    | `topicPrefixPrepended`                                 | `test_producer_topic_prefix.py`                                                  |
| `KafkaProducerBridgePerStreamCapTest`   | `depthCapEnforcedSeparateFromTrades`                   | (new; mapping §4 producer per-stream cap behavior)                                |
|                                         | `overflowWindowEmitsGapOnRecovery`                     | `test_emit_gap.py::test_emit_overflow_gap_increments_gaps_detected_total` extended |
| `DepthStreamHandlerTest`                | `handleValidDiffProduces`                              | port of `streams/depth.py::handle` happy path                                    |
|                                         | `handleStaleDiffRecordsDrop`                           | port of stale path                                                               |
|                                         | `handlePuChainBreakTriggersResyncCallback`             | port of pu-break path                                                            |
|                                         | `handleBeforeSyncBuffersDiff`                          | port of buffered path                                                            |
|                                         | `bufferFullRecordsDropAndTriggersResync`               | port of `_MAX_PENDING_DIFFS` path                                                 |
|                                         | `setSyncPointReplaysBufferedDiffs`                     | port of `set_sync_point` replay                                                  |
|                                         | `setSyncPointEmitsDeferredPuChainBreakGap`              | port of deferred-gap emission on first drops                                     |
| `DepthSnapshotResyncTest`               | `waitProducerHealthyTimeoutEmitsGap`                   | port of `_depth_resync` 60s timeout path                                         |
|                                         | `backupChainReaderSuccessSkipsSnapshot`                | port of `read_last_depth_update_id` non-None path                                |
|                                         | `snapshotSuccessSetsSyncPointAndProducesDepthSnapshot` | port of `_depth_resync` happy path                                               |
|                                         | `snapshotExhaustedEmitsPuChainBreakGap`                | port of retries-exhausted path                                                   |
|                                         | `rateLimit429HonorsRetryAfter`                         | port of 429 handling                                                              |
| `BackupChainReaderTest`                 | `otherDepthTopicPrimaryPrefixReturnsBackup`            | `test_backup_chain_reader.py::test_other_depth_topic_primary`                    |
|                                         | `otherDepthTopicBackupPrefixReturnsPrimary`            | `::test_other_depth_topic_backup`                                                |
|                                         | `readLastUpdateIdFiltersBySymbol`                      | `::test_read_last_depth_update_id_filters_symbol` (Testcontainers Kafka)         |
|                                         | `readLastUpdateIdHonorsMaxAge`                         | `::test_read_last_depth_update_id_max_age`                                        |
|                                         | `readLastUpdateIdReturnsNoneWhenTopicMissing`          | `::test_read_last_depth_update_id_topic_missing`                                  |
| `SnapshotSchedulerTest`                 | `intervalParsesMinutesAndSeconds`                      | `test_snapshot.py` (parse_interval_seconds)                                      |
|                                         | `fetchSnapshotSuccess`                                 | `::test_fetch_snapshot_success`                                                  |
|                                         | `fetchSnapshot429HonorsRetryAfter`                     | (new, Tier 5 D4)                                                                  |
|                                         | `allRetriesFailedEmitsSnapshotPollMissGap`             | port of fetch_snapshot None path                                                  |
| `OpenInterestPollerTest`                | `pollSuccessProducesEnvelope`                          | `open_interest.py` happy path                                                    |
|                                         | `pollExhaustedEmitsSnapshotPollMissGap`                | exhausted path                                                                    |
|                                         | `stopInterruptsInitialDelay`                           | Tier 5 A3                                                                        |
| `RawDataExtractorTest`                  | `simpleObject`                                         | (new)                                                                             |
|                                         | `nestedObjectsPreserveBytes`                            | (new)                                                                             |
|                                         | `stringWithEscapedBraces`                              | (new, Tier 5 B4)                                                                  |
|                                         | `unbalancedBracesThrows`                                | (new)                                                                             |
|                                         | `unicodeAndEmojiPreserved`                              | (new, Rule 1 byte identity)                                                      |
| `BinanceAdapterTest`                    | `routeStreamExtractsRawTextVerbatim`                   | (new; fixture corpus)                                                             |
|                                         | `parseDepthUpdateIdsReturnsLongs`                      | (new, Tier 5 E1)                                                                  |
|                                         | `parseSnapshotLastUpdateIdReturnsLong`                 | (new)                                                                             |
|                                         | `getWsUrlsSplitsPublicMarket`                          | (new)                                                                             |
|                                         | `buildSnapshotUrlUpperCasesSymbol`                      | (new, Tier 5 M1)                                                                  |
| `ReconnectPolicyTest`                   | `exponentialBackoffCappedAt60s`                        | Tier 5 D7                                                                        |
|                                         | `resetOnConnectRestoresOneSecond`                      |                                                                                  |
|                                         | `shouldProactivelyReconnectAt23h50m`                   | Tier 5 D6                                                                        |
| `FrameAccumulatorTest`                  | `singleFragmentCompleteReturnsString`                  | Tier 5 D1                                                                        |
|                                         | `multiFragmentAccumulatesUntilLast`                    |                                                                                  |
|                                         | `resetBetweenFrames`                                   |                                                                                  |
|                                         | `multiByteUtf8Preserved`                                | byte-identity                                                                    |
| `BackpressureGateTest`                  | `dropsBelowThresholdDoNotPause`                        |                                                                                  |
|                                         | `thresholdReachedPauses`                                |                                                                                  |
|                                         | `recoveryClearsCounter`                                |                                                                                  |
| `MetricSkeletonDumpTest`                | `canonicalizedOutputMatchesFixture`                     | gate 4 regression                                                                 |
| `RawTextParityHarnessTest`              | `fixtureCorpusMatchesByteForByte`                       | gate 3 regression                                                                 |
| `CollectorChaosIT` (`@Tag("chaos")`)    | `wsDisconnectSlowReconnectEmitsSingleGap`               | mapping §8 chaos 1                                                                 |
|                                         | `puChainBreakTriggersResync`                            | chaos 2                                                                           |
|                                         | `snapshotPollMissGapEmitted`                             | chaos 3                                                                           |
|                                         | `bufferOverflowPerStreamIsolation`                      | chaos 4                                                                           |
|                                         | `producerUnhealthyAbortsResync`                         | chaos 5                                                                           |
|                                         | `backupChainReaderSkipsSnapshotWhenRecent`              | chaos 6                                                                           |

### 8.2 Gate 3 harness — `:collector:runRawTextParity`

Implementation is `harness.RawTextParityHarness.main`. Gradle task:
```text
tasks.register<JavaExec>("runRawTextParity") {
  group = "port"
  description = "Replay fixtures through capture path; compare raw_text/raw_sha256."
  classpath = sourceSets["main"].runtimeClasspath
  mainClass.set("com.cryptolake.collector.harness.RawTextParityHarness")
  args(
    rootProject.file("../parity-fixtures/websocket-frames").absolutePath,
    layout.buildDirectory.file("reports/gate3-parity.txt").get().asFile.absolutePath
  )
}
```
Exit code 0 = pass; 1 = any byte or SHA mismatch.

### 8.3 Gate 4 harness — `:collector:dumpMetricSkeleton`

Implementation is `harness.MetricSkeletonDump.main`. Gradle task writes to `build/reports/collector-metric-skeleton.txt`; orchestrator diffs against `parity-fixtures/metrics/collector.txt` (produced from Python by `scripts/capture_fixtures.sh`). Must strip `_max` lines. Only `collector_*` name prefix retained.

---

## 9. Metrics plan

Eight Prometheus meters, all owned by `CollectorMetrics`. Names and labels MUST match `src/collector/metrics.py` byte-for-byte (Tier 3 §18). Registered WITHOUT the `_total` suffix (Tier 5 H4); Prometheus scrape appends it. `NamingConvention.identity` applied once at construction. Gauges use holder-backed suppliers (Tier 5 H6).

| # | Prometheus metric                         | Java accessor (`CollectorMetrics`)                        | Type       | Label keys                              |
| - | ----------------------------------------- | --------------------------------------------------------- | ---------- | --------------------------------------- |
| 1 | `collector_messages_produced_total`       | `Counter messagesProduced(exchange, symbol, stream)`      | Counter    | `exchange, symbol, stream`              |
| 2 | `collector_ws_connections_active`         | `void setWsConnectionsActive(exchange, value)` (gauge)    | Gauge      | `exchange`                              |
| 3 | `collector_ws_reconnects_total`            | `Counter wsReconnects(exchange)`                          | Counter    | `exchange`                              |
| 4 | `collector_gaps_detected_total`           | `Counter gapsDetected(exchange, symbol, stream, reason)`  | Counter    | `exchange, symbol, stream, reason`      |
| 5 | `collector_exchange_latency_ms`            | `DistributionSummary exchangeLatencyMs(exchange, symbol, stream)` | Histogram | `exchange, symbol, stream`         |
| 6 | `collector_snapshots_taken_total`          | `Counter snapshotsTaken(exchange, symbol)`                | Counter    | `exchange, symbol`                      |
| 7 | `collector_snapshots_failed_total`         | `Counter snapshotsFailed(exchange, symbol)`               | Counter    | `exchange, symbol`                      |
| 8 | `collector_messages_dropped_total`          | `Counter messagesDropped(exchange, symbol, stream)`       | Counter    | `exchange, symbol, stream`              |

Histogram buckets (Tier 5 H5 — `serviceLevelObjectives(...)`, NOT `publishPercentileHistogram(true)`):
- `collector_exchange_latency_ms`: `1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000` (mirrors `metrics.py:27-32`).

Implementation notes per writer's precedent:
- `NamingConvention.identity` set at constructor top before any meter registration.
- Counters registered via `Counter.builder("collector_messages_produced").description(...).tags(...).register(registry)` — the builder name omits `_total`; Prometheus scrape appends it automatically.
- Gauges via supplier-backed `MetricHolders` inner class (strongly-referenced — Tier 5 H6 watch-out).
- `MetricSkeletonDump` strips `_max` from the canonical output (Tier 5 H5 `_max` is Micrometer-only).

### 9.1 Metric-parity generated test

`MetricSkeletonDumpTest` runs `MetricSkeletonDump.canonicalize(...)` against a live scrape and asserts the TreeSet output matches a fixture copy of `parity-fixtures/metrics/collector.txt`. Orchestrator gate 4 diffs the harness's file output against the fixture.

---

## 10. Rule compliance

All rules from Tiers 1, 2, 3, 5. Each entry: `honored by <class/method>` or `N/A because <reason>`.

### Tier 1 — Invariants

1. **raw_text captured pre-parse from WebSocket callback** — honored by `WebSocketListenerImpl.onText/onBinary` → `FrameAccumulator.complete()` → `RawFrameCapture.onFrame(rawFrame)` → `BinanceAdapter.routeStream(rawFrame)` → `RawDataExtractor.extractDataValue(...)`. Bytes never pass through `objectMapper.readTree(...)` upstream of the substring extraction. Spike-proven byte identity (Tier 5 B4, D1). Gate 3 harness enforces it.
2. **raw_sha256 computed over raw_text bytes once at capture** — honored by `DataEnvelope.create(...)` (accepted in common); calls `Sha256.hexDigestUtf8(rawText)` exactly once. No downstream recomputation in `KafkaProducerBridge`, `GapEmitter`, or anywhere.
3. **Disabled streams emit zero artifacts** — honored by: (a) `StreamsConfig` + `BinanceExchangeConfig.getEnabledStreams()` (accepted common) returns the enabled list; (b) `WebSocketSupervisor` filters `_REST_ONLY_STREAMS` and only builds URLs for enabled; (c) `Main` only creates handlers for enabled streams; (d) `RawFrameCapture.onFrame` drops silently when `handlers.get(streamType)==null`; (e) `SnapshotScheduler`/`OpenInterestPoller` are only started when their respective streams are enabled.
4. **Kafka offsets committed after fsync** — N/A: collector is a producer, not a consumer of business data. (The one-shot `BackupChainReader` uses a consumer but never commits offsets — `enable.auto.commit=false` + never calls `commit*`).
5. **Every gap emits metric + log + archived record** — honored by the single-owner `GapEmitter`: every call does (metric++) + structured log + produce gap envelope to Kafka. `DepthStreamHandler`, `DepthSnapshotResync`, `RawFrameCapture.onDisconnect`, `KafkaProducerBridge` overflow path, `SnapshotScheduler`, `OpenInterestPoller`, `SessionSeqTracker`-callers all route through `GapEmitter`. No direct `producer.produce(gap)` bypass exists.
6. **Recovery prefers replay** — honored by `DepthSnapshotResync`: step 2 (`tryBackupChainReader`) runs BEFORE step 3 (`fetchSnapshotWithRetries`), and on success skips the REST fetch (mirrors Python `connection.py:224-238`). `DepthStreamHandler.setSyncPoint` replays buffered diffs instead of reconstructing.
7. **JSON codec preserves raw_text byte-for-byte** — honored by `EnvelopeCodec` (accepted): `@JsonPropertyOrder`, no indent, no naming strategy, `writeValueAsBytes`. `rawText` is a plain `String` field — Jackson emits it as a JSON string literal without reformatting. Gate 3 asserts it.

### Tier 2 — Java practices

8. **Java 21 only** — honored: records, sealed interfaces, `StructuredTaskScope` (+preview flag in build), virtual threads, switch expressions.
9. **No `synchronized` around blocking calls** — honored: all blocking calls (Kafka send, HttpClient send, KafkaConsumer poll, CountDownLatch await) are outside of any `synchronized`/`ReentrantLock` region. The one lock (`KafkaProducerBridge.lock`) is held only for the count check/update; `send` runs outside it (Tier 5 A5).
10. **No `Thread.sleep` in hot paths** — honored: `CountDownLatch.await(timeout)` or `LockSupport.parkNanos` used for pacing and backoffs. `ScheduledExecutorService` considered but rejected in favor of virtual-thread-friendly latches (Tier 5 A3).
11. **No reflection-heavy frameworks** — honored: no Spring/Micronaut/Quarkus/CDI/Guice. Explicit wiring in `Main.main()`. Hibernate Validator is annotation-driven bean validation only — not a DI/IoC framework.
12. **Immutable records, no setters** — honored: all contracts are records (`DataEnvelope`, `GapEnvelope`, `FrameRoute`, `PendingDiff`, `OverflowWindow`, `DepthUpdateIds`, `DiffValidationResult`, `SeqGap`, `ComponentRuntimeState`, etc.). Mutable state is scoped to per-owner-thread fields or atomic primitives.
13. **No checked exception leaks across module boundaries** — honored: `UncheckedIOException` wraps Jackson IO; `CryptoLakeConfigException` wraps config load; `LifecycleStateManager` swallows `SQLException` internally; `BackupChainReader` returns `Optional` instead of throwing.
14. **Single `HttpClient` / `KafkaProducer` / `ObjectMapper` per service** — honored: `Main` constructs exactly one of each and passes by reference to `SnapshotFetcher`, `OpenInterestPoller`, `KafkaProducerBridge`, `EnvelopeCodec`. (Tier 5 B6, C1, D3.)
15. **JSON logs via Logback + Logstash + MDC** — honored via common's `LogInit` + `StructuredLogger`. `RawFrameCapture.onFrame` wraps handler dispatch in `MDC.putCloseable("symbol", symbol)` + `MDC.putCloseable("stream", stream)` (Tier 5 H3). `Main` sets `collector_session_id` in MDC at startup.
16. **Fail-fast on invariant violation; retry only declared-transient errors** — honored: `GapReasons.requireValid` throws `IllegalArgumentException`; config validation throws `CryptoLakeConfigException`. Retries are limited to: 429 (snapshot/OI), `BufferExhaustedException` (via overflow window), WebSocket reconnect (exp backoff), 3-attempt retries in `SnapshotFetcher`/`OpenInterestPoller`/`LifecycleStateManager`. No blanket `catch Exception retry`.

### Tier 3 — Parity rules

17. **Every Python test has a JUnit 5 counterpart with trace comment** — honored via §8 (test plan): every ported test method has a `// ports: tests/unit/...::test_name` comment in its JavaDoc or source comment.
18. **Prometheus metric names + labels diff-match Python** — honored: §9 enumerates all 8 metrics with identical names + label keys. `MetricSkeletonDump` + `parity-fixtures/metrics/collector.txt` diff is gate 4.
19. **raw_text / raw_sha256 byte-identity via fixture corpus** — honored by `harness.RawTextParityHarness` (§8.2); Gradle `:collector:runRawTextParity` task is gate 3.
20. **Python `verify` CLI passes on Java archives** — N/A at collector phase: `verify` targets writer-produced archive files. Collector's output is Kafka messages — consumed by the writer (already accepted). Gate 5 runs at CLI-module phase, not here.
21. **Envelope JSON field order follows Python canonical order** — honored by accepted common's `@JsonPropertyOrder` on `DataEnvelope` and `GapEnvelope`. Collector does not redefine envelope shapes.

### Tier 5 — Translation patterns

- **A1 (fan-out → StructuredTaskScope)** — honored: `WebSocketSupervisor.start`, `SnapshotScheduler.start`, `OpenInterestPoller.start` each use `StructuredTaskScope.ShutdownOnFailure`. Backpressure callback (`BackpressureGate.onDrop`) uses `AtomicInteger` for thread-safe increment from kafka-clients IO thread.
- **A2 (run_in_executor → direct blocking)** — honored: `KafkaProducerBridge.isConnected/flush/poll` and `BackupChainReader.readLastDepthUpdateId` and `SnapshotFetcher.fetch` + `OpenInterestPoller._poll_once` all block directly on their owning virtual thread.
- **A3 (asyncio.Event → CountDownLatch)** — honored: `SnapshotScheduler`, `OpenInterestPoller`, `HeartbeatScheduler`, `WebSocketSupervisor` reconnect sleep all use `CountDownLatch.await(timeout, unit)`.
- **A4 (CancelledError → InterruptedException + restore flag)** — honored: every `catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }` pattern.
- **A5 (threading.Lock → ReentrantLock)** — honored: `KafkaProducerBridge.lock`, `DepthSnapshotResync.perSymbolLock`. Lock released before any Kafka/HTTP blocking call.
- **A6 (cancel-and-forget → raw executor submit)** — honored: `LifecycleStateManager.markCleanShutdown` is invoked inline from shutdown; no best-effort fire-and-forget executor in this module.
- **A7 (cancel_tasks helper → scope.shutdown/executor.close)** — honored: deleted. Replaced by `StructuredTaskScope.close()` + `mainExec.close()`.
- **A8 (uvloop → delete)** — honored: no analog.
- **B1 (orjson preserve order → @JsonPropertyOrder)** — honored by reused common envelopes.
- **B2 (orjson compact → Jackson no-indent)** — honored by `EnvelopeCodec.newMapper()`; `toJsonBytes` returns bytes, newline appended by caller.
- **B3 (readTree for routing; readValue for typed)** — honored: `BinanceAdapter.routeStream` uses `codec.readTree(...)`; `BackupChainReader` uses `codec.readTree(...)` then `readTree(rawText)`.
- **B4 (raw_text bytes extracted BEFORE outer parse)** — honored by `RawDataExtractor.extractDataValue` (balanced-brace slice BEFORE any `readTree` call). Gate 3 enforces.
- **B5 (BigDecimal serializer)** — N/A: no BigDecimal in collector envelopes. Binance prices stay as strings (Tier 5 E3).
- **B6 (one ObjectMapper per service)** — honored: `Main` creates one via `EnvelopeCodec.newMapper()`.
- **B7 (YAML uses same mapper family)** — N/A in collector runtime; `YamlConfigLoader` (common accepted) handles config.
- **C1 (producer config mapping)** — honored: `KafkaProducerBridge` sets `ACKS=all`, `LINGER_MS=5`, `BUFFER_MEMORY=1GB`, `MAX_IN_FLIGHT=1`, `ENABLE_IDEMPOTENCE=true`.
- **C2 (poll returns batch)** — applies only in `BackupChainReader`; honored (loops `ConsumerRecords` batch-first; `poll` returns empty, never null).
- **C3 (commitAsync)** — N/A: collector never commits offsets.
- **C4 (rebalance listener)** — N/A.
- **C5 (BufferError → BufferExhaustedException + TimeoutException)** — honored: `KafkaProducerBridge.produce` catches both.
- **C6 (watermark offsets)** — N/A.
- **C7 (strip backup prefix via substring)** — honored via `TopicNames.stripBackupPrefix` (accepted common); used by `BackupChainReader.otherDepthTopic`.
- **C8 (flush-before-commit single function)** — N/A: collector does not hold the commit pair.
- **D1 (java.net.http.WebSocket + ping via sendPing)** — honored by `WebSocketClient`/`WebSocketListenerImpl`/`FrameAccumulator`. Ping scheduled on a virtual thread at 30s interval (tradeoff: matches Python's `ping_interval=30`). Multi-frame accumulation until `last=true`.
- **D2 (async for → listener with backpressure)** — honored: `WebSocketListenerImpl.onText` calls `ws.request(1)` AFTER capture pipeline acceptance; never `Long.MAX_VALUE`.
- **D3 (one HttpClient per service)** — honored: `Main` creates one and passes to `SnapshotFetcher`, `OpenInterestPoller`.
- **D4 (raise_for_status → explicit status check)** — honored in `SnapshotFetcher` and `OpenInterestPoller`: 429 reads `Retry-After` header, non-2xx throws exception mapped to retry/log.
- **D5 (resp.text() → ofByteArray + UTF_8 decode)** — honored: `SnapshotFetcher` and `OpenInterestPoller` use `BodyHandlers.ofByteArray()` + `new String(bytes, UTF_8)`.
- **D6 (24h proactive reconnect)** — honored via `ReconnectPolicy.shouldProactivelyReconnect` using `Duration.ofHours(23).plusMinutes(50)` and `Instant`-based comparison.
- **D7 (exponential backoff with cap, no library)** — honored by `ReconnectPolicy.nextBackoffMillis`.
- **E1 (update IDs as `long`, never `int`)** — honored: `DepthUpdateIds`, `DepthGapDetector`, `SessionSeqTracker`, `BackupChainReader.readLastDepthUpdateId`, `BinanceAdapter.parseSnapshotLastUpdateId/parseDepthUpdateIds` — all `long`. `.asLong()` used throughout, never `.asInt()`.
- **E2 (received_at nanosecond epoch)** — honored via `Clocks.systemNanoClock()` (accepted common): `Math.addExact(Math.multiplyExact(seconds, 1e9), nanos)`.
- **E3 (Binance numeric strings stay as String)** — honored: collector never parses `"p"`, `"q"`, `"r"` from raw_text.
- **E4 (ns→ms integer division)** — honored: `exchangeTsMs = receivedAtNs / 1_000_000L` in `ExchangeLatencyRecorder`.
- **E5 (ns → seconds as long, not 1e9)** — honored where applicable (no conversion in this module; nanoseconds pass through).
- **F1 (Instant.now().toString() for PG ISO)** — honored by `LifecycleStateManager.registerStart` (started_at via `Instant.now().toString()`).
- **F2 (Instant parse with tz tolerance)** — honored by `LifecycleStateManager.loadActiveMaintenanceIntent` (OffsetDateTime.parse catch-fallback on LocalDateTime).
- **F3 (hourly rotation tuple)** — N/A (writer-only).
- **F4 (monotonic time with System.nanoTime)** — honored: `ReconnectPolicy` uses `Instant.now()` + `Duration.between` (OK because proactive reconnect wall-clock 24h is insensitive to NTP steps <<1h). Latency histogram uses `clock.nowNs()` (wall-clock ns) to match Python `time.time_ns()`.
- **G1 (best-effort catch Exception ignored + comment)** — honored by `LifecycleStateManager` methods, `BackupChainReader.close` finally, `HealthServer.stop`.
- **G2 (CryptoLakeConfigException for config)** — honored (accepted common provides class).
- **G3 (3-retry exp backoff inline, no library)** — honored by `SnapshotFetcher`, `OpenInterestPoller`, `LifecycleStateManager`.
- **G4 (corrupt message → emit gap + continue)** — honored: `BackupChainReader` catches `IOException`/`RuntimeException` from `codec.readTree` and continues the poll loop; `KafkaProducerBridge.produce` catches serialization failure and drops+metrics (logs `serialization_failed` per Python).
- **G5 (asyncio.TimeoutError idiom → no Java analog)** — honored (no catch; `latch.await(timeout)==false` replaces it).
- **H1 (structlog → SLF4J per class)** — honored via `private static final Logger log = LoggerFactory.getLogger(<class>.class);`.
- **H2 (event_name + KV pairs)** — honored: `log.info("ws_connected", kv("socket", name), kv("url", shortUrl))` style across all log sites.
- **H3 (MDC for symbol/stream)** — honored by `RawFrameCapture.onFrame` try-with-resources `MDC.putCloseable`.
- **H4 (Counter registered WITHOUT _total)** — honored in `CollectorMetrics` constructor: `registry.config().namingConvention(NamingConvention.identity)`; builder names omit `_total`.
- **H5 (Histogram via serviceLevelObjectives)** — honored: `exchangeLatencyMs` uses explicit SLO buckets; `MetricSkeletonDump` strips `_max` lines.
- **H6 (Gauge via supplier + holder)** — honored: `setWsConnectionsActive` uses an internal `MetricHolders` strong-referenced inner.
- **I1–I7 (file I/O, zstd, fsync, sha256)** — N/A (writer-only).
- **J1 (Pydantic → records + @JsonProperty)** — honored via accepted common config records.
- **J2 (default_factory → compact ctor default)** — honored.
- **J3 (field_validator → Hibernate Validator + compact ctor)** — honored.
- **J4 (env-var overrides `__` separator)** — honored by accepted common's `EnvOverrides` + `YamlConfigLoader`.
- **K1–K3 (picocli)** — N/A (collector is not a CLI).
- **L1 (pytest fixture → helper method)** — honored in test plan.
- **L2 (Testcontainers Kafka)** — honored for `BackupChainReaderTest` integration tests.
- **L3 (pytest-asyncio → sync on VT)** — honored: no async test framework.
- **L4 (CliRunner → CommandLine)** — N/A.
- **L5 (fixture files verbatim)** — honored: gate 3 reads `parity-fixtures/websocket-frames/` verbatim.
- **M1 (symbols lowercase internally, UPPER in URLs)** — honored: `BinanceExchangeConfig` compact constructor lowercases; `BinanceAdapter.buildSnapshotUrl/buildOpenInterestUrl` uppercases with `Locale.ROOT`.
- **M2 (stream key map)** — honored via `adapter.StreamKey`.
- **M3 (natural keys a/u)** — N/A in collector (writer-only failover concern).
- **M4 (funding rate 8-decimal format)** — N/A (CLI-only).
- **M5 (daily file sort key)** — N/A (CLI-only).
- **M6 (gap reason string set, not enum)** — honored via accepted common `GapReasons.VALID`.
- **M7 (collector_session_id format)** — honored via `CollectorSession.create` using `DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC).format(Instant.now())` (NOT `Instant.toString()`).
- **M8 (_partition int, _offset long)** — N/A (writer-side broker coordinates).
- **M9 (_offset=-1 sentinel)** — N/A (writer-side).
- **M10 (session_seq=-1 sentinel)** — honored: `DepthStreamHandler.setSyncPoint` passes `sessionSeq=-1L` for the deferred pu-chain-break gap (matches `streams/depth.py:118`).
- **M11 (hours_sealed_today reset at UTC midnight)** — N/A (writer-only).
- **M12 (topic prefix "" vs "backup.")** — honored via accepted common `TopicNames.forStream`.
- **M13 (symbol key is `symbol.getBytes(UTF_8)`)** — honored via accepted common `TopicNames.symbolKey`.
- **M14 (FileTarget tuple-identity)** — N/A.
- **M15 (late file naming)** — N/A.
- **M16 (funding rate 8h boundaries)** — N/A.

---

## 11. Open questions for developer

Each question lists a preferred approach and a fallback. The developer MUST NOT invent a different answer — if preferred path blocks, escalate.

1. **Does `java.net.http.WebSocket` expose enough of `ping_interval=30, ping_timeout=10` semantics, or do we need a manual ping scheduler?**
   - Preferred: schedule `ws.sendPing(ByteBuffer.allocate(0))` every 30s via a dedicated virtual thread started when the socket opens and cancelled on close (matches Python `ping_interval=30`). Monitor responses via `Listener.onPong` — if no pong within 10s, close the socket and let the reconnect loop take over.
   - Fallback: do NOT implement `ping_timeout` at all. Binance will disconnect a dead connection anyway and the 24h proactive reconnect catches the extreme case. Document this as a known minor deviation from Python.

2. **How many worker threads should the HeartbeatScheduler use, and what should the initial delay be?**
   - Preferred: single virtual thread; initial delay 0s; interval 30s (matches Python `_heartbeat_interval = 30.0`).
   - Fallback: if PG heartbeats become a bottleneck (they shouldn't — one UPSERT every 30s), split into read/write threads. Escalate if more than 1-2 writes per second are needed.

3. **Should `BackupChainReader.readLastDepthUpdateId` use the service's single `KafkaConsumer` or instantiate its own?**
   - Preferred: instantiate its own consumer each call (matches Python's one-shot pattern; `group.id` includes `epochMs` to avoid rebalancing interference). Short-lived, closed in finally.
   - Fallback: if Testcontainers tests show repeated consumer creation is too slow (>1s), maintain a single long-lived consumer inside `BackupChainReader` with `assign(...)` + `seekToEnd(...)` reused across calls. Requires careful thread-safety review.

4. **How does `DepthSnapshotResync.start(symbol)` interact with `StructuredTaskScope` when it's invoked from multiple triggers simultaneously (first connect + pu-chain-break)?**
   - Preferred: the per-symbol `ReentrantLock` serializes. The second caller blocks on `lock.lock()`; when the first finishes, the second observes `DepthState.Healthy` and can either skip or re-run (idempotent). Use `tryLock()` — if already held, simply return (the in-progress resync covers the new trigger).
   - Fallback: use a per-symbol `AtomicReference<DepthState>` with CAS and explicit state-transition guards; no lock. More complex; only adopt if `tryLock` proves to have a testable race.

5. **Should `RawTextParityHarness` read `depth_snapshot` and `open_interest` fixtures differently from WebSocket-framed fixtures (they don't have the outer `"data":` wrapper)?**
   - Preferred: branch on the parent directory name (`websocket-frames/depth/` vs `websocket-frames/depth_snapshot/`). For REST-origin streams (`depth_snapshot`, `open_interest`), treat the fixture raw bytes as `rawText` directly, bypassing `BinanceAdapter.routeStream`. Compare bytes and SHA-256 directly.
   - Fallback: require fixture capture to produce two file flavors — `.wsraw` (full frame) and `.restraw` (body only). More work for Python side; escalate if the simple directory-branching approach produces false negatives.

6. **How are `public_streams` / `market_streams` subsets enforced when a future Binance stream is introduced (e.g., `@continuousKline`)?**
   - Preferred: the `StreamKey` constants are the single source of truth. Adding a new stream requires adding both `_STREAM_KEY_MAP` and `_PUBLIC_STREAMS`/`_MARKET_STREAMS` entries. A unit test asserts `publicStreams()` ∩ `marketStreams() == ∅` (Tier 5 M2 assumes disjoint).
   - Fallback: none — this is an invariant in both Python and Java. Escalate if a future stream must appear on both sockets.

7. **Is `HttpClient.newBuilder()` sufficient for the `HttpClient` lifecycle, or do we need `executor(virtualThreadExecutor)`?**
   - Preferred: `HttpClient.newBuilder().version(Version.HTTP_2).connectTimeout(Duration.ofSeconds(5)).build()`. Java 21's default executor is already virtual-thread-aware for client-side I/O (design spec §1.4).
   - Fallback: explicitly pass `.executor(Executors.newVirtualThreadPerTaskExecutor())`. Adds one more close-target to shutdown plumbing.

8. **Should `MetricSkeletonDump` exercise `collector_ws_connections_active` at a non-zero value?**
   - Preferred: yes — the skeleton should include the gauge line. Python's prometheus-client emits the metric family even at 0; Micrometer does too when a holder is registered. Exercise with `metrics.setWsConnectionsActive("binance", 1)`.
   - Fallback: if Micrometer drops lazily-registered gauges that are never written, move the gauge registration to the `CollectorMetrics` constructor (matching the writer's no-label gauges pattern).

9. **Where should the `_MAX_PENDING_DIFFS=5000` constant live — `DepthStreamHandler` or `BinanceExchangeConfig.depth`?**
   - Preferred: as a `static final` in `DepthStreamHandler`, matching Python's module-level constant. Exposing it via config without a use-case is premature.
   - Fallback: move to `DepthConfig` with a `@Min(100)` validator if operators ever want to tune it. No current use-case; keep as constant.

10. **Gate 3 harness: should `RawTextParityHarness` assert canonical envelope re-serialization byte-identity as well (not just `raw_text`)?**
    - Preferred: `raw_text` bytes + `raw_sha256` only. The envelope shape is already covered by gate 4 / common's JsonPropertyOrder, and introducing a full-envelope byte comparison here duplicates gate 5's scope (verify CLI). Keep gate 3 narrowly scoped.
    - Fallback: add an optional `--full-envelope` flag that also serializes via `EnvelopeCodec.toJsonBytes(...)` and compares to the fixture `.json` exactly. Only if a future gate-3 failure would otherwise be undetected.
