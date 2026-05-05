# CryptoLake Python → Java Translation Rules (Tier 5 draft)

This document enumerates **codebase-specific mechanical translation rules** mined
from the CryptoLake Python source. Agents porting a module (Analyst, Architect,
Developer) consult this catalog **before** re-deriving micro-decisions. Each rule
names a Python pattern found in this repo at a specific `file:line`, the Java
pattern to emit, the rationale, and the failure mode a careful-but-unprimed
translator is likely to hit.

Tier placement: these rules are **Tier 5 — codebase-specific mechanics**.
Tier 1 invariants (design §1.4.1) and Tier 2/3/4 rules live in SKILL.md and
override anything here on conflict. When in doubt, quote the rule number and
escalate.

The list is intentionally broad (covers all thirteen categories) and is neither
a JVM style guide nor a design for any module. "Use a `long`, not an `int`" is
listed only when a specific Python call site would silently overflow under a
naive translation.

---

## A — Concurrency (asyncio → virtual threads / StructuredTaskScope)

### RULE A1 — `asyncio.create_task(...)` fan-out groups → `StructuredTaskScope`

**Python pattern** (`src/collector/connection.py:77-80`, `src/collector/main.py:213-250`):
```python
self._tasks = []
for socket_name, url in urls.items():
    self._tasks.append(asyncio.create_task(self._connection_loop(socket_name, url)))
await asyncio.gather(*self._tasks, return_exceptions=True)
```

**Java pattern**:
```java
try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
    for (var entry : urls.entrySet()) {
        scope.fork(() -> connectionLoop(entry.getKey(), entry.getValue()));
    }
    scope.join();          // wait for all forks
    scope.throwIfFailed(); // propagate first failure
}
```

**Rationale**: `gather(..., return_exceptions=True)` is deliberately *lenient*. The Java structural equivalent is a scope that joins all forks; match Python's leniency by choosing `ShutdownOnFailure` vs a custom policy based on whether the caller drains exceptions. Never translate `gather` into `CompletableFuture.allOf()` — that loses the join+cancel structure needed for clean shutdown.

**Watch-out**: A common failure is forgetting `scope.join()` before leaving the try-with-resources — the scope will close and cancel all subtasks mid-flight. Always `join()`.

### RULE A2 — `asyncio.get_running_loop().run_in_executor(None, fn, arg)` → direct blocking call on virtual thread

**Python pattern** (`src/writer/consumer.py:867`, `921`, `943`, `976`):
```python
msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
```

**Java pattern**:
```java
ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
```

**Rationale**: The entire reason Python uses `run_in_executor` here is that `confluent_kafka.Consumer.poll` is blocking. On a Java 21 virtual thread, blocking calls are free — no executor, no wrapper. Translating to `ExecutorService.submit(...)` recreates the thread-pool handoff this port is specifically meant to eliminate.

**Watch-out**: The whole consume loop must run *on* a virtual thread. If it runs on a platform thread, `poll()` blocks a real OS thread. Bootstrap via `Executors.newVirtualThreadPerTaskExecutor().submit(this::consumeLoop)` in `main()`.

### RULE A3 — `asyncio.Event()` pattern for interruptible sleep → `BlockingQueue.poll(timeout)` or `LockSupport.parkNanos`

**Python pattern** (`src/collector/snapshot.py:119-122`, `src/collector/streams/open_interest.py:72-76`):
```python
try:
    await asyncio.wait_for(self._stop_event.wait(), timeout=interval)
    return  # stop requested
except asyncio.TimeoutError:
    pass  # keep looping
```

**Java pattern**:
```java
if (stopLatch.await(interval, TimeUnit.SECONDS)) {
    return;  // stop was signalled
}
// otherwise timeout elapsed, keep looping
```

**Rationale**: `CountDownLatch.await(timeout)` returns `true` when released and `false` on timeout — this mirrors the Python "timeout = keep running, set = stop" contract exactly, without try/catch boilerplate or the cancel-via-exception pattern.

**Watch-out**: `CountDownLatch` can only count down once. If the service can be restarted in the same process (tests do this), use a `volatile boolean stopped` + a `Semaphore(0)` + `tryAcquire(timeout, unit)`, or use a fresh latch per lifecycle.

### RULE A4 — `asyncio.CancelledError` swallow → interrupt-handling block

**Python pattern** (`src/collector/main.py:256-261`):
```python
self._heartbeat_task.cancel()
try:
    await self._heartbeat_task
except asyncio.CancelledError:
    pass
```

**Java pattern**:
```java
heartbeatFuture.cancel(true);
try {
    heartbeatFuture.get(5, TimeUnit.SECONDS);
} catch (CancellationException | InterruptedException | ExecutionException e) {
    // expected on cancel
    Thread.currentThread().interrupt();
}
```

**Rationale**: Python's `CancelledError` is *thrown* inside the cancelled coroutine. The equivalent in Java is `InterruptedException` inside the cancelled task, plus `CancellationException` from `Future.get()` on the caller. Both must be caught, and `Thread.currentThread().interrupt()` re-raised to preserve the interrupt flag when re-thrown back into user code.

**Watch-out**: Never swallow `InterruptedException` without restoring the interrupt flag. The lint rule (Tier 2 #9) catches this.

### RULE A5 — `threading.Lock` in the collector producer → `ReentrantLock`, never `synchronized`

**Python pattern** (`src/collector/producer.py:42`, `86`):
```python
self._lock = threading.Lock()
...
with self._lock:
    current = self._buffer_counts.get(stream, 0)
    ...
```

**Java pattern**:
```java
private final ReentrantLock lock = new ReentrantLock();
lock.lock();
try {
    int current = bufferCounts.getOrDefault(stream, 0);
    ...
} finally {
    lock.unlock();
}
```

**Rationale**: This lock wraps `self._producer.produce(...)` — a blocking Kafka call. Per Tier 2 #9 `synchronized` around a blocking call pins the virtual thread. `ReentrantLock` does not.

**Watch-out**: Locked section must not include the Kafka `produce` call itself; Python releases the lock (line 98) *before* calling `produce`. Preserve that ordering in Java.

### RULE A6 — "Best-effort" cancel-and-forget → cancel + do not `join`

**Python pattern** (`src/writer/consumer.py:234-237`):
```python
try:
    loop = asyncio.get_running_loop()
    loop.create_task(self._flush_and_commit())
except RuntimeError:
    pass
```

**Java pattern**:
```java
try {
    executor.submit(this::flushAndCommit);  // fire-and-forget
} catch (RejectedExecutionException ignored) {
    // executor shutting down — best-effort already satisfied
}
```

**Rationale**: This is called from a Kafka callback where the event loop may already be shutting down. Don't build a scope or track this task; it's a best-effort side channel.

**Watch-out**: Do NOT use `StructuredTaskScope` here — it would require `join()` which defeats "best effort". This is the one place in the codebase where a raw executor submit is correct.

### RULE A7 — `cancel_tasks(list)` utility → `scope.shutdown()` or executor close

**Python pattern** (`src/common/async_utils.py:6-13`):
```python
async def cancel_tasks(tasks: list[asyncio.Task]) -> None:
    for t in tasks:
        if not t.done():
            t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    tasks.clear()
```

**Java pattern**: Delete. The java analog is `scope.shutdown()` inside a `StructuredTaskScope`, or `executor.shutdownNow(); executor.awaitTermination(...)`. Do not port the helper.

**Rationale**: This helper exists because Python task lifetime is disconnected from scope. Java's structured concurrency makes it redundant.

### RULE A8 — `uvloop.install()` → delete, no replacement

**Python pattern** (`src/common/service_runner.py:8,24`):
```python
import uvloop
uvloop.install()
```

**Java pattern**: none — virtual threads *are* the performance story.

**Rationale**: uvloop is a C-extension event loop. Its whole purpose is gone under virtual threads.

---

## B — JSON codec (orjson → Jackson; preserving raw bytes and field order)

### RULE B1 — `orjson.dumps(envelope)` preserves insertion order; Jackson must match via `@JsonPropertyOrder`

**Python pattern** (`src/common/envelope.py:42-65`, `121-122`):
```python
def serialize_envelope(envelope):
    return orjson.dumps(envelope)  # preserves dict insertion order
```

**Java pattern**:
```java
@JsonPropertyOrder({
    "v", "type", "exchange", "symbol", "stream", "received_at",
    "exchange_ts", "collector_session_id", "session_seq",
    "raw_text", "raw_sha256"
})
public record DataEnvelope(...) {}
```

**Rationale**: The envelope's canonical on-disk field order is fixed by Python dict insertion order in `create_data_envelope`. Jackson alphabetizes by default. Without `@JsonPropertyOrder`, the archive will diverge from Python byte-for-byte on every line.

**Watch-out**: The order must include ALL fields, and the gap envelope has a *different* order than the data envelope — see `create_gap_envelope`. The optional restart-metadata fields (`component`, `cause`, `planned`, `classifier`, `evidence`, `maintenance_id`) are appended last in the Python dict and must appear last in Java too. Use `@JsonInclude(NON_NULL)` for those.

### RULE B2 — `orjson.dumps` emits compact JSON with no trailing newline

**Python pattern** (`src/writer/buffer_manager.py:104`):
```python
lines = [orjson.dumps(env) + b"\n" for env in messages]
```

**Java pattern**:
```java
objectMapper
    .copy()
    .disable(SerializationFeature.INDENT_OUTPUT)
    // orjson strips whitespace between tokens by default
    .writeValueAsBytes(envelope);  // then caller appends (byte) '\n'
```

**Rationale**: orjson's default output is `{"k":"v","k2":2}` — no spaces, ASCII-only non-escape. Jackson's default is `{"k":"v","k2":2}` too; the trap is feature flags that insert space after `:` or `,` via a custom `DefaultPrettyPrinter`. Leave Jackson at defaults and explicitly append `0x0A` for the newline.

**Watch-out**: Never use `writeValue(writer, env)` followed by `writer.write('\n')` through a Writer — you'll introduce CRLF on Windows. Work in bytes.

### RULE B3 — `orjson.loads(raw_bytes)` → `ObjectMapper.readTree(bytes)` for raw frames; typed `readValue` for envelopes

**Python pattern** (`src/writer/consumer.py:728`, `src/exchanges/binance.py:70-71`):
```python
envelope = deserialize_envelope(raw_value)        # dict
parsed = orjson.loads(raw_frame)                  # dict for routing only
```

**Java pattern**:
```java
// envelope: typed record
EnvelopeRecord env = objectMapper.readValue(rawValue, EnvelopeRecord.class);
// raw frame (short-lived, routing only): JsonNode
JsonNode parsed = objectMapper.readTree(rawFrame);
String streamKey = parsed.get("stream").asText();
```

**Rationale**: Writers deserializing from Kafka need every field (typed record). The collector routing code only inspects the `"stream"` field and hands `raw_text` through opaque — `JsonNode` is fine and allocation-cheap. Match Python's cost profile.

**Watch-out**: Never parse the envelope into a `Map<String, Object>`. That loses type info, silently widens `int` to `Integer`, and defeats `@JsonPropertyOrder` on reserialize.

### RULE B4 — `raw_text` must be extracted as bytes BEFORE the outer parse

**Python pattern** (`src/exchanges/binance.py:62-74`):
```python
# Locate the "data": key and extract the value via balanced brace counting
data_key = '"data":'
data_idx = raw_frame.index(data_key)
raw_data_text = _extract_data_value(raw_frame, data_start)
parsed = orjson.loads(raw_frame)  # only for routing
```

**Java pattern**: Port `_extract_data_value` as a byte-level scanner that walks the WebSocket frame bytes and returns a `ByteBuffer` slice covering the inner object. Do NOT `objectMapper.writeValueAsString(node.get("data"))` — Jackson will re-serialize with its own formatting, breaking byte identity.

**Rationale**: This is a Tier 1 invariant (`raw_text` byte-identity). The extractor preserves exact bytes through balanced-brace counting; any parse-and-reserialize round trip violates gate 3.

**Watch-out**: `java.net.http.WebSocket.Listener.onText` gives you a `CharSequence`; you need to hold the full `ByteBuffer` from `onBinary` or know the exact UTF-8 byte-for-byte serialization of the text frame. Binance sends text frames, so decode via `StandardCharsets.UTF_8` consistently *on both sides* of the comparison.

### RULE B5 — `orjson.dumps` defaults → register custom serializer for `BigDecimal` (none currently needed)

**Python pattern**: envelopes never contain a `Decimal` — numeric fields are `int` or strings from Binance.

**Java pattern**: If a future envelope introduces `BigDecimal`, register
```java
objectMapper.disable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
objectMapper.enable(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN);
```
(the second prevents `"1E+1"` scientific notation that orjson never emits).

**Rationale**: orjson always emits decimal strings in plain form; Jackson's scientific notation default would break byte identity on the rare path where a `BigDecimal` is serialized.

**Watch-out**: None in the current corpus — flag if you encounter `Decimal`/`BigDecimal` fields.

### RULE B6 — One `ObjectMapper` per service, never per call

**Python pattern**: `orjson` has no mapper concept — stateless C calls.

**Java pattern**: Single `ObjectMapper` field in `Main` wiring, passed to every component. Tier 2 rule 14 makes this explicit. Instantiation is expensive; per-call allocation would swamp a 100k-message/s Kafka consumer.

### RULE B7 — YAML config loader uses the same mapper family (not PyYAML drop-in)

**Python pattern** (`src/common/config.py:163`):
```python
data = yaml.safe_load(path.read_text()) or {}
```

**Java pattern**:
```java
ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
AppConfig cfg = yamlMapper.readValue(path.toFile(), AppConfig.class);
```

**Rationale**: `jackson-dataformat-yaml` reuses Jackson databind's typing machinery, so the same `@JsonProperty` alias map works for both YAML config and JSON envelopes.

---

## C — Kafka (confluent-kafka → kafka-clients)

### RULE C1 — Producer config `acks=all, linger.ms=5, queue.buffering.max.*` → map to kafka-clients properties

**Python pattern** (`src/collector/producer.py:47-53`):
```python
self._producer = KafkaProducer({
    "bootstrap.servers": ",".join(brokers),
    "acks": "all",
    "linger.ms": 5,
    "queue.buffering.max.messages": max_buffer,
    "queue.buffering.max.kbytes": 1048576,
})
```

**Java pattern**:
```java
Properties p = new Properties();
p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", brokers));
p.put(ProducerConfig.ACKS_CONFIG, "all");
p.put(ProducerConfig.LINGER_MS_CONFIG, 5);
p.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_073_741_824L);  // 1 GB
// NOTE: kafka-clients has no direct "max.messages" — use in-flight + batch sizes
p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);  // preserve ordering
p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```

**Rationale**: librdkafka ("queue.buffering.max.messages") and kafka-clients ("buffer.memory" in bytes) gate overflow differently. Preserve the *behavior* — back-pressure when buffer fills — not the raw config name. `max.in.flight=1` + idempotence is the standard kafka-clients equivalent of librdkafka's `enable.idempotence=true` default.

**Watch-out**: kafka-clients `BufferExhaustedException` is the equivalent of librdkafka `BufferError`. Per-stream caps (`buffer_caps`) have no direct kafka-clients analog — implement the cap in a wrapper as the Python code does.

### RULE C2 — `consumer.poll(1.0)` returns a single `Message` in confluent-kafka → a batch in kafka-clients

**Python pattern** (`src/writer/consumer.py:867`):
```python
msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
if msg is None: ...
```

**Java pattern**:
```java
ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
for (ConsumerRecord<byte[], byte[]> rec : records) { ... }
```

**Rationale**: kafka-clients is batch-first. The whole inner loop structure flips: Python poll→handle-one→poll, Java poll→for-each→poll. Commit ownership (Tier 1 #4) still runs after the batch flushes, not per record.

**Watch-out**: `consumer.poll` returns an empty `ConsumerRecords`, never `null`. Translating a `msg is None` check to `records == null` is a bug.

### RULE C3 — `enable.auto.commit=false` + manual `commit(offsets=..., asynchronous=True)` → `commitAsync(offsets, callback)`

**Python pattern** (`src/writer/consumer.py:1347-1348`):
```python
if offsets_to_commit:
    self._consumer.commit(offsets=offsets_to_commit, asynchronous=True)
```

**Java pattern**:
```java
Map<TopicPartition, OffsetAndMetadata> offsets = ...;
consumer.commitAsync(offsets, (committed, err) -> {
    if (err != null) {
        log.error("kafka_commit_failed", err);
        metrics.kafkaCommitFailures.increment();
    }
});
```

**Rationale**: Commit callback behavior is identical; name just differs. Keep the metric name `writer_kafka_commit_failures_total` exactly (Tier 3 rule 18).

**Watch-out**: kafka-clients `commitAsync` may not have fired by shutdown. The stop path must `commitSync()` remaining offsets after the final flush — Python hides this in librdkafka's `consumer.close()`. Add an explicit `commitSync` before `close()` in Java.

### RULE C4 — `on_assign` / `on_revoke` callbacks with manual seek → `ConsumerRebalanceListener`

**Python pattern** (`src/writer/consumer.py:205-248`):
```python
def _on_assign(consumer, partitions):
    self._assigned = True
    for tp in partitions:
        if key in self._pending_seeks:
            tp.offset = self._pending_seeks[key]
self._consumer.subscribe(self.topics, on_assign=_on_assign, on_revoke=_on_revoke)
```

**Java pattern**:
```java
consumer.subscribe(topics, new ConsumerRebalanceListener() {
    @Override public void onPartitionsAssigned(Collection<TopicPartition> parts) {
        for (TopicPartition tp : parts) {
            Long seek = pendingSeeks.get(tp);
            if (seek != null) consumer.seek(tp, seek);
        }
    }
    @Override public void onPartitionsRevoked(Collection<TopicPartition> parts) {
        // flush before losing ownership
        executor.submit(this::flushAndCommit);
    }
});
```

**Rationale**: In kafka-clients you `seek()` *inside* the listener after assignment, not by mutating the `TopicPartition` argument. librdkafka expects the mutated list; kafka-clients reads it as an immutable collection.

**Watch-out**: The Python `_on_revoke` does `raise RuntimeError` when revocation is spontaneous — this becomes a fatal exit. In Java, throwing from the listener aborts the poll. Use a `volatile boolean fatalError` and check it at the top of the poll loop instead.

### RULE C5 — librdkafka `BufferError` → kafka-clients `BufferExhaustedException`

**Python pattern** (`src/collector/producer.py:119-127`):
```python
try:
    self._producer.produce(topic=topic, key=key, value=value, on_delivery=cb)
except BufferError:
    # overflow path
```

**Java pattern**:
```java
try {
    producer.send(new ProducerRecord<>(topic, key, value), cb);
} catch (BufferExhaustedException e) {
    // overflow path
}
```

**Rationale**: Both are synchronous throws on buffer-full. The callback decrement pattern (line 139-148) is identical shape.

**Watch-out**: kafka-clients also throws `TimeoutException` when `max.block.ms` fires; map that to the same overflow handler to match Python's "drop and record" behavior.

### RULE C6 — `get_watermark_offsets(tp)` for consumer lag → `endOffsets(Collection<TopicPartition>)`

**Python pattern** (`src/writer/consumer.py:1382-1383`):
```python
_, high = self._consumer.get_watermark_offsets(tp)
positions = self._consumer.position([TopicPartition(tp.topic, tp.partition)])
```

**Java pattern**:
```java
Map<TopicPartition, Long> ends = consumer.endOffsets(Collections.singleton(tp));
long position = consumer.position(tp);
long lag = Math.max(0, ends.get(tp) - position);
```

**Rationale**: Same math, different method names. `endOffsets` is a blocking call — on a virtual thread, no wrapping needed.

### RULE C7 — Stripping topic prefix for backup records → deterministic string suffix

**Python pattern** (`src/writer/consumer.py:764-768`):
```python
if msg_topic.startswith(self._failover._backup_prefix):
    primary_topic = msg_topic[len(self._failover._backup_prefix):]
```

**Java pattern**:
```java
String primaryTopic = msgTopic.startsWith(backupPrefix)
    ? msgTopic.substring(backupPrefix.length())
    : msgTopic;
```

**Rationale**: Trivial but load-bearing: this is the sole mechanism that de-duplicates backup-vs-primary records in a single archive file. A `replace(backupPrefix, "")` would wrongly strip every occurrence of the substring.

### RULE C8 — Flush-before-commit ordering is a *single function*

**Python pattern** (`src/writer/consumer.py:1287-1348`, `_commit_state`):
```python
# 1. PG state save  (atomic)
await self.state_manager.save_states_and_checkpoints(...)
# 2. in-memory checkpoint cache update
# 3. Kafka commitAsync
self._consumer.commit(offsets=..., asynchronous=True)
```

**Java pattern**: This function must port as a **single method** with exactly this linear sequence — do NOT split into `StateManager.save(...)` + `Consumer.commit(...)` called from different places. Tier 1 #4 mandates a single owner.

**Rationale**: A gate 7 (architect sign-off) test specifically checks this. Splitting introduces race windows where a partial flush-commit leaves the archive ahead of the offsets or vice versa — causing data loss or duplicates on restart.

**Watch-out**: The Python code also rolls back on PG failure (`return` without committing offsets). Java must do the same; a `try/catch` that logs-and-continues is a data-loss bug.

---

## D — WebSocket & HTTP (websockets + aiohttp → java.net.http)

### RULE D1 — `websockets.connect(url, ping_interval=30, ping_timeout=10)` → `HttpClient.newWebSocketBuilder().buildAsync(...)`

**Python pattern** (`src/collector/connection.py:107-108`):
```python
async with websockets.connect(url, ping_interval=30, ping_timeout=10, close_timeout=5) as ws:
```

**Java pattern**:
```java
WebSocket ws = httpClient.newWebSocketBuilder()
    .connectTimeout(Duration.ofSeconds(5))
    .buildAsync(URI.create(url), listener)
    .get(30, TimeUnit.SECONDS);
```

**Rationale**: `java.net.http.WebSocket` does NOT expose `ping_interval` / `ping_timeout` natively — Binance handles heartbeats at the protocol layer; you must implement ping via `ws.sendPing(ByteBuffer.allocate(0))` on a scheduled virtual thread.

**Watch-out**: Gate 3 parity depends on the exact bytes of each received frame. `Listener.onText(CharSequence, boolean last)` is called per-fragment — you must concatenate fragments until `last==true` before treating the frame as complete. Binance sends single-fragment text frames, but never assume.

### RULE D2 — `async for raw_frame in ws:` → `Listener.onText` callbacks with backpressure

**Python pattern** (`src/collector/connection.py:149`):
```python
async for raw_frame in ws:
    ...
```

**Java pattern**:
```java
class BinanceListener implements WebSocket.Listener {
    @Override public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
        // append to buffer, if last: enqueue onto frame queue (blocking put — backpressure)
        frameQueue.put(new Frame(buffer.toString()));
        ws.request(1);  // one-at-a-time pacing
        return null;
    }
}
```

**Rationale**: The `async for` loop naturally serializes and back-pressures. In Java, `Listener.onText` returns a `CompletionStage`; the WebSocket won't deliver the next frame until that stage completes. Use `ws.request(1)` for 1-at-a-time pacing, matching Python's behavior.

**Watch-out**: If you `ws.request(Long.MAX_VALUE)` up front, you get Python's `websockets` default behavior — unbounded queue growth under load. Always gate with `request(1)` per consumed frame.

### RULE D3 — `aiohttp.ClientSession()` per request vs per service

**Python pattern**: mixed — `src/collector/connection.py:244` creates a session per resync call; `src/collector/snapshot.py:62` holds one for the scheduler lifetime.

**Java pattern**: **One `HttpClient` per service**, shared across all HTTP callers (Tier 2 rule 14). `HttpClient` is thread-safe and connection-pooling; there is never a reason to create a per-call client.

**Rationale**: The Python inconsistency is incidental. Java's `HttpClient` is heavy (holds a connection pool, TLS session cache), so a single instance is the correct choice.

### RULE D4 — `resp.raise_for_status()` → check `HttpResponse.statusCode()` explicitly

**Python pattern** (`src/collector/snapshot.py:89-93`):
```python
if resp.status == 429:
    retry_after = int(resp.headers.get("Retry-After", 5))
    await asyncio.sleep(retry_after)
    continue
resp.raise_for_status()
```

**Java pattern**:
```java
HttpResponse<String> resp = httpClient.send(req, BodyHandlers.ofString());
if (resp.statusCode() == 429) {
    int retryAfter = resp.headers().firstValueAsLong("Retry-After").orElse(5L);
    Thread.sleep(Duration.ofSeconds(retryAfter));
    continue;
}
if (resp.statusCode() >= 400) {
    throw new HttpException(resp.statusCode(), resp.body());
}
```

**Rationale**: Java's `HttpClient` does NOT throw on 4xx/5xx — you must check explicitly. Translating "`raise_for_status`" to "send + assume 200" is a silent-swallow bug.

### RULE D5 — `resp.text()` / `resp.json()` and `raw_text` byte-identity

**Python pattern** (`src/collector/snapshot.py:94`):
```python
raw_text = await resp.text()
```

**Java pattern**:
```java
HttpResponse<byte[]> resp = httpClient.send(req, BodyHandlers.ofByteArray());
String rawText = new String(resp.body(), StandardCharsets.UTF_8);
```

**Rationale**: aiohttp's `resp.text()` respects the Content-Type charset header — for Binance this is always UTF-8. Java's `BodyHandlers.ofString()` also uses Content-Type, but not consistently across versions; use `ofByteArray()` + explicit UTF-8 decode so the result is deterministic.

### RULE D6 — 24-hour proactive reconnect is a wall-clock timer, not a counter

**Python pattern** (`src/collector/connection.py:23`, `152-156`):
```python
_RECONNECT_BEFORE_24H = 23 * 3600 + 50 * 60  # 23h50m
if time.monotonic() - connect_time > _RECONNECT_BEFORE_24H:
    await ws.close()
    return
```

**Java pattern**:
```java
private static final Duration RECONNECT_AFTER = Duration.ofHours(23).plusMinutes(50);
if (Duration.between(connectTime, Instant.now()).compareTo(RECONNECT_AFTER) > 0) {
    ws.sendClose(WebSocket.NORMAL_CLOSURE, "proactive").join();
    return;
}
```

**Rationale**: Binance force-disconnects WebSocket connections at 24h. The reconnect must happen pre-emptively to avoid a gap. Use monotonic time (`Instant.now()` with `Clock.systemUTC()` is fine on JVM); don't use scheduled tasks — they race with frame delivery.

### RULE D7 — Exponential reconnect backoff with cap

**Python pattern** (`src/collector/connection.py:100`, `145-146`):
```python
backoff = 1
...
await asyncio.sleep(min(backoff, _MAX_BACKOFF))
backoff = min(backoff * 2, _MAX_BACKOFF)
```

**Java pattern**: Use `Math.min(backoff, MAX_BACKOFF_SEC)` + `TimeUnit.SECONDS.sleep(...)` on a virtual thread. Do NOT use a third-party retry library (`failsafe`, `resilience4j`) — the Python policy is 5 lines; it stays 5 lines.

**Rationale**: Framework-less. One less dependency, one less thing to audit for virtual-thread compatibility.

---

## E — Numerics & precision

### RULE E1 — Binance `lastUpdateId`, `U`, `u`, `pu`, aggregate trade `a` → `long`, never `int`

**Python pattern** (`src/exchanges/binance.py:151-157`, `src/writer/failover.py:16-20`):
```python
def parse_snapshot_last_update_id(self, raw_text: str) -> int:
    parsed = orjson.loads(raw_text)
    return parsed["lastUpdateId"]
```

**Java pattern**:
```java
public long parseSnapshotLastUpdateId(String rawText) throws IOException {
    JsonNode node = objectMapper.readTree(rawText);
    return node.get("lastUpdateId").asLong();   // NEVER .asInt()
}
```

**Rationale**: Binance `lastUpdateId`/`U`/`u`/`pu` regularly exceed 2^31 (~2.1 B). Python's unbounded `int` hides this. An `asInt()` call overflows silently, the pu-chain validator then fails with a nonsense error days after deployment.

**Watch-out**: The entire failover `extract_natural_key` + depth `validate_diff` chain must use `long` end-to-end. One `int` cast breaks gate 3.

### RULE E2 — `received_at` / `exchange_ts` nanoseconds → `long`

**Python pattern** (`src/common/envelope.py:59`):
```python
"received_at": time.time_ns()
```

**Java pattern**:
```java
public static long nowNs() {
    Instant n = Instant.now();
    return Math.addExact(
        Math.multiplyExact(n.getEpochSecond(), 1_000_000_000L),
        n.getNano());
}
```

**Rationale**: `Instant.toEpochMilli()` truncates; `System.currentTimeMillis() * 1_000_000` loses nanosecond precision. Use `Instant.now().getEpochSecond() * 1e9 + getNano()` for full nanosecond wall-clock.

**Watch-out**: `Instant` resolution is platform-dependent (usually microseconds on Linux, milliseconds on Windows). The value still fits `long` but the last 3–6 digits may be zeros. This is acceptable — matches Python `time.time_ns()` platform resolution.

### RULE E3 — Binance numeric string fields (`p`, `q`, `r`) stay as `String`, not `BigDecimal`

**Python pattern**: Binance always sends prices/quantities as strings (`"65432.10"`). Python keeps them as `str` in the envelope.

**Java pattern**: Declare as `String` in envelope records. Do NOT convert to `BigDecimal` on the ingest path — Tier 1 #7 requires byte-identical round-trip. Parsing + reserializing `"0.00100000"` through `BigDecimal` gives `"0.001"` or `"1E-3"`.

**Rationale**: The collector does not do any arithmetic on these; the writer archives them; the consumer of archived data decides how to parse. Preserving the string is the only byte-safe choice.

### RULE E4 — `int(received_at / 1_000_000)` ns→ms convert → integer division, not float divide

**Python pattern** (`src/collector/snapshot.py:141`):
```python
exchange_ts = int(received_at / 1_000_000)
```

**Java pattern**:
```java
long exchangeTsMs = receivedAtNs / 1_000_000L;  // integer division
```

**Rationale**: Python's `/` returns float; `int(...)` truncates toward zero. Java's `long / long` is already integer division. A translator copying the form "`(long)(receivedAtNs / 1_000_000.0)`" introduces FP rounding error near 2^53.

### RULE E5 — `time.time_ns() / 1_000_000_000` for seconds → `/ 1_000_000_000L`, not `1e9`

**Python pattern** (`src/writer/buffer_manager.py:124`):
```python
dt = datetime.datetime.fromtimestamp(received_at_ns / 1_000_000_000, tz=datetime.timezone.utc)
```

**Java pattern**:
```java
Instant inst = Instant.ofEpochSecond(
    receivedAtNs / 1_000_000_000L,
    receivedAtNs % 1_000_000_000L);
```

**Rationale**: `Instant.ofEpochSecond(seconds, nanos)` preserves precision — `Instant.ofEpochMilli(receivedAtNs / 1_000_000L)` loses sub-ms digits.

---

## F — Timestamps & time

### RULE F1 — `datetime.now(timezone.utc).isoformat()` for PG storage → `Instant.now().toString()`

**Python pattern** (`src/collector/main.py:114`, `src/writer/main.py:75-77`):
```python
now = datetime.now(timezone.utc).isoformat()
```

**Java pattern**:
```java
String iso = Instant.now().toString();  // "2026-04-18T12:34:56.123Z"
```

**Rationale**: `Instant.toString()` emits ISO-8601 UTC with `Z` suffix, matching Python's `isoformat()` for UTC-aware datetimes. Do NOT use `LocalDateTime` — it drops timezone info and breaks PG `TIMESTAMPTZ` round-trip.

**Watch-out**: Python's `isoformat()` emits `+00:00` for tz-aware datetimes, `Z` only for `UTC`. PG `TIMESTAMPTZ` round-trip reads these back as `2026-04-18T12:34:56.123+00:00`. The `restart_gap_classifier._is_intent_valid` function specifically handles both forms by stripping tzinfo — preserve that tolerance in Java.

### RULE F2 — `datetime.datetime.fromisoformat(...)` with tz handling → `Instant.parse(...)` or `OffsetDateTime`

**Python pattern** (`src/writer/consumer.py:490-492`, `src/writer/restart_gap_classifier.py:29-33`):
```python
cp_dt = datetime.datetime.fromisoformat(checkpoint.last_received_at)
if cp_dt.tzinfo is None:
    cp_dt = cp_dt.replace(tzinfo=datetime.timezone.utc)
```

**Java pattern**:
```java
Instant ts;
try {
    ts = OffsetDateTime.parse(raw).toInstant();
} catch (DateTimeParseException e) {
    // naive timestamp — interpret as UTC
    ts = LocalDateTime.parse(raw).toInstant(ZoneOffset.UTC);
}
```

**Rationale**: PG `TIMESTAMPTZ` round-trip may return `+00:00` suffix, `+0000`, or no offset depending on driver. Parse both forms.

**Watch-out**: `Instant.parse` only accepts the `Z` suffix form. `OffsetDateTime.parse` accepts both. Use the latter.

### RULE F3 — hourly-rotation tuple `(hour, date)` from nanosecond timestamp

**Python pattern** (`src/writer/consumer.py:835-838`):
```python
msg_dt = datetime.datetime.fromtimestamp(
    envelope["received_at"] / 1_000_000_000, tz=datetime.timezone.utc)
current_hour = msg_dt.hour
current_date = msg_dt.strftime("%Y-%m-%d")
```

**Java pattern**:
```java
Instant inst = Instant.ofEpochSecond(
    receivedAtNs / 1_000_000_000L, receivedAtNs % 1_000_000_000L);
ZonedDateTime zdt = inst.atZone(ZoneOffset.UTC);
int hour = zdt.getHour();
String date = DateTimeFormatter.ISO_LOCAL_DATE.format(zdt);
```

**Rationale**: Archive directory structure (`{exchange}/{symbol}/{stream}/{YYYY-MM-DD}/hour-{H}.jsonl.zst`) is a public interface (verify CLI depends on it). A mistake here breaks gate 5.

**Watch-out**: Never use `DateTimeFormatter.ofPattern("yyyy-MM-dd")` without `.withZone(ZoneOffset.UTC)` on an `Instant` — throws `UnsupportedTemporalTypeException`. `ISO_LOCAL_DATE` handles this correctly when applied to a `ZonedDateTime`.

### RULE F4 — `time.monotonic()` for timeouts → `System.nanoTime()` arithmetic

**Python pattern** (`src/writer/consumer.py:860`, `866`):
```python
last_flush_time = time.monotonic()
...
if time.monotonic() - last_flush_time >= self.buffer_manager.flush_interval_seconds:
```

**Java pattern**:
```java
long lastFlushNanos = System.nanoTime();
...
if (System.nanoTime() - lastFlushNanos >= TimeUnit.SECONDS.toNanos(flushIntervalSeconds)) {
```

**Rationale**: `System.nanoTime()` is monotonic like Python's `time.monotonic()`; `Instant.now()` is wall-clock and susceptible to NTP steps. Never mix the two in timeouts.

---

## G — Exceptions & error handling

### RULE G1 — Naked `except Exception: pass` best-effort cleanup → `try/catch (Exception ignored) { }` with a comment

**Python pattern** (`src/collector/main.py:192-195`, `src/writer/consumer.py:1032-1033`):
```python
try:
    await self._state_manager.close()
except Exception:
    pass
```

**Java pattern**:
```java
try {
    stateManager.close();
} catch (Exception ignored) {
    // best-effort shutdown; never block main shutdown path
}
```

**Rationale**: This is a deliberate "shut up and continue" during shutdown or callbacks where no one can handle the error. Suppressing the warning is fine *with a comment explaining why*. Tier 2 #13 bans checked-exception leakage — shutdown paths must swallow.

**Watch-out**: Do NOT catch `Throwable` — that hides `OutOfMemoryError` and `StackOverflowError`, which must propagate.

### RULE G2 — Custom `ConfigValidationError` → `CryptoLakeConfigException extends RuntimeException`

**Python pattern** (`src/common/config.py:11-12`):
```python
class ConfigValidationError(Exception):
    """Raised when configuration data fails validation."""
```

**Java pattern**:
```java
public final class CryptoLakeConfigException extends RuntimeException {
    public CryptoLakeConfigException(String msg, Throwable cause) { super(msg, cause); }
}
```

**Rationale**: Tier 2 #13 — no checked exceptions across module boundaries. Config validation happens in `main()` bootstrap; a fatal `RuntimeException` is correct.

### RULE G3 — Three-retry exponential backoff pattern with re-raise

**Python pattern** (`src/writer/state_manager.py:283-299`):
```python
async def _retry_transaction(self, operation, log_label: str = "pg_save_failed") -> None:
    max_retries = 3
    for attempt in range(max_retries):
        try:
            ...; return
        except Exception as e:
            logger.warning(log_label, attempt=attempt + 1, error=str(e))
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
```

**Java pattern**: Inline. Do NOT extract a retry library; 10 lines of Java with `Thread.sleep`, a loop, and a `try/catch`. Verbose-but-readable beats a 3rd-party dependency for this.

**Rationale**: Consistent with the Tier 2 "no framework" rule.

### RULE G4 — Corrupt message path emits a gap envelope, does not throw

**Python pattern** (`src/writer/consumer.py:727-749`):
```python
try:
    envelope = deserialize_envelope(raw_value)
except Exception:
    logger.error("corrupt_message_skipped", ...)
    gap = create_gap_envelope(..., reason="deserialization_error", ...)
    ...
    return None
```

**Java pattern**: Catch `JsonProcessingException`, log, emit a `deserialization_error` gap, continue. **NEVER** re-throw — a single malformed Kafka record must not kill the consumer loop.

**Watch-out**: Jackson throws `JsonProcessingException` (checked) and `JsonParseException` (unchecked). Catch the checked parent `IOException` to cover both plus any I/O failure reading the message.

### RULE G5 — `asyncio.TimeoutError` in `wait_for` idiom → do not translate — it's part of the event loop pattern

See RULE A3. The Python `asyncio.TimeoutError` caught around `asyncio.wait_for` is an idiom for "sleep or exit early" — the Java translation has no exception, so no catch.

---

## H — Logging & observability

### RULE H1 — `structlog.get_logger()` at module level → SLF4J `Logger` per class

**Python pattern** (`src/writer/consumer.py:36`):
```python
logger = structlog.get_logger()
```

**Java pattern**:
```java
private static final Logger log = LoggerFactory.getLogger(WriterConsumer.class);
```

**Rationale**: Per-class named logger is the SLF4J convention and lets Logstash encoder emit a `logger_name` field equivalent to structlog's `logger` field.

### RULE H2 — `logger.info("event_name", key=val, key2=val2)` → MDC-scoped SLF4J with KV pairs

**Python pattern** (`src/writer/consumer.py:165-177`):
```python
logger.info("recovery_state_loaded",
    checkpoints=len(self._durable_checkpoints),
    previous_boot_id=...,
    current_boot_id=self._current_boot_id,
)
```

**Java pattern** (using `net.logstash.logback` structured args):
```java
log.info("recovery_state_loaded",
    kv("checkpoints", durableCheckpoints.size()),
    kv("previous_boot_id", previousBootId),
    kv("current_boot_id", currentBootId));
```
(`kv` = `StructuredArguments.keyValue` from Logstash encoder)

**Rationale**: The event-name-then-structured-KV style is structlog's canonical form. Logstash encoder's `StructuredArguments` is the byte-equivalent for emitting JSON with those fields as top-level keys.

**Watch-out**: `log.info("recovery_state_loaded checkpoints={}", ...)` uses SLF4J's positional formatter — this makes the event name a substring of a free-form message, breaking log-analysis queries that search on `event == "recovery_state_loaded"`.

### RULE H3 — `logger.bind(symbol=s)` / `structlog.contextvars.merge_contextvars` → MDC `try-with-resources`

**Python pattern**: The codebase does not heavily use `bind`, but structlog's contextvars merge processor is configured (`src/common/logging.py:15`).

**Java pattern**:
```java
try (MDCCloseable c = MDC.putCloseable("symbol", symbol);
     MDCCloseable c2 = MDC.putCloseable("stream", streamType)) {
    handler.handle(symbol, rawText, exchangeTs, seq);
}
```

**Rationale**: Tier 2 #15 mandates JSON logs + MDC. MDC keys appear as top-level JSON fields through the Logstash encoder.

### RULE H4 — Prometheus `Counter("name_total", "desc", ["label1"])` → Micrometer `Counter.builder("name_total")`

**Python pattern** (`src/writer/metrics.py:3-7`):
```python
messages_consumed_total = Counter(
    "writer_messages_consumed_total",
    "Messages read from Redpanda",
    ["exchange", "symbol", "stream"],
)
```

**Java pattern**:
```java
// register on PrometheusMeterRegistry
public static Counter messagesConsumed(String exchange, String symbol, String stream) {
    return Counter.builder("writer_messages_consumed_total")
        .description("Messages read from Redpanda")
        .tags("exchange", exchange, "symbol", symbol, "stream", stream)
        .register(registry);
}
```

**Rationale**: Metric name + label set must diff-match (Tier 3 rule 18). Micrometer's Prometheus registry preserves names verbatim **if** no naming convention is applied (disable the default snake_case conventionalizer).

**Watch-out**: Micrometer's default `PrometheusNamingConvention` appends `_total` to counters automatically. If your metric name already ends in `_total` (as this one does), you'll get `_total_total` in the output unless you call `meterRegistry.config().namingConvention(NamingConvention.identity);`.

### RULE H5 — Prometheus `Histogram(buckets=[1,5,10,...])` → Micrometer `DistributionSummary.publishPercentileHistogram(false).serviceLevelObjectives(...)`

**Python pattern** (`src/writer/metrics.py:55-60`):
```python
flush_duration_ms = Histogram(
    "writer_flush_duration_ms",
    "Time to flush buffer to disk (ms)",
    ["exchange", "symbol", "stream"],
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
)
```

**Java pattern**:
```java
DistributionSummary.builder("writer_flush_duration_ms")
    .baseUnit("milliseconds")
    .serviceLevelObjectives(1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000)
    .tags("exchange", exchange, "symbol", symbol, "stream", stream)
    .register(registry);
```

**Rationale**: `serviceLevelObjectives(double... values)` is Micrometer's explicit-bucket API. Do NOT use `.publishPercentileHistogram(true)` — that picks buckets for you and breaks parity with Python's fixed bucket set.

### RULE H6 — Gauge `.set(value)` → Micrometer `Gauge` with a supplier

**Python pattern** (`src/writer/consumer.py:1371-1372`):
```python
writer_metrics.disk_usage_bytes.set(usage.used)
writer_metrics.disk_usage_pct.set(usage.used / usage.total * 100)
```

**Java pattern**:
```java
// on startup, register once:
Gauge.builder("writer_disk_usage_bytes", () -> diskUsageHolder.bytes).register(registry);
// to update, mutate the backing holder:
diskUsageHolder.bytes = usage.used();
```

**Rationale**: Micrometer gauges are pulled, not pushed — opposite of prometheus-client's `.set(...)`. The translation is a holder object whose value the supplier reads.

**Watch-out**: Do NOT hold a strong reference to a short-lived object; Micrometer uses a weak reference and the gauge will report `NaN` after GC. Keep the holder alive for the service lifetime.

---

## I — File I/O, compression, durability

### RULE I1 — `zstandard.ZstdCompressor(level=3).compress(data)` framed output → zstd-jni `Zstd.compress(data, level)`

**Python pattern** (`src/writer/compressor.py:14-21`):
```python
self._cctx = zstd.ZstdCompressor(level=level)
def compress_frame(self, lines):
    data = b"".join(lines)
    return self._cctx.compress(data)  # one independent frame
```

**Java pattern**:
```java
public byte[] compressFrame(List<byte[]> lines) {
    int total = lines.stream().mapToInt(b -> b.length).sum();
    byte[] joined = new byte[total];
    int off = 0;
    for (byte[] line : lines) { System.arraycopy(line, 0, joined, off, line.length); off += line.length; }
    return Zstd.compress(joined, level);
}
```

**Rationale**: `Zstd.compress(src, level)` produces a single independent zstd frame. Concatenation of frames is a valid zstd stream (zstd spec), exactly matching Python `zstandard.ZstdCompressor.compress(...)`. The crash-safety property (truncate at any frame boundary on recovery) depends on this behavior — never use `ZstdOutputStream` for per-batch compression; that produces a single frame for the whole file.

**Watch-out**: zstd-jni is a JNI library. It does NOT pin virtual threads (Tier 2 rule 9), but verify that in CI — a regression in zstd-jni could silently reintroduce pinning.

### RULE I2 — Streaming decompress with `zstd.ZstdDecompressor().stream_reader(f)` → `ZstdInputStream` wrapping a `FileInputStream`

**Python pattern** (`src/cli/consolidate.py:93-96`):
```python
dctx = zstd.ZstdDecompressor()
with open(file_path, "rb") as fh:
    reader = dctx.stream_reader(fh)
    text_reader = io.TextIOWrapper(reader, encoding="utf-8")
```

**Java pattern**:
```java
try (var fis = Files.newInputStream(path);
     var zstdIn = new ZstdInputStream(fis);
     var reader = new BufferedReader(new InputStreamReader(zstdIn, StandardCharsets.UTF_8))) {
    String line;
    while ((line = reader.readLine()) != null) { ... }
}
```

**Rationale**: `ZstdInputStream` handles multi-frame streams transparently. Do NOT read the whole file into memory then `Zstd.decompress()` — the bookticker daily files are multi-GB.

### RULE I3 — `f.flush(); os.fsync(f.fileno())` → `FileChannel.force(true)` BEFORE close

**Python pattern** (`src/writer/consumer.py:1220-1222`):
```python
with open(file_path, "ab") as f:
    f.write(compressed)
    f.flush()
    os.fsync(f.fileno())
```

**Java pattern**:
```java
try (var fc = FileChannel.open(path, StandardOpenOption.APPEND, StandardOpenOption.CREATE)) {
    fc.write(ByteBuffer.wrap(compressed));
    fc.force(true);  // true = fsync data + metadata
}
```

**Rationale**: Tier 1 #4 — fsync MUST complete before commit. `Files.write(..., APPEND)` does NOT fsync; only `FileChannel.force(true)` guarantees durability.

**Watch-out**: `force(false)` fsyncs data but not metadata (file size) — that's equivalent to `fdatasync`, not `fsync`. The archive recovery path reads `file.length()` from the FS to truncate; metadata must be durable. Always use `force(true)`.

### RULE I4 — Crash-safe partial-frame truncation

**Python pattern** (`src/writer/consumer.py:1218-1230`):
```python
pos_before = f.tell()
try:
    f.write(compressed); f.flush(); os.fsync(f.fileno())
except OSError:
    try:
        f.truncate(pos_before); f.flush(); os.fsync(f.fileno())
    except OSError:
        pass
    raise
```

**Java pattern**:
```java
long posBefore = fc.position();
try {
    fc.write(ByteBuffer.wrap(compressed));
    fc.force(true);
} catch (IOException e) {
    try {
        fc.truncate(posBefore);
        fc.force(true);
    } catch (IOException ignored) {}
    throw e;
}
```

**Rationale**: If the write fails (ENOSPC), a partial frame may be on disk. Python truncates back to the pre-write position and fsyncs again. The pattern must port 1:1 — skipping the inner truncate breaks crash-safety.

### RULE I5 — SHA-256 of a file, chunked

**Python pattern** (`src/writer/file_rotator.py:56-61`):
```python
def compute_sha256(file_path: Path) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
```

**Java pattern**:
```java
public static String sha256Hex(Path p) throws IOException, NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    try (InputStream in = Files.newInputStream(p)) {
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) > 0) md.update(buf, 0, n);
    }
    return HexFormat.of().formatHex(md.digest());
}
```

**Rationale**: The sidecar format is `<hex>  <filename>\n` (two spaces) — matches `sha256sum(1)`. The chunk size matches Python; no reason to diverge.

**Watch-out**: `HexFormat.of().formatHex` returns lowercase — matches hashlib.

### RULE I6 — Atomic-ish sidecar write: write to final path, not temp-rename

**Python pattern** (`src/writer/file_rotator.py:64-66`):
```python
def write_sha256_sidecar(data_path, sc_path):
    digest = compute_sha256(data_path)
    sc_path.write_text(f"{digest}  {data_path.name}\n")
```

**Java pattern**: Write directly to the sidecar path. The Python code does not bother with temp+rename — the sidecar is tiny and the failure mode (empty/missing sidecar) is handled by the recovery path. Match that simplicity.

**Rationale**: Don't over-engineer. The existing recovery path in `_discover_sealed_files` tolerates missing sidecars.

### RULE I7 — `Path.rglob("*.jsonl.zst")` → `Files.walk(base).filter(p -> p.toString().endsWith(".jsonl.zst"))`

**Python pattern** (`src/writer/consumer.py:275`, `1130`):
```python
for zst_file in base.rglob("*.jsonl.zst"):
```

**Java pattern**:
```java
try (Stream<Path> stream = Files.walk(base)) {
    stream.filter(p -> p.toString().endsWith(".jsonl.zst"))
          .forEach(path -> { ... });
}
```

**Rationale**: `Files.walk` returns lazy `Stream<Path>`. Always use try-with-resources — it holds a directory handle.

**Watch-out**: `DirectoryStream` is a close second and avoids `Stream` overhead, but doesn't recurse. Always use `Files.walk` for rglob-semantics.

---

## J — Configuration

### RULE J1 — Pydantic `BaseModel` with nested models → Java records with `@JsonProperty`

**Python pattern** (`src/common/config.py:39-49`):
```python
class BinanceExchangeConfig(BaseModel):
    enabled: bool = True
    market: str = "usdm_futures"
    symbols: list[str]
    streams: StreamsConfig = Field(default_factory=StreamsConfig)
    writer_streams_override: list[str] | None = None
```

**Java pattern**:
```java
public record BinanceExchangeConfig(
    @JsonProperty("enabled") boolean enabled,
    @JsonProperty("market") String market,
    @JsonProperty("symbols") List<String> symbols,
    @JsonProperty("streams") StreamsConfig streams,
    @JsonProperty("writer_streams_override") List<String> writerStreamsOverride
) {
    public BinanceExchangeConfig {
        if (symbols == null || symbols.isEmpty())
            throw new ValidationException("symbols must be non-empty");
        symbols = symbols.stream().map(String::toLowerCase).toList();
    }
}
```

**Rationale**: Records are immutable by default. The compact constructor is the Java equivalent of Pydantic's `field_validator("symbols", mode="before")`.

**Watch-out**: Jackson does NOT auto-convert `writer_streams_override` (YAML snake_case) to `writerStreamsOverride` (Java camelCase) without explicit `@JsonProperty` OR a global `PropertyNamingStrategies.SNAKE_CASE` on the mapper. Pick one approach and document it; mixing is a common source of silent-null bugs.

### RULE J2 — `Field(default_factory=lambda: {"depth": 80_000, "trades": 10_000})` → record default in a static helper

**Python pattern** (`src/common/config.py:86`):
```python
buffer_caps: dict[str, int] = Field(default_factory=lambda: {"depth": 80_000, "trades": 10_000})
```

**Java pattern**:
```java
public record ProducerConfig(
    @JsonProperty("max_buffer") int maxBuffer,
    @JsonProperty("buffer_caps") Map<String, Integer> bufferCaps,
    @JsonProperty("default_stream_cap") int defaultStreamCap
) {
    public ProducerConfig {
        bufferCaps = bufferCaps == null
            ? Map.of("depth", 80_000, "trades", 10_000)
            : Map.copyOf(bufferCaps);
    }
}
```

**Rationale**: Record compact constructor is the only place to replace nulls with defaults. `Map.copyOf` enforces immutability.

### RULE J3 — `field_validator("retention_hours")` → Hibernate Validator annotation + compact ctor

**Python pattern** (`src/common/config.py:95-100`):
```python
@field_validator("retention_hours")
@classmethod
def validate_retention_hours(cls, value: int) -> int:
    if value < 12:
        raise ValueError("retention_hours must be >= 12")
    return value
```

**Java pattern**:
```java
public record RedpandaConfig(
    @JsonProperty("brokers") List<String> brokers,
    @JsonProperty("retention_hours") @Min(12) int retentionHours,
    @JsonProperty("producer") ProducerConfig producer
) {}

// in loader:
Validator v = Validation.buildDefaultValidatorFactory().getValidator();
Set<ConstraintViolation<RedpandaConfig>> violations = v.validate(cfg);
if (!violations.isEmpty()) throw new CryptoLakeConfigException(...);
```

**Rationale**: `@Min(12)` is declarative and composable. Custom validators use `@Constraint(validatedBy = ...)`.

### RULE J4 — Env-var overrides via `WRITER__BASE_DIR` → custom loader, not Jackson

**Python pattern** (`src/common/config.py:135-148`):
```python
def _apply_env_overrides(data, overrides):
    for key, raw_value in overrides.items():
        target = data
        parts = key.lower().split("__")
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        ...
```

**Java pattern**: Implement an identical helper over a `Map<String, Object>` before calling Jackson. Do NOT rely on Micronaut-style env-property-source — there's no framework.

**Rationale**: The `__` separator maps to YAML nesting. The precedence is (YAML file) < (env var). Preserve this behavior exactly; ops scripts depend on it.

**Watch-out**: The Python code has a special case that copies `HOST_DATA_DIR` to `WRITER__BASE_DIR` if not already set (`_normalize_env_overrides`). This is load-bearing; skip it and docker-compose volume mounts break.

---

## K — CLI (click → picocli)

### RULE K1 — `@click.group()` + `@cli.command()` → `@Command(name=..., subcommands={...})` with picocli

**Python pattern** (`src/cli/verify.py:274-290`):
```python
@click.group()
def cli(): ...

@cli.command()
@click.option("--date", required=True)
@click.option("--base-dir", default=DEFAULT_ARCHIVE_DIR)
def verify(date, base_dir, ...):
```

**Java pattern**:
```java
@Command(name = "cryptolake-verify", subcommands = {VerifyCmd.class, ManifestCmd.class})
public class VerifyCli implements Runnable { public void run() {} }

@Command(name = "verify")
class VerifyCmd implements Callable<Integer> {
    @Option(names = "--date", required = true) String date;
    @Option(names = "--base-dir", defaultValue = "${DEFAULT_ARCHIVE_DIR}") String baseDir;
    @Override public Integer call() { ... return 0; }
}
```

**Rationale**: picocli has a direct mapping for every click concept. The design doc (§1.1) already splits `cli/` into `verify`, `backfill`, `consolidation` subprojects — each gets one picocli Main.

### RULE K2 — `click.echo(...)` → `System.out.println(...)`, not `log.info`

**Python pattern** (`src/cli/verify.py:303`, `357-373`):
```python
click.echo(f"Verification complete for {date}")
```

**Java pattern**:
```java
System.out.println("Verification complete for " + date);
```

**Rationale**: CLI output is contractual — it goes to stdout for human/scripted consumption. Log output goes to stderr via Logback. Mixing them breaks pipelines and Tier 3 rule 20 (gate 5 compares `verify` stdout line-by-line).

### RULE K3 — `SystemExit(1)` → return `int` from `Callable`

**Python pattern** (`src/cli/verify.py:376`):
```python
if all_errors:
    raise SystemExit(1)
```

**Java pattern**:
```java
if (!errors.isEmpty()) return 1;
return 0;
```

**Rationale**: picocli's `Callable<Integer>` return value becomes the process exit code via `CommandLine.execute`. Don't use `System.exit()` from inside a command — kills unit tests.

---

## L — Testing patterns

### RULE L1 — `@pytest.fixture` → `@RegisterExtension` + helper method, or JUnit 5 `@BeforeEach`

**Python pattern** (`tests/conftest.py:20-31`):
```python
@pytest.fixture
def load_fixture():
    def _load(name: str) -> dict[str, Any]:
        path = FIXTURES_DIR / name
        return orjson.loads(path.read_bytes())
    return _load
```

**Java pattern**:
```java
// helper method in test base class:
protected JsonNode loadFixture(String name) throws IOException {
    return objectMapper.readTree(Files.readAllBytes(FIXTURES_DIR.resolve(name)));
}
```

**Rationale**: JUnit 5 fixtures are usually just test utility methods. Only reach for `@RegisterExtension` when state is shared across tests.

### RULE L2 — `@pytest.mark.integration` + per-module Testcontainers → `@Testcontainers` + `@Container`

**Python pattern** (`tests/integration/test_redpanda_roundtrip.py:23-57`):
```python
@pytest.fixture(scope="module")
def redpanda() -> str:
    with DockerContainer("redpandadata/redpanda:v24.1.2") ... as _container:
        yield f"127.0.0.1:{host_port}"
```

**Java pattern**:
```java
@Testcontainers
class RedpandaRoundtripTest {
    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName
        .parse("redpandadata/redpanda:v24.1.2")
        .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
        .withReuse(true);  // match pytest scope="module"
}
```

**Rationale**: Testcontainers for Java has `KafkaContainer` (Redpanda-compatible). `@Container` on a `static` field = module scope.

**Watch-out**: The Python test binds an ephemeral host port. Testcontainers does that automatically via `getBootstrapServers()` — never hard-code `19092`.

### RULE L3 — `pytest-asyncio` + `async def test_...` → JUnit 5 sync tests on virtual thread

**Python pattern**: async tests like `test_consumer_failover_integration.py` use `pytest-asyncio`.

**Java pattern**: No async-test framework. Every test method is sync; any would-be "async" operation blocks a virtual thread.

**Rationale**: The whole async infrastructure disappears under virtual threads. A test that does `await producer.produce(...)` in Python becomes `producer.produce(...)` blocking in Java.

### RULE L4 — `CliRunner()` from Click → `CommandLine().execute(args)` returning int

**Python pattern** (`tests/unit/test_backfill.py:28-33`):
```python
runner = CliRunner()
result = runner.invoke(cli, ["backfill", "--base-dir", str(tmp_path), ...])
assert result.exit_code == 0
assert "16" in result.output
```

**Java pattern**:
```java
var sw = new StringWriter();
int rc = new CommandLine(new BackfillCmd()).setOut(new PrintWriter(sw))
    .execute("backfill", "--base-dir", tmpPath.toString(), ...);
assertThat(rc).isEqualTo(0);
assertThat(sw.toString()).contains("16");
```

### RULE L5 — Fixture JSON in `tests/fixtures/` → same directory, copied verbatim

The test fixture files (`binance_aggtrade.json`, `binance_depth_diff.json`, etc.) are known-good exchange payloads. Copy them byte-for-byte into the Java project under `src/test/resources/fixtures/` — never regenerate, never reformat with an IDE JSON plugin.

---

## M — Domain-specific numeric/string invariants

### RULE M1 — Symbols are lowercase internally, UPPERCASE in REST URLs

**Python pattern** (`src/common/config.py:51-54`, `src/exchanges/binance.py:38`, `86-89`):
```python
@field_validator("symbols", mode="before")
def lowercase_symbols(cls, value): return [s.lower() for s in value]

def build_snapshot_url(self, symbol):
    return f"{self.rest_base}/fapi/v1/depth?symbol={symbol.upper()}&limit={limit}"
```

**Java pattern**: Enforce the contract in the config record's compact constructor (lowercase) and apply `.toUpperCase(Locale.ROOT)` at every URL-builder call site. Never rely on the caller to do the right thing.

**Watch-out**: Use `Locale.ROOT` for `toUpperCase` / `toLowerCase` — default locale turns Turkish "i" into "İ" and breaks symbol matching.

### RULE M2 — Binance WebSocket subscription keys mapping (`aggTrade` → `trades`)

**Python pattern** (`src/exchanges/binance.py:6-25`):
```python
_STREAM_KEY_MAP = {
    "aggTrade": "trades", "depth": "depth", "bookTicker": "bookticker",
    "markPrice": "funding_rate", "forceOrder": "liquidations",
}
_SUBSCRIPTION_MAP = {
    "depth": "@depth@100ms", "bookticker": "@bookTicker", "trades": "@aggTrade",
    "funding_rate": "@markPrice@1s", "liquidations": "@forceOrder",
}
```

**Java pattern**: Port as `static final Map<String, String>` with the same keys. Do NOT derive these from an enum — the mapping is asymmetric (`bookTicker` vs `bookticker` vs `bookticker@bookTicker`).

### RULE M3 — Depth diff natural keys — trades `a`, depth `u`, bookticker `u`

**Python pattern** (`src/writer/failover.py:16-43`):
```python
_RAW_KEY_STREAMS = {"trades": "a", "depth": "u", "bookticker": "u"}
```

**Java pattern**: Same map; return `long`. Note: `extract_natural_key` returns `null` for gap envelopes and missing raw data — preserve the tri-state (present / null).

**Watch-out**: `Optional<Long>` is cleaner but costs one allocation per call; on the hot path (every Kafka record), prefer returning a primitive `long` with a sentinel `-1L` for "missing" — and document it.

### RULE M4 — Funding rate interpolation emits numbers as strings with 8-decimal format

**Python pattern** (`src/cli/gaps.py:178-181`):
```python
records.append({
    "p": f"{p:.8f}", "ap": f"{p:.8f}", "P": f"{P:.8f}", "i": f"{i:.8f}",
    ...
})
```

**Java pattern**:
```java
String.format(Locale.ROOT, "%.8f", p);
```

**Rationale**: Binance wire format uses strings with fixed 8-decimal precision. A bare `Double.toString()` produces `"0.001"` not `"0.00100000"`. Use `Locale.ROOT` to avoid comma-decimal locales writing `"0,00100000"`.

### RULE M5 — Daily file sort key per stream

**Python pattern** (`src/cli/consolidate.py:79-87`):
```python
def _data_sort_key(record, stream):
    if stream == "trades": return orjson.loads(record["raw_text"]).get("a", record["exchange_ts"])
    if stream == "depth":  return orjson.loads(record["raw_text"]).get("u", record["exchange_ts"])
    return record["exchange_ts"]
```

**Java pattern**: Same logic, but re-parse `raw_text` through `ObjectMapper.readTree(raw_text)` (String → JsonNode); pull `.get("a").asLong()` / `.get("u").asLong()`. All `long`.

**Watch-out**: `exchange_ts` is `int` in Python but can exceed int range on ms-resolution Binance timestamps (`2^31 ms` ≈ 2038). Use `long` everywhere.

### RULE M6 — Gap reason is a fixed-vocabulary string set — not an enum

**Python pattern** (`src/common/envelope.py:9-24`):
```python
VALID_GAP_REASONS = frozenset({
    "ws_disconnect", "pu_chain_break", "session_seq_skip",
    "buffer_overflow", "snapshot_poll_miss", "collector_restart",
    "restart_gap", "recovery_depth_anchor", "write_error",
    "deserialization_error", "checkpoint_lost", "missing_hour",
})
```

**Java pattern**: Keep as `Set<String>` (or a sealed interface with singletons), emitted to JSON as a raw string. Do NOT use an enum with `@JsonValue` — downstream readers (Python `verify`, `consolidate`) must tolerate unknown reasons without crashing, which enums would fail on deserialize.

**Rationale**: The migration note on `collector_restart` shows the set is additive-only for archival compatibility. Enum serialization is brittle here.

### RULE M7 — `collector_session_id` format `{collector_id}_{ISO-8601-UTC}` is user-visible

**Python pattern** (`src/collector/main.py:32`):
```python
self.session_id = f"{collector_id}_{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}"
```

**Java pattern**:
```java
String sessionId = collectorId + "_" + DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
    .withZone(ZoneOffset.UTC)
    .format(Instant.now());
```

**Rationale**: This string is stored in the archive (envelope field) and becomes a primary key for `component_runtime_state` in PG. Do NOT use `Instant.toString()` — that includes fractional seconds (`.123Z`) and breaks the PK collision expectation.

### RULE M8 — `_partition` field in envelope is an `int`, `_offset` is a `long`

**Python pattern** (`src/common/envelope.py:129-139`, `src/writer/buffer_manager.py:106`):
```python
def add_broker_coordinates(envelope, *, topic, partition, offset):
    envelope["_topic"] = topic
    envelope["_partition"] = partition
    envelope["_offset"] = offset
```

**Java pattern**: `_partition` is an `int`, `_offset` is a `long`. Kafka partitions never exceed 2^15 in practice; offsets routinely exceed 2^31.

### RULE M9 — Special `_offset = -1` means "synthetic record, no Kafka offset"

**Python pattern** (`src/writer/consumer.py:745`, `769`, `810`):
```python
gap = add_broker_coordinates(gap, topic=msg_topic, partition=msg_partition, offset=-1)
```

**Java pattern**: Use `long` `-1L` literal. Document as a sentinel. The `verify.py` CLI explicitly skips `_offset < 0` records in duplicate-detection (line 62-64) — keep this behavior in the Java `verify`.

**Watch-out**: Never emit `null` for `_offset`. Python's JSON `null` would serialize differently than `-1` and break `verify` line-diff comparison.

### RULE M10 — `session_seq = -1` means "injected by writer, not the collector"

**Python pattern** (`src/writer/consumer.py:538`, `683`, `1191`):
```python
return create_gap_envelope(..., session_seq=-1, ...)
```

**Java pattern**: `long` or `int` `-1`. Session-seq tracking in `SessionSeqTracker` (collector side) will never produce negative values, so `-1` is unambiguous.

### RULE M11 — `hours_sealed_today` gauge resets at UTC midnight, not local midnight

**Python pattern** (`src/writer/consumer.py:844-849`):
```python
msg_dt = datetime.datetime.fromtimestamp(
    envelope["received_at"] / 1_000_000_000, tz=datetime.timezone.utc)
current_date = msg_dt.strftime("%Y-%m-%d")
... if current_date != prev_date: ... reset hours_sealed_today ...
```

**Java pattern**: Same — always `ZoneOffset.UTC`. Do NOT use `ZoneId.systemDefault()`; operators run the container in arbitrary timezones.

### RULE M12 — Topic prefix for primary vs backup (`""` vs `"backup."`)

**Python pattern** (`src/collector/producer.py:69`, `src/collector/backup_chain_reader.py:18-26`):
```python
topic = f"{self.topic_prefix}{self.exchange}.{stream}"
```

**Java pattern**: `topicPrefix + exchange + "." + stream`. Keep the empty-string default — do NOT use `""` → `null` substitution.

### RULE M13 — Topic name regex: `{exchange}.{stream}` — partition by symbol key

**Python pattern** (`src/collector/producer.py:69-70`):
```python
topic = f"{self.topic_prefix}{self.exchange}.{stream}"
key = symbol.encode()
```

**Java pattern**:
```java
String topic = topicPrefix + exchange + "." + stream;
byte[] key = symbol.getBytes(StandardCharsets.UTF_8);
```

**Rationale**: Kafka partitioner hashes the key. Preserving `symbol.getBytes(UTF_8)` gives identical partition assignment across Python and Java collectors — essential when both run simultaneously during failover.

### RULE M14 — Buffer-key tuple identity `(exchange, symbol, stream, date, hour)`

**Python pattern** (`src/writer/file_rotator.py:17-18`):
```python
@property
def key(self) -> tuple:
    return (self.exchange, self.symbol, self.stream, self.date, self.hour)
```

**Java pattern**: Use `record FileTarget(String exchange, String symbol, String stream, String date, int hour) {}`. Records auto-implement `equals`/`hashCode` by component identity, matching Python's tuple behavior.

**Watch-out**: `date` is a `String` (`"2026-04-18"`), not a `LocalDate` — comparison must be lex-order on the string for the rotation check to work. Don't "improve" it to `LocalDate`.

### RULE M15 — Late-file sequence generation — `hour-14.late-1.jsonl.zst`

**Python pattern** (`src/writer/consumer.py:707-717`):
```python
stem = file_path.stem.split(".")[0]  # "hour-14"
seq += 1
late_name = f"{stem}.late-{seq}.jsonl.zst"
```

**Java pattern**: Same exact template — `stem + ".late-" + seq + ".jsonl.zst"`. The file-discovery regex in `consolidate._RE_LATE` depends on this naming.

### RULE M16 — Funding rate intervals: 8h boundaries — `{00, 08, 16}` UTC

**Python pattern** (`src/cli/gaps.py:105-116`):
```python
def _compute_next_funding_time(event_time_ms):
    dt = datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
    hour = dt.hour
    if hour < 8: next_dt = dt.replace(hour=8, ...)
    elif hour < 16: next_dt = dt.replace(hour=16, ...)
    else: next_dt = (dt + timedelta(days=1)).replace(hour=0, ...)
```

**Java pattern**: Same logic. Always UTC. The boundaries are a Binance contract, not a code concern.

---

## Appendix: cross-cutting watch-outs

- **Never** use `CompletableFuture.supplyAsync` without a virtual-thread executor — it defaults to the common ForkJoinPool (platform threads).
- **Never** call `ObjectMapper.writeValueAsString(someRecord)` from within a hot path without caching the mapper — even though they're cheap, the recommendation is one mapper per service (Tier 2 rule 14).
- **Never** translate `contextlib.closing` to `try-finally` — use try-with-resources with `AutoCloseable`.
- **Never** translate `dict.pop(k, default)` as `map.remove(k)` — that returns `null` on missing, which is NOT the same as Python's default-returning `pop`. Use `Optional.ofNullable(map.remove(k)).orElse(default)`.
- **Never** assume `JsonNode.get(field).asLong()` returns `0` for missing fields — it throws `NullPointerException`. Use `asLong(0L)` with the default arg, or `path(field).asLong(0L)`.
