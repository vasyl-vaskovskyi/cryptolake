---
module: common
status: approved
produced_by: architect
based_on_mapping: 003375e8690f6696838506400e920af5dd97b471
---

## 1. Package layout

Root Gradle subproject: `cryptolake-java/common/` (already scaffolded). Root package: `com.cryptolake.common`. Java 21, jar artifact (no `main()` in this module).

```
com.cryptolake.common
├── envelope/
│   ├── DataEnvelope              (record)  — canonical data envelope
│   ├── GapEnvelope               (record)  — canonical gap envelope (+ optional restart metadata)
│   ├── BrokerCoordinates         (record)  — (_topic,_partition,_offset) carrier
│   ├── GapReasons                (final class) — Set<String> vocabulary + validation
│   └── EnvelopeCodec             (final class) — ObjectMapper factory + serialize/deserialize/
│                                               addBrokerCoordinates helpers
├── config/
│   ├── AppConfig                 (record)  — root; maps to CryptoLakeConfig
│   ├── DatabaseConfig            (record)
│   ├── ExchangesConfig           (record)
│   ├── BinanceExchangeConfig     (record)  — symbol lowercasing + depth_snapshot auto-include
│   ├── StreamsConfig             (record)
│   ├── DepthConfig               (record)
│   ├── OpenInterestConfig        (record)
│   ├── MonitoringConfig          (record)
│   ├── ProducerConfig            (record)  — buffer_caps default factory
│   ├── RedpandaConfig            (record)  — retention_hours >= 12
│   ├── WriterConfig              (record)
│   ├── GapFilterConfig           (record)
│   ├── CollectorConfig           (record)
│   ├── YamlConfigLoader          (final class) — loadConfig(path, envOverrides)
│   ├── EnvOverrides              (final class, package-private) — parse "MODULE__KEY" env map
│   └── CryptoLakeConfigException (final class extends RuntimeException)
├── logging/
│   ├── StructuredLogger          (final class) — SLF4J facade: event(name, kv...) etc.
│   ├── LogInit                   (final class) — one-time programmatic setup, UTC ISO-8601
│   └── LogbackConfigResource     (resource)  — src/main/resources/logback.xml with Logstash
├── jsonl/
│   ├── JsonlReader               (final class) — readAll(Path) -> List<JsonNode>, skip bad lines
│   └── JsonlWriter               (final class) — appendOne(Path, JsonNode)
├── health/
│   ├── HealthServer              (final class) — jdk.httpserver impl; /health /ready /metrics
│   ├── ReadyCheck                (functional interface) — Supplier<Map<String,Boolean>>
│   └── MetricsSource             (functional interface) — Supplier<byte[]> for Prometheus bytes
├── identity/
│   └── SystemIdentity            (final class) — getHostBootId(), getSessionId(collectorId, clock)
├── kafka/
│   └── TopicNames                (final class) — topic/key helpers (cross-service constants)
└── util/
    ├── Sha256                    (final class) — hexDigestUtf8(String), hexFile(Path)
    ├── ClockSupplier             (functional interface) — long nowNs() (testable clock)
    ├── Clocks                    (final class) — Clocks.systemNanoClock()
    └── Shutdownable              (interface)   — void shutdown() throws InterruptedException
```

No class uses inheritance beyond `extends RuntimeException` for the one exception type. Every data carrier is a record. No package has a `main()`.

## 2. Class catalog

### 2.1 `envelope/`

#### `DataEnvelope` — record
- **Purpose**: canonical data envelope carrier.
- **Public API**:
  ```
  @JsonPropertyOrder({"v","type","exchange","symbol","stream","received_at",
                      "exchange_ts","collector_session_id","session_seq",
                      "raw_text","raw_sha256"})
  public record DataEnvelope(
      @JsonProperty("v") int v,
      @JsonProperty("type") String type,
      @JsonProperty("exchange") String exchange,
      @JsonProperty("symbol") String symbol,
      @JsonProperty("stream") String stream,
      @JsonProperty("received_at") long receivedAt,
      @JsonProperty("exchange_ts") long exchangeTs,
      @JsonProperty("collector_session_id") String collectorSessionId,
      @JsonProperty("session_seq") long sessionSeq,
      @JsonProperty("raw_text") String rawText,
      @JsonProperty("raw_sha256") String rawSha256)
  ```
- **Factory** (public static): `create(exchange, symbol, stream, rawText, exchangeTs, collectorSessionId, sessionSeq, clock)` computes `v=1`, `type="data"`, `receivedAt=clock.nowNs()`, `rawSha256=Sha256.hexDigestUtf8(rawText)`.
- **Dependencies**: none (pure record); factory uses `ClockSupplier` + `Sha256`.
- **Thread safety**: immutable record — safe for any thread.

#### `GapEnvelope` — record
- **Purpose**: canonical gap envelope carrier, with optional restart-metadata fields rendered only when non-null.
- **Public API**:
  ```
  @JsonPropertyOrder({"v","type","exchange","symbol","stream","received_at",
                      "collector_session_id","session_seq","gap_start_ts","gap_end_ts",
                      "reason","detail",
                      "component","cause","planned","classifier","evidence","maintenance_id"})
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public record GapEnvelope(
      @JsonProperty("v") int v,
      @JsonProperty("type") String type,
      @JsonProperty("exchange") String exchange,
      @JsonProperty("symbol") String symbol,
      @JsonProperty("stream") String stream,
      @JsonProperty("received_at") long receivedAt,
      @JsonProperty("collector_session_id") String collectorSessionId,
      @JsonProperty("session_seq") long sessionSeq,
      @JsonProperty("gap_start_ts") long gapStartTs,
      @JsonProperty("gap_end_ts") long gapEndTs,
      @JsonProperty("reason") String reason,
      @JsonProperty("detail") String detail,
      @JsonProperty("component") String component,
      @JsonProperty("cause") String cause,
      @JsonProperty("planned") Boolean planned,
      @JsonProperty("classifier") String classifier,
      @JsonProperty("evidence") Map<String,Object> evidence,
      @JsonProperty("maintenance_id") String maintenanceId)
  ```
- **Factories** (public static):
  - `create(...core args...)` — required fields only; optional fields all `null`.
  - `createWithRestartMetadata(...core args..., component, cause, planned, classifier, evidence, maintenanceId)` — any of the optional args may be null; non-null ones become serialized fields.
- **Compact constructor** validates `reason` is in `GapReasons.VALID` and throws `IllegalArgumentException` (which `GapReasons.requireValid` rewraps if preferred).
- **Dependencies**: `GapReasons`, `ClockSupplier`.
- **Thread safety**: immutable.

#### `BrokerCoordinates` — record
- **Purpose**: `_topic`, `_partition`, `_offset` fields added to an envelope after a successful Kafka produce/consume handshake. Written alongside envelope in writer's archive path.
- **Public API**:
  ```
  @JsonPropertyOrder({"_topic","_partition","_offset"})
  public record BrokerCoordinates(
      @JsonProperty("_topic") String topic,
      @JsonProperty("_partition") int partition,
      @JsonProperty("_offset") long offset)
  ```
- **Note**: `_offset = -1L` is a sentinel meaning "synthetic record, no Kafka offset" (Tier 5 M9).
- **Thread safety**: immutable.

#### `GapReasons` — final class
- **Purpose**: vocabulary guard for gap `reason` strings. Set<String>, never an enum (Tier 5 M6).
- **Public API**:
  ```
  public static final Set<String> VALID = Set.of(
      "ws_disconnect","pu_chain_break","session_seq_skip","buffer_overflow",
      "snapshot_poll_miss","collector_restart","restart_gap","recovery_depth_anchor",
      "write_error","deserialization_error","checkpoint_lost","missing_hour");
  public static void requireValid(String reason);  // throws IllegalArgumentException
  ```
- **Thread safety**: constants only.

#### `EnvelopeCodec` — final class
- **Purpose**: single `ObjectMapper` owner for the envelope codec path (Tier 2 #14). Writers/collectors will receive one by DI; this class is a factory + thin serialize/deserialize wrappers used across the project.
- **Public API**:
  ```
  public static ObjectMapper newMapper();                // configured JSON mapper
  public static ObjectMapper newYamlMapper();            // configured YAML mapper (same family)
  public byte[] toJsonBytes(Object envelope);            // orjson-equivalent compact output
  public DataEnvelope readData(byte[] bytes);
  public GapEnvelope readGap(byte[] bytes);
  public JsonNode readTree(byte[] bytes);                // for routing (rawFrame peek)
  public byte[] appendNewline(byte[] jsonBytes);         // returns bytes + 0x0A
  public static DataEnvelope withBrokerCoordinates(DataEnvelope env, BrokerCoordinates c);
  public static GapEnvelope  withBrokerCoordinates(GapEnvelope env, BrokerCoordinates c);
  ```
- **Mapper config** (see §6 for full list):
  `ORDER_MAP_ENTRIES_BY_KEYS=false`, `INDENT_OUTPUT=false`, `WRITE_DATES_AS_TIMESTAMPS=false`, `FAIL_ON_UNKNOWN_PROPERTIES=false`, naming convention: identity (no snake_case auto-map — we use explicit `@JsonProperty`).
- **Dependencies** (constructor-injected — Tier 2 #11): `ObjectMapper mapper` (but static factory `newMapper()` is provided so there's no implicit framework).
- **Thread safety**: `ObjectMapper` is thread-safe after configuration; instance is immutable post-construction.
- **Note on broker coordinates**: Python mutates the envelope dict in place. In Java, records are immutable, so we return a sibling "envelope + broker coordinates" serialization path: in the writer, the archive writer serializes the envelope, then appends a comma and the three broker fields to the same JSON object. Exact serialization strategy is pinned in §6 below — the output byte layout matches Python's post-mutation `orjson.dumps()`.

### 2.2 `config/`

All records use explicit `@JsonProperty` for snake_case mapping (Tier 5 J1 watch-out — we do NOT use a global `PropertyNamingStrategies.SNAKE_CASE`). Hibernate Validator annotations attach where Python has a `field_validator`.

#### `AppConfig` — record (ports `CryptoLakeConfig`)
```
public record AppConfig(
    @JsonProperty("database")  @NotNull @Valid DatabaseConfig database,
    @JsonProperty("exchanges") @NotNull @Valid ExchangesConfig exchanges,
    @JsonProperty("redpanda")  @NotNull @Valid RedpandaConfig redpanda,
    @JsonProperty("writer")    @Valid WriterConfig writer,
    @JsonProperty("monitoring")@Valid MonitoringConfig monitoring,
    @JsonProperty("collector") @Valid CollectorConfig collector)
```
Compact constructor supplies defaults for `writer` / `monitoring` / `collector` when null.

#### `DatabaseConfig` — record
`@JsonProperty("url") @NotBlank String url`.

#### `ExchangesConfig` — record
`@JsonProperty("binance") @NotNull @Valid BinanceExchangeConfig binance`.

#### `BinanceExchangeConfig` — record
```
public record BinanceExchangeConfig(
    @JsonProperty("enabled") boolean enabled,
    @JsonProperty("market") String market,
    @JsonProperty("ws_base") String wsBase,
    @JsonProperty("rest_base") String restBase,
    @JsonProperty("symbols") @NotNull @Size(min=1) List<String> symbols,
    @JsonProperty("streams") @Valid StreamsConfig streams,
    @JsonProperty("writer_streams_override") List<String> writerStreamsOverride,
    @JsonProperty("depth") @Valid DepthConfig depth,
    @JsonProperty("open_interest") @Valid OpenInterestConfig openInterest,
    @JsonProperty("collector_id") String collectorId)
```
Compact constructor:
1. lowercases `symbols` with `Locale.ROOT` (Tier 5 M1).
2. If `writerStreamsOverride` is non-null and contains `"depth"` and not `"depth_snapshot"`, appends `"depth_snapshot"` (Tier 5 J1-equivalent validator).
3. Defaults: `market="usdm_futures"`, `wsBase="wss://fstream.binance.com"`, `restBase="https://fapi.binance.com"`, `collectorId="binance-collector-01"`, `streams=new StreamsConfig(true,true,true,true,true,true)`, `depth=new DepthConfig(...)`, `openInterest=new OpenInterestConfig(...)` — applied when incoming value is null.
4. Makes `symbols`, `writerStreamsOverride` immutable via `List.copyOf` (or `null` preserved as-is for `writerStreamsOverride`).
- **Method**: `public List<String> getEnabledStreams()` — returns dynamic list matching Python's `get_enabled_streams()`.

#### `StreamsConfig` — record
`trades, depth, bookticker, funding_rate, liquidations, open_interest : boolean`, defaults all true. `@JsonProperty` on each.

#### `DepthConfig` — record
`updateSpeed="100ms"`, `snapshotInterval="5m"`, `snapshotOverrides Map<String,String>` (default `Map.of()` in compact ctor).

#### `OpenInterestConfig` — record
`pollInterval="5m"` (`@JsonProperty("poll_interval")`).

#### `MonitoringConfig` — record
`prometheusPort=8000`, `webhookUrl=""`.

#### `ProducerConfig` — record
```
public record ProducerConfig(
    @JsonProperty("max_buffer") int maxBuffer,
    @JsonProperty("buffer_caps") Map<String, Integer> bufferCaps,
    @JsonProperty("default_stream_cap") int defaultStreamCap)
```
Compact ctor: if `bufferCaps == null`, use `Map.of("depth", 80_000, "trades", 10_000)`; else `Map.copyOf(bufferCaps)`. `maxBuffer` default 100_000, `defaultStreamCap` default 10_000 when the deserialized record is reconstructed with nulls (Jackson maps missing to `null` for boxed; for primitives we deserialize into a wrapper via a custom reader, see §7).

#### `RedpandaConfig` — record
```
public record RedpandaConfig(
    @JsonProperty("brokers") @NotNull @Size(min=1) List<String> brokers,
    @JsonProperty("retention_hours") @Min(12) int retentionHours,
    @JsonProperty("producer") @Valid ProducerConfig producer)
```
Compact ctor: default `retentionHours=48`, default `producer=new ProducerConfig(100_000, null, 10_000)`, wraps `brokers` via `List.copyOf`.

#### `WriterConfig` — record
```
public record WriterConfig(
    @JsonProperty("base_dir") String baseDir,
    @JsonProperty("rotation") String rotation,
    @JsonProperty("compression") String compression,
    @JsonProperty("compression_level") int compressionLevel,
    @JsonProperty("checksum") String checksum,
    @JsonProperty("flush_messages") int flushMessages,
    @JsonProperty("flush_interval_seconds") int flushIntervalSeconds,
    @JsonProperty("gap_filter") @Valid GapFilterConfig gapFilter)
```
Defaults: `baseDir = System.getenv().getOrDefault("HOST_DATA_DIR","/data")` (Tier 5 J4-tie), `rotation="hourly"`, `compression="zstd"`, `compressionLevel=3`, `checksum="sha256"`, `flushMessages=10_000`, `flushIntervalSeconds=30`, `gapFilter=new GapFilterConfig(10.0, 60.0)`.

#### `GapFilterConfig` — record
`@DecimalMin("0.0") double gracePeriodSeconds`, `@DecimalMin("0.0") double snapshotMissGraceSeconds`; defaults `10.0` and `60.0`.

#### `CollectorConfig` — record
`@JsonProperty("tap_output_dir") String tapOutputDir` (nullable; holds a path string; default `null`).

#### `YamlConfigLoader` — final class
- **Purpose**: port of `load_config()`. Reads YAML → applies env overrides → validates.
- **Public API**:
  ```
  public static AppConfig load(Path path, Map<String, String> envOverrides);
  public static AppConfig load(Path path);  // envOverrides = System.getenv()
  public static String defaultArchiveDir();  // HOST_DATA_DIR or /data
  ```
- **Algorithm**:
  1. `if (!Files.exists(path)) throw new CryptoLakeConfigException("Config file not found: " + path, null);` (wraps Python's `FileNotFoundError`, which is unchecked in translation — see §7).
  2. Read YAML into a `Map<String,Object>` via `EnvelopeCodec.newYamlMapper().readValue(path.toFile(), Map.class)` (if null/empty, `Map.of()`).
  3. Compute `overrides`: if caller-provided map is non-null, treat as "explicit, do not consult System.getenv()" (Tier 5 J4-tie / mapping §10 "Config environment override isolation"). If null, build from `System.getenv()` filtered by first-segment whitelist (`database, exchanges, redpanda, writer, monitoring, collector`) plus `HOST_DATA_DIR`.
  4. Normalize overrides: if `HOST_DATA_DIR` present and `WRITER__BASE_DIR` not, set `WRITER__BASE_DIR = HOST_DATA_DIR` (then remove `HOST_DATA_DIR`) — Tier 5 J4 watch-out.
  5. Apply overrides via `EnvOverrides.apply(dataMap, normalizedOverrides)` (port of `_apply_env_overrides`). Comma-split values into lists, `__` splits into nested map keys, all segments lowercased.
  6. Convert the resulting Map to `AppConfig` via `objectMapper.convertValue(data, AppConfig.class)`.
  7. Run Bean Validation with a static `Validator`; if violations: throw `CryptoLakeConfigException` with concatenated messages.
  8. Jackson `JsonProcessingException` (thrown when YAML is malformed or a numeric field cannot parse) is caught and wrapped as `CryptoLakeConfigException`.
- **Dependencies**: `ObjectMapper yamlMapper`, `ObjectMapper jsonMapper` (for convertValue), `Validator validator` — all created once at class load as static finals (Tier 2 #14). Validator factory is closed at JVM shutdown via a shutdown hook.
- **Thread safety**: all static + stateless; safe from any thread.

#### `EnvOverrides` — package-private final class
- **Purpose**: port of `_apply_env_overrides` and `_normalize_env_overrides`.
- **Public API** (package-private):
  ```
  static Map<String,String> normalize(Map<String,String> in);
  static Map<String,Object> apply(Map<String,Object> data, Map<String,String> overrides);
  ```
- **Thread safety**: stateless.

#### `CryptoLakeConfigException` — final class extends RuntimeException
- **Purpose**: single unchecked config failure type (Tier 2 #13; Tier 5 G2).
- **Public API**: `(String, Throwable)`, `(String)`.

### 2.3 `logging/`

#### `StructuredLogger` — final class
- **Purpose**: SLF4J facade that preserves the structlog convention "event name is the log message; KV pairs become top-level JSON fields" (Tier 5 H2).
- **Public API** (thin wrapper using `net.logstash.logback.argument.StructuredArguments.keyValue`):
  ```
  public static StructuredLogger of(Class<?> cls);
  public void info(String event, Object... kvs);      // kvs alternating key, value
  public void warn(String event, Object... kvs);
  public void error(String event, Throwable t, Object... kvs);
  public void debug(String event, Object... kvs);
  // context:
  public static AutoCloseable mdc(String... keyValuePairs);    // try-with-resources MDC scope
  ```
- **Dependencies**: wraps `org.slf4j.Logger`.
- **Thread safety**: logger is thread-safe; MDC is per-thread (ThreadLocal in Logback — works correctly with virtual threads when `MDCCloseable` is used in try-with-resources).

#### `LogInit` — final class
- **Purpose**: optional programmatic initialization (default config is loaded from `logback.xml` classpath resource). Provides a safety call `LogInit.setLevel(String)` for the `level` arg historically passed to `setup_logging(level)`.
- **Public API**: `public static void setLevel(String);` — mutates root logger level via `ch.qos.logback.classic.Logger`.

#### `logback.xml` (classpath resource)
- Root logger: `INFO`; console appender; `LogstashEncoder` with:
  - `timeZone=UTC`
  - `@timestamp` field rendered via ISO-8601 with `Z` suffix (Tier 5 H2)
  - `message` renamed to `event` to match structlog's key (Tier 5 H2). Implementation: `LogstashEncoder` + `<provider>` chain — simplest is default `LogstashEncoder` which emits `message`; the `StructuredLogger` facade emits the event text as the message, and we add a Logstash encoder field-renamer or skip rename and accept `message` vs `event` — **open question Q1 in §11**.

### 2.4 `jsonl/`

#### `JsonlReader` — final class
- **Purpose**: crash-resilient JSONL reader; drops malformed/empty lines (Tier 5 I3 for I/O shape).
- **Public API**:
  ```
  public static List<JsonNode> readAll(Path path, ObjectMapper mapper);
  ```
- Uses `Files.newBufferedReader(path, UTF_8)`. For each line, trims, skips blanks, `try { mapper.readTree(line) } catch (JsonProcessingException) { continue; }`. Returns `List.of()` if file missing.
- **Dependencies**: caller-supplied `ObjectMapper`.
- **Thread safety**: stateless.

#### `JsonlWriter` — final class
- **Purpose**: append one record per syscall; minimal crash window.
- **Public API**:
  ```
  public static void appendOne(Path path, JsonNode record, ObjectMapper mapper);
  ```
- Creates parent dirs, writes `mapper.writeValueAsBytes(record)` + `0x0A` via `FileChannel.open(APPEND, CREATE).write(ByteBuffer)`. **Does not fsync** (this helper is used for the lifecycle ledger in scripts; Python doesn't fsync either).
- **Thread safety**: stateless; caller serializes access if needed.

### 2.5 `health/`

#### `HealthServer` — final class
- **Purpose**: tiny HTTP server (`com.sun.net.httpserver.HttpServer`, JDK built-in) exposing `/health`, `/ready`, `/metrics`. No framework.
- **Public API**:
  ```
  public HealthServer(int port, ReadyCheck readyCheck, MetricsSource metricsSource);
  public void start();        // binds + spawns a virtual-thread executor
  public void stop();         // calls HttpServer.stop(1)
  public int boundPort();     // for tests
  ```
- **Endpoints**:
  - `/health` → 200, body `{"status":"ok"}` (Content-Type `application/json`).
  - `/ready` → 200 if `Map<String,Boolean>` values all true else 503; body = JSON of the map.
  - `/metrics` → 200, body = `metricsSource.get()` (Prometheus text; Content-Type from `io.prometheus.client.exporter.common.TextFormat.CONTENT_TYPE_004` or Micrometer's `PrometheusMeterRegistry.scrape()` return).
- **Dependencies** (constructor-injected): `ReadyCheck`, `MetricsSource`. `ObjectMapper` is reused from caller via a setter (default: internal minimal mapper).
- **Concurrency**: `HttpServer` uses a `setExecutor(Executors.newVirtualThreadPerTaskExecutor())` so every request lands on a fresh virtual thread (Tier 2 #10-equivalent, natural blocking).
- **Thread safety**: handlers are stateless; the `readyCheck`/`metricsSource` suppliers MUST be thread-safe (documented on `ReadyCheck`/`MetricsSource`).

### 2.6 `identity/`

#### `SystemIdentity` — final class
- **Purpose**: boot-ID resolution + session-ID formatting.
- **Public API**:
  ```
  public static String getHostBootId();
  public static String buildSessionId(String collectorId, Instant now);
  public static String buildSessionId(String collectorId);   // uses Instant.now()
  ```
- **Boot ID resolution order** (port of `system_identity.py`):
  1. `System.getenv("CRYPTOLAKE_TEST_BOOT_ID")` — if present, trimmed.
  2. `Files.readString(Path.of("/proc/sys/kernel/random/boot_id")).strip()` — if readable.
  3. `"unknown"` sentinel on any exception (`NoSuchFileException`, `AccessDeniedException`, `IOException`).
- **Session ID format**: `collectorId + "_" + DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC).format(now)` — **no fractional seconds** (Tier 5 M7, mapping §10-M7). Never `Instant.toString()`.
- **Thread safety**: stateless; formatters are immutable.

### 2.7 `kafka/`

#### `TopicNames` — final class
- **Purpose**: shared topic-name / key constants so collector and writer agree byte-for-byte. Ports the `f"{topic_prefix}{exchange}.{stream}"` pattern (Tier 5 M12, M13).
- **Public API**:
  ```
  public static String forStream(String topicPrefix, String exchange, String stream);
  public static byte[] symbolKey(String symbol);     // symbol.getBytes(UTF_8)
  public static String stripBackupPrefix(String topic, String backupPrefix);  // Tier 5 C7
  ```
- **Thread safety**: stateless.

### 2.8 `util/`

#### `Sha256` — final class
- **Public API**:
  ```
  public static String hexDigestUtf8(String text);   // SHA-256 of text.getBytes(UTF_8) → lowercase hex
  public static String hexFile(Path p) throws IOException;  // 8192-byte-chunk SHA-256 (Tier 5 I5)
  ```
- **Implementation**: `MessageDigest.getInstance("SHA-256")`; hex via `HexFormat.of().formatHex(...)`.
- **Thread safety**: stateless; each call builds a fresh `MessageDigest`.

#### `ClockSupplier` — functional interface
```
@FunctionalInterface
public interface ClockSupplier {
    long nowNs();
}
```
(Tier 5 E2.) Default impl: `Clocks.systemNanoClock()` returns a `ClockSupplier` that evaluates `Math.addExact(Math.multiplyExact(now.getEpochSecond(), 1_000_000_000L), now.getNano())`.

#### `Clocks` — final class
- `public static ClockSupplier systemNanoClock();`
- `public static ClockSupplier fixed(long ns);`   // for tests

#### `Shutdownable` — interface
- Single method `void shutdown() throws InterruptedException;` — used by the writer/collector for clean-shutdown chaining; `common` provides the interface so `service_runner` (writer/collector modules) can uniformly signal child components.

## 3. Concurrency design

Scope of this module's concurrency surface is intentionally small. Mapping §5 lists four concurrency-bearing items. Here is the per-item design:

### 3.1 `cancel_tasks` → **deleted, not ported** (Tier 5 A7)
- The Python helper exists only because asyncio task lifetime is disconnected from scope. Java's structured concurrency makes it redundant.
- Callers in the writer/collector modules will use `StructuredTaskScope.shutdown()` or `ExecutorService.shutdownNow() + awaitTermination(...)` directly. No common equivalent exists.

### 3.2 `start_health_server` → `HealthServer.start()` on virtual-thread executor
- `com.sun.net.httpserver.HttpServer.create(new InetSocketAddress(port), 0)` returns a server.
- `server.setExecutor(Executors.newVirtualThreadPerTaskExecutor())` — each request is served on a fresh virtual thread (Tier 2 #10 analog).
- Handlers are stateless; the `ReadyCheck` and `MetricsSource` suppliers are called synchronously in the handler thread; they block naturally (virtual thread, no pinning). No `StructuredTaskScope` needed — one request, one thread.
- Shutdown: `HealthServer.stop()` calls `server.stop(1)` giving in-flight requests up to 1s to drain; the virtual-thread executor is automatically GC'd.
- **No locking**: handlers read from injected suppliers only; no mutable state.

### 3.3 `run_service` (bootstrap) → **not ported to common** (lives in each service's `Main`)
- The spec (§1.4) mandates `main()`-local wiring (Tier 2 #11). The common module does NOT ship a `run_service` facility because it would need to know which service it's bootstrapping (collector vs writer) and would encourage a hidden framework.
- Shared pieces that `run_service` performs do live in common: `LogInit.setLevel()`, `YamlConfigLoader.load()`, `SystemIdentity.getHostBootId()`. The concurrent bootstrap — signal handlers, virtual-thread executor open/close, shutdown gating — is written once per service's `Main` (Tier 5 A8: uvloop has no replacement; virtual threads ARE the story).
- In the `completion.md` for `common`, the Developer notes this as a *deliberate* non-port (Tier 4 #24 Rule compliance), not an omission.
- **Shared utility**: `util/Shutdownable` interface is provided so each service's `Main` can express clean shutdown in a uniform shape.

### 3.4 `uvloop.install()` → **deleted, no replacement** (Tier 5 A8)
- Virtual threads are the performance story. Any `uvloop` mention in Python is removed without a Java equivalent.

### 3.5 JSONL I/O and boot-ID read
- `JsonlReader.readAll` / `JsonlWriter.appendOne` / `SystemIdentity.getHostBootId` perform synchronous file I/O on the calling thread. On virtual threads, this is the correct shape (Tier 5 A2). No explicit async wrapping.
- No locking within `common`: each call opens its own `FileChannel`/reader and closes via try-with-resources. Concurrent writers to the same JSONL path must be serialized by the caller (writer's host-lifecycle reader is single-threaded per Python's `_lifecycle_task`).

### 3.6 `EnvelopeCodec`
- The `ObjectMapper` is thread-safe after configuration. `EnvelopeCodec` holds it as `final` and exposes pure methods; no locking. Multiple virtual threads may call `toJsonBytes`/`readData` concurrently — Jackson is designed for this.

### 3.7 Cancellation & shutdown
- `common` exports no long-lived tasks. The one daemon it *does* own — `HealthServer` — is stopped via `HealthServer.stop()` from the service's `Main.close()` path, which runs after all other subsystems have stopped (ensures `/ready` still answers 503 during shutdown).
- No `InterruptedException` handling lives in common; services that call `HealthServer.stop()` from within a shutdown hook handle interrupts according to Tier 5 A4.

### 3.8 Locking strategy
- **None.** No `synchronized`, no `ReentrantLock` in `common`. The only shared-mutable state candidates (Jackson's `ObjectMapper`, Hibernate `Validator`) are thread-safe by contract.
- If a future change introduces shared mutable state, Tier 2 #9 mandates `ReentrantLock`, never `synchronized`.

## 4. Python → Java mapping table

Symbol ordering matches mapping §3 + §4.

| Python symbol / file | Java class / method | Notes |
|---|---|---|
| `src/common/__init__.py` | (none) | Empty marker, no port. |
| `src/common/async_utils.py::cancel_tasks` | **deleted** | Tier 5 A7 — structured concurrency renders redundant. |
| `src/common/config.py::ConfigValidationError` | `config.CryptoLakeConfigException` | Unchecked `RuntimeException`; Tier 5 G2. |
| `src/common/config.py::StreamsConfig` | `config.StreamsConfig` (record) | — |
| `src/common/config.py::DepthConfig` | `config.DepthConfig` (record) | — |
| `src/common/config.py::OpenInterestConfig` | `config.OpenInterestConfig` (record) | — |
| `src/common/config.py::MonitoringConfig` | `config.MonitoringConfig` (record) | — |
| `src/common/config.py::BinanceExchangeConfig` | `config.BinanceExchangeConfig` (record) | Compact ctor: lowercase symbols, depth_snapshot auto-include. |
| `BinanceExchangeConfig.get_enabled_streams` | `BinanceExchangeConfig.getEnabledStreams()` | Same logic. |
| `BinanceExchangeConfig.lowercase_symbols` | (compact ctor) | Tier 5 M1; `Locale.ROOT`. |
| `BinanceExchangeConfig.auto_include_depth_snapshot` | (compact ctor) | — |
| `src/common/config.py::ExchangesConfig` | `config.ExchangesConfig` (record) | — |
| `src/common/config.py::ProducerConfig` | `config.ProducerConfig` (record) | Tier 5 J2 default factory via compact ctor. |
| `src/common/config.py::RedpandaConfig` | `config.RedpandaConfig` (record) | `@Min(12)` via Hibernate Validator (Tier 5 J3). |
| `RedpandaConfig.validate_retention_hours` | `@Min(12)` annotation | — |
| `src/common/config.py::DatabaseConfig` | `config.DatabaseConfig` (record) | — |
| `src/common/config.py::default_archive_dir` | `config.YamlConfigLoader.defaultArchiveDir()` | `HOST_DATA_DIR` env or `/data`. |
| `src/common/config.py::GapFilterConfig` | `config.GapFilterConfig` (record) | `@DecimalMin("0.0")`. |
| `src/common/config.py::WriterConfig` | `config.WriterConfig` (record) | Default `baseDir` via `defaultArchiveDir()`. |
| `src/common/config.py::CollectorConfig` | `config.CollectorConfig` (record) | `tapOutputDir` nullable String (path). |
| `src/common/config.py::CryptoLakeConfig` | `config.AppConfig` (record) | Root. |
| `src/common/config.py::_apply_env_overrides` | `config.EnvOverrides.apply` | Package-private helper. |
| `src/common/config.py::_normalize_env_overrides` | `config.EnvOverrides.normalize` | `HOST_DATA_DIR` → `WRITER__BASE_DIR` alias. |
| `src/common/config.py::load_config` | `config.YamlConfigLoader.load(Path, Map)` and `.load(Path)` | Env-override isolation preserved; Tier 5 J4. |
| `src/common/envelope.py::VALID_GAP_REASONS` | `envelope.GapReasons.VALID` (`Set<String>`) | Tier 5 M6. |
| `src/common/envelope.py::DATA_ENVELOPE_FIELDS` | (implicit in `DataEnvelope`'s `@JsonPropertyOrder`) | Not ported as a separate Set — the record is the source of truth. |
| `src/common/envelope.py::GAP_ENVELOPE_FIELDS` | (implicit in `GapEnvelope`) | Ditto. |
| `src/common/envelope.py::BROKER_COORD_FIELDS` | (implicit in `BrokerCoordinates`) | Ditto. |
| `src/common/envelope.py::_SENTINEL` | **not ported** | Java uses `null` directly with `@JsonInclude(NON_NULL)`; the Python sentinel disambiguates "not provided" vs "provided as None", which Java never does (nullable optional fields render as missing when null). |
| `src/common/envelope.py::create_data_envelope` | `envelope.DataEnvelope.create(...)` static factory | Tier 5 B1, E1, E2, B4-scope. |
| `src/common/envelope.py::create_gap_envelope` | `envelope.GapEnvelope.create(...)` + `.createWithRestartMetadata(...)` | Two factories; compact ctor validates reason. |
| `src/common/envelope.py::serialize_envelope` | `envelope.EnvelopeCodec.toJsonBytes(Object)` | Tier 5 B2; ASCII-compact JSON, no trailing `\n`. |
| `src/common/envelope.py::deserialize_envelope` | `EnvelopeCodec.readData`, `.readGap`, `.readTree` | Tier 5 B3. |
| `src/common/envelope.py::add_broker_coordinates` | `EnvelopeCodec.withBrokerCoordinates(env, BrokerCoordinates)` | Returns a NEW record instance (Tier 2 #12 — no in-place mutation). The JSON emitted after calling this mirrors Python's post-mutation output — see §6. |
| `src/common/health_server.py::start_health_server` | `health.HealthServer(port, readyCheck, metricsSource).start()` | Constructor-injected deps (Tier 2 #11). |
| `src/common/jsonl.py::read_jsonl` | `jsonl.JsonlReader.readAll(Path, ObjectMapper)` | Drops bad lines. |
| `src/common/jsonl.py::append_jsonl` | `jsonl.JsonlWriter.appendOne(Path, JsonNode, ObjectMapper)` | Parent dir created; single syscall per record. |
| `src/common/logging.py::setup_logging` | `logging.LogInit.setLevel(String)` + `logback.xml` | Primary config is declarative; programmatic entry point preserves the level argument. |
| `src/common/logging.py::get_logger` | `logging.StructuredLogger.of(Class)` | SLF4J per-class; Tier 5 H1. |
| `src/common/logging.py::_json_dumps` | **not ported** | Logstash encoder handles JSON output; structlog's JSON renderer has no direct Java equivalent. |
| `src/common/service_runner.py::run_service` | **not ported to `common`** | Per §3.3 above and Tier 2 #11 (explicit wiring per service's `Main`). |
| `src/common/system_identity.py::get_host_boot_id` | `identity.SystemIdentity.getHostBootId()` | Tier 5 M7-adjacent (same module). |

Dropped symbols: `_SENTINEL`, `_json_dumps`, `run_service`, `cancel_tasks`, the three `*_FIELDS` frozensets, `default_archive_dir` (folded into `YamlConfigLoader.defaultArchiveDir()`), and `uvloop.install()`. All drops are structural (framework-free, record-immutable, structured-concurrency) and accompanied by the rule or design §1 citation above.

Internal calls (mapping §4):
- `load_config` → `_normalize_env_overrides` → `_apply_env_overrides` ⇒ `YamlConfigLoader.load` → `EnvOverrides.normalize` → `EnvOverrides.apply`.
- Pydantic `model_validate` ⇒ Jackson `convertValue` + Hibernate `Validator.validate`.
- `create_data_envelope` internal calls to `time.time_ns()` + `hashlib.sha256` ⇒ `Clocks.systemNanoClock().nowNs()` + `Sha256.hexDigestUtf8`.
- `create_gap_envelope` optional-sentinel loop ⇒ single compact-ctor invocation with nullable record components + `@JsonInclude(NON_NULL)`.

## 5. Library mapping

| Python dep (mapping §6 / observed) | Java replacement (version) | Source in `libs.versions.toml` | Justification |
|---|---|---|---|
| `orjson` | `jackson-databind` 2.17.2 + `jackson-dataformat-yaml` 2.17.2 | `jackson` ref | Spec §1.3; Tier 5 B1–B3. Raw-text passthrough via `String`. |
| `pydantic` + `pydantic-settings` | Java 21 records + Hibernate Validator 8.0.1.Final | `hibernateValidator` ref | Spec §1.3; Tier 5 J1–J4. |
| `pyyaml` | `jackson-dataformat-yaml` 2.17.2 | `jackson` ref | Spec §1.3; Tier 5 B7. Same mapper family as envelope. |
| `structlog` | SLF4J 2.0.13 + Logback 1.5.8 + Logstash encoder 7.4 | `slf4j`, `logback`, `logstashEncoder` refs | Spec §1.3; Tier 2 #15, Tier 5 H1–H3. |
| `aiohttp` (for `/health`, `/ready`, `/metrics`) | `com.sun.net.httpserver.HttpServer` (JDK-builtin) | (no dep) | JDK built-in; virtual-thread friendly; zero framework. Spec §1.3 does not mandate aiohttp's replacement for the *health* server specifically (it lists `java.net.http.HttpClient` for outbound). The built-in `HttpServer` is the spec-approved equivalent because it is framework-free and implied by Tier 2 #11. |
| `prometheus-client` (for `/metrics` output) | `micrometer-registry-prometheus` 1.13.4 | `micrometer` ref | Spec §1.3; Tier 5 H4-H6. `common` only exposes a `MetricsSource` `Supplier<byte[]>` — the actual registry lives in writer/collector. |
| `uvloop` | — (deleted, Tier 5 A8) | (n/a) | No Java equivalent needed; virtual threads. |
| `asyncio` | Java 21 virtual threads | (JDK) | Spec §1.3. |
| (testing) `pytest` | JUnit 5 (Jupiter) 5.11.0 | `junit` ref | Spec §1.3. |
| (testing) `testcontainers[kafka]` | `testcontainers:kafka` 1.20.1 | `testcontainers` ref | Spec §1.3; Tier 5 L2. |
| (testing) `assertj` | `assertj-core` 3.26.3 | `assertj` ref | Spec-consistent testing DSL. |

**No deviations from spec §1.3.** The only non-listed entry (JDK built-in `HttpServer`) is strictly a substitute where the spec is silent on inbound HTTP and Tier 2 #11 forbids adding a server framework.

## 6. Data contracts

### 6.1 Envelope canonical JSON byte layout

#### Data envelope

Field order is **locked** by `@JsonPropertyOrder`:

```
v, type, exchange, symbol, stream, received_at,
exchange_ts, collector_session_id, session_seq, raw_text, raw_sha256
```

| Field | Java type | Python type | JSON shape | Notes |
|---|---|---|---|---|
| `v` | `int` | int | number | Constant `1`. |
| `type` | `String` | str | string | Constant `"data"`. |
| `exchange` | `String` | str | string | e.g. `"binance"`. |
| `symbol` | `String` | str | string | Lowercase (Tier 5 M1). |
| `stream` | `String` | str | string | e.g. `"trades"`. |
| `received_at` | `long` | int (ns) | number | Nanoseconds since epoch (Tier 5 E2). |
| `exchange_ts` | `long` | int (ms/ns) | number | May exceed 2^31 (Tier 5 E1, M5 watch-out). |
| `collector_session_id` | `String` | str | string | `{collectorId}_{yyyy-MM-dd'T'HH:mm:ss'Z'}` — NO fractional seconds (Tier 5 M7). |
| `session_seq` | `long` | int | number | `-1L` is sentinel for writer-injected (Tier 5 M10). |
| `raw_text` | `String` | str | string (JSON text form) | **Byte-for-byte passthrough**. UTF-8 round-trip. Tier 1 #7. |
| `raw_sha256` | `String` | str (hex) | string | 64 lowercase hex chars of SHA-256 over `rawText.getBytes(UTF_8)` (Tier 1 #2; Tier 5 E1/E2 not applicable). |

#### Gap envelope

`@JsonPropertyOrder` + `@JsonInclude(NON_NULL)`:

```
v, type, exchange, symbol, stream, received_at,
collector_session_id, session_seq, gap_start_ts, gap_end_ts, reason, detail,
component, cause, planned, classifier, evidence, maintenance_id
```

| Field | Java type | Notes |
|---|---|---|
| `v` | `int` | `1` |
| `type` | `String` | `"gap"` |
| `exchange` | `String` | — |
| `symbol` | `String` | — |
| `stream` | `String` | — |
| `received_at` | `long` | ns (Tier 5 E2) |
| `collector_session_id` | `String` | Tier 5 M7 |
| `session_seq` | `long` | `-1L` sentinel for writer-injected (Tier 5 M10) |
| `gap_start_ts` | `long` | ns |
| `gap_end_ts` | `long` | ns |
| `reason` | `String` | MUST be in `GapReasons.VALID` (Tier 5 M6) |
| `detail` | `String` | free text |
| `component` | `String` (nullable) | omitted when null |
| `cause` | `String` (nullable) | omitted when null |
| `planned` | `Boolean` (nullable) | omitted when null |
| `classifier` | `String` (nullable) | omitted when null |
| `evidence` | `Map<String,Object>` (nullable) | omitted when null |
| `maintenance_id` | `String` (nullable) | omitted when null |

#### Broker coordinates

`_topic`, `_partition` (int, ≤2^15 in practice — Tier 5 M8), `_offset` (long; `-1L` sentinel for synthetic — Tier 5 M9).

Python mutates the envelope dict to append `_topic`, `_partition`, `_offset` at the end. Java records are immutable, so `EnvelopeCodec.withBrokerCoordinates(env, coords)` produces the serialization by writing the envelope JSON, inserting these three fields at the end of the object:

**Serialization strategy** (implemented in `EnvelopeCodec.toJsonBytes` when receiving a "`EnvelopeWithBroker`"-like wrapper):
- Option A (preferred): introduce two thin wrapper records `DataWithBroker(DataEnvelope env, BrokerCoordinates c)` and `GapWithBroker(GapEnvelope env, BrokerCoordinates c)` with `@JsonUnwrapped` on both component fields. Jackson respects `@JsonPropertyOrder` on each unwrapped record and emits: `{ ...envelope fields..., "_topic": ..., "_partition": ..., "_offset": ... }` — matching Python's post-mutation bytes.
- Option B (fallback if `@JsonUnwrapped` interacts badly with `@JsonPropertyOrder`): build a `LinkedHashMap<String,Object>`, copy in envelope fields in order, append the three broker fields, serialize. Jackson's `ObjectMapper` respects insertion order when `ORDER_MAP_ENTRIES_BY_KEYS=false` (the mapper config mandates this).
- **Developer picks Option A first**; if a parity-fixture test fails with a byte-diff, fall back to Option B. Open Q2 in §11.

### 6.2 ObjectMapper configuration

Created once by `EnvelopeCodec.newMapper()`:

```
ObjectMapper m = new ObjectMapper();
m.disable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);   // Tier 5 B1
m.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, false);
m.disable(SerializationFeature.INDENT_OUTPUT);              // Tier 5 B2
m.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
// Do NOT apply a global SNAKE_CASE naming strategy — we use explicit @JsonProperty (Tier 5 J1).
// Number handling: leave defaults — BigDecimal handling is currently N/A (Tier 5 B5).
```

YAML mapper: `new ObjectMapper(new YAMLFactory())` with the same features applied.

### 6.3 Config records

All fields listed in §2.2. Highlights where parity matters:

- `RedpandaConfig.retentionHours` — `@Min(12)`, Hibernate Validator throws on violation → `YamlConfigLoader` wraps as `CryptoLakeConfigException` (Tier 5 J3; mapping §10 G2).
- `BinanceExchangeConfig.symbols` — compact ctor lowercases with `Locale.ROOT` (Tier 5 M1) and `List.copyOf` for immutability (Tier 2 #12).
- `BinanceExchangeConfig.writerStreamsOverride` — may be `null` (no override) or `List<String>`; compact ctor appends `"depth_snapshot"` if `"depth"` present and not already there.
- `WriterConfig.baseDir` — default pulls from `HOST_DATA_DIR` env once at record-default construction time, matching Python's `default_factory`.
- `ProducerConfig.bufferCaps` — default `Map.of("depth", 80_000, "trades", 10_000)` via compact ctor (Tier 5 J2).

### 6.4 JSONL ledger records

`JsonlReader.readAll` returns `List<JsonNode>`; `JsonlWriter.appendOne` accepts a `JsonNode`. No schema is imposed — the lifecycle ledger uses `{ts, event_type, ...}` but that is writer's concern.

## 7. Error model

Top-level exception for the module:

```
public final class CryptoLakeConfigException extends RuntimeException {
    public CryptoLakeConfigException(String msg) { super(msg); }
    public CryptoLakeConfigException(String msg, Throwable cause) { super(msg, cause); }
}
```

### 7.1 What gets wrapped

| Source | Thrown as | Catch site |
|---|---|---|
| `java.nio.file.NoSuchFileException` on config path | `CryptoLakeConfigException` | `YamlConfigLoader.load` (ports Python's `FileNotFoundError` to an unchecked type; Tier 2 #13). |
| `IOException` reading YAML | `CryptoLakeConfigException` | `YamlConfigLoader.load`. |
| `JsonProcessingException` during YAML parse / `convertValue` | `CryptoLakeConfigException` | `YamlConfigLoader.load`. |
| `ConstraintViolationException` from Hibernate Validator | `CryptoLakeConfigException` with violation list in message | `YamlConfigLoader.load`. |
| Bad gap reason in `GapEnvelope` compact ctor | `IllegalArgumentException` | Caller (collector/writer); not wrapped in common. Tier 5 M6. |
| `NoSuchAlgorithmException` from `MessageDigest.getInstance("SHA-256")` | `IllegalStateException` (unchecked) | `Sha256`. |
| `IOException` in `JsonlReader`/`JsonlWriter` | Propagated as unchecked `UncheckedIOException` | Callers. JSON parse errors inside `readAll` are silently skipped. |
| `IOException` in `SystemIdentity.getHostBootId` | Caught, returns `"unknown"` sentinel. | Same as Python. |
| `IOException` in `HealthServer` start/stop | Propagated as `UncheckedIOException`. | Caller's `Main`. |

### 7.2 What is retried

- **Nothing in `common`.** Retry logic (three-retry exponential backoff per Tier 5 G3) lives in the writer (PG state saves) and the collector (HTTP snapshot fetches). `common` is a library — failures are reported, never retried.

### 7.3 What kills the process

- Any unwrapped `OutOfMemoryError` / `StackOverflowError` propagates per Tier 5 G1 (never caught).
- A `CryptoLakeConfigException` thrown from `YamlConfigLoader.load` will propagate out of the service's `Main` and terminate with a non-zero exit code — consistent with Tier 2 #16 "fail fast on invariant violation".

### 7.4 Best-effort shutdown paths

Tier 5 G1: `HealthServer.stop()` catches `Exception` (NOT `Throwable`) internally around `server.stop(1)` and logs via `StructuredLogger`; the outer `Main` still considers shutdown complete. The catch is commented `// best-effort shutdown; never block main shutdown path`.

## 8. Test plan

All tests JUnit 5 under `cryptolake-java/common/src/test/java/com/cryptolake/common/`. Each test class has a file-level comment `// ports: tests/unit/...` (Tier 3 #17). Integration tests marked with JUnit's `@Tag("integration")` and gated by `@EnabledIfEnvironmentVariable` on `DOCKER_HOST` (matches Python's skip-if-no-docker behavior).

### Unit tests

| Python test | Java test | Fixture? |
|---|---|---|
| `tests/unit/test_async_utils.py::test_cancel_tasks_cancels_and_clears` | **not ported** — helper deleted (Tier 5 A7); documented in `completion.md §2`. | — |
| `tests/unit/test_async_utils.py::test_cancel_tasks_handles_empty_list` | **not ported** — same rationale. | — |
| `tests/unit/test_config.py::test_load_valid_config` | `config.YamlConfigLoaderTest#loadValidConfig` | new `config.sample.yaml` fixture |
| `tests/unit/test_config.py::test_config_symbols_are_lowercase` | `config.BinanceExchangeConfigTest#symbolsAreLowercased` | inline |
| `tests/unit/test_config.py::test_config_all_streams_enabled` | `config.YamlConfigLoaderTest#allStreamsEnabled` | same fixture |
| `tests/unit/test_config.py::test_config_depth_snapshot_override` | `config.YamlConfigLoaderTest#depthSnapshotOverride` | — |
| `tests/unit/test_config.py::test_config_writer_defaults` | `config.YamlConfigLoaderTest#writerDefaultsApplied` | — |
| `tests/unit/test_config.py::test_config_retention_minimum_rejected` | `config.YamlConfigLoaderTest#retentionBelowMinimumRejected` | assert `CryptoLakeConfigException` |
| `tests/unit/test_config.py::test_config_env_override` | `config.YamlConfigLoaderTest#envOverrideBeatsYaml` | — |
| `tests/unit/test_config.py::test_host_data_dir_alias_overrides_writer_base_dir` | `config.YamlConfigLoaderTest#hostDataDirAlias` | — |
| `tests/unit/test_config.py::test_config_missing_file_raises` | `config.YamlConfigLoaderTest#missingFileThrows` | asserts `CryptoLakeConfigException` (wrapping) |
| `tests/unit/test_config.py::test_disabled_stream_no_depth_snapshot` | `config.BinanceExchangeConfigTest#disabledStreamNoDepthSnapshot` | — |
| `tests/unit/test_config.py::test_enabled_stream_list` | `config.BinanceExchangeConfigTest#enabledStreamList` | — |
| `tests/unit/test_config.py::test_writer_streams_override_auto_includes_depth_snapshot` | `config.BinanceExchangeConfigTest#writerOverrideAutoIncludesSnapshot` | — |
| `tests/unit/test_config.py::test_writer_streams_override_no_duplicate_depth_snapshot` | `config.BinanceExchangeConfigTest#writerOverrideNoDuplicateSnapshot` | — |
| `tests/unit/test_config.py::test_writer_streams_override_none_by_default` | `config.BinanceExchangeConfigTest#writerOverrideNullByDefault` | — |
| `tests/unit/test_config.py::test_env_override_does_not_bleed_os_environ` | `config.YamlConfigLoaderTest#explicitEnvOverrideIsolatesFromSystemEnv` | sets a misleading `System.getenv` via a test-only indirection — see Q3 below |
| `tests/unit/test_envelope.py::test_create_data_envelope` | `envelope.DataEnvelopeTest#createDataEnvelope` | — |
| `tests/unit/test_envelope.py::test_create_gap_envelope` | `envelope.GapEnvelopeTest#createGapEnvelope` | — |
| `tests/unit/test_envelope.py::test_data_envelope_raw_sha256_integrity` | `envelope.DataEnvelopeTest#rawSha256IsOverRawTextBytes` | fixture `raw_text` from `parity-fixtures/` if present, else inline |
| `tests/unit/test_envelope.py::test_envelope_received_at_is_nanoseconds` | `envelope.DataEnvelopeTest#receivedAtIsNanoseconds` | fixed `Clocks.fixed(...)` |
| `tests/unit/test_envelope.py::test_gap_reason_values` | `envelope.GapReasonsTest#validSetContainsExpected` | — |
| `tests/unit/test_envelope.py::test_create_restart_gap_envelope` | `envelope.GapEnvelopeTest#restartMetadataIncluded` | — |
| `tests/unit/test_envelope.py::test_restart_gap_envelope_optional_fields_omitted` | `envelope.GapEnvelopeTest#optionalFieldsOmittedWhenNull` | — |
| `tests/unit/test_envelope.py::test_restart_gap_envelope_partial_optional_fields` | `envelope.GapEnvelopeTest#partialOptionalFields` | — |
| `tests/unit/test_envelope.py::test_non_restart_gap_ignores_extra_fields` | `envelope.GapEnvelopeTest#nonRestartGapOmitsMetadata` | — |
| `tests/unit/test_envelope.py::test_checkpoint_lost_is_valid_gap_reason` | `envelope.GapReasonsTest#checkpointLostValid` | — |
| `tests/unit/test_envelope.py::test_create_gap_envelope_with_checkpoint_lost` | `envelope.GapEnvelopeTest#createWithCheckpointLost` | — |
| `tests/unit/test_envelope.py::test_gap_invalid_reason_raises` | `envelope.GapEnvelopeTest#invalidReasonThrows` | expects `IllegalArgumentException` |
| `tests/unit/test_envelope.py::test_serialize_data_envelope_to_bytes` | `envelope.EnvelopeCodecTest#serializeDataEnvelope` | fixture `data_envelope.expected.json` (Tier 3 #19 parity) |
| `tests/unit/test_envelope.py::test_serialize_gap_envelope_to_bytes` | `envelope.EnvelopeCodecTest#serializeGapEnvelope` | fixture `gap_envelope.expected.json` |
| `tests/unit/test_envelope.py::test_deserialize_envelope_round_trip` | `envelope.EnvelopeCodecTest#roundTripPreservesFields` | — |
| `tests/unit/test_envelope.py::test_add_broker_coordinates` | `envelope.EnvelopeCodecTest#withBrokerCoordinatesAppendsFields` | asserts byte order matches Python post-mutation `orjson.dumps` |
| `tests/unit/test_logging.py::test_json_output` | `logging.StructuredLoggerTest#jsonOutputContainsEventAndKvPairs` | uses Logback test appender (`ListAppender`) |
| `tests/unit/test_logging.py::test_warning_level_suppresses_info` | `logging.LogInitTest#warningLevelSuppressesInfo` | — |
| `tests/unit/test_system_identity.py::test_env_override_takes_precedence` | `identity.SystemIdentityTest#envOverridePrecedence` | uses JUnit `@SetEnvironmentVariable` (JUnit Pioneer) OR a test-only indirection — see Q4 |
| `tests/unit/test_system_identity.py::test_reads_proc_boot_id` | `identity.SystemIdentityTest#readsProcBootId` | `@EnabledOnOs(OS.LINUX)` |
| `tests/unit/test_system_identity.py::test_strips_whitespace_from_boot_id` | `identity.SystemIdentityTest#stripsWhitespace` | — |
| `tests/unit/test_system_identity.py::test_fallback_when_proc_unavailable` | `identity.SystemIdentityTest#fallbackWhenProcUnavailable` | `@DisabledOnOs(OS.LINUX)` or mock |
| `tests/unit/test_system_identity.py::test_fallback_on_permission_error` | `identity.SystemIdentityTest#fallbackOnPermissionError` | simulate via a custom Path provider or mock FileSystem |
| `tests/unit/test_system_identity.py::test_return_type_is_str` | `identity.SystemIdentityTest#alwaysReturnsString` | trivial |
| `tests/unit/test_system_identity.py::test_env_override_strips_whitespace` | `identity.SystemIdentityTest#envOverrideStrippedOfWhitespace` | — |
| `tests/unit/test_host_lifecycle_agent.py` (ledger I/O subset) | `jsonl.JsonlReaderTest` + `jsonl.JsonlWriterTest` | ported selectively: partial-line tolerance, empty/missing file, parent-dir-create |

### Integration tests

| Python test | Java test | Container |
|---|---|---|
| `tests/integration/test_redpanda_roundtrip.py::test_produce_consume_envelope` | `envelope.EnvelopeRedpandaRoundtripIT` under `src/test/java`, `@Tag("integration")` | `KafkaContainer("redpandadata/redpanda:v24.1.2").asCompatibleSubstituteFor("confluentinc/cp-kafka")` with `.withReuse(true)` (Tier 5 L2) |

### New tests added (not in Python)

- `envelope.EnvelopeCodecTest#fieldOrderMatchesPython` — asserts serialized JSON of a fixed envelope equals a byte-for-byte fixture file `parity-fixtures/common/data_envelope.golden.json`. Enforces Tier 3 #21 + Tier 5 B1.
- `health.HealthServerTest#readyReturns503WhenAnyCheckFails` — two-check scenario.
- `health.HealthServerTest#metricsIsServedUnchanged` — passthrough of `MetricsSource` bytes.
- `kafka.TopicNamesTest#forStreamFormat` — asserts `"" + "binance" + "." + "trades"` matches Python-produced topic.
- `util.Sha256Test#matchesPythonHashlib` — fixture pairs of `{"text":"...","sha256":"..."}` taken from Python `hashlib.sha256(...).hexdigest()`.

## 9. Metrics plan

The `common` module itself **owns zero named metrics** — it provides the plumbing for services to publish. Concretely:

- `HealthServer` accepts a `MetricsSource` `Supplier<byte[]>` and serves the bytes at `/metrics`. `common` does not create a Prometheus registry.
- The writer/collector modules each own one `PrometheusMeterRegistry` (Tier 2 #14) and pass `registry::scrape` (as `byte[]`) into `HealthServer`. See writer's `WriterMetrics` and collector's `CollectorMetrics` classes (not in this module's scope).

Parity test (generated, Tier 3 #18): lives in writer / collector modules — not `common`. This section exists to explicitly confirm none are owed here.

**What `common` WILL need if a later module change adds metric infrastructure here**:
- `MeterRegistryHolder` (util) — but this is deferred. Current design explicitly excludes a shared registry from `common` to prevent accidental framework-style wiring (Tier 2 #11).

## 10. Rule compliance

### Tier 1 — Invariants

1. **`raw_text` captured pre-parse** — honored by `envelope.DataEnvelope.create(rawText, ...)`: rawText is received as a `String` parameter the caller has already extracted byte-for-byte pre-parse. `common` never parses WebSocket frames; enforcement lives at the collector boundary. Documented on the factory Javadoc so collector developers honor it.
2. **`raw_sha256` computed once at capture** — honored by `envelope.DataEnvelope.create()`: `rawSha256 = Sha256.hexDigestUtf8(rawText)` is computed in the factory and stored as an immutable record component. No setter exists (Tier 2 #12); recomputation is impossible.
3. **Disabled streams emit zero artifacts** — N/A to `common`: stream-enablement decisions live in the collector. `common` merely carries the `StreamsConfig` + `BinanceExchangeConfig.getEnabledStreams()` list.
4. **Offsets committed only after fsync flush** — N/A to `common`: flush-commit ordering owner lives in the writer (Tier 5 C8 mandates a single-function owner in writer's `OffsetCommitCoordinator`).
5. **Every gap emits metric + log + archived record** — partially honored by `envelope.GapEnvelope.create` + `GapReasons.requireValid`: the archived-record contract is enforced here (reason vocabulary + field order). Metric and log emission are writer concerns.
6. **Recovery prefers replay over reconstruction** — N/A to `common`: recovery strategy lives in the writer.
7. **JSON codec preserves `raw_text` byte-for-byte** — honored by `envelope.EnvelopeCodec.newMapper()` configuration (Tier 5 B1/B2/B3): `SORT_PROPERTIES_ALPHABETICALLY=false`, `INDENT_OUTPUT=false`, `ORDER_MAP_ENTRIES_BY_KEYS=false`, and `rawText` is typed `String` in the record (never `JsonNode`). Gate 3 parity fixture + `EnvelopeCodecTest#fieldOrderMatchesPython` enforce this.

### Tier 2 — Java practices

8. **Java 21 only** — honored by root `build.gradle.kts` `sourceCompatibility = JavaVersion.VERSION_21`. No `--enable-preview` needed in `common` (no `StructuredTaskScope` usage).
9. **No `synchronized` around blocking calls** — honored trivially: `common` has no `synchronized` blocks anywhere. `HealthServer` delegates request handling to a virtual-thread executor; no shared mutable state.
10. **No `Thread.sleep` in hot paths** — honored: no `Thread.sleep` anywhere in `common`. Bounded-blocking I/O is used in health server request handlers only; non-hot path.
11. **No reflection-heavy frameworks** — honored: no Spring/Micronaut/Quarkus. Jackson and Hibernate Validator use reflection, but they are Tier 5 B6 / J3-sanctioned. DI is constructor-injected or static-helper (Tier 5 J1 records, `EnvelopeCodec.newMapper()`).
12. **Immutable records, no setters** — honored by all data carriers: `DataEnvelope`, `GapEnvelope`, `BrokerCoordinates`, every `config/*` record. `EnvelopeCodec.withBrokerCoordinates` returns a NEW wrapper rather than mutating an existing envelope.
13. **No checked exception leaks** — honored: `CryptoLakeConfigException extends RuntimeException`. `JsonProcessingException`, `IOException`, `ConstraintViolationException` all wrapped inside `YamlConfigLoader.load`. `UncheckedIOException` is used where wrapping into `CryptoLakeConfigException` would be semantically wrong (jsonl helpers). Tier 5 G2.
14. **Single `HttpClient` / `KafkaProducer` / `ObjectMapper` per service** — honored by design: `EnvelopeCodec.newMapper()` is a factory, and the service's `Main` creates ONE instance and passes to every component. `common` does not create any `HttpClient` or `KafkaProducer` — those live in the services. Tier 5 B6.
15. **JSON logs via Logback + Logstash + MDC** — honored by `logging/logback.xml` classpath resource with `LogstashEncoder`, plus `StructuredLogger.mdc(...)` for per-thread key-value context. Tier 5 H1–H3.
16. **Fail-fast on invariant violation** — honored: `GapEnvelope` compact ctor throws on invalid reason; `YamlConfigLoader.load` throws `CryptoLakeConfigException` on any schema/validation failure. No silent fallback.

### Tier 3 — Parity rules

17. **Every Python test has a JUnit 5 counterpart with trace comment** — honored by the test plan (§8): each ported test carries a `// ports: tests/unit/...` file-header comment. Dropped tests (`test_cancel_tasks_*`) explicitly documented in the `completion.md §2 Deviations` section by the Developer.
18. **Prometheus metric names + labels diff-match Python** — N/A to `common` in the owner sense, but `HealthServer` serves `MetricsSource` bytes verbatim — it does not transform the output. Registries and parity generator live in writer/collector.
19. **`raw_text` / `raw_sha256` byte-identity via fixture corpus** — honored by `EnvelopeCodecTest#fieldOrderMatchesPython` (writes fixture through Java codec, compares to Python-generated `parity-fixtures/common/*.golden.json`) and `util.Sha256Test#matchesPythonHashlib`. Fixtures captured during `/port-init`.
20. **Python `verify` CLI passes on Java archives** — N/A to `common` (no archives produced). Writer's archive path is the relevant module.
21. **Envelope field order follows Python canonical order** — honored by `@JsonPropertyOrder` on `DataEnvelope` and `GapEnvelope` matching Python dict insertion order exactly (see §6.1). Tier 5 B1 + EnvelopeCodecTest.

## 11. Open questions for developer

Q1. **Logstash encoder `message` vs `event` key.** Python's structlog emits `event` as the top-level key for the event name; Logstash's default encoder emits `message`. Options:
   (a) accept the `message` key — users/log-aggregators update their queries;
   (b) use `LogstashEncoder`'s `<provider>` mechanism to rename `message → event`;
   (c) configure `StructuredLogger` to also emit an explicit `kv("event", eventName)` arg so the field appears under both names during a migration window.

   The Python test `test_json_output` asserts `event == "<event name>"`. The clean port is (b); if that requires a custom provider class, option (c) is acceptable. Developer MUST NOT silently accept (a) — escalate to user if (b)/(c) prove infeasible.

Q2. **`@JsonUnwrapped` + `@JsonPropertyOrder` interaction.** Option A in §6.1 uses `@JsonUnwrapped` on two wrapper records. If Jackson reorders or breaks field order under this annotation combo, fall back to Option B (LinkedHashMap). A fixture test will catch mis-ordering on the first run; escalate if Option B also proves incompatible.

Q3. **`System.getenv()` for tests and runtime.** `YamlConfigLoader.load(Path)` (no env map) must consult `System.getenv()`. For the test `explicitEnvOverrideIsolatesFromSystemEnv`, we need to assert that the 2-arg overload does NOT read `System.getenv()` even when it contains conflicting values. Two approaches:
   (a) use the JUnit Pioneer `@SetEnvironmentVariable` extension;
   (b) inject a `Supplier<Map<String,String>>` behind a package-private hook used ONLY by `load(Path)`.
   Developer should pick (b) to keep the test deps minimal — a package-private static field `Supplier<Map<String,String>> envSupplier = System::getenv` that test code can swap.

Q4. **System env tests on macOS.** The boot ID fallback test (`fallbackWhenProcUnavailable`) runs fine on macOS, but `readsProcBootId` requires a readable `/proc/sys/kernel/random/boot_id`. Use `@EnabledOnOs(OS.LINUX)` for the positive case and `@DisabledOnOs(OS.LINUX)` for the negative case. For permission-error simulation, prefer a dependency-injection seam on the file path rather than filesystem-level tricks — introduce a package-private constant holder `Path bootIdPath = Path.of(BOOT_ID_PATH)` that tests can override via reflection-free package access.

Q5. **YAML value coercion in `EnvOverrides.apply`.** Python's `_apply_env_overrides` splits comma-containing values into a list of trimmed strings. If YAML downstream types the field as `int` or `boolean`, we rely on Jackson `convertValue` to coerce. Confirm: for the known use case `REDPANDA__BROKERS=host1,host2` (list<string>), Jackson handles a `List<String>` from `String[]`-split. If a future override targets a non-string field, Developer MUST add a test — but no action is needed now.

Q6. **Parity fixture locations.** §8 references `parity-fixtures/common/data_envelope.golden.json` and `gap_envelope.golden.json`. These are produced during `/port-init` (per Tier 3 #19). If the `common` fixtures are not yet captured at Developer dispatch time, Developer pauses and asks the orchestrator to run the fixture-capture step. No such fixtures should be hand-written.

None of Q1–Q6 block design approval; each has a preferred path with a fallback. Developer may proceed and escalate via `completion.md §4` only if all preferred paths fail.
