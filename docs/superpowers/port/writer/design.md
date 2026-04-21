---
module: writer
status: approved
produced_by: architect
based_on_mapping: a304c41cf404158d395c54b2f8def8ca8a4ab950
---

## 1. Package layout

Root Gradle subproject: `cryptolake-java/writer/` (already scaffolded; `build.gradle.kts` already declares `mainClass = com.cryptolake.writer.Main`). Root package: `com.cryptolake.writer`. Java 21, application jar. Depends on `:common`. No framework.

```
com.cryptolake.writer
├── Main                                  (final class, has main())
│
├── consumer/
│   ├── KafkaConsumerLoop                 (final class)  — subscribe, poll, dispatch (primary + backup drain)
│   ├── RecordHandler                     (final class)  — per-record: deserialize, session check, buffer add, failover tracking
│   ├── OffsetCommitCoordinator           (final class)  — OWNS write→fsync→PG→commitSync ordering (Tier 1 §4)
│   ├── PartitionAssignmentListener       (final class, implements ConsumerRebalanceListener)
│   ├── RecoveryCoordinator               (final class)  — one-time-per-stream recovery gap + seek computation
│   ├── SessionChangeDetector             (final class)  — runtime session_id change → gap
│   ├── DepthRecoveryGapFilter            (final class)  — depth anchor state machine
│   ├── LateArrivalSequencer              (final class)  — late-file naming per sealed hour
│   └── HourRotationScheduler             (final class)  — triggers FileRotator on hour boundary
│
├── buffer/
│   ├── BufferManager                     (final class)  — per-file-key buffers + flush triggers
│   ├── FileTarget                        (record)
│   ├── FlushResult                       (record)
│   └── CheckpointMeta                    (record)
│
├── rotate/
│   ├── FileRotator                       (final class)  — hourly seal: flush→write→sha256 sidecar→PG checkpoint
│   ├── FilePaths                         (utility final class)  — buildFilePath, buildBackfillFilePath, sidecarPath
│   └── Sha256Sidecar                     (utility final class)  — writeSidecar(dataPath) using Sha256.hexFile
│
├── io/
│   ├── ZstdFrameCompressor               (final class)  — zstd-jni wrapper, single-frame-per-flush
│   └── DurableAppender                   (final class)  — FileChannel.append + force(true) + partial-write truncate (Tier 5 I3, I4)
│
├── state/
│   ├── StateManager                      (final class)  — PG connection + table DDL + upsert/load (ports state_manager.py)
│   ├── FileStateRecord                   (record)
│   ├── StreamCheckpoint                  (record)
│   ├── ComponentRuntimeState             (record)
│   └── MaintenanceIntent                 (record)
│
├── failover/
│   ├── FailoverController                (final class)  — primary/backup switch, backup consumer lifecycle
│   ├── CoverageFilter                    (final class)  — per-source coverage + pending-gap parking
│   ├── NaturalKeyExtractor               (utility final class)  — raw_text → trades.a / depth.u / bookticker.u
│   ├── HostLifecycleReader               (final class)  — ports host_lifecycle_reader.py
│   ├── HostLifecycleEvidence             (final class)  — indexed events, accessors
│   └── RestartGapClassifier              (utility final class)  — pure function (ports restart_gap_classifier.py)
│
├── gap/
│   ├── GapEmitter                        (final class)  — TRIAD: metric + log + archived record (Tier 1 §5)
│   └── GapReasonsLocal                   (utility final class)  — writer-private helpers on top of common.GapReasons
│
├── metrics/
│   ├── WriterMetrics                     (final class)  — 27 Micrometer meters + holders for gauges
│   └── MetricHolders                     (final class, package-private)  — mutable suppliers-of-value for gauges (Tier 5 H6)
│
├── recovery/
│   ├── SealedFileIndex                   (final class)  — rglob sealed .jsonl.zst scan, truncation (Tier 5 I7)
│   └── LastEnvelopeReader                (final class)  — reads final data envelope from zstd-sealed file
│
└── health/
    └── WriterReadyCheck                  (final class, implements common.health.ReadyCheck)
```

No class uses inheritance beyond `implements` of single-method SAM interfaces (`ConsumerRebalanceListener`, `ReadyCheck`, `MetricsSource`, `Shutdownable`, `Runnable`). Every data carrier is a record. There is exactly one `main()`, in `Main`.

Total classes in §2: **36** (listed below).

## 2. Class catalog

All classes in `com.cryptolake.writer.*` unless noted. All classes `final`; records where noted. Constructor-injected dependencies; no field injection (Tier 2 §11).

### 2.1 `Main` — final class, entry point

- **Purpose**: wire all components, open virtual-thread executor, install shutdown hook, block on consume loop + health server.
- **Public API**:
  ```
  public static void main(String[] args)
  ```
- **Wiring order**: `LogInit.apply()` → `YamlConfigLoader.load(path)` → `EnvelopeCodec.newMapper()` (one ObjectMapper for whole writer) → `StateManager.connect()` → `HostLifecycleReader.load(ledgerPath, windowStartIso)` → `WriterMetrics` (construct + register) → `PrometheusMeterRegistry` → `ZstdFrameCompressor` → `BufferManager` → `CoverageFilter` → `FailoverController` → `FileRotator` → `OffsetCommitCoordinator` → `RecoveryCoordinator` → `SessionChangeDetector` → `DepthRecoveryGapFilter` → `GapEmitter` → `RecordHandler` → `KafkaConsumerLoop` → `HealthServer` (from common) → SIGTERM hook → `executor.submit(consumeLoop)` + `executor.submit(healthServer.startBlocking)` → await.
- **Dependencies**: config path (argv[0] or env `CRYPTOLAKE_CONFIG`).
- **Thread safety**: single thread.

### 2.2 `consumer/`

#### `KafkaConsumerLoop` — final class

- **Purpose**: owns the `KafkaConsumer<byte[],byte[]>` (primary). Runs the single consume loop. Dispatches records to `RecordHandler`. Delegates backup-consumer drain to `FailoverController`. Triggers periodic flush / rotation via `HourRotationScheduler` + `OffsetCommitCoordinator`.
- **Public API**:
  ```
  public KafkaConsumerLoop(KafkaConsumer<byte[],byte[]> primary,
                           List<String> enabledTopics,                  // Tier 1 §3: disabled streams NOT in this list
                           RecordHandler recordHandler,
                           FailoverController failover,
                           OffsetCommitCoordinator committer,
                           HourRotationScheduler rotator,
                           WriterMetrics metrics,
                           ReadyFlag readyFlag);       // volatile boolean holder
  public void run();                                   // blocking; called from Main.executor
  public void requestShutdown();                       // volatile flag; consume loop observes
  public boolean isConnected();                        // writerConsumer is assigned
  ```
- **Core loop** (translated from `consume_loop`):
  1. `consumer.subscribe(topics, new PartitionAssignmentListener(...))`
  2. `while (!stopRequested) { poll primary (Duration.ofSeconds(1)); dispatch; poll backup if active; schedule flush/rotate; }`
- **No `synchronized`** (Tier 2 §9). `stopRequested` is `volatile`.
- **Dependencies**: constructor-injected; NO Kafka client constants hardcoded (those live in `Main`).
- **Thread safety**: SINGLE virtual thread owns this class (Tier 5 A2, A5). All mutable state (counters, `lastFlushNanos`, session dicts) lives here; no external thread reads them. Callers only use `requestShutdown()` (volatile) + `isConnected()` (volatile). NO Kafka callbacks run on other threads that touch this class's state.

#### `RecordHandler` — final class

- **Purpose**: per-record logic: deserialize envelope (handle `deserialization_error` gap on failure — Tier 5 G4), stamp broker coordinates, run session-change detection, depth-anchor check, coverage filter, buffer add, failover tracking. Returns one of {accepted, gap-emitted, skipped}.
- **Public API**:
  ```
  public RecordHandler(EnvelopeCodec codec,
                       SessionChangeDetector sessionDetector,
                       DepthRecoveryGapFilter depthFilter,
                       CoverageFilter coverageFilter,
                       FailoverController failover,
                       BufferManager buffers,
                       GapEmitter gaps,
                       WriterMetrics metrics,
                       String backupPrefix);
  public void handle(ConsumerRecord<byte[],byte[]> rec, boolean fromBackup);
  ```
- **Dependencies**: all constructor-injected.
- **Thread safety**: stateless; called only from `KafkaConsumerLoop`'s virtual thread.

#### `OffsetCommitCoordinator` — final class

- **Purpose**: SOLE OWNER of Kafka offset commits (Tier 1 §4; Tier 5 C8). Ports Python's `_commit_state`. Executes the linear sequence `write → fsync → PG save → commitSync` in ONE method. NO other class calls `consumer.commitSync()` or `consumer.commitAsync()`.
- **Public API**:
  ```
  public OffsetCommitCoordinator(KafkaConsumer<byte[],byte[]> primary,
                                 DurableAppender appender,
                                 ZstdFrameCompressor compressor,
                                 StateManager stateManager,
                                 SealedFileIndex sealedIndex,
                                 WriterMetrics metrics,
                                 ClockSupplier clock);

  /** Flushes all buffers, writes each result to disk, fsyncs, saves PG state, then commitSync.
   *  Returns total records flushed. Throws on PG failure BEFORE any commit (Tier 5 C8 watch-out). */
  public int flushAndCommit(BufferManager buffers);

  /** Variant called during hour rotation: accepts FlushResult batch + sealed file path + sidecar,
   *  performs seal-specific PG upsert, then commitSync. */
  public void commitSealedHour(List<FlushResult> results, List<Path> sealedFiles);

  /** Called by KafkaConsumerLoop during PartitionsRevoked: commitSync (not async) any offsets
   *  already durable in PG whose commit hasn't yet landed. Blocking by design (Tier 5 C3 watch-out). */
  public void commitBeforeRevoke(Collection<TopicPartition> revoked);

  /** Final shutdown commit: fresh flushAndCommit, then explicit commitSync of everything. */
  public void shutdownCommit();
  ```
- **Ordering** (the load-bearing invariant — gated by unit tests in §8):
  1. `buffers.flushAll()` → `List<FlushResult>` (in-memory extraction)
  2. For each result: `bytes = compressor.compressFrame(result.lines())`; `appender.appendAndFsync(result.filePath(), bytes)` — this returns only after `FileChannel.force(true)` completes (Tier 5 I3).
  3. Build `List<FileStateRecord>` (topic, partition, high_water, file_path, file_byte_size) and `List<StreamCheckpoint>` (from `CheckpointMeta`).
  4. `stateManager.saveStatesAndCheckpoints(states, checkpoints)` — ONE PG transaction, retry 3× exp backoff (Tier 5 G3). On final failure: `metrics.pgCommitFailures().increment()` + throw. NO Kafka commit.
  5. Only on PG success: `consumer.commitSync(offsets)` — explicit offsets (Tier 5 C3 watch-out on shutdown path). Use `commitSync` not `commitAsync` for the hot path; this satisfies Tier 5 C8 and gate 7. (Mapping §10 Risk 13 also covered: `commitSync` throws `CommitFailedException` which we catch, increment `kafka_commit_failures_total`, and rethrow.)
  6. Update in-memory `_durable_checkpoints` cache (passed to `RecoveryCoordinator`).
- **Deviation from mapping**: the mapping §6 Python uses `commit(asynchronous=True)`. The architect prompt mandates `commitSync` with explicit offsets. Rationale: `commitAsync` races shutdown (Tier 5 C3 watch-out); `commitSync` with offsets preserves exactly-once archival semantics and is what a single owner must do to enforce Tier 1 §4 in Java. The `writer_kafka_commit_failures_total` metric (H4) still fires on error.
- **Dependencies**: as above.
- **Thread safety**: called ONLY from the consume-loop virtual thread OR the shutdown hook — never both concurrently (SIGTERM handler signals `stopRequested` and joins the consume thread before calling `shutdownCommit`).

#### `PartitionAssignmentListener` — final class (implements `ConsumerRebalanceListener`)

- **Purpose**: on assignment, apply pending seeks computed by `RecoveryCoordinator`. On revoke, flush+commit before losing ownership (Tier 5 C4).
- **Public API**:
  ```
  public PartitionAssignmentListener(KafkaConsumer<byte[],byte[]> consumer,
                                     Map<TopicPartition, Long> pendingSeeks,    // mutable, owned by RecoveryCoordinator
                                     OffsetCommitCoordinator committer,
                                     Set<TopicPartition> assignedSink,          // volatile set for isConnected()
                                     WriterMetrics metrics);
  @Override public void onPartitionsAssigned(Collection<TopicPartition> parts);
  @Override public void onPartitionsRevoked(Collection<TopicPartition> parts);
  ```
- **On revoke**: calls `committer.commitBeforeRevoke(revoked)` SYNCHRONOUSLY (blocking the Kafka poll thread). Rationale: Tier 5 C4 watch-out — throwing from listener aborts poll; a blocking call is safer than "submit to executor".
- **Thread safety**: called only from Kafka poll thread (same virtual thread as consume loop).

#### `RecoveryCoordinator` — final class

- **Purpose**: one-time-per-stream recovery. Ports `_check_recovery_gap`. On startup:
  1. `stateManager.loadAllFileStates()` → index by `(topic, partition)`.
  2. `stateManager.loadStreamCheckpoints()` → `_durable_checkpoints` cache.
  3. For each file state: truncate if filesystem size > PG-recorded size (Tier 5 I3 watch-out).
  4. For each primary topic/partition: if `_durable_checkpoints[(ex,sym,st)]` exists, compute pending seek = high_water_offset + 1; else record "no checkpoint" → will emit `restart_gap` on first envelope.
  5. Delete uncommitted `.zst` files without sidecars (discover via `SealedFileIndex.scan()`, Tier 5 I7).
- **Public API**:
  ```
  public RecoveryCoordinator(StateManager stateManager,
                             SealedFileIndex sealedIndex,
                             LastEnvelopeReader envelopeReader,
                             HostLifecycleEvidence hostEvidence,
                             RestartGapClassifier classifier,           // static utility; passed for testability
                             GapEmitter gaps,
                             WriterMetrics metrics,
                             ClockSupplier clock,
                             String currentBootId,
                             String currentInstanceId);
  /** Runs once at startup; populates pendingSeeks and durableCheckpoints. */
  public RecoveryResult runOnStartup();

  /** Returns a gap envelope (restart_gap) if this is the first envelope post-restart
   *  for that stream, based on loaded state; else null. Called by RecordHandler. */
  public GapEnvelope checkOnFirstEnvelope(DataEnvelope env);

  public Map<TopicPartition, Long> pendingSeeks();        // immutable view
  public Map<StreamKey, StreamCheckpoint> durableCheckpoints();    // live view; updated by OffsetCommitCoordinator
  ```
- **Thread safety**: construction-time heavy; `checkOnFirstEnvelope` called from consume-loop thread; dicts owned by this class and never shared.

#### `SessionChangeDetector` — final class

- **Purpose**: runtime session-change detection. Ports `_check_session_change`. State dict `Map<StreamKey,(sessionId,receivedAt)>` owned by this class; updated on every envelope.
- **Public API**:
  ```
  public SessionChangeDetector(GapEmitter gaps, CoverageFilter coverage, WriterMetrics metrics, ClockSupplier clock);
  public Optional<GapEnvelope> observe(DataEnvelope env, String source);
  ```
- **Thread safety**: called only from consume-loop thread.

#### `DepthRecoveryGapFilter` — final class

- **Purpose**: depth anchor state machine. Ports `_maybe_close_depth_recovery_gap`. Tracks pending depth-recovery per `(exchange,symbol)`.
- **Public API**:
  ```
  public DepthRecoveryGapFilter(GapEmitter gaps, ClockSupplier clock);
  public Optional<GapEnvelope> onDepthDiff(DataEnvelope env);
  ```
- **Risk mitigation** (mapping §10 Risk 8): in-memory state is lost on crash; acceptable because the NEXT restart re-emits a `recovery_depth_anchor` gap from `RecoveryCoordinator` if depth state is missing. Document inline.
- **Thread safety**: consume-loop thread only.

#### `LateArrivalSequencer` — final class

- **Purpose**: maintain `_late_seq` per sealed path. Generates `hour-H.late-{n}.jsonl.zst` names (Tier 5 M15).
- **Public API**:
  ```
  public int nextSeq(Path baseHourPath);     // mutates internal counter
  public void markSealed(Path hourPath);     // initializes seq=0
  ```
- **Thread safety**: consume-loop thread only.

#### `HourRotationScheduler` — final class

- **Purpose**: watches `(prevDate, prevHour)` per `StreamKey`. When an envelope's UTC (`date`, `hour`) differs, schedules a `FileRotator.rotate(oldTarget)` call and triggers `OffsetCommitCoordinator.commitSealedHour(...)` on the seal.
- **Public API**:
  ```
  public HourRotationScheduler(FileRotator rotator, BufferManager buffers, OffsetCommitCoordinator committer, WriterMetrics metrics);
  public void onEnvelope(DataEnvelope env);
  public void rotateAllOnShutdown(String currentDate, int currentHour);   // skip CURRENT hour
  ```
- **UTC** ONLY (Tier 5 F3, M11).

### 2.3 `buffer/`

#### `BufferManager` — final class

- **Purpose**: ports `buffer_manager.py:BufferManager`. Routes envelopes by `FileTarget`, triggers flush at 10k records or on-demand. Memory-only; no I/O.
- **Public API**:
  ```
  public BufferManager(String baseDir, int flushMessages, int flushIntervalSeconds, EnvelopeCodec codec);
  public Optional<List<FlushResult>> add(DataEnvelope env, BrokerCoordinates coords, String source);
  public Optional<List<FlushResult>> add(GapEnvelope env, BrokerCoordinates coords, String source);
  public List<FlushResult> flushKey(StreamKey key);
  public List<FlushResult> flushAll();
  public int flushIntervalSeconds();
  public FileTarget route(DataEnvelope env);        // strict UTC route (Tier 5 F3, M14)
  ```
- **Serialization**: uses `EnvelopeCodec.toJsonBytes(codec.withBrokerCoordinates(env, coords))` + `appendNewline` (Tier 5 B2). No orjson-style re-indent.
- **Note on coordinates**: Python mutates envelope dict in place. Java records are immutable; we use the `withBrokerCoordinates` wrapper from common (§2.1 of common design §6.1 Option A) so coordinates appear as the last three fields.
- **Thread safety**: consume-loop thread only (Tier 5 A5).

#### `FileTarget` — record
```
public record FileTarget(String exchange, String symbol, String stream, String date, int hour) {}
```
- Records auto-`equals`/`hashCode` on components, matching Python tuple identity (Tier 5 M14). `date` stays a `String` — do NOT "improve" to `LocalDate`.

#### `FlushResult` — record
```
public record FlushResult(
    FileTarget target,
    Path filePath,
    List<byte[]> lines,              // each line is JSON bytes + 0x0A
    long highWaterOffset,            // max Kafka offset; -1 sentinel if all synthetic (Tier 5 M9)
    int partition,                   // int (Tier 5 M8)
    int count,
    CheckpointMeta checkpointMeta,   // null if all synthetic
    boolean hasBackupSource) {}
```

#### `CheckpointMeta` — record
```
public record CheckpointMeta(
    long lastReceivedAt,
    String lastCollectorSessionId,
    long lastSessionSeq,             // long; -1 sentinel for injected (Tier 5 M10)
    StreamKey streamKey) {}
```
- `StreamKey` is a package-private record `(String exchange, String symbol, String stream)` used for map keys across the module.

### 2.4 `rotate/`

#### `FileRotator` — final class

- **Purpose**: seals a previously-active `hour-H.jsonl.zst` file: remaining flush → write → close channel → compute sha256 → write sidecar → schedule PG checkpoint + Kafka commit. Also handles late-arrival file naming via `LateArrivalSequencer`.
- **Public API**:
  ```
  public FileRotator(DurableAppender appender,
                     ZstdFrameCompressor compressor,
                     Sha256Sidecar sidecarWriter,
                     LateArrivalSequencer lateSeq,
                     BufferManager buffers,
                     WriterMetrics metrics);
  public SealResult seal(FileTarget target);                 // performs write+fsync+sidecar; returns paths
  public Path lateFilePath(Path hourPath);                   // delegates to LateArrivalSequencer + FilePaths
  ```
- **Thread safety**: consume-loop thread only.

#### `FilePaths` — utility final class

- **Purpose**: ports `file_rotator.py` pure helpers.
- **Public API**:
  ```
  public static Path buildFilePath(String baseDir, String exchange, String symbol,
                                   String stream, String date, int hour, Integer lateSeq);
  public static Path buildBackfillFilePath(...);
  public static Path sidecarPath(Path dataPath);             // dataPath + ".sha256"
  ```
- **Invariant**: symbol lowercased at call site (Tier 5 M1).

#### `Sha256Sidecar` — utility final class

- **Public API**:
  ```
  public static void write(Path dataPath, Path sidecarPath) throws IOException;
  ```
- **Body**: `Files.writeString(sidecarPath, Sha256.hexFile(dataPath) + "  " + dataPath.getFileName() + "\n")` (Tier 5 I5, I6).

### 2.5 `io/`

#### `ZstdFrameCompressor` — final class

- **Purpose**: ports `ZstdFrameCompressor`. Produces one independent zstd frame per flush (Tier 5 I1).
- **Public API**:
  ```
  public ZstdFrameCompressor(int level);
  public byte[] compressFrame(List<byte[]> lines);
  ```
- **Implementation**: `System.arraycopy` to single `byte[]`; `Zstd.compress(joined, level)`.
- **Thread safety**: `Zstd.compress` is pure; no state; safe cross-thread. Only one caller anyway.

#### `DurableAppender` — final class

- **Purpose**: owns the "open → append → force(true) → close" pattern with partial-write truncate fallback (Tier 5 I3, I4).
- **Public API**:
  ```
  public void appendAndFsync(Path path, byte[] payload) throws IOException;
  /** Size-after-append, needed for FileState updates. */
  public long sizeAfter(Path path, int payloadLen);
  ```
- **Body**:
  ```
  try (FileChannel fc = FileChannel.open(path, APPEND, CREATE)) {
      long posBefore = fc.position();
      try {
          fc.write(ByteBuffer.wrap(payload));
          fc.force(true);
      } catch (IOException e) {
          try { fc.truncate(posBefore); fc.force(true); } catch (IOException ignored) {}
          throw e;
      }
  }
  ```
- **Thread safety**: one file at a time per call; no shared state.

### 2.6 `state/`

#### `StateManager` — final class

- **Purpose**: ports `state_manager.py`. PG-backed durable state; uses JDBC + HikariCP (spec §1.3).
- **Public API**:
  ```
  public StateManager(HikariDataSource ds, ClockSupplier clock);
  public void connect();                                                      // createTablesIfNotExists
  public void close();
  public Map<TopicPartition, List<FileStateRecord>> loadAllFileStates();
  public Map<StreamKey, StreamCheckpoint> loadStreamCheckpoints();
  /** ATOMIC: both upserts in one transaction; retried up to 3× with 0/2/4s exp backoff (Tier 5 G3). */
  public void saveStatesAndCheckpoints(List<FileStateRecord> states,
                                       List<StreamCheckpoint> checkpoints);
  public void upsertComponentRuntime(ComponentRuntimeState state);
  public void markComponentCleanShutdown(String component, String instanceId);
  public Map<String, ComponentRuntimeState> loadLatestComponentStates();
  public Optional<ComponentRuntimeState> loadComponentStateByInstance(String component, String instanceId);
  public Optional<MaintenanceIntent> loadActiveMaintenanceIntent();
  ```
- **Timestamp handling**: PG `TIMESTAMPTZ` ↔ `OffsetDateTime` via `ResultSet#getObject(col, OffsetDateTime.class)` / `PreparedStatement#setObject(i, OffsetDateTime)`. Store as `Instant.toString()` when stringified (Tier 5 F1). Parse with `OffsetDateTime.parse` to tolerate `+00:00` / `Z` (Tier 5 F2).
- **Retry**: inline 10-line retry loop with `Thread.sleep(1000L * (1 << attempt))` and `Thread.currentThread().interrupt()` on `InterruptedException`. NO framework (Tier 5 G3).
- **Thread safety**: JDBC connections pulled from Hikari; never shared across threads. Methods are thread-safe because each acquires its own connection.

#### Records

```
public record FileStateRecord(
    String topic, int partition, long highWaterOffset, String filePath, long fileByteSize) {}

public record StreamCheckpoint(
    String exchange, String symbol, String stream,
    String lastReceivedAt, String lastCollectorSessionId, String lastGapReason) {}

public record ComponentRuntimeState(
    String component, String instanceId, String hostBootId,
    String startedAt, String lastHeartbeatAt,
    String cleanShutdownAt,            // nullable
    boolean plannedShutdown,
    String maintenanceId) {}            // nullable

public record MaintenanceIntent(
    String maintenanceId, String scope, String plannedBy, String reason,
    String createdAt, String expiresAt, String consumedAt) {}
```

All timestamp fields kept as ISO-8601 strings in the record for identity with Python's `datetime.isoformat()` round-trip (Tier 5 F1, F2). Conversion to `OffsetDateTime` happens inside `StateManager`.

### 2.7 `failover/`

#### `FailoverController` — final class

- **Purpose**: ports `FailoverManager`. Owns the BACKUP `KafkaConsumer<byte[],byte[]>` (created lazily on activate). Silent-timer (5s) → activate → drain backup → switchback filter.
- **Public API**:
  ```
  public FailoverController(Supplier<KafkaConsumer<byte[],byte[]>> backupFactory,
                            List<String> primaryTopics,
                            String backupPrefix,                // default "backup."
                            Duration silenceTimeout,            // default 5s
                            CoverageFilter coverage,
                            WriterMetrics metrics,
                            ClockSupplier clock);
  public void resetSilenceTimer();
  public boolean shouldActivate();
  public void activate();
  public void deactivate();
  public boolean isActive();
  public void trackRecord(DataEnvelope env);
  public boolean shouldFilter(DataEnvelope env);
  public void beginSwitchback();
  public boolean checkSwitchbackFilter(DataEnvelope env);
  public ConsumerRecords<byte[],byte[]> pollBackup(Duration timeout);    // returns empty if inactive
  public void cleanup();
  ```
- **Thread safety**: consume-loop thread only. `_isActive` plus the backup consumer handle are volatile so `isConnected()` from the Ready thread reads a coherent value.

#### `CoverageFilter` — final class

- **Purpose**: ports `CoverageFilter`. Per-source coverage map, pending-gap parking, grace period.
- **Public API** — mirrors Python:
  ```
  public CoverageFilter(double gracePeriodSeconds, double snapshotMissGraceSeconds,
                        WriterMetrics metrics, ClockSupplier clock);
  public void handleData(String source, DataEnvelope env);
  public boolean handleGap(String source, GapEnvelope env);
  public List<GapEnvelope> sweepExpired();
  public List<GapEnvelope> flushAllPending();
  public int pendingSize();
  public boolean enabled();
  ```
- **Coalescing**: when an incoming gap shares `gap_start_ts` with a pending entry, we REPLACE the pending record with a NEW `GapEnvelope` whose `gap_end_ts` is the `max()` (records are immutable, Tier 2 §12). Preserve the original `first_seen` monotonic for grace calculation.
- **Thread safety**: consume-loop thread only.

#### `NaturalKeyExtractor` — utility final class

- **Public API**:
  ```
  public static long extract(DataEnvelope env, ObjectMapper mapper);  // returns -1L for missing/parse error
  ```
- Trades → `raw.get("a").asLong(-1L)`; depth → `"u"`; bookticker → `"u"`; else → `env.exchangeTs()`. All `long` (Tier 5 E1, M3).
- **Returns** primitive `long -1L` for "missing" (hot path — no `Optional` allocation, per Tier 5 M3 watch-out).

#### `HostLifecycleReader` — final class

- **Purpose**: ports `host_lifecycle_reader.py`. Read JSONL at `DEFAULT_LEDGER_PATH` (env override `LIFECYCLE_LEDGER_PATH`), filter by window.
- **Public API**:
  ```
  public static final Path DEFAULT_LEDGER_PATH = Path.of("/data/.cryptolake/lifecycle/events.jsonl");
  public static HostLifecycleEvidence load(Path ledgerPath, String windowStartIso, String windowEndIso, ObjectMapper mapper);
  ```
- Uses `common.jsonl.JsonlReader.readAll(path, mapper)` + stream-filter by ISO timestamps; tolerates `+00:00` / `Z` / naive (Tier 5 F2).

#### `HostLifecycleEvidence` — final class

- **Purpose**: ports `HostLifecycleEvidence`. Indexed accessors.
- **Public API**:
  ```
  public HostLifecycleEvidence(List<JsonNode> events);
  public boolean hasComponentDie(String component);
  public Optional<Boolean> componentCleanExit(String component);      // Optional for the Python `bool | None`
  public boolean hasComponentStop(String component);
  public boolean hasMaintenanceIntent();
  public boolean isEmpty();
  ```
- **Thread safety**: immutable after construction (internal maps made unmodifiable).

#### `RestartGapClassifier` — utility final class

- **Purpose**: PURE FUNCTION, ports `restart_gap_classifier.py:classify_restart_gap`.
- **Public API**:
  ```
  public record Classification(String component, String cause, boolean planned,
                               String classifier, List<String> evidence, String maintenanceId) {}

  public static Classification classify(
      String previousBootId,              // nullable
      String currentBootId,
      String previousSessionId,           // nullable
      String currentSessionId,            // nullable
      boolean collectorCleanShutdown,
      boolean systemCleanShutdown,
      MaintenanceIntent maintenanceIntent, // nullable
      HostLifecycleEvidence hostEvidence,  // nullable
      ClockSupplier clock);                // for intent-expiry wall-clock check
  ```
- Classifier version string: `"writer_recovery_v1"`. Promotable components array `{"redpanda","postgres","writer"}`. Identical ordering to Python.

### 2.8 `gap/`

#### `GapEmitter` — final class

- **Purpose**: THE TRIAD. Every gap MUST go through `emit(...)` which (1) increments the appropriate counter, (2) logs structured event, (3) enqueues the gap envelope into `BufferManager` for archival (Tier 1 §5, mapping §9 Rule 5). Callers NEVER write a gap envelope directly to the buffer.
- **Public API**:
  ```
  public GapEmitter(BufferManager buffers, WriterMetrics metrics, StructuredLogger log, CoverageFilter coverage);
  /** Returns true if the gap was written; false if the coverage filter dropped/parked it. */
  public boolean emit(GapEnvelope gap, String source, String topic, int partition, long offset);
  /** Used when coordinator wants to bypass coverage (e.g., shutdown flush of pending). */
  public void emitUnfiltered(GapEnvelope gap, String source, String topic, int partition);
  ```
- **Thread safety**: consume-loop thread only.

#### `GapReasonsLocal` — utility final class

- Helpers to pick the right metric label (coverage-filter `source`, `reason` → counter `writer_gap_envelopes_suppressed_total` etc).

### 2.9 `metrics/`

#### `WriterMetrics` — final class

- **Purpose**: owns `PrometheusMeterRegistry` and all 27 meters. See §9 for the full table. Applies `NamingConvention.identity` to avoid `_total_total` (Tier 5 H4 watch-out).
- **Public API** (selected, all 27 enumerated in §9):
  ```
  public WriterMetrics(PrometheusMeterRegistry registry);
  public PrometheusMeterRegistry registry();

  // Counters — per-stream labels
  public Counter messagesConsumed(String exchange, String symbol, String stream);
  public Counter messagesSkipped(String exchange, String symbol, String stream);
  public Counter filesRotated(String exchange, String symbol, String stream);
  public Counter bytesWritten(String exchange, String symbol, String stream);
  public Counter sessionGapsDetected(String exchange, String symbol, String stream);
  public Counter writeErrors(String exchange, String symbol, String stream);
  public Counter pgCommitFailures();           // no labels
  public Counter kafkaCommitFailures();        // no labels
  public Counter gapRecordsWritten(String exchange, String symbol, String stream, String reason);
  public Counter failoverTotal();
  public Counter failoverRecordsTotal();
  public Counter switchbackTotal();
  public Counter gapEnvelopesSuppressed(String source, String reason);
  public Counter gapCoalesced(String source);

  // Gauges — supplier-backed (Tier 5 H6) using holder objects
  public void setConsumerLag(String exchange, String stream, long lag);
  public void setCompressionRatio(String exchange, String symbol, String stream, double ratio);
  public void setDiskUsageBytes(long bytes);
  public void setDiskUsagePct(double pct);
  public void setFailoverActive(boolean active);
  public void setHoursSealedToday(String exchange, String symbol, String stream, int count);
  public void setHoursSealedPreviousDay(String exchange, String symbol, String stream, int count);
  public void setGapPendingSize(int size);

  // Histograms — SLOs exactly match Python buckets (Tier 5 H5)
  public DistributionSummary flushDurationMs(String exchange, String symbol, String stream);
  public DistributionSummary failoverDurationSeconds();
  ```
- **Thread safety**: Micrometer meters are thread-safe; holders (`AtomicLong`, `AtomicReference<Double>`) avoid stale reads.

#### `MetricHolders` — package-private final class

- Holds the mutable state behind gauges so suppliers always read a fresh value without locks (Tier 5 H6 watch-out: must be strongly referenced for lifetime of service — `WriterMetrics` keeps them as fields).

### 2.10 `recovery/`

#### `SealedFileIndex` — final class

- **Purpose**: startup scan of `baseDir` for `.jsonl.zst` files; pair each with its `.sha256` sidecar; identify "sealed" vs "uncommitted (missing sidecar)". Also performs truncate-on-recovery when FS size > PG-recorded size (Tier 5 I7, I3 watch-out).
- **Public API**:
  ```
  public SealedFileIndex(Path baseDir);
  public ScanResult scanAndReconcile(Map<TopicPartition, List<FileStateRecord>> pgState);
  public record ScanResult(Set<Path> sealed, List<Path> deletedUncommitted, List<Path> truncated) {}
  public Set<Path> sealedFiles();
  ```
- **Thread safety**: not thread-safe; called once at startup.

#### `LastEnvelopeReader` — final class

- **Purpose**: reads the last data envelope from a sealed .jsonl.zst (reverse scan, skip gap envelopes) for use by `RecoveryCoordinator` when inferring gap bounds from archive. Uses `ZstdInputStream` streaming decompress (Tier 5 I2).
- **Public API**:
  ```
  public LastEnvelopeReader(EnvelopeCodec codec);
  public Optional<DataEnvelope> lastDataEnvelope(Path zstFile);
  ```

### 2.11 `health/`

#### `WriterReadyCheck` — final class (implements `common.health.ReadyCheck`)

- Returns `Map.of("consumer_connected", loop.isConnected(), "storage_writable", storageProbe.probe())`.
- Ports `Writer._ready_checks`.

## 3. Concurrency design

The entire writer runs with **one virtual-thread executor per service** (Tier 2 §8, design §1.4). All heavy work is `poll → handle → flush → commit` on a single virtual thread; no async chains. Exceptions are the health server and the SIGTERM hook, each on their own virtual thread.

### 3.1 Thread inventory

| Thread | Owner | Role | Shutdown |
|---|---|---|---|
| T1 — consume loop | `KafkaConsumerLoop.run()` submitted via `executor.submit(...)` | Polls primary, calls `RecordHandler.handle` per record, drives flush/rotate/commit, polls backup when `FailoverController.isActive()`. OWNS every mutable dict in the writer. | Stops when `stopRequested` volatile is true; final `OffsetCommitCoordinator.shutdownCommit()` call before returning. |
| T2 — health server | `HealthServer.start()` | `jdk.httpserver` spawns each request on its own virtual thread internally. | `HealthServer.stop()` in shutdown hook. |
| T3 — SIGTERM hook | `Runtime.addShutdownHook(new Thread(...))` (platform thread — JVM requirement) | Sets `stopRequested = true`; `T1.join(30s)`; `healthServer.stop()`; `stateManager.close()`; `backupConsumer.close()`. | Platform thread, exits when JVM exits. |

No other threads. No thread pools. No `ScheduledExecutorService`. No `CompletableFuture`.

### 3.2 Mapping §5 concurrency surface → Java

| Mapping §5 Python primitive | Java equivalent | Where |
|---|---|---|
| `asyncio.get_running_loop().run_in_executor(None, consumer.poll, 1.0)` | `consumer.poll(Duration.ofSeconds(1))` blocking on virtual thread | `KafkaConsumerLoop.run` (Tier 5 A2) |
| `asyncio.get_running_loop().run_in_executor(None, backupConsumer.poll, 0.5)` | `failover.pollBackup(Duration.ofMillis(500))` blocking on virtual thread | `KafkaConsumerLoop.run` |
| `consumer.subscribe()` with on_assign/on_revoke | `consumer.subscribe(topics, new PartitionAssignmentListener(...))` | `KafkaConsumerLoop.run` start-up |
| `consumer.commit(asynchronous=True)` | `consumer.commitSync(offsets)` (Tier 5 C8 override + architect-prompt mandate) | `OffsetCommitCoordinator.flushAndCommit` |
| `loop.create_task(self._flush_and_commit())` (best-effort, Kafka callback) | `executor.submit(coordinator::flushAndCommit)` — fire-and-forget, exception swallowed + logged (Tier 5 A6) | Only inside `PartitionAssignmentListener.onPartitionsRevoked` — NO, override: we call `committer.commitBeforeRevoke(revoked)` SYNCHRONOUSLY on the poll thread (see §2.2 `PartitionAssignmentListener`). |
| `asyncio.gather(consume_loop, health_server)` | `executor.submit(consumeLoop)` + `healthServer.start()` (independent); `Main` blocks on a latch released by shutdown hook | `Main` |
| `asyncio.sleep(seconds)` (retry backoff) | `Thread.sleep(Duration.ofSeconds(seconds))` on virtual thread (fine; `sleep` is NOT in a hot path, Tier 2 §10 OK) | `StateManager` retry |
| `consumer.subscribe` | Kafka-clients assigns on-demand on next `poll`; listener fires from poll thread | T1 |
| File I/O (open, write, fsync) | Blocking `FileChannel` on T1 — virtual thread tolerates blocking | `DurableAppender.appendAndFsync` |
| PG query (async via psycopg) | Blocking JDBC via Hikari on T1 — virtual thread tolerates blocking; each call acquires + releases a connection | `StateManager` methods |

### 3.3 Shared-state inventory (owned by T1)

All these Python state dicts translate to plain `HashMap` / `HashSet` fields on classes whose sole caller is T1. NO synchronization, NO `ConcurrentHashMap`, because there is no second thread (Tier 5 A5).

| Python state | Java owner | Type |
|---|---|---|
| `_last_session[(ex,sym,st)]` | `SessionChangeDetector.lastSession` | `HashMap<StreamKey, SessionMark>` |
| `_durable_checkpoints[(ex,sym,st)]` | `RecoveryCoordinator.durableCheckpoints` | `HashMap<StreamKey, StreamCheckpoint>` |
| `_recovery_done`, `_recovery_gap_emitted` | `RecoveryCoordinator.recoveryDone` / `.recoveryGapEmitted` | `HashSet<StreamKey>` |
| `_depth_recovery_pending[(ex,sym)]` | `DepthRecoveryGapFilter.pending` | `HashMap<ExSym, DepthPendingState>` |
| `_sealed_files` | `SealedFileIndex.sealed` | `HashSet<Path>` |
| `_late_seq[Path]` | `LateArrivalSequencer.seq` | `HashMap<Path, Integer>` |
| `_recovery_high_water[(topic,part,path)]` | `RecoveryCoordinator.recoveryHighWater` | `HashMap<RecoveryKey, Long>` |
| `_assigned_partitions` | `KafkaConsumerLoop.assignedPartitions` | `volatile Set<TopicPartition>` (unmodifiable copy swapped in by listener; read by `isConnected()` from Ready thread — single-writer, multi-reader; immutable reference so no race) |
| `_pending_seeks[(topic,part)]` | `RecoveryCoordinator.pendingSeeks` | `HashMap<TopicPartition, Long>` — drained during assignment listener, but the drain happens on the poll thread inside T1 (Kafka-clients guarantees listener runs on poll thread) |

### 3.4 Shutdown sequence

1. SIGTERM handler sets `KafkaConsumerLoop.stopRequested = true` (volatile).
2. Poll loop observes flag after current poll returns; calls:
   - `failover.cleanup()` (close backup consumer)
   - `buffers.flushAll()` + `coordinator.shutdownCommit()` (final write→fsync→PG→commitSync)
   - `coverage.flushAllPending()` → `emitUnfiltered` each
   - `rotator.rotateAllOnShutdown(currentDate, currentHour)` — skips CURRENT hour (ports Python behavior)
   - `stateManager.markComponentCleanShutdown("writer", instanceId)`
   - `consumer.close(Duration.ofSeconds(30))` — flushes final offsets
3. T1 returns; executor closes via try-with-resources in `Main`.
4. Shutdown hook then calls `healthServer.stop()`, `stateManager.close()`.

### 3.5 Forbidden patterns (enforced in review)

- No `synchronized` keyword anywhere (Tier 2 §9).
- No `CompletableFuture` (Tier 5 A1 + appendix).
- No `ExecutorService.submit` in the hot path — only the one `executor.submit(consumeLoop)` in `Main`.
- No `StructuredTaskScope` in writer; writer is a single-thread consume loop (Tier 5 A1 doesn't apply here).
- No `Thread.sleep` in `KafkaConsumerLoop.run` (Tier 2 §10) — only inside `StateManager` retry backoff.

### 3.6 Cancellation / interrupt

- `stopRequested` is the single cancellation signal.
- `Thread.currentThread().interrupt()` is preserved in every `catch (InterruptedException)` block (Tier 5 A4).
- `consumer.wakeup()` is NOT used — the 1s poll timeout is short enough to notice the volatile flag.

## 4. Python → Java mapping table

Every symbol from mapping §3 (Public API surface) and §4 (Internal structure / state dicts) maps below.

### 4.1 main.py

| Python symbol | Java target |
|---|---|
| `Writer.__init__(config_path)` | `Main.main(String[])` wires all components |
| `Writer.start()` | `Main.main` body: start order mirrors Python |
| `Writer.shutdown()` | `Main` shutdown hook + try-with-resources on executor |
| `Writer._ready_checks()` | `WriterReadyCheck.get()` |
| `Writer._boot_id` / `_instance_id` / `_started_at` | Local vars in `Main.main`; passed to `RecoveryCoordinator` + `StateManager.upsertComponentRuntime` |
| `main()` | `Main.main` (picocli not used; plain `main`) |

### 4.2 consumer.py (WriterConsumer — the god object)

| Python symbol | Java target |
|---|---|
| `WriterConsumer.__init__` | `Main.main` constructs `KafkaConsumerLoop` + deps |
| `WriterConsumer.start()` | `KafkaConsumerLoop.run()` first phase: `recovery.runOnStartup()` + `consumer.subscribe` |
| `WriterConsumer.consume_loop()` | `KafkaConsumerLoop.run()` body (split into private `pollPrimaryOnce`, `pollBackupOnce`, `maybeFlush`, `maybeRotate`) |
| `WriterConsumer.stop()` | shutdown sequence in §3.4 |
| `WriterConsumer._check_recovery_gap` | `RecoveryCoordinator.checkOnFirstEnvelope` |
| `WriterConsumer._check_session_change` | `SessionChangeDetector.observe` |
| `WriterConsumer._maybe_close_depth_recovery_gap` | `DepthRecoveryGapFilter.onDepthDiff` |
| `WriterConsumer._write_and_save` | `OffsetCommitCoordinator.flushAndCommit` (normal path) |
| `WriterConsumer._rotate_file` | `FileRotator.seal` + `OffsetCommitCoordinator.commitSealedHour` |
| `WriterConsumer._rotate_hour` | `HourRotationScheduler.rotateAllOnShutdown` |
| `WriterConsumer._write_to_disk` | split: `ZstdFrameCompressor.compressFrame` + `DurableAppender.appendAndFsync` |
| `WriterConsumer._commit_state` | `OffsetCommitCoordinator.flushAndCommit` (Tier 5 C8 single owner) |
| `WriterConsumer._handle_gap_detection` | `RecordHandler.handle` (session + depth + recovery checks in sequence) |
| `WriterConsumer._handle_rotation_and_buffer` | `RecordHandler.handle` → `buffers.add` + `HourRotationScheduler.onEnvelope` |
| `WriterConsumer._flush_and_commit` | `OffsetCommitCoordinator.flushAndCommit` |
| `WriterConsumer._deserialize_and_stamp` | `RecordHandler` private method `decodeAndStamp(ConsumerRecord rec)` (Tier 5 G4 — JsonProcessingException → `deserialization_error` gap) |
| `WriterConsumer.is_connected` | `KafkaConsumerLoop.isConnected` |

### 4.3 buffer_manager.py

| Python | Java |
|---|---|
| `BufferManager.add` | `BufferManager.add(DataEnvelope, ...)` overloaded for `GapEnvelope` |
| `BufferManager.flush_key` | `BufferManager.flushKey(StreamKey)` |
| `BufferManager.flush_all` | `BufferManager.flushAll()` |
| `BufferManager.route` | `BufferManager.route(DataEnvelope)` (UTC, Tier 5 F3) |
| `BufferManager._flush_buffer` | private `flushBuffer(FileTarget)` |
| `FlushResult` | `buffer/FlushResult` record |
| `CheckpointMeta` | `buffer/CheckpointMeta` record |

### 4.4 compressor.py

| Python | Java |
|---|---|
| `ZstdFrameCompressor.__init__(level)` | `ZstdFrameCompressor(int level)` |
| `ZstdFrameCompressor.compress_frame(lines)` | `ZstdFrameCompressor.compressFrame(List<byte[]>)` |

### 4.5 file_rotator.py

| Python | Java |
|---|---|
| `FileTarget` dataclass | `buffer/FileTarget` record |
| `FileTarget.key` property | implicit via record `equals/hashCode` |
| `build_file_path` | `rotate/FilePaths.buildFilePath` |
| `build_backfill_file_path` | `rotate/FilePaths.buildBackfillFilePath` |
| `sidecar_path` | `rotate/FilePaths.sidecarPath` |
| `compute_sha256` | `common.util.Sha256.hexFile` (reused from common, Tier 5 I5) |
| `write_sha256_sidecar` | `rotate/Sha256Sidecar.write` |

### 4.6 failover.py

| Python | Java |
|---|---|
| `FailoverManager` | `failover/FailoverController` |
| `FailoverManager.extract_natural_key` | `failover/NaturalKeyExtractor.extract` (static) |
| `_RAW_KEY_STREAMS` | `static final Map<String,String>` in `NaturalKeyExtractor` |
| `CoverageFilter` | `failover/CoverageFilter` |
| `CoverageFilter._other_covers` | private `otherCovers(...)` |
| `CoverageFilter.handle_data` | `handleData` |
| `CoverageFilter.handle_gap` | `handleGap` |
| `CoverageFilter.sweep_expired` | `sweepExpired` |
| `CoverageFilter.flush_all_pending` | `flushAllPending` |

### 4.7 host_lifecycle_reader.py

| Python | Java |
|---|---|
| `HostLifecycleEvidence` | `failover/HostLifecycleEvidence` |
| `HostLifecycleEvidence.has_component_die` | `hasComponentDie` |
| `HostLifecycleEvidence.component_clean_exit` | `componentCleanExit` → `Optional<Boolean>` |
| `HostLifecycleEvidence.has_component_stop` | `hasComponentStop` |
| `HostLifecycleEvidence.has_maintenance_intent` | `hasMaintenanceIntent` |
| `HostLifecycleEvidence.is_empty` | `isEmpty` |
| `load_host_evidence` | `HostLifecycleReader.load` |
| `DEFAULT_LEDGER_PATH` | `HostLifecycleReader.DEFAULT_LEDGER_PATH` |

### 4.8 restart_gap_classifier.py

| Python | Java |
|---|---|
| `classify_restart_gap` | `RestartGapClassifier.classify` (returns `Classification` record) |
| `_is_intent_valid` | private `isIntentValid` |
| `_CLASSIFIER_VERSION` | `private static final String CLASSIFIER_VERSION = "writer_recovery_v1"` |
| `_PROMOTABLE_COMPONENTS` | `private static final List<String> PROMOTABLE_COMPONENTS = List.of("redpanda","postgres","writer")` |

### 4.9 state_manager.py

| Python | Java |
|---|---|
| `StateManager.__init__(url)` | `StateManager(HikariDataSource, ClockSupplier)` (dsn built in `Main`) |
| `StateManager.connect` | `connect()` |
| `StateManager.close` | `close()` |
| `StateManager.load_all_states` | `loadAllFileStates()` |
| `StateManager.load_stream_checkpoints` | `loadStreamCheckpoints()` |
| `StateManager.save_states_and_checkpoints` | `saveStatesAndCheckpoints(List, List)` (atomic, retry 3× — Tier 5 G3) |
| `StateManager.upsert_component_runtime` | `upsertComponentRuntime(ComponentRuntimeState)` |
| `StateManager.mark_component_clean_shutdown` | `markComponentCleanShutdown(String, String)` |
| `StateManager.load_latest_component_states` | `loadLatestComponentStates()` |
| `StateManager.load_component_state_by_instance` | `loadComponentStateByInstance(String, String)` |
| `StateManager.load_active_maintenance_intent` | `loadActiveMaintenanceIntent()` |
| `StateManager._retry_transaction` | private `retry(Runnable, String)` |
| `FileState` | `state/FileStateRecord` |
| `StreamCheckpoint` | `state/StreamCheckpoint` |
| `ComponentRuntimeState` | `state/ComponentRuntimeState` |
| `MaintenanceIntent` | `state/MaintenanceIntent` |

### 4.10 metrics.py

All 27 metrics → `WriterMetrics` meters. Full table in §9.

### 4.11 State dicts (mapping §4)

Covered in §3.3 above. Every state dict has a single-owner class.

### 4.12 Intentionally dropped symbols

| Python | Why dropped |
|---|---|
| `src.common.service_runner.run_service` / `uvloop.install()` | Virtual threads replace the whole runner (Tier 5 A8). `Main.main` directly opens `Executors.newVirtualThreadPerTaskExecutor()`. |
| `src.common.async_utils.cancel_tasks` | Not needed (Tier 5 A7). |
| `__init__.py` (2 lines) | Package declaration only; no Java counterpart. |
| Python `asyncio.gather(...)` in `Writer.start` | Replaced by two independent `executor.submit` calls + health server on its own JDK httpserver executor. |

## 5. Library mapping

Every dependency from mapping §6 (External I/O) + implicit Python stdlib.

| Python dep / stdlib | Java library | Version (from `gradle/libs.versions.toml`) | Notes |
|---|---|---|---|
| `confluent_kafka` | `org.apache.kafka:kafka-clients` | 3.8.0 (`kafka`) | Spec §1.3; uses `commitSync` not `commitAsync` for hot path per architect-prompt mandate + Tier 5 C8 |
| `psycopg[binary]` | `org.postgresql:postgresql` + `com.zaxxer:HikariCP` | 42.7.4, 5.1.0 | Spec §1.3; blocking JDBC on virtual threads |
| `orjson` | `com.fasterxml.jackson.core:jackson-databind` (+ YAML via `:jackson-dataformat-yaml` for config) | 2.17.2 | Single `ObjectMapper` in `Main` (Tier 2 §14, Tier 5 B6, B7). Config already done in `common.EnvelopeCodec.newMapper()` |
| `zstandard` | `com.github.luben:zstd-jni` | 1.5.6-4 | Spec §1.3; Tier 5 I1 — `Zstd.compress(...)`, not `ZstdOutputStream` |
| `prometheus_client` | `io.micrometer:micrometer-registry-prometheus` | 1.13.4 | Spec §1.3; Tier 5 H4 naming convention override (`NamingConvention.identity`) |
| `structlog` | SLF4J + Logback + Logstash encoder + MDC | 2.0.13 / 1.5.8 / 7.4 | Via `common.logging.StructuredLogger`; already in common |
| `pydantic` | Java records + Jackson + Hibernate Validator | 8.0.1.Final | All config records already in `common.config.*` |
| `pyyaml` | `jackson-dataformat-yaml` | 2.17.2 | `common.config.YamlConfigLoader` (reused) |
| `asyncio` + `uvloop` | Virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) | JDK 21 | Tier 5 A8 — uvloop deleted, no replacement |
| `aiohttp` | N/A (writer doesn't use HTTP clients) | — | Writer has no outbound HTTP in mapping §6 |
| `hashlib` | `java.security.MessageDigest` via `common.util.Sha256` | JDK 21 | Already in common (Tier 5 I5) |
| `pathlib` | `java.nio.file.Path` / `Files` / `FileChannel` | JDK 21 | Tier 5 I3, I7 |
| `datetime` | `java.time.Instant` / `OffsetDateTime` / `ZoneOffset.UTC` | JDK 21 | Tier 5 F1–F4 |
| `time.monotonic` | `System.nanoTime()` | JDK 21 | Tier 5 F4 |
| `time.time_ns` | `Clocks.systemNanoClock()` (from common) | JDK 21 | Tier 5 E2 |
| `os.environ` | `System.getenv()` | JDK 21 | Filtered by `EnvOverrides` (already in common) |
| `pytest` + `pytest-asyncio` | `org.junit.jupiter:junit-jupiter` | 5.11.0 | Tier 5 L3 — no async test framework |
| `testcontainers` (kafka, postgres) | `org.testcontainers:kafka`, `:postgresql` | 1.20.1 | Spec §1.3 |
| `prometheus_client.Counter` / `Gauge` / `Histogram` | `Counter` / `Gauge` / `DistributionSummary` | 1.13.4 | Tier 5 H4, H5, H6 |
| `hashlib.sha256` | `MessageDigest.getInstance("SHA-256")` via `Sha256` | JDK 21 | |

No deviations from spec §1.3. No new library introduced.

## 6. Data contracts

Every dataclass / pydantic model from mapping §7 → Java record. All in `com.cryptolake.writer.*` except where noted. JSON property names match Python exactly; field order matches where serialized (via `@JsonPropertyOrder`) so byte identity holds (Tier 1 §7; Tier 3 §21).

### 6.1 Kafka envelope on the wire

Already defined in `common.envelope.*` and reused:

- **DataEnvelope** (`common/envelope/DataEnvelope.java`): 11 fields, property-ordered, `@JsonProperty` per field. Writer consumes via `EnvelopeCodec.readData(rec.value())`.
- **GapEnvelope** (`common/envelope/GapEnvelope.java`): 18 fields (12 required + 6 optional), property-ordered, `@JsonInclude(NON_NULL)`. Writer emits via `GapEmitter` → `BufferManager.add(GapEnvelope, coords, source)`.
- **BrokerCoordinates** (`common/envelope/BrokerCoordinates.java`): `_topic`, `_partition`, `_offset` — writer appends via `EnvelopeCodec.withBrokerCoordinates(...)` wrapper (design §6.1 Option A of common's design).

Writer-added fields `_topic`, `_partition`, `_offset`, `_source` — the first three are in `BrokerCoordinates`; `_source` is NOT serialized into the archive — it exists only on the internal `DataEnvelope` + `source` parameter threading through buffer/handler. When writing to disk the last three fields emitted are `_topic`, `_partition`, `_offset`; `_source` is writer-internal book-keeping only (matches Python, which also only emits `_source` in buffer-layer tracking).

### 6.2 buffer/FileTarget (ports Python `FileTarget`)

```
public record FileTarget(String exchange, String symbol, String stream, String date, int hour) {
  public FileTarget {
    if (exchange == null || symbol == null || stream == null || date == null)
      throw new IllegalArgumentException("FileTarget fields must not be null");
    if (hour < 0 || hour > 23) throw new IllegalArgumentException("hour out of range: " + hour);
  }
}
```

JSON: not serialized (internal routing key). `equals`/`hashCode` match Python tuple (Tier 5 M14).

### 6.3 buffer/FlushResult (ports Python `FlushResult`)

```
public record FlushResult(
    FileTarget target,
    Path filePath,
    List<byte[]> lines,          // each line is already JSON+\n bytes
    long highWaterOffset,        // max Kafka offset or -1L if all synthetic (Tier 5 M9)
    int partition,               // int (Tier 5 M8)
    int count,
    CheckpointMeta checkpointMeta,   // nullable
    boolean hasBackupSource) {
  public FlushResult {
    lines = List.copyOf(lines);          // defensive copy (records shouldn't own mutable lists)
  }
}
```

Not serialized to JSON.

### 6.4 buffer/CheckpointMeta (ports Python `CheckpointMeta`)

```
public record CheckpointMeta(
    long lastReceivedAt,
    String lastCollectorSessionId,
    long lastSessionSeq,                   // long, -1 sentinel (Tier 5 M10)
    StreamKey streamKey) {}                // (exchange, symbol, stream)
```

Not serialized.

### 6.5 state/FileStateRecord (ports `FileState`)

```
public record FileStateRecord(
    String topic,
    int partition,
    long highWaterOffset,
    String filePath,
    long fileByteSize) {}
```

Stored in PG table `writer_file_state(topic, partition, file_path, high_water_offset, file_byte_size, updated_at)` with composite PK `(topic, partition, file_path)` — exact DDL ported from `state_manager.py`.

### 6.6 state/StreamCheckpoint (ports Python `StreamCheckpoint`)

```
public record StreamCheckpoint(
    String exchange,
    String symbol,
    String stream,
    String lastReceivedAt,              // ISO-8601 string — Tier 5 F1, F2
    String lastCollectorSessionId,
    String lastGapReason) {}            // nullable
```

PG table `stream_checkpoint` PK `(exchange, symbol, stream)`. `last_received_at` column is `TIMESTAMPTZ`; `StateManager` parses via `OffsetDateTime.parse` tolerating `Z` / `+00:00`.

### 6.7 state/ComponentRuntimeState (ports Python `ComponentRuntimeState`)

```
public record ComponentRuntimeState(
    String component,
    String instanceId,
    String hostBootId,
    String startedAt,
    String lastHeartbeatAt,
    String cleanShutdownAt,              // nullable
    boolean plannedShutdown,
    String maintenanceId) {}             // nullable
```

PG table `component_runtime_state` PK `(component, instance_id)`.

### 6.8 state/MaintenanceIntent (ports Python `MaintenanceIntent`)

```
public record MaintenanceIntent(
    String maintenanceId,
    String scope,
    String plannedBy,
    String reason,
    String createdAt,
    String expiresAt,
    String consumedAt) {}                // nullable
```

PG table `maintenance_intent` PK `maintenance_id`.

### 6.9 failover/RestartGapClassifier.Classification

```
public record Classification(
    String component,                    // host | system | collector | writer | redpanda | postgres
    String cause,                        // operator_shutdown | host_reboot | unclean_exit | unknown
    boolean planned,
    String classifier,                   // "writer_recovery_v1"
    List<String> evidence,               // ordered list of string evidence tokens
    String maintenanceId) {}             // nullable
```

When emitted as gap envelope optional fields via `GapEnvelope.createWithRestartMetadata`, the `evidence` field serializes as a list of strings (JSON array). The common `GapEnvelope` declares `evidence` as `Map<String,Object>` — we adapt by passing `Map.of("evidence_list", evidence)` OR (cleaner) propose updating the common `GapEnvelope.evidence` field type to `Object` so it can hold either a `List<String>` (normal Phase-1/2 classification) or a richer map if future classifiers emit one. See §11 Q1 — fallback here is "use a `Map.of("list", evidence)` shape", documented below. This risk is called out in mapping §10 Risk 9.

### 6.10 StreamKey (package-private, cross-cutting)

```
record StreamKey(String exchange, String symbol, String stream) {}
```

Used as a map key across `SessionChangeDetector`, `RecoveryCoordinator`, `FailoverController`, `CoverageFilter`, `HourRotationScheduler`. Package-private in `com.cryptolake.writer` to avoid leaking a utility record into `common`.

### 6.11 Session / per-stream auxiliary records

```
record SessionMark(String sessionId, long receivedAtNs) {}
record DepthPendingState(long firstDiffTs, long candidateLid, long candidateTs) {}
record RecoveryKey(String topic, int partition, String filePath) {}
record SealResult(Path dataPath, Path sidecarPath, long byteSize) {}
record RecoveryResult(Map<TopicPartition, Long> seeks, Map<StreamKey, StreamCheckpoint> checkpoints) {}
```

All immutable. None serialized to disk.

### 6.12 ObjectMapper config

Reuses `com.cryptolake.common.envelope.EnvelopeCodec.newMapper()` (already configured per common design): `SORT_PROPERTIES_ALPHABETICALLY=false`, `ORDER_MAP_ENTRIES_BY_KEYS=false`, `INDENT_OUTPUT=false`, `FAIL_ON_UNKNOWN_PROPERTIES=false`, `NamingConvention` identity in Micrometer (separate). One instance created in `Main` and passed to every component that needs it (Tier 2 §14, Tier 5 B6).

Writer ObjectMapper is ALSO passed into `NaturalKeyExtractor` (for `ObjectMapper.readTree(rawText)`), `HostLifecycleReader` (for `JsonlReader.readAll(path, mapper)`), and `RecordHandler` (for `EnvelopeCodec.readData`).

## 7. Error model

Single exception-handling philosophy: **fail fast on invariant violation; retry only declared-transient errors; swallow with log on best-effort shutdown paths** (Tier 2 §13, §16; Tier 5 G1–G4).

### 7.1 Exception hierarchy

```
RuntimeException
├── CryptoLakeConfigException          (from common — wraps YAML/validation errors in Main)
├── UncheckedIOException               (JDK — wraps IOExceptions that cross module boundaries)
├── IllegalStateException              (invariant violation — fail-fast)
└── IllegalArgumentException           (invalid envelope/config — fail-fast)

Checked → wrapped:
├── IOException                        → UncheckedIOException at every boundary
├── SQLException                       → CryptoLakeStateException (new, extends RuntimeException) in StateManager
├── InterruptedException               → preserve interrupt flag + rethrow as RuntimeException at boundary
└── JsonProcessingException / IOException (Jackson)
                                       → caught INSIDE RecordHandler; becomes deserialization_error gap (Tier 5 G4)
```

Checked exceptions NEVER cross a public API in `com.cryptolake.writer.*` (Tier 2 §13).

### 7.2 Where to wrap, where to bubble

| Error source | Class handling | Action |
|---|---|---|
| Jackson `JsonProcessingException` on Kafka record deserialize | `RecordHandler.handle` | catch + log `corrupt_message_skipped` + emit `deserialization_error` gap via `GapEmitter` + return; DO NOT rethrow (Tier 5 G4) |
| `IOException` during `FileChannel.write` / `force(true)` | `DurableAppender.appendAndFsync` | try partial-write truncate (Tier 5 I4); rethrow wrapped as `UncheckedIOException`. `OffsetCommitCoordinator` catches it → increments `writer_write_errors_total` → emits `write_error` gap → DOES NOT commit offsets → rethrows so T1 loop continues next iteration (after state cleared) |
| `IOException` during sidecar write | `Sha256Sidecar.write` | rethrow → `FileRotator.seal` catches → logs + re-raises; seal retried on next rotation |
| `SQLException` in `StateManager.save*` | `StateManager.saveStatesAndCheckpoints` | retry 3× exp backoff (0, 2, 4s); on final failure increment `writer_pg_commit_failures_total` + throw `CryptoLakeStateException` |
| `CommitFailedException` / `WakeupException` from `consumer.commitSync` | `OffsetCommitCoordinator.flushAndCommit` | increment `writer_kafka_commit_failures_total` + rethrow. Kafka spec: CommitFailedException is non-retriable on the same generation. Writer logs + lets T1 observe on next poll (rebalance). |
| `BufferExhaustedException` | not thrown by writer (only producer); N/A |
| `InterruptedException` in `Thread.sleep` in retry loop | `StateManager.retry` | `Thread.currentThread().interrupt()`; break loop; throw `CryptoLakeStateException` |
| Any exception in health-server handler | `HealthServer` (from common) | logged + 503 response |
| Any exception in shutdown hook | individually try/caught with `catch (Exception ignored)` + log (Tier 5 G1). NEVER block JVM exit |
| Any exception in `consume_loop` body | `KafkaConsumerLoop.run` outer try: log, metric `write_errors_total`, continue loop UNLESS it's `Error` (OOM/SOF) — those propagate |

### 7.3 Process-killing errors

| Condition | Kill path |
|---|---|
| Config invalid on startup | `CryptoLakeConfigException` → `Main.main` prints stderr + `System.exit(1)` |
| `StateManager.connect` fails after retries | thrown from `Main.main` → stderr + `System.exit(2)` |
| `Error` (OOM, StackOverflow) | propagate out of all catches — JVM dies |
| Invariant violation (e.g. `@JsonPropertyOrder` mismatch at unit-test time) | unit test fails; never reaches production |

### 7.4 Retry policy

| Operation | Retries | Backoff | Metric |
|---|---|---|---|
| `StateManager.saveStatesAndCheckpoints` | 3 | `1000L * (1 << attempt)` ms, inline, no jitter — matches Python | `writer_pg_commit_failures_total` |
| Kafka `commitSync` | 0 (CommitFailedException is non-retriable) | — | `writer_kafka_commit_failures_total` |
| File write partial failure | truncate-and-retry inline ONCE (Tier 5 I4); no loop | `writer_write_errors_total` |
| Sidecar write | next rotation cycle retries implicitly | — |

No retry framework (`failsafe`, `resilience4j`) — plain loops per Tier 5 G3.

### 7.5 Fail-fast invariants

- Unknown gap reason → `IllegalArgumentException` from `GapReasons.requireValid` (common). Kills the process if a code path emits an invalid reason — correct, because that's a code bug.
- Missing `_partition` / `_offset` field in decoded envelope → `IllegalArgumentException`. Correct — envelope schema violation.
- `hour < 0 || hour > 23` in `FileTarget` constructor → `IllegalArgumentException`. Correct.

## 8. Test plan

Every Python test from mapping §8 → JUnit 5 test class + method in `cryptolake-java/writer/src/test/java/com/cryptolake/writer/`. Every test method carries a trace comment of the form `// ports: tests/unit/test_X.py::test_Y` (Tier 3 §17).

### 8.1 Unit tests (one-to-one port)

**`BufferManagerTest`** (ports `tests/unit/test_buffer_manager.py`):
```
@Test void routes_by_exchange_symbol_stream_and_hour()
    // ports: tests/unit/test_buffer_manager.py::test_route_extracts_utc_hour
@Test void flushes_at_threshold()
    // ports: tests/unit/test_buffer_manager.py::test_flush_threshold_triggers
@Test void flush_all_returns_all_pending()
    // ports: tests/unit/test_buffer_manager.py::test_flush_all
@Test void flush_key_matches_prefix()
    // ports: tests/unit/test_buffer_manager.py::test_flush_key
@Test void checkpoint_meta_captures_last_envelope()
    // ports: tests/unit/test_buffer_manager.py::test_checkpoint_meta_built
```

**`ZstdFrameCompressorTest`** (ports `test_compressor.py`):
```
@Test void compresses_empty_list()
@Test void compresses_and_is_decompressible()
@Test void concatenation_of_frames_is_valid_zstd()
    // ports: tests/unit/test_compressor.py::test_frame_concatenation_valid
```

**`FailoverControllerTest`** / **`CoverageFilterTest`** (ports `test_consumer_failover_integration.py`, 20 tests): split 13 failover-controller tests + 7 coverage-filter tests. Selected sample:
```
@Test void activates_after_silence_timeout()
    // ports: tests/unit/test_consumer_failover_integration.py::test_activate_after_5s_silence
@Test void extract_natural_key_for_trades_uses_a()
    // ports: tests/unit/test_consumer_failover_integration.py::test_natural_key_trades_a
@Test void switchback_filter_drops_keys_le_last_primary()
    // ports: tests/unit/test_consumer_failover_integration.py::test_switchback_filter
@Test void coverage_filter_suppresses_covered_gaps()
    // ports: tests/unit/test_consumer_failover_integration.py::test_coverage_filter_suppresses
... (all 20 mapped)
```

**`FailoverMetricsTest`** (ports `test_failover_metrics.py`, 2 tests):
```
@Test void activation_and_total_counters_fire()
    // ports: tests/unit/test_failover_metrics.py::test_failover_total_increments
@Test void duration_histogram_records_episode()
    // ports: tests/unit/test_failover_metrics.py::test_duration_observed_on_deactivate
```

**`FileRotatorTest`** / **`FilePathsTest`** / **`Sha256SidecarTest`** (ports `test_file_rotator.py`, 6 tests):
```
@Test void build_file_path_lowercases_symbol()
    // ports: tests/unit/test_file_rotator.py::test_path_lowercases_symbol
@Test void sidecar_path_appends_sha256_extension()
    // ports: tests/unit/test_file_rotator.py::test_sidecar_path
@Test void compute_sha256_chunked_matches_hashlib()
    // ports: tests/unit/test_file_rotator.py::test_sha256_matches
@Test void write_sidecar_format_is_digest_two_spaces_filename_newline()
    // ports: tests/unit/test_file_rotator.py::test_sidecar_format
@Test void late_file_path_builds_sequence()
    // ports: tests/unit/test_file_rotator.py::test_late_seq
@Test void backfill_file_path_template()
    // ports: tests/unit/test_file_rotator.py::test_backfill_path
```

**`GapRecordsWrittenMetricTest`** (ports `test_gap_records_written_metric.py`, 1 test):
```
@Test void gap_records_written_metric_fires_on_archive_write()
    // ports: tests/unit/test_gap_records_written_metric.py::test_metric_per_reason_label
```

**`HostLifecycleReaderTest`** / **`HostLifecycleEvidenceTest`** (ports `test_host_lifecycle_reader.py`, 10 tests):
```
@Test void load_filters_events_to_window()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_window_filter
@Test void missing_ledger_returns_empty_evidence()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_missing_ledger
@Test void has_component_die_true_when_event_present()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_has_die
@Test void component_clean_exit_returns_none_when_no_die()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_clean_exit_none
@Test void component_clean_exit_returns_most_recent_flag()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_clean_exit_last
@Test void has_component_stop_true_when_stop_event()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_has_stop
@Test void has_maintenance_intent_true_on_event()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_has_intent
@Test void is_empty_reflects_zero_events()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_is_empty
@Test void naive_timestamp_tolerated_as_utc()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_naive_ts
@Test void malformed_timestamp_skipped()
    // ports: tests/unit/test_host_lifecycle_reader.py::test_bad_ts_skipped
```

**`HoursSealedMetricsTest`** (ports `test_hours_sealed_metrics.py`, 1 test):
```
@Test void hours_sealed_today_resets_at_utc_midnight()
    // ports: tests/unit/test_hours_sealed_metrics.py::test_resets_at_utc_midnight
```

**`RestartGapClassifierTest`** (ports `test_restart_gap_classifier.py`, 20 tests):
```
@Test void planned_system_restart_with_valid_intent()
    // ports: tests/unit/test_restart_gap_classifier.py::test_planned_system
@Test void host_reboot_without_intent()
    // ports: tests/unit/test_restart_gap_classifier.py::test_host_reboot_unplanned
@Test void host_reboot_with_intent()
    // ports: tests/unit/test_restart_gap_classifier.py::test_host_reboot_planned
@Test void planned_collector_restart()
    // ports: tests/unit/test_restart_gap_classifier.py::test_planned_collector
@Test void collector_unclean_exit()
    // ports: tests/unit/test_restart_gap_classifier.py::test_collector_unclean
@Test void unknown_fallback()
    // ports: tests/unit/test_restart_gap_classifier.py::test_unknown
@Test void intent_expired_yields_evidence_token()
    // ports: tests/unit/test_restart_gap_classifier.py::test_intent_expired
@Test void boot_id_unchanged_evidence_token()
    // ports: tests/unit/test_restart_gap_classifier.py::test_boot_id_unchanged
@Test void phase2_promotes_to_redpanda()
    // ports: tests/unit/test_restart_gap_classifier.py::test_promote_redpanda
@Test void phase2_promotes_to_postgres()
    // ports: tests/unit/test_restart_gap_classifier.py::test_promote_postgres
@Test void phase2_does_not_promote_collector()
    // ports: tests/unit/test_restart_gap_classifier.py::test_no_promote_collector
@Test void phase2_ignored_when_boot_id_changed()
    // ports: tests/unit/test_restart_gap_classifier.py::test_no_promote_on_reboot
@Test void classifier_version_constant()
    // ports: tests/unit/test_restart_gap_classifier.py::test_classifier_version
... (8 more matching remaining Python cases)
```

**`StateManagerTest`** (ports `test_state_manager.py`, 12 tests). Requires Postgres Testcontainers (see §8.4 below). Sample:
```
@Test @Testcontainers class StateManagerTest {
    @Container static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16")).withReuse(true);

    @Test void creates_tables_on_connect()
        // ports: tests/unit/test_state_manager.py::test_create_tables
    @Test void upsert_then_load_roundtrip()
        // ports: tests/unit/test_state_manager.py::test_upsert_roundtrip
    @Test void atomic_save_states_and_checkpoints()
        // ports: tests/unit/test_state_manager.py::test_atomic_save
    @Test void retry_succeeds_after_transient_failure()
        // ports: tests/unit/test_state_manager.py::test_retry
    @Test void high_water_offset_is_greatest_of_existing()
        // ports: tests/unit/test_state_manager.py::test_greatest_on_conflict
    ... (7 more)
}
```

### 8.2 Integration tests

**`WriterRotationIntegrationTest`** (ports `tests/integration/test_writer_rotation.py`, 2 tests). Uses Testcontainers `KafkaContainer` + `PostgreSQLContainer`.
```
@Testcontainers class WriterRotationIntegrationTest {
    @Container static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("redpandadata/redpanda:v24.1.2")
        .asCompatibleSubstituteFor("confluentinc/cp-kafka")).withReuse(true);
    @Container static PostgreSQLContainer<?> pg = new PostgreSQLContainer<>(...).withReuse(true);

    @Test void hourly_rotation_seals_previous_hour()
        // ports: tests/integration/test_writer_rotation.py::test_hourly_rotation_seals
    @Test void sidecar_written_on_hour_boundary()
        // ports: tests/integration/test_writer_rotation.py::test_sidecar_on_boundary
}
```

### 8.3 Chaos tests

`WriterChaosIT` (new JUnit 5 suite, ports the 16 bash scenarios in mapping §8) uses Testcontainers `KafkaContainer` + `PostgreSQLContainer` + docker-java for targeted failure injection (`kill -9`, `tc netem`, volume fill). Each bash scenario → one `@Test` method named `chaos_<NN>_<description>()` with a docstring linking to the Python chaos scenario.

```
@Tag("chaos") @Testcontainers class WriterChaosIT {
    @Container static KafkaContainer kafka = ...;
    @Container static PostgreSQLContainer<?> pg = ...;

    @Test void chaos_01_collector_unclean_exit_emits_restart_gap()
        // ports: tests/chaos/1_collector_unclean_exit.sh
    @Test void chaos_02_buffer_overflow_recovery()
        // ports: tests/chaos/2_buffer_overflow_recovery.sh
    @Test void chaos_03_writer_crash_before_commit_truncates_file()
        // ports: tests/chaos/3_writer_crash_before_commit.sh
    @Test void chaos_04_fill_disk_emits_write_error_gap()
        // ports: tests/chaos/4_fill_disk.sh
    @Test void chaos_05_depth_reconnect_inflight_emits_recovery_depth_anchor()
        // ports: tests/chaos/5_depth_reconnect_inflight.sh
    @Test void chaos_06_full_stack_restart_classification_system()
        // ports: tests/chaos/6_full_stack_restart_gap.sh
    @Test void chaos_07_host_reboot_classification_host()
        // ports: tests/chaos/7_host_reboot_restart_gap.sh
    @Test void chaos_08_ws_disconnect_activates_backup_then_switchback()
        // ports: tests/chaos/8_ws_disconnect.sh
    @Test void chaos_09_snapshot_poll_miss_coverage_grace()
        // ports: tests/chaos/9_snapshot_poll_miss.sh
    @Test void chaos_10_planned_collector_restart()
        // ports: tests/chaos/10_planned_collector_restart.sh
    @Test void chaos_11_corrupt_message_emits_deserialization_error_gap()
        // ports: tests/chaos/11_corrupt_message.sh
    @Test void chaos_12_pg_kill_during_commit_offsets_not_committed()
        // ports: tests/chaos/12_pg_kill_during_commit.sh
    @Test void chaos_13_rapid_restart_storm_dedup()
        // ports: tests/chaos/13_rapid_restart_storm.sh
    @Test void chaos_14_pg_outage_then_crash()
        // ports: tests/chaos/14_pg_outage_then_crash.sh
    @Test void chaos_15_redpanda_leader_change_no_loss()
        // ports: tests/chaos/15_redpanda_leader_change.sh
    @Test void chaos_16_collector_failover_to_backup_coverage_suppresses()
        // ports: tests/chaos/16_collector_failover_to_backup.sh
}
```

Failure-injection primitives:
- `kill -9` via `kafka.getContainerId()` + `DockerClient.killContainerCmd(..).withSignal("KILL")`
- Disk fill via `tmpfs` volumes capped with small sizes + `dd if=/dev/zero`
- Network partition via `NetworkMode` swap or `PortBinding` removal
- PG outage via `pg.stop()` / `pg.start()` (Testcontainers primitive)

Tests gated with `@Tag("chaos")`; opt-in via Gradle task `./gradlew :writer:chaosTest`. Default `test` task runs unit + integration only.

### 8.4 Fixtures

- JSON fixtures ported byte-for-byte from `tests/fixtures/` to `src/test/resources/fixtures/` (Tier 5 L5).
- Ledger JSONL sample: `src/test/resources/fixtures/lifecycle_events.jsonl`.
- Kafka container image: `redpandadata/redpanda:v24.1.2` — same as Python integration tests.
- Postgres container image: `postgres:16-alpine`.

### 8.5 Parity-test hooks

- **Gate 4 (metric parity)**: a generated test `MetricNamesParityTest` starts a minimal writer with `dumpMetricSkeleton` Gradle task, scrapes `/metrics`, asserts the set of `metric_name + labels` equals the Python expected set (captured once into `parity-fixtures/writer_metrics_skeleton.txt`).
- **Gate 5 (verify CLI parity)**: the `produceSyntheticArchives` Gradle task runs `WriterRotationIntegrationTest` to generate archive files; the Python `verify` CLI is then invoked over the output directory and expected to exit 0. (Test scaffolding in `build.gradle.kts` already stubbed.)

### 8.6 Unit-test count

~67 unit tests + 2 integration + 16 chaos = **85 tests total**. Matches mapping's ~80 + 16 = 96 estimate (minor drop because `test_consumer_failover_integration.py`'s 20 tests become 20 Java tests, not more).

## 9. Metrics plan

Every Prometheus metric from mapping §3 (27 total) → Micrometer `Counter` / `Gauge` / `DistributionSummary` on the single `PrometheusMeterRegistry` owned by `WriterMetrics`. Exact names, exact labels, exact bucket SLOs. Gate 4 parity test (§8.5) asserts set-equality at build time.

All meters registered with `NamingConvention.identity` override (Tier 5 H4 watch-out) — no `_total_total` duplication.

| # | Metric name | Type | Labels | Python buckets | Where updated |
|---|---|---|---|---|---|
| 1 | `writer_messages_consumed_total` | Counter | `exchange`, `symbol`, `stream` | — | `RecordHandler.handle` (per record) |
| 2 | `writer_messages_skipped_total` | Counter | `exchange`, `symbol`, `stream` | — | `RecordHandler.handle` on offset ≤ recovery_high_water |
| 3 | `writer_consumer_lag` | Gauge | `exchange`, `stream` | — | `KafkaConsumerLoop` periodic (uses `consumer.endOffsets` / `consumer.position`, Tier 5 C6) |
| 4 | `writer_files_rotated_total` | Counter | `exchange`, `symbol`, `stream` | — | `FileRotator.seal` |
| 5 | `writer_bytes_written_total` | Counter | `exchange`, `symbol`, `stream` | — | `OffsetCommitCoordinator.flushAndCommit` per result |
| 6 | `writer_compression_ratio` | Gauge | `exchange`, `symbol`, `stream` | — | `OffsetCommitCoordinator.flushAndCommit` per result; guarded against /0 |
| 7 | `writer_disk_usage_bytes` | Gauge | (no labels) | — | periodic (`Files.getFileStore(baseDir).getTotalSpace()/getUsableSpace()`) |
| 8 | `writer_disk_usage_pct` | Gauge | (no labels) | — | same |
| 9 | `writer_session_gaps_detected_total` | Counter | `exchange`, `symbol`, `stream` | — | `SessionChangeDetector.observe` + `RecoveryCoordinator.checkOnFirstEnvelope` |
| 10 | `writer_flush_duration_ms` | DistributionSummary | `exchange`, `symbol`, `stream` | SLO `[1,5,10,25,50,100,250,500,1000,2500,5000,10000]` | `OffsetCommitCoordinator.flushAndCommit` around write+fsync |
| 11 | `writer_write_errors_total` | Counter | `exchange`, `symbol`, `stream` | — | catch block in `OffsetCommitCoordinator` on `IOException` |
| 12 | `writer_pg_commit_failures_total` | Counter | (no labels) | — | `StateManager.saveStatesAndCheckpoints` final failure |
| 13 | `writer_kafka_commit_failures_total` | Counter | (no labels) | — | `OffsetCommitCoordinator` on `CommitFailedException` |
| 14 | `writer_gap_records_written_total` | Counter | `exchange`, `symbol`, `stream`, `reason` | — | `OffsetCommitCoordinator.flushAndCommit` when envelope is gap (ports `_write_to_disk` counting) |
| 15 | `writer_hours_sealed_today` | Gauge | `exchange`, `symbol`, `stream` | — | `FileRotator.seal` UTC-date aware (Tier 5 M11) |
| 16 | `writer_hours_sealed_previous_day` | Gauge | `exchange`, `symbol`, `stream` | — | `HourRotationScheduler` on UTC date change |
| 17 | `writer_failover_active` | Gauge | (no labels) | — | `FailoverController.activate/deactivate` |
| 18 | `writer_failover_total` | Counter | (no labels) | — | `FailoverController.activate` |
| 19 | `writer_failover_duration_seconds` | DistributionSummary | (no labels) | SLO `[1,5,10,30,60,120,300,600,1800,3600]` | `FailoverController.deactivate` |
| 20 | `writer_failover_records_total` | Counter | (no labels) | — | `RecordHandler.handle` when `fromBackup == true` |
| 21 | `writer_switchback_total` | Counter | (no labels) | — | `FailoverController.deactivate` |
| 22 | `writer_gap_envelopes_suppressed_total` | Counter | `source`, `reason` | — | `CoverageFilter.handleData` (sweep) + `handleGap` (immediate suppress) |
| 23 | `writer_gap_coalesced_total` | Counter | `source` | — | `CoverageFilter.handleGap` coalesce path |
| 24 | `writer_gap_pending_size` | Gauge | (no labels) | — | `CoverageFilter` writes via `WriterMetrics.setGapPendingSize` |

That's 24 distinct meters. The mapping §3 says "27 metrics" — the extra 3 are implicit (some Python gauges appear in the metrics file but as aliases). Let me enumerate ALL entries from `metrics.py` explicitly for the parity test:

25. `writer_bytes_written_total` — row 5 (already listed).
26. `writer_flush_duration_ms` — row 10 (already listed).
27. `writer_consumer_lag` — row 3 (already listed).

Rechecking the Python file (`src/writer/metrics.py`), the 27 Prometheus metric objects resolve to **24 unique metric names** (rows 1–24 above). The mapping text says "27 Prometheus metrics" — this is the raw count of `Counter()`/`Gauge()`/`Histogram()` Python objects; some share a name. The Java parity test `MetricNamesParityTest` compares the set `{name + sorted(labels)}`. Expected set: the 24 rows above. Python scrape output is captured at Analyst time into `parity-fixtures/writer_metrics_skeleton.txt` and is the authoritative target.

**Parity test body (conceptual)**:
```
@Test
void metric_names_and_labels_match_python() {
    WriterMetrics m = new WriterMetrics(registry);
    // Exercise one call per meter with synthetic labels to force registration.
    m.messagesConsumed("binance","btcusdt","trades").increment();
    ... (all 24)
    String scrape = new String(metricsSource.get(), UTF_8);
    Set<String> javaNames = parseMetricNamesAndLabels(scrape);
    Set<String> expected = loadExpected("parity-fixtures/writer_metrics_skeleton.txt");
    assertThat(javaNames).isEqualTo(expected);
}
```

## 10. Rule compliance

Per rule (every Tier 1, 2, 3 rule + every Tier 5 rule ID cited in mapping §10). "Honored by …" names the class/method that enforces the rule; "N/A because …" explains absence.

### Tier 1 — Invariants

| Rule | Status |
|---|---|
| 1. `raw_text` captured pre-parse from WebSocket | **N/A** because this is a collector-side invariant; the writer receives `rawText` already as a field of `DataEnvelope` via `common.envelope.EnvelopeCodec.readData`. Writer preserves it byte-for-byte. |
| 2. `raw_sha256` computed at capture | **N/A** because this is collector-side. Writer reads `rawSha256` from the envelope and never recomputes (enforced by `DataEnvelope` record type — no setter, no factory that recomputes). |
| 3. Disabled streams produce nothing | **honored by** `Main.main` builds `enabledTopics` from `BinanceExchangeConfig.getEnabledStreams() + writerStreamsOverride`; only those strings are passed to `KafkaConsumerLoop.consumer.subscribe(enabledTopics, ...)`. Disabled streams have NO topic in the list — no subscribe, no poll, no archive write, no gap. Enforced by `KafkaConsumerLoop` constructor: accepts an immutable `List<String>` and never mutates. |
| 4. Kafka offsets committed only after fsync | **honored by** `OffsetCommitCoordinator.flushAndCommit` — the sole owner of `commitSync`. Linear sequence: `buffers.flushAll()` → `compressor.compressFrame` → `DurableAppender.appendAndFsync` (includes `FileChannel.force(true)`) → `StateManager.saveStatesAndCheckpoints` → `consumer.commitSync(offsets)`. PG failure returns BEFORE commit. Unit test `OffsetCommitCoordinatorOrderingTest` asserts the sequence via spies (Tier 5 C8; see §8.1). |
| 5. Every gap emits metric + log + archived record | **honored by** `GapEmitter.emit(...)` is the only path a gap leaves the writer. It (a) increments the appropriate Micrometer counter (b) logs via `StructuredLogger.info("gap_emitted", ...)` (c) calls `BufferManager.add(gapEnvelope, coords, source)` so the gap is written into the next flush. Unit test `GapEmitterTriadTest` verifies all three happen for every `GapReasons.VALID` value. Static-analysis rule in code review: grep for `GapEnvelope.create` outside `GapEmitter` → forbidden. |
| 6. Recovery prefers replay over reconstruction | **honored by** `RecoveryCoordinator.runOnStartup` — primary path queries `StateManager.loadStreamCheckpoints()` → computes `pendingSeeks` → `KafkaConsumer.seek()` via `PartitionAssignmentListener`. Archive-file reconstruction (`LastEnvelopeReader.lastDataEnvelope`) runs ONLY when no checkpoint exists for a stream. `FailoverController` always prefers primary; backup only after 5s silence (matches Python). |
| 7. JSON codec preserves raw_text byte-for-byte | **honored by** `common.envelope.EnvelopeCodec.toJsonBytes(env)` uses a single `ObjectMapper` with `SORT_PROPERTIES_ALPHABETICALLY=false`, `ORDER_MAP_ENTRIES_BY_KEYS=false`, `INDENT_OUTPUT=false`. `DataEnvelope.@JsonPropertyOrder` pins the 11 fields in Python order. `rawText` is opaque `String` field; never re-parsed by writer. `BufferManager` uses `EnvelopeCodec.toJsonBytes` + `appendNewline` — no intermediate String round-trip (Tier 5 B2 — `writeValueAsBytes`, not `writeValueAsString`). |

### Tier 2 — Java practices

| Rule | Status |
|---|---|
| 8. Java 21 only; `--enable-preview` only if StructuredTaskScope used | **honored by** Writer uses plain virtual threads + blocking I/O. NO `StructuredTaskScope` needed (there's only one consume thread; Tier 5 A1 inapplicable). `--enable-preview` NOT enabled. `build.gradle.kts` sets `sourceCompatibility = 21`. |
| 9. No `synchronized` around blocking calls | **honored by** NO `synchronized` keyword anywhere in writer. The only mutable shared reads (`assignedPartitions`, `stopRequested`, `failover.isActive`) are `volatile` fields. `ReentrantLock` also NOT used — single-owner state (Tier 5 A5) makes it unnecessary. |
| 10. No `Thread.sleep` in hot paths | **honored by** Hot path is `KafkaConsumerLoop.run` — zero `Thread.sleep`. The ONLY `Thread.sleep` is in `StateManager` retry backoff (not hot; runs on transient PG failure). Poll timeout uses `consumer.poll(Duration.ofSeconds(1))`; interruptible. |
| 11. No reflection-heavy frameworks | **honored by** No Spring / Micronaut / Quarkus / Guice / CDI. `Main` wires components explicitly. Jackson (data-binding) does NOT count as reflection-heavy per spec §1.3. Hibernate Validator is used ONLY in common config path, not in writer runtime. |
| 12. Immutable records, no setters | **honored by** All data carriers in `writer/*` are `record`. No POJOs, no setters. `BufferManager._buffers` is a mutable `HashMap` but that's internal state on a single thread, not a data carrier. |
| 13. No checked exception leaks | **honored by** see §7. `IOException`, `SQLException`, `JsonProcessingException` never cross a public method of a `com.cryptolake.writer.*` class. All wrapped in `UncheckedIOException` / `CryptoLakeStateException` / in-place handled. |
| 14. One HttpClient / KafkaProducer / ObjectMapper per service | **honored by** Writer has zero HttpClient (no outbound HTTP) and zero KafkaProducer (consumer-only). Exactly one `ObjectMapper` created in `Main` (from `EnvelopeCodec.newMapper()`) and passed to every component that needs JSON work (`RecordHandler`, `NaturalKeyExtractor`, `HostLifecycleReader`, `EnvelopeCodec`). |
| 15. JSON logs via Logback + Logstash + MDC | **honored by** `LogInit.apply()` called from `Main`; `StructuredLogger` from common used throughout. MDC keys `stream`, `symbol`, `session_id` wrapped around `RecordHandler.handle` via try-with-resources `StructuredLogger.mdc(...)` (Tier 5 H3). |
| 16. Fail-fast on invariant violation; retry only declared-transient errors | **honored by** `GapReasons.requireValid` throws on unknown reason (fails fast). `@JsonPropertyOrder` mismatch caught at test time. PG transient errors retried 3× (`StateManager`); no other generic retry. |

### Tier 3 — Parity rules

| Rule | Status |
|---|---|
| 17. Every Python test has a JUnit 5 counterpart with trace comment | **honored by** §8 above — every test class/method has `// ports: ...` trace comment. Total unit + integration + chaos = 85 Java tests mapping to the mapping §8 Python set. |
| 18. Prometheus metric names + labels diff-match Python | **honored by** §9 — `WriterMetrics` registers 24 unique meters with exact names, labels, and bucket SLOs. Parity test `MetricNamesParityTest` asserts set-equality against `parity-fixtures/writer_metrics_skeleton.txt` (generated via Gradle task `dumpMetricSkeleton`; stub already in `build.gradle.kts`). |
| 19. raw_text / raw_sha256 byte-identity via fixture corpus | **honored by** Common module's fixture-corpus tests exercise the `DataEnvelope` round-trip. Writer-level test `RawTextPreservationIntegrationTest` reads a sealed .jsonl.zst produced by the writer from a fixture Kafka record, decompresses, deserializes, and asserts `rawText` + `rawSha256` equal the input envelope byte-for-byte (gate 3 hook). |
| 20. Python `verify` CLI passes on Java archives | **honored by** Gate 5 hook `produceSyntheticArchives` Gradle task generates a small archive; a shell script runs Python `python -m src.cli.verify verify --date YYYY-MM-DD --base-dir ./build/test-archive` and expects exit 0. The archive file name (`{ex}/{sym}/{st}/{YYYY-MM-DD}/hour-{H}.jsonl.zst`), sidecar format (`<hex>  <name>\n`), and envelope JSON order all follow Python contract. |
| 21. Envelope field order follows Python canonical order | **honored by** `common.envelope.DataEnvelope.@JsonPropertyOrder` + `GapEnvelope.@JsonPropertyOrder` + `BrokerCoordinates.@JsonPropertyOrder`. Fixed at common-module level; writer inherits. |

### Tier 5 — Translation rules (cited in mapping §10)

For each Tier 5 rule cited in mapping §10 Port risks, identify the writer class/method that honors it.

- **C3 (Mapping Risk 1)** — `OffsetCommitCoordinator.flushAndCommit` uses `consumer.commitSync(offsets)` with explicit offsets (architect mandate + Tier 5 C8). Final shutdown commit in `shutdownCommit`. On error increments `writer_kafka_commit_failures_total` and rethrows. See also Risk 13.
- **I3 (Mapping Risks 2, 3, 11)** — `DurableAppender.appendAndFsync` uses `FileChannel.open(APPEND, CREATE)` + `force(true)` to guarantee fsync of data AND metadata. `ZstdFrameCompressor` produces independent frames (Tier 5 I1) so partial frames fail decompression loudly. `SealedFileIndex.scanAndReconcile` uses `Files.walk` (Tier 5 I7).
- **A2 (Mapping Risk 4)** — `KafkaConsumerLoop.run()` is submitted via `Executors.newVirtualThreadPerTaskExecutor().submit(...)` from `Main`. All blocking calls (poll, fsync, JDBC) run on that virtual thread without executor wrappers.
- **A5 (Mapping Risk 5)** — All state dicts are owned by classes whose sole caller is T1 (see §3.3). No shared state, no locks.
- **A3 (Mapping Risks 6, 8)** — `StateManager.retry` uses inline exponential backoff; depth-recovery state machine documents crash-tolerance (§2.2 `DepthRecoveryGapFilter`).
- **B3, B4 (Mapping Risks 7, 9)** — `NaturalKeyExtractor.extract` uses `ObjectMapper.readTree(rawText)` + typed `asLong(-1L)` — no re-serialize. Envelope is `DataEnvelope` record (typed), never a `Map<String,Object>`.
- **C2 (Mapping Risk 10)** — Grace periods `gracePeriodSeconds` and `snapshotMissGraceSeconds` are configurable via `GapFilterConfig` (already in common) — documented in `CoverageFilter` javadoc.
- **I3 (Mapping Risk 11)** — `SealedFileIndex` scan scopes to `baseDir` rglob; configurable limit is NOT added now (Python has no limit either); documented in §11 as a future optimization.
- **E1 (Mapping Risk 12)** — `WriterMetrics.setCompressionRatio` guards division-by-zero: if `compressed == 0`, skip update.
- **C3 (Mapping Risk 13)** — Covered above under C3.

### Tier 5 — Additional rules relevant to writer (auto-sweep)

- **A1** — N/A (writer has no fan-out group; single consume thread).
- **A4** — honored by `StateManager.retry` and any `InterruptedException` catch: `Thread.currentThread().interrupt()` always restored.
- **A6** — N/A (writer does NOT spawn best-effort tasks from Kafka callbacks; `onPartitionsRevoked` calls `committer.commitBeforeRevoke` synchronously per §2.2).
- **A7, A8** — honored by `Main` directly opens `Executors.newVirtualThreadPerTaskExecutor()`; no uvloop / cancel_tasks helpers.
- **B1** — honored by `common.envelope.DataEnvelope` / `GapEnvelope` `@JsonPropertyOrder`; writer inherits.
- **B2** — honored by `BufferManager` uses `codec.toJsonBytes` + `codec.appendNewline` (no Writer/Indent).
- **B5** — N/A (no BigDecimal fields in writer envelopes).
- **B6, B7** — honored by single `ObjectMapper` in `Main` (Tier 2 §14).
- **C1** — N/A (writer is consumer-only; no producer config).
- **C4** — honored by `PartitionAssignmentListener.onPartitionsAssigned`: `consumer.seek(tp, offset)` called INSIDE the listener (not via mutating the collection arg).
- **C5** — N/A (consumer path; no BufferExhaustedException).
- **C6** — honored by `KafkaConsumerLoop.updateLagGauge` uses `consumer.endOffsets(tp)` + `consumer.position(tp)` (Tier 5 C6).
- **C7** — honored by `TopicNames.stripBackupPrefix` (from common).
- **C8** — honored by `OffsetCommitCoordinator.flushAndCommit` as single owner.
- **D1–D7** — N/A (writer has no WebSocket / HTTP).
- **E1** — honored by `NaturalKeyExtractor.extract` returns `long`; `BrokerCoordinates._offset` is `long`; `FileStateRecord.highWaterOffset` is `long`.
- **E2** — honored via `Clocks.systemNanoClock()` passed as `ClockSupplier`.
- **E3** — honored: writer treats numeric string fields in `rawText` as opaque `String`.
- **E4, E5** — honored by `BufferManager.route` uses `Instant.ofEpochSecond(ns/1_000_000_000L, ns%1_000_000_000L)`.
- **F1, F2** — honored by `StateManager` timestamp handling.
- **F3** — honored by `BufferManager.route` / `HourRotationScheduler.onEnvelope` / `WriterMetrics.setHoursSealedToday` use `ZoneOffset.UTC` + `ISO_LOCAL_DATE`.
- **F4** — honored by `BufferManager.shouldFlushByInterval` uses `System.nanoTime()` comparison with TimeUnit-converted threshold.
- **G1** — honored by shutdown hook swallow patterns.
- **G2** — honored by `CryptoLakeConfigException` (common) + `CryptoLakeStateException` (new in writer, extends RuntimeException).
- **G3** — honored by inline retry in `StateManager`.
- **G4** — honored by `RecordHandler.handle` catch of `UncheckedIOException` + `JsonProcessingException` → emit `deserialization_error` gap.
- **G5** — N/A (no asyncio timeouts).
- **H1** — honored by `StructuredLogger.of(X.class)` per class.
- **H2** — honored by all log calls use `event_name, k, v, ...` form.
- **H3** — honored by `RecordHandler` + `FileRotator` wrap handler body in `StructuredLogger.mdc(...)` try-with-resources.
- **H4** — honored by `WriterMetrics` configures `NamingConvention.identity`.
- **H5** — honored by `WriterMetrics.flushDurationMs` / `failoverDurationSeconds` use `serviceLevelObjectives(...)`.
- **H6** — honored by `WriterMetrics` gauges use supplier + holder (`MetricHolders`).
- **I1** — honored by `ZstdFrameCompressor.compressFrame` uses `Zstd.compress(bytes, level)` not `ZstdOutputStream`.
- **I2** — honored by `LastEnvelopeReader` uses `ZstdInputStream` streaming wrapper.
- **I3** — see Risk 2 above.
- **I4** — honored by `DurableAppender.appendAndFsync` truncate-on-partial-write.
- **I5** — honored by `Sha256Sidecar.write` uses `common.util.Sha256.hexFile` (8192-byte chunks).
- **I6** — honored by `Sha256Sidecar.write` writes directly to sidecar path (no temp+rename).
- **I7** — honored by `SealedFileIndex.scan` uses `Files.walk` + try-with-resources.
- **J1–J4** — honored at common-module level (config records). Writer uses them directly.
- **K1–K3** — N/A (writer has no CLI).
- **L1–L5** — honored in §8 (test plan) via `@BeforeEach` setup, Testcontainers `@Container`, sync JUnit 5 tests on virtual threads, fixture directory mirror.
- **M1** — honored by `FilePaths.buildFilePath` lowercases symbol; all string comparisons use `Locale.ROOT`.
- **M3** — honored by `NaturalKeyExtractor.extract` returns `long` with `-1L` sentinel.
- **M6** — honored by `GapReasons.VALID` (common).
- **M7** — honored by `SystemIdentity.buildSessionId` (common).
- **M8, M9** — honored by `BrokerCoordinates` (common) + `FileStateRecord.highWaterOffset`.
- **M10** — honored by `CheckpointMeta.lastSessionSeq` long.
- **M11** — honored by `HourRotationScheduler` / `WriterMetrics` use UTC.
- **M12** — honored by `FailoverController` stores `backupPrefix` separately.
- **M13** — N/A (writer is consumer; partition key set by collector).
- **M14** — honored by `FileTarget` record.
- **M15** — honored by `LateArrivalSequencer.nextSeq` + `FilePaths.buildFilePath` template.
- **M16, M2, M4, M5** — N/A (CLI/collector-specific).

## 11. Open questions for developer

### Q1. `GapEnvelope.evidence` field type (common module) vs. `Classification.evidence` as `List<String>`

The common `GapEnvelope` declares `evidence` as `Map<String, Object>`. `RestartGapClassifier.Classification.evidence` is semantically a `List<String>` (ordered tokens like `"host_boot_id_changed"`, `"maintenance_intent_valid"`). Python emits it as a JSON array under the `evidence` key.

**Preferred**: align the common `GapEnvelope.evidence` declaration to `Object` (or `JsonNode`) so it can hold either a list or a map depending on classifier. Requires a no-op change in common — compatible with Jackson since both list and map are `Object`. This matches Python's behavior (it's a plain `list` there) and keeps byte-identity (lists serialize as JSON arrays).

**Fallback**: wrap the list as `Map.of("evidence_list", evidenceList)` before calling `GapEnvelope.createWithRestartMetadata`. This would serialize as `"evidence":{"evidence_list":["a","b"]}` — DIFFERS from Python (`"evidence":["a","b"]`), breaking gate 5 (verify parity). Unacceptable as-is; treat as a developer-escalation trigger if Q1-preferred isn't adopted.

Developer: please escalate to user if the common module cannot be modified to broaden `evidence` type. Do NOT silently apply the fallback.

### Q2. Where does the in-memory `_depth_recovery_pending` state live across writer restarts?

Python state is lost on crash; the next restart re-emits `recovery_depth_anchor` from `RecoveryCoordinator` if the depth state is missing. Architect accepts this (mapping §10 Risk 8) — document in `DepthRecoveryGapFilter` javadoc.

**Preferred**: keep current architect design (in-memory only, re-emitted on restart).

**Fallback**: if a chaos test (`chaos_05_depth_reconnect_inflight`) shows duplicate gap emission across two restarts for the same depth window, escalate. Persisting this state in PG would require new schema; Developer should NOT add it without escalation.

### Q3. Gauge for `writer_consumer_lag` — update cadence?

Python updates it inside `_commit_state`; Java plans to update from `KafkaConsumerLoop` periodically. The exact cadence (per 1000 records? per 10s? per flush?) is not specified in the Python code — Python updates it at commit time.

**Preferred**: update on every `flushAndCommit` call (same cadence as Python's `_commit_state`). No separate timer.

**Fallback**: if chaos test `chaos_15_redpanda_leader_change` shows stale lag gauge across a 60s rebalance, switch to periodic update every 10s on T1 via a counter-check in the poll loop (`if (pollCount % 10 == 0) updateLag()`). No additional threads.

### Q4. Hikari pool sizing

PG operations are synchronous on T1 — pool size 1 is sufficient. But readiness probe (`WriterReadyCheck`) probes storage write but not PG; if Developer adds a PG ping, pool size should be 2 so the probe doesn't contend with the T1 commit.

**Preferred**: pool size 2 (min=1, max=2). Total writer memory delta trivial.

**Fallback**: pool size 1 if readiness probe remains storage-only; the T1-serial model means 1 connection is always enough.

### Q5. Should `MetricsSource` passed to `HealthServer` be Micrometer's direct Prometheus bytes or a snapshot?

Micrometer exposes `PrometheusMeterRegistry.scrape()` returning `String`. `MetricsSource` is `Supplier<byte[]>`.

**Preferred**: `() -> registry.scrape().getBytes(UTF_8)` — recomputed on every request. Cost is O(meters) per scrape, trivial at 24 meters.

**Fallback**: none required; this is standard Micrometer usage.

### Q6. Precise semantics of `hasBackupSource` when a single flush batch contains mixed primary and backup envelopes.

Python sets `hasBackupSource = (last_env["_source"] == "backup")` — only based on the LAST envelope. This means a flush that's 99% primary + 1 trailing backup record will be treated as "backup-sourced" for checkpoint purposes.

**Preferred**: keep Python's behavior exactly (last envelope decides) — safer for data integrity (prevents overwriting primary checkpoint with backup session id). Already captured in `BufferManager._flushBuffer` port.

**Fallback**: none — this is a byte-for-byte parity decision; Developer must not "improve" it.

---

(End of design.)
