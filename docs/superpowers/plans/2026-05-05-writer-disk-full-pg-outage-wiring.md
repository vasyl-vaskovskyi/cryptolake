# Writer DiskFull / PgOutage Hold-Controller Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing-but-orphaned `DiskFullHoldController` and `PgOutageHoldController` into the writer's actual flush/commit code path so chaos scenarios 02 and 07 exercise real production behavior.

**Architecture:** Both controllers become constructor parameters of `OffsetCommitCoordinator`. `flushAndCommit` early-returns 0 when either hold is active. The IOException catch on `appendAndFsync` calls `diskHold.onWriteError(e)` and short-circuits when ENOSPC. The `CryptoLakeStateException` catch on PG save calls `pgHold.recordPgFailure()` and short-circuits when the threshold flips hold active. The PG-save success path calls `pgHold.recordPgSuccess()`. Lifecycle (`start`/`stop`) handled in `Main.java` next to the consume-loop startup and shutdown-hook teardown.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Mockito, Gradle wrapper. No new dependencies. Spec at `docs/superpowers/specs/2026-05-05-writer-disk-full-pg-outage-wiring-design.md`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java` | modify | Append two constructor params (controllers). Early-return at top of `flushAndCommit`. Modify IOException catch. Modify `CryptoLakeStateException` catch. Add `pgHold.recordPgSuccess()` on success. |
| `writer/src/main/java/com/cryptolake/writer/Main.java` | modify | Construct both controllers (probes, symbol/stream list, factory). Pass to `OffsetCommitCoordinator`. `start()` before consume loop spawns. `stop()` in shutdown hook after `shutdownLatch.await()` returns. |
| `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorTest.java` | modify | Update three constructor call sites to pass real controller instances (constructed once at top of file via a small helper). Asserts unchanged. |
| `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java` | new | Five integration tests covering: pre-active early-return; ENOSPC entry; non-ENOSPC propagation; recovery + accumulated commit; PG threshold. Uses real controller instances + Mockito-mocked `KafkaConsumer`/`DurableAppender`/`StateManager`. |

`DiskFullHoldController.java` and `PgOutageHoldController.java` are NOT modified — their internals are correct; we only consume them.

---

## Task 1: Constructor wiring + early-gate (test 3)

**Goal:** Add the two controllers as constructor params; early-return from `flushAndCommit` when either hold is already active.

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`
- Modify: `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorTest.java` (3 call sites)
- Create: `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java`

- [ ] **Step 1.1: Create the integration test file with the early-return test (FAILING)**

Use the `Write` tool to create `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java` with this exact content:

```java
package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.durability.DiskFullHoldController;
import com.cryptolake.writer.durability.PgOutageHoldController;
import com.cryptolake.writer.io.DurableAppender;
import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.metrics.WriterMetrics;
import com.cryptolake.writer.recovery.SealedFileIndex;
import com.cryptolake.writer.state.StateManager;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the DiskFull / PgOutage hold controller wiring inside {@link
 * OffsetCommitCoordinator#flushAndCommit}. Uses real controller instances (no mocks) so the
 * state-machine semantics are exercised end-to-end. Outer collaborators ({@link KafkaConsumer},
 * {@link DurableAppender}, {@link StateManager}) are Mockito-mocked because they are out of test
 * scope.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-disk-full-pg-outage-wiring-design.md.
 */
class OffsetCommitCoordinatorHoldIntegrationTest {

  private static final String EXCHANGE = "binance";
  private static final List<DiskFullHoldController.SymbolStream> DISK_SS =
      List.of(new DiskFullHoldController.SymbolStream("btcusdt", "trades"));
  private static final List<PgOutageHoldController.SymbolStream> PG_SS =
      List.of(new PgOutageHoldController.SymbolStream("btcusdt", "trades"));

  private static DiskFullHoldController newDiskHoldNeverFreeing() {
    AtomicBoolean diskFree = new AtomicBoolean(false); // probe always reports "still full"
    return new DiskFullHoldController(
        Clocks.systemNanoClock(), diskFree::get, (sym, str, seq, r, d, s, e) -> {}, DISK_SS);
  }

  private static PgOutageHoldController newPgHoldNeverRecovering() {
    AtomicBoolean pgUp = new AtomicBoolean(false); // probe always reports "still down"
    return new PgOutageHoldController(
        Clocks.systemNanoClock(), pgUp::get, (sym, str, seq, r, d, s, e) -> {}, PG_SS);
  }

  /** Test 3: hold pre-active → flushAndCommit early-returns 0 without touching any collaborator. */
  @Test
  void flushAndCommit_returnsZero_immediately_whenDiskHoldAlreadyActive() {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task1", 100, 60, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    // Pre-activate disk hold so the gate short-circuits before any I/O.
    diskHold.onWriteError(new IOException("No space left on device"));
    assertThat(diskHold.isHoldActive()).isTrue();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    int n = coord.flushAndCommit(buffers);

    assertThat(n).isEqualTo(0);
    // No collaborators touched — early-return happened before flushAll().
    verify(consumer, never()).commitSync(any(Map.class));
    try {
      verify(appender, never()).appendAndFsync(any(), any());
    } catch (IOException unreachable) {
      throw new AssertionError(unreachable);
    }
  }
}
```

Notes on the test:
- The two "never" lambdas (probes returning `false`) ensure controllers never auto-recover during the test, so once we put the disk-hold into the active state via `onWriteError`, it stays active.
- The `BufferManager` constructor signature mirrors the existing `OffsetCommitCoordinatorTest.java` — same args.
- We pre-activate disk hold by calling `onWriteError` directly (not via the coordinator), then construct the coordinator and call `flushAndCommit`. The early-return at the top of `flushAndCommit` is what we're verifying.
- The `Map` import for the `verify(consumer, never()).commitSync(any(Map.class))` line — note `Map` is `java.util.Map` already imported above.

- [ ] **Step 1.2: Run the test, confirm it fails for the right reason**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: COMPILE FAILURE. The error will be along the lines of "constructor OffsetCommitCoordinator cannot be applied to given types" — the existing constructor takes 7 args; the test passes 9. This is the failing-test confirmation.

- [ ] **Step 1.3: Update `OffsetCommitCoordinator` constructor and add the early-return gate**

Open `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`. Find this exact block (around lines 59–92):

```java
public final class OffsetCommitCoordinator {

  private static final Logger log = LoggerFactory.getLogger(OffsetCommitCoordinator.class);

  private final KafkaConsumer<byte[], byte[]> primary;
  private final DurableAppender appender;
  private final ZstdFrameCompressor compressor;
  private final StateManager stateManager;
  private final SealedFileIndex sealedIndex;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /**
   * Mutable cache of durable checkpoints — updated after every successful PG+Kafka commit. Owned by
   * T1; shared (read-only) with RecoveryCoordinator via the method view.
   */
  private final Map<StreamKey, StreamCheckpoint> durableCheckpoints = new HashMap<>();

  public OffsetCommitCoordinator(
      KafkaConsumer<byte[], byte[]> primary,
      DurableAppender appender,
      ZstdFrameCompressor compressor,
      StateManager stateManager,
      SealedFileIndex sealedIndex,
      WriterMetrics metrics,
      ClockSupplier clock) {
    this.primary = primary;
    this.appender = appender;
    this.compressor = compressor;
    this.stateManager = stateManager;
    this.sealedIndex = sealedIndex;
    this.metrics = metrics;
    this.clock = clock;
  }
```

Replace with (note: adds two fields, adds two ctor args, both nullable for backward compat with empty-buffer tests in OffsetCommitCoordinatorTest — but real callers MUST pass non-null):

```java
public final class OffsetCommitCoordinator {

  private static final Logger log = LoggerFactory.getLogger(OffsetCommitCoordinator.class);

  private final KafkaConsumer<byte[], byte[]> primary;
  private final DurableAppender appender;
  private final ZstdFrameCompressor compressor;
  private final StateManager stateManager;
  private final SealedFileIndex sealedIndex;
  private final WriterMetrics metrics;
  private final ClockSupplier clock;

  /**
   * Disk-full hold controller. When {@link DiskFullHoldController#isHoldActive()} returns true,
   * {@link #flushAndCommit} early-returns 0 without flushing buffers or committing offsets.
   * Nullable for legacy unit tests that exercise empty-buffer paths only; real wiring (Main.java)
   * always provides a non-null instance.
   */
  private final com.cryptolake.writer.durability.DiskFullHoldController diskHold;

  /**
   * PG-outage hold controller. Same role as {@link #diskHold} but for prolonged PG unavailability.
   * Nullable for legacy unit tests.
   */
  private final com.cryptolake.writer.durability.PgOutageHoldController pgHold;

  /**
   * Mutable cache of durable checkpoints — updated after every successful PG+Kafka commit. Owned by
   * T1; shared (read-only) with RecoveryCoordinator via the method view.
   */
  private final Map<StreamKey, StreamCheckpoint> durableCheckpoints = new HashMap<>();

  public OffsetCommitCoordinator(
      KafkaConsumer<byte[], byte[]> primary,
      DurableAppender appender,
      ZstdFrameCompressor compressor,
      StateManager stateManager,
      SealedFileIndex sealedIndex,
      WriterMetrics metrics,
      ClockSupplier clock,
      com.cryptolake.writer.durability.DiskFullHoldController diskHold,
      com.cryptolake.writer.durability.PgOutageHoldController pgHold) {
    this.primary = primary;
    this.appender = appender;
    this.compressor = compressor;
    this.stateManager = stateManager;
    this.sealedIndex = sealedIndex;
    this.metrics = metrics;
    this.clock = clock;
    this.diskHold = diskHold;
    this.pgHold = pgHold;
  }
```

Now add the early-return gate inside `flushAndCommit`. Find this exact block (around lines 107–115):

```java
  public int flushAndCommit(BufferManager buffers) {
    // Step 1: Extract all buffered lines (in-memory, no I/O yet)
    List<FlushResult> results = buffers.flushAll();

    if (results.isEmpty()) {
      return 0;
    }
```

Replace with:

```java
  public int flushAndCommit(BufferManager buffers) {
    // Hold-controller gate (spec §Architecture; design path 2). If either disk-full hold or
    // pg-outage hold is active, return 0 without flushing buffers, saving PG state, or
    // committing offsets. Records remain in buffers; the controller's retry loop probes for
    // recovery every 30s and flips hold off via onRecovery / recordPgSuccess.
    if ((diskHold != null && diskHold.isHoldActive())
        || (pgHold != null && pgHold.isHoldActive())) {
      return 0;
    }

    // Step 1: Extract all buffered lines (in-memory, no I/O yet)
    List<FlushResult> results = buffers.flushAll();

    if (results.isEmpty()) {
      return 0;
    }
```

- [ ] **Step 1.4: Update existing `OffsetCommitCoordinatorTest.java` constructor call sites**

Open `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorTest.java`. Three places construct `new OffsetCommitCoordinator(...)` (lines 37, 57, 78). Each currently ends with `metrics, Clocks.systemNanoClock()` followed by a `)`. Append two `null` args before the closing paren so the call passes the new params.

Find each occurrence of:

```java
        new OffsetCommitCoordinator(
            null, null, null, null, null, metrics, Clocks.systemNanoClock());
```

Replace each occurrence (use `replace_all` since the line is identical across the three sites) with:

```java
        new OffsetCommitCoordinator(
            null, null, null, null, null, metrics, Clocks.systemNanoClock(), null, null);
```

(For the *first* test in that file — `flushAndCommit_emptyBuffers_returnsZero_noConsumerAccess` — the signature spans multiple lines. If the literal `replace_all` doesn't match because of line-break differences, use the `Edit` tool three times instead, once per call site, looking for the unique surrounding context.)

The two `null` controllers are safe here because the early-return gate uses `!= null` checks; a null controller is treated as "no hold." This preserves these legacy unit tests' contracts (they exercise empty-buffer / cache-only paths that don't depend on hold semantics).

- [ ] **Step 1.5: Run the integration test, confirm it passes**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: 1 test passes. The pre-active disk hold causes `flushAndCommit` to return 0; `verify(consumer, never()).commitSync(...)` and `verify(appender, never()).appendAndFsync(...)` succeed.

- [ ] **Step 1.6: Run the existing OffsetCommitCoordinator tests, confirm they still pass**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinator*" -i
```

Expected: all 4 tests pass (3 legacy + 1 new integration).

- [ ] **Step 1.7: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java \
        writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorTest.java \
        writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java
git commit -m "$(cat <<'EOF'
feat(writer): wire hold-controller early-gate into flushAndCommit

Adds DiskFullHoldController and PgOutageHoldController as constructor
parameters of OffsetCommitCoordinator, and short-circuits flushAndCommit
with return 0 when either hold is active. Records remain in buffers
during hold; the controllers' retry loops drive recovery via probes.

Updates the three existing OffsetCommitCoordinatorTest call sites to
pass null controllers (legacy empty-buffer tests don't exercise hold
semantics; null is treated as "no hold" via != null checks at the gate).

Adds OffsetCommitCoordinatorHoldIntegrationTest with the first of five
integration cases: pre-active disk hold causes flushAndCommit to
early-return 0 without touching any collaborator.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: IOException catch wiring (tests 1 + 2)

**Goal:** Route ENOSPC IOExceptions from `appendAndFsync` to `diskHold.onWriteError`; short-circuit when ENOSPC; preserve today's UncheckedIOException rethrow for non-ENOSPC.

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`
- Modify: `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java`

- [ ] **Step 2.1: Add tests 1 and 2 to the integration test file (FAILING)**

Use the `Edit` tool. Find this exact block at the end of `OffsetCommitCoordinatorHoldIntegrationTest.java`:

```java
    assertThat(n).isEqualTo(0);
    // No collaborators touched — early-return happened before flushAll().
    verify(consumer, never()).commitSync(any(Map.class));
    try {
      verify(appender, never()).appendAndFsync(any(), any());
    } catch (IOException unreachable) {
      throw new AssertionError(unreachable);
    }
  }
}
```

Replace with (adds tests 1 and 2; replaces the closing `}` of the class):

```java
    assertThat(n).isEqualTo(0);
    // No collaborators touched — early-return happened before flushAll().
    verify(consumer, never()).commitSync(any(Map.class));
    try {
      verify(appender, never()).appendAndFsync(any(), any());
    } catch (IOException unreachable) {
      throw new AssertionError(unreachable);
    }
  }

  /**
   * Test 1: ENOSPC IOException on appendAndFsync triggers diskHold.onWriteError, hold becomes
   * active, flushAndCommit returns 0 cleanly (no rethrow), no commitSync call.
   */
  @Test
  void flushAndCommit_returnsZero_whenDiskHoldEntered_byEnospcIoException() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    // BufferManager with one buffered record so flushAll() returns a non-empty result.
    BufferManager buffers = new BufferManager("/tmp/test-hold-task2-1", 100, 60, codec);
    enqueueOneBufferedRecord(buffers, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    // Compressor returns a small byte[] so the for-loop reaches appendAndFsync.
    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1, 2, 3});
    // Appender throws ENOSPC on the first (and only) call.
    org.mockito.Mockito.doThrow(new IOException("No space left on device"))
        .when(appender)
        .appendAndFsync(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    int n = coord.flushAndCommit(buffers);

    assertThat(n).isEqualTo(0);
    assertThat(diskHold.isHoldActive()).isTrue();
    verify(appender).appendAndFsync(any(), any()); // called exactly once
    verify(consumer, never()).commitSync(any(Map.class));
    verify(stateManager, never()).saveStatesAndCheckpoints(any(), any());
  }

  /**
   * Test 2: non-ENOSPC IOException on appendAndFsync still throws UncheckedIOException
   * (preserves today's behavior). Disk hold is NOT entered.
   */
  @Test
  void flushAndCommit_rethrowsUncheckedIO_whenIoExceptionIsNotEnospc() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task2-2", 100, 60, codec);
    enqueueOneBufferedRecord(buffers, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1});
    org.mockito.Mockito.doThrow(new IOException("Permission denied"))
        .when(appender)
        .appendAndFsync(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(java.io.UncheckedIOException.class)
        .hasMessageContaining("File write failed");

    assertThat(diskHold.isHoldActive()).isFalse();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a
   * non-empty FlushResult. Uses the writer's existing buffer API.
   */
  private static void enqueueOneBufferedRecord(BufferManager buffers, EnvelopeCodec codec) {
    com.cryptolake.writer.StreamKey key =
        new com.cryptolake.writer.StreamKey(EXCHANGE, "btcusdt", "trades");
    com.cryptolake.common.envelope.Envelope env =
        com.cryptolake.common.envelope.Envelope.create(
            EXCHANGE,
            "btcusdt",
            "trades",
            "binance-collector-01_2026-05-05T14:00:00Z",
            1L,
            1_000_000L,
            1_000_001L,
            "{}",
            "deadbeef",
            null,
            null);
    byte[] line = codec.encodeLine(env);
    com.cryptolake.writer.buffer.FileTarget target =
        new com.cryptolake.writer.buffer.FileTarget(EXCHANGE, "btcusdt", "trades", "2026-05-05", 14);
    buffers.append(
        key,
        target,
        line,
        new com.cryptolake.writer.buffer.CheckpointMeta(
            key, 1_000_000L, "binance-collector-01_2026-05-05T14:00:00Z"),
        0,
        0L);
  }
}
```

Notes for the implementer:
- The `enqueueOneBufferedRecord` helper builds one `Envelope`, encodes it, and appends to the buffer at `(symbol=btcusdt, stream=trades, hour=14)`. Field names and types follow `Envelope.create` and `BufferManager.append` as defined in the codebase. If signatures have drifted (e.g., constructor args added), adjust the helper to match — the principle (one record buffered, ready to flush) is what matters.
- `org.mockito.Mockito.doThrow(...).when(appender).appendAndFsync(any(), any())` configures the mock to throw on the first call. The test asserts the throw was observed exactly once (`verify(appender).appendAndFsync(any(), any())`).

- [ ] **Step 2.2: Run, confirm tests 1 & 2 fail**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: tests `flushAndCommit_returnsZero_whenDiskHoldEntered_byEnospcIoException` and `flushAndCommit_rethrowsUncheckedIO_whenIoExceptionIsNotEnospc` both FAIL. Test 1 fails because the current catch wraps and rethrows UncheckedIOException without ever calling `onWriteError` — the test expects `n==0` and `diskHold.isHoldActive()==true`, both of which require the new wiring. Test 2 currently *passes* by accident (the throw is the right behavior for non-ENOSPC), but its `assertThat(diskHold.isHoldActive()).isFalse()` already passes because the controller isn't called at all today. **Re-check:** if test 2 passes despite the missing wiring, that's fine — it documents the no-regression contract for non-ENOSPC.

- [ ] **Step 2.3: Update the IOException catch in `OffsetCommitCoordinator.flushAndCommit`**

Open `OffsetCommitCoordinator.java`. Find this exact block (around lines 128–136):

```java
      byte[] compressed = compressor.compressFrame(r.lines());
      try {
        appender.appendAndFsync(r.filePath(), compressed); // force(true) inside (Tier 5 I3)
      } catch (IOException e) {
        // Increment write_errors metric here; caller may emit write_error gap
        metrics
            .writeErrors(r.target().exchange(), r.target().symbol(), r.target().stream())
            .increment();
        throw new UncheckedIOException("File write failed for " + r.filePath(), e);
      }
```

Replace with:

```java
      byte[] compressed = compressor.compressFrame(r.lines());
      try {
        appender.appendAndFsync(r.filePath(), compressed); // force(true) inside (Tier 5 I3)
      } catch (IOException e) {
        // Increment write_errors metric here; caller may emit write_error gap
        metrics
            .writeErrors(r.target().exchange(), r.target().symbol(), r.target().stream())
            .increment();
        // Route to disk-full hold controller. If this is ENOSPC, the controller flips its
        // hold state to active and emits LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED. We then
        // short-circuit the rest of flushAndCommit (no PG save, no Kafka commit) so the
        // records stay in Kafka and replay on recovery. For non-ENOSPC IOExceptions, the
        // controller is a no-op (isEnospc returns false), and we rethrow as today.
        if (diskHold != null) {
          diskHold.onWriteError(e);
          if (diskHold.isHoldActive()) {
            return 0;
          }
        }
        throw new UncheckedIOException("File write failed for " + r.filePath(), e);
      }
```

- [ ] **Step 2.4: Run, confirm tests 1 & 2 pass**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: all 3 integration tests pass.

- [ ] **Step 2.5: Run all writer tests, confirm no regression**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 2.6: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java \
        writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java
git commit -m "$(cat <<'EOF'
feat(writer): route ENOSPC IOException to DiskFullHoldController

In OffsetCommitCoordinator.flushAndCommit, the appendAndFsync catch
block now calls diskHold.onWriteError(e). If the controller flips
hold active (ENOSPC detected), flushAndCommit returns 0 cleanly so
records remain in Kafka and replay after recovery. Non-ENOSPC
IOExceptions still rethrow as UncheckedIOException — today's
behavior is preserved.

Adds two integration tests: ENOSPC entry triggers hold + clean
return, and non-ENOSPC propagation does NOT trigger hold.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: PG-save catch wiring + threshold short-circuit (test 5)

**Goal:** Route `CryptoLakeStateException` from `saveStatesAndCheckpoints` to `pgHold.recordPgFailure`; short-circuit cleanly only when the threshold flips hold active; preserve today's "throw on first/second failure" behavior below threshold.

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`
- Modify: `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java`

- [ ] **Step 3.1: Add test 5 to the integration test file (FAILING)**

Use the `Edit` tool. Find this line at the end of the test class (just before the closing `}` of the class):

```java
  private static void enqueueOneBufferedRecord(BufferManager buffers, EnvelopeCodec codec) {
```

Insert the new test BEFORE that helper method. Find this exact block:

```java
    assertThat(diskHold.isHoldActive()).isFalse();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a
```

Replace with:

```java
    assertThat(diskHold.isHoldActive()).isFalse();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Test 5: three consecutive PG failures → recordPgFailure on each → threshold reached →
   * pgHold.isHoldActive flips true → third call returns 0 cleanly (no exception). Calls 1 and
   * 2 still throw — today's "fail fast on first PG failure" behavior is preserved below the
   * threshold.
   */
  @Test
  void flushAndCommit_returnsZero_whenPgHoldEntered_afterThreeConsecutiveFailures()
      throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task3", 100, 60, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1});
    // Appender succeeds — PG is the failure point.
    // saveStatesAndCheckpoints throws on every call.
    org.mockito.Mockito.doThrow(new com.cryptolake.writer.state.CryptoLakeStateException("pg down"))
        .when(stateManager)
        .saveStatesAndCheckpoints(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    // Buffer a fresh record before each call so flushAll() returns non-empty.
    enqueueOneBufferedRecord(buffers, codec);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(com.cryptolake.writer.state.CryptoLakeStateException.class);

    enqueueOneBufferedRecord(buffers, codec);
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> coord.flushAndCommit(buffers))
        .isInstanceOf(com.cryptolake.writer.state.CryptoLakeStateException.class);

    enqueueOneBufferedRecord(buffers, codec);
    int n = coord.flushAndCommit(buffers);

    assertThat(n).isEqualTo(0);
    assertThat(pgHold.isHoldActive()).isTrue();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a
```

- [ ] **Step 3.2: Run, confirm test 5 fails**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: `flushAndCommit_returnsZero_whenPgHoldEntered_afterThreeConsecutiveFailures` FAILS — the third call still throws because nothing routes the `CryptoLakeStateException` to `pgHold.recordPgFailure`.

- [ ] **Step 3.3: Update the PG-save catch in `OffsetCommitCoordinator.flushAndCommit`**

Open `OffsetCommitCoordinator.java`. Find this exact block (around lines 207–214):

```java
    // Step 4: PG save — atomic, retry 3× (Tier 5 G3). On failure: metric + throw, NO commit
    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      log.error("pg_commit_failed", e, "error", e.getMessage());
      throw e; // NO Kafka commit (Tier 5 C8 watch-out)
    }
```

Replace with:

```java
    // Step 4: PG save — atomic, retry 3× (Tier 5 G3). On failure: metric + threshold-tracked
    // hold via pgHold; below threshold the call still throws (preserves today's fail-fast on
    // first PG failure); at the threshold pgHold.isHoldActive flips true and we return 0
    // cleanly so the consume loop continues without crashing.
    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      log.error("pg_commit_failed", e, "error", e.getMessage());
      if (pgHold != null) {
        pgHold.recordPgFailure();
        if (pgHold.isHoldActive()) {
          return 0;
        }
      }
      throw e; // <-threshold: NO Kafka commit; preserves today's behavior (Tier 5 C8 watch-out)
    }
```

- [ ] **Step 3.4: Run, confirm test 5 passes**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: all 4 integration tests pass.

- [ ] **Step 3.5: Run all writer tests, confirm no regression**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 3.6: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java \
        writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java
git commit -m "$(cat <<'EOF'
feat(writer): route PG outage to PgOutageHoldController threshold

In OffsetCommitCoordinator.flushAndCommit, the saveStatesAndCheckpoints
catch block now calls pgHold.recordPgFailure(). Below the controller's
threshold (3 consecutive failures), the exception still propagates —
today's fail-fast behavior on transient PG hiccups is preserved. At the
threshold the call returns 0 cleanly so the consume loop continues.

Adds the integration test for the threshold flow.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: PG-success path + recovery test (test 4)

**Goal:** Call `pgHold.recordPgSuccess()` after a successful `saveStatesAndCheckpoints` so the failure counter resets and active holds exit cleanly. Validate end-to-end recovery on the disk-full path: enter hold → manual `onRecovery()` → next flush succeeds → `commitSync` advances offsets.

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`
- Modify: `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java`

- [ ] **Step 4.1: Add test 4 to the integration test file (FAILING)**

Find this block in `OffsetCommitCoordinatorHoldIntegrationTest.java` (just after test 5, before the helper):

```java
    assertThat(n).isEqualTo(0);
    assertThat(pgHold.isHoldActive()).isTrue();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a
```

Replace with:

```java
    assertThat(n).isEqualTo(0);
    assertThat(pgHold.isHoldActive()).isTrue();
    verify(consumer, never()).commitSync(any(Map.class));
  }

  /**
   * Test 4: enter disk hold via ENOSPC; simulate disk recovery via manual onRecovery() (the
   * controller's retry-loop probe is the production path, but bypassing it makes the test
   * deterministic and fast); next flushAndCommit succeeds and commitSync advances offsets.
   */
  @Test
  void flushAndCommit_recoversAfterDiskHoldExits_andCommitsAccumulatedOffsets() throws Exception {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    EnvelopeCodec codec = new EnvelopeCodec(EnvelopeCodec.newMapper());
    BufferManager buffers = new BufferManager("/tmp/test-hold-task4", 100, 60, codec);

    @SuppressWarnings("unchecked")
    KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
    DurableAppender appender = mock(DurableAppender.class);
    ZstdFrameCompressor compressor = mock(ZstdFrameCompressor.class);
    StateManager stateManager = mock(StateManager.class);
    SealedFileIndex sealedIndex = mock(SealedFileIndex.class);

    org.mockito.Mockito.when(compressor.compressFrame(any())).thenReturn(new byte[] {0, 1});

    // First call: appendAndFsync throws ENOSPC. Subsequent calls: succeed.
    java.util.concurrent.atomic.AtomicInteger appendCalls = new java.util.concurrent.atomic.AtomicInteger();
    org.mockito.Mockito.doAnswer(
            inv -> {
              if (appendCalls.getAndIncrement() == 0) {
                throw new IOException("No space left on device");
              }
              return null; // success
            })
        .when(appender)
        .appendAndFsync(any(), any());

    DiskFullHoldController diskHold = newDiskHoldNeverFreeing();
    PgOutageHoldController pgHold = newPgHoldNeverRecovering();

    OffsetCommitCoordinator coord =
        new OffsetCommitCoordinator(
            consumer,
            appender,
            compressor,
            stateManager,
            sealedIndex,
            metrics,
            Clocks.systemNanoClock(),
            diskHold,
            pgHold);

    // First flush: enters hold, returns 0.
    enqueueOneBufferedRecord(buffers, codec);
    int n1 = coord.flushAndCommit(buffers);
    assertThat(n1).isEqualTo(0);
    assertThat(diskHold.isHoldActive()).isTrue();

    // Simulate probe success — production path is the controller's retry loop, but we drive
    // it manually for determinism.
    diskHold.onRecovery();
    assertThat(diskHold.isHoldActive()).isFalse();

    // Second flush: appendAndFsync now succeeds; PG save succeeds; commitSync runs.
    enqueueOneBufferedRecord(buffers, codec);
    int n2 = coord.flushAndCommit(buffers);
    assertThat(n2).isGreaterThan(0);
    verify(consumer).commitSync(any(Map.class));
  }

  /**
   * Helper: enqueue one buffered record into the BufferManager so flushAll() returns a
```

- [ ] **Step 4.2: Run, confirm test 4 fails (or passes — depends on whether `recordPgSuccess` is needed for this path)**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: This test ONLY uses the disk-full path; `recordPgSuccess` is not needed for it to pass. So it likely already passes after Task 2's wiring. **If it passes:** still proceed to Step 4.3 — adding `recordPgSuccess` is required by the spec for the symmetric PG recovery path (counter reset on success), even though we don't have a dedicated integration test for it (the production behavior is exercised end-to-end by chaos scenario 07). Steps 4.3–4.6 are unconditional.

- [ ] **Step 4.3: Add `pgHold.recordPgSuccess()` on the PG-save success path**

Open `OffsetCommitCoordinator.java`. Find the block we just modified in Task 3:

```java
    // Step 4: PG save — atomic, retry 3× (Tier 5 G3). On failure: metric + threshold-tracked
    // hold via pgHold; below threshold the call still throws (preserves today's fail-fast on
    // first PG failure); at the threshold pgHold.isHoldActive flips true and we return 0
    // cleanly so the consume loop continues without crashing.
    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      log.error("pg_commit_failed", e, "error", e.getMessage());
      if (pgHold != null) {
        pgHold.recordPgFailure();
        if (pgHold.isHoldActive()) {
          return 0;
        }
      }
      throw e; // <-threshold: NO Kafka commit; preserves today's behavior (Tier 5 C8 watch-out)
    }
```

Replace with:

```java
    // Step 4: PG save — atomic, retry 3× (Tier 5 G3). On failure: metric + threshold-tracked
    // hold via pgHold; below threshold the call still throws (preserves today's fail-fast on
    // first PG failure); at the threshold pgHold.isHoldActive flips true and we return 0
    // cleanly so the consume loop continues without crashing.
    try {
      stateManager.saveStatesAndCheckpoints(states, checkpoints);
      if (pgHold != null) {
        // Resets the consecutive-failure counter and exits hold if it was active. Idempotent
        // when the counter is already 0.
        pgHold.recordPgSuccess();
      }
    } catch (CryptoLakeStateException e) {
      metrics.pgCommitFailures().increment();
      log.error("pg_commit_failed", e, "error", e.getMessage());
      if (pgHold != null) {
        pgHold.recordPgFailure();
        if (pgHold.isHoldActive()) {
          return 0;
        }
      }
      throw e; // <-threshold: NO Kafka commit; preserves today's behavior (Tier 5 C8 watch-out)
    }
```

- [ ] **Step 4.4: Run, confirm all integration tests pass**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" -i
```

Expected: all 5 integration tests pass.

- [ ] **Step 4.5: Run all writer tests, confirm no regression**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 4.6: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java \
        writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java
git commit -m "$(cat <<'EOF'
feat(writer): pgHold.recordPgSuccess on PG-save success; recovery test

Calls pgHold.recordPgSuccess() after every successful saveStatesAndCheckpoints.
This resets the consecutive-failure counter and exits PG hold if active.
Idempotent when the counter is already 0.

Adds the disk-full recovery integration test: enter hold via ENOSPC,
simulate probe success via onRecovery, next flush succeeds and
commitSync advances offsets.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Wire controllers into `Main.java`

**Goal:** Construct both controllers in `Main.java` with their probes and (symbol, stream) lists; pass them to `OffsetCommitCoordinator`; start both before the consume loop spawns; stop both during the SIGTERM shutdown hook after the consume-loop drains.

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/Main.java`

- [ ] **Step 5.1: Identify the insertion points in `Main.java`**

Run:

```bash
grep -nE "OffsetCommitCoordinator committer =|new OffsetCommitCoordinator\(|gapEmitter = new GapEmitter|consumerLoop.requestShutdown|shutdownLatch.await" \
  /Users/vasyl.vaskovskyi/data/cryptolake/writer/src/main/java/com/cryptolake/writer/Main.java
```

Expected output (line numbers may shift; use grep to relocate before editing):

- `OffsetCommitCoordinator committer = ...` around line 254 — this construction needs to be moved AFTER the controllers are built, OR the controllers are built before this and passed in. We'll do the latter: build controllers earlier, pass into existing committer construction.
- `gapEmitter = new GapEmitter(...)` around line 265 — controllers need `gapEmitter`, so they must be constructed AFTER this line.
- `consumerLoop.requestShutdown()` in shutdown hook around line 359 — `controllers.stop()` belongs in this block, AFTER `shutdownLatch.await()` but before `healthServer.stop()`.

So we have three change regions:
1. After `gapEmitter = new GapEmitter(...)` and before the existing `OffsetCommitCoordinator committer = ...`: construct both controllers.
2. The existing `new OffsetCommitCoordinator(...)` call needs two more args.
3. In the shutdown hook, after `shutdownLatch.await(...)` returns: `diskHold.stop(); pgHold.stop();`.
4. After construction (before the consume loop spawns): `diskHold.start(); pgHold.start();` so retry loops are running.

- [ ] **Step 5.2: Add imports to `Main.java`**

The Main class currently does not import the controllers. Find the import block at the top of `Main.java`. Add these two import lines (in alphabetical order with the existing `com.cryptolake.writer.durability.*` imports):

```java
import com.cryptolake.writer.durability.DiskFullHoldController;
import com.cryptolake.writer.durability.PgOutageHoldController;
```

Use the `Edit` tool. Locate any existing `import com.cryptolake.writer.durability.` line in `Main.java` and add the two new imports adjacent to it. If no `durability` import exists yet, place the two new lines alphabetically among the `com.cryptolake.writer.*` imports.

Also ensure `java.nio.file.Files`, `java.nio.file.Path`, `java.util.function.BooleanSupplier`, `java.util.List` are imported. They likely already are; if any are missing the compile error in Step 5.6 will surface them. (`java.util.List` is the most likely to already be present; `BooleanSupplier` may need to be added.)

- [ ] **Step 5.3: Insert controller construction and pass into `OffsetCommitCoordinator`**

Find this exact block in `Main.java` (around lines 252–262):

```java
      // ── OffsetCommitCoordinator ───────────────────────────────────────────────────────────────
      OffsetCommitCoordinator committer =
          new OffsetCommitCoordinator(
              primaryConsumer,
              appender,
              compressor,
              stateManager,
              sealedIndex,
              metrics,
              Clocks.systemNanoClock());
```

Replace with (NOTE: `gapEmitter` is constructed slightly LATER in the current file order, around line 265; the controllers depend on `gapEmitter`, so we must move the committer construction below `gapEmitter`'s creation. This single replacement reorders the wiring):

```java
      // ── GapEmitter (now we have coverage and buffers) ─────────────────────────────────────────
      gapEmitter = new GapEmitter(buffers, metrics, null, coverage);

      // ── (symbol, stream) lists for hold controllers ───────────────────────────────────────────
      // Build the same list shape both controllers need. Sourced from the writer's enabled
      // symbols × streams (config). One pair per (symbol, stream) the writer is responsible for.
      var binance = config.exchanges().binance();
      java.util.List<String> enabledStreamsForHolds =
          binance.writerStreamsOverride() != null
              ? binance.writerStreamsOverride()
              : binance.getEnabledStreams();
      java.util.List<DiskFullHoldController.SymbolStream> diskSymbolStreams =
          new java.util.ArrayList<>();
      java.util.List<PgOutageHoldController.SymbolStream> pgSymbolStreams =
          new java.util.ArrayList<>();
      for (String sym : binance.symbols()) {
        for (String stream : enabledStreamsForHolds) {
          diskSymbolStreams.add(new DiskFullHoldController.SymbolStream(sym, stream));
          pgSymbolStreams.add(new PgOutageHoldController.SymbolStream(sym, stream));
        }
      }

      // ── DiskFullHoldController ────────────────────────────────────────────────────────────────
      // Probe: usable bytes on the baseDir filesystem > 50 MiB. Conservative: stay in hold on
      // probe failure so we don't exit prematurely on a flaky FS.
      final long minFreeBytesForRecovery = 50L * 1024 * 1024;
      java.util.function.BooleanSupplier diskProbe =
          () -> {
            try {
              return java.nio.file.Files.getFileStore(java.nio.file.Path.of(baseDir))
                      .getUsableSpace()
                  > minFreeBytesForRecovery;
            } catch (java.io.IOException probeErr) {
              return false;
            }
          };
      DiskFullHoldController diskHold =
          DiskFullHoldController.of(
              Clocks.systemNanoClock(), diskProbe, gapEmitter, "binance", diskSymbolStreams);

      // ── PgOutageHoldController ────────────────────────────────────────────────────────────────
      // Probe: empty-list saveStatesAndCheckpoints round-trip. Cheap PG ping; throws if PG is
      // unreachable, in which case we stay in hold (conservative).
      java.util.function.BooleanSupplier pgProbe =
          () -> {
            try {
              stateManager.saveStatesAndCheckpoints(java.util.List.of(), java.util.List.of());
              return true;
            } catch (Exception probeErr) {
              return false;
            }
          };
      PgOutageHoldController pgHold =
          PgOutageHoldController.of(
              Clocks.systemNanoClock(), pgProbe, gapEmitter, "binance", pgSymbolStreams);

      // ── OffsetCommitCoordinator ───────────────────────────────────────────────────────────────
      OffsetCommitCoordinator committer =
          new OffsetCommitCoordinator(
              primaryConsumer,
              appender,
              compressor,
              stateManager,
              sealedIndex,
              metrics,
              Clocks.systemNanoClock(),
              diskHold,
              pgHold);
```

This block subsumes the original `gapEmitter = new GapEmitter(...)` line and the original `OffsetCommitCoordinator committer = new OffsetCommitCoordinator(...)` block. The original `gapEmitter` line and the original `committer` construction must be removed by the same replacement to avoid duplication. After the replacement, search the file for any leftover duplicates:

```bash
grep -n "gapEmitter = new GapEmitter\|new OffsetCommitCoordinator(" \
  /Users/vasyl.vaskovskyi/data/cryptolake/writer/src/main/java/com/cryptolake/writer/Main.java
```

Expected: exactly one occurrence of each. If there are duplicates, use `Edit` to remove the old ones.

- [ ] **Step 5.4: Start the controllers before the consume loop spawns**

Find this exact block in `Main.java` (around line 380):

```java
      // ── Start consume loop on virtual thread (Tier 5 A2; design §3.1 T1) ────────────────────
      try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
        var future =
            executor.submit(
                () -> {
                  try {
                    consumerLoop.run();
                  } finally {
                    shutdownLatch.countDown();
                  }
                });
```

Replace with:

```java
      // ── Start hold-controller retry loops ────────────────────────────────────────────────────
      // Each starts a virtual-thread retry loop that probes the underlying resource every 30s
      // and flips its hold state off via onRecovery / recordPgSuccess once recovered.
      diskHold.start();
      pgHold.start();

      // ── Start consume loop on virtual thread (Tier 5 A2; design §3.1 T1) ────────────────────
      try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
        var future =
            executor.submit(
                () -> {
                  try {
                    consumerLoop.run();
                  } finally {
                    shutdownLatch.countDown();
                  }
                });
```

- [ ] **Step 5.5: Stop the controllers in the shutdown hook**

Find this exact block in `Main.java` (around lines 358–378):

```java
                  () -> {
                    log.info("shutdown_hook_triggered");
                    consumerLoop.requestShutdown();
                    try {
                      shutdownLatch.await(
                          java.util.concurrent.TimeUnit.SECONDS.toMillis(35),
                          java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    try {
                      healthServer.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown; never block main shutdown path
                    }
                    try {
                      stateManager.markComponentCleanShutdown("writer", instanceId);
                      stateManager.close();
                    } catch (Exception ignored) {
                      // best-effort shutdown; never block main shutdown path
                    }
                  }));
```

Replace with:

```java
                  () -> {
                    log.info("shutdown_hook_triggered");
                    consumerLoop.requestShutdown();
                    try {
                      shutdownLatch.await(
                          java.util.concurrent.TimeUnit.SECONDS.toMillis(35),
                          java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                    // Stop hold-controller retry loops AFTER the consume loop has drained,
                    // to avoid racing the probes against a closing PG / FS.
                    try {
                      diskHold.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown
                    }
                    try {
                      pgHold.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown
                    }
                    try {
                      healthServer.stop();
                    } catch (Exception ignored) {
                      // best-effort shutdown; never block main shutdown path
                    }
                    try {
                      stateManager.markComponentCleanShutdown("writer", instanceId);
                      stateManager.close();
                    } catch (Exception ignored) {
                      // best-effort shutdown; never block main shutdown path
                    }
                  }));
```

- [ ] **Step 5.6: Compile + format check**

```bash
./gradlew :writer:installDist :writer:spotlessCheck -i
```

Expected: BUILD SUCCESSFUL. If `spotlessCheck` fails, run:

```bash
./gradlew :writer:spotlessApply
```

then re-run `:writer:installDist`.

Common compile errors and fixes:
- `cannot find symbol: BooleanSupplier` → ensure the lambda uses fully qualified `java.util.function.BooleanSupplier` (already qualified in the snippet above; if you reformatted with imports it should still work).
- `cannot find symbol: variable baseDir` → the `baseDir` local should already be in scope at the construction point; if not, search backwards for its declaration (it's used by `FileRotator` construction earlier in the same method).
- `gapEmitter cannot be resolved before assignment` → make sure the original `GapEmitter gapEmitter; // declared here, constructed below` declaration is still present BEFORE the construction block and the duplicate has been removed.

- [ ] **Step 5.7: Run the full writer test suite**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL. The 5 new integration tests pass; the 3 legacy `OffsetCommitCoordinatorTest` cases still pass; everything else unchanged.

- [ ] **Step 5.8: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/Main.java
git commit -m "$(cat <<'EOF'
feat(writer): construct and start hold controllers in Main wiring

Constructs DiskFullHoldController and PgOutageHoldController in
Main.java alongside the other long-lived components, with:
- disk probe = Files.getFileStore(baseDir).getUsableSpace() > 50 MiB
- pg probe = empty-list saveStatesAndCheckpoints round-trip
- (symbol, stream) lists derived from binance.symbols() x enabled streams
- factory wiring through GapEmitter so hold-window gap envelopes
  flow through the standard archive path

Both controllers are passed into OffsetCommitCoordinator via the new
constructor params. start() is called before the consume loop spawns;
stop() is called in the SIGTERM shutdown hook after the consume loop
drains, before healthServer.stop() and stateManager.close().

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Build gates (verification only)

**Goal:** Confirm the entire writer module passes all CI gates with the wiring in place.

**Files:** none (verification-only).

- [ ] **Step 6.1: Full module build**

```bash
./gradlew :writer:build -i
```

Expected: BUILD SUCCESSFUL. This runs `:writer:compileJava`, `:writer:spotlessCheck`, `:writer:test`, and `:writer:installDist`.

- [ ] **Step 6.2: Confirm all 5 new integration tests are present and pass**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinatorHoldIntegrationTest*" --info \
  | grep -E "PASSED|FAILED|test\(\)"
```

Expected output includes 5 PASSED lines for the methods:
- `flushAndCommit_returnsZero_immediately_whenDiskHoldAlreadyActive`
- `flushAndCommit_returnsZero_whenDiskHoldEntered_byEnospcIoException`
- `flushAndCommit_rethrowsUncheckedIO_whenIoExceptionIsNotEnospc`
- `flushAndCommit_recoversAfterDiskHoldExits_andCommitsAccumulatedOffsets`
- `flushAndCommit_returnsZero_whenPgHoldEntered_afterThreeConsecutiveFailures`

- [ ] **Step 6.3: Confirm OffsetCommitCoordinatorTest legacy tests still pass**

```bash
./gradlew :writer:test --tests "OffsetCommitCoordinatorTest" -i \
  | grep -E "PASSED|FAILED|test\(\)"
```

Expected output: 3 PASSED lines for the legacy tests.

- [ ] **Step 6.4: No commit**

This task is verification-only. If any gate fails, return to the relevant earlier task and fix; do not paper over with a no-op commit here.

---

## Self-Review Checklist (run before handoff)

- **Spec coverage:**
  - [x] Goal 1 (DiskFullHoldController engages on ENOSPC, pauses commits, exits on recovery): Tasks 1–4 + Task 5 wiring.
  - [x] Goal 2 (PgOutageHoldController engages after 3 failures, pauses commits, exits on recovery): Task 3 (entry) + Task 4 (success path).
  - [x] Goal 3 (chaos scenarios 02 and 07 exercise real production paths): all-of, validated end-to-end via the chaos branches.
  - [x] Goal 4 (preserve non-ENOSPC IOException + below-threshold PG behavior): Task 2 step 2.3 (non-ENOSPC rethrow); Task 3 step 3.3 (sub-threshold throw).
  - [x] Five integration tests (entry, non-trigger, steady-state, recovery, PG threshold): Task 1 step 1.1 (test 3); Task 2 step 2.1 (tests 1, 2); Task 3 step 3.1 (test 5); Task 4 step 4.1 (test 4).
  - [x] OffsetCommitCoordinatorTest constructor update: Task 1 step 1.4.
  - [x] Disk probe = `Files.getFileStore(baseDir).getUsableSpace() > 50 MiB`: Task 5 step 5.3.
  - [x] PG probe = empty-list `saveStatesAndCheckpoints`: Task 5 step 5.3 (StateManager has no `ping()` method; fallback is the documented choice).
  - [x] Symbol/stream list from `binance.symbols()` × enabled streams: Task 5 step 5.3.
  - [x] `start()` before consume loop, `stop()` in shutdown hook: Task 5 steps 5.4 and 5.5.
  - [x] Build gates run: Task 6.

- **Placeholder scan:** No "TBD", "TODO", or "implement later". Each step has either complete code or a concrete shell command with expected output. The note in Task 4 step 4.2 ("test likely already passes after Task 2's wiring") is a deliberate observation, not a placeholder — Steps 4.3–4.6 are unconditional.

- **Type / name consistency:**
  - `DiskFullHoldController` / `PgOutageHoldController` used identically across all tasks.
  - `diskHold` / `pgHold` field names consistent in `OffsetCommitCoordinator` and the lambdas.
  - `DISK_SS` / `PG_SS` constants in the integration test class.
  - `enqueueOneBufferedRecord` helper used by tests 1, 2, 4, 5 with consistent signature.
  - `Clocks.systemNanoClock()`, `EnvelopeCodec.newMapper()` match the existing `OffsetCommitCoordinatorTest.java` conventions.
  - Constructor param order in `OffsetCommitCoordinator` (clock, diskHold, pgHold) consistent in all 5 integration tests.

---

## Acceptance Criteria

- `./gradlew :writer:build` passes (compile + spotless + tests + installDist).
- 5 new integration tests in `OffsetCommitCoordinatorHoldIntegrationTest` all pass.
- Legacy `OffsetCommitCoordinatorTest`'s 3 cases still pass with the updated constructor signature.
- A subsequent run of chaos scenario 02 (on the chaos branch, after this branch merges and that branch rebases on top) shows `LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED` and `LIFECYCLE WRITER_DISK_FULL_HOLD_EXITED` events bracketing the held window in `build/chaos-logs/02/writer.log`.
- No regression in any other writer test or chaos scenario.

---

## Out of scope (deliberate; will not be done in this plan)

- Wiring the controllers into `FileRotator.seal()` (the hour-rotation path). Documented in the spec as known limitation E4.
- Refactoring the two `SymbolStream` records into a shared common type.
- Introducing a `HoldController` interface.
- Modifying `tests/chaos/02_fill_disk.sh` or `tests/chaos/07_pg_kill_during_commit.sh`. Those tweaks live on the chaos branch.
- Configurable disk-recovery threshold via env. 50 MiB hard-coded for now.
