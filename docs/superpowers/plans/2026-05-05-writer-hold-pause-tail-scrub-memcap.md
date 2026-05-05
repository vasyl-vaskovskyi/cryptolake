# Writer Hold-Pause + Tail-Scrub + Memory-Cap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the writer from OOM-dying during long disk-full holds; heal torn zstd frames at startup; bound writer memory; clean up the chaosfs upstream image after each chaos teardown.

**Architecture:** When any hold is active, `KafkaConsumerLoop` calls `primary.pause(primary.assignment())` so records remain in Kafka instead of accumulating in `BufferManager`. A new `ZstdTailScrubber` runs at startup before the consume loop, finding the last valid zstd-frame boundary in each `.jsonl.zst` and truncating beyond it. `docker-compose.yml` caps the writer container at 768 MB and the JVM heap at 512 MB so any future memory regression yields a logged `OutOfMemoryError` instead of kernel SIGKILL. `tests/chaos/common.sh` removes the cached `itsthenetwork/nfs-server-alpine` image after each scenario teardown.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Mockito, zstd-jni (`com.github.luben.zstd.Zstd`), Gradle wrapper, Docker Compose v2. Spec at `docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java` | modify | Add public `boolean isAnyHoldActive()` accessor that returns the OR of both controllers' `isHoldActive()` (with `!= null` null-tolerance). |
| `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java` | modify | Add private `lastKnownHeld` field, package-private `applyHoldPauseState()` method (testability seam), and call site at the top of every `while (!stopRequested)` iteration before `primary.poll`. |
| `writer/src/main/java/com/cryptolake/writer/recovery/ZstdTailScrubber.java` | new | Public utility with one method `int scrub(Path baseDir)`. Walks `*.jsonl.zst` recursively; uses `Zstd.findFrameCompressedSize` to find the last valid frame; truncates beyond it; recomputes sidecar via `Sha256Sidecar.write` + `FilePaths.sidecarPath`. Returns count of healed files. |
| `writer/src/main/java/com/cryptolake/writer/Main.java` | modify | Insert one line invoking `ZstdTailScrubber.scrub(Path.of(baseDir))` AFTER the existing `rotator.writeMissingSidecars()` call and BEFORE the `RecoveryCoordinator` block. |
| `writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopHoldPauseTest.java` | new | Mockito test class. Tests `applyHoldPauseState()` directly (the testability seam), not the full consume loop. Four test methods covering inactive, entry-edge, steady-state-active, exit-edge. |
| `writer/src/test/java/com/cryptolake/writer/recovery/ZstdTailScrubberTest.java` | new | JUnit5 `@TempDir`-driven tests. Five test methods: clean-file unchanged, torn-tail truncated + sidecar recomputed, entirely-corrupt → 0, ignores non-zstd files, returns correct healed count. |
| `docker-compose.yml` | modify | Add `deploy.resources.limits.memory: 768M` and `JAVA_TOOL_OPTIONS=-Xmx512m -Xms256m` to writer service. |
| `tests/chaos/common.sh` | modify | In `teardown_stack`, after `rm -rf "$HOST_DATA_DIR"`, add `docker rmi itsthenetwork/nfs-server-alpine@sha256:7fa99ae65c23c5af87dd4300e543a86b119ed15ba61422444207efc7abd0ba20 2>/dev/null \|\| true`. |

`DurableAppender.java`, `DiskFullHoldController.java`, `PgOutageHoldController.java`, `BufferManager.java`, `RecordHandler.java`, `FileRotator.java`, `RecoveryCoordinator.java` are NOT modified.

---

## Task 1: Add `isAnyHoldActive()` accessor to `OffsetCommitCoordinator`

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`

- [ ] **Step 1.1: Locate the existing `flushAndCommit` early-gate**

Run:

```bash
grep -n "if ((diskHold != null && diskHold.isHoldActive())" writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java
```

Expected: one match showing the early-gate inside `flushAndCommit`. Confirm the field names are `diskHold` and `pgHold` (per the prior wiring branch).

- [ ] **Step 1.2: Add the public accessor immediately after the constructor**

Find the closing brace of the `OffsetCommitCoordinator(...)` constructor in `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java`. The closing brace ends the constructor body and is followed by the `flushAndCommit` method. Insert this exact javadoc + method between them:

```java
  /**
   * Returns {@code true} if either the disk-full hold or the PG-outage hold is currently active.
   *
   * <p>Used by {@link KafkaConsumerLoop} to decide whether to pause primary Kafka consumption.
   * When either hold is active, the consume loop pauses {@link
   * org.apache.kafka.clients.consumer.KafkaConsumer#poll} so records remain in Kafka rather than
   * accumulating in {@link com.cryptolake.writer.buffer.BufferManager}.
   *
   * <p>Null-tolerant for legacy unit tests that construct this coordinator without controllers.
   *
   * @return {@code true} iff at least one hold is active
   */
  public boolean isAnyHoldActive() {
    return (diskHold != null && diskHold.isHoldActive())
        || (pgHold != null && pgHold.isHoldActive());
  }
```

- [ ] **Step 1.3: Run all OffsetCommitCoordinator tests**

```bash
./gradlew :writer:test --tests "*OffsetCommitCoordinator*" -i
```

Expected: BUILD SUCCESSFUL. All 4 existing tests still pass (3 legacy + 1 integration). No new tests yet.

- [ ] **Step 1.4: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java
git commit -m "$(cat <<'EOF'
feat(writer): expose isAnyHoldActive accessor on OffsetCommitCoordinator

KafkaConsumerLoop will use this accessor to pause primary Kafka
consumption while either the disk-full hold or the PG-outage hold is
active. Records then remain in Kafka rather than accumulating in
BufferManager — preventing OOM during long holds.

No behavior change yet; the consumer-loop wiring lands in the next
commit.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Hold-pause in `KafkaConsumerLoop` (TDD)

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java`
- Create: `writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopHoldPauseTest.java`

- [ ] **Step 2.1: Create the test class with the FIRST test (no-pause-when-inactive)**

Create `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopHoldPauseTest.java` with this exact content:

```java
package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.cryptolake.writer.buffer.BufferManager;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.gap.GapEmitter;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link KafkaConsumerLoop#applyHoldPauseState()} — the testability seam that
 * gates Kafka consumption on hold state. We exercise the seam directly rather than driving the
 * full consume loop because the loop has many dependencies and timing-sensitive iteration.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
class KafkaConsumerLoopHoldPauseTest {

  @SuppressWarnings("unchecked")
  private KafkaConsumer<byte[], byte[]> newMockConsumer(Set<TopicPartition> assignment) {
    KafkaConsumer<byte[], byte[]> c = mock(KafkaConsumer.class);
    when(c.assignment()).thenReturn(assignment);
    return c;
  }

  private KafkaConsumerLoop newLoop(KafkaConsumer<byte[], byte[]> consumer, OffsetCommitCoordinator committer) {
    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    WriterMetrics metrics = new WriterMetrics(registry);
    return new KafkaConsumerLoop(
        consumer,
        null, // BackupTailConsumer — not exercised by this test
        List.of("binance.trades"),
        mock(RecordHandler.class),
        mock(FailoverController.class),
        committer,
        mock(RecoveryCoordinator.class),
        mock(HourRotationScheduler.class),
        mock(BufferManager.class),
        mock(CoverageFilter.class),
        mock(GapEmitter.class),
        metrics);
  }

  /** Test 1: no pause/resume calls when hold is never active. */
  @Test
  void applyHoldPauseState_doesNotPauseOrResume_whenHoldNeverActive() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(false);

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    // Drive the seam several times.
    loop.applyHoldPauseState();
    loop.applyHoldPauseState();
    loop.applyHoldPauseState();

    verify(consumer, never()).pause(any());
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }
}
```

Notes:
- The package-private `applyHoldPauseState()` and a package-private test-only getter `isLastKnownHeldForTest()` are the testability seam. We'll add both in Step 2.3.
- `newLoop` constructs a `KafkaConsumerLoop` with mocked collaborators. The test never calls `loop.run()`; it only invokes the seam directly.

- [ ] **Step 2.2: Run the test, confirm it fails to compile**

```bash
./gradlew :writer:test --tests "*KafkaConsumerLoopHoldPauseTest*" -i
```

Expected: COMPILE FAILURE — `KafkaConsumerLoop` has no method `applyHoldPauseState()` or `isLastKnownHeldForTest()`. This is the failing-test confirmation.

- [ ] **Step 2.3: Add `applyHoldPauseState()` + `lastKnownHeld` field + test-only accessor to `KafkaConsumerLoop`**

Open `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java`. Find this exact field block near the top of the class (around line 70):

```java
  /** Volatile flag set by SIGTERM hook to stop the consume loop. No synchronized needed. */
  private volatile boolean stopRequested = false;
```

Replace with (adds the new `lastKnownHeld` field with explanatory javadoc):

```java
  /** Volatile flag set by SIGTERM hook to stop the consume loop. No synchronized needed. */
  private volatile boolean stopRequested = false;

  /**
   * Tracks whether the previous {@link #applyHoldPauseState()} call observed an active hold.
   * Used to emit the LIFECYCLE pause/resume log lines on edge-changes only (not every iteration).
   * Read and written by T1 only — no synchronization needed.
   */
  private boolean lastKnownHeld = false;
```

Now add the `applyHoldPauseState()` method (and a test-only accessor for `lastKnownHeld`). Find the existing `requestShutdown()` method (around line 232):

```java
  /** Signals the consume loop to stop. Called from the SIGTERM handler (T3 — volatile write). */
  public void requestShutdown() {
    this.stopRequested = true;
    log.info("consume_loop_shutdown_requested");
  }
```

Insert the new method AFTER `requestShutdown()`'s closing brace and BEFORE `isConnected()` (search for `isConnected()` to confirm location):

```java
  /**
   * Pauses or resumes primary Kafka consumption based on the committer's hold state.
   *
   * <p>Called at the top of every consume-loop iteration BEFORE {@code primary.poll}. When any
   * hold is active, primary partitions are paused so records remain in Kafka rather than
   * accumulating in {@link com.cryptolake.writer.buffer.BufferManager} (which has no max-bytes
   * cap and would OOM during long holds).
   *
   * <p>The pause/resume is edge-triggered for log emission (single LIFECYCLE line per state
   * change), but the underlying {@code primary.pause(...)} call is repeated each iteration while
   * held to cover partition reassignment during a hold (Kafka client treats pause on already-
   * paused partitions as a no-op).
   *
   * <p>Package-private for unit-test access; production callers go through {@link #run()}.
   */
  void applyHoldPauseState() {
    boolean nowHeld = committer.isAnyHoldActive();
    if (nowHeld != lastKnownHeld) {
      try {
        if (nowHeld) {
          primary.pause(primary.assignment());
          log.info(
              "LIFECYCLE WRITER_KAFKA_CONSUMPTION_PAUSED: Hold is active — the writer is"
                  + " pausing primary Kafka consumption so records remain in Kafka rather than"
                  + " accumulating in the in-memory buffer. They will replay when the hold"
                  + " exits.");
        } else {
          primary.resume(primary.assignment());
          log.info(
              "LIFECYCLE WRITER_KAFKA_CONSUMPTION_RESUMED: Hold has cleared — the writer is"
                  + " resuming primary Kafka consumption; records that accumulated in Kafka"
                  + " during the hold will now flow through.");
        }
      } catch (Exception e) {
        // Pause/resume can throw IllegalStateException if the consumer is already closed
        // (only happens during shutdown; defensive).
        log.warn("hold_pause_state_apply_failed", "now_held", nowHeld, "error", e.getMessage());
      }
      lastKnownHeld = nowHeld;
    } else if (nowHeld) {
      // Re-pause the current assignment every iteration while held so a rebalance during
      // hold does not leak unpaused partitions.
      try {
        primary.pause(primary.assignment());
      } catch (Exception ignored) {
        // best-effort
      }
    }
  }

  /**
   * Test-only: returns the current {@link #lastKnownHeld} value. Package-private accessor exposed
   * for assertions in {@link KafkaConsumerLoopHoldPauseTest}.
   */
  boolean isLastKnownHeldForTest() {
    return lastKnownHeld;
  }
```

- [ ] **Step 2.4: Run the test, confirm it passes**

```bash
./gradlew :writer:test --tests "*KafkaConsumerLoopHoldPauseTest*" -i
```

Expected: 1 test passes (`applyHoldPauseState_doesNotPauseOrResume_whenHoldNeverActive`).

- [ ] **Step 2.5: Add the remaining three tests**

Open `KafkaConsumerLoopHoldPauseTest.java`. Find this exact closing block at the end of the test class:

```java
    verify(consumer, never()).pause(any());
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }
}
```

Replace with (adds three more tests; replaces the closing `}` of the class):

```java
    verify(consumer, never()).pause(any());
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }

  /** Test 2: pause fires on the first iteration where hold becomes active. */
  @Test
  void applyHoldPauseState_pausesPrimary_onActiveEdge() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(false, true); // first call: false, second: true

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // hold inactive → no-op
    loop.applyHoldPauseState(); // hold flipped active → pause

    verify(consumer, times(1)).pause(assignment);
    verify(consumer, never()).resume(any());
    assertThat(loop.isLastKnownHeldForTest()).isTrue();
  }

  /**
   * Test 3: hold stays active across many iterations — pause is re-issued each iteration (so
   * rebalance-mid-hold is covered) but the LIFECYCLE log line is emitted exactly once via the
   * edge-detect. We verify edge-detect via {@link KafkaConsumerLoop#isLastKnownHeldForTest()};
   * the underlying pause call may run multiple times (test asserts at-least-once).
   */
  @Test
  void applyHoldPauseState_doesNotResetEdge_whileHoldStaysActive() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(true);

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // edge: false→true; pause + log
    loop.applyHoldPauseState(); // no edge; pause again (rebalance defense), no log
    loop.applyHoldPauseState(); // no edge; pause again (rebalance defense), no log

    // Edge state is true after first call and stays true.
    assertThat(loop.isLastKnownHeldForTest()).isTrue();
    // pause is called every iteration while held (3 total here); resume never.
    verify(consumer, times(3)).pause(assignment);
    verify(consumer, never()).resume(any());
  }

  /** Test 4: resume fires on the first iteration where hold becomes inactive. */
  @Test
  void applyHoldPauseState_resumesPrimary_onInactiveEdge() {
    Set<TopicPartition> assignment = Set.of(new TopicPartition("binance.trades", 0));
    KafkaConsumer<byte[], byte[]> consumer = newMockConsumer(assignment);
    OffsetCommitCoordinator committer = mock(OffsetCommitCoordinator.class);
    when(committer.isAnyHoldActive()).thenReturn(true, false); // first: true, second: false

    KafkaConsumerLoop loop = newLoop(consumer, committer);

    loop.applyHoldPauseState(); // edge: false→true; pause
    loop.applyHoldPauseState(); // edge: true→false; resume

    verify(consumer, times(1)).resume(assignment);
    assertThat(loop.isLastKnownHeldForTest()).isFalse();
  }
}
```

- [ ] **Step 2.6: Run all 4 tests, confirm they pass**

```bash
./gradlew :writer:test --tests "*KafkaConsumerLoopHoldPauseTest*" -i
```

Expected: 4 tests pass.

- [ ] **Step 2.7: Wire `applyHoldPauseState()` into the consume loop**

Open `KafkaConsumerLoop.java`. Find this exact block (around line 132):

```java
    while (!stopRequested) {
      try {
        // Poll primary (Tier 5 A2 — blocking call on virtual thread; no run_in_executor wrapper)
        ConsumerRecords<byte[], byte[]> records = primary.poll(Duration.ofSeconds(1));
```

Replace with:

```java
    while (!stopRequested) {
      try {
        // Pause/resume primary consumption based on hold state. When either disk-full or
        // pg-outage hold is active, primary partitions are paused so records remain in Kafka
        // rather than accumulating in BufferManager (no max-bytes cap → OOM risk under long
        // holds). Backup-tail is unaffected (liveness only).
        applyHoldPauseState();

        // Poll primary (Tier 5 A2 — blocking call on virtual thread; no run_in_executor wrapper)
        ConsumerRecords<byte[], byte[]> records = primary.poll(Duration.ofSeconds(1));
```

- [ ] **Step 2.8: Run all writer tests, confirm no regression**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL. All existing + 4 new tests pass.

- [ ] **Step 2.9: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java \
        writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopHoldPauseTest.java
git commit -m "$(cat <<'EOF'
feat(writer): pause primary Kafka consumption during any hold

KafkaConsumerLoop now calls primary.pause(primary.assignment()) when
the OffsetCommitCoordinator reports any active hold (disk-full or
pg-outage). Records remain in Kafka rather than accumulating in the
unbounded BufferManager during long holds. Pause/resume edge-triggered
for LIFECYCLE log emission; pause is re-issued each iteration while
held to cover rebalance-mid-hold.

Adds KafkaConsumerLoopHoldPauseTest with four cases: no-pause-inactive,
pause-on-active-edge, no-edge-reset-while-active, resume-on-inactive-
edge. Tests exercise the seam directly via package-private methods to
avoid driving the full timing-sensitive consume loop.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Create `ZstdTailScrubber` utility class (TDD)

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/recovery/ZstdTailScrubber.java`
- Create: `writer/src/test/java/com/cryptolake/writer/recovery/ZstdTailScrubberTest.java`

- [ ] **Step 3.1: Create the test file with the FIRST test (clean file unchanged)**

Create `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/test/java/com/cryptolake/writer/recovery/ZstdTailScrubberTest.java` with this exact content:

```java
package com.cryptolake.writer.recovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.writer.io.ZstdFrameCompressor;
import com.cryptolake.writer.rotate.FilePaths;
import com.cryptolake.writer.rotate.Sha256Sidecar;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ZstdTailScrubber}.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
class ZstdTailScrubberTest {

  /** Helper: write a sequence of valid zstd frames into the file plus a fresh sidecar. */
  private static Path writeHealthyArchive(Path dir, String name, int frames) throws IOException {
    Files.createDirectories(dir);
    Path file = dir.resolve(name);
    ZstdFrameCompressor compressor = new ZstdFrameCompressor(3);
    for (int i = 0; i < frames; i++) {
      byte[] line = ("{\"i\":" + i + "}\n").getBytes();
      byte[] frame = compressor.compressFrame(List.of(line));
      Files.write(file, frame, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
    Sha256Sidecar.write(file, FilePaths.sidecarPath(file));
    return file;
  }

  /** Helper: append raw bytes (e.g., to simulate a torn-tail). */
  private static void appendRaw(Path file, byte[] bytes) throws IOException {
    Files.write(file, bytes, StandardOpenOption.APPEND);
  }

  /** Test 1: a clean file with valid frames + sidecar is left untouched. */
  @Test
  void scrub_leavesHealthyFileUnchanged(@TempDir Path tmp) throws IOException {
    Path file = writeHealthyArchive(tmp, "hour-14.jsonl.zst", 3);
    Path sidecar = FilePaths.sidecarPath(file);
    long sizeBefore = Files.size(file);
    byte[] sidecarBefore = Files.readAllBytes(sidecar);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isZero();
    assertThat(Files.size(file)).isEqualTo(sizeBefore);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarBefore);
  }
}
```

- [ ] **Step 3.2: Run the test, confirm it fails to compile**

```bash
./gradlew :writer:test --tests "*ZstdTailScrubberTest*" -i
```

Expected: COMPILE FAILURE — `ZstdTailScrubber` class does not exist. Failing-test confirmation.

- [ ] **Step 3.3: Create the `ZstdTailScrubber` class**

Create `/Users/vasyl.vaskovskyi/data/cryptolake/writer/src/main/java/com/cryptolake/writer/recovery/ZstdTailScrubber.java` with this exact content:

```java
package com.cryptolake.writer.recovery;

import com.cryptolake.common.logging.StructuredLogger;
import com.cryptolake.writer.rotate.FilePaths;
import com.cryptolake.writer.rotate.Sha256Sidecar;
import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Startup utility that heals torn zstd frame tails in {@code *.jsonl.zst} archive files.
 *
 * <p>Used to recover from the on-disk damage caused by SIGKILL/OOM/segfault crashes that
 * interrupt a write mid-frame. The truncate path inside {@link
 * com.cryptolake.writer.io.DurableAppender#appendAndFsync} only runs when the JVM lives long
 * enough to handle the IOException — kernel-level kills bypass it. This scrubber picks up the
 * pieces on the next startup, before the consume loop reads or writes anything.
 *
 * <p>Algorithm: walk {@code baseDir} recursively for {@code *.jsonl.zst} files. For each file,
 * read the bytes and iterate frames using {@link Zstd#findFrameCompressedSize(byte[], int, int)}
 * to find the last valid frame boundary. If anything follows it (torn partial frame, garbage),
 * truncate to the boundary, fsync, and recompute the sibling {@code .sha256} sidecar.
 *
 * <p>Files with a clean frame-aligned EOF are unchanged. Empty files are unchanged. Non-{@code
 * .jsonl.zst} files are ignored.
 *
 * <p>Spec: docs/superpowers/specs/2026-05-05-writer-hold-pause-tail-scrub-memcap-design.md.
 */
public final class ZstdTailScrubber {

  private static final StructuredLogger log = StructuredLogger.of(ZstdTailScrubber.class);
  private static final String ARCHIVE_SUFFIX = ".jsonl.zst";

  private ZstdTailScrubber() {}

  /**
   * Walks {@code baseDir} for archive files and heals any with torn tails. See class javadoc for
   * algorithm. Returns the number of files truncated.
   *
   * @param baseDir root archive directory; must exist
   * @return count of files whose tail was truncated
   * @throws IOException if {@code baseDir} cannot be walked
   */
  public static int scrub(Path baseDir) throws IOException {
    if (!Files.isDirectory(baseDir)) {
      return 0;
    }
    AtomicInteger healed = new AtomicInteger(0);
    try (Stream<Path> walk = Files.walk(baseDir)) {
      walk.filter(p -> p.getFileName().toString().endsWith(ARCHIVE_SUFFIX))
          .filter(Files::isRegularFile)
          .forEach(
              file -> {
                try {
                  if (scrubOne(file)) {
                    healed.incrementAndGet();
                  }
                } catch (NoSuchFileException nsfe) {
                  log.debug("scrub_file_disappeared", "path", file.toString());
                } catch (IOException e) {
                  log.warn("scrub_file_failed", "path", file.toString(), "error", e.getMessage());
                }
              });
    }
    return healed.get();
  }

  /** Heals one file. Returns true iff the file was truncated. */
  private static boolean scrubOne(Path file) throws IOException {
    long size = Files.size(file);
    if (size == 0) {
      return false;
    }
    byte[] buf = Files.readAllBytes(file);
    long lastGoodEndOffset = walkFrames(buf);
    if (lastGoodEndOffset == size) {
      return false; // healthy
    }
    // Truncate to last good frame boundary and fsync.
    try (FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE)) {
      fc.truncate(lastGoodEndOffset);
      fc.force(true);
    }
    // Recompute the sidecar so the .sha256 matches the truncated bytes.
    Path sidecar = FilePaths.sidecarPath(file);
    Sha256Sidecar.write(file, sidecar);
    log.info(
        "LIFECYCLE WRITER_STARTUP_TAIL_TRUNCATED: A previous unexpected exit left a torn zstd"
            + " frame at the tail of an archive file. The scrubber truncated to the last valid"
            + " frame boundary and recomputed the .sha256 sidecar.",
        "path",
        file.toString(),
        "bytes_dropped",
        size - lastGoodEndOffset,
        "last_good_offset",
        lastGoodEndOffset);
    return true;
  }

  /**
   * Walks zstd frames from offset 0 and returns the offset of the byte just past the end of the
   * last valid frame. If the file is entirely garbage from byte 0, returns 0.
   */
  private static long walkFrames(byte[] buf) {
    long offset = 0;
    long lastGoodEnd = 0;
    while (offset < buf.length) {
      long frameSize;
      try {
        frameSize =
            Zstd.findFrameCompressedSize(buf, (int) offset, buf.length - (int) offset);
      } catch (Throwable t) {
        // Malformed frame header or runtime error — stop walking; lastGoodEnd is the truncate
        // point.
        break;
      }
      if (frameSize <= 0 || frameSize > buf.length - offset) {
        break; // invalid size or extends past EOF (torn final frame)
      }
      offset += frameSize;
      lastGoodEnd = offset;
    }
    return lastGoodEnd;
  }
}
```

- [ ] **Step 3.4: Run the test, confirm it passes**

```bash
./gradlew :writer:test --tests "*ZstdTailScrubberTest*" -i
```

Expected: 1 test passes.

- [ ] **Step 3.5: Add the remaining four tests**

Open `ZstdTailScrubberTest.java`. Find this exact closing block at the end of the class:

```java
    assertThat(healed).isZero();
    assertThat(Files.size(file)).isEqualTo(sizeBefore);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarBefore);
  }
}
```

Replace with (adds tests 2–5):

```java
    assertThat(healed).isZero();
    assertThat(Files.size(file)).isEqualTo(sizeBefore);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarBefore);
  }

  /** Test 2: a torn tail is truncated and the sidecar is recomputed to match. */
  @Test
  void scrub_truncatesTornTail_andRecomputesSidecar(@TempDir Path tmp) throws IOException {
    Path file = writeHealthyArchive(tmp, "hour-14.jsonl.zst", 2);
    Path sidecar = FilePaths.sidecarPath(file);
    long sizeAfterTwoFrames = Files.size(file);
    byte[] sidecarAfterTwoFrames = Files.readAllBytes(sidecar);

    // Append a partial third frame (zstd magic prefix + truncated content).
    byte[] zstdMagic = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD};
    byte[] tornBytes = new byte[zstdMagic.length + 64];
    System.arraycopy(zstdMagic, 0, tornBytes, 0, zstdMagic.length);
    // Remaining 64 bytes are zeros — not a valid frame body.
    appendRaw(file, tornBytes);
    assertThat(Files.size(file)).isGreaterThan(sizeAfterTwoFrames);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(1);
    assertThat(Files.size(file)).isEqualTo(sizeAfterTwoFrames);
    // Sidecar matches the post-truncation file (which is identical to the 2-frame state).
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(sidecarAfterTwoFrames);
  }

  /** Test 3: an entirely-corrupt file (no valid zstd frames) is truncated to size 0. */
  @Test
  void scrub_truncatesEntirelyCorruptFile_toZero(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp);
    Path file = tmp.resolve("hour-14.jsonl.zst");
    byte[] garbage = new byte[200];
    for (int i = 0; i < garbage.length; i++) garbage[i] = (byte) (i * 7);
    Files.write(file, garbage);
    Sha256Sidecar.write(file, FilePaths.sidecarPath(file));
    Path sidecar = FilePaths.sidecarPath(file);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(1);
    assertThat(Files.size(file)).isZero();
    // Sidecar reflects the empty file.
    Path freshSidecar = tmp.resolve("hour-14.jsonl.zst.fresh.sha256");
    Sha256Sidecar.write(file, freshSidecar);
    assertThat(Files.readAllBytes(sidecar)).isEqualTo(Files.readAllBytes(freshSidecar));
  }

  /** Test 4: non-{@code .jsonl.zst} files are ignored. */
  @Test
  void scrub_ignoresNonZstdFiles(@TempDir Path tmp) throws IOException {
    Files.createDirectories(tmp);
    Path txt = tmp.resolve("readme.txt");
    Path tmpFile = tmp.resolve("partial.tmp");
    Files.write(txt, "hello".getBytes());
    Files.write(tmpFile, new byte[]{0x00, 0x01, 0x02});
    long txtSize = Files.size(txt);
    long tmpSize = Files.size(tmpFile);

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isZero();
    assertThat(Files.size(txt)).isEqualTo(txtSize);
    assertThat(Files.size(tmpFile)).isEqualTo(tmpSize);
  }

  /** Test 5: returned count equals the number of files actually truncated. */
  @Test
  void scrub_returnsHealedCount(@TempDir Path tmp) throws IOException {
    // 3 healthy files
    writeHealthyArchive(tmp.resolve("a"), "hour-1.jsonl.zst", 1);
    writeHealthyArchive(tmp.resolve("b"), "hour-2.jsonl.zst", 1);
    writeHealthyArchive(tmp.resolve("c"), "hour-3.jsonl.zst", 1);

    // 2 files with torn tails
    Path tornD = writeHealthyArchive(tmp.resolve("d"), "hour-4.jsonl.zst", 1);
    appendRaw(tornD, new byte[]{0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, 0x00, 0x00, 0x00});
    Path tornE = writeHealthyArchive(tmp.resolve("e"), "hour-5.jsonl.zst", 1);
    appendRaw(tornE, new byte[]{0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, 0x00, 0x00, 0x00});

    int healed = ZstdTailScrubber.scrub(tmp);

    assertThat(healed).isEqualTo(2);
  }
}
```

- [ ] **Step 3.6: Run all 5 tests, confirm they pass**

```bash
./gradlew :writer:test --tests "*ZstdTailScrubberTest*" -i
```

Expected: 5 tests pass.

- [ ] **Step 3.7: Run all writer tests, confirm no regression**

```bash
./gradlew :writer:test
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 3.8: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/recovery/ZstdTailScrubber.java \
        writer/src/test/java/com/cryptolake/writer/recovery/ZstdTailScrubberTest.java
git commit -m "$(cat <<'EOF'
feat(writer): add ZstdTailScrubber for startup tail-truncation

New utility that heals torn zstd frame tails in *.jsonl.zst archive
files at writer startup. Walks each file with
Zstd.findFrameCompressedSize to find the last valid frame boundary;
truncates beyond it; recomputes the .sha256 sidecar.

Recovers the on-disk damage caused by SIGKILL/OOM/segfault crashes
that interrupt a write mid-frame — DurableAppender's truncate path
only runs when the JVM lives long enough to handle the IOException;
kernel-level kills bypass it.

Adds ZstdTailScrubberTest with five cases: healthy unchanged, torn
truncated + sidecar recomputed, entirely-corrupt → 0, ignores non-
.jsonl.zst, returns correct healed count.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Wire `ZstdTailScrubber` into `Main.java`

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/Main.java`

- [ ] **Step 4.1: Add the import**

Open `writer/src/main/java/com/cryptolake/writer/Main.java`. Find the import block at the top. Add this import alphabetically among the `com.cryptolake.writer.recovery.*` imports (`LastEnvelopeReader`, `SealedFileIndex` are already there):

```java
import com.cryptolake.writer.recovery.ZstdTailScrubber;
```

- [ ] **Step 4.2: Insert the scrubber call after `rotator.writeMissingSidecars()`**

Find this exact block in `Main.java` (around line 326–330):

```java
      // ── Startup sidecar repair (Bug B / Tier 1 sidecar-per-archive) ──────────────────────────
      // If the writer crashed before writing sidecars, any *.jsonl.zst without a .sha256 sibling
      // will be repaired here — BEFORE the consume loop reads any messages. This covers the
      // "writer crashed before sidecar was written" path that writeMissingSidecars at shutdown
      // cannot reach (design §3.4; Tier 1 sidecar invariant).
      rotator.writeMissingSidecars();
      log.info("startup_sidecar_repair_complete");

      // ── HourRotationScheduler ────────────────────────────────────────────────────────────────
```

Replace with:

```java
      // ── Startup sidecar repair (Bug B / Tier 1 sidecar-per-archive) ──────────────────────────
      // If the writer crashed before writing sidecars, any *.jsonl.zst without a .sha256 sibling
      // will be repaired here — BEFORE the consume loop reads any messages. This covers the
      // "writer crashed before sidecar was written" path that writeMissingSidecars at shutdown
      // cannot reach (design §3.4; Tier 1 sidecar invariant).
      rotator.writeMissingSidecars();
      log.info("startup_sidecar_repair_complete");

      // ── Startup zstd-tail scrub ──────────────────────────────────────────────────────────────
      // If a previous unexpected exit (SIGKILL, OOM, segfault, host crash) left a torn zstd
      // frame at the tail of an archive file, ZstdTailScrubber finds the last valid frame
      // boundary and truncates beyond it (recomputing the sidecar). This complements
      // DurableAppender.appendAndFsync's IOException-truncate path — which only runs when the
      // JVM lives long enough to handle the exception. Kernel-level kills bypass that path;
      // this scrubber heals the leftover damage on the next startup.
      int scrubHealed = ZstdTailScrubber.scrub(java.nio.file.Path.of(baseDir));
      log.info("startup_tail_scrub_complete", "healed_files", scrubHealed);

      // ── HourRotationScheduler ────────────────────────────────────────────────────────────────
```

- [ ] **Step 4.3: Compile + run all writer tests**

```bash
./gradlew :writer:installDist :writer:test
```

Expected: BUILD SUCCESSFUL. All tests still pass.

- [ ] **Step 4.4: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/Main.java
git commit -m "$(cat <<'EOF'
feat(writer): run ZstdTailScrubber at startup

Wires the new scrubber into Main.java's startup sequence, AFTER the
existing rotator.writeMissingSidecars() and BEFORE the
RecoveryCoordinator. Heals any torn zstd-frame tails left by a prior
SIGKILL/OOM/segfault crash before the consume loop touches anything.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Add memory limit + JVM heap to `docker-compose.yml`

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 5.1: Add `deploy.resources.limits.memory` and `JAVA_TOOL_OPTIONS` to the writer service**

Open `/Users/vasyl.vaskovskyi/data/cryptolake/docker-compose.yml`. Find this exact block (around line 146–150):

```yaml
    environment:
      - LOG_LEVEL=INFO
      - HOST_DATA_DIR=${HOST_DATA_DIR:-/data}
      - DATABASE__URL=jdbc:postgresql://postgres:5432/cryptolake?user=cryptolake&password=${POSTGRES_PASSWORD:-postgres}
      - MONITORING__WEBHOOK_URL=${WEBHOOK_URL:-}
    command: ["/etc/cryptolake/config.yml"]
```

Replace with:

```yaml
    environment:
      - LOG_LEVEL=INFO
      - HOST_DATA_DIR=${HOST_DATA_DIR:-/data}
      - DATABASE__URL=jdbc:postgresql://postgres:5432/cryptolake?user=cryptolake&password=${POSTGRES_PASSWORD:-postgres}
      - MONITORING__WEBHOOK_URL=${WEBHOOK_URL:-}
      # Bound JVM heap so any future buffer-growth regression yields a logged
      # OutOfMemoryError BEFORE the kernel SIGKILL fires. -Xms below -Xmx so startup
      # doesn't allocate the whole heap eagerly.
      - JAVA_TOOL_OPTIONS=-Xmx512m -Xms256m
    command: ["/etc/cryptolake/config.yml"]
    deploy:
      resources:
        limits:
          memory: 768M
```

- [ ] **Step 5.2: Validate compose merge**

```bash
HOST_DATA_DIR=/tmp/dc-validate docker compose --file docker-compose.yml config > /tmp/dc-merged.yml
echo "exit=$?"
grep -E "JAVA_TOOL_OPTIONS|memory:" /tmp/dc-merged.yml | grep -A0 -B0 "" | head -10
```

Expected: `exit=0`. The grep output shows the `JAVA_TOOL_OPTIONS` env var with `-Xmx512m -Xms256m` and the `memory: 768M` (or `805306368` bytes — Compose normalizes) under the writer service.

- [ ] **Step 5.3: Commit**

```bash
git add docker-compose.yml
git commit -m "$(cat <<'EOF'
chore(writer): cap container memory at 768M, JVM heap at 512m

Bounds writer container memory and JVM heap so any future buffer-
growth regression yields a logged java.lang.OutOfMemoryError BEFORE
the kernel OOM-killer fires SIGKILL. Operator sees the cause; the
JVM's hs_err_pid file is produced; the truncate-on-error path inside
DurableAppender.appendAndFsync gets a chance to run; the next startup's
ZstdTailScrubber heals any leftover damage.

768M container, 512m -Xmx, 256m -Xms. ~256 MB headroom for off-heap
(DirectByteBuffer for Kafka, native zstd state, threads).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Free chaosfs image after each chaos teardown

**Files:**
- Modify: `tests/chaos/common.sh`

- [ ] **Step 6.1: Locate the teardown_stack final block**

```bash
grep -nE "rm -rf \"\\\$HOST_DATA_DIR\"|=== Teardown complete ===" /Users/vasyl.vaskovskyi/data/cryptolake/tests/chaos/common.sh
```

Expected: two lines around 1095–1098 showing the host-dir cleanup and the trailing teardown-complete log.

- [ ] **Step 6.2: Insert chaosfs image rmi after host-dir cleanup**

Open `tests/chaos/common.sh`. Find this exact block (around lines 1094–1098):

```bash
    if [[ -n "${HOST_DATA_DIR:-}" && -d "$HOST_DATA_DIR" ]]; then
        rm -rf "$HOST_DATA_DIR"
        msg "Removed ${HOST_DATA_DIR}"
    fi
    msg "=== Teardown complete ==="
}
```

Replace with:

```bash
    if [[ -n "${HOST_DATA_DIR:-}" && -d "$HOST_DATA_DIR" ]]; then
        rm -rf "$HOST_DATA_DIR"
        msg "Removed ${HOST_DATA_DIR}"
    fi

    # Free the cached chaosfs upstream image so each scenario start is hermetic. The image
    # is pinned by digest in docker-compose.chaos-02-nfs.yml; we rmi by the same digest here
    # so the rmi targets exactly that image regardless of any other tagged variant. Best-
    # effort; harmless when no chaosfs scenario was loaded (image not present, rmi exits 1).
    docker rmi itsthenetwork/nfs-server-alpine@sha256:7fa99ae65c23c5af87dd4300e543a86b119ed15ba61422444207efc7abd0ba20 2>/dev/null \
        && msg "Removed cached chaosfs image" \
        || true

    msg "=== Teardown complete ==="
}
```

- [ ] **Step 6.3: Syntax-check**

```bash
bash -n tests/chaos/common.sh
```

Expected: exit 0, no output.

- [ ] **Step 6.4: Commit**

```bash
git add tests/chaos/common.sh
git commit -m "$(cat <<'EOF'
chore(chaos): rmi chaosfs upstream image after each teardown

teardown_stack now removes the cached itsthenetwork/nfs-server-alpine
image (pinned by sha256 digest matching docker-compose.chaos-02-nfs.yml)
after every chaos scenario. Ensures repeated runs are hermetic and the
chaosfs image isn't retained between iterations of the chaos suite.
Best-effort: silent when not present.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Build gates verification (no commit)

**Files:** none.

- [ ] **Step 7.1: Full writer module build**

```bash
./gradlew :writer:build -i 2>&1 | tail -10
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 7.2: Confirm all new tests pass and existing tests still green**

```bash
./gradlew :writer:test --tests "*KafkaConsumerLoopHoldPause*" --tests "*ZstdTailScrubber*" --tests "*OffsetCommitCoordinator*" --rerun-tasks 2>&1 | tail -15
```

Expected: BUILD SUCCESSFUL. JUnit XML in `writer/build/test-results/test/` shows:
- `TEST-...KafkaConsumerLoopHoldPauseTest.xml` — `tests="4" failures="0"`
- `TEST-...ZstdTailScrubberTest.xml` — `tests="5" failures="0"`
- `TEST-...OffsetCommitCoordinatorHoldIntegrationTest.xml` — `tests="5" failures="0"`
- `TEST-...OffsetCommitCoordinatorTest.xml` — `tests="3" failures="0"`

- [ ] **Step 7.3: Validate compose YAML**

```bash
HOST_DATA_DIR=/tmp/dc-validate docker compose --file docker-compose.yml config > /tmp/dc-final.yml
echo "exit=$?"
```

Expected: `exit=0`.

- [ ] **Step 7.4: Validate chaos common.sh**

```bash
bash -n tests/chaos/common.sh && echo "chaos common.sh OK"
```

Expected: `chaos common.sh OK`.

- [ ] **Step 7.5: No commit**

This task is verification-only.

---

## Self-Review Checklist (run before handoff)

- **Spec coverage:**
  - [x] Goal 1 (pause Kafka during hold): Tasks 1 + 2.
  - [x] Goal 2 (heal torn zstd tails on startup): Tasks 3 + 4.
  - [x] Goal 3 (bound JVM memory + container limit): Task 5.
  - [x] Goal 4 (free chaosfs image after teardown): Task 6.
  - [x] Five new test methods for hold-pause: Task 2 step 2.5 (4 methods total — one per spec contract point).
  - [x] Five new test methods for ZstdTailScrubber: Task 3 step 3.5 (5 methods).
  - [x] OffsetCommitCoordinator's new accessor: Task 1.
  - [x] Build gate verification: Task 7.

- **Placeholder scan:** No "TBD", "TODO", "implement later". Each step has either complete code or a concrete shell command with expected output. The image digest is the literal hash from `docker-compose.chaos-02-nfs.yml`.

- **Type / name consistency:**
  - `applyHoldPauseState` / `lastKnownHeld` / `isLastKnownHeldForTest` — used identically across implementation (Task 2.3) and test (Task 2.1, 2.5).
  - `isAnyHoldActive` — defined in Task 1, consumed in Task 2.
  - `ZstdTailScrubber.scrub(Path)` returning `int` — defined in Task 3.3, consumed in Task 4.2.
  - Sidecar handling via `Sha256Sidecar.write(file, FilePaths.sidecarPath(file))` — consistent across `ZstdTailScrubber` and tests.
  - Image digest `sha256:7fa99ae65c23c5af87dd4300e543a86b119ed15ba61422444207efc7abd0ba20` — matches `docker-compose.chaos-02-nfs.yml`.

---

## Acceptance Criteria

- `./gradlew :writer:build` BUILD SUCCESSFUL.
- 4 new `KafkaConsumerLoopHoldPauseTest` tests pass; 5 new `ZstdTailScrubberTest` tests pass; existing `OffsetCommitCoordinatorTest` (3) and `OffsetCommitCoordinatorHoldIntegrationTest` (5) still pass.
- `docker compose --file docker-compose.yml config` parses without error and shows `JAVA_TOOL_OPTIONS=-Xmx512m -Xms256m` plus `memory: 768M` (or normalized byte count) on the writer service.
- `bash -n tests/chaos/common.sh` exits 0.
- The user's manual chaos-02 run is expected to: emit `LIFECYCLE WRITER_KAFKA_CONSUMPTION_PAUSED` shortly after hold entry; emit `LIFECYCLE WRITER_KAFKA_CONSUMPTION_RESUMED` shortly after hold exit; the writer JVM does not die; verify reports zero decompression errors; the chaosfs image is removed at teardown. End-to-end validation is the operator's call, not part of this branch's CI gate.

---

## Out of scope (deliberate)

- Modifying `BufferManager` to enforce a max-bytes cap. Pause solves the same problem without lossy semantics.
- Modifying `DurableAppender.appendAndFsync`'s truncate-on-IOException path. Already correct.
- Wiring controllers into `commitSealedHour` or `commitBeforeRevoke`.
- Pausing the `BackupTailConsumer`. Liveness only; never accumulates.
- Tuning the 30 s probe interval, 50 MiB recovery threshold, 3-failure PG threshold, or `MAX_POLL_RECORDS_CONFIG=500`.
- Adjusting chaos-02's `expect_no_gaps_check` assertion. Out of scope here; chaos-script change.
- Re-running the chaos suite as part of this branch's CI gate.
