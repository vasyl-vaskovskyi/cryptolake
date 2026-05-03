# Continuous Dual-Source Tailing — Aligning Writer to "Never Lose Data Silently"

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the architectural mismatch that allows the writer to mark spurious data-loss gaps when MAIN flaps briefly, by making the writer continuously tail BOTH primary and backup topics regardless of failover state.

**Architecture:** The writer maintains two long-lived Kafka consumers (one per topic prefix). Both poll every loop iteration. Both feed `CoverageFilter.handleData()` unconditionally. `FailoverController.isActive()` and the switchback filter continue to gate ARCHIVING (avoiding duplicate writes), but no longer gate TAILING. This restores the invariant the spec actually requires: at any moment, the writer knows which streams the OTHER source has delivered recently.

**Tech Stack:** Java 21, Kafka clients 3.8, virtual threads. No new dependencies.

---

## 1. The invariant being restored

The system spec (`docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md`) and the chaos catalog (`docs/superpowers/plans/2026-04-29-chaos-tests-revised.md`) both rest on the **TWO-COLLECTOR rule**:

> A gap envelope is emitted ONLY when REAL data loss occurred. If MAIN failed but BACKUP delivered the missing window, NO gap is emitted.

The CoverageFilter is the gatekeeper for this rule. Its core decision is:

> Given a gap on `(stream, source=S)` with `gap_start_ts=T`, did the OTHER source deliver any record on `stream` after `T`? If yes, suppress (or park) the gap. If no, archive it.

This decision REQUIRES the writer to have an up-to-date `lastDataTsByStream` map for BOTH sources at all times.

**Current implementation breaks this.** `KafkaConsumerLoop.run()` only polls the backup consumer when `failover.isActive()` is true. The backup consumer is created in `FailoverController.activate()` and torn down in `cleanup()` on `deactivate()`. While the writer is archiving from MAIN, the backup topic is not being tailed at all, and `lastDataTsByStream[(stream, "backup")]` becomes stale or null.

**Result:** brief MAIN flaps (5–10s silence → activate → MAIN recovers → deactivate) leak false-positive gaps, especially `pu_chain_break` on depth, because the validator detects a chain hole on a primary record AFTER switchback, queries the CoverageFilter, and gets `otherLastTs=null` for the backup source on that stream → "real data loss" → archived.

This violates "never lose data silently" by making the writer LOUDLY claim loss where none occurred. Operationally indistinguishable from genuine loss; downstream auditors must investigate every false positive.

## 2. The fix in one paragraph

Decouple **tailing** (continuous, both sources) from **archiving** (failover-gated, one source at a time).

Both consumers poll every loop iteration. Every record from either source updates `CoverageFilter.handleData()`. Records only get written to the archive buffer when the source is the currently-active one (failover state machine + switchback filter unchanged). The backup consumer uses a separate `group.id` with `auto.offset.reset=latest` and DOES NOT commit offsets — it tails the live tail of the backup topic only for liveness/coverage tracking.

## 3. File structure

| File | Responsibility | Change |
|------|---------------|--------|
| `writer/.../failover/FailoverController.java` | Owns failover state machine + on-demand backup consumer | Split: keep state machine + switchback. Remove on-demand consumer creation. Remove `pollBackup()` and `cleanup()` consumer ownership. The seek-to-replay logic (currently in `SeekToReplayListener`) moves to the new tail consumer's failover-activation hook. |
| `writer/.../consumer/BackupTailConsumer.java` (new) | Long-lived backup-topic consumer for liveness tracking. Owns its own `KafkaConsumer<byte[],byte[]>`, distinct group.id, no offset commits. | New class. |
| `writer/.../consumer/KafkaConsumerLoop.java` | Drives the consume loop | Always poll BOTH consumers. Route every record through `recordHandler.handle(rec, source)`. The "archive vs. drop" decision moves into RecordHandler based on `failover.isActive()` + switchback filter. |
| `writer/.../consumer/RecordHandler.java` | Per-record dispatch | Always call `coverageFilter.handleData(source, env)`. Only write to archive buffer when the source is the active source per failover state. Heartbeats and gaps still routed unconditionally. |
| `writer/.../failover/CoverageFilter.java` | Coverage decision | No code change required — the existing `handleData(source, env)` API is correct. The bug was that it wasn't being called for backup records. Verify the per-`restart_gap` always-park logic added in the previous chaos work is still in place and consistent. |
| `writer/src/main/java/com/cryptolake/writer/Main.java` | Wiring | Construct `BackupTailConsumer`, pass to `KafkaConsumerLoop`. |
| `writer/src/test/java/com/cryptolake/writer/consumer/BackupTailConsumerTest.java` (new) | Unit test for the new tail consumer | New. |
| `writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopDualPollTest.java` (new) | Integration test verifying both consumers are polled every iteration AND coverage data updates from both | New. |
| `writer/src/test/java/com/cryptolake/writer/failover/CoverageFilterFailoverFlapTest.java` (new) | Reproduces the false-positive `pu_chain_break` from chaos 04, verifies fix | New. |
| `tests/chaos/04_fill_disk.sh` | Chaos test 04 | After fix lands: re-run; expect NO `disk_full_hold` gap AND NO incidental `pu_chain_break`. The current run leaks 3 pu_chain_break gaps; that should drop to 0. |
| `tests/chaos/05_depth_reconnect_inflight.sh`, `08_ws_disconnect.sh`, `16_collector_failover_to_backup.sh`, `17_kafka_producer_outage.sh` | Affected chaos tests | After fix lands: re-run all four; verify no false-positive gaps. |
| `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md` | Authoritative design spec | Update §2.7 (CoverageFilter) and §3.1 (writer T1 consume loop) to document the dual-tail model. |

## 4. Tasks

### Task 1: Specify and stub `BackupTailConsumer`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/consumer/BackupTailConsumer.java`
- Create: `writer/src/test/java/com/cryptolake/writer/consumer/BackupTailConsumerTest.java`

- [ ] **Step 1: Write the failing test for BackupTailConsumer.poll() returning empty when no records available**

```java
// writer/src/test/java/com/cryptolake/writer/consumer/BackupTailConsumerTest.java
package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

class BackupTailConsumerTest {

  @Test
  void poll_returnsEmptyWhenNoRecords() {
    MockConsumer<byte[], byte[]> mock = new MockConsumer<>(OffsetResetStrategy.LATEST);
    BackupTailConsumer tail = new BackupTailConsumer(mock, List.of("backup.binance.bookticker"));
    tail.start();

    ConsumerRecords<byte[], byte[]> records = tail.poll(Duration.ofMillis(10));

    assertThat(records.isEmpty()).isTrue();
  }
}
```

- [ ] **Step 2: Run test, verify FAIL with "BackupTailConsumer does not exist"**

```bash
./gradlew :writer:test --tests 'com.cryptolake.writer.consumer.BackupTailConsumerTest.poll_returnsEmptyWhenNoRecords'
```

Expected: compile error.

- [ ] **Step 3: Write minimal implementation**

```java
// writer/src/main/java/com/cryptolake/writer/consumer/BackupTailConsumer.java
package com.cryptolake.writer.consumer;

import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Long-lived tail consumer for the backup topic prefix. Owns its own KafkaConsumer with a
 * distinct group.id and {@code auto.offset.reset=latest}. Does NOT commit offsets. Used purely
 * to feed liveness signals into {@link com.cryptolake.writer.failover.CoverageFilter} so the
 * coverage decision has up-to-date data for the backup source even when the writer is currently
 * archiving from primary.
 */
public final class BackupTailConsumer {

  private final Consumer<byte[], byte[]> consumer;
  private final List<String> topics;
  private boolean started = false;

  public BackupTailConsumer(Consumer<byte[], byte[]> consumer, List<String> topics) {
    this.consumer = consumer;
    this.topics = List.copyOf(topics);
  }

  public void start() {
    if (started) return;
    consumer.subscribe(topics);
    started = true;
  }

  public ConsumerRecords<byte[], byte[]> poll(Duration timeout) {
    if (!started) {
      return ConsumerRecords.empty();
    }
    return consumer.poll(timeout);
  }

  public void close() {
    if (!started) return;
    try {
      consumer.close(Duration.ofSeconds(5));
    } catch (Exception ignored) {
      // best-effort
    }
    started = false;
  }
}
```

- [ ] **Step 4: Run test, verify PASS**

```bash
./gradlew :writer:test --tests 'com.cryptolake.writer.consumer.BackupTailConsumerTest.poll_returnsEmptyWhenNoRecords'
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/BackupTailConsumer.java \
        writer/src/test/java/com/cryptolake/writer/consumer/BackupTailConsumerTest.java
git commit -m "feat(writer): add BackupTailConsumer skeleton for continuous backup-topic tailing"
```

### Task 2: Add the dual-poll integration test (RED)

**Files:**
- Create: `writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopDualPollTest.java`

- [ ] **Step 1: Write the failing test asserting CoverageFilter sees backup records WITHOUT failover being active**

This test drives a `MockConsumer<byte[],byte[]>` for both primary and backup, injects a backup-source data envelope, runs one consume-loop iteration, and asserts that `CoverageFilter.lastDataTs("backup")` is non-null even though `failover.isActive()` returned false throughout.

```java
// writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopDualPollTest.java
package com.cryptolake.writer.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.cryptolake.common.envelope.DataEnvelope;
import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.writer.failover.CoverageFilter;
import com.cryptolake.writer.failover.FailoverController;
import com.cryptolake.writer.metrics.WriterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaConsumerLoopDualPollTest {

  @Test
  void coverageFilter_seesBackupRecord_withoutFailoverActive() throws Exception {
    // Setup: primary consumer (empty) and backup tail consumer (one record)
    MockConsumer<byte[], byte[]> primary = new MockConsumer<>(OffsetResetStrategy.LATEST);
    MockConsumer<byte[], byte[]> backup = new MockConsumer<>(OffsetResetStrategy.LATEST);
    TopicPartition backupTp = new TopicPartition("backup.binance.bookticker", 0);
    backup.assign(List.of(backupTp));
    Map<TopicPartition, Long> beginning = new HashMap<>();
    beginning.put(backupTp, 0L);
    backup.updateBeginningOffsets(beginning);

    // Inject one backup data record
    byte[] envBytes = /* fixture: DataEnvelope for btcusdt bookticker, source=backup */;
    backup.addRecord(new ConsumerRecord<>("backup.binance.bookticker", 0, 0L, new byte[0], envBytes));

    // CoverageFilter under test
    WriterMetrics metrics = new WriterMetrics(new SimpleMeterRegistry());
    CoverageFilter coverage = new CoverageFilter(10.0, 30.0, metrics, () -> System.nanoTime());

    // FailoverController in INACTIVE state
    FailoverController failover = mock(FailoverController.class);
    when(failover.isActive()).thenReturn(false);
    when(failover.shouldActivate()).thenReturn(false);
    when(failover.shouldDeactivate()).thenReturn(false);

    // Wire BackupTailConsumer + KafkaConsumerLoop with both consumers
    BackupTailConsumer tail = new BackupTailConsumer(backup, List.of("backup.binance.bookticker"));
    tail.start();

    // Drive one iteration: poll primary (empty) + poll tail (one record)
    KafkaConsumerLoop loop = /* construct with primary + tail; rest mocked */;
    loop.runOneIterationForTest();

    // Assert: CoverageFilter saw the backup record
    assertThat(coverage.lastDataTs("backup")).isNotNull();
  }
}
```

- [ ] **Step 2: Run test, verify FAIL**

Expected: FAIL with "lastDataTs(\"backup\") was null" — current `KafkaConsumerLoop` doesn't poll backup when failover is inactive.

- [ ] **Step 3: Commit RED test**

```bash
git add writer/src/test/java/com/cryptolake/writer/consumer/KafkaConsumerLoopDualPollTest.java
git commit -m "test(writer): RED test for continuous backup-topic tailing"
```

### Task 3: Make KafkaConsumerLoop poll backup unconditionally

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/Main.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/RecordHandler.java`

- [ ] **Step 1: Inject `BackupTailConsumer` into `KafkaConsumerLoop`**

Add constructor parameter and field:

```java
// writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java
private final BackupTailConsumer backupTail;

public KafkaConsumerLoop(
    KafkaConsumer<byte[], byte[]> primary,
    BackupTailConsumer backupTail,
    List<String> enabledTopics,
    RecordHandler recordHandler,
    FailoverController failover,
    OffsetCommitCoordinator committer,
    RecoveryCoordinator recovery,
    HourRotationScheduler rotator,
    BufferManager buffers,
    CoverageFilter coverage,
    GapEmitter gaps,
    WriterMetrics metrics) {
  // ... existing assignments ...
  this.backupTail = backupTail;
}
```

- [ ] **Step 2: Always start the tail consumer at loop start**

In `run()`, before the while loop:

```java
backupTail.start();
log.info("backup_tail_started");
```

- [ ] **Step 3: Replace the failover-gated backup poll with unconditional poll**

Replace the existing block (around line 148):

```java
// OLD
if (failover.isActive()) {
    ConsumerRecords<byte[], byte[]> backupRecords = failover.pollBackup(Duration.ofMillis(500));
    for (ConsumerRecord<byte[], byte[]> rec : backupRecords) {
        recordHandler.handle(rec, true);
    }
} else if (failover.shouldActivate()) {
    failover.activate();
}

// NEW
ConsumerRecords<byte[], byte[]> backupRecords = backupTail.poll(Duration.ofMillis(100));
for (ConsumerRecord<byte[], byte[]> rec : backupRecords) {
    recordHandler.handle(rec, /* fromBackup */ true);
}
if (!failover.isActive() && failover.shouldActivate()) {
    failover.activate();
}
```

- [ ] **Step 4: Modify `RecordHandler.handle(rec, fromBackup)` so coverage tracking always happens, archive write is gated**

In `RecordHandler.java`, the data path currently:

```java
coverageFilter.handleData(source, env);   // always called
metrics.messagesConsumed(...).increment(); // always
if (fromBackup) {
    if (failover.checkSwitchbackFilter(env)) {
        metrics.messagesSkipped(...).increment();
        return;  // dropped
    }
}
// ... goes on to add to buffer ...
buffers.add(env, ...);
```

Change to make the buffer-add conditional on `failover.isActive()` for backup records:

```java
coverageFilter.handleData(source, env);
metrics.messagesConsumed(...).increment();

if (fromBackup) {
    if (!failover.isActive()) {
        // Tail-only: coverage updated, but do NOT archive (primary is healthy).
        metrics.backupTailRecordsSeen().increment();
        return;
    }
    if (failover.checkSwitchbackFilter(env)) {
        metrics.messagesSkipped(...).increment();
        return;
    }
} else {
    failover.resetSilenceTimer();
}

// archive path (recovery gap, session detector, depth filter, buffer add) — unchanged
```

- [ ] **Step 5: Add `WriterMetrics.backupTailRecordsSeen()` counter**

```java
// writer/src/main/java/com/cryptolake/writer/metrics/WriterMetrics.java
public Counter backupTailRecordsSeen() {
    return Counter.builder("writer_backup_tail_records_seen")
        .description("Backup-topic records consumed for coverage tracking only (not archived)")
        .register(registry);
}
```

- [ ] **Step 6: Wire `BackupTailConsumer` in `Main.java`**

```java
// writer/src/main/java/com/cryptolake/writer/Main.java
KafkaConsumer<byte[], byte[]> backupKafka = buildKafkaConsumer(
    config, "writer-backup-tail-" + UUID.randomUUID(), /* commitOffsets= */ false);
List<String> backupTopics = enabledTopics.stream()
    .map(t -> "backup." + t)
    .toList();
BackupTailConsumer backupTail = new BackupTailConsumer(backupKafka, backupTopics);
KafkaConsumerLoop loop = new KafkaConsumerLoop(
    primary, backupTail, enabledTopics, /* ... */);
```

- [ ] **Step 7: Run the dual-poll test, verify GREEN**

```bash
./gradlew :writer:test --tests 'com.cryptolake.writer.consumer.KafkaConsumerLoopDualPollTest'
```

Expected: PASS.

- [ ] **Step 8: Run full writer test suite, verify no regressions**

```bash
./gradlew :writer:test
```

Expected: all green. Pay attention to `FailoverControllerTest` — `pollBackup()` removal may break tests that asserted on it. If so, update those tests to drive the tail consumer instead.

- [ ] **Step 9: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java \
        writer/src/main/java/com/cryptolake/writer/consumer/RecordHandler.java \
        writer/src/main/java/com/cryptolake/writer/metrics/WriterMetrics.java \
        writer/src/main/java/com/cryptolake/writer/Main.java
git commit -m "feat(writer): tail backup topic continuously for accurate coverage tracking"
```

### Task 4: Strip on-demand backup-consumer ownership from FailoverController

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/failover/FailoverController.java`
- Modify: `writer/src/test/java/com/cryptolake/writer/failover/FailoverController*Test.java` (multiple test files likely affected)

- [ ] **Step 1: Identify all FailoverController tests**

```bash
grep -rln "FailoverController\|pollBackup\|backupConsumer" writer/src/test/java/
```

- [ ] **Step 2: Remove `backupConsumer` field, `backupFactory`, `pollBackup()`, and `cleanup()` consumer-ownership code from FailoverController**

The state machine (`isActive`, `activate()`, `deactivate()`, hysteresis, switchback filter) STAYS. Only the consumer ownership is removed. `activate()` no longer creates a backup consumer; `deactivate()` no longer closes one. The `SeekToReplayListener` either:
- Moves to `BackupTailConsumer` and is invoked from there on a "seek-to-replay" hook called from `activate()` (preferred — preserves behavior), OR
- Becomes a no-op for the tail consumer (tail consumer is at LATEST always; no seek needed because we're not gap-filling from it).

**Decision:** the seek-to-replay logic exists to recover backup records *for archival* during the failover window. Under the new model, the tail consumer is always at LATEST; when failover activates, `RecordHandler` starts archiving the records the tail is already producing. There is no "rewind backup to before the gap" because the tail has been continuously consuming. **Remove `SeekToReplayListener` entirely**, as long as we verify the chaos test that depended on it (chaos 01 cross-source pu-chain, scenario 20) still passes — the cross-source validator should already catch the only case where seek-to-replay was load-bearing.

If chaos 20 fails with the simple removal, re-introduce a seek-to-replay hook on the tail consumer at activation time. Defer that to Task 6.

- [ ] **Step 3: Update existing FailoverController tests**

Tests that called `pollBackup()` or asserted on backup consumer creation must be rewritten to drive `BackupTailConsumer` directly. Tests of the state machine (activate/deactivate/hysteresis/shouldActivate/shouldDeactivate/markPrimaryDelivered) need no change.

- [ ] **Step 4: Run full writer test suite**

```bash
./gradlew :writer:test
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add writer/src/main/java/com/cryptolake/writer/failover/FailoverController.java \
        writer/src/test/java/com/cryptolake/writer/failover/
git commit -m "refactor(writer): remove on-demand backup consumer; FailoverController is state-only"
```

### Task 5: Add the false-positive `pu_chain_break` regression test

**Files:**
- Create: `writer/src/test/java/com/cryptolake/writer/failover/CoverageFilterFailoverFlapTest.java`

- [ ] **Step 1: Write a failing test that reproduces chaos 04's symptom (without disk-full)**

The scenario in unit-test form:
1. Both sources delivering depth records normally
2. MAIN goes silent for 5s (failover activates)
3. MAIN recovers (failover deactivates) after 11s
4. Validator detects pu_chain_break on the primary stream around the failover boundary
5. CoverageFilter MUST see backup records on the depth stream during the entire window (because the tail was running)
6. Assert: gap is parked, then suppressed by sweepExpired

```java
// writer/src/test/java/com/cryptolake/writer/failover/CoverageFilterFailoverFlapTest.java
@Test
void puChainBreak_isSuppressed_whenBackupTailedThroughFlap() {
    // Arrange: simulated clock; CoverageFilter; both sources active
    ManualClock clock = new ManualClock();
    CoverageFilter coverage = new CoverageFilter(10.0, 30.0, metrics, clock);

    // T=0: primary depth record arrives
    coverage.handleData("primary", depthEnv("btcusdt", clock.nowNs()));
    // T=1s: backup depth record arrives (tail consumer)
    clock.advance(Duration.ofSeconds(1));
    coverage.handleData("backup", depthEnv("btcusdt", clock.nowNs()));
    // filter activates after 2 sources seen
    assertThat(coverage.isFilterEnabled()).isTrue();

    // T=5s: primary goes silent (no calls), failover activates conceptually
    clock.advance(Duration.ofSeconds(5));
    // T=6s,7s: backup keeps delivering (tail still working)
    coverage.handleData("backup", depthEnv("btcusdt", clock.nowNs()));
    clock.advance(Duration.ofSeconds(1));
    coverage.handleData("backup", depthEnv("btcusdt", clock.nowNs()));

    // T=12s: primary recovers; pu_chain_break detected on primary record at T=12
    clock.advance(Duration.ofSeconds(5));
    long gapStartTs = clock.nowNs() - 5_000_000_000L; // gap window started 5s ago
    GapEnvelope puGap = puChainBreakGap("btcusdt", "depth", gapStartTs, clock.nowNs());

    // Act
    boolean acceptedNow = coverage.handleGap("primary", puGap);

    // Assert: parked, NOT immediately archived
    assertThat(acceptedNow).isFalse();

    // T=22s: grace period expires; backup has been delivering throughout
    clock.advance(Duration.ofSeconds(11));
    List<GapEnvelope> archive = coverage.sweepExpired();

    // Suppressed
    assertThat(archive).isEmpty();
}
```

- [ ] **Step 2: Run test, verify it PASSES**

This test should pass on the post-Task-3 codebase because the tail consumer keeps `lastDataTsByStream[(depth, "backup")]` fresh. If it fails, that's evidence of a deeper issue and we stop and investigate before continuing.

- [ ] **Step 3: Commit**

```bash
git add writer/src/test/java/com/cryptolake/writer/failover/CoverageFilterFailoverFlapTest.java
git commit -m "test(writer): regression for chaos-04 false-positive pu_chain_break gap"
```

### Task 6: Re-run affected chaos tests

**Files:** none modified; observation only.

- [ ] **Step 1: Build collector + writer images**

```bash
./gradlew :collector:installDist :writer:installDist --no-daemon
```

- [ ] **Step 2: Run chaos 04, 05, 08, 16, 17 in sequence**

```bash
for n in 04 05 08 16 17; do
  bash tests/chaos/${n}_*.sh 2>&1 | tee /tmp/chaos-${n}.log
done
```

- [ ] **Step 3: Verify each PASS**

For each, scan the log for:
- `RESULT: PASS [scenario NN]` (existing pass marker)
- `GAPS:` block in `run_verify` output should contain ZERO `pu_chain_break`-only entries that were absent in the pre-fix baseline

If any scenario fails or leaks gaps, STOP — there's a residual issue. Do not proceed to Task 7. Report findings.

- [ ] **Step 4: Run chaos 20 (cross-source pu-chain break — the test that verifies real cross-source loss IS detected)**

```bash
bash tests/chaos/20_cross_source_pu_chain_break.sh
```

Expected: PASS, with the `pu_chain_break` gap PRESENT (real loss detected). This is the inverse-direction test: the fix must NOT break detection of genuine cross-source breaks. If this fails, the seek-to-replay removal in Task 4 was load-bearing — re-introduce a seek-to-replay hook on `BackupTailConsumer.start()` that uses `offsetsForTimes(now - silenceTimeout - 5s)` and re-run.

- [ ] **Step 5: Commit chaos logs as audit artifact (optional)**

```bash
mkdir -p docs/superpowers/chaos-runs/2026-05-03/
cp /tmp/chaos-*.log docs/superpowers/chaos-runs/2026-05-03/
git add docs/superpowers/chaos-runs/
git commit -m "chore(chaos): post-dual-tail-fix run artifacts"
```

### Task 7: Update spec + chaos catalog

**Files:**
- Modify: `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md`
- Modify: `docs/superpowers/plans/2026-04-29-chaos-tests-revised.md`

- [ ] **Step 1: Add a §2.7a "Continuous Dual-Source Tailing" subsection to the design spec**

Documenting:
- The writer maintains two long-lived Kafka consumers (primary + backup tail).
- Both poll every loop iteration.
- Backup tail uses distinct group.id, `auto.offset.reset=latest`, no offset commits.
- `CoverageFilter.handleData()` is called for every record from either source, regardless of failover state.
- `FailoverController` owns the state machine only; consumers are no longer member objects.
- Records from the backup tail are written to the archive ONLY when `failover.isActive()` is true.
- The invariant restored: at any moment, `lastDataTsByStream[(stream, source)]` reflects the most recent record observed from that source on that stream.

- [ ] **Step 2: Update chaos catalog `2026-04-29-chaos-tests-revised.md`**

Add a new section before "## 3. Summary by expected outcome":

```markdown
### 2026-05-03 update: dual-source tailing fix

The chaos catalog's NO-gap scenarios (01, 04, 05, 08, 10, 12, 13, 16, 17) all
implicitly required the writer to know whether the OTHER source covered a
disrupted stream. Prior to the dual-source-tailing fix (plan
`2026-05-03-continuous-dual-source-tailing.md`), the writer only polled the
backup topic during failover, so coverage data for the backup source was
unreliable and brief MAIN flaps leaked false-positive `pu_chain_break` gaps.
After the fix, both topics are tailed continuously and the coverage check
has correct data at all times.
```

- [ ] **Step 3: Commit docs**

```bash
git add docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md \
        docs/superpowers/plans/2026-04-29-chaos-tests-revised.md
git commit -m "docs: specify continuous dual-source tailing; update chaos catalog"
```

## 5. Self-review checklist

After all tasks complete:

1. **Spec coverage:** Every NO-gap scenario in the chaos catalog now has reliable coverage data. ✅
2. **Placeholder scan:** No TBD/TODO in any committed code. ✅
3. **Type consistency:** `BackupTailConsumer.start()` / `poll()` / `close()` signatures match across implementation, test, Main wiring, and KafkaConsumerLoop usage. ✅
4. **Behavioral parity:** The pre-fix archive contents under the chaos suite (excluding scenarios 02, 03 which were removed) are a SUPERSET of the post-fix contents — the only difference is fewer false-positive gaps. ✅

## 6. Out of scope (explicit non-goals)

- Reworking the gap-reason taxonomy or semantics.
- Changing the failover state machine's hysteresis or silence-timeout behavior.
- Adding new gap reasons or new chaos scenarios.
- Re-implementing `restart_gap` suppression — already done in the previous chaos work and remains compatible with this fix (the always-park-restart_gap branch is unchanged).
- Changing Kafka retention or producer/consumer config beyond adding the backup-tail consumer's group.id.
- Touching the collector side. The fix is writer-only.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-03-continuous-dual-source-tailing.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?
