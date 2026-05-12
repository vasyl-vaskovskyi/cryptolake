# GapReason Enum Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `Set<String> GapReasons.VALID` + the two helper classes (`RuntimeOnlyReasons`, `ReasonCausedBy`) with a single `enum GapReason` carrying the wire string, persistence classification, and causal mapping inline. Archive byte format unchanged.

**Architecture:** New `GapReason` enum in `common`. Strict JSON deserialization (unknown wire string throws). `@JsonValue`/`@JsonCreator` preserves the on-disk JSON contract. Records `GapEnvelope` (common) and `GapRecord` / `GapRecord.DiffKey` (verify) hold a typed `GapReason` field instead of `String`. Call sites updated to enum constants.

**Tech Stack:** Java 21, Gradle, Jackson, JUnit 5 + AssertJ, Spotless (google-java-format 1.23.0).

**Reference spec:** `docs/superpowers/specs/2026-05-13-gap-reason-enum-design.md`

**Operational notes for every task:**
- Commit directly to `main`; no feature branches.
- Before any commit: `./gradlew spotlessApply`.
- After each task: `./gradlew build` must pass end-to-end.
- Conventional Commits prefix style (`refactor(...)`), no `Co-Authored-By` trailer.

---

## File structure

| Status | Path | Module | Role |
|---|---|---|---|
| **CREATE** | `common/src/main/java/com/cryptolake/common/envelope/GapReason.java` | common | The enum (vocabulary + classification + causal map) |
| **CREATE** | `common/src/test/java/com/cryptolake/common/envelope/GapReasonTest.java` | common | Round-trip JSON, strict unknown, `explains`, partition |
| **MODIFY** | `common/src/main/java/com/cryptolake/common/envelope/GapEnvelope.java` | common | `reason: String` → `GapReason` |
| **MODIFY** | `common/src/test/java/com/cryptolake/common/envelope/GapEnvelopeTest.java` | common | Tests use enum constants |
| **MODIFY** | `common/src/test/java/com/cryptolake/common/envelope/EnvelopeCodecTest.java` | common | Tests use enum constants |
| **MODIFY** | `collector/src/main/java/com/cryptolake/collector/gap/GapEmitter.java` | collector | Producer; receives + emits `GapReason` |
| **MODIFY** | `collector/src/main/java/com/cryptolake/collector/streams/DepthStreamHandler.java` | collector | Calls into emitter with enum constants |
| **MODIFY** | several `collector/src/test/...` | collector | Tests pass enum constants |
| **MODIFY** | `writer/src/main/java/com/cryptolake/writer/gap/GapEmitter.java` | writer | Same shape change |
| **MODIFY** | `writer/.../consumer/RecoveryCoordinator.java`, `SessionChangeDetector.java`, `KafkaConsumerLoop.java`, `DepthRecoveryGapFilter.java` | writer | Use enum constants |
| **MODIFY** | `writer/.../durability/DiskFullHoldController.java`, `PgOutageHoldController.java`, `KafkaConsumerOutageDetector.java` | writer | Use enum constants |
| **MODIFY** | `writer/.../failover/CoverageFilter.java` | writer | Reads/passes `GapReason` |
| **MODIFY** | `writer/.../validation/SilenceInferredGapEmitter.java` | writer | Uses enum constants |
| **MODIFY** | several `writer/src/test/...` | writer | Tests use enum constants |
| **MODIFY** | `consolidation/src/main/java/com/cryptolake/consolidation/core/MissingHourGapFactory.java` | consolidation | Enum constant |
| **MODIFY** | `verify/src/main/java/com/cryptolake/verify/audit/GapRecord.java` | verify | `reason: String` → `GapReason`; same for `DiffKey` |
| **MODIFY** | `verify/.../audit/{FileGapSource,KafkaOutageGapSource,LedgerGapSource,ManifestGapSource,MissingHourGapSource,PgComponentRuntimeGapSource,PgMaintenanceIntentGapSource,SequenceIdGapSource}.java` | verify | Construct `GapRecord` with enum |
| **MODIFY** | `verify/.../audit/GapRecordReconciler.java` | verify | `state.explains(file)` and `isPersistent()` |
| **MODIFY** | `verify/.../audit/OutputFormatter.java` | verify | Render `reason.wire()` |
| **MODIFY** | `verify/.../maintenance/MaintenanceWriter.java` | verify | Enum constant where applicable |
| **MODIFY** | all `verify/src/test/...` | verify | Enum constants |
| **DELETE** | `common/src/main/java/com/cryptolake/common/envelope/GapReasons.java` | common | Replaced by enum |
| **DELETE** | `common/src/test/java/com/cryptolake/common/envelope/GapReasonsTest.java` | common | Facts become compile-time |
| **DELETE** | `verify/src/main/java/com/cryptolake/verify/audit/RuntimeOnlyReasons.java` | verify | Moved into enum |
| **DELETE** | `verify/src/test/java/com/cryptolake/verify/audit/RuntimeOnlyReasonsTest.java` | verify | Partition is compile-time |
| **DELETE** | `verify/src/main/java/com/cryptolake/verify/audit/ReasonCausedBy.java` | verify | Moved into enum |
| **DELETE** | `verify/src/test/java/com/cryptolake/verify/audit/ReasonCausedByTest.java` | verify | Replaced by `GapReasonTest` |
| **KEEP** | `writer/src/main/java/com/cryptolake/writer/gap/GapReasonsLocal.java` | writer | Out of scope (coverage-filter labels, not envelope reasons) |

---

## Task 1: Add `GapReason` enum

**Goal:** New file compiles and has dedicated unit tests covering JSON round-trip, strict unknown, classification, and causal-map. Other code untouched.

**Files:**
- Create: `common/src/main/java/com/cryptolake/common/envelope/GapReason.java`
- Create: `common/src/test/java/com/cryptolake/common/envelope/GapReasonTest.java`

- [ ] **Step 1: Write the failing test**

```java
// common/src/test/java/com/cryptolake/common/envelope/GapReasonTest.java
package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class GapReasonTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void serializesToWireString() throws Exception {
    assertThat(mapper.writeValueAsString(GapReason.WS_DISCONNECT)).isEqualTo("\"ws_disconnect\"");
  }

  @Test
  void deserializesFromWireString() throws Exception {
    GapReason r = mapper.readValue("\"collector_restart\"", GapReason.class);
    assertThat(r).isEqualTo(GapReason.COLLECTOR_RESTART);
  }

  @Test
  void unknownWireStringThrows() {
    assertThatThrownBy(() -> GapReason.fromWire("not_a_real_reason"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not_a_real_reason");
  }

  @Test
  void unknownWireStringThrowsViaJackson() {
    assertThatThrownBy(() -> mapper.readValue("\"bogus\"", GapReason.class))
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void classificationPartitionsAllValues() {
    for (GapReason r : GapReason.values()) {
      assertThat(r.isPersistent() ^ r.isRuntimeOnly())
          .as("reason %s must be in exactly one bucket", r)
          .isTrue();
    }
  }

  @Test
  void persistentSetMatchesSpec() {
    assertThat(java.util.Arrays.stream(GapReason.values()).filter(GapReason::isPersistent))
        .containsExactlyInAnyOrder(
            GapReason.COLLECTOR_RESTART,
            GapReason.RESTART_GAP,
            GapReason.KAFKA_PRODUCER_OUTAGE,
            GapReason.MISSING_HOUR);
  }

  @Test
  void collectorRestartExplainsTransientReasons() {
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.WS_DISCONNECT)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.PU_CHAIN_BREAK)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.SESSION_SEQ_SKIP)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.RECOVERY_DEPTH_ANCHOR)).isTrue();
    assertThat(GapReason.COLLECTOR_RESTART.explains(GapReason.RESTART_GAP)).isTrue();
  }

  @Test
  void kafkaProducerOutageExplainsRelatedTransients() {
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.KAFKA_DELIVERY_FAILED)).isTrue();
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.KAFKA_OFFSET_RESET)).isTrue();
    assertThat(GapReason.KAFKA_PRODUCER_OUTAGE.explains(GapReason.WRITE_ERROR)).isTrue();
  }

  @Test
  void missingHourExplainsOnlyItself() {
    assertThat(GapReason.MISSING_HOUR.explains(GapReason.MISSING_HOUR)).isTrue();
    assertThat(GapReason.MISSING_HOUR.explains(GapReason.WS_DISCONNECT)).isFalse();
  }

  @Test
  void runtimeOnlyExplainsNothing() {
    assertThat(GapReason.WS_DISCONNECT.explains(GapReason.WS_DISCONNECT)).isFalse();
    assertThat(GapReason.PU_CHAIN_BREAK.explains(GapReason.SESSION_SEQ_SKIP)).isFalse();
  }

  @Test
  void allKnownWireStringsRoundTrip() throws Exception {
    for (GapReason r : GapReason.values()) {
      String json = mapper.writeValueAsString(r);
      assertThat(mapper.readValue(json, GapReason.class)).isEqualTo(r);
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :common:test --tests "*GapReasonTest*"`
Expected: FAIL (class `GapReason` does not exist).

- [ ] **Step 3: Implement `GapReason`**

```java
// common/src/main/java/com/cryptolake/common/envelope/GapReason.java
package com.cryptolake.common.envelope;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum GapReason {

  // ---- Persistent class — must reconcile between file and state ----
  COLLECTOR_RESTART("collector_restart", Classification.PERSISTENT),
  RESTART_GAP("restart_gap", Classification.PERSISTENT),
  KAFKA_PRODUCER_OUTAGE("kafka_producer_outage", Classification.PERSISTENT),
  MISSING_HOUR("missing_hour", Classification.PERSISTENT),

  // ---- Runtime-only — file-side only, no state counterpart by design ----
  WS_DISCONNECT("ws_disconnect", Classification.RUNTIME_ONLY),
  PU_CHAIN_BREAK("pu_chain_break", Classification.RUNTIME_ONLY),
  SESSION_SEQ_SKIP("session_seq_skip", Classification.RUNTIME_ONLY),
  RECOVERY_DEPTH_ANCHOR("recovery_depth_anchor", Classification.RUNTIME_ONLY),
  BUFFER_OVERFLOW("buffer_overflow", Classification.RUNTIME_ONLY),
  SNAPSHOT_POLL_MISS("snapshot_poll_miss", Classification.RUNTIME_ONLY),
  WRITE_ERROR("write_error", Classification.RUNTIME_ONLY),
  DESERIALIZATION_ERROR("deserialization_error", Classification.RUNTIME_ONLY),
  HANDLER_ERROR("handler_error", Classification.RUNTIME_ONLY),
  CHECKPOINT_LOST("checkpoint_lost", Classification.RUNTIME_ONLY),
  KAFKA_OFFSET_RESET("kafka_offset_reset", Classification.RUNTIME_ONLY),
  KAFKA_DELIVERY_FAILED("kafka_delivery_failed", Classification.RUNTIME_ONLY),
  KAFKA_CONSUMER_OUTAGE("kafka_consumer_outage", Classification.RUNTIME_ONLY),
  PG_OUTAGE_HOLD("pg_outage_hold", Classification.RUNTIME_ONLY),
  DISK_FULL_HOLD("disk_full_hold", Classification.RUNTIME_ONLY),
  CROSS_SOURCE_PU_CHAIN_BREAK("cross_source_pu_chain_break", Classification.RUNTIME_ONLY),
  BOTH_COLLECTORS_SILENT("both_collectors_silent", Classification.RUNTIME_ONLY);

  public enum Classification {
    PERSISTENT,
    RUNTIME_ONLY
  }

  private final String wire;
  private final Classification classification;

  GapReason(String wire, Classification classification) {
    this.wire = wire;
    this.classification = classification;
  }

  @JsonValue
  public String wire() {
    return wire;
  }

  public Classification classification() {
    return classification;
  }

  public boolean isPersistent() {
    return classification == Classification.PERSISTENT;
  }

  public boolean isRuntimeOnly() {
    return classification == Classification.RUNTIME_ONLY;
  }

  /**
   * True iff this (persistent) reason can cause the given file-side reason.
   * Runtime-only reasons cause nothing (return false).
   */
  public boolean explains(GapReason fileReason) {
    EnumSet<GapReason> set = CAUSED_BY.get(this);
    return set != null && set.contains(fileReason);
  }

  @JsonCreator
  public static GapReason fromWire(String wire) {
    GapReason r = BY_WIRE.get(wire);
    if (r == null) {
      throw new IllegalArgumentException("Unknown gap reason: '" + wire + "'");
    }
    return r;
  }

  private static final Map<String, GapReason> BY_WIRE;
  private static final EnumMap<GapReason, EnumSet<GapReason>> CAUSED_BY =
      new EnumMap<>(GapReason.class);

  static {
    Map<String, GapReason> m = new HashMap<>();
    for (GapReason r : values()) {
      m.put(r.wire, r);
    }
    BY_WIRE = Map.copyOf(m);

    CAUSED_BY.put(
        COLLECTOR_RESTART,
        EnumSet.of(
            COLLECTOR_RESTART,
            RESTART_GAP,
            WS_DISCONNECT,
            PU_CHAIN_BREAK,
            SESSION_SEQ_SKIP,
            RECOVERY_DEPTH_ANCHOR));
    CAUSED_BY.put(
        RESTART_GAP,
        EnumSet.of(
            RESTART_GAP, WS_DISCONNECT, PU_CHAIN_BREAK, SESSION_SEQ_SKIP, RECOVERY_DEPTH_ANCHOR));
    CAUSED_BY.put(
        KAFKA_PRODUCER_OUTAGE,
        EnumSet.of(KAFKA_PRODUCER_OUTAGE, KAFKA_DELIVERY_FAILED, KAFKA_OFFSET_RESET, WRITE_ERROR));
    CAUSED_BY.put(MISSING_HOUR, EnumSet.of(MISSING_HOUR));
  }
}
```

- [ ] **Step 4: Format**

Run: `./gradlew spotlessApply`

- [ ] **Step 5: Run test to verify it passes**

Run: `./gradlew :common:test --tests "*GapReasonTest*"`
Expected: PASS (all 11 tests).

- [ ] **Step 6: Full build (sanity)**

Run: `./gradlew build`
Expected: PASS (no other code touched yet; pre-existing tests still pass).

- [ ] **Step 7: Commit**

```bash
git add common/src/main/java/com/cryptolake/common/envelope/GapReason.java \
        common/src/test/java/com/cryptolake/common/envelope/GapReasonTest.java
git commit -m "feat(common): add GapReason enum (vocabulary + classification + causal map)"
```

---

## Task 2: Migrate `GapEnvelope` to typed `GapReason`

**Goal:** `GapEnvelope.reason: String` becomes `GapEnvelope.reason: GapReason`. Wire JSON is byte-identical (verified by an explicit round-trip test). All envelope construction and read sites across collector, writer, consolidation, and their tests are updated.

**Files (production):**
- Modify: `common/src/main/java/com/cryptolake/common/envelope/GapEnvelope.java`
- Modify: `collector/src/main/java/com/cryptolake/collector/gap/GapEmitter.java`
- Modify: `collector/src/main/java/com/cryptolake/collector/streams/DepthStreamHandler.java` (and any other collector site referencing the eight reason constants relevant to it — search-and-replace literal-by-literal)
- Modify: `writer/src/main/java/com/cryptolake/writer/gap/GapEmitter.java`
- Modify: `writer/.../consumer/{RecoveryCoordinator,SessionChangeDetector,KafkaConsumerLoop,DepthRecoveryGapFilter}.java`
- Modify: `writer/.../durability/{DiskFullHoldController,PgOutageHoldController,KafkaConsumerOutageDetector}.java`
- Modify: `writer/.../failover/CoverageFilter.java`
- Modify: `writer/.../validation/SilenceInferredGapEmitter.java`
- Modify: `consolidation/.../core/MissingHourGapFactory.java`

**Files (tests):**
- Modify: `common/src/test/java/com/cryptolake/common/envelope/GapEnvelopeTest.java`
- Modify: `common/src/test/java/com/cryptolake/common/envelope/EnvelopeCodecTest.java`
- Modify: all `collector/src/test/...` referenced in the grep results (KafkaProducerHealthMonitorTest, DepthStreamHandlerTest, DepthGapDetectorTest, GapEmitterTest)
- Modify: all `writer/src/test/...` referenced (RecordHandlerGapRoutingTest, CoverageFilter*Test, GapEmitterTest, KafkaConsumerOutageDetectorTest, DiskFullHoldControllerTest, PgOutageHoldControllerTest, SessionChangeDetectorTest, KafkaOffsetResetTest)

- [ ] **Step 1: Add the wire-format-stability test**

Append to `common/src/test/java/com/cryptolake/common/envelope/GapEnvelopeTest.java`:

```java
@Test
void wireFormatStableAcrossEnumMigration() throws Exception {
  ObjectMapper m = com.cryptolake.common.envelope.EnvelopeCodec.newMapper();
  GapEnvelope env = GapEnvelope.create(
      "binance",
      "btcusdt",
      "depth",
      "session-1",
      42L,
      1_000L,
      2_000L,
      GapReason.WS_DISCONNECT,
      "detail",
      () -> 1_500_000_000L);
  String json = m.writeValueAsString(env);
  // Wire string must remain "ws_disconnect" — NOT "WS_DISCONNECT".
  org.assertj.core.api.Assertions.assertThat(json).contains("\"reason\":\"ws_disconnect\"");
  GapEnvelope round = m.readValue(json, GapEnvelope.class);
  org.assertj.core.api.Assertions.assertThat(round.reason()).isEqualTo(GapReason.WS_DISCONNECT);
}
```

- [ ] **Step 2: Run test — expect compile failure (factory signature changed)**

Run: `./gradlew :common:compileTestJava`
Expected: FAIL (`GapEnvelope.create` does not accept `GapReason`).

- [ ] **Step 3: Modify `GapEnvelope`**

Replace the record header in `common/src/main/java/com/cryptolake/common/envelope/GapEnvelope.java`:

```java
@JsonProperty("reason") GapReason reason,
```

(line 53 — type change only).

Replace the compact constructor (lines 64-66):

```java
public GapEnvelope {
  java.util.Objects.requireNonNull(reason, "reason");
}
```

In both static factories, change the `String reason` parameter to `GapReason reason`. Drop the now-obsolete import of `GapReasons`. The factories' bodies need no other changes (Jackson serializes the enum via `@JsonValue`).

Update the class javadoc (line 16-17): the sentence

> Compact constructor validates {@code reason} against {@link GapReasons#VALID} (Tier 2 §16; Tier 5 M6).

becomes

> Compact constructor null-checks {@code reason}. Vocabulary is enforced by the {@link GapReason} type.

- [ ] **Step 4: Update collector production sites**

For each file listed under "Files (production) - collector":

1. Replace literal strings like `"ws_disconnect"` (passed into `GapEnvelope.create` or `GapEmitter.emit`) with `GapReason.WS_DISCONNECT`.
2. If a method signature accepts `String reason`, change it to `GapReason reason`.

Example — `collector/.../gap/GapEmitter.java`:

```java
// before:
public void emit(String exchange, String symbol, String stream, long startTs, long endTs,
                 String reason, String detail) { ... }
// after:
public void emit(String exchange, String symbol, String stream, long startTs, long endTs,
                 GapReason reason, String detail) { ... }
```

3. Search the file for every reason literal from the enum's wire list and replace with the matching enum constant. Use the wire-string → enum-name mapping from `GapReason.java` as the lookup.

4. Where the file logs `reason` to structured logging, change `"reason", reason` → `"reason", reason.wire()` so the log field stays a plain string.

- [ ] **Step 5: Update writer production sites**

Same pattern as Step 4 for every file under "Files (production) - writer". Note: `CoverageFilter.java` passes `reason` through from envelopes — its parameter type changes from `String` to `GapReason`. `GapEmitter.java` (writer) follows the same pattern as the collector's.

`GapReasonsLocal.COVERED` and friends are NOT gap-envelope reasons — leave those calls alone.

- [ ] **Step 6: Update consolidation**

`consolidation/.../core/MissingHourGapFactory.java`: replace `"missing_hour"` literal with `GapReason.MISSING_HOUR`. Update factory signature if it passes `String` through.

- [ ] **Step 7: Update test sites**

For every test file listed under "Files (tests)":

1. Replace `"ws_disconnect"` (and the other 17 reason literals) with the corresponding `GapReason.X` constant. Use static import where it keeps tests readable.
2. Where a test asserts `envelope.reason()` equals a string, change the expected value to the matching `GapReason` constant.
3. Where a test JSON-decodes a fixture and then reads `.reason`, no source change is needed for the JSON itself — the wire string in the fixture remains valid.

- [ ] **Step 8: Format**

Run: `./gradlew spotlessApply`

- [ ] **Step 9: Build**

Run: `./gradlew build`
Expected: PASS. If a test fails:
- A reason literal was missed → grep for the failing literal, replace with enum.
- A factory signature change cascaded into an unmigrated test → update that test.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor(common): migrate GapEnvelope reason from String to GapReason"
```

---

## Task 3: Migrate `GapRecord` (verify), delete classification + caused-by helpers

**Goal:** `GapRecord.reason: String` and `GapRecord.DiffKey.reason: String` become `GapReason`. All eight gap sources, the reconciler, output formatter, and tests use the enum. The two helper classes and their tests are deleted in this same task because the reconciler is their sole consumer and updating those call sites is the smallest atomic change.

**Files:**
- Modify: `verify/src/main/java/com/cryptolake/verify/audit/GapRecord.java`
- Modify: `verify/.../audit/FileGapSource.java`
- Modify: `verify/.../audit/KafkaOutageGapSource.java`
- Modify: `verify/.../audit/LedgerGapSource.java`
- Modify: `verify/.../audit/ManifestGapSource.java`
- Modify: `verify/.../audit/MissingHourGapSource.java`
- Modify: `verify/.../audit/PgComponentRuntimeGapSource.java`
- Modify: `verify/.../audit/PgMaintenanceIntentGapSource.java`
- Modify: `verify/.../audit/SequenceIdGapSource.java`
- Modify: `verify/.../audit/GapRecordReconciler.java`
- Modify: `verify/.../audit/OutputFormatter.java`
- Modify: `verify/.../maintenance/MaintenanceWriter.java`
- Modify: all `verify/src/test/...` files
- Delete: `verify/.../audit/RuntimeOnlyReasons.java`
- Delete: `verify/src/test/java/com/cryptolake/verify/audit/RuntimeOnlyReasonsTest.java`
- Delete: `verify/.../audit/ReasonCausedBy.java`
- Delete: `verify/src/test/java/com/cryptolake/verify/audit/ReasonCausedByTest.java`

- [ ] **Step 1: Modify `GapRecord` (record + DiffKey)**

```java
// verify/src/main/java/com/cryptolake/verify/audit/GapRecord.java
package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.GapReason;

public record GapRecord(
    String source,
    String exchange,
    String symbol,
    String stream,
    long startMs,
    long endMs,
    GapReason reason,
    String detail) {

  public GapRecord {
    java.util.Objects.requireNonNull(reason, "reason");
  }

  public DiffKey diffKey() {
    return new DiffKey(exchange, symbol, stream, startMs, endMs, reason);
  }

  public record DiffKey(
      String exchange,
      String symbol,
      String stream,
      long startMs,
      long endMs,
      GapReason reason) {}
}
```

- [ ] **Step 2: Update all eight gap sources**

For each of `FileGapSource`, `KafkaOutageGapSource`, `LedgerGapSource`, `ManifestGapSource`, `MissingHourGapSource`, `PgComponentRuntimeGapSource`, `PgMaintenanceIntentGapSource`, `SequenceIdGapSource`:

1. Where the source constructs `new GapRecord(..., "missing_hour", ...)` (or any other reason literal), replace the literal with `GapReason.X`. If the reason originates from reading an envelope's `reason` field, it's already a `GapReason` (post-Task-2) and is passed through.
2. `FileGapSource` reads envelopes; its old code that mapped String reason to a synthetic `GapRecord` is now type-safe.
3. Where the source catches an exception while parsing an archive file, wrap it with the file path so the audit operator can identify the bad file. Example for `FileGapSource`:

```java
try {
  envelope = mapper.readValue(line, GapEnvelope.class);
} catch (com.fasterxml.jackson.databind.JsonProcessingException e) {
  throw new IllegalStateException("Failed to parse gap envelope in " + filePath, e);
}
```

(Only add this wrap where envelope parsing currently allows the exception to propagate unwrapped.)

- [ ] **Step 3: Update `GapRecordReconciler`**

In the reconciler, replace:

- `RuntimeOnlyReasons.PERSISTENT_CLASS.contains(record.reason())` → `record.reason().isPersistent()`
- `RuntimeOnlyReasons.RUNTIME_ONLY.contains(record.reason())` → `record.reason().isRuntimeOnly()`
- `ReasonCausedBy.explains(stateRecord.reason(), fileRecord.reason())` → `stateRecord.reason().explains(fileRecord.reason())`

Remove imports for `RuntimeOnlyReasons` and `ReasonCausedBy`.

- [ ] **Step 4: Update `OutputFormatter`**

Anywhere the formatter prints `record.reason()` as text, change to `record.reason().wire()`. Column widths and ordering are unchanged.

- [ ] **Step 5: Update `MaintenanceWriter` (if it references reason strings)**

Grep the file for any reason literal; replace with the enum constant. If it just writes maintenance intents (no reason field), no change needed.

- [ ] **Step 6: Update all verify test sites**

For each test file referencing reason literals: replace `"ws_disconnect"` etc. with `GapReason.X`. Tests that decode JSON keep the wire strings in the JSON; only Java-side comparisons change.

- [ ] **Step 7: Delete the two helper classes and their tests**

```bash
git rm verify/src/main/java/com/cryptolake/verify/audit/RuntimeOnlyReasons.java \
       verify/src/test/java/com/cryptolake/verify/audit/RuntimeOnlyReasonsTest.java \
       verify/src/main/java/com/cryptolake/verify/audit/ReasonCausedBy.java \
       verify/src/test/java/com/cryptolake/verify/audit/ReasonCausedByTest.java
```

- [ ] **Step 8: Format**

Run: `./gradlew spotlessApply`

- [ ] **Step 9: Build**

Run: `./gradlew build`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor(verify): migrate GapRecord to GapReason; remove RuntimeOnlyReasons and ReasonCausedBy"
```

---

## Task 4: Delete `GapReasons` vocabulary

**Goal:** With Tasks 1-3 done, `GapReasons.VALID` and `GapReasons.requireValid` have no remaining callers. Delete them.

**Files:**
- Delete: `common/src/main/java/com/cryptolake/common/envelope/GapReasons.java`
- Delete: `common/src/test/java/com/cryptolake/common/envelope/GapReasonsTest.java`

- [ ] **Step 1: Verify no remaining callers**

Run: `grep -rn "GapReasons[^L]" /Users/vasyl.vaskovskyi/data/cryptolake --include="*.java"`
Expected: only the two files we're about to delete. `GapReasonsLocal` (the writer's coverage-filter labels, with the `L`) is unrelated and must not match.

If anything else matches, fix that file's callers to use `GapReason` before deleting.

- [ ] **Step 2: Delete the files**

```bash
git rm common/src/main/java/com/cryptolake/common/envelope/GapReasons.java \
       common/src/test/java/com/cryptolake/common/envelope/GapReasonsTest.java
```

- [ ] **Step 3: Format**

Run: `./gradlew spotlessApply`

- [ ] **Step 4: Full build**

Run: `./gradlew build`
Expected: PASS.

- [ ] **Step 5: Run chaos harness sanity check (the integration suite)**

Run: `./gradlew :consolidation:test --tests "*ChaosVerifyIT*"`
Expected: PASS. (Skip if it's marked `@Disabled` in this repo state.)

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(common): remove GapReasons vocabulary (replaced by GapReason enum)"
```

---

## Final code review

After Task 4 commits, dispatch a code-reviewer subagent over the full set of commits from this refactor (use `git log --oneline` to find the four refactor commits and pass that range to the reviewer).
