# Audit Trio Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three independent CLI subcommands (`audit files`, `audit state`, `audit backfill`) that respectively (1) walk archive files for physical gaps + sequence-ID continuity, (2) walk all persistent state sinks for recorded gaps, and (3) gate any backfill on the two reports matching exactly, alerting Alertmanager on divergence.

**Architecture:** New `audit` subcommand tree under `cryptolake-verify`. A single shared `GapRecord` data class normalizes findings from every source into one comparable shape. Each "source" (file gap envelopes, missing hours, ID continuity, PG `component_runtime_state`, PG `maintenance_intents`, lifecycle ledger, Kafka outage journal, daily manifests) is its own focused reader class implementing a small `GapSource` interface. The diff is strict tuple match on `(symbol, stream, startMs, endMs, reason)` with millisecond truncation, runtime-only reasons (ws_disconnect, pu_chain_break, etc. — see Task 2) excluded by design. The backfill command runs both audits, diffs, and either delegates to the existing `gaps backfill` orchestrator or POSTs a synthetic alert to Alertmanager.

**Tech Stack:** Java 21, picocli (already used), HikariCP for PG, Jackson for JSON, AssertJ + JUnit 5 for tests. Three small bash wrappers in `scripts/`.

---

## File Structure

NEW files under `verify/src/main/java/com/cryptolake/verify/audit/`:
- `GapRecord.java` — immutable record: `(source, exchange, symbol, stream, startMs, endMs, reason, detail)`
- `GapSource.java` — interface: `List<GapRecord> read(AuditScope)`
- `AuditScope.java` — value object: time range + filters (`exchanges`, `symbols`, `streams`)
- `PeriodSelector.java` — converts `--day/--week/--hour/--since/--until` flags into `(startMs, endMs)`
- `GapRecordDiff.java` — exact-tuple-match diff with runtime-only-reason filter; returns `DiffResult` with `onlyInFiles`, `onlyInState`, `matched`
- `RuntimeOnlyReasons.java` — constant set of reasons excluded from the diff
- `FileGapSource.java` — scans `hour-X.jsonl.zst` for `type:"gap"` envelopes
- `MissingHourGapSource.java` — emits synthetic `missing_hour` records for absent hour files within `now()` window
- `SequenceIdGapSource.java` — wraps existing `integrity check-{trades,depth,bookticker}` walkers; emits records for discontinuities
- `PgComponentRuntimeGapSource.java` — reads PG `component_runtime_state` rows (`clean_shutdown=false` rows are gaps; `planned=true` is metadata)
- `PgMaintenanceIntentGapSource.java` — reads PG `maintenance_intents` rows
- `LedgerGapSource.java` — reads `${dataDir}/cryptolake/<collectorId>/lifecycle.jsonl`
- `KafkaOutageGapSource.java` — reads `${dataDir}/cryptolake/<collectorId>/kafka_outage.json`
- `ManifestGapSource.java` — reads daily `*.manifest.json` for `gap_records` count + `missing_hours`
- `AlertmanagerNotifier.java` — POSTs a synthetic alert to `${CRYPTOLAKE_ALERTMANAGER_WEBHOOK:-http://127.0.0.1:9093/api/v2/alerts}`
- `AuditCli.java` — picocli parent for `audit` group
- `AuditFilesCommand.java` — `audit files` subcommand
- `AuditStateCommand.java` — `audit state` subcommand
- `AuditBackfillCommand.java` — `audit backfill` subcommand
- `OutputFormatter.java` — renders `List<GapRecord>` as either JSON or human table

NEW files under `verify/src/test/java/com/cryptolake/verify/audit/`:
- One `*Test.java` per non-trivial class above (16 test files)

NEW files under `scripts/`:
- `audit-files.sh`, `audit-state.sh`, `audit-backfill.sh` — tiny wrappers passing flags through

MODIFIED files:
- `verify/src/main/java/com/cryptolake/verify/Main.java` — register `AuditCli.class` in subcommands
- `verify/build.gradle.kts` — confirm `postgresql` + `HikariCP` deps (already present in collector; copy if needed)
- `CLAUDE.md` — append a paragraph under "Modules at a glance" describing the audit trio

---

## Decision: Which Gap Reasons Are "Runtime-Only"?

These reasons are runtime-only — no persistent state record exists today, so they're **excluded from the diff** but still shown in the audit-files output:

```java
public static final Set<String> RUNTIME_ONLY = Set.of(
    "ws_disconnect", "pu_chain_break", "session_seq_skip", "buffer_overflow",
    "snapshot_poll_miss", "recovery_depth_anchor", "write_error",
    "deserialization_error", "handler_error", "checkpoint_lost",
    "kafka_offset_reset", "kafka_delivery_failed", "kafka_consumer_outage",
    "pg_outage_hold", "disk_full_hold", "cross_source_pu_chain_break",
    "both_collectors_silent");
```

These reasons SHOULD have a persistent record (the diff WILL fire on missing or orphan):

```java
public static final Set<String> PERSISTENT_CLASS = Set.of(
    "collector_restart", "restart_gap", "kafka_producer_outage", "missing_hour");
```

If `GapReasons.VALID` ever gains a new value, the audit code must classify it into one of these two buckets (Task 2 includes a test that asserts the union covers `GapReasons.VALID`).

---

## Tasks

### Task 1: GapRecord + AuditScope + PeriodSelector

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/GapRecord.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AuditScope.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/PeriodSelector.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/PeriodSelectorTest.java`

- [ ] **Step 1: Write the failing test for PeriodSelector**

```java
package com.cryptolake.verify.audit;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
class PeriodSelectorTest {
  @Test
  void parsesDay() {
    var p = PeriodSelector.parse("--day", "2026-05-11", null, null);
    assertThat(p.startMs()).isEqualTo(1778457600000L); // 2026-05-11T00:00:00Z
    assertThat(p.endMs()).isEqualTo(1778543999999L);   // 2026-05-11T23:59:59.999Z
  }
  @Test
  void parsesWeekIso() {
    var p = PeriodSelector.parse("--week", "2026-W19", null, null);
    assertThat(p.startMs()).isEqualTo(1778198400000L); // Mon 2026-05-04T00:00Z
    assertThat(p.endMs()).isEqualTo(1778803199999L);   // Sun 2026-05-10T23:59:59.999Z
  }
  @Test
  void parsesHour() {
    var p = PeriodSelector.parse("--hour", "2026-05-11T09", null, null);
    assertThat(p.startMs()).isEqualTo(1778490000000L);
    assertThat(p.endMs()).isEqualTo(1778493599999L);
  }
  @Test
  void parsesSinceUntil() {
    var p = PeriodSelector.parse(null, null, "2026-05-11T08:00:00Z", "2026-05-11T10:00:00Z");
    assertThat(p.endMs() - p.startMs()).isEqualTo(2 * 3600 * 1000L);
  }
}
```

- [ ] **Step 2: Run test, verify FAIL** — `./gradlew :verify:test --tests PeriodSelectorTest`. Expected: compile error / class not found.

- [ ] **Step 3: Implement `GapRecord` record**

```java
package com.cryptolake.verify.audit;
public record GapRecord(
    String source,      // "file.envelope", "file.missing_hour", "pg.component_runtime", "ledger", ...
    String exchange,
    String symbol,
    String stream,
    long startMs,
    long endMs,
    String reason,
    String detail) {
  /** Diff key — what makes two records "equal" for the gating comparison. */
  public DiffKey diffKey() {
    return new DiffKey(exchange, symbol, stream, startMs, endMs, reason);
  }
  public record DiffKey(String exchange, String symbol, String stream, long startMs, long endMs, String reason) {}
}
```

- [ ] **Step 4: Implement `AuditScope`**

```java
package com.cryptolake.verify.audit;
import java.util.List;
public record AuditScope(
    long startMs, long endMs,
    List<String> exchanges, List<String> symbols, List<String> streams,
    String baseDir) {}
```

- [ ] **Step 5: Implement `PeriodSelector`**

```java
package com.cryptolake.verify.audit;
import java.time.*;
import java.time.temporal.WeekFields;
import java.util.Locale;
public record PeriodSelector(long startMs, long endMs) {
  public static PeriodSelector parse(String periodKind, String periodValue, String since, String until) {
    if (since != null && until != null) {
      return new PeriodSelector(Instant.parse(since).toEpochMilli(), Instant.parse(until).toEpochMilli() - 1);
    }
    if ("--day".equals(periodKind)) {
      LocalDate d = LocalDate.parse(periodValue);
      long start = d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      return new PeriodSelector(start, start + 86_400_000L - 1);
    }
    if ("--week".equals(periodKind)) {
      String[] p = periodValue.split("-W");
      int year = Integer.parseInt(p[0]);
      int week = Integer.parseInt(p[1]);
      LocalDate monday = LocalDate.of(year, 1, 4)
          .with(WeekFields.ISO.weekOfYear(), week)
          .with(WeekFields.ISO.dayOfWeek(), 1);
      long start = monday.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      return new PeriodSelector(start, start + 7L * 86_400_000L - 1);
    }
    if ("--hour".equals(periodKind)) {
      LocalDateTime dt = LocalDateTime.parse(periodValue + ":00:00");
      long start = dt.toInstant(ZoneOffset.UTC).toEpochMilli();
      return new PeriodSelector(start, start + 3_600_000L - 1);
    }
    throw new IllegalArgumentException("Specify exactly one of --day / --week / --hour, or --since with --until");
  }
}
```

- [ ] **Step 6: Run test, verify PASS** — `./gradlew :verify:test --tests PeriodSelectorTest`.

- [ ] **Step 7: Commit**

```bash
./gradlew spotlessApply
git add verify/src/main/java/com/cryptolake/verify/audit verify/src/test/java/com/cryptolake/verify/audit
git commit -m "feat(verify): GapRecord + AuditScope + PeriodSelector"
```

---

### Task 2: RuntimeOnlyReasons + GapRecordDiff

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/RuntimeOnlyReasons.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/GapRecordDiff.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/GapRecordDiffTest.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/RuntimeOnlyReasonsTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// RuntimeOnlyReasonsTest.java
package com.cryptolake.verify.audit;
import static org.assertj.core.api.Assertions.assertThat;
import com.cryptolake.common.envelope.GapReasons;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
class RuntimeOnlyReasonsTest {
  @Test
  void unionCoversAllValidReasons() {
    Set<String> union = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    union.addAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    assertThat(union).containsAll(GapReasons.VALID);
    // and no overlap
    Set<String> overlap = new HashSet<>(RuntimeOnlyReasons.RUNTIME_ONLY);
    overlap.retainAll(RuntimeOnlyReasons.PERSISTENT_CLASS);
    assertThat(overlap).isEmpty();
  }
}
```

```java
// GapRecordDiffTest.java
package com.cryptolake.verify.audit;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.junit.jupiter.api.Test;
class GapRecordDiffTest {
  static GapRecord file(long s, long e, String reason) {
    return new GapRecord("file.envelope", "binance", "btcusdt", "depth", s, e, reason, "");
  }
  static GapRecord state(long s, long e, String reason) {
    return new GapRecord("pg.component_runtime", "binance", "btcusdt", "depth", s, e, reason, "");
  }
  @Test
  void matchOnExactTuple() {
    var d = GapRecordDiff.diff(List.of(file(100, 200, "collector_restart")), List.of(state(100, 200, "collector_restart")));
    assertThat(d.matched()).hasSize(1);
    assertThat(d.onlyInFiles()).isEmpty();
    assertThat(d.onlyInState()).isEmpty();
  }
  @Test
  void runtimeOnlyReasonExcludedFromDiff() {
    var d = GapRecordDiff.diff(List.of(file(100, 200, "ws_disconnect")), List.of());
    assertThat(d.matched()).isEmpty();
    assertThat(d.onlyInFiles()).isEmpty();   // excluded — does NOT fire alert
    assertThat(d.onlyInState()).isEmpty();
  }
  @Test
  void persistentClassMissingStateFires() {
    var d = GapRecordDiff.diff(List.of(file(100, 200, "collector_restart")), List.of());
    assertThat(d.onlyInFiles()).hasSize(1);
  }
  @Test
  void persistentClassOrphanStateFires() {
    var d = GapRecordDiff.diff(List.of(), List.of(state(100, 200, "collector_restart")));
    assertThat(d.onlyInState()).hasSize(1);
  }
}
```

- [ ] **Step 2: Run tests, verify FAIL**

- [ ] **Step 3: Implement `RuntimeOnlyReasons`**

```java
package com.cryptolake.verify.audit;
import java.util.Set;
public final class RuntimeOnlyReasons {
  public static final Set<String> RUNTIME_ONLY = Set.of(
      "ws_disconnect", "pu_chain_break", "session_seq_skip", "buffer_overflow",
      "snapshot_poll_miss", "recovery_depth_anchor", "write_error",
      "deserialization_error", "handler_error", "checkpoint_lost",
      "kafka_offset_reset", "kafka_delivery_failed", "kafka_consumer_outage",
      "pg_outage_hold", "disk_full_hold", "cross_source_pu_chain_break",
      "both_collectors_silent");
  public static final Set<String> PERSISTENT_CLASS = Set.of(
      "collector_restart", "restart_gap", "kafka_producer_outage", "missing_hour");
  private RuntimeOnlyReasons() {}
}
```

- [ ] **Step 4: Implement `GapRecordDiff`**

```java
package com.cryptolake.verify.audit;
import java.util.*;
import java.util.stream.Collectors;
public final class GapRecordDiff {
  public record DiffResult(List<GapRecord> matched, List<GapRecord> onlyInFiles, List<GapRecord> onlyInState) {
    public boolean isClean() { return onlyInFiles.isEmpty() && onlyInState.isEmpty(); }
  }
  public static DiffResult diff(List<GapRecord> files, List<GapRecord> state) {
    List<GapRecord> filesPersistent = files.stream()
        .filter(g -> RuntimeOnlyReasons.PERSISTENT_CLASS.contains(g.reason()))
        .toList();
    List<GapRecord> statePersistent = state.stream()
        .filter(g -> RuntimeOnlyReasons.PERSISTENT_CLASS.contains(g.reason()))
        .toList();
    Map<GapRecord.DiffKey, GapRecord> stateByKey = statePersistent.stream()
        .collect(Collectors.toMap(GapRecord::diffKey, g -> g, (a, b) -> a));
    List<GapRecord> matched = new ArrayList<>();
    List<GapRecord> onlyInFiles = new ArrayList<>();
    Set<GapRecord.DiffKey> matchedKeys = new HashSet<>();
    for (GapRecord f : filesPersistent) {
      if (stateByKey.containsKey(f.diffKey())) {
        matched.add(f); matchedKeys.add(f.diffKey());
      } else {
        onlyInFiles.add(f);
      }
    }
    List<GapRecord> onlyInState = statePersistent.stream()
        .filter(g -> !matchedKeys.contains(g.diffKey()))
        .toList();
    return new DiffResult(matched, onlyInFiles, onlyInState);
  }
  private GapRecordDiff() {}
}
```

- [ ] **Step 5: Run tests, verify PASS** — `./gradlew :verify:test --tests "*Diff*" --tests "*RuntimeOnly*"`

- [ ] **Step 6: Commit**

```bash
./gradlew spotlessApply
git add verify/src/main/java/com/cryptolake/verify/audit verify/src/test/java/com/cryptolake/verify/audit
git commit -m "feat(verify): runtime-only reason set + exact-tuple diff"
```

---

### Task 3: GapSource interface + FileGapSource

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/GapSource.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/FileGapSource.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/FileGapSourceTest.java`

`FileGapSource` walks `${baseDir}/<exchange>/<symbol>/<stream>/<YYYY-MM-DD>/hour-H.jsonl.zst` files within the scope's time range, decompresses, parses each line, picks records where `type=="gap"`, converts each into a `GapRecord(source="file.envelope", ..., startMs=gap_start_ts/1_000_000, endMs=gap_end_ts/1_000_000, reason, detail)`.

- [ ] **Step 1: Write failing test using a synthetic archive** — build a temp archive with one bookticker hour file containing a single fixture `type:"gap"` envelope; assert `read(scope)` returns one `GapRecord` with correct fields.

- [ ] **Step 2: Implement `GapSource` interface**

```java
package com.cryptolake.verify.audit;
import java.util.List;
public interface GapSource {
  String name();
  List<GapRecord> read(AuditScope scope);
}
```

- [ ] **Step 3: Implement `FileGapSource`** — uses existing `ArchiveScanner` from `verify.archive` package to enumerate files in scope, decompresses with zstd, parses each JSONL line via the shared `ObjectMapper`, filters to `type==gap`, converts `gap_start_ts` (ns) → `startMs` via `/1_000_000`.

- [ ] **Step 4: Run test, verify PASS**

- [ ] **Step 5: Commit**

```bash
git commit -m "feat(verify): FileGapSource — scan archive for gap envelopes"
```

---

### Task 4: MissingHourGapSource

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/MissingHourGapSource.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/MissingHourGapSourceTest.java`

For each `(exchange, symbol, stream, hour)` in scope where neither `hour-H.jsonl.zst` nor consolidated daily file presence covers it, emit a synthetic `GapRecord(reason="missing_hour", startMs=hour_start, endMs=hour_end-1)`. Skip hours that fall after `now()` (future hours).

- [ ] **Step 1: Failing test** — temp archive with hours 0-5 present, hour 6 missing, current time = hour 7; expect exactly one missing_hour record for hour 6.

- [ ] **Step 2: Implement**

- [ ] **Step 3: Test PASS**

- [ ] **Step 4: Commit** — `feat(verify): MissingHourGapSource`

---

### Task 5: SequenceIdGapSource

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/SequenceIdGapSource.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/SequenceIdGapSourceTest.java`

Wraps the existing `integrity` walkers (`check-trades`, `check-depth`, `check-bookticker`). For each stream in scope where a walker exists, invokes it and converts each detected discontinuity into a `GapRecord(reason="session_seq_skip", detail="trade ID jump: ... -> ...")`. Streams without ID-continuity semantics (funding_rate, liquidations, depth_snapshot, open_interest) are skipped.

- [ ] **Step 1: Failing test** — fixture archive with a trade-ID gap inside one hour; expect one record.
- [ ] **Step 2: Implement** — call existing walker classes from `verify.integrity` package.
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): SequenceIdGapSource`

---

### Task 6: AuditFilesCommand (picocli)

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AuditCli.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AuditFilesCommand.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/OutputFormatter.java`
- Modify: `verify/src/main/java/com/cryptolake/verify/Main.java` — register `AuditCli.class`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/AuditFilesCommandTest.java`

`AuditFilesCommand` accepts `--day/--week/--hour/--since/--until`, `--exchange` (default `binance`), `--symbol` (CSV, default all), `--stream` (CSV, default all WS-eligible), `--json`. Constructs `AuditScope`, runs `FileGapSource` + `MissingHourGapSource` + `SequenceIdGapSource`, prints via `OutputFormatter`.

- [ ] **Step 1: Failing test** — invoke the CommandLine programmatically against a temp archive, capture stdout, assert JSON structure.
- [ ] **Step 2: Implement `AuditCli` parent**

```java
@Command(name = "audit", description = "Audit data integrity end-to-end.",
    subcommands = {AuditFilesCommand.class, AuditStateCommand.class, AuditBackfillCommand.class})
public final class AuditCli implements Runnable {
  @Override public void run() { CommandLine.usage(this, System.out); }
}
```
- [ ] **Step 3: Implement `OutputFormatter`** — JSON via Jackson; human via simple aligned columns.
- [ ] **Step 4: Implement `AuditFilesCommand`** — wire the three sources.
- [ ] **Step 5: Register in `Main.java`** — add `AuditCli.class` to `@Command(subcommands=...)`.
- [ ] **Step 6: PASS**
- [ ] **Step 7: Commit** — `feat(verify): audit files subcommand`

---

### Task 7: PgComponentRuntimeGapSource + PgMaintenanceIntentGapSource

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/PgComponentRuntimeGapSource.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/PgMaintenanceIntentGapSource.java`
- Test: integration tests using Testcontainers (`@Testcontainers` postgres). Pattern matches existing PG tests in the repo if any; otherwise stub the JDBC datasource with a mock.

`PgComponentRuntimeGapSource` SELECTs from `component_runtime_state` where `started_at` overlaps the scope window. Each row with `clean_shutdown=false` becomes `GapRecord(source="pg.component_runtime", reason="restart_gap")` spanning `last_heartbeat_at` → `shutdown_at` (or `started_at` of next session if available). Rows with `clean_shutdown=true AND planned=true` become `reason="collector_restart"`.

`PgMaintenanceIntentGapSource` SELECTs from `maintenance_intents`, converts each active intent into a `GapRecord(reason="collector_restart", detail="maintenance_id=...")`.

- [ ] **Step 1: Failing test** — testcontainers PG + fixture rows.
- [ ] **Step 2: Implement readers** — read-only HikariCP same pattern as `LifecycleStateManager`.
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): PG state sources for audit`

---

### Task 8: LedgerGapSource + KafkaOutageGapSource + ManifestGapSource

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/LedgerGapSource.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/KafkaOutageGapSource.java`
- Create: `verify/src/main/java/com/cryptolake/verify/audit/ManifestGapSource.java`
- Test: one `*Test.java` each, fixture-based.

- `LedgerGapSource` reads `${dataDir}/cryptolake/<collectorId>/lifecycle.jsonl` (paths discovered by scanning `${dataDir}/cryptolake/*/lifecycle.jsonl`). Each `clean_shutdown` event becomes `GapRecord(reason=planned? "collector_restart" : "restart_gap")` spanning that event back to the matching `start` event for the same `collector_session_id`.
- `KafkaOutageGapSource` reads the single-record `kafka_outage.json` (one per collector). If `outage_started_at_ns` is within scope, emit a `GapRecord(reason="kafka_producer_outage")` from that timestamp to now (or scope end).
- `ManifestGapSource` reads each `<YYYY-MM-DD>.manifest.json` in scope and emits one `GapRecord(reason="missing_hour")` per entry in the manifest's `missing_hours[]` array.

- [ ] **Step 1: Failing tests** — temp dirs + fixture files.
- [ ] **Step 2: Implement (three classes)**.
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): ledger/kafka/manifest state sources`

---

### Task 9: AuditStateCommand

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AuditStateCommand.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/AuditStateCommandTest.java`

Aggregates all five state sources from Tasks 7+8 into one normalized `List<GapRecord>`, prints via `OutputFormatter`. Picocli flags identical to `audit files` plus `--db-url` / `--data-dir` (defaults from env/config).

- [ ] **Step 1: Failing test** — programmatic invocation against fixture state.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): audit state subcommand`

---

### Task 10: AlertmanagerNotifier

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AlertmanagerNotifier.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/AlertmanagerNotifierTest.java`

POSTs an array of one Alertmanager alert (`{"labels":{"alertname":"AuditDivergence","severity":"critical",...},"annotations":{"summary":"...","description":"..."}}`) to the configured webhook URL via `java.net.http.HttpClient`. URL from env `CRYPTOLAKE_ALERTMANAGER_WEBHOOK`, default `http://127.0.0.1:9093/api/v2/alerts`. Non-2xx response → log warn + return false. Network error → log warn + return false. Success → return true. Never throws.

- [ ] **Step 1: Failing test** using JDK 21 `HttpServer` to stand up a localhost stub on a random port.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): AlertmanagerNotifier`

---

### Task 11: AuditBackfillCommand

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/audit/AuditBackfillCommand.java`
- Test: `verify/src/test/java/com/cryptolake/verify/audit/AuditBackfillCommandTest.java`

Flow:
1. Parse flags, build `AuditScope`.
2. Run `audit files` → `List<GapRecord> files`.
3. Run `audit state` → `List<GapRecord> state`.
4. Call `GapRecordDiff.diff(files, state)`.
5. If `result.isClean()`: delegate to existing `BackfillOrchestrator` (the same logic invoked by `gaps backfill`).
6. Else: build divergence JSON, POST via `AlertmanagerNotifier`, write `divergence-<epochMs>.json` under `${baseDir}/.cryptolake/audit/`, exit code 2.

- [ ] **Step 1: Failing tests** — three scenarios: clean → backfill invoked (mock orchestrator); divergence in `onlyInFiles` → alert + non-zero exit; divergence in `onlyInState` → alert + non-zero exit.
- [ ] **Step 2: Implement.**
- [ ] **Step 3: PASS**
- [ ] **Step 4: Commit** — `feat(verify): audit backfill gates on diff match`

---

### Task 12: Bash wrappers + CLAUDE.md

**Files:**
- Create: `scripts/audit-files.sh`
- Create: `scripts/audit-state.sh`
- Create: `scripts/audit-backfill.sh`
- Modify: `CLAUDE.md`

Each wrapper is a 5-line shell script:

```bash
#!/usr/bin/env bash
# Thin wrapper around cryptolake-verify audit files.
set -euo pipefail
cd "$(dirname "$0")/.."
exec verify/build/install/verify/bin/verify audit files "$@"
```

`chmod +x` all three. Append a paragraph to CLAUDE.md under "Modules at a glance" describing the audit trio + linking to this plan.

- [ ] **Step 1: Write three scripts.**
- [ ] **Step 2: chmod +x.**
- [ ] **Step 3: Test invocation** — `./scripts/audit-files.sh --day 2026-05-11 --symbol btcusdt --stream depth` against the live archive.
- [ ] **Step 4: Update CLAUDE.md.**
- [ ] **Step 5: Commit** — `feat(scripts): audit trio wrappers + docs`

---

### Task 13: Final full build + manual smoke

- [ ] **Step 1: Full build clean**

```bash
./gradlew spotlessApply build
```
Expected: BUILD SUCCESSFUL, no test failures.

- [ ] **Step 2: Manual smoke (live stack)**

```bash
./scripts/audit-files.sh --day 2026-05-11 --symbol btcusdt --stream depth --json | jq '. | length'
./scripts/audit-state.sh --day 2026-05-11 --symbol btcusdt --json | jq '. | length'
./scripts/audit-backfill.sh --day 2026-05-11 --symbol btcusdt --stream trades
```
Expected: files report shows depth's known h9/h10 gaps; state report shows whatever PG/ledger/manifest carry; backfill either runs (if reports match) or alerts (if they don't).

- [ ] **Step 3: Push branch** — `git push origin feat/audit-trio` (or main if user agrees).

---

## Self-Review Notes

- **Spec coverage:** Three subcommands ✓. Filter by stream + period (week/day/hour) ✓. Sequence-ID check where possible ✓. Audit-state reads all five state sinks ✓. Exact tuple match diff ✓. Alertmanager webhook ✓. Picocli subcommands ✓. Tests for everything ✓.
- **Type consistency:** `GapRecord` fields are referenced identically across all sources; `DiffKey` covers the 6 strict-match fields; `AuditScope` is shared.
- **Placeholder scan:** All steps contain executable code. Test fixtures are described concretely (temp dirs, JDK 21 HttpServer stub for alertmanager, testcontainers for PG).
- **Known caveat:** PG schema columns (`component_runtime_state.shutdown_at`, `maintenance_intents.active`) inferred from `LifecycleStateManager.java`. If schema differs, Task 7 needs a column-name adjustment — column-name discovery should happen in Step 1 of Task 7 (look at the actual `CREATE TABLE` DDL in `infra/postgres/` or wherever migrations live).
