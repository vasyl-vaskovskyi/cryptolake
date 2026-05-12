# GapReason Enum Refactor — Design Spec

**Date:** 2026-05-13
**Status:** approved (user delegated implementation)

## Goal

Replace the `Set<String> GapReasons.VALID` vocabulary with a typed `enum GapReason`. Collapse the three fragmented sources of gap-reason knowledge (`GapReasons`, `RuntimeOnlyReasons`, `ReasonCausedBy`, plus `writer`'s duplicate `GapReasonsLocal`) into one file. The refactor must not change archive bytes on disk.

## Motivation

The current design splits gap-reason knowledge across four files:

| File | Module | Carries |
|---|---|---|
| `GapReasons.java` | common | The valid wire-string vocabulary (`Set<String>`) |
| `GapReasonsLocal.java` | writer | A near-duplicate of the above |
| `RuntimeOnlyReasons.java` | verify | Classification: `PERSISTENT_CLASS` vs `RUNTIME_ONLY` |
| `ReasonCausedBy.java` | verify | State-reason → file-reason causal map |

A unit test (`RuntimeOnlyReasonsTest.unionPartitionsAllValidReasons`) enforces at build time that the two classification sets partition the vocabulary. Adding a new reason today requires updating all four files in sync. The user wants a single source of truth so the file ↔ state correspondence can be investigated by reading one file.

## Scope

In:
- New: `common/src/main/java/com/cryptolake/common/envelope/GapReason.java`
- Delete: `GapReasons.java`, `GapReasonsLocal.java`, `RuntimeOnlyReasons.java`, `ReasonCausedBy.java`, and the two partition-invariant unit tests.
- Update: `GapEnvelope.reason: String` → `GapReason`. `GapRecord.reason: String` → `GapReason`. Same for `GapRecord.DiffKey`.
- Update all ~155 raw reason-string literals across `collector`, `writer`, `verify`, `consolidation`, and their tests to enum constants.
- Update output paths (`OutputFormatter`, audit JSON, log fields) to render `reason.wire()` so on-the-wire output is bit-identical.

Out of scope:
- No new reasons.
- No new audit features, no `--compact` flag on audit-files/audit-state (that work is the *next* design; this is a pure refactor).
- No changes to PG schema, ledger format, kafka topic shape.

## Architecture

### The enum

```java
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

  /** The on-the-wire string used in JSON and on disk. Stable archive contract. */
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
   * Returns true iff this (persistent-class) reason can cause the given file-side reason.
   * Returns false for runtime-only reasons (they cause nothing — they ARE the leaves).
   */
  public boolean explains(GapReason fileReason) {
    EnumSet<GapReason> set = CAUSED_BY.get(this);
    return set != null && set.contains(fileReason);
  }

  /** Strict JSON deserialization: unknown wire string throws. */
  @JsonCreator
  public static GapReason fromWire(String wire) {
    GapReason r = BY_WIRE.get(wire);
    if (r == null) {
      throw new IllegalArgumentException("Unknown gap reason: '" + wire + "'");
    }
    return r;
  }

  private static final Map<String, GapReason> BY_WIRE;

  static {
    Map<String, GapReason> m = new HashMap<>();
    for (GapReason r : values()) {
      m.put(r.wire, r);
    }
    BY_WIRE = Map.copyOf(m);
  }

  /** State-reason → set of file-reasons it can explain. */
  private static final EnumMap<GapReason, EnumSet<GapReason>> CAUSED_BY =
      new EnumMap<>(GapReason.class);

  static {
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
            RESTART_GAP,
            WS_DISCONNECT,
            PU_CHAIN_BREAK,
            SESSION_SEQ_SKIP,
            RECOVERY_DEPTH_ANCHOR));
    CAUSED_BY.put(
        KAFKA_PRODUCER_OUTAGE,
        EnumSet.of(KAFKA_PRODUCER_OUTAGE, KAFKA_DELIVERY_FAILED, KAFKA_OFFSET_RESET, WRITE_ERROR));
    CAUSED_BY.put(MISSING_HOUR, EnumSet.of(MISSING_HOUR));
  }
}
```

### Wire format

`@JsonValue` on `wire()` makes Jackson serialize a `GapReason` as the lower-snake-case string. `@JsonCreator` on `fromWire` makes Jackson deserialize the same string back. **No archive byte changes.** A pre-refactor `.jsonl.zst` file remains readable; a post-refactor archive serializes identically.

### Strict deserialization (option A)

Unknown wire strings throw `IllegalArgumentException("Unknown gap reason: 'foo'")`. At gap-source layers that walk many archive files, wrap the resulting exception with the offending file path so the audit operator sees *which* file is the problem, not just the bad string. Example:

```java
try {
  envelope = mapper.readValue(line, GapEnvelope.class);
} catch (Exception e) {
  throw new IllegalStateException("Failed to parse gap envelope in " + path, e);
}
```

Trade-off acknowledged: any archive containing a reason not in this enum becomes unreadable by this binary. All known historical reasons are in the enum (exactly matches today's `GapReasons.VALID`). Going forward, every new reason must land in `GapReason` *before* any code emits it. The risk is bounded by build-time discipline.

### Record changes

`GapEnvelope` and `GapRecord` (plus `GapRecord.DiffKey`) become enum-typed for the `reason` field:

```java
public record GapEnvelope(
    int v,
    String type,
    String exchange,
    String symbol,
    String stream,
    long receivedAt,
    String collectorSessionId,
    long sessionSeq,
    long gapStartTs,
    long gapEndTs,
    GapReason reason,            // was String
    String detail,
    String component,
    String cause,
    Boolean planned,
    String classifier,
    Map<String, Object> evidence,
    String maintenanceId) {

  public GapEnvelope {
    java.util.Objects.requireNonNull(reason, "reason");
  }
  // factories unchanged in shape; their `String reason` params become `GapReason reason`
}
```

```java
public record GapRecord(
    String source,
    String exchange,
    String symbol,
    String stream,
    long startMs,
    long endMs,
    GapReason reason,            // was String
    String detail) {
  public DiffKey diffKey() {
    return new DiffKey(exchange, symbol, stream, startMs, endMs, reason);
  }
  public record DiffKey(
      String exchange, String symbol, String stream,
      long startMs, long endMs, GapReason reason) {}
}
```

### Call-site sweep

| Today | After |
|---|---|
| `"ws_disconnect"` (literal) | `GapReason.WS_DISCONNECT` |
| `GapReasons.requireValid(r)` | gone — type system enforces |
| `RuntimeOnlyReasons.PERSISTENT_CLASS.contains(r)` | `r.isPersistent()` |
| `RuntimeOnlyReasons.RUNTIME_ONLY.contains(r)` | `r.isRuntimeOnly()` |
| `ReasonCausedBy.explains(state, file)` | `state.explains(file)` |
| `record.reason()` returns `String` | returns `GapReason` |
| Logging `"reason"` field with `r` | log `r.wire()` so log shape is unchanged |
| Audit JSON `"reason"` field | unchanged — `@JsonValue` on enum serializes to wire string |

### Output stability

- Archive `.jsonl.zst` envelopes: byte-identical before/after.
- Audit JSON output (`audit files --json`, `audit state --json`, `audit reconcile --json`): byte-identical.
- Human audit table output (`OutputFormatter.toHuman`): unchanged columns/widths (reason rendered via `wire()`).
- Log lines: `reason` field renders to `wire()` (the StructuredLogger value-to-string conversion is fine for any `Object`, but we'll explicitly pass `.wire()` to be unambiguous).
- Prometheus metric labels using reason as a label value: render via `wire()`.

### Module dependency

`ReasonCausedBy`'s logic moves from `verify` to `common`. This is correct on principle (the causal map is a property of the vocabulary, not of the reconciler) and incurs no new module-cycle risk (`common` already has no upward dependencies).

### Tests

Drop:
- `common/src/test/java/com/cryptolake/common/envelope/GapReasonsTest.java`
- `verify/src/test/java/com/cryptolake/verify/audit/RuntimeOnlyReasonsTest.java`

Both encode invariants that become compile-time facts of the enum.

Add:
- `common/src/test/java/com/cryptolake/common/envelope/GapReasonTest.java`:
  - Round-trip `WS_DISCONNECT` ⇄ `"ws_disconnect"` via Jackson.
  - Unknown wire string `"foo"` throws on `fromWire` (and on `mapper.readValue` of a JSON containing it).
  - `COLLECTOR_RESTART.explains(WS_DISCONNECT)` is `true`; `WS_DISCONNECT.explains(WS_DISCONNECT)` is `false` (runtime-only causes nothing).
  - For each value: `isPersistent() XOR isRuntimeOnly()` is `true` (partition).

All other existing tests are updated mechanically to use enum constants instead of string literals.

## Error handling

| Error site | Behavior |
|---|---|
| `GapReason.fromWire("foo")` | throws `IllegalArgumentException` |
| Deserializing a `GapEnvelope` with unknown reason | Jackson wraps in `ValueInstantiationException`; gap source rethrows with file path |
| Constructing `GapEnvelope` with `null` reason | compact constructor throws `NullPointerException` |
| Constructing `GapRecord` with `null` reason | record-validated `NullPointerException` (added to compact constructor) |

## Risk register

1. **Archive incompatibility.** A future archive containing a reason that someone adds without first updating `GapReason` will fail to read. *Mitigation:* code review discipline; the enum is now the central choke point.
2. **Mass-edit hazard.** ~155 string literals get rewritten. *Mitigation:* implementer subagent runs per-task in TDD style; tests catch regressions per task; per-task commits are reversible.
3. **Test-only string literals (e.g. fixture JSON).** Some tests construct envelopes from raw JSON strings to assert deserialization works. Those strings already use the wire form; they continue to work because `@JsonCreator` accepts the same strings. No edits needed except where tests construct `GapRecord`/`GapEnvelope` directly via constructor.

## Out of scope (explicit)

- Audit-files/audit-state diff-able output rework (separate spec, post-refactor).
- New gap reasons.
- Schema migrations.
- Spotless config changes.
