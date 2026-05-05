# Writer DiskFull / PgOutage Hold Controllers — Wiring Design

**Date:** 2026-05-05
**Branch:** `writer-disk-full-pg-outage-wiring` (off `main`)
**Related:** chaos scenario 02 (`writer_disk_full_brief`) end-to-end run on the `chaos-02-nfs-sidecar` branch surfaced the gap that motivated this work.

## Problem

Two durability controllers were added to the writer module but never wired into production code paths:

- `DiskFullHoldController` (commit `34e8d1f`) — designed to detect ENOSPC on archive writes, pause Kafka offset commits, and emit lifecycle/gap events.
- `PgOutageHoldController` (commit `013420c`) — designed to detect prolonged PostgreSQL unavailability with the same shape.

Both classes have unit tests but no production references. `grep -rn DiskFullHoldController writer/src/main/` returns only the file's own definition. `grep -rn PgOutageHoldController writer/src/main/` returns only its own definition plus a `@link` reference from `DiskFullHoldController`'s javadoc. The controllers' lifecycle events (`WRITER_DISK_FULL_HOLD_ENTERED`, `WRITER_DISK_FULL_HOLD_EXITED`, `WRITER_PG_OUTAGE_HOLD_ENTERED`, `WRITER_PG_OUTAGE_HOLD_EXITED`) can never fire today because the controllers are never invoked.

The end-to-end run of chaos scenario 02 with the new NFS sidecar confirms the impact: the writer's tmpfs fills, `appendAndFsync` throws `IOException("No space left on device")`, the IOException is caught in `OffsetCommitCoordinator.flushAndCommit:128-136`, wrapped as `UncheckedIOException`, swallowed by `KafkaConsumerLoop`'s generic catch as `consume_loop_error`, the consume loop continues, the next flush fails the same way, and eventually the writer's healthcheck fails enough times that Docker restarts the container — producing spurious `restart_gap` envelopes in the archive. The intended behavior (controlled hold; offsets preserved; recovery from Kafka after disk frees) never engages.

## Goals

1. Make `DiskFullHoldController` engage when `appendAndFsync` returns ENOSPC, pause Kafka commits, and exit hold cleanly when disk space recovers.
2. Make `PgOutageHoldController` engage after three consecutive `CryptoLakeStateException`s on `saveStatesAndCheckpoints`, pause Kafka commits, and exit hold when PG recovers.
3. Make chaos scenarios `02_fill_disk` and `07_pg_kill_during_commit` exercise the real production code paths.
4. Preserve all existing flush/commit ordering invariants and existing failure-mode behavior for non-ENOSPC IOExceptions and for the first/second PG failures (today's "fail fast on first PG failure" continues to apply below the threshold).

## Non-goals

- Changing the controllers' internal state machines or contracts. Both `DiskFullHoldController` and `PgOutageHoldController` stay byte-for-byte identical; we only wire them.
- Wiring controllers into `FileRotator.seal()`. Hour-rotation during hold is documented as a known limitation; the chaos windows are short enough that rotation cannot fire during them. A follow-up may address this if production rotation-during-outage becomes observed.
- Adding a `HoldController` interface or other abstraction. Two controllers, two `isHoldActive()` calls in the gate. YAGNI on a third.
- Modifying chaos scenario 02 / 07 scripts. The chaos branch will adjust assertions in a follow-up after this branch merges (specifically, scenario 02's `expect_no_gaps_check` becomes `expect_disk_full_hold_gaps_only_check` because hold-marker envelopes ARE emitted by design).

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│ Main.java (writer wiring, single virtual-thread top-level)          │
│                                                                      │
│  ┌─────────────────────────────┐   ┌──────────────────────────────┐ │
│  │ DiskFullHoldController      │   │ PgOutageHoldController       │ │
│  │  - diskProbe (>50 MiB free) │   │  - pgProbe (cheap PG ping)   │ │
│  │  - retry loop (30 s vthread)│   │  - retry loop (30 s vthread) │ │
│  │  - LIFECYCLE events on edge │   │  - LIFECYCLE events on edge  │ │
│  │  - hold-window gap emit     │   │  - hold-window gap emit      │ │
│  └────────────┬────────────────┘   └──────────────┬───────────────┘ │
│               │ injected (constructor arg)        │ injected         │
│               ▼                                   ▼                  │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ OffsetCommitCoordinator.flushAndCommit(BufferManager)          │ │
│  │  ── early gate ──                                              │ │
│  │  if (diskHold.isHoldActive() || pgHold.isHoldActive()) return 0│ │
│  │  ── flush loop ──                                              │ │
│  │  for (FlushResult r : results) {                               │ │
│  │    try appendAndFsync(r) ...                                   │ │
│  │    catch IOException e:                                        │ │
│  │      metrics.writeErrors++                                     │ │
│  │      diskHold.onWriteError(e)                                  │ │
│  │      if (diskHold.isHoldActive()) return 0   // option-C       │ │
│  │      throw new UncheckedIOException(e)        // non-ENOSPC    │ │
│  │  }                                                             │ │
│  │  ── PG save ──                                                 │ │
│  │  try saveStatesAndCheckpoints(...)                             │ │
│  │  catch CryptoLakeStateException e:                             │ │
│  │    metrics.pgCommitFailures++                                  │ │
│  │    pgHold.recordPgFailure()                                    │ │
│  │    if (pgHold.isHoldActive()) return 0                         │ │
│  │    throw e   // <-threshold: preserve today's fail-fast        │ │
│  │  pgHold.recordPgSuccess()  // resets counter on success path   │ │
│  │  ── Kafka commit ── (unchanged; unreachable while held)        │ │
│  │  primary.commitSync(offsets)                                   │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

The two controllers run independent retry loops on separate virtual threads, both started by `Main.java` and stopped during the shutdown sequence. The consume-loop thread (T1) only ever calls `isHoldActive()` (lock-free atomic read), `onWriteError` / `recordPgFailure` (CAS plus log emit), and `recordPgSuccess` (counter reset, optional CAS). No new synchronization, no new locks.

## Components and File-level changes

| File | Change |
|---|---|
| `writer/src/main/java/com/cryptolake/writer/consumer/OffsetCommitCoordinator.java` | Append two constructor params (`DiskFullHoldController`, `PgOutageHoldController`) at the end. Add early-return at the top of `flushAndCommit`. Modify the IOException catch in the flush loop. Modify the `CryptoLakeStateException` catch on PG save. Add `pgHold.recordPgSuccess()` on the PG-save success path. |
| `writer/src/main/java/com/cryptolake/writer/Main.java` | Construct `DiskFullHoldController` and `PgOutageHoldController` AFTER `OffsetCommitCoordinator`'s deps (`gapEmitter`, `stateManager`, `baseDir`, enabled `(symbol, stream)` list) are ready. Wire them as the new constructor args. `start()` both before consume loop spawns. `stop()` both during shutdown sequence after consume loop terminates and before `primary.close()`. |
| `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorOrderingTest.java` | Update existing test to pass real-controller instances configured for `isHoldActive=false` (just construct the controllers and don't call `onWriteError` / `recordPgFailure`). Asserts unchanged. |
| `writer/src/test/java/com/cryptolake/writer/consumer/OffsetCommitCoordinatorHoldIntegrationTest.java` (new) | Five-test integration suite (see Testing section). |

`DiskFullHoldController.java` and `PgOutageHoldController.java` are NOT modified. Their internal state machines are correct; we only consume them.

## Probe implementations

**Disk probe (used by `DiskFullHoldController`).** A `BooleanSupplier` constructed in `Main.java`:

```java
final long minFreeBytesForRecovery = 50L * 1024 * 1024; // 50 MiB
BooleanSupplier diskProbe = () -> {
  try {
    return Files.getFileStore(Path.of(baseDir)).getUsableSpace() > minFreeBytesForRecovery;
  } catch (IOException e) {
    return false; // probe failure -> conservatively stay in hold
  }
};
```

The 50 MiB threshold is deliberately conservative (≈10 typical flush cycles of headroom) to avoid flap. Hard-coded; no env knob. If a real production scenario needs tuning, that is a follow-up.

**PG probe (used by `PgOutageHoldController`).** A `BooleanSupplier` that performs a cheap PG round-trip via `StateManager`. The exact API depends on `StateManager`'s public surface (to be confirmed during implementation). Two acceptable shapes:

```java
// preferred — if StateManager has a ping() method
BooleanSupplier pgProbe = () -> {
  try { stateManager.ping(); return true; }
  catch (Exception e) { return false; }
};

// fallback — if not, use empty-list saveStatesAndCheckpoints as the round-trip
BooleanSupplier pgProbe = () -> {
  try { stateManager.saveStatesAndCheckpoints(List.of(), List.of()); return true; }
  catch (Exception e) { return false; }
};
```

The implementation step that touches `Main.java` will pick the cleaner of the two and document the choice inline. Returning `false` on probe exception is conservative for the same reason as the disk probe.

## Symbol/stream list

Both controllers take a `List<SymbolStream>` for emitting hold-window gap envelopes. The list is built from the writer's enabled symbols × enabled streams (already accessible in `Main.java` from config). The same list is passed to both controllers; their internal `SymbolStream` records are package-private to each controller (the type is `DiskFullHoldController.SymbolStream` vs `PgOutageHoldController.SymbolStream`). Two thin one-liner conversions in `Main.java`:

```java
List<DiskFullHoldController.SymbolStream> diskSymbolStreams =
    enabledStreamPairs.stream()
        .map(p -> new DiskFullHoldController.SymbolStream(p.symbol(), p.stream()))
        .toList();
List<PgOutageHoldController.SymbolStream> pgSymbolStreams =
    enabledStreamPairs.stream()
        .map(p -> new PgOutageHoldController.SymbolStream(p.symbol(), p.stream()))
        .toList();
```

(Refactoring the two `SymbolStream` records into a shared common type is not in scope; it would mean modifying the controller files we promised not to touch.)

## Data flow — the four interesting paths

**Path 1 — Disk-full entry (chaos-02 scenario).**
1. `flushAndCommit` invoked; both holds inactive; gate passes.
2. First `FlushResult` r: `appendAndFsync(r.filePath(), compressed)` throws `IOException("No space left on device")`.
3. Catch: `metrics.writeErrors(...).increment()` then `diskHold.onWriteError(e)`.
4. `onWriteError`: `isEnospc(e)==true`, CAS `holdActive false→true`, emits `LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED`, emits one hold-start gap envelope per configured `(symbol, stream)`.
5. Catch returns 0 from `flushAndCommit` (option-C short-circuit). No PG save attempted, no `commitSync` attempted.
6. Control returns to `KafkaConsumerLoop`; loop continues to next poll iteration.

**Path 2 — Steady-state during hold.** Subsequent `flushAndCommit` calls hit the early gate (`isHoldActive()==true`) and return 0 immediately. Buffered records accumulate in memory, bounded by `bufferMaxBytes`. Meanwhile the controller's retry-loop virtual thread sleeps 30 s, calls `diskProbe.getAsBoolean()`, returns `false` (still full), logs `disk_full_hold_probe_failed`, sleeps again.

**Path 3 — Disk recovery.** Test/operator removes the filler; tmpfs free space jumps from 0 to 300 MiB. Controller's next probe returns `true`; `onRecovery` CAS-flips `holdActive true→false`, emits `LIFECYCLE WRITER_DISK_FULL_HOLD_EXITED`, emits one hold-end gap envelope per `(symbol, stream)`. Next `flushAndCommit` passes the gate, calls `flushAll()` on buffers (which now contain the records that arrived during hold), `appendAndFsync` succeeds, `saveStatesAndCheckpoints` succeeds, `commitSync` advances offsets, archive is whole.

**Path 4 — PG outage.** `flushAndCommit` proceeds past the gate; all `appendAndFsync`s succeed (disk is fine); `saveStatesAndCheckpoints` throws `CryptoLakeStateException`. Catch: `metrics.pgCommitFailures++`, `pgHold.recordPgFailure()`. Failure 1: counter==1, hold not active → throw (preserves today's behavior). The consume loop logs `consume_loop_error`, continues. Next flush, failure 2: same. Next flush, failure 3: counter==3, CAS-flip to hold active, emit `LIFECYCLE WRITER_PG_OUTAGE_HOLD_ENTERED` + per-stream hold-start gaps; catch returns 0 cleanly (no throw). Steady-state: gate short-circuits future flushes. Recovery: pg-probe round-trip succeeds → `recordPgSuccess` flips out of hold and emits `WRITER_PG_OUTAGE_HOLD_EXITED`.

## Error handling and edge cases

- **E1. Non-ENOSPC IOException** (permission denied, broken pipe, NFS server crash). `diskHold.onWriteError(e)` returns without state change because `isEnospc(e)` is false. The catch then re-throws `UncheckedIOException` exactly as today. No regression.
- **E2. PG outage that resolves before threshold.** Failures 1 and 2 throw (today's behavior preserved). The next successful flush calls `recordPgSuccess` which resets the counter to 0. Three consecutive failures within ~90 s trigger hold.
- **E3. Both holds active simultaneously** (rare, e.g., disk fills *and* PG dies). The gate triggers on either. Recovery requires both probes to clear independently. Independent retry loops, no coupling. Acceptable.
- **E4. Hour rotation while held.** `HourRotationScheduler` calls `FileRotator.seal()` which calls `appendAndFsync` directly, NOT through the coordinator. If disk is full, seal throws `UncheckedIOException` and the consume loop swallows it. Documented as a known limitation; chaos windows are < 1 hour so rotation cannot fire during them. A future plan may extend the wiring to `FileRotator` if needed.
- **E5. Shutdown while held.** SIGTERM → `requestShutdown` → consume loop returns; `shutdownSequence()` calls `committer.shutdownCommit(buffers)` which calls `flushAndCommit` which early-returns 0 (no flush, no commit). `Main.java` then calls `diskHold.stop()` and `pgHold.stop()` (interrupting their retry threads). Records remain uncommitted in Kafka and replay on next start. Acceptable.
- **E6. Probe failures.** Both probe lambdas catch any exception and return `false` (stay in hold). Conservative; avoids flap. The alternative — exit hold optimistically — could lead to immediate re-entry if the underlying issue persists.
- **E7. Race conditions inside `flushAndCommit`.** The consume loop is a single virtual thread (T1). All state mutations on the controllers from T1 are local to one `flushAndCommit` call. The retry-loop-driven exits only happen between flushes, never inside one. No new synchronization needed.

## Testing strategy

**Existing coverage to preserve.**
- `DiskFullHoldControllerTest` — exercises the controller in isolation via functional seams. Stays as-is.
- `PgOutageHoldControllerTest` — same. Stays as-is.
- `OffsetCommitCoordinatorOrderingTest` — asserts the existing flush → PG → Kafka commit ordering. Constructor signature changes; the test must pass real controllers configured to never enter hold. Asserts unchanged.

**New: `OffsetCommitCoordinatorHoldIntegrationTest`.** Drives `OffsetCommitCoordinator` end-to-end with real `DiskFullHoldController` / `PgOutageHoldController` instances (no Mockito for them — pass test `BooleanSupplier` probes and a test gap-emit lambda so we can observe hold-window gap envelopes). The `KafkaConsumer`, `DurableAppender`, and `StateManager` are still mocked because they are out of test scope. Five test methods:

1. **`flushAndCommit_returnsZero_whenDiskHoldEntered_byEnospcIoException`** — Stub `DurableAppender.appendAndFsync` to throw `IOException("No space left on device")` on first call. Drive `flushAndCommit` once with one buffered FlushResult. Assert: returns 0; `appendAndFsync` was called exactly once; `consumer.commitSync` was NOT called; `disk.isHoldActive() == true`; gap-emit lambda received N hold-start envelopes (N = configured (symbol, stream) count); test gap-emit recorded `reason="disk_full_hold"`, `detail="disk_full_hold_started"`.
2. **`flushAndCommit_rethrowsUncheckedIO_whenIoExceptionIsNotEnospc`** — Same setup but throw `IOException("Permission denied")`. Drive `flushAndCommit`. Assert: `UncheckedIOException` propagates; `disk.isHoldActive() == false` (no hold for non-ENOSPC); no hold gap envelopes emitted; `metrics.writeErrors` incremented (preserved behavior).
3. **`flushAndCommit_returnsZero_immediately_whenDiskHoldAlreadyActive`** — Pre-set the controller's hold state by calling `disk.onWriteError(new IOException("No space left on device"))` BEFORE driving `flushAndCommit`. Then drive once. Assert: returns 0; `appendAndFsync` NOT invoked at all (early-return BEFORE the loop); `consumer.commitSync` NOT called; `stateManager.saveStatesAndCheckpoints` NOT called.
4. **`flushAndCommit_recoversAfterDiskHoldExits_andCommitsAccumulatedOffsets`** — Stub appender to throw ENOSPC once, then succeed forever. Drive `flushAndCommit` (enters hold, returns 0). Manually call `disk.onRecovery()` (simulates probe success). Drive `flushAndCommit` again. Assert: returns >0; `commitSync` called once with the expected offsets; one hold-end gap envelope per (symbol, stream); `disk.isHoldActive() == false`.
5. **`flushAndCommit_returnsZero_whenPgHoldEntered_afterThreeConsecutiveFailures`** — Stub `StateManager.saveStatesAndCheckpoints` to throw `CryptoLakeStateException` always; `appendAndFsync` succeeds. Drive `flushAndCommit` three times. Assert: calls 1 and 2 throw the exception (today's behavior preserved); call 3 enters PG hold and returns 0 with no exception; `commitSync` never called; `pg.isHoldActive() == true` after the third call; one hold-start gap envelope per (symbol, stream).

These five cover entry, non-trigger, steady-state, recovery, and the PG-threshold path. Together with the existing controller-isolated unit tests and the existing ordering test, they pin both wiring and semantics.

**Chaos coverage.** Scenario 02 (`writer_disk_full_brief`) and scenario 07 (`pg_kill_during_commit`) become end-to-end validation of this wiring. The chaos branch will adjust scenario 02's assertions (the `expect_no_gaps_check` will need to allow `disk_full_hold` envelopes — they are emitted by design) in a follow-up commit on the chaos branch after this writer branch merges. That tweak is out of scope for this plan.

**Build gates.**
```
./gradlew :writer:test
./gradlew spotlessCheck
./gradlew :writer:installDist
```

CI runs these via `./gradlew build`.

## Acceptance criteria

- Both `DiskFullHoldController` and `PgOutageHoldController` are constructed in `Main.java` and started before the consume loop spawns.
- `OffsetCommitCoordinator.flushAndCommit` early-returns 0 when either hold is active.
- ENOSPC IOException from `appendAndFsync` triggers `diskHold.onWriteError(e)`; the IOException is short-circuited (no rethrow) when ENOSPC; non-ENOSPC IOExceptions still rethrow as `UncheckedIOException`.
- `CryptoLakeStateException` triggers `pgHold.recordPgFailure()`; the exception still throws below the threshold; at the threshold (third consecutive failure) the call returns 0 cleanly.
- The PG-save success path calls `pgHold.recordPgSuccess()` (resets counter; exits hold if active).
- Five new integration tests pass; `OffsetCommitCoordinatorOrderingTest` still passes with the new constructor signature.
- `./gradlew build` passes (compile + spotless + tests).
- After this branch merges, chaos scenario 02's writer log shows `LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED` and `LIFECYCLE WRITER_DISK_FULL_HOLD_EXITED` events bracketing the held window. (Validated end-to-end after the chaos branch's follow-up commit adjusts assertions; out of scope here.)

## Out of scope (deliberate)

- Wiring the controllers into `FileRotator.seal()`. Documented as known limitation E4.
- Refactoring `SymbolStream` into a shared common type.
- Introducing a `HoldController` interface.
- Modifying chaos scenarios `02_fill_disk.sh` or `07_pg_kill_during_commit.sh`. Those tweaks live on the chaos branch.
- Configurable disk-recovery threshold via env. 50 MiB hard-coded for now; revisit only if a real scenario requires it.
- Production tuning of the 30 s probe interval. Both controllers' `RETRY_INTERVAL_NS = 30 s` is left as-is.
