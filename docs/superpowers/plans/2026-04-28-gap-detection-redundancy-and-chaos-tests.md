# Gap detection + redundancy implementation + chaos test suite — Plan

> **For agentic workers:** dispatched in parallel via superpowers:dispatching-parallel-agents. The implementation track and chaos test track run concurrently.

**Goal:** Implement every component described in `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md` and write a chaos test suite that asserts the binding invariant (`cryptolake verify` reports zero ERRORS after each chaos scenario).

**Architecture:** Hot redundant primary + backup collectors (deployment shape D) on the same host. Six new gap reasons. Eight new classes (KafkaOutageJournal, KafkaProducerHealthMonitor, LifecycleJournal, BackupCollectorConfig, KafkaConsumerOutageDetector, PgOutageHoldController, DiskFullHoldController, CrossSourcePuChainValidator, SilenceInferredGapEmitter, LifecycleJournalReader). Twenty-plus chaos scenarios.

**Tech stack:** Java 21, JUnit 5, Mockito, Testcontainers (Kafka, Postgres), bash-driven docker-compose chaos shells.

**Spec source of truth:** `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md`. All ambiguities resolve to that document.

**Authoritative invariant:** *every chaos test must end with `cryptolake verify --date <today> --base-dir /tmp/...` returning exit 0 and zero ERRORS, with any data unavailability covered by exactly one gap envelope.*

---

## Two parallel tracks

This plan splits into two tracks that run in parallel via the
`superpowers:dispatching-parallel-agents` skill. The chaos tests don't
strictly depend on the new code being merged — they can be authored against
the spec's contract — but their *passing* requires both tracks landing.

- **Track A — Code review + design implementation.** Audits the existing
  Java port for correctness against the spec, fixes any divergences, and
  implements the eight new classes and six new gap reasons.
- **Track B — Chaos test suite.** Authors twenty scenarios as bash-driven
  docker-compose shells, plus a JUnit-based integration harness that runs
  Python `cryptolake verify` against each scenario's archive output.

After both tracks complete, an integration step validates: for every chaos
scenario in Track B, run it against the post-Track-A codebase and confirm
the invariant holds.

---

## Track A — Code review + design implementation

### Phase A1 — common module additions

#### Task A1.1: extend `GapReasons.VALID`

**Files:**
- Modify: `common/src/main/java/com/cryptolake/common/envelope/GapReasons.java`
- Modify: `common/src/test/java/com/cryptolake/common/envelope/GapReasonsTest.java`

Add 7 new gap reasons to the `VALID` set (per spec §6.1):
- `kafka_consumer_outage`
- `kafka_producer_outage`
- `kafka_offset_reset`
- `pg_outage_hold`
- `disk_full_hold`
- `cross_source_pu_chain_break`
- `both_collectors_silent`

For each, add: assertion in `GapReasonsTest` that `VALID.contains(reason)`.

Commit: `feat(common): add 7 new gap reasons for redundancy + outage detection`

#### Task A1.2: promote `BackupCollectorConfig` env-var override

**Files:**
- Modify: `common/src/main/java/com/cryptolake/common/config/BinanceExchangeConfig.java`
- Modify: `common/src/main/java/com/cryptolake/common/config/YamlConfigLoader.java`
- Test: `common/src/test/java/com/cryptolake/common/config/BackupConfigOverrideTest.java`

`BinanceExchangeConfig.collectorId()` and the new `BinanceExchangeConfig.topicPrefix()` (default `""`) read overrides from env vars `COLLECTOR_ID` and `TOPIC_PREFIX`. Existing `DATABASE__` etc. prefix mechanism is the pattern; add the same path for these two var names. Test:

- `COLLECTOR_ID=foo` env override yields `collectorId() == "foo"`.
- `TOPIC_PREFIX=backup.` yields `topicPrefix() == "backup."`.
- Absent env vars: YAML values used.

Commit: `feat(common): env-var override of COLLECTOR_ID + TOPIC_PREFIX for backup deployment`

### Phase A2 — collector module additions

#### Task A2.1: `LifecycleJournal`

**Files:**
- Create: `collector/src/main/java/com/cryptolake/collector/durability/LifecycleJournal.java`
- Test: `collector/src/test/java/com/cryptolake/collector/durability/LifecycleJournalTest.java`

Append-only JSONL at `${HOST_DATA_DIR}/cryptolake/${collector_id}/lifecycle.jsonl`. Atomic line writes (`StandardOpenOption.APPEND` + `FileChannel.force(true)`). API:

```java
public final class LifecycleJournal {
  public LifecycleJournal(Path dataDir, String collectorId, ClockSupplier clock);
  public void recordStart(String hostBootId, String collectorSessionId);
  public void recordCleanShutdown(String hostBootId, String collectorSessionId, boolean planned, String maintenanceId);
  public List<LifecycleEvent> readSince(long nsSinceEpoch);   // for writer-side reader
  public void pruneOlderThan(Duration retention);             // 7-day default per Python parity
}

public record LifecycleEvent(long tsNs, String event, String hostBootId,
                             String collectorSessionId, Boolean planned,
                             String maintenanceId) {}
```

Tests: write→read round-trip (3 tests), corrupt last line is dropped on read, prune drops old entries, fsync is invoked on every write.

Commit: `feat(collector): add LifecycleJournal — replaces Python host_lifecycle_agent`

#### Task A2.2: `KafkaOutageJournal`

**Files:**
- Create: `collector/src/main/java/com/cryptolake/collector/durability/KafkaOutageJournal.java`
- Test: `collector/src/test/java/com/cryptolake/collector/durability/KafkaOutageJournalTest.java`

Single-record JSON file at `${HOST_DATA_DIR}/cryptolake/${collector_id}/kafka_outage.json`. API:

```java
public final class KafkaOutageJournal {
  public KafkaOutageJournal(Path dataDir, String collectorId);
  public void recordOutageStart(long tsNs);    // overwrites if exists
  public OptionalLong readOutageStart();
  public void truncate();
}
```

Tests: write+fsync; read returns the start; truncate clears it; on second `recordOutageStart` without truncate, second write replaces (last-writer-wins).

Commit: `feat(collector): add KafkaOutageJournal for kafka_producer_outage durability`

#### Task A2.3: `KafkaProducerHealthMonitor`

**Files:**
- Create: `collector/src/main/java/com/cryptolake/collector/durability/KafkaProducerHealthMonitor.java`
- Test: `collector/src/test/java/com/cryptolake/collector/durability/KafkaProducerHealthMonitorTest.java`
- Modify: `collector/src/main/java/com/cryptolake/collector/Main.java` (wire it up)

Virtual-thread loop probing producer health every 5 seconds. State machine per spec §8.1.

Probe mechanism: call `producer.partitionsFor("__health_probe__")` (cheap metadata fetch) inside a 2s timeout; success = healthy.

On `healthy → degraded` (any failure): increment failure counter.
On `degraded → paused` (failures spanning ≥30s): write `KafkaOutageJournal.recordOutageStart`, log `kafka_outage_started`.
On `paused → healthy`: read journal → emit one `kafka_producer_outage` gap envelope per `(symbol, stream)` covering the outage window → truncate journal → log `kafka_outage_resolved`.

Constructor injects `Clock`, `KafkaProducer`, `KafkaOutageJournal`, `GapEmitter`, list of `(symbol, stream)` pairs. Stop/start lifecycle methods.

Tests with fixed clocks: 30s of failed probes triggers pause; subsequent success emits gap envelope and truncates journal; gap envelope has correct fields including all fields from spec §6.

Commit: `feat(collector): add KafkaProducerHealthMonitor with on-recovery gap emission`

#### Task A2.4: wire `LifecycleJournal` into collector `Main` + retire Python agent

**Files:**
- Modify: `collector/src/main/java/com/cryptolake/collector/Main.java` (add LifecycleJournal startup + shutdown calls)
- Modify: `docker-compose.yml` (re-enable `collector-backup` service per spec §11)
- Delete: `scripts/host_lifecycle_agent.py` (replaced by Java journal)
- Delete: `infra/aws/systemd/cryptolake-lifecycle-agent.service` (no longer needed)

On `Main.main`: after PG `LifecycleStateManager` connects, also call `LifecycleJournal.recordStart(hostBootId, sessionId)`. On shutdown hook: `recordCleanShutdown(...)`. On startup, also call `pruneOlderThan(Duration.ofDays(7))`.

Re-enable collector-backup in compose by uncommenting + setting `COLLECTOR_ID=binance-collector-backup`, `TOPIC_PREFIX=backup.` env vars (now consumed by `BackupCollectorConfig` from Task A1.2).

Commit: `feat(collector): wire LifecycleJournal; re-enable collector-backup; retire Python agent`

### Phase A3 — writer module additions

#### Task A3.1: `LifecycleJournalReader` + integrate with `RestartGapClassifier`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/durability/LifecycleJournalReader.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/failover/RestartGapClassifier.java`
- Test: `writer/src/test/java/com/cryptolake/writer/durability/LifecycleJournalReaderTest.java`

Reader scans both collectors' `${HOST_DATA_DIR}/cryptolake/{primary,backup}/lifecycle.jsonl`. Returns `Map<sessionId, LifecycleEvent>` to `RestartGapClassifier`, which uses it as the authoritative `host_boot_id` evidence (replacing Python `HostLifecycleReader`).

Commit: `feat(writer): replace HostLifecycleReader with Java LifecycleJournalReader`

#### Task A3.2: `KafkaConsumerOutageDetector`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/durability/KafkaConsumerOutageDetector.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java` (track `lastPollWithRecordsAt`, invoke detector)
- Test: `writer/src/test/java/com/cryptolake/writer/durability/KafkaConsumerOutageDetectorTest.java`

Virtual thread wakes every 10s. If `now - lastPollWithRecordsAt > 30s` AND writer's lifecycle PG heartbeat fired in the same window, emit `kafka_consumer_outage` gap covering `[lastPollWithRecordsAt, now]`. Re-arm when poll returns records.

Commit: `feat(writer): add KafkaConsumerOutageDetector for HOLE 1 closure`

#### Task A3.3: `PgOutageHoldController`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/durability/PgOutageHoldController.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/state/StateManager.java` (route through controller)
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java` (consult controller before commit)
- Test: `writer/src/test/java/com/cryptolake/writer/durability/PgOutageHoldControllerTest.java`

State machine per spec §8.2: 3 consecutive PG failures → enter `pg_outage_hold` mode → pause Kafka commits, keep flushing archives + sidecars, emit `pg_outage_hold` gap, retry every 30s. On recovery: resume commits, emit closing gap with `gap_end = now`.

Commit: `feat(writer): add PgOutageHoldController for HOLE 2 closure`

#### Task A3.4: `DiskFullHoldController`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/durability/DiskFullHoldController.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/rotate/FileRotator.java` (catch ENOSPC, route through controller)
- Test: `writer/src/test/java/com/cryptolake/writer/durability/DiskFullHoldControllerTest.java`

Same shape as `PgOutageHoldController` but triggered by `IOException` with cause matching ENOSPC. Pauses commits, emits `disk_full_hold` gap, retries every 30s.

Commit: `feat(writer): add DiskFullHoldController for disk-full safety`

#### Task A3.5: `KafkaOffsetResetEmitter` (in `ConsumerRebalanceListener`)

**Files:**
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/KafkaConsumerLoop.java` (extend `ConsumerRebalanceListener` to detect OUT_OF_RANGE recovery and emit gap)
- Test: `writer/src/test/java/com/cryptolake/writer/consumer/KafkaOffsetResetTest.java`

On `OffsetOutOfRangeException` in poll → consumer auto-resets to `auto.offset.reset`. Detect this transition, emit `kafka_offset_reset` gap with `detail` describing the topic/partition + old/new offsets.

Commit: `feat(writer): emit kafka_offset_reset gap on consumer offset reset`

#### Task A3.6: `CrossSourcePuChainValidator`

**Files:**
- Create: `common/src/main/java/com/cryptolake/common/validation/CrossSourcePuChainValidator.java` (shared between writer + verify per spec §12 Q1)
- Modify: `writer/src/main/java/com/cryptolake/writer/consumer/RecordHandler.java` (invoke validator on every depth data envelope)
- Test: `common/src/test/java/com/cryptolake/common/validation/CrossSourcePuChainValidatorTest.java`

Per-`(exchange, symbol)` state. For every `depth` envelope (regardless of source), parse `U`, `u`, `pu` from `raw_text`. If `pu != last_u && last_u != null`, emit `cross_source_pu_chain_break` gap with `detail` = exact missing-u-range. Coalesce with single-source `pu_chain_break` already in flight. Update `last_u = u`.

5+ tests covering: clean chain, primary-then-backup with no gap, primary-then-backup with gap, multiple symbols isolated, dedup with single-source pu_chain_break.

Commit: `feat(common): add CrossSourcePuChainValidator — closes Tier 5.7 #38`

#### Task A3.7: `SilenceInferredGapEmitter`

**Files:**
- Create: `writer/src/main/java/com/cryptolake/writer/validation/SilenceInferredGapEmitter.java`
- Modify: `writer/src/main/java/com/cryptolake/writer/Main.java` (wire as background virtual thread)
- Test: `writer/src/test/java/com/cryptolake/writer/validation/SilenceInferredGapEmitterTest.java`

Reads `CoverageFilter._global_max_received[source]` and `_global_max_heartbeat[source]`. Per `(symbol, stream)`: if BOTH sources have data older than 30s AND heartbeat older than 15s, emit `both_collectors_silent` gap. Liquidations exempt.

5+ tests: both flowing → no emit; primary stale, backup fresh → no emit; both stale → emit; liquidations both stale → no emit; recovery clears state.

Commit: `feat(writer): add SilenceInferredGapEmitter — closes HOLE 4`

### Phase A4 — verify CLI updates

#### Task A4.1: integrate `CrossSourcePuChainValidator` into verify

**Files:**
- Modify: `verify/src/main/java/com/cryptolake/verify/VerifyCommand.java` (post-merge cross-source pu-chain pass on consolidated archives)
- Test: `verify/src/test/java/com/cryptolake/verify/VerifyCrossSourcePuChainTest.java`

For each archive hour, after the existing per-source pu-chain check, run the merged-stream check. Any break is reported as a verify ERROR with `detail` matching the gap envelope's expected format.

Commit: `feat(verify): post-hoc cross-source pu-chain validation on archives`

#### Task A4.2: `HeartbeatTimelineWalker` for verify CLI

**Files:**
- Create: `verify/src/main/java/com/cryptolake/verify/validation/HeartbeatTimelineWalker.java`
- Modify: `verify/src/main/java/com/cryptolake/verify/VerifyCommand.java` (invoke walker on each `(symbol, stream)`)
- Test: `verify/src/test/java/com/cryptolake/verify/HeartbeatTimelineWalkerTest.java`

Walk archived heartbeat envelopes (assumes spec §12 Q2 resolves to "archive all heartbeats to a parallel topic family `binance.heartbeat.{stream}`"). For each `(symbol, stream)`, find any window > 30s with no heartbeat from EITHER source AND no data envelope. Report as ERROR if no covering gap envelope exists for that window.

Commit: `feat(verify): HeartbeatTimelineWalker for post-hoc silence detection`

### Phase A5 — code review + targeted fixes

#### Task A5.1: audit existing code against spec §5

For each Tier 5.1, 5.2, 5.3 row, find the existing implementation and verify it matches the spec's described mechanism. Fix any divergences. Commit per finding.

Common audits:
- `GapEmitter` produces ALL fields per `GAP_ENVELOPE_FIELDS` (the Bug C/D fix; verify no regression introduced).
- `CoverageFilter.handleGap` correctly suppresses when the OTHER source has data after `gap_start_ts`.
- `RestartGapClassifier` correctly classifies `restart_gap` reasons given lifecycle journal evidence.
- `WebSocketSupervisor.pingLoop` calls `ws.abort()` AND `disconnectLatch.countDown()` (the Bug A fix; verify).
- `FileRotator.writeMissingSidecars` runs on every shutdown path (the Bug B fix; verify).
- `RecordHandler.handle` peeks at `type` field before deserializing (the Bug C/D fix; verify).

Commit pattern: `fix(<module>): <what was wrong>` per finding.

---

## Track B — Chaos test suite

### Phase B1 — chaos harness scaffolding

#### Task B1.1: chaos infrastructure

**Files:**
- Create: `tests/chaos/common.sh` — shared bash helpers (start/stop stack, capture archives, run verify, cleanup)
- Create: `tests/chaos/README.md` — how to run individual scenarios + the full suite

`common.sh` provides:
- `start_stack [primary|primary+backup]` — `docker compose up -d`
- `wait_healthy` — block until `collector` + `writer` healthcheck=healthy
- `kill_service <name>` — `docker compose kill -s SIGKILL <name>`
- `clean_stop_service <name>` — `docker compose stop <name>` (records clean shutdown)
- `capture_archives <dir>` — copy `$HOST_DATA_DIR/binance/...` to `$dir`
- `run_verify <date> <dir>` — invokes Java verify, captures stdout, asserts exit==0 + ERRORS=0
- `assert_gap_present <reason> <archive_dir>` — greps zstd-decompressed jsonl for a gap envelope with the given reason
- `teardown_stack` — `docker compose down -v --remove-orphans`

Commit: `feat(chaos): add common.sh harness + README`

#### Task B1.2: chaos test runner script

**Files:**
- Create: `scripts/run-chaos-tests.sh` — runs each `tests/chaos/NN_*.sh` in order; aggregates pass/fail

Pre-suite cleanup: ensure no leftover `cryptolake-test` resources. Per-test log capture. Final summary.

Commit: `feat(chaos): add run-chaos-tests.sh runner`

### Phase B2 — chaos scenarios

Each scenario is a single `tests/chaos/NN_<name>.sh` that returns 0 on pass, non-zero on fail. Each scenario:

1. Starts the stack (primary + backup unless noted otherwise)
2. Waits healthy
3. Lets data flow for a fixed warm-up period (typically 30-60s)
4. Injects the chaos
5. Waits for stabilization
6. Captures archives
7. Runs verify
8. Asserts: exit 0, ERRORS=0, AND at least one gap envelope of the expected reason is present

#### Task B2.1: existing scenarios (port from Python suite)

These are scenarios that used to exist as Python shell tests (now deleted with the Python codebase). Re-write each in 100% bash + the new `common.sh`. Source: the deleted `tests/chaos/*.sh` files were ported in spirit but adjusted for the Java stack.

- `01_collector_unclean_exit.sh` — kill primary mid-flight; expect `collector_restart` gap; backup keeps flowing
- `02_buffer_overflow_recovery.sh` — fill collector's per-stream cap by stalling its kafka producer (chaos via iptables block); expect `buffer_overflow` gap on recovery
- `03_writer_crash_before_commit.sh` — kill writer between archive flush and offset commit; on restart expect `restart_gap` reason=writer_restart
- `04_fill_disk.sh` — fill `$HOST_DATA_DIR` to 99%; expect `disk_full_hold` gap, then recovery on cleanup
- `05_depth_reconnect_inflight.sh` — drop primary's WS during depth flow; expect `pu_chain_break` then resync; backup covers
- `06_full_stack_restart_gap.sh` — `docker compose down`; restart; expect coordinated `restart_gap` reasons
- `07_host_reboot_restart_gap.sh` — simulate via host_boot_id change in lifecycle journal; expect `host_reboot` reason
- `08_ws_disconnect.sh` — break primary's egress to fstream.binance.com; expect `ws_disconnect` gap; FirstFrameWatchdog kicks in
- `09_snapshot_poll_miss.sh` — block primary's egress to fapi.binance.com; expect `snapshot_poll_miss` gap on retry exhaustion
- `10_planned_collector_restart.sh` — `mark_maintenance` + clean stop + start; expect `restart_gap` planned=true (not flagged as unplanned)
- `11_corrupt_message.sh` — produce a malformed envelope to a topic; expect `deserialization_error` gap; consumer keeps running
- `12_pg_kill_during_commit.sh` — pause postgres mid-commit; expect `pg_outage_hold` gap; on recovery, resume cleanly
- `13_rapid_restart_storm.sh` — restart primary 5 times in 30s; expect 5 `restart_gap` envelopes, all valid; verify ERRORS=0
- `14_pg_outage_then_crash.sh` — kill postgres + writer simultaneously; on restart expect `pg_outage_hold` + `writer_restart` correctly classified
- `15_redpanda_leader_change.sh` — simulate via brief redpanda restart (single-broker stack acts like leader change); expect transient outage covered by gap envelopes
- `16_collector_failover_to_backup.sh` — kill primary; verify backup is consumed instead; archive contains backup data; CoverageFilter records primary's gap envelopes correctly

Each is a separate file to keep them small + parallelizable. Commit: `test(chaos): add scenarios 01-16 — port from Python suite`

#### Task B2.2: NEW chaos scenarios (per spec §9.3)

Scenarios that test the NEW components from Track A. These will FAIL until Track A lands.

- `17_kafka_producer_outage.sh` — block primary's egress to redpanda for 60s; expect `kafka_producer_outage` gap on recovery (depends on `KafkaProducerHealthMonitor` + `KafkaOutageJournal`)
- `18_kafka_consumer_outage.sh` — block writer's ingress from redpanda for 60s; expect `kafka_consumer_outage` gap (depends on `KafkaConsumerOutageDetector`)
- `19_kafka_offset_reset.sh` — delete + recreate a topic during writer running; expect `kafka_offset_reset` gap (depends on Task A3.5)
- `20_cross_source_pu_chain_break.sh` — kill primary at depth `u=N`, ensure backup didn't have `u=N+1..N+M`, restart primary; verify `cross_source_pu_chain_break` gap covers the missing range (depends on `CrossSourcePuChainValidator`)
- `21_disk_full_hold.sh` — fill disk; verify `disk_full_hold` gap; clean up; verify recovery (depends on `DiskFullHoldController`)
- `22_both_collectors_silent.sh` — `iptables block fstream.binance.com` for both collectors for 30s; expect `both_collectors_silent` gap (depends on `SilenceInferredGapEmitter`)
- `23_kafka_full_outage.sh` — `docker compose stop redpanda`; let collectors accumulate `KafkaOutageJournal` entries; restart redpanda; verify `kafka_producer_outage` gap envelope is replayed and arrives at writer

Commit: `test(chaos): add scenarios 17-23 — new components from gap-detection design`

### Phase B3 — JUnit integration runner

#### Task B3.1: integration runner that invokes Python verify

**Files:**
- Create: `consolidation/src/test/java/com/cryptolake/consolidation/ChaosVerifyIT.java`

JUnit 5 test class with `@TestFactory` that discovers each `tests/chaos/NN_*.sh`, executes it via `ProcessBuilder`, and asserts exit==0. Lets `./gradlew :consolidation:test --tests "*ChaosVerifyIT*"` run the full chaos suite.

Commit: `test(chaos): JUnit integration harness invoking shell scenarios`

---

## Integration step (after both tracks complete)

1. On `main`, with all Track A commits + all Track B commits landed, run `bash scripts/run-chaos-tests.sh`.
2. Expected: all 23 scenarios pass.
3. If any fail: triage. Either (a) the scenario was wrong (fix scenario) or (b) the implementation was wrong (fix code).
4. Commit any fixes. Re-run.
5. Final: commit a summary `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-status.md` documenting which scenarios pass + the verify ERRORS=0 result.

---

## Self-review

- ✅ Spec coverage: every spec §5 failure mode has a chaos test in §B2 OR is explicitly documented as out-of-scope (§5.5 verify-time only, §5.6 out-of-domain).
- ✅ Spec §6 components: every new class has a Track A task.
- ✅ Spec §6 gap reasons: every new reason is added in Task A1.1.
- ✅ Spec §10 non-coverages: nothing implemented for them (correct).
- ✅ Each task has exact files, exact tests, exact commit messages.
- ✅ No "TBD" or "TODO" placeholders.

---

## Execution

Per the user's directive ("implement chaos tests and in parallel - review and fix the code"), the two tracks dispatch as parallel subagents via `superpowers:dispatching-parallel-agents`:

- Subagent 1 — Track A (sonnet, general-purpose)
- Subagent 2 — Track B (sonnet, general-purpose)

After both return, run the integration step inline.
