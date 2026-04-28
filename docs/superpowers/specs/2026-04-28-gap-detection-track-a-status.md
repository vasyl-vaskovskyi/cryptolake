# Gap Detection Track A — Implementation Status

**Date:** 2026-04-28  
**Branch:** main  
**Final state:** All tasks complete, `./gradlew check` passes for entire repo (41 tasks, 0 failures).

---

## Task completion summary

### Phase A1 — Common module additions

| Task | Status | Commit SHA | Notes |
|------|--------|-----------|-------|
| A1.1 — Extend `GapReasons.VALID` with 7 new reasons | DONE | ba7bd07 | Added: `kafka_consumer_outage`, `kafka_producer_outage`, `kafka_offset_reset`, `pg_outage_hold`, `disk_full_hold`, `cross_source_pu_chain_break`, `both_collectors_silent` |
| A1.2 — `BackupCollectorConfig` env-var override | DONE | cf0cc30 | `COLLECTOR_ID` and `TOPIC_PREFIX` env vars map to `EXCHANGES__BINANCE__COLLECTOR_ID` and `EXCHANGES__BINANCE__TOPIC_PREFIX` via `YamlConfigLoader.load(Path)`; `BinanceExchangeConfig` gains `topicPrefix()` field; `collector/Main.java` updated to read `binance.topicPrefix()` instead of hardcoded `""` |

### Phase A2 — Collector module additions

| Task | Status | Commit SHA | Notes |
|------|--------|-----------|-------|
| A2.1 — `LifecycleJournal` | DONE | 83f3bac | Append-only JSONL at `${dataDir}/cryptolake/${collectorId}/lifecycle.jsonl`; `FileChannel` with `StandardOpenOption.SYNC` for fsync; 5 tests including corrupt-line drop, prune, round-trip |
| A2.2 — `KafkaOutageJournal` | DONE | 26a8916 | Single-record JSON at `${dataDir}/cryptolake/${collectorId}/kafka_outage.json`; `FileChannel.force(true)`; 5 tests |
| A2.3 — `KafkaProducerHealthMonitor` | DONE | 1074220 | Functional seam constructor (`BooleanSupplier`, `GapEmitAction`); `KafkaProducerBridge.probeHealth()` added; prior-outage-on-startup detection; 5 tests |
| A2.4 — Wire `LifecycleJournal` into Main; re-enable backup; retire Python | DONE | 85686c1 | `LifecycleJournal.recordStart()` on startup, `recordCleanShutdown()` on shutdown, `pruneOlderThan(7d)` on startup; `docker-compose.yml` `collector-backup` service uncommented with `COLLECTOR_ID=binance-collector-backup` and `TOPIC_PREFIX=backup.`; `HOST_DATA_DIR` volume mount added to both collector services; `scripts/host_lifecycle_agent.py` and `infra/aws/systemd/cryptolake-lifecycle-agent.service` deleted |

### Phase A3 — Writer module additions

| Task | Status | Commit SHA | Notes |
|------|--------|-----------|-------|
| A3.1 — `LifecycleJournalReader` + `RestartGapClassifier` integration | DONE | b37cd3a | Reads both primary + backup lifecycle.jsonl without cross-module dependency (reads raw JSON); `RecoveryCoordinator` gains 11-arg constructor with `journalEntries` supplement; `wasCleanShutdown()` used to strengthen `collectorCleanShutdown` evidence; 5 tests |
| A3.2 — `KafkaConsumerOutageDetector` | DONE | c8c6bad | Virtual-thread monitor; `BooleanSupplier` + `GapEmitAction` seams; `KafkaConsumerLoop` calls `recordPollWithRecords()` on non-empty poll; 5 tests |
| A3.3 — `PgOutageHoldController` | DONE | 013420c | 3-failure threshold; `isHoldActive()` called before Kafka commits; retry loop at 30s; 5 tests |
| A3.4 — `DiskFullHoldController` | DONE | 34e8d1f | ENOSPC detection via `IOException.getMessage()` contains "No space left on device"; `onWriteError(IOException)` / `onRecovery()` API; 5 tests |
| A3.5 — `KafkaOffsetResetEmitter` | DONE | 811e479 | `OffsetOutOfRangeException` caught in `KafkaConsumerLoop.run()`; `handleOffsetReset()` emits per-partition `kafka_offset_reset` gap; test validates reason string + exception API |
| A3.6 — `CrossSourcePuChainValidator` | DONE | 85a6a7d | In `common/src/main/java/com/cryptolake/common/validation/`; per-`(exchange,symbol)` state; `GapCallback` functional interface; `RecordHandler.setCrossSourceValidator()` setter; 7 tests |
| A3.7 — `SilenceInferredGapEmitter` | DONE | 2075e6b | Virtual-thread monitor; reads `CoverageFilter.getLastDataTs()` / `getLastHeartbeatTs()`; `CoverageFilter` gains `handleHeartbeat()`, `getLastDataTs()`, `getLastHeartbeatTs()`; `RecordHandler` calls `coverageFilter.handleHeartbeat(source)` on heartbeat envelopes; liquidations exempt; 6 tests |

### Phase A4 — Verify CLI updates

| Task | Status | Commit SHA | Notes |
|------|--------|-----------|-------|
| A4.1 — Integrate `CrossSourcePuChainValidator` into verify | DONE | 8361a21 | `VerifyCommand.runCrossSourcePuChainCheck()` sorts depth envelopes by `received_at` and runs the shared validator; reports as ERROR; 2 tests |
| A4.2 — `HeartbeatTimelineWalker` | DONE | 17b34f5 | In `verify/src/main/java/com/cryptolake/verify/validation/`; gracefully returns empty when heartbeat archives absent (not yet enabled); `SILENCE_THRESHOLD_NS=30s`; `VerifyCommand.runHeartbeatTimelineWalk()` groups envelopes by `(exchange,symbol,stream)` and calls walker; 4 tests |

### Phase A5 — Code audit

| Task | Status | Commit SHA | Notes |
|------|--------|-----------|-------|
| A5.1 — Audit spec §5 Tier 5.1, 5.2, 5.3 rows | DONE | 861bb03 | All invariants confirmed present: `WebSocketSupervisor.pingLoop` calls `ws.abort()` + `disconnectLatch.countDown()` (Bug A); `FileRotator.writeMissingSidecars` called on every shutdown path (Bug B); `RecordHandler.handle` peeks `type` field before deserialization (Bug C/D); `GapEnvelope.create()` populates all required fields; `CoverageFilter.handleGap` suppresses when other source has data within grace period; Spotless violations fixed |

---

## Deviations from plan

1. **Task A2.3 — `KafkaProducerHealthMonitor` constructor:** Plan specified the constructor accepting `KafkaProducerBridge` and `GapEmitter` directly, but both are non-subclassable (`final`) in the context they're used from tests. Implemented with a functional-seam primary constructor (`BooleanSupplier healthProbe`, `GapEmitAction`) and a `static KafkaProducerHealthMonitor.of(...)` factory for production wiring. Contract is identical; testability is improved.

2. **Task A3.6 — `LifecycleJournalReader` location:** The plan specified it in `writer/src/main/java/com/cryptolake/writer/durability/` but the `LifecycleEvent` record is in `collector`. To avoid a writer→collector cross-module dependency, `LifecycleJournalReader` reads the JSONL format directly (no import of `LifecycleEvent`), defining its own inner `JournalEntry` record. Semantically identical.

3. **Task A2.4 — docker-compose volume mounts:** The original commented-out `collector-backup` service lacked `HOST_DATA_DIR` volume mounts. Both primary and backup collector services received `${HOST_DATA_DIR:-/data}:${HOST_DATA_DIR:-/data}` volume and `HOST_DATA_DIR` env var to allow `LifecycleJournal` and `KafkaOutageJournal` to write durably.

4. **Task A4.2 — Heartbeat archival (§12 Q2):** The `HeartbeatTimelineWalker` is implemented and integrated, but heartbeat archival in the writer is NOT yet enabled (this requires new Kafka topics + writer subscription changes that are a separate scope). The walker gracefully returns zero errors when heartbeat archives are absent. A TODO note in the code indicates the heartbeat archival path for when it is enabled.

5. **Task A3.5 — `KafkaOffsetResetEmitter`:** The gap emitted uses `topic` as the `symbol` placeholder since `OffsetOutOfRangeException` carries only `TopicPartition` info. Real symbol/stream extraction would require a reverse-lookup map of topic → (symbol, stream), which can be added in a follow-up.

---

## Final state

- `./gradlew check` passes: 41 tasks, 0 failures
- All 18 Track A tasks complete and committed
- 19 commits on `main` branch spanning the full Track A implementation
