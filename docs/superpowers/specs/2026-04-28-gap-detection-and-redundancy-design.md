---
title: Gap detection and primary/backup redundancy — complete design
date: 2026-04-28
status: approved
supersedes:
  - 2026-03-29-gap-detection-and-backfill-design.md
  - 2026-04-03-backup-collector-design.md
  - 2026-04-06-writer-failover-design.md
  - 2026-04-08-gap-filter-via-backup-coverage-design.md
---

# Gap detection and primary/backup redundancy

## 1. Goal

> The system MUST never lose data silently. Every period during which expected
> data was not archived MUST be recorded as a gap envelope visible to the
> verify CLI.

This is the binding invariant. "Recorded as a gap" means a gap envelope with a
non-zero-width window, all required fields populated, sealed into the
`{exchange}/{symbol}/{stream}/{date}/hour-NN.jsonl.zst` archive (and reachable
via the verify CLI's coverage walk), OR detectable by the verify CLI from
heartbeat absence + lifecycle journal evidence.

## 2. Non-goals

- 100% data delivery. We accept losing data in genuinely impossible situations
  (Binance outage, host loss). The invariant is **observability of loss**, not
  prevention.
- Sub-second gap-window precision. The accepted detection floor is ~10s
  (heartbeat 5s × 2). Tightening would optimize latency, not completeness.
- Multi-region replication. Both collectors run on the same host (deployment
  shape **D**). Multi-region is documented as a future option but out of
  scope.
- Replacing Kafka or Postgres with alternative storage. They remain the
  in-flight transport (Kafka) and lifecycle state store (Postgres).

## 3. Architectural commitments

### 3.0 The TWO-COLLECTOR rule (load-bearing — read this first)

The system runs **two collectors at all times**: **MAIN** (also called
"primary" in some files) and **BACKUP**. They are not optional. They are
not active/passive. They are **both running 24/7**, both subscribed to the
same Binance streams independently, both publishing data to Kafka.

The runtime behavior is exactly four rules:

1. **MAIN healthy + BACKUP healthy** → writer archives MAIN's data,
   ignores BACKUP's data (already covered, dedup at the archive layer).
   No gap.
2. **MAIN fails (process crash, WS half-open, network blip, anything)**
   → writer immediately archives BACKUP's data instead.
   **No gap is recorded.** This is the redundancy mechanism doing its job.
3. **MAIN comes back (process restarts and resumes producing)** →
   writer continues with whichever source is fresher; once MAIN is
   delivering data again, writer prefers MAIN. **No gap.**
4. **MAIN fails AND BACKUP also fails for some sub-window** → writer
   has no data for that sub-window. **Only then is a gap envelope
   archived**, with `gap_start_ts` = last data from either source,
   `gap_end_ts` = first data from either source after recovery.

In plain language:

> **Gap envelopes are archived if and only if both collectors were
> simultaneously unable to deliver data to the writer.** Any other
> situation — MAIN flapping, BACKUP flapping, either restarting, either
> reconnecting WebSockets — is invisible in the archive.

The writer's `CoverageFilter` enforces this. Every gap signal (collector-
emitted or writer-emitted) is suppressed if **any** source has data
covering its window. Only signals describing a window neither source
covered survive to the archive.

### 3.1 Deployment shape: D — same-host hot redundancy

The two collectors run side-by-side in the same `docker-compose` stack:

- **MAIN** service name `collector` — `COLLECTOR_ID=binance-collector-01`,
  `TOPIC_PREFIX=""`, publishes to `binance.{stream}` topics.
- **BACKUP** service name `collector-backup` — `COLLECTOR_ID=binance-collector-backup`,
  `TOPIC_PREFIX="backup."`, publishes to `backup.binance.{stream}` topics.

Both run continuously. Both connect to Binance independently from the same
egress IP. The single writer instance subscribes to both topic families.
At any instant the writer treats MAIN's data as the canonical source and
falls through to BACKUP's data automatically when MAIN's is missing or
stale (per §3.0 rule 2).

This shape catches: MAIN process crash, container OOM, MAIN's WebSocket
half-open, per-stream subscription drop on MAIN, single-process deadlock
on MAIN, Kafka send failure on MAIN — all without recording a gap,
because BACKUP covers.

This shape does NOT catch: host reboot, full ISP outage, the shared egress
IP being throttled by Binance, NIC failure. Those affect both collectors
together — §3.0 rule 4 applies and a gap is recorded.

### 3.2 Redundancy mode: A — both running 24/7

No leader election. No failover state machine across collectors. MAIN and
BACKUP are independent, unaware of each other, and identically configured
except for `COLLECTOR_ID` and `TOPIC_PREFIX` (per §3.1). The writer's
`CoverageFilter` is the **only place** that knows about the two-source
relationship; it performs all source-selection and gap-suppression
decisions on the consumption side per the §3.0 rules.

When MAIN fails and BACKUP takes over (§3.0 rule 2), the writer does NOT
need to "switch" — it consumes both topic families continuously and
already has BACKUP's data buffered. When MAIN recovers (§3.0 rule 3),
the writer dedups and prefers MAIN's records again automatically. There is
no explicit failover/failback signal anywhere in the system — the
architecture makes the question moot.

### 3.3 Coverage cadence: G — 5s heartbeats, 10s detection floor

Each collector emits a heartbeat envelope per `(symbol, stream)` every 5
seconds regardless of data flow. The writer flags a true coverage gap when
both sources have `last_data_at_ns` older than 30s AND last heartbeat older
than 15s for that stream. Worst-case undetected silent-loss window: ~10s.

### 3.4 The single rule of gap emission (load-bearing)

> **A gap envelope is archived if and only if no source had data for the
> window `[gap_start_ts, gap_end_ts]`.**

Concretely:

1. Every "gap signal" — from a collector (`ws_disconnect`, `pu_chain_break`,
   `session_seq_skip`, `buffer_overflow`, `snapshot_poll_miss`,
   `kafka_delivery_failed`, `handler_error`) OR from the writer
   (`collector_restart`, `restart_gap`, `kafka_consumer_outage`,
   `kafka_producer_outage`, `pg_outage_hold`, `disk_full_hold`,
   `kafka_offset_reset`, `cross_source_pu_chain_break`,
   `both_collectors_silent`, `missing_hour`, `deserialization_error`,
   `write_error`) — flows through `CoverageFilter`.

2. `CoverageFilter` suppresses the gap if **any** source has data envelopes
   with `received_at ∈ [gap_start_ts, gap_end_ts]`, OR if a heartbeat from
   any source carries `last_data_at_ns ∈ [gap_start_ts, gap_end_ts]`.

3. Gaps that survive the filter are archived. Spurious gaps from "the
   redundancy mechanism doing its job" never reach the archive.

**Consequence for redundancy tests.** When primary dies and backup covers,
the writer must NOT archive a gap. The session change is not data loss; it
is the redundancy mechanism working as designed. A test that asserts
"gap envelope present" after such a scenario is asserting on cosmetics, not
on the binding invariant.

**Consequence for the binding invariant.** The chaos test acceptance is
reframed:

- **Pass:** `verify` reports `Errors: 0`. The archive contains all
  data that any source delivered for the window.
- **Pass with gap:** `verify` reports `Errors: 0` AND a gap envelope is
  present, **only when no source covered some sub-window of the chaos**.
  The gap's `[gap_start_ts, gap_end_ts]` accurately describes that
  uncovered window.
- **Fail:** `verify` reports any ERROR (duplicates, missing sidecars,
  pu-chain breaks not bridged by either source, missing required gap
  fields), OR a sub-window where neither source had data is not covered
  by an archived gap.

**Implementation.** Every emitter calls `CoverageFilter.maybeEmit(gap)`
which routes through the existing `_other_covers` check. Writer-emitted
gaps (collector_restart etc.) MUST go through the same path; today some
bypass it (the bug exposed by chaos scenario 01).

## 4. The four "rulers" of gap detection

Gap detection layers four independent mechanisms. Each catches a different
class of loss; together they form the audit trail required by §1.

| Ruler | Owner | Catches |
|---|---|---|
| 1. Binance native sequence (per-source) | Collector | Frames Binance dropped before reaching one collector — only valid for `depth`'s `pu`-chain |
| 2. Internal `session_seq` (per-source) | Writer | Frames lost between collector WS receive and Kafka delivery |
| 3. Heartbeat presence (per-source) | Writer | Collector silence (process death, hang, full-WS down) |
| **4. Binance native sequence (post-merge)** | Writer | Frames lost in the gap between primary's failure and backup's first delivery — **only valid for `depth`'s `pu`-chain** |

Streams without a reliable Binance-native sequence (`trades`, `bookticker`,
`funding_rate`, `liquidations`, `open_interest`, `depth_snapshot`) rely on
rulers 2 and 3 only. The deliberate choice not to apply timestamp-delta
heuristics to these streams is documented in §10.

## 5. Failure-mode taxonomy

The complete enumeration of failure classes the design covers, with the
specific mechanism that records each one as a gap.

### 5.1 Per-process / per-container (covered by current code)

| # | Failure | Detector | Gap reason |
|---|---|---|---|
| 1 | One collector's WS disconnect | `WebSocketSupervisor` exception path | `ws_disconnect` |
| 2 | One collector's WS half-open (ping fail) | `pingLoop` → `ws.abort()` (commit e241640) | `ws_disconnect` |
| 3 | One collector's SUBSCRIBE silent on subset | `FirstFrameWatchdog` 30s deadline | `ws_disconnect` |
| 4 | One collector's pu-chain breaks | `DepthGapDetector` | `pu_chain_break` |
| 5 | One collector's `session_seq` skips | `SessionSeqTracker` (writer-side) | `session_seq_skip` |
| 6 | One collector's per-stream buffer cap exceeded | `BackpressureGate` overflow window | `buffer_overflow` |
| 7 | One collector's snapshot poll fails N times | `SnapshotScheduler` / `OpenInterestPoller` | `snapshot_poll_miss` |
| 8 | One collector's Kafka delivery callback receives err | `KafkaProducerBridge` callback (commit 30348b2) | `kafka_delivery_failed` |
| 9 | One collector's stream handler raises on a frame | `_receive_loop` try/catch (commit 30348b2) | `handler_error` |
| 10 | One collector's process crashes | Writer sees `collector_session_id` change | `collector_restart` |
| 11 | One collector's process hangs | Docker healthcheck → restart → same as #10 | `collector_restart` |

### 5.2 Per-process — partial today, closed by this design

| # | Failure | Detector | Gap reason |
|---|---|---|---|
| 12 | Collector internal exception stops emission, WS still up | Heartbeats keep firing with `subscribed_silent`; writer's CoverageFilter switches to backup | covered iff backup healthy; both-silent case → see §5.4 #21 |

### 5.3 Writer-side

| # | Failure | Detector | Gap reason |
|---|---|---|---|
| 13 | Writer process crash mid-batch | `RestartGapClassifier` from PG `last_heartbeat_at` | `writer_restart`/`host` |
| 14 | Writer clean shutdown | `LifecycleStateManager.markCleanShutdown(planned=true)` | none (planned shutdowns aren't gaps) |
| 15 | Writer can't reach Kafka | **NEW**: `KafkaConsumerOutageDetector` — empty polls ≥30s while writer's lifecycle heartbeat keeps firing | **NEW** `kafka_consumer_outage` |
| 16 | Writer can't reach Postgres | **NEW**: `PgOutageHoldController` — pauses Kafka commits, keeps flushing archives + sidecars, emits hold lifecycle event | **NEW** `pg_outage_hold` |
| 17 | Writer's local disk fills | `FileChannel.write` ENOSPC → `write_error` gap | `write_error` |
| 18 | Writer's archive bit-rot | Verify CLI sidecar SHA-256 mismatch at read time | (verify-time only, no gap envelope — documented in §10) |

### 5.4 Both-collectors-fail (the dangerous classes for §1)

| # | Failure | Detector | Gap reason |
|---|---|---|---|
| 19 | Host reboot (planned/unplanned) | **NEW Java**: `LifecycleJournal` records process start/stop + `host_boot_id`; `RestartGapClassifier` reads it; replaces Python `host_lifecycle_agent.py` | `host_reboot` / `host_unclean_shutdown` |
| 20 | Host kernel panic / power loss | Same as #19, recognized by missing `clean_shutdown_marker` | `host_unclean_shutdown` |
| 21 | Both WS half-open simultaneously | Each collector's `FirstFrameWatchdog` fires within 30s; both emit `ws_disconnect`; CoverageFilter cannot suppress (no other source has data either) | `ws_disconnect` × 2 (de-duped to one merged window) |
| 22 | Both collectors crash simultaneously | Two `collector_restart` envelopes, dedup by `gap_start_ts` | `collector_restart` |
| 23 | Both collectors' Kafka producers blocked | **NEW**: `KafkaOutageJournal` — single-record on-disk durability per collector; on Kafka recovery, replays as one `kafka_producer_outage` gap envelope spanning the outage window | **NEW** `kafka_producer_outage` |
| 24 | PG down (writer + collectors) | Writer: covered by #16. Collectors: PG is best-effort; lifecycle heartbeat retried on recovery; no data path impact | (no gap; PG is metadata only for collectors) |
| 25 | Kafka cluster fully down | Same mechanism as #23 — KafkaOutageJournal records the meta-gap; on recovery the gap envelopes describing the outage are sent to Kafka before any other data resumes | `kafka_producer_outage` |
| 26 | Network partition isolating one collector | Affected collector: same as #23. Healthy collector: keeps emitting | `kafka_producer_outage` from affected source on recovery |
| 27 | Time skew between collectors | N/A under D (same host, shared clock); documented assumption | n/a |
| 28 | docker-compose down without `markCleanShutdown` | LifecycleJournal lacks the clean shutdown record; classifier treats as unclean | `unclean_shutdown` |
| 29 | Disk full prevents archive seal | `write_error` gap; **NEW**: `DiskFullHoldController` enters "writer paused" mode, pauses Kafka commits, retries, alerts | **NEW** `disk_full_hold` |
| 30 | Kafka offset reset (OUT_OF_RANGE on rebalance) | **NEW**: `ConsumerRebalanceListener` emits gap on `OUT_OF_RANGE` recovery | **NEW** `kafka_offset_reset` |

### 5.5 Storage / verify-time only (acknowledged, no real-time gap envelope)

| # | Failure | Detector | Action |
|---|---|---|---|
| 31 | Missing hour file | Verify CLI / consolidation `MissingHourGapFactory` | `missing_hour` gap (consolidation-time) |
| 32 | Truncated hour file | Verify CLI sidecar SHA mismatch | Verify ERROR; operator action (no real-time gap) |
| 33 | Missing/wrong sidecar | Verify CLI | Verify ERROR (sidecar emission fixed, commit 43d41c2) |

### 5.6 Out-of-scope (acknowledged)

| # | Failure | Mitigation |
|---|---|---|
| 34 | Binance API contract change | Logged as parse error; operator monitors |
| 35 | Binance global outage | Both heartbeats `subscribed_silent`; verify-time visible |
| 36 | DNS / TLS / cert expiry on fstream | TLS error → `ws_disconnect` → reconnect loop |
| 37 | Operator deletes archive files | Verify CLI manifest mismatch |

### 5.7 Cross-source — closed by this design

| # | Failure | Detector | Gap reason |
|---|---|---|---|
| **38 (was HOLE 9)** | Binance dropped depth diffs that fell in the gap between primary's last frame and backup's first frame on the merged stream | **NEW**: `CrossSourcePuChainValidator` — writer-side `(symbol)`-keyed merger that re-applies the pu-chain check on the dedup'd merged stream | **NEW** `cross_source_pu_chain_break` |

## 6. Component catalog

This section enumerates new and existing classes by module.

### 6.1 Common module additions

```
common/src/main/java/com/cryptolake/common/envelope/
  GapReasons.java           # extend VALID set with 6 new reasons
```

New gap reasons added to `GapReasons.VALID`:

- `kafka_consumer_outage`
- `kafka_producer_outage`
- `kafka_offset_reset`
- `pg_outage_hold`
- `disk_full_hold`
- `cross_source_pu_chain_break`

### 6.2 Collector module additions

```
collector/src/main/java/com/cryptolake/collector/
  durability/
    KafkaOutageJournal.java       # NEW — single-record on-disk durability for the meta-gap
    KafkaProducerHealthMonitor.java # NEW — 5s metadata probe; flips state on prolonged failure
    LifecycleJournal.java         # NEW — append-only process start/stop ledger; replaces Python agent
  config/
    BackupCollectorConfig.java    # NEW — env-var override of topicPrefix + collectorId
```

**`KafkaProducerHealthMonitor`** — final class. Virtual-thread loop probing
`producer.metrics()` or a sentinel-topic metadata fetch every 5 seconds. State
transitions:

- `healthy` → `paused`: ≥30s of unreachable broker. Writes `{outage_started_at_ns}`
  to `KafkaOutageJournal`. Logs `kafka_outage_started`. Continues to attempt
  `producer.send` (fail-fast, BufferExhaustedException not retried).
- `paused` → `healthy`: probe succeeds. Reads journal, computes
  `gap_end = time.time_ns()`, emits one `kafka_producer_outage` gap envelope
  spanning `[outage_started_at_ns, gap_end]` per `(symbol, stream)`. Truncates
  journal. Logs `kafka_outage_resolved`.

**`KafkaOutageJournal`** — final class. Path:
`/data/cryptolake/{collector_id}/kafka_outage.json`. Single-record JSON file,
`FileChannel.force(true)` on write. Survives container restart. On boot, if
file is non-empty, the collector knows a prior outage was unresolved and
emits the bridging gap envelope before any new data flows.

**`LifecycleJournal`** — final class. Path:
`/data/cryptolake/{collector_id}/lifecycle.jsonl`. Append-only JSONL.
Each line: `{ts_ns, event, host_boot_id, collector_session_id, planned?, maintenance_id?}`.
Events: `start`, `clean_shutdown`, (and on next-boot inference: `unclean_shutdown`).
Pruning: lines older than 7 days dropped on startup (matches Python agent
behavior). Read by writer's `RestartGapClassifier` for restart classification.

**`BackupCollectorConfig`** — record in common's `BinanceExchangeConfig`. Env
vars `COLLECTOR_ID` and `TOPIC_PREFIX` override the YAML values. Currently
the Java collector hardcodes `topicPrefix = ""`; this class allows the
backup collector to override to `backup.` and a different `collectorId`.
Closes the docker-compose comment in `docker-compose.yml`.

### 6.3 Writer module additions

```
writer/src/main/java/com/cryptolake/writer/
  durability/
    KafkaConsumerOutageDetector.java # NEW
    PgOutageHoldController.java      # NEW
    DiskFullHoldController.java      # NEW
    KafkaOffsetResetEmitter.java     # NEW (in ConsumerRebalanceListener)
    LifecycleJournalReader.java      # NEW — reads collector journals; replaces Python HostLifecycleReader
  validation/
    CrossSourcePuChainValidator.java # NEW
    SilenceInferredGapEmitter.java   # NEW — emits both_collectors_silent / collector_silent
```

**`KafkaConsumerOutageDetector`** — final class. Tracks `last_poll_with_records_at`.
A virtual-thread monitor wakes every 10s. If `now - last_poll_with_records_at > 30s`
AND lifecycle heartbeat has fired in the same window (writer is alive),
emits `kafka_consumer_outage` gap with `gap_start = last_poll_with_records_at`,
`gap_end = now`. Re-armed when poll returns records.

**`PgOutageHoldController`** — final class. Wraps `StateManager` PG calls.
On 3 consecutive failures (matches existing retry policy), enters
`pg_outage_hold` state:

- Stops `Consumer.commitAsync` calls (last-committed-offset stays put).
- Continues writing archives + sidecars (Tier 1 §4: archive durability is
  independent of PG availability).
- Emits a `pg_outage_hold` gap envelope per active `(symbol, stream)` for
  observability. Updates `received_at` once per minute while hold continues.
- Probes PG every 30s; on recovery, exits hold, resumes commits, emits a
  closing `pg_outage_hold` gap with `gap_end = recovery_time`.

**`DiskFullHoldController`** — final class. Triggered by `IOException` with
`ENOSPC` on archive writes. Same shape as `PgOutageHoldController`: pauses
Kafka commits, emits `disk_full_hold` gap, retries. Operator action expected.

**`CrossSourcePuChainValidator`** — final class. Per-`(exchange, symbol)`
state machine:

- Maintains `last_u` per symbol across primary + backup combined.
- For each incoming `depth` data envelope (from either source), parses
  `U`, `u`, `pu` from raw_text.
- If `pu != last_u` AND `last_u != null` AND no recent `pu_chain_break` from
  either source covers this position: emit
  `cross_source_pu_chain_break` gap with
  `detail="merged stream pu={pu} expected={last_u}, primary_last_u=...,
  backup_last_u=..."`, `gap_start = previous_received_at`, `gap_end = current_received_at`.
- Updates `last_u = u` on success.
- Coalesces with `pu_chain_break` from a single source (if primary already
  flagged, no duplicate cross-source flag for the same `u`-range).

This is the new ruler #4 from §4.

**`SilenceInferredGapEmitter`** — final class. The "synthetic from absence"
emitter. Virtual-thread monitor:

- Reads `CoverageFilter._global_max_received` and `_global_max_heartbeat`
  per source.
- If for any `(symbol, stream)`: BOTH sources have `last_data_at_ns` older
  than 30s AND last heartbeat older than 15s (per §3.3), emits
  `both_collectors_silent` gap envelope with the bounded window. (Liquidations
  are exempt — no data is normal there.)
- If only ONE source is silent past these thresholds and the other has fresh
  data, no gap is emitted (CoverageFilter has already swapped to the healthy
  source for archive writes).

**`LifecycleJournalReader`** — replaces Python `HostLifecycleReader`. Reads
each collector's `lifecycle.jsonl` from the shared host volume on writer
startup. Provides classifier with definitive `host_boot_id` evidence so
restart-gap classification doesn't depend on PG availability.

### 6.4 Verify CLI updates

```
verify/src/main/java/com/cryptolake/verify/
  validation/
    CrossSourcePuChainValidator.java  # SHARED with writer (move to common?)
    HeartbeatTimelineWalker.java      # NEW — at verify time, reads heartbeat envelopes from
                                      #       archives, flags any (symbol, stream) windows
                                      #       missing both data and heartbeat
```

Verify CLI inherits the same cross-source pu-chain logic for post-hoc
validation of consolidated daily archives. Heartbeat timeline walker
catches the case where the writer itself was down during the silence (so
neither source's silence triggered a synthetic gap at the time, but the
gap is still inferable post-hoc from archived heartbeats).

## 7. Data flow

### 7.1 Steady state (everything healthy)

```
[Binance] --WS--> primary collector --kafka--> binance.{stream}     ----+
                                                                        |
[Binance] --WS--> backup  collector --kafka--> backup.binance.{stream}--+--> writer
                                                                        |        |
                  primary heartbeat 5s -----> binance.{stream}     -----+        |
                  backup  heartbeat 5s -----> backup.binance.{stream}--+         v
                                                                          archive
                                                                            +
                                                                          sidecar
```

Writer's `RecordHandler` routes by `type` field:
- `data` → `CoverageFilter.handleData(source, env)` → `CrossSourcePuChainValidator` → archive
- `gap` → `CoverageFilter.handleGap(source, env)` → maybe-suppress → maybe-archive
- `heartbeat` → `CoverageFilter.handleHeartbeat(source, env)` → liveness-only, not archived as data but recorded for verify-time analysis

### 7.2 Primary collector silent, backup flowing

CoverageFilter sees `_global_max_received[backup] > _global_max_received[primary]`.
On primary's gap envelopes, `_other_covers(backup, ...)` returns true → suppress.
Archive contains backup's data. No alert.

### 7.3 Both collectors silent (Tier 5.4 #21)

CoverageFilter sees both stale. After 30s × 2 thresholds, `SilenceInferredGapEmitter`
emits a `both_collectors_silent` gap. If individual `ws_disconnect` gaps
also arrive (because the FirstFrameWatchdog fired), CoverageFilter dedups
by `gap_start_ts` so the merged window is one envelope.

### 7.4 Kafka producer outage (Tier 5.4 #23)

```
collector --send fails 30s--> KafkaProducerHealthMonitor: paused
                              KafkaOutageJournal.write({outage_started_at_ns})
                              fsync
                              ↓
                              [waits — can't send anything to Kafka]
                              ↓
                              Kafka recovers
                              ↓
                              KafkaOutageJournal.read() → outage_start
                              emit gap_envelope(reason=kafka_producer_outage,
                                                gap_start=outage_start, gap_end=now)
                              KafkaOutageJournal.truncate()
                              resume normal send
```

### 7.5 Writer Kafka consumer outage (HOLE 1)

```
writer poll loop: empty polls, last data 30s ago
KafkaConsumerOutageDetector.check(): outage detected
  emit gap_envelope(reason=kafka_consumer_outage, gap_start=last_data, gap_end=now)
  log kafka_outage_detected
  continue polling
poll returns records: detector resets
```

### 7.6 Cross-source pu-chain break (Tier 5.7 #38)

```
primary depth diff stream:  ..., u=200, [primary dies], ...
backup depth diff stream:   ..., u=210 (pu=209), ...   ← backup itself is fine

writer merges into archive: u=199, u=200, [u=201..209 missing], u=210
CrossSourcePuChainValidator: previous u=200, current pu=209, expected=200 — break
  emit gap_envelope(reason=cross_source_pu_chain_break,
                    detail="primary stopped at u=200, backup resumed at u=210, missing 9 diffs",
                    gap_start=primary_last_received_at,
                    gap_end=backup_first_received_at)
```

## 8. State machines

### 8.1 KafkaProducerHealthMonitor (collector)

```
healthy ── send fails N times ──> degraded ── still failing 30s ──> paused
   ↑                                  │                                │
   └──── send succeeds ───────────────┘                                │
                                                                       │
   ↑──────────────── probe succeeds ──────────────────────────────────┘
   (emits kafka_producer_outage gap on transition)
```

### 8.2 PgOutageHoldController (writer)

```
healthy ── PG fail × 3 ──> hold (commits paused, archives flushing) ── PG recovers ──> healthy
                              │                                                          ↑
                              ├── emit pg_outage_hold (open) ─────────────────────────── │
                              ├── retry every 30s ────────────────────────────────────── │
                              └── emit pg_outage_hold (close, gap_end=recovery_time) ─── ┘
```

### 8.3 SilenceInferredGapEmitter (writer)

```
per (symbol, stream):
  monitoring ── BOTH sources stale (data >30s, hb >15s) ──> emit_pending
                                                              │
                                                              ▼
  ─── any source returns data ──────────────────  emit_pending: emit both_collectors_silent
                                                              │
                                                              ▼
  ─────────────────────────────────────────── return to monitoring
```

## 9. Test plan

### 9.1 Unit tests (deterministic, in-process)

Per new class, ≥3 tests:
- happy path
- transition path (healthy→degraded, hold→recovery, etc.)
- edge case (zero data ever seen, journal corrupt mid-write, restart mid-outage)

### 9.2 Integration tests

- Two collector instances against an in-memory broker (kafka-clients
  TestKit) producing to different topic prefixes; writer consumes both;
  assert `CoverageFilter` and `CrossSourcePuChainValidator` behave correctly
  under: primary dies, backup dies, both die, network partition between
  one collector and broker.

### 9.3 Chaos tests (the next phase after this design is approved)

The full chaos suite will be planned in a separate document
(`docs/superpowers/plans/...-chaos-tests-plan.md`). Anticipated scenarios
(one per Tier 5.4 row plus selected from 5.1–5.3):

- one collector unclean exit
- both collectors unclean exit simultaneously
- buffer overflow recovery
- writer crash before commit
- fill disk
- depth reconnect inflight
- full stack restart gap
- host reboot restart gap
- ws disconnect (single + simultaneous)
- snapshot poll miss
- planned collector restart
- corrupt message in topic
- pg kill during commit
- rapid restart storm
- pg outage then crash
- kafka producer outage (with KafkaOutageJournal replay)
- kafka consumer outage
- kafka offset reset
- cross-source pu-chain break (synthetic — kill primary at u=N while backup is at u=N-50; resume backup; verify cross-source break is emitted)
- disk full hold (with eventual recovery)

Each chaos test asserts the SAME invariant: after the chaos and after the
system stabilizes, `cryptolake verify` reports zero ERRORS, and any data
unavailability is explained by exactly one gap envelope (or the absence of
a heartbeat covered by `HeartbeatTimelineWalker`).

### 9.4 Gate-level integration (post-port re-verify)

Re-run the end-to-end Java stack test from this session (commits e241640
through 3f71bc9): bring up `docker compose up`, capture for 10 minutes,
run Python `cryptolake verify` against archives, confirm ERRORS=0. Re-run
with new gap reasons emitted by inducing each failure class.

## 10. Deliberate non-coverages

### 10.1 No timestamp-delta gap heuristic

We deliberately do NOT emit gap envelopes from "the gap between two
consecutive `received_at` values exceeded N seconds". Reason: stream cadences
vary 4 orders of magnitude (bookticker ~150/s, liquidations 0–N/hour). A
universal threshold false-positives on quiet streams; per-stream calibrated
thresholds duplicate what session_seq + heartbeats already detect. Document
this explicitly so future contributors don't propose adding it again.

### 10.2 No verify-time auto-remediation

Verify CLI catches storage-side corruption (bit rot, truncated files, missing
sidecars) but does not auto-emit gap envelopes for these. They are reported
as ERRORS for operator action, not gaps. Adding auto-remediation would conflate
"data was never received" (real gap) with "data was received but disk failed
to keep it" (operational issue). Keep them separate.

### 10.3 No multi-region failover

D is the chosen deployment shape. F (multi-region) is documented as a future
possibility. The current design's invariants (single shared clock, single
shared egress IP, single shared volume) are mathematically incompatible with
F without additional design work — explicitly noted so any future move to F
triggers a new spec.

### 10.4 Aggregate trade `a` and bookticker `u` continuity

Despite both fields being monotonic per-symbol, we do NOT validate their
continuity. Binance documentation explicitly notes both can skip values for
internal reasons. Validating them would generate false-positive gaps. They
remain useful for forensic ordering (verify CLI's duplicate-detection uses
them) but not for gap detection.

## 11. Migration / rollout

This design adds new classes and gap reasons; it does NOT modify the on-disk
archive format or break verify CLI compatibility for existing archives.

1. Land the 6 new gap reasons in `common/GapReasons.java` (additive).
2. Implement collector-side classes (`KafkaOutageJournal`, `KafkaProducerHealthMonitor`, `LifecycleJournal`, `BackupCollectorConfig`).
3. Implement writer-side classes (the four hold controllers + `CrossSourcePuChainValidator` + `SilenceInferredGapEmitter` + `LifecycleJournalReader`).
4. Re-enable `collector-backup` service in `docker-compose.yml` (uncomment).
5. Update verify CLI with `HeartbeatTimelineWalker` and the cross-source check.
6. Retire `scripts/host_lifecycle_agent.py` (Python) — the Java
   `LifecycleJournal` replaces it.
7. Plan + implement the chaos test suite (separate plan doc).
8. Re-run end-to-end re-verify (this session's procedure) under chaos
   conditions; confirm `verify` ERRORS=0 in every chaos scenario.

## 12. Open questions

1. **Is `CrossSourcePuChainValidator` shared by writer and verify CLI?** Recommended yes — promote to a `common/validation/` package. Avoids drift.
2. **Heartbeat archive policy.** Today heartbeats are NOT archived (writer routes them to CoverageFilter only). For `HeartbeatTimelineWalker` to work post-hoc, heartbeats DO need to be archived in a parallel topic or a separate JSONL stream. **Decision needed:** archive all heartbeats, or sample 1-in-N to control storage growth?
3. **`KafkaOutageJournal` location.** `/data/cryptolake/{collector_id}/kafka_outage.json` requires the host data dir to be writable from inside the container. Confirm this works with the existing `HOST_DATA_DIR` mount.
4. **Should `pg_outage_hold` and `disk_full_hold` block ALL data writes or only commits?** Per §6.3 they only pause Kafka commits and let archives flush. Confirm this is the desired semantic; the alternative is a full halt (safer but loses data the producer-side could have sent).

## 13. Acceptance criteria

The design is "done" when:

- §3.4 is honored: every gap-emitter routes through `CoverageFilter`. Spurious gaps from "the redundancy mechanism working" are never archived.
- All 38 failure modes in §5 either map to a documented gap reason or an explicit non-coverage decision in §10.
- Every chaos scenario asserts the §3.4 acceptance bar: `verify Errors: 0`, and a gap envelope is present **only** when no source covered some sub-window.
- The Java port retains its 7-gate cleanliness (no architect re-rejection from the changes).
