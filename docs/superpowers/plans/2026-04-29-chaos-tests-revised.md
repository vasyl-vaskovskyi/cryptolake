# Chaos test list — revised under the TWO-COLLECTOR rule

> Spec: `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md` §3.0–§3.4
> Status: revised 2026-04-29 to make the MAIN+BACKUP failover loop explicit

## 1. The TWO-COLLECTOR model (read this first)

The system runs **two collectors at all times**, side-by-side in the same
`docker-compose` stack, both subscribed to the same Binance streams,
both publishing to Kafka:

- **MAIN** (also called *primary* in code: service `collector`,
  `COLLECTOR_ID=binance-collector-01`, no topic prefix)
- **BACKUP** (service `collector-backup`,
  `COLLECTOR_ID=binance-collector-backup`, `TOPIC_PREFIX=backup.`)

They are **not** active/passive. They are **both running 24/7**.
The writer is the single consumer that decides, second-by-second, which
of the two streams to archive.

### 1.1 The failover-and-recovery loop

```
            ┌─────────────────────────────────────────────────────────┐
            │                                                         │
            │   MAIN healthy        BACKUP healthy                    │
            │   (writer archives MAIN's data; BACKUP data deduped)    │
            │                                                         │
            │                MAIN fails ↓        ↑ MAIN recovers      │
            │                                                         │
            │   MAIN failing       BACKUP healthy                     │
            │   (writer archives BACKUP's data — NO GAP)              │
            │                                                         │
            └─────────────────────────────────────────────────────────┘

                                Both fail ↓        ↑ either recovers

                          MAIN failing  + BACKUP failing
                          (no source provides data — GAP archived
                           for the exact sub-window neither covered)
```

### 1.2 The single rule of gap emission

> **A gap envelope is archived if and only if BOTH collectors were
> simultaneously unable to deliver data to the writer for that sub-window.**

That is the only situation that produces a gap. Every other situation —
MAIN dying, MAIN restarting, BACKUP flapping, MAIN's WebSocket
reconnecting, MAIN's Kafka producer being blocked — is invisible in
the archive, because BACKUP keeps delivering and the writer keeps
archiving without interruption. The writer's `CoverageFilter`
enforces the rule on every gap signal: any signal whose window is
covered by *either* source is suppressed before it reaches the archive.

### 1.3 Recovery: writer switches back to MAIN automatically

When MAIN comes back (process restarts, WS reconnects, Kafka producer
unblocks) and starts delivering data again, the writer's failover
controller observes MAIN's data is now flowing and switches the active
source back to MAIN. BACKUP keeps running in parallel, ready for the
next failure. The handoff is zero-loss: the writer only switches at
record boundaries where both sources agree on the position.

### 1.4 Acceptance bar for every chaos scenario

Every scenario asserts both:

1. `verify Errors: 0` — no integrity errors in the archive.
2. The presence/absence of a gap envelope **matches the table below**.
   - **NO gap** → the redundancy mechanism worked: only one of the two
     collectors was affected, and the other kept the writer fed.
   - **gap (reason=…)** → both collectors were unable to deliver for
     a window, OR a writer-side failure prevented archiving entirely.
     The reason string must match the listed value.

A scenario that asserts the wrong outcome is itself wrong.

## 2. The 23 scenarios

| #  | Scenario name (doc)              | Filename                              | Chaos action                                                                         | Expected             | Why (under TWO-COLLECTOR rule)                                                                                          |
|----|----------------------------------|---------------------------------------|--------------------------------------------------------------------------------------|----------------------|--------------------------------------------------------------------------------------------------------------------------|
| 01 | main_unclean_exit                | `01_collector_unclean_exit.sh`        | SIGKILL MAIN; BACKUP keeps running; restart MAIN                                     | **NO gap**           | Only MAIN failed. BACKUP delivers throughout. Writer archives BACKUP, then switches back to MAIN on recovery.            |
| 02 | main_buffer_overflow             | `02_buffer_overflow_recovery.sh`      | Block MAIN's egress to Kafka 90s; MAIN's per-stream buffer overflows                 | **NO gap**           | Only MAIN's producer path is blocked. BACKUP's producer is unaffected; BACKUP feeds the writer. MAIN recovers when egress restored. |
| 03 | writer_crash                     | `03_writer_crash_before_commit.sh`    | SIGKILL writer mid-batch; restart writer                                             | **gap, reason=writer_restart** | Writer is the **only** writer. While dead, neither MAIN nor BACKUP can be archived. Real loss for the down window.       |
| 04 | writer_disk_full                 | `04_fill_disk.sh`                     | Fill `$HOST_DATA_DIR` to 99%; wait; free disk                                        | **gap, reason=disk_full_hold** | Writer enters disk-full hold and pauses commits. MAIN+BACKUP both deliver to Kafka, but writer cannot archive.            |
| 05 | main_depth_resync_inflight       | `05_depth_reconnect_inflight.sh`      | Drop MAIN's depth WS mid-flow; MAIN snapshots and resyncs                            | **NO gap**           | Only MAIN's depth stream broke. BACKUP's depth pu-chain bridges the missing diffs. Cross-source pu-chain validator confirms continuity. |
| 06 | full_stack_restart               | `06_full_stack_restart_gap.sh`        | `docker compose down` then `up`                                                      | **gap, reason=collector_restart OR unclean_shutdown** | Both MAIN and BACKUP off simultaneously. Writer also off. No source covered the window. Real loss.                       |
| 07 | host_reboot                      | `07_host_reboot_restart_gap.sh`       | Inject new `host_boot_id` into LifecycleJournal; restart full stack                  | **gap, reason=host_reboot OR host_unclean_shutdown** | All processes off; lifecycle journal proves the host reboot gap. Real loss.                                              |
| 08 | main_ws_disconnect               | `08_ws_disconnect.sh`                 | iptables-block MAIN's egress to `fstream.binance.com`                                | **NO gap**           | Only MAIN's WS is severed. BACKUP's WS stays up; BACKUP feeds the writer. MAIN reconnects when egress restored.          |
| 09 | both_ws_disconnect               | `09_snapshot_poll_miss.sh`            | iptables-block BOTH MAIN and BACKUP egress to `fstream.binance.com` for 60s          | **gap, reason=ws_disconnect OR both_collectors_silent** | Both WS severed. Neither collector receives data. Real loss for the 60s window.                                          |
| 10 | planned_main_restart             | `10_planned_collector_restart.sh`     | `mark_maintenance` + clean stop + start MAIN                                         | **NO gap**           | Planned shutdown of MAIN only. BACKUP covers throughout. MAIN rejoins after restart. Maintenance is not loss.            |
| 11 | corrupt_message                  | `11_corrupt_message.sh`               | Produce a malformed envelope to a topic the writer reads                             | **gap, reason=deserialization_error** | The single corrupt envelope is undecodable by the writer. That record is lost — even if BACKUP's parallel record is fine, the writer flags the loss. |
| 12 | pg_outage_writer_holds           | `12_pg_kill_during_commit.sh`         | `dc pause` postgres for 60s; `dc unpause` postgres                                   | **NO gap**           | Writer enters pg-outage hold; archives keep flushing; Kafka commits resume on PG up. MAIN+BACKUP both keep delivering. No data loss. |
| 13 | rapid_main_restart_storm         | `13_rapid_restart_storm.sh`           | Restart MAIN 5× in 30s (SIGKILL + restart each time)                                 | **NO gap**           | MAIN flaps repeatedly; BACKUP delivers continuously through every blip. Writer never lacks a source.                     |
| 14 | both_collectors_kill             | `14_pg_outage_then_crash.sh`          | SIGKILL MAIN AND BACKUP simultaneously; sleep 30s; restart both                      | **gap, reason=collector_restart** | Both collectors dead at once. No source covered the 30s window. Real loss.                                               |
| 15 | redpanda_brief_restart           | `15_redpanda_leader_change.sh`        | `docker compose restart redpanda`                                                    | **gap, reason=kafka_producer_outage** (transient) | Both producer paths blocked simultaneously. May or may not produce a gap depending on outage length vs `linger.ms`.       |
| 16 | main_failover_to_backup          | `16_collector_failover_to_backup.sh`  | SIGKILL MAIN; observe writer consume BACKUP; restart MAIN                            | **NO gap**           | This **is** the failover working. BACKUP covers; writer switches sources transparently. Not a loss event.                |
| 17 | main_kafka_producer_outage       | `17_kafka_producer_outage.sh`         | iptables-block MAIN→redpanda for 60s                                                 | **NO gap**           | Only MAIN's producer path blocked. BACKUP's producer is unaffected; BACKUP feeds writer. MAIN recovers on unblock.       |
| 18 | writer_kafka_consumer_outage     | `18_kafka_consumer_outage.sh`         | iptables-block writer↔redpanda for 60s                                               | **gap, reason=kafka_consumer_outage** | Writer is the **only** consumer. While blocked, neither MAIN's nor BACKUP's records reach the archive. Real loss.        |
| 19 | writer_kafka_offset_reset        | `19_kafka_offset_reset.sh`            | Force OUT_OF_RANGE on writer's consumer (delete + recreate a topic)                  | **gap, reason=kafka_offset_reset** | Range of offsets that existed on the topic is unrecoverable. No source can replay them. Real loss.                       |
| 20 | cross_source_pu_chain_break      | `20_cross_source_pu_chain_break.sh`   | Kill MAIN at depth u=N; BACKUP's last-seen u was N-50; restart MAIN                  | **gap, reason=cross_source_pu_chain_break** | By construction, neither MAIN nor BACKUP delivered u=N-49…u=N-1. CrossSourcePuChainValidator catches the joint hole.    |
| 21 | writer_disk_full_hold            | `21_disk_full_hold.sh`                | Fill disk to 99%; wait for gap to be emitted; free disk (state-machine variant of #04) | **gap, reason=disk_full_hold** | Same root cause as #04; this scenario asserts the disk-full state-machine emits the gap envelope shape correctly.        |
| 22 | both_collectors_silent_inferred  | `22_both_collectors_silent.sh`        | iptables-block `fstream.binance.com` for BOTH collectors while heartbeats keep firing | **gap, reason=both_collectors_silent** | SilenceInferredGapEmitter sees both sources stale via heartbeat absence. Neither delivered data. Real loss.              |
| 23 | redpanda_full_outage_long        | `23_kafka_full_outage.sh`             | Stop redpanda; collectors accumulate KafkaOutageJournal entries; restart redpanda    | **gap, reason=kafka_producer_outage** | All Kafka traffic blocked for both producers. KafkaOutageJournal replays one gap envelope per affected stream on recovery. |

## 3. Summary by expected outcome

- **NO-gap scenarios (redundancy worked, MAIN failed alone OR BACKUP failed alone OR both healthy)**:
  01, 02, 05, 08, 10, 12, 13, 16, 17 — **9 scenarios**.
- **Gap-required scenarios (real loss: both collectors silent OR writer-side failure)**:
  03, 04, 06, 07, 09, 11, 14, 15, 18, 19, 20, 21, 22, 23 — **14 scenarios**.

The 9-vs-14 split is the empirical proof that the redundancy is doing
useful work: in 9 of 23 disruptions, the failure is invisible to the
archive. In the remaining 14, the failure was either truly unrecoverable
(both collectors blocked, or writer-side failure) or surfaced as a
correctly-shaped gap envelope.

## 4. Header format for every chaos script

Every `tests/chaos/NN_*.sh` opens with this fixed-shape header:

```bash
#!/usr/bin/env bash
# NN_<filename>.sh
#
# Scenario: <doc name from §2 above>
# Chaos:    <one-line action>
# Expected: NO gap (redundancy worked)  -- OR --  gap reason=<reason> (real loss)
# Flow:     <one-line description of MAIN/BACKUP behavior across the chaos>
# Why:      <one-line justification under the TWO-COLLECTOR rule>
```

`Flow` is the load-bearing addition: it states which collector(s) failed,
which kept delivering, and (for NO-gap scenarios) when MAIN switches back.
A reader skimming the header should be able to verify the expected outcome
without opening the spec.

## 5. Scenario-name vs filename note

A handful of filenames are legacy from the original Python suite and do
not match the doc scenario name (e.g. `09_snapshot_poll_miss.sh` actually
implements `both_ws_disconnect`). The bodies are correct; only the
filenames are stale. We keep the filenames as-is to avoid breaking
ChaosVerifyIT's parameterised discovery; the canonical scenario name is
recorded on the `Scenario:` line of every header.

## 6. Implementation tracking

- [x] Spec §3.0 documents the four-rule TWO-COLLECTOR model.
- [x] Spec §3.4 documents the single rule of gap emission.
- [x] Plan doc (this file) re-states both inline so it stands alone.
- [x] Acceptance table revised to reflect MAIN+BACKUP failover behavior.
- [ ] All 23 chaos script headers updated to the §4 fixed-shape format
      (with `Scenario:` and `Flow:` lines added).
- [ ] Iterate scenarios 01→23 to confirm each asserts the expected outcome
      against the actual archive output.
