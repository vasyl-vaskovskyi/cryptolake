# Chaos test list — revised under the single rule of gap emission

> Spec: `docs/superpowers/specs/2026-04-28-gap-detection-and-redundancy-design.md` §3.4

The single rule: **a gap envelope is archived only when no source had data for the window**. Many of the original 23 scenarios were testing redundancy; their correct expected outcome is **NO gap** (redundancy worked). Only scenarios that genuinely deprive the writer of data from BOTH sources should produce a gap.

Acceptance bar for every scenario: `verify Errors: 0` AND a gap envelope appears **only** when there is a sub-window that no source covered.

| #  | Scenario                          | Chaos action (one line)                                                                  | Expected             | Why                                                                                                                            |
|----|-----------------------------------|------------------------------------------------------------------------------------------|----------------------|--------------------------------------------------------------------------------------------------------------------------------|
| 01 | primary_unclean_exit              | SIGKILL primary; backup keeps running                                                    | **NO gap**           | Backup covers the window. Redundancy worked.                                                                                   |
| 02 | primary_buffer_overflow           | Block primary's egress to Kafka; primary's per-stream buffer overflows                   | **NO gap**           | Backup's Kafka path is unaffected; backup data covers the window.                                                              |
| 03 | writer_crash                      | SIGKILL writer mid-batch; restart it                                                     | **gap (writer side)** | Writer is the only writer. While dead, no source could be archived. Real loss for the down window.                             |
| 04 | writer_disk_full                  | Fill `$HOST_DATA_DIR` to 99%; eventually free                                            | **gap**              | Writer enters `disk_full_hold` and pauses commits; archive frozen for the hold window.                                         |
| 05 | primary_depth_resync_inflight     | Drop primary's WS during depth flow; primary recovers via snapshot                       | **NO gap**           | Backup's depth pu-chain bridges the missing diffs. CrossSourcePuChainValidator confirms no merged-stream break.                |
| 06 | full_stack_restart                | `docker compose down`; `docker compose up`                                               | **gap**              | All sources off simultaneously. Real loss.                                                                                     |
| 07 | host_reboot                       | Simulate host_boot_id change in lifecycle journal                                        | **gap**              | All sources off. Real loss.                                                                                                    |
| 08 | primary_ws_disconnect             | Block primary's egress to fstream.binance.com                                            | **NO gap**           | Backup's WS still up; backup data covers.                                                                                      |
| 09 | both_ws_disconnect                | Block BOTH collectors' egress to fstream.binance.com                                     | **gap**              | Neither collector receives data. Real loss while both blocked.                                                                 |
| 10 | planned_collector_restart         | `mark_maintenance` + clean stop + start primary                                          | **NO gap**           | Backup covers; planned shutdown is not loss.                                                                                   |
| 11 | corrupt_message                   | Produce a malformed envelope to a topic the writer reads                                 | **gap**              | The corrupt message itself is undecodable; that record is lost. Writer emits `deserialization_error` gap for that envelope only. |
| 12 | pg_outage_writer_holds            | Pause postgres while writer running; resume                                              | **NO gap**           | Writer enters `pg_outage_hold`, keeps flushing archives, holds Kafka commits. No data loss.                                    |
| 13 | rapid_primary_restart_storm       | Restart primary 5× in 30s                                                                | **NO gap**           | Backup covers continuously through every primary blip.                                                                         |
| 14 | both_collectors_kill              | SIGKILL primary AND backup simultaneously; both restart                                  | **gap**              | No source covered the window. Real loss.                                                                                       |
| 15 | redpanda_brief_restart            | `docker compose restart redpanda`                                                        | **gap (transient)**  | Both producer paths blocked; writer consumer paused. Sources may emit `kafka_producer_outage` if outage ≥30s.                  |
| 16 | primary_failover_to_backup        | SIGKILL primary, observe writer consume backup; restart primary                           | **NO gap**           | Same as 01 from the writer's POV. The "failover" is the steady-state mechanism; it isn't a loss event.                         |
| 17 | primary_kafka_producer_outage     | Block primary→redpanda for 60s                                                           | **NO gap**           | Backup's producer path is unaffected; backup data covers.                                                                      |
| 18 | writer_kafka_consumer_outage      | Block writer←redpanda for 60s                                                            | **gap**              | Writer is the only consumer; while blocked nothing reaches archive. Real loss.                                                 |
| 19 | writer_kafka_offset_reset         | Force OUT_OF_RANGE on writer's consumer (delete + recreate a topic)                       | **gap**              | Some range of offsets that existed on the topic is unrecoverable. Real loss.                                                   |
| 20 | cross_source_pu_chain_break       | Kill primary at depth u=N; backup's last-seen u was N-50; restart primary                 | **gap**              | By construction, neither source had u=N-49…u=N-1. Real loss bridged by neither. CrossSourcePuChainValidator catches it.        |
| 21 | writer_disk_full_hold             | Same as #04 but emphasis on `disk_full_hold` gap shape                                    | **gap**              | (Duplicate of #04, kept for state-machine coverage). Same expected outcome.                                                    |
| 22 | both_collectors_silent_inferred   | iptables-block fstream.binance.com for both for 30s while heartbeats keep firing          | **gap**              | SilenceInferredGapEmitter sees BOTH sources stale and emits `both_collectors_silent`. Real loss.                                |
| 23 | redpanda_full_outage_long         | Stop redpanda; collectors accumulate KafkaOutageJournal entries; restart                  | **gap**              | All Kafka traffic blocked for both producers; KafkaOutageJournal replays one `kafka_producer_outage` envelope on recovery.     |

## Summary

- **NO-gap scenarios** (redundancy worked): 01, 02, 05, 08, 10, 12, 13, 16, 17 — **9 scenarios**
- **Gap-required scenarios** (real loss): 03, 04, 06, 07, 09, 11, 14, 15, 18, 19, 20, 21, 22, 23 — **14 scenarios**

A scenario that asserts the wrong outcome is itself wrong. Each scenario file's header now documents both the chaos and the expected outcome (no-gap or gap with reason).

## Drop / fold-in

- Original #15 (`redpanda_leader_change`) was a vague "transient outage" — replaced with the concrete `redpanda_brief_restart` (#15) and the more aggressive `redpanda_full_outage_long` (#23). Two distinct scenarios; the brief one is borderline (gap may or may not appear depending on `linger.ms` + restart speed) and is acceptable as `verify Errors: 0` plus optional gap.
- Original #21 was redundant with #04. Kept as a state-machine variant; same expected outcome.

## Implementation note

Scenario header comments must encode the spec rule. Format:

```bash
#!/usr/bin/env bash
# NN_<name>.sh
#
# Chaos:    <one-line action>
# Expected: NO gap (backup covered)  -- OR --  gap reason=<reason> (real loss)
# Why:      <one-line justification>
```

The `assert_gap_present` / `assert_gap_absent` calls in the body must match the header. The next implementation pass updates all 23 files to this shape.
