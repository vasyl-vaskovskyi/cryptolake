# CryptoLake Chaos Test Suite

This directory contains the bash-driven chaos scenarios for the gap-detection
and redundancy implementation. Each scenario injects a specific failure into an
isolated docker-compose stack and then asserts that `cryptolake verify` exits 0
with ERRORS=0 and the expected gap envelope is present in the archives.

## Quick start

### Run a single scenario

```bash
# From the repository root
bash tests/chaos/01_collector_unclean_exit.sh
```

Each scenario exits 0 on pass, non-zero on fail, and prints `RESULT: PASS` or
`RESULT: FAIL` to stderr.

### Run all scenarios

```bash
bash scripts/run-chaos-tests.sh
```

The runner executes each `NN_*.sh` in lexicographic order, captures stdout+stderr
per scenario into `build/chaos-logs/NN_<name>.log`, and prints a final summary
table.

### Run the JUnit harness (discovers all retained scenarios)

```bash
./gradlew :consolidation:test --tests "*ChaosVerifyIT*"
```

This runs each `tests/chaos/NN_*.sh` via `ProcessBuilder` and asserts exit 0.

## Prerequisites

| Tool | Why needed |
|------|-----------|
| `docker compose` v2 | Stack management |
| `zstd` | Archive decompression in assertions |
| `python3` | JSON parsing in assertion helpers |
| Java 21 (via Gradle wrapper) | Building + running verify CLI |

The verify CLI is built automatically on first use by `common.sh` if
`verify/build/install/verify/bin/verify` does not exist.

## Isolation model

Each scenario uses:

- A unique compose project name: `cryptolake-chaos-NN`
- A unique data directory: `/tmp/cryptolake-chaos-NN-data`
- No shared state between scenarios

Cleanup (`docker compose down -v --remove-orphans` + `rm -rf /tmp/...`) is
registered via `trap EXIT` in `common.sh` so a scenario failing with `set -e`
still removes its containers and volumes.

## Scenario index

| # | Name | What it tests | Expected gap reason |
|---|------|--------------|---------------------|
| 01 | collector_unclean_exit       | SIGKILL primary mid-flight; backup keeps flowing                                       | `collector_restart` (only on polled streams; high-freq fully covered) |
| 02 | fill_disk                    | Fill chaosfs NFS-sidecar tmpfs (300 MiB cap, overshoot 320 MiB); hold 120s; free       | `disk_full_hold` (controller-emitted hold-window markers at entry/exit — NOT data loss; records replay from Kafka). Writer enters `WRITER_DISK_FULL_HOLD_ENTERED`+`WRITER_KAFKA_CONSUMPTION_PAUSED`, exits cleanly. Runs by default — chaosfs sidecar (digest-pinned `itsthenetwork/nfs-server-alpine`) replaces the host-disk fill that previously SKIPed on dev machines |
| 03 | depth_reconnect_inflight     | Drop primary egress for 45s; primary self-heals via snapshot resync                    | `snapshot_poll_miss` (only on the 30s-polled depth_snapshot stream; live diffs fully covered) |
| 04 | full_stack_restart_gap       | Down + restart entire stack after 60s                                                  | `restart_gap` parked then suppressed by mutual coverage; only `pu_chain_break` may survive (real loss case for "both silent" lives in test 15) |
| 05 | planned_collector_restart    | Clean stop + start primary with maintenance marker                                     | NO gap; full failover → recovery lifecycle |
| 06 | corrupt_message              | Produce 3 malformed envelopes to a writer-consumed topic                               | NO gap; writer logs `corrupt_message_skipped` ERROR per record and continues |
| 07 | pg_kill_during_commit        | `dc kill postgres` for 180s; restart                                                   | `pg_save_failed` log; gap-set ⊆ {`pg_outage_hold`, `collector_restart`}. `WRITER_PG_OUTAGE_HOLD_ENTERED` is wired but rarely fires in chaos timing because `StateManager.retry`'s 3-attempt internal retry absorbs failures before the 3-failure threshold trips at the OffsetCommitCoordinator level — unit tests cover the threshold semantics directly |
| 08 | rapid_restart_storm          | Restart primary 5× with 8s down per cycle (each above 5s silence threshold)            | `collector_restart` only on polled streams; ≥3 of 5 failover round-trips fire |
| 10 | redpanda_leader_change       | Stop redpanda for 45s, restart                                                         | NO uncovered gap; producer health-monitor stays HEALTHY for brief outages (sustained-outage path is test 16) |
| 11 | kafka_producer_outage        | Network-isolate primary collector for 60s                                              | gap-set ⊆ {`collector_restart`, `pu_chain_break`, `snapshot_poll_miss`}; writer's silence-based failover takes over (sustained-outage path is test 16) |
| 12 | kafka_consumer_outage        | Network-isolate writer from redpanda for 60s                                           | NO gap; consumer catches up from last committed offset (Kafka 48h retention covers); `KafkaConsumerOutageDetector` exists but is dead code in current build |
| 13 | kafka_offset_reset           | Delete + recreate `binance.bookticker` mid-run                                         | NO `kafka_offset_reset` gap; auto.offset.reset=earliest silently resets the consumer (the gap-emit path requires auto.offset.reset=none which prod doesn't use) |
| 15 | both_collectors_silent       | Block both collectors' egress for 60s                                                  | `snapshot_poll_miss` on polled streams + `pu_chain_break` on depth (correct GAP_ACCEPTED_NO_COVERAGE); `both_collectors_silent` reason currently UNREACHABLE — `SilenceInferredGapEmitter` is dead code in current build (silent loss on continuous WS streams) |
| 16 | kafka_full_outage            | Stop redpanda for 180s (>delivery.timeout.ms)                                          | `kafka_producer_outage` + `kafka_delivery_failed` archived; both collectors transition through COLLECTOR_KAFKA_OUTAGE_ENTERED/EXITED via journal replay |

### Previously-numbered scenarios that no longer exist

The suite was first compacted from a sparse 1..23 numbering down to 1..16,
then further reduced to 14 retained tests. Removals are preserved here so
anyone re-discovering those code paths knows the history:

#### Removed during the original compaction (1..23 → 1..16)

- **host_reboot_restart_gap** — same chaos as the current full_stack_restart_gap
  (test 04). Its boot_id injection had no effect: it wrote to the per-collector
  `lifecycle.jsonl`, but host_reboot classification reads
  `/proc/sys/kernel/random/boot_id` (overridable only via
  `CRYPTOLAKE_TEST_BOOT_ID`, which isn't propagated through compose). The
  host_reboot path itself is unit-tested in
  `writer/.../RestartGapClassifierTest.java`.
- **ws_disconnect** — duplicate chaos with depth_reconnect_inflight (test 03)
  and asserted a failover that never fires. Egress block cuts the upstream WS
  but leaves the collector's Kafka producer alive, so primary keeps
  publishing heartbeats/state to Kafka and the writer's `MAIN_FAILURE_DETECTED`
  (5s topic silence) never trips. The "primary dies, backup covers" failover
  path is reliably exercised by test 01 via SIGKILL.
- **snapshot_poll_miss** — misnamed; its chaos was identical to
  both_collectors_silent (test 15). The `snapshot_poll_miss` reason on polled
  streams is observed and accepted in tests 01 and 03 where it actually arises.
- **collector_failover_to_backup** — same chaos as collector_unclean_exit
  (test 01) with only the downtime length differing. Its strict gaps=∅
  assertion happened to pass when backup's polled-stream samples aligned
  with the gap-decision moment, but was structurally flaky for the same
  OI / depth_snapshot poll-cadence reasons test 01 handles correctly.
- **cross_source_pu_chain_break** — `CrossSourcePuChainValidator` is referenced
  in `RecordHandler` but `setCrossSourceValidator()` is never called from
  `writer/Main.java`, so the validator stays null in the current build and
  `cross_source_pu_chain_break` cannot be emitted under any chaos. The chaos
  itself (SIGKILL primary, backup covers, restart) also duplicated test 01.

#### Removed during the second compaction (1..16 → current 14 retained)

- **09_both_collectors_kill** — SIGKILL both collectors simultaneously for 30 s,
  then restart both together. Empirically the writer's CoverageFilter sees both
  new sessions as "currently fresh" on resume and suppresses the gap — the
  same outcome as test 04 (full-stack restart). Test 04's header explicitly
  documents this and points at test 15 (`both_collectors_silent` /
  `SilenceInferredGapEmitter`) as the proper convergence path for "both silent
  during the gap window". Removed because test 09 cannot reliably trigger its
  expected `GAP_ACCEPTED_NO_COVERAGE` outcome and duplicates either test 04 or
  test 15 depending on how the result is read.
- **14_disk_full_hold** — the state-machine variant of test 02. Both verified
  the same `disk_full_hold` gap envelope shape. Test 14 still used
  `safe_disk_fill_or_skip` + host-side `dd`, which SKIPs whenever
  `HOST_DATA_DIR` is on the host's primary filesystem (i.e., always on dev
  machines). Test 02 was rewritten to use a chaosfs NFS sidecar with a 300 MiB
  tmpfs cap, runs by default everywhere, and asserts the same state-machine
  contract. Test 14 was strictly inferior and was removed.

## Writing a new scenario

1. Source `common.sh` at the top.
2. Call `init_scenario <NN> [primary|primary+backup]` — this sets project name,
   data dir, and registers `teardown_stack` on EXIT.
3. Call `start_stack`, `wait_healthy`, warm-up, inject chaos, wait, `capture_archives`.
4. Call `run_verify "$(today)" "$HOST_DATA_DIR"` and `assert_gap_present <reason> "$HOST_DATA_DIR"`.
5. Call `scenario_pass` or let a non-zero assertion trigger `set -e` → EXIT trap.

## common.sh helper reference

```bash
init_scenario <nn> [primary|primary+backup]
start_stack [mode]
wait_healthy [timeout_secs]
wait_data_flowing [stream] [timeout_secs]
wait_data_flowing_chaosfs [stream] [timeout_secs]   # for chaosfs-NFS scenarios (test 02)
warm_up [secs]
kill_service <svc>
clean_stop_service <svc>
start_service <svc>
block_egress_via_network <svc>
restore_egress_via_network <svc>
block_service_network <svc>        # full isolation
restore_service_network <svc>      # restarts the service
capture_archives <dest_dir>
run_verify <date> <base_dir>       # asserts exit==0 AND ERRORS=0
assert_gap_present <reason> <dir>
assert_gap_absent  <reason> <dir>
wait_for_gap <reason> [timeout_secs]
fill_disk [dir] [percent]          # legacy host-disk fill — see safe_disk_fill_or_skip
free_disk
fill_via_chaosfs <megabytes>       # fills the chaosfs NFS-sidecar tmpfs to ENOSPC
free_via_chaosfs                   # idempotent; safe in teardown
materialize_archive_to_host        # copies chaosfs:/exports → $HOST_DATA_DIR for host-side verify
teardown_stack                     # called by trap EXIT automatically
today                              # prints YYYY-MM-DD (UTC)
scenario_pass
scenario_fail <message>
```

### chaosfs sidecar (test 02 opt-in)

Scenarios that need a small, capped filesystem mediating writer's `/data`
opt in via two env vars set BEFORE `init_scenario`:

```bash
export CHAOS_EXTRA_COMPOSE_FILES="docker-compose.chaos-02-nfs.yml"
export CHAOS_EXTRA_SERVICES="chaosfs"
```

`init_scenario` consumes `CHAOS_EXTRA_COMPOSE_FILES` (colon-separated; appends
each as `--file` to `COMPOSE_OPTS`). `start_stack` consumes
`CHAOS_EXTRA_SERVICES` (space-separated; prepends to the services list so
extras come up before the writer's `depends_on`).

`teardown_stack` removes the `itsthenetwork/nfs-server-alpine` image (pinned
by sha256 digest in `docker-compose.chaos-02-nfs.yml`) at the end of each
scenario run so repeated runs are hermetic. Best-effort `docker rmi`; silent
if not present.

## Writer-side hold-controller wiring (relevant to tests 02 and 07)

`DiskFullHoldController` and `PgOutageHoldController` are wired in
`writer/.../OffsetCommitCoordinator.flushAndCommit`:

- Disk-full path: any `IOException` from `appendAndFsync` whose message
  contains `"No space left on device"` flips `holdActive` true, emits
  `LIFECYCLE WRITER_DISK_FULL_HOLD_ENTERED` + per-(symbol,stream)
  `disk_full_hold` gap envelopes, and the catch returns 0 cleanly so records
  remain in Kafka.
- PG-outage path: `CryptoLakeStateException` from
  `saveStatesAndCheckpoints` calls `pgHold.recordPgFailure()`; on the third
  consecutive failure `holdActive` flips and `WRITER_PG_OUTAGE_HOLD_ENTERED`
  fires. (In practice `StateManager.retry`'s 3-attempt internal retry absorbs
  most failures before they reach the OffsetCommitCoordinator catch — see
  test 07 header for why the threshold rarely trips in chaos timing.)
- While either hold is active, `KafkaConsumerLoop` calls
  `primary.pause(primary.assignment())` so records remain in Kafka rather
  than ballooning the unbounded `BufferManager`. `WRITER_KAFKA_CONSUMPTION_PAUSED`
  fires once per entry edge.
- Recovery probes run on independent virtual threads every 30 s. Disk probe
  is `Files.getFileStore(baseDir).getUsableSpace() > 50 MiB`; PG probe is
  `Connection.isValid(5)` via `StateManager.ping()`. On success, hold exits
  and `WRITER_KAFKA_CONSUMPTION_RESUMED` fires.

`ZstdTailScrubber` runs at writer startup (after sidecar repair, before the
consume loop spawns) and truncates any torn zstd-frame tails in
`*.jsonl.zst` files left by an unexpected exit (SIGKILL, OOM, segfault).
The writer container is capped at 768 M memory with `JAVA_TOOL_OPTIONS=-Xmx512m -Xms256m`
so memory regressions fail with a logged `OutOfMemoryError` rather than a
silent kernel SIGKILL.
