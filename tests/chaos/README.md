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

### Run the JUnit harness (discovers all 23 scenarios)

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
| 01 | collector_unclean_exit | Kill primary mid-flight; backup keeps flowing | `collector_restart` (only on polled streams; high-freq fully covered) |
| 04 | fill_disk | Fill HOST_DATA_DIR to 99%; hold 120s; free disk | NO gap (writer re-polls uncommitted offsets from Kafka) — **requires small dedicated fs at HOST_DATA_DIR; SKIPs otherwise** |
| 05 | depth_reconnect_inflight | Drop primary egress for 45s; primary self-heals via snapshot resync | `snapshot_poll_miss` (only on the 30s-polled depth_snapshot stream; live diffs fully covered) |
| 06 | full_stack_restart_gap | Down + restart entire stack after 60s | `restart_gap` parked then suppressed by mutual coverage; only `pu_chain_break` may survive (real loss case for "both silent" lives in test 22) |
<!-- 07 removed: identical chaos to 06 (full stack down + restart) and its boot_id
     injection had no effect — it wrote to the per-collector lifecycle.jsonl,
     but host_reboot classification reads /proc/sys/kernel/random/boot_id
     (overridable only via CRYPTOLAKE_TEST_BOOT_ID, which isn't propagated
     through compose). The host_reboot path itself is unit-tested in
     writer/.../RestartGapClassifierTest.java. -->
<!-- 08 removed: duplicate chaos with test 05 (block_egress_via_network "collector"
     for 45s) and asserted a failover that never fires. Egress block cuts the
     upstream WS but leaves the collector's Kafka producer alive — primary
     keeps publishing heartbeats/state to Kafka so the writer's
     MAIN_FAILURE_DETECTED (5s topic silence) never trips. The "primary
     dies, backup covers" failover path is reliably exercised by test 01
     via SIGKILL, which actually severs the producer. -->
<!-- 09 removed: misnamed — its chaos was identical to test 22 (block egress
     for BOTH collectors, not "primary REST endpoint"). Test 22 is the
     stricter version (adds expect_gap_present_check for
     both_collectors_silent and a narrower gap whitelist). The
     `snapshot_poll_miss` reason on polled streams is observed and
     accepted in tests 01 and 05. -->
| 10 | planned_collector_restart | Clean stop + start with maintenance marker | `restart_gap` planned=true |
| 11 | corrupt_message | Produce 3 malformed envelopes to a writer-consumed topic | NO gap; writer logs `corrupt_message_skipped` ERROR per record and continues |
| 12 | pg_kill_during_commit | Pause postgres for 120s | NO gap; writer logs `pg_save_failed`, archive flushes continue uninterrupted |
| 13 | rapid_restart_storm | Restart primary 5× with 8s down per cycle (each above 5s silence threshold) | `collector_restart` only on polled streams; ≥3 of 5 failover round-trips fire |
| 14 | both_collectors_kill | SIGKILL both collectors for 30s while writer/PG/redpanda stay up | `collector_restart` archived via CoverageFilter no-coverage path |
| 15 | redpanda_leader_change | Stop redpanda for 45s, restart | NO uncovered gap; producer health-monitor stays HEALTHY for brief outages (sustained-outage path is test 23) |
<!-- 16 removed: same chaos as test 01 (SIGKILL primary, backup covers,
     restart) with the only difference being a 90s vs 45s downtime. Its
     strict gaps=∅ assertion happens to pass when backup's polled-stream
     samples align well with the gap-decision moment, but is structurally
     flaky for the same OI / depth_snapshot poll-cadence reasons test 01
     handles. Test 01 is the canonical version. -->
| 17 | kafka_producer_outage | Network-isolate primary collector for 60s | NO uncovered gap; writer's silence-based failover takes over (sustained-outage path is test 23); only `collector_restart` permitted on polled streams |
| 18 | kafka_consumer_outage | Block writer←redpanda for 60s | `kafka_consumer_outage` |
| 19 | kafka_offset_reset | Delete + recreate topic during writer run | `kafka_offset_reset` |
| 20 | cross_source_pu_chain_break | Kill primary at depth u=N; backup has gap | `cross_source_pu_chain_break` |
| 21 | disk_full_hold | Fill disk; verify hold + recovery | `disk_full_hold` |
| 22 | both_collectors_silent | Block both collectors' egress | `both_collectors_silent` |
| 23 | kafka_full_outage | Stop redpanda; restart; verify journal replay | `kafka_producer_outage` |

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
fill_disk [dir] [percent]
free_disk
teardown_stack                     # called by trap EXIT automatically
today                              # prints YYYY-MM-DD (UTC)
scenario_pass
scenario_fail <message>
```
