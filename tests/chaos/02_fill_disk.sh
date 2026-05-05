#!/usr/bin/env bash
# 02_fill_disk.sh
#
# Scenario: writer_disk_full_brief
# Chaos:    Fill HOST_DATA_DIR to 99%; hold ~120s; free disk
# Expected: NO gap (writer recovers from Kafka after disk freed)
# Flow:     MAIN+BACKUP both delivering normally → writer's appendAndFsync
#           hits IOException on disk-full → writeErrors metric increments,
#           Kafka offsets are NOT committed (PG-then-Kafka ordering in
#           OffsetCommitCoordinator) → MAIN+BACKUP keep producing to Kafka,
#           records remain durable for 48h → disk freed → writer's next
#           flushAndCommit succeeds → consumer re-reads uncommitted
#           offsets → archive completes with no missing data.
# Why:      Under TWO-COLLECTOR + Kafka 48h retention + commit-after-fsync
#           invariant, a brief disk-full episode is fully recoverable.
#           Both MAIN and BACKUP records during the held window are
#           durable in Kafka and replayed on recovery. No real data loss,
#           no gap envelope. (A SUSTAINED hold exceeding 48h would lose
#           data — that case is operationally surfaced via writer_write_errors
#           rate alerting, not via a gap envelope, since chaos-testing it
#           reliably is impractical.)
#
# NOTE: This scenario writes real data with `dd if=/dev/zero` to HOST_DATA_DIR
# until the underlying filesystem is 99% full. On a typical dev machine
# /tmp is on the host disk (APFS on macOS), so running this naively would
# fill hundreds of GB and likely crash Docker. The `safe_disk_fill_or_skip`
# guard below SKIPs the scenario unless HOST_DATA_DIR is on a small
# dedicated filesystem (tmpfs / loopback ≤ ~2 GiB) or
# CRYPTOLAKE_CHAOS_DANGEROUS_DISK=1 is set explicitly. The teardown trap
# calls free_disk to clean the filler file even on failure.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "02" "primary+backup"

# Disk-fill is safe only on a small dedicated filesystem at HOST_DATA_DIR.
# On a regular dev machine /tmp lives on the host disk; filling it to 99%
# can write hundreds of GB and crash Docker. The guard skips this scenario
# unless the env is configured for it (or the operator opted in).
safe_disk_fill_or_skip "$HOST_DATA_DIR"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Filling HOST_DATA_DIR to 99% ==="
fill_disk "$HOST_DATA_DIR" 99

# Hold the disk-full state long enough for the writer to attempt several
# flush cycles and accumulate writeErrors. Default flush interval is 5s,
# so 120s gives ~24 failed-flush attempts.
msg "Holding disk-full state for 120s (writer's flushAndCommit will fail repeatedly)…"
sleep 120

msg "Freeing disk…"
free_disk

# After the disk is freed, the writer's next flushAndCommit succeeds. The
# Kafka consumer position is unchanged (no commits happened during the
# hold), so the next poll re-reads the records published during the hold
# from BOTH topics. They get archived now, completing the gap.
msg "Waiting 90s for writer recovery (re-poll uncommitted offsets, archive backlog)…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — brief disk-full is recoverable; no real data loss.
expect_lifecycle_event        "writer enters disk-full hold"     "WRITER_DISK_FULL_HOLD_ENTERED"
expect_lifecycle_event        "writer exits disk-full hold"      "WRITER_DISK_FULL_HOLD_EXITED"
expect_lifecycle_event_absent "no uncovered gap accepted"        "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
