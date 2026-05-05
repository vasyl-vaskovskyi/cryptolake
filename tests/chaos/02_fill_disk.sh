#!/usr/bin/env bash
# 02_fill_disk.sh
#
# Scenario: writer_disk_full_brief
# Chaos:    Fill the writer's /data filesystem (a 300 MiB tmpfs served by
#           the chaosfs NFS sidecar) to ~96%; hold ~120s; free disk.
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
# Implementation: This scenario uses a sidecar NFSv4 server (`chaosfs`)
# whose backing store is a 300 MiB tmpfs. The writer mounts /data over NFS
# from chaosfs. Filling 290 MiB of the tmpfs leaves ~10 MiB headroom and
# triggers ENOSPC on the writer's next fsync without touching the host
# filesystem. After recovery, the writer is gracefully stopped (its
# shutdown hook performs a final flushAndCommit) and the archive is
# materialized from chaosfs back to HOST_DATA_DIR so the host-side verify
# CLI can read a stable, fully-flushed snapshot.

set -euo pipefail
source "$(dirname "$0")/common.sh"

# Opt in to the chaosfs sidecar via the generic CHAOS_EXTRA_* hooks.
# init_scenario reads CHAOS_EXTRA_COMPOSE_FILES; start_stack reads CHAOS_EXTRA_SERVICES.
export CHAOS_EXTRA_COMPOSE_FILES="docker-compose.chaos-02-nfs.yml"
export CHAOS_EXTRA_SERVICES="chaosfs"

init_scenario "02" "primary+backup"

start_stack "primary+backup"
wait_healthy 180

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Filling chaosfs tmpfs (290 MiB of 300 MiB cap) ==="
fill_via_chaosfs 290

# Hold the disk-full state long enough for the writer to attempt several
# flush cycles and accumulate writeErrors. Production flush interval is
# 30s (config/config.yaml: flush_interval_seconds), so 120s gives ~4
# failed-flush attempts and ensures DiskFullHoldController has seen the
# ENOSPC signal multiple times.
msg "Holding disk-full state for 120s (writer's flushAndCommit will fail repeatedly)…"
sleep 120

msg "Freeing chaosfs tmpfs…"
free_via_chaosfs

# Recovery budget. DiskFullHoldController probes free-space every 30s,
# so worst case after free is ~30s to flip out of hold + 30s for the
# next successful flush cycle = ~60s. We sleep 120s to absorb scheduling
# jitter and to let the consumer drain the post-hold backlog from Kafka.
msg "Waiting 120s for writer recovery (re-poll uncommitted offsets, archive backlog)…"
sleep 120

# Gracefully stop the writer so its shutdown hook performs a final
# flushAndCommit. Without this, materialize_archive_to_host could tar a
# partial-flush snapshot while the writer is still writing, and the
# archive on HOST_DATA_DIR would diverge from what's on chaosfs.
# `dc stop` issues SIGTERM; the writer's shutdown hook (see writer/Main.java)
# drains the consumer, flushes buffered records, and commits offsets
# before exiting. stop_grace_period=30s in docker-compose.yml is enough.
msg "Stopping writer for clean snapshot…"
dc stop writer

# The archive lives inside chaosfs:/exports (the writer's NFS-mounted /data).
# Now that the writer is stopped, the on-disk state is stable; copy it to
# HOST_DATA_DIR so the host-side verify CLI can see it.
materialize_archive_to_host

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — brief disk-full is recoverable; no real data loss.
expect_lifecycle_event        "writer enters disk-full hold"     "WRITER_DISK_FULL_HOLD_ENTERED"
expect_lifecycle_event        "writer exits disk-full hold"      "WRITER_DISK_FULL_HOLD_EXITED"
expect_lifecycle_event_absent "no uncovered gap accepted"        "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
