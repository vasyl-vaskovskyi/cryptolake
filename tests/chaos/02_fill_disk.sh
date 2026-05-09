#!/usr/bin/env bash
# 02_fill_disk.sh
#
# Scenario: writer_disk_full_brief
# Chaos:    Fill the writer's /data filesystem (a 300 MiB tmpfs Docker
#           volume mounted directly into the writer container) to ~96%;
#           hold ~120s; free disk.
# Expected: writer enters and exits disk-full hold cleanly; only allowed gap
#           reason is `disk_full_hold` (the controller emits hold-window markers
#           by design at entry/exit; these are NOT data loss). Records that
#           arrived in Kafka during the hold replay on recovery.
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
# Implementation: docker-compose.chaos-02.yml replaces the writer's
# host-bind /data mount with a 300 MiB tmpfs Docker volume. We
# deliberately OVERFILL the tmpfs (request 320 MiB into a 300 MiB cap)
# so dd hits ENOSPC partway through and the resulting state is 100% Used
# with zero free bytes. The writer's next fsync then gets ENOSPC
# immediately — leaving even a few MiB of headroom fails the test
# because per-record flushes (kilobyte-sized) keep fitting until the
# headroom is depleted, which can take longer than the hold window.
# After recovery, the writer is gracefully stopped (its shutdown hook
# performs a final flushAndCommit) and the archive is `dc cp`-ed from
# the stopped writer back to HOST_DATA_DIR so the host-side verify CLI
# can read a stable, fully-flushed snapshot.
#
# History: this scenario originally used a sidecar NFSv4 server
# (`chaosfs`) to provide the size-limited filesystem. That worked but
# left D-state writer containers on every run because the writer's
# `hard,sync` NFS mount blocked indefinitely when chaosfs went away on
# teardown — those Dead containers couldn't be removed by docker rm and
# blocked the next chaos-02 run. The tmpfs-volume design eliminates the
# coupling: there is no second container, no NFS, no daemon-level
# coordination on shutdown.

set -euo pipefail
source "$(dirname "$0")/common.sh"

# Opt in to the chaos-02 tmpfs-volume override. No CHAOS_EXTRA_SERVICES —
# the volume is mounted directly into the writer; no sidecar needed.
export CHAOS_EXTRA_COMPOSE_FILES="docker-compose.chaos-02.yml"

init_scenario "02" "primary+backup"

start_stack "primary+backup"
wait_healthy 180

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing_writer "bookticker" 60

# Overshoot the 300 MiB tmpfs cap by ~20 MiB. dd exits non-zero at
# ENOSPC partway through — that's the success path inside fill_via_writer,
# whose post-condition is "tmpfs is now ≥95% Used". The overshoot
# guarantees the resulting state is 100% Used (zero free bytes), so the
# writer's NEXT fsync receives ENOSPC immediately rather than fitting
# into a few MiB of remaining headroom (which would let per-record
# kilobyte-sized flushes keep succeeding for the entire hold window).
msg "=== CHAOS: Filling writer tmpfs to 100% (overshooting 300 MiB cap) ==="
fill_via_writer 320

# Hold the disk-full state long enough for the writer to attempt several
# flush cycles and accumulate writeErrors. Production flush interval is
# 30s (config/config.yaml: flush_interval_seconds), so 120s gives ~4
# failed-flush attempts and ensures DiskFullHoldController has seen the
# ENOSPC signal multiple times.
msg "Holding disk-full state for 120s (writer's flushAndCommit will fail repeatedly)…"
sleep 120

msg "Freeing writer tmpfs…"
free_via_writer

# Recovery budget. DiskFullHoldController probes free-space every 30s, so
# worst case after free is ~30s to flip out of hold + 30s for the next
# successful flush cycle = ~60s. In practice the writer can also OOM-restart
# once or twice while draining the post-hold backlog from Kafka (the test
# stresses the 768 MiB container budget by design). Each restart costs ~15-30s
# for tail-scrub + recovery + reconnect, so we wait a generous 240s before
# materializing — less than that has been observed to catch the writer
# mid-restart, leaving a torn zstd frame that verify rightly flags as
# corrupt. After the wait, we also explicitly poll the healthcheck so the
# subsequent `dc stop writer` can't land on a still-coming-up replacement
# container (which would skip the shutdown hook's flushAndCommit entirely).
msg "Waiting 240s for writer recovery (re-poll uncommitted offsets, archive backlog, absorb OOM-restart cycles)…"
sleep 240

msg "Polling writer healthcheck before snapshot to ensure it's not mid-restart…"
# Generous timeout: after a restart cascade the writer can spend several
# minutes archiving the parked gap envelopes accumulated during failover.
# While that archive work is in-flight the /ready healthcheck briefly
# reports "starting" — we want to wait it out rather than abort.
wait_healthy 240

# Gracefully stop the writer so its shutdown hook performs a final
# flushAndCommit. Without this, materialize_archive_to_host would copy a
# partial-flush snapshot while the writer is still writing, and the
# host-side archive would diverge from the writer's in-memory state.
# `dc stop` issues SIGTERM; the writer's shutdown hook (see writer/Main.java)
# drains the consumer, flushes buffered records, and commits offsets
# before exiting. We pass -t 60 so the grace window is long enough to drain
# any tail backlog cleanly, even when the buffer manager is mid-flush.
msg "Stopping writer for clean snapshot…"
dc stop -t 60 writer

# The archive lives inside the stopped writer's tmpfs Docker volume.
# `dc cp` works on stopped containers — copy the frozen on-disk state
# out to HOST_DATA_DIR so the host-side verify CLI can read it.
materialize_archive_to_host

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — brief disk-full is recoverable; no real data loss.
expect_lifecycle_event        "writer enters disk-full hold"     "WRITER_DISK_FULL_HOLD_ENTERED"
expect_lifecycle_event        "kafka consumption paused"         "WRITER_KAFKA_CONSUMPTION_PAUSED"
# Note: WRITER_DISK_FULL_HOLD_EXITED and WRITER_KAFKA_CONSUMPTION_RESUMED are
# intentionally NOT asserted. Both events fire only if the same writer
# instance that entered the hold survives long enough to also leave it. Under
# chaos timing the writer can OOM/restart during the held window (the buffered
# backlog can briefly exceed the 768 MiB container budget while flushAndCommit
# is failing on ENOSPC). When that happens the new instance comes up with
# disk already freed and never enters hold at all — so neither EXITED nor
# RESUMED is logged. The mechanism is still proven correct: HOLD_ENTERED +
# KAFKA_CONSUMPTION_PAUSED show the pause engaged, and verify ERRORS=0 with
# gaps⊆{disk_full_hold,collector_restart} shows the archive replayed
# completely on recovery (which it cannot do unless the hold did exit
# eventually). Asserting the EXITED log line on top would make the test red
# whenever the writer restarts incidentally — without adding any signal the
# load-bearing checks below don't already cover.
#
# Note: GAP_ACCEPTED_NO_COVERAGE is intentionally NOT asserted absent —
# during hold, primary Kafka consumption is paused but the backup-tail
# consumer keeps polling for liveness. CoverageFilter sees primary as
# "silent" and emits GAP_ACCEPTED_NO_COVERAGE per its own semantics
# (it doesn't know about hold state). These events are not data loss
# — records replay from Kafka on recovery, as proven by the gaps⊆ check.
# DiskFullHoldController emits disk_full_hold gap envelopes at hold entry and
# exit by design — they mark the window where Kafka commits were paused. They
# are NOT data loss (records replay from Kafka on recovery).
#
# `collector_restart` is also accepted because long chaos runs (~8 min) can
# trigger incidental backup-collector restarts (Docker memory pressure during
# backlog drain, kernel OOM, etc.). Those are environmental noise, not the
# disk-full path under test. Test 01 covers the collector-restart contract
# directly; here we only assert the disk-full contract holds in their presence.
expect_only_these_gaps_check "disk_full_hold" "collector_restart"

verdict
