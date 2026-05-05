#!/usr/bin/env bash
# 14_disk_full_hold.sh
#
# Scenario: writer_disk_full_hold (state-machine variant of #02)
# Chaos:    Fill HOST_DATA_DIR to 99%; wait until a disk_full_hold gap
#           envelope is emitted; then free disk
# Expected: gap reason=disk_full_hold (real loss)
# Flow:     MAIN+BACKUP both healthy and delivering → writer cannot write
#           archives because disk is full → writer enters disk_full_hold
#           and stops committing → wait for the disk_full_hold gap
#           envelope to be archived (state machine assertion) → free
#           disk → writer resumes commits → second gap envelope
#           closes the hold window.
# Why:      Same root cause as #04, but this scenario specifically
#           asserts the disk_full_hold state-machine emits the
#           gap-envelope shape correctly. Writer-side failure under
#           the TWO-COLLECTOR rule = real loss.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "14" "primary+backup"

# Disk-fill is safe only on a small dedicated filesystem at HOST_DATA_DIR.
# See the safe_disk_fill_or_skip helper in common.sh — same guard as test 02.
safe_disk_fill_or_skip "$HOST_DATA_DIR"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Filling disk to 99% ==="
fill_disk "$HOST_DATA_DIR" 99

# Wait up to 180s for disk_full_hold gap
msg "Waiting for disk_full_hold gap (up to 180s)…"
FOUND=false
DEADLINE=$(( SECONDS + 180 ))
while (( SECONDS < DEADLINE )); do
    if assert_gap_present "disk_full_hold" "$HOST_DATA_DIR" 2>/dev/null; then
        FOUND=true
        break
    fi
    sleep 15
done

msg "Freeing disk…"
free_disk

if ! $FOUND; then
    msg "WARNING: disk_full_hold gap not found during fill phase"
fi

msg "Waiting 60s for writer recovery after disk freed…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — sustained disk-full hold should produce a disk_full_hold gap.
expect_lifecycle_event   "writer enters disk-full hold"      "WRITER_DISK_FULL_HOLD_ENTERED"
expect_lifecycle_event   "writer exits disk-full hold"       "WRITER_DISK_FULL_HOLD_EXITED"
expect_lifecycle_event   "gap was archived"                  "GAP_ARCHIVED"
expect_gap_present_check "disk_full_hold gap recorded"       "disk_full_hold"
expect_only_these_gaps_check disk_full_hold

verdict
