#!/usr/bin/env bash
# 21_disk_full_hold.sh
#
# Invariant: Fill HOST_DATA_DIR to 99% triggering DiskFullHoldController in
# the writer. The controller emits disk_full_hold gaps. After freeing space
# the writer recovers, resumes commits, and emits a closing gap.
# verify exits 0 with ERRORS=0.
#
# This scenario is more explicit than 04_fill_disk.sh — it waits for the gap
# to appear before freeing disk, then confirms the recovery path.
#
# Depends on: DiskFullHoldController (Task A3.4)
#
# Expected gap reason: disk_full_hold

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "21" "primary+backup"

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
assert_gap_present "disk_full_hold" "$HOST_DATA_DIR"

scenario_pass
