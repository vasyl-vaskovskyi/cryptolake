#!/usr/bin/env bash
# 04_fill_disk.sh
#
# Chaos:    Fill HOST_DATA_DIR to 99%; eventually free
# Expected: gap reason=disk_full_hold (real loss)
# Why:      Writer pauses commits; archive frozen during hold.
#
# NOTE: This scenario fills /tmp which is typically tmpfs and may affect the
# host. The teardown_stack trap calls free_disk to clean up even on failure.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "04" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Filling HOST_DATA_DIR to 99% ==="
fill_disk "$HOST_DATA_DIR" 99

# Wait for the writer to hit ENOSPC and emit disk_full_hold gap
# DiskFullHoldController retries every 30s; allow up to 3 cycles
msg "Waiting 120s for disk_full_hold gap to appear…"
sleep 120

msg "Freeing disk…"
free_disk

# Wait for recovery
msg "Waiting 60s for writer recovery after disk freed…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "disk_full_hold" "$HOST_DATA_DIR"

scenario_pass
