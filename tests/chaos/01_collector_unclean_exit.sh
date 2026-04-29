#!/usr/bin/env bash
# 01_collector_unclean_exit.sh
#
# Chaos:    SIGKILL primary collector; backup collector keeps running
# Expected: NO gap (redundancy worked)
# Why:      Backup covers the window. Redundancy worked.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "01" "primary+backup"

# Start full stack (primary + backup collectors)
start_stack "primary+backup"
wait_healthy 150

# Warm up: let both collectors flow for 60s so we have some archive data
msg "Warm-up 60s — letting data flow before chaos…"
warm_up 60

# Verify data is actually being written
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Killing primary collector (SIGKILL) ==="
kill_service "collector"

# Wait 45s for writer to detect the session change and emit gap envelope
msg "Waiting 45s for writer to detect collector_restart and emit gap…"
sleep 45

# Restart primary collector so the stack stabilises
msg "Restarting primary collector…"
start_service "collector"
wait_healthy 120

# Another 30s of normal operation to ensure archives are flushed
warm_up 30

# Assertions
run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_absent "collector_restart" "$HOST_DATA_DIR"

scenario_pass
