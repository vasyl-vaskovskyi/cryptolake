#!/usr/bin/env bash
# 13_rapid_restart_storm.sh
#
# Chaos:    Restart primary collector 5 times in 30s (SIGKILL + restart each time)
# Expected: NO gap (redundancy worked)
# Why:      Backup covers continuously through every primary blip.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "13" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Rapid restart storm — 5 kills in 30s ==="
for i in 1 2 3 4 5; do
    msg "Kill ${i}/5…"
    kill_service "collector"
    sleep 3
    start_service "collector"
    sleep 3
done

msg "Letting stack stabilise for 90s…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_absent "collector_restart" "$HOST_DATA_DIR"

scenario_pass
