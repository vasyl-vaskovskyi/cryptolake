#!/usr/bin/env bash
# 08_rapid_restart_storm.sh
#
# Scenario: rapid_main_restart_storm
# Chaos:    Restart MAIN 5× in 30s (SIGKILL + restart each iteration)
# Expected: NO gap (redundancy worked)
# Flow:     Loop ×5: MAIN killed → BACKUP keeps delivering → writer
#           archives BACKUP → MAIN restarts and starts delivering →
#           writer briefly switches back to MAIN → MAIN killed again.
#           BACKUP is never interrupted across the entire 30s storm.
# Why:      Only MAIN flaps. BACKUP is the steady source the entire
#           time. No window had zero sources. No gap under the
#           TWO-COLLECTOR rule — this is exactly what redundancy is for.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "08" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Restart storm — 5 kills, each cycle >5s silence threshold ==="
# Each cycle must keep MAIN down for >5s so the writer's silence-based
# failover detection (MAIN_FAILURE_DETECTED at 5s) actually fires.
# 8s down + 4s up = 12s per cycle, 5 cycles = 60s.
for i in 1 2 3 4 5; do
    msg "Kill ${i}/5…"
    kill_service "collector"
    sleep 8
    start_service "collector"
    sleep 4
done

msg "Letting stack stabilise for 90s…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — MAIN flapped 5x with silence > detection threshold each cycle;
# BACKUP covered live diffs throughout. Polled streams (open_interest,
# depth_snapshot) may legitimately record `collector_restart` per cycle
# when the gap-decision lands between backup polls (see test 01 header).
expect_lifecycle_event_count  "writer detects MAIN failure (≥3 of 5 cycles)"   "MAIN_FAILURE_DETECTED"            3
expect_lifecycle_event_count  "MAIN recovered (≥3 of 5 cycles)"                "MAIN_RECOVERED"                   3
expect_lifecycle_event_count  "writer fails over to BACKUP (≥3 of 5 cycles)"   "WRITER_NOW_ARCHIVING_FROM=BACKUP" 3
expect_lifecycle_event_count  "writer switches back to MAIN (≥3 of 5 cycles)"  "WRITER_NOW_ARCHIVING_FROM=MAIN"   3
expect_only_these_gaps_check  collector_restart

verdict
