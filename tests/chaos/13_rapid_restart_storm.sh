#!/usr/bin/env bash
# 13_rapid_restart_storm.sh
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

# Assertions — MAIN flapped 5x; BACKUP covered. Use ≥3 (3s sleeps may
# collapse a 5s silence-detection window — see scenarios review).
expect_lifecycle_event_count  "writer detects MAIN failure (≥3 times)"   "MAIN_FAILURE_DETECTED"        3
expect_lifecycle_event_count  "MAIN recovered (≥3 times)"               "MAIN_RECOVERED"               3
expect_lifecycle_event_count  "writer fails over to BACKUP (≥3 times)"  "WRITER_NOW_ARCHIVING_FROM=BACKUP" 3
expect_lifecycle_event_count  "writer switches back to MAIN (≥3 times)" "WRITER_NOW_ARCHIVING_FROM=MAIN"   3
expect_lifecycle_event_absent "no uncovered gap accepted"               "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
