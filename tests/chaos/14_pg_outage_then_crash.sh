#!/usr/bin/env bash
# 14_pg_outage_then_crash.sh
#
# Scenario: both_collectors_kill
# Chaos:    SIGKILL MAIN AND BACKUP at the same instant; sleep 30s; restart both
# Expected: gap reason=collector_restart (real loss)
# Flow:     MAIN+BACKUP both healthy → SIGKILL both at the same moment →
#           neither delivers anything for 30s → writer has no source to
#           archive → both restart and resume → gap envelope is emitted
#           covering the 30s where neither delivered.
# Why:      The canonical "BOTH collectors fail simultaneously" case
#           from the TWO-COLLECTOR rule. Real loss; gap is correct.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "14" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: SIGKILLing primary AND backup collectors simultaneously ==="
kill_service "collector"
kill_service "collector-backup"

msg "Holding both collectors down for 30s (no source covers this window)…"
sleep 30

msg "Restarting primary and backup collectors…"
start_service "collector"
start_service "collector-backup"
wait_healthy 150

msg "Waiting 90s for gap classification…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "collector_restart" "$HOST_DATA_DIR"

scenario_pass
