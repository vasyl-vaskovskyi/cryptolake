#!/usr/bin/env bash
# 09_both_collectors_kill.sh
# (Originally misnamed pg_outage_then_crash; renamed to match its actual
# chaos. The PG outage path is covered by test 07.)
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

init_scenario "09" "primary+backup"

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

# Assertions — both collectors killed simultaneously; expect real loss
# detected via the CoverageFilter "no-coverage" path (NOT the silence-
# inferred path, which is for alive-but-silent collectors — see test 15).
# When both processes die, GAP_ACCEPTED_NO_COVERAGE fires immediately at
# session-change time (no parking grace), so GAP_ARCHIVED (which is for
# the parked-then-timeout path) does NOT fire here.
expect_lifecycle_event       "uncovered gap accepted"          "GAP_ACCEPTED_NO_COVERAGE"
expect_gap_present_check     "collector_restart gap recorded"  "collector_restart"
expect_only_these_gaps_check collector_restart

verdict
