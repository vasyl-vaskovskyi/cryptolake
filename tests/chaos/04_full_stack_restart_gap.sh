#!/usr/bin/env bash
# 04_full_stack_restart_gap.sh
#
# Scenario: full_stack_restart
# Chaos:    docker compose down then up after 60s (every container off)
# Expected: per-stream restart_gap candidate is detected and PARKED, then
#           SUPPRESSED_BY_COVERAGE because both collectors come back together
#           and the coverage filter sees both currently fresh. NO gap envelope
#           is archived — full-stack restart is handled gracefully when
#           both sides resume in lockstep.
# Flow:     MAIN, BACKUP, writer, redpanda, postgres ALL killed → 60s of
#           silence → stack restarts → writer reads its committed offsets
#           from PG, resumes consuming → sees primary's session_id changed
#           since last commit → emits restart_gap candidate → parks it
#           (backup is also delivering fresh data) → 10s grace window
#           passes with backup still publishing → gap suppressed.
# Why:      The coverage filter's heuristic is "is backup currently fresh?"
#           When both collectors restart together, both ARE fresh post-
#           restart, so the filter suppresses. This documents and locks in
#           that current behavior. Detection of the "both silent during the
#           gap window" case requires the SilenceInferredGapEmitter path —
#           see test 15 (both_collectors_silent) which forces it via
#           sustained egress block while collectors stay alive.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "04" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Full stack down (no clean shutdown markers) ==="
# Stop WITHOUT -v so data is preserved (volumes stay)
dc down --remove-orphans

msg "Waiting 60s (simulates downtime gap; long enough that backup's Kafka backlog can't bridge the gap window)…"
sleep 60

msg "Restarting full stack…"
start_stack "primary+backup"
wait_healthy 150

# Give writer time to read LifecycleJournal and emit restart gap envelopes
msg "Waiting 60s for restart gap classification…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — restart_gap is detected, parked, then suppressed by mutual
# coverage when both collectors come back together. The only gap that may
# survive is a depth pu_chain_break from whichever collector resumes
# publishing first (the other hasn't sent data yet so it can't cover);
# that's a real restart artifact and is permitted.
expect_lifecycle_event       "restart-gap candidate detected"            "GAP_PARKED.*restart_gap"
expect_lifecycle_event       "candidate suppressed by mutual coverage"   "GAP_SUPPRESSED_BY_COVERAGE.*restart_gap"
expect_only_these_gaps_check pu_chain_break

verdict
