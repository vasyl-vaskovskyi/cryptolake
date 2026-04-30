#!/usr/bin/env bash
# 03_writer_crash_before_commit.sh
#
# Scenario: writer_crash
# Chaos:    SIGKILL writer mid-batch; restart writer
# Expected: gap reason=writer_restart (real loss)
# Flow:     MAIN healthy + BACKUP healthy → writer SIGKILL'd → both collectors
#           still delivering to Kafka, but archive is frozen because the
#           writer is the SINGLE archiver → writer restarts, resumes consuming.
# Why:      The writer is the choke point. While it is dead, neither MAIN
#           nor BACKUP can be archived. Real loss for the down window —
#           the gap reflects writer-side failure, not collector failure.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "03" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Killing writer (SIGKILL — crash before commit) ==="
kill_service "writer"

# Wait a moment then restart
sleep 5

msg "Restarting writer…"
start_service "writer"
wait_healthy 120

# Give writer time to classify the restart gap
msg "Waiting 60s for writer_restart gap classification…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "writer_restart" "$HOST_DATA_DIR"

scenario_pass
