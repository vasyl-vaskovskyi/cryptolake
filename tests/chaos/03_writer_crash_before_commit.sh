#!/usr/bin/env bash
# 03_writer_crash_before_commit.sh
#
# Invariant: Kill the writer with SIGKILL (simulating a crash between archive
# flush and Kafka offset commit). On restart the RestartGapClassifier sees the
# gap and emits a writer_restart gap. verify exits 0 with ERRORS=0.
#
# Expected gap reason: writer_restart

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
