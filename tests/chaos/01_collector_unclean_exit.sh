#!/usr/bin/env bash
# 01_collector_unclean_exit.sh
#
# Scenario: main_unclean_exit
# Chaos:    SIGKILL MAIN collector; BACKUP keeps running; restart MAIN
# Expected: NO gap (redundancy worked)
# Flow:     MAIN dies → BACKUP keeps delivering → writer archives BACKUP →
#           MAIN restarts and resumes → writer switches back to MAIN.
# Why:      Only MAIN failed. BACKUP fed the writer throughout, so no
#           sub-window had zero sources. Under the TWO-COLLECTOR rule
#           (gap iff BOTH collectors fail simultaneously), no gap is emitted.

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

# Assertions — the contract is "TWO-COLLECTOR rule held; no data loss".
# Each expect_* registers a check; verdict() runs all and prints PASS/FAIL.
run_verify "$(today)" "$HOST_DATA_DIR"

expect_lifecycle_event "redundancy active before chaos (both collectors delivering)" "COVERAGE_FILTER_ACTIVATED"
expect_lifecycle_event "writer detects MAIN failure"                                 "MAIN_FAILURE_DETECTED"
expect_lifecycle_event "writer fails over to BACKUP"                                 "WRITER_NOW_ARCHIVING_FROM=BACKUP"
expect_lifecycle_event "MAIN comes back online"                                      "MAIN_RECOVERED"
expect_lifecycle_event "writer switches back to MAIN"                                "WRITER_NOW_ARCHIVING_FROM=MAIN"
expect_lifecycle_event_absent "no uncovered gap accepted"                            "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check "no gap envelopes archived (TWO-COLLECTOR rule held)"

verdict
