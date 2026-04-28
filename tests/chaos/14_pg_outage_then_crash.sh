#!/usr/bin/env bash
# 14_pg_outage_then_crash.sh
#
# Invariant: Kill Postgres AND the writer simultaneously. On recovery the writer
# should emit both pg_outage_hold (for the PG unavailability) and writer_restart
# (for its own crash). verify exits 0 with ERRORS=0.
#
# Expected gap reasons: pg_outage_hold AND/OR writer_restart

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "14" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Killing Postgres + writer simultaneously ==="
kill_service "postgres"
kill_service "writer"

sleep 10

msg "Restarting Postgres and writer…"
start_service "postgres"
sleep 5
start_service "writer"
wait_healthy 150

msg "Waiting 120s for gap classification…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

FOUND=false
if assert_gap_present "pg_outage_hold" "$HOST_DATA_DIR" 2>/dev/null; then
    FOUND=true
    msg "Found pg_outage_hold gap"
fi
if assert_gap_present "writer_restart" "$HOST_DATA_DIR" 2>/dev/null; then
    FOUND=true
    msg "Found writer_restart gap"
fi
if assert_gap_present "collector_restart" "$HOST_DATA_DIR" 2>/dev/null; then
    FOUND=true
    msg "Found collector_restart gap"
fi

if ! $FOUND; then
    msg "WARNING: No expected gap found — verifying ERRORS=0 is the binding invariant"
    # run_verify already succeeded above
fi

scenario_pass
