#!/usr/bin/env bash
# 12_pg_kill_during_commit.sh
#
# Chaos:    dc pause postgres for 60s; dc unpause postgres (do NOT kill writer)
# Expected: NO gap (redundancy worked)
# Why:      Writer enters pg_outage_hold; archives keep flushing; commits resume on PG up.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "12" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Pausing postgres (writer enters pg_outage_hold) ==="
dc pause postgres

# PgOutageHoldController triggers after 3 consecutive failures (~90s at 30s retry).
# We hold for 60s — enough to trigger hold mode; writer keeps flushing archives.
msg "Holding postgres paused for 60s…"
sleep 60

msg "Unpausing postgres (writer exits hold, resumes commits)…"
dc unpause postgres

msg "Waiting 90s for PG recovery and archive flush to complete…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_absent "pg_outage_hold" "$HOST_DATA_DIR"

scenario_pass
