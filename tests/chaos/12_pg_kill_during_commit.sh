#!/usr/bin/env bash
# 12_pg_kill_during_commit.sh
#
# Invariant: Kill Postgres mid-operation so the writer's PgOutageHoldController
# engages. The controller pauses Kafka commits, keeps flushing archives, and
# emits a pg_outage_hold gap. On Postgres recovery the controller emits the
# closing gap and resumes commits. verify exits 0 with ERRORS=0.
#
# Expected gap reason: pg_outage_hold

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "12" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Killing Postgres ==="
kill_service "postgres"

# PgOutageHoldController triggers after 3 consecutive failures (~90s at 30s retry)
msg "Waiting 120s for pg_outage_hold gap to appear…"
sleep 120

msg "Restarting Postgres…"
start_service "postgres"

msg "Waiting 90s for PG recovery and closing gap emission…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "pg_outage_hold" "$HOST_DATA_DIR"

scenario_pass
