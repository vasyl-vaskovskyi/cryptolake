#!/usr/bin/env bash
# 10_planned_collector_restart.sh
#
# Chaos:    mark_maintenance + clean stop + start primary collector
# Expected: NO gap (redundancy worked)
# Why:      Backup covers; planned shutdown is not loss.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "10" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

# Write a maintenance_intent record into the lifecycle ledger BEFORE stopping.
# This simulates what cryptolake-maintenance.sh does (minus the CLI call).
MAINT_ID="chaos-10-$(date -u +%s)"
JOURNAL_DIR="${HOST_DATA_DIR}/cryptolake/binance-collector-01"
mkdir -p "$JOURNAL_DIR"
LEDGER_PATH="${JOURNAL_DIR}/lifecycle.jsonl"
TS_NS=$(python3 -c "import time; print(int(time.time_ns()))")
printf '{"ts_ns":%s,"event":"clean_shutdown","host_boot_id":"chaos-boot","collector_session_id":"chaos-10-session","planned":true,"maintenance_id":"%s"}\n' \
    "$TS_NS" "$MAINT_ID" >> "$LEDGER_PATH" || true
msg "=== Wrote planned clean_shutdown marker to lifecycle journal ==="

msg "=== CHAOS: Clean stopping primary collector (planned restart) ==="
clean_stop_service "collector"

msg "Waiting 20s (planned downtime)…"
sleep 20

msg "Restarting primary collector…"
start_service "collector"
wait_healthy 120

msg "Waiting 60s for gap classification…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Planned restart must not produce any gap — backup covered the window.
assert_gap_absent "collector_restart" "$HOST_DATA_DIR"
msg "PASS: planned restart — verify ERRORS=0, no gap"

scenario_pass
