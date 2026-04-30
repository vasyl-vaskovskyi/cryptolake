#!/usr/bin/env bash
# 20_cross_source_pu_chain_break.sh
#
# Scenario: cross_source_pu_chain_break
# Chaos:    Kill MAIN at depth u=N while BACKUP's last-seen u was N-50;
#           restart MAIN, which resumes at u=N+something via fresh snapshot
# Expected: gap reason=cross_source_pu_chain_break (real loss)
# Flow:     MAIN delivering at u=N → MAIN killed → BACKUP only has data
#           up to u=N-50, was already lagging → for u=N-49..u=N-1 NEITHER
#           collector ever delivered the diffs → MAIN restarts and
#           snapshots, resumes at u>N → CrossSourcePuChainValidator
#           inspects the merged stream, detects the joint hole,
#           emits gap envelope for u=N-49..N-1.
# Why:      This is the TWO-COLLECTOR rule applied at u-chain granularity:
#           a sub-window where BOTH sources had no data, even though
#           neither source was "down" in the heartbeat sense. The
#           validator surfaces it as a real loss. Gap is correct.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "20" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 90s — letting depth streams sync fully…"
warm_up 90
wait_data_flowing "depth" 60

msg "=== CHAOS: Killing primary during depth stream (pu-chain gap) ==="
kill_service "collector"

# Backup will continue but primary's last few depth diffs are not in backup's
# stream yet — this creates the cross-source gap. Wait 30s before restart to
# widen the potential gap.
msg "Holding primary down 30s (backup covers but may miss some u-values)…"
sleep 30

msg "Restarting primary collector…"
start_service "collector"
wait_healthy 120

msg "Waiting 90s for gap emission and archival…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

assert_gap_present "cross_source_pu_chain_break" "$HOST_DATA_DIR"

scenario_pass
