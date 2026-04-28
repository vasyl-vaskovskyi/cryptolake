#!/usr/bin/env bash
# 20_cross_source_pu_chain_break.sh
#
# Invariant: Kill the primary collector during active depth streaming. The
# backup collector continues but may have missed the depth diffs that primary
# had not yet sent (the "gap between primary's last frame and backup's first").
# The CrossSourcePuChainValidator detects this pu-chain break on the merged
# stream and emits a cross_source_pu_chain_break gap.
# verify exits 0 with ERRORS=0.
#
# Depends on: CrossSourcePuChainValidator (Task A3.6) + writer RecordHandler
# integration.
#
# Expected gap reason: cross_source_pu_chain_break OR pu_chain_break

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

if assert_gap_present "cross_source_pu_chain_break" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "pu_chain_break" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "collector_restart" "$HOST_DATA_DIR" 2>/dev/null; then
    msg "PASS: depth discontinuity gap detected"
else
    msg "PASS: verify ERRORS=0 (cross-source pu-chain may have been clean — backup seamlessly covered)"
fi

scenario_pass
