#!/usr/bin/env bash
# 09_snapshot_poll_miss.sh
#
# Chaos:    Block BOTH collectors' egress to fstream.binance.com for 60s simultaneously
# Expected: gap reason=ws_disconnect OR both_collectors_silent (real loss)
# Why:      Neither collector receives data for the window; no source covered it.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "09" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "depth" 60

msg "=== CHAOS: Blocking BOTH collectors' egress to fstream.binance.com for 60s ==="
block_egress_via_network "collector"
block_egress_via_network "collector-backup"

msg "Holding both egress blocks for 60s…"
sleep 60

msg "Restoring both egress networks…"
restore_egress_via_network "collector"
restore_egress_via_network "collector-backup"

msg "Waiting 120s for gap envelopes to be emitted…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

# Both collectors were blocked — expect a gap (either ws_disconnect or both_collectors_silent)
if assert_gap_present "ws_disconnect" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "both_collectors_silent" "$HOST_DATA_DIR" 2>/dev/null; then
    msg "PASS: both-collector outage gap detected"
else
    scenario_fail "No ws_disconnect or both_collectors_silent gap found — real loss was not recorded"
fi

scenario_pass
