#!/usr/bin/env bash
# 09_snapshot_poll_miss.sh
#
# Scenario: both_ws_disconnect
# Chaos:    iptables-block BOTH MAIN and BACKUP egress to fstream.binance.com
#           simultaneously for 60s; then unblock both
# Expected: gap reason=ws_disconnect OR both_collectors_silent (real loss)
# Flow:     MAIN's WS severed AND BACKUP's WS severed at the same time →
#           neither collector receives data for the 60s window → writer
#           has no source to archive → both unblocked, both reconnect →
#           gap envelope emitted for the 60s where neither delivered.
# Why:      This is the canonical "BOTH collectors fail" scenario from
#           the TWO-COLLECTOR rule. Both upstream links are dead at the
#           same time. Real loss; gap is correct.

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
