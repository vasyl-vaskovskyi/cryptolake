#!/usr/bin/env bash
# 05_depth_reconnect_inflight.sh
#
# Invariant: Simulate a depth WebSocket disconnect during active depth streaming
# by disconnecting the primary collector from the egress network (where Binance
# WS lives). The WebSocketSupervisor detects ping failure → ws.abort() →
# reconnect → snapshot fetch → resync. During the gap backup covers. On
# reconnect the DepthGapDetector may emit a pu_chain_break. verify exits 0.
#
# Expected gap reason: ws_disconnect OR pu_chain_break

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "05" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "depth" 60

msg "=== CHAOS: Disconnecting primary collector egress (WS drop) ==="
block_egress_via_network "collector"

# Hold for 45s — enough for ping timeout (30s) to fire
msg "Holding egress block for 45s…"
sleep 45

msg "Restoring primary egress…"
restore_egress_via_network "collector"

# Give collector time to reconnect, re-subscribe, fetch snapshot, and resync
msg "Waiting 90s for reconnect and resync…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"

# Either ws_disconnect or pu_chain_break is acceptable (both indicate detection)
if assert_gap_present "ws_disconnect" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "pu_chain_break" "$HOST_DATA_DIR" 2>/dev/null; then
    msg "PASS: gap detected (ws_disconnect or pu_chain_break)"
else
    scenario_fail "Neither ws_disconnect nor pu_chain_break gap found"
fi

scenario_pass
