#!/usr/bin/env bash
# 08_ws_disconnect.sh
#
# Invariant: Break the primary collector's egress network connection to
# fstream.binance.com (the WebSocket endpoint). The WebSocketSupervisor ping
# loop fires after ~30s of silence → ws.abort() → disconnectLatch → reconnect.
# The collector emits a ws_disconnect gap. verify exits 0 with ERRORS=0.
#
# Expected gap reason: ws_disconnect

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "08" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Blocking primary collector egress (WS disconnect simulation) ==="
block_egress_via_network "collector"

# Hold for 45s (> 30s ping timeout)
msg "Holding egress block for 45s…"
sleep 45

msg "Restoring egress…"
restore_egress_via_network "collector"

# Wait for reconnect + gap emission
msg "Waiting 90s for ws_disconnect gap to appear…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "ws_disconnect" "$HOST_DATA_DIR"

scenario_pass
