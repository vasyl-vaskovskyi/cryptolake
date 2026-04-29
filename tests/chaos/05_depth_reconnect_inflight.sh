#!/usr/bin/env bash
# 05_depth_reconnect_inflight.sh
#
# Chaos:    Drop primary collector's WS during depth flow; primary recovers via snapshot
# Expected: NO gap (redundancy worked)
# Why:      Backup's depth pu-chain bridges the missing diffs.

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
assert_gap_absent "ws_disconnect" "$HOST_DATA_DIR"

scenario_pass
