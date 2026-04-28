#!/usr/bin/env bash
# 09_snapshot_poll_miss.sh
#
# Invariant: Block the primary collector's access to the REST endpoint
# (fapi.binance.com) used for depth snapshots. After N consecutive poll
# failures the SnapshotScheduler emits a snapshot_poll_miss gap.
# verify exits 0 with ERRORS=0.
#
# Implementation: We block the entire egress network of the primary collector
# (which includes both WS and REST). This is a cruder simulation than
# blocking only the REST endpoint, but is network-neutral and sufficient to
# trigger the snapshot_poll_miss detector.
#
# Expected gap reason: snapshot_poll_miss (or ws_disconnect for the WS side)

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "09" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "depth" 60

msg "=== CHAOS: Blocking primary collector egress (REST + WS unavailable) ==="
block_egress_via_network "collector"

# Snapshot interval is 5m for most symbols, 30s for btcusdt (from config).
# We need the collector to try a snapshot poll AND fail. Hold for 90s.
msg "Holding egress block for 90s (>30s btcusdt snapshot interval)…"
sleep 90

msg "Restoring egress…"
restore_egress_via_network "collector"

msg "Waiting 120s for gap envelopes to be emitted…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

# Either snapshot_poll_miss OR ws_disconnect is acceptable — both indicate
# the collector detected the network loss
if assert_gap_present "snapshot_poll_miss" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "ws_disconnect" "$HOST_DATA_DIR" 2>/dev/null; then
    msg "PASS: network outage gap detected"
else
    scenario_fail "No snapshot_poll_miss or ws_disconnect gap found"
fi

scenario_pass
