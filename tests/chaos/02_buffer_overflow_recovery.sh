#!/usr/bin/env bash
# 02_buffer_overflow_recovery.sh
#
# Chaos:    Block primary collector's egress to Kafka for 90s; primary buffer overflows
# Expected: NO gap (redundancy worked)
# Why:      Backup's Kafka path is unaffected; backup data covers the window.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "02" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Blocking primary collector from Kafka (simulating backpressure) ==="
block_service_network "collector"

# Hold the isolation long enough for per-stream buffers to fill and overflow
# The BackpressureGate threshold is typically 10k messages; with ~150 msg/s
# bookticker, that takes ~67s. We wait 90s to be safe.
msg "Holding isolation for 90s…"
sleep 90

msg "Restoring primary collector network…"
restore_service_network "collector"
# wait_healthy gives it time to re-connect and replay
wait_healthy 120

msg "Waiting 60s for recovery gap envelopes to be written…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_absent "buffer_overflow" "$HOST_DATA_DIR"

scenario_pass
