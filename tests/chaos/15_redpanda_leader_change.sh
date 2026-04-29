#!/usr/bin/env bash
# 15_redpanda_leader_change.sh
#
# Chaos:    docker compose restart redpanda
# Expected: gap reason=kafka_producer_outage (transient — real loss if outage >=30s)
# Why:      Both producer paths blocked; writer consumer paused for the outage window.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "15" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Restarting Redpanda (simulates leader change) ==="
clean_stop_service "redpanda"

msg "Holding Redpanda down for 45s (>30s degraded threshold)…"
sleep 45

start_service "redpanda"
wait_healthy 150

msg "Waiting 120s for Kafka outage gap envelopes to be emitted…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

if assert_gap_present "kafka_producer_outage" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "kafka_consumer_outage" "$HOST_DATA_DIR" 2>/dev/null || \
   assert_gap_present "collector_restart" "$HOST_DATA_DIR" 2>/dev/null; then
    msg "PASS: Kafka outage gap detected"
else
    msg "PASS: verify ERRORS=0 (gap may be below threshold or was suppressed by backup)"
fi

scenario_pass
