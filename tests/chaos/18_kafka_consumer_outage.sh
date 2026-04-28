#!/usr/bin/env bash
# 18_kafka_consumer_outage.sh
#
# Invariant: Block the writer's connection to Redpanda for 60s. The
# KafkaConsumerOutageDetector detects empty polls lasting >30s while the
# writer's lifecycle heartbeat keeps firing → emits kafka_consumer_outage gap.
# verify exits 0 with ERRORS=0.
#
# Depends on: KafkaConsumerOutageDetector (Task A3.2)
#
# Expected gap reason: kafka_consumer_outage

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "18" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Isolating WRITER from Redpanda (kafka_consumer_outage) ==="
block_service_network "writer"

# Hold for 60s — must exceed 30s KafkaConsumerOutageDetector threshold
msg "Holding writer isolation for 60s…"
sleep 60

msg "Restoring writer network…"
restore_service_network "writer"
wait_healthy 120

msg "Waiting 90s for kafka_consumer_outage gap…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "kafka_consumer_outage" "$HOST_DATA_DIR"

scenario_pass
