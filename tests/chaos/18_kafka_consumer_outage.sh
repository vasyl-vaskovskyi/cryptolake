#!/usr/bin/env bash
# 18_kafka_consumer_outage.sh
#
# Chaos:    Block writer's connection to redpanda for 60s
# Expected: gap reason=kafka_consumer_outage (real loss)
# Why:      Writer is the only consumer; while blocked nothing reaches the archive.

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
