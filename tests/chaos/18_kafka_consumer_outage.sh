#!/usr/bin/env bash
# 18_kafka_consumer_outage.sh
#
# Scenario: writer_kafka_consumer_outage
# Chaos:    iptables-block writer↔redpanda for 60s; then unblock
# Expected: gap reason=kafka_consumer_outage (real loss)
# Flow:     MAIN+BACKUP both healthy and publishing to redpanda → writer's
#           consumer link to redpanda blocked → records accumulate on
#           Kafka topics but writer reads NOTHING → archive frozen for
#           60s → unblocked, writer resumes consuming from last committed
#           offset → writer emits a gap envelope for the offsets that
#           Kafka retention may purge before it caught up (or for the
#           down window if the consumer outage controller flags it).
# Why:      The writer is the single consumer. While its link is blocked,
#           neither MAIN's nor BACKUP's records reach the archive.
#           Writer-side failure is real loss under the TWO-COLLECTOR rule.

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
