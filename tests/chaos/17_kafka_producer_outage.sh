#!/usr/bin/env bash
# 17_kafka_producer_outage.sh
#
# Invariant: Block the primary collector's connection to Redpanda for 60s.
# The KafkaProducerHealthMonitor detects the outage (probe fails ≥30s →
# enters "paused" state, writes KafkaOutageJournal). On recovery it reads
# the journal and emits a kafka_producer_outage gap envelope spanning the
# outage window. verify exits 0 with ERRORS=0.
#
# Depends on: KafkaProducerHealthMonitor (Task A2.3) + KafkaOutageJournal (A2.2)
#
# Expected gap reason: kafka_producer_outage

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "17" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Isolating PRIMARY collector from Redpanda (kafka_producer_outage) ==="
# Fully isolate the collector from all networks
block_service_network "collector"

# Hold for 60s — must exceed the 30s degraded→paused threshold
msg "Holding isolation for 60s (>30s KafkaProducerHealthMonitor threshold)…"
sleep 60

msg "Restoring collector network (triggers journal replay + gap emission)…"
restore_service_network "collector"
wait_healthy 120

msg "Waiting 90s for kafka_producer_outage gap to be emitted and archived…"
sleep 90

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "kafka_producer_outage" "$HOST_DATA_DIR"

scenario_pass
