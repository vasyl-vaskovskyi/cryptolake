#!/usr/bin/env bash
# 17_kafka_producer_outage.sh
#
# Scenario: main_kafka_producer_outage
# Chaos:    iptables-block MAIN→redpanda for 60s; then unblock
# Expected: NO gap (redundancy worked)
# Flow:     MAIN+BACKUP both healthy → MAIN's producer can no longer
#           reach redpanda → MAIN's records pile up in its in-process
#           buffer → BACKUP's producer is unaffected, BACKUP keeps
#           publishing to redpanda → writer consumes BACKUP's records
#           and archives them → MAIN's egress restored, MAIN drains
#           buffer → writer switches back to MAIN.
# Why:      Only MAIN's producer path failed. BACKUP fed Kafka the
#           whole time; no window had zero archived data. No gap
#           under the TWO-COLLECTOR rule.

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
assert_gap_absent "kafka_producer_outage" "$HOST_DATA_DIR"

scenario_pass
