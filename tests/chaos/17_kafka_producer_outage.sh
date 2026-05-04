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

# Assertions — only MAIN's producer path failed; BACKUP covered.
expect_lifecycle_event        "MAIN collector enters kafka outage"  "COLLECTOR_KAFKA_OUTAGE_ENTERED" collector
expect_lifecycle_event        "MAIN collector exits kafka outage"   "COLLECTOR_KAFKA_OUTAGE_EXITED"  collector
expect_lifecycle_event        "writer fails over to BACKUP"         "WRITER_NOW_ARCHIVING_FROM=BACKUP"
expect_lifecycle_event        "writer switches back to MAIN"        "WRITER_NOW_ARCHIVING_FROM=MAIN"
expect_lifecycle_event_absent "no uncovered gap accepted"           "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived"

verdict
