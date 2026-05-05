#!/usr/bin/env bash
# 12_kafka_consumer_outage.sh
#
# Scenario: writer_kafka_consumer_outage
# Chaos:    Network-isolate writer from redpanda for 60s; then restore
# Expected: NO gap. Writer's consumer reconnects from its last committed
#           offset; records are still in Kafka (48h retention) and just
#           catch up. Archive resumes cleanly.
# Flow:     MAIN+BACKUP both healthy and publishing → writer isolated →
#           consumer FETCH cancelled, heartbeat fails → writer can't
#           archive new records → 60s later isolation lifted → consumer
#           rejoins group, resumes from committed offset → catches up
#           on backlog.
# Why:      Kafka retention (48h) covers any consumer outage well below
#           that horizon. KafkaConsumerOutageDetector exists in the
#           codebase but is NOT instantiated in writer/Main.java
#           (dead code), so the kafka_consumer_outage gap reason is
#           never emitted in the current build — what matters is that
#           the writer survives the outage and doesn't archive synthetic
#           gaps for catch-up windows.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "12" "primary+backup"

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

# Assertions — writer survives a 60s isolation; consumer catches up from
# committed offset; no gap is archived because Kafka retention covers
# the catch-up window. KafkaConsumerOutageDetector is dead code in the
# current writer wiring so its gap reason cannot fire here.
expect_log_event              "writer's consumer disconnected from broker"  "Cancelled in-flight FETCH request"
expect_lifecycle_event_absent "no uncovered gap accepted on consumer outage" "GAP_ACCEPTED_NO_COVERAGE"
expect_no_gaps_check          "no gap envelopes archived (Kafka retention covered catch-up)"

verdict
