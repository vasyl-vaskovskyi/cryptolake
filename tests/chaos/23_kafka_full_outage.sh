#!/usr/bin/env bash
# 23_kafka_full_outage.sh
#
# Invariant: Stop Redpanda entirely so all collectors lose Kafka producer
# connectivity. Each collector's KafkaProducerHealthMonitor enters "paused"
# state and writes a KafkaOutageJournal entry. When Redpanda restarts, each
# collector reads its journal and emits a kafka_producer_outage gap envelope
# spanning the outage window. verify exits 0 with ERRORS=0.
#
# Depends on: KafkaProducerHealthMonitor + KafkaOutageJournal (Tasks A2.2, A2.3)
#
# Expected gap reason: kafka_producer_outage

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "23" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Stopping Redpanda (full Kafka outage) ==="
clean_stop_service "redpanda"

# Hold for 60s — both collectors must accumulate KafkaOutageJournal entries
# (KafkaProducerHealthMonitor needs >30s to enter paused state)
msg "Holding Redpanda down for 60s…"
sleep 60

# Confirm KafkaOutageJournal files were created
PRIMARY_JOURNAL="${HOST_DATA_DIR}/cryptolake/binance-collector-01/kafka_outage.json"
BACKUP_JOURNAL="${HOST_DATA_DIR}/cryptolake/binance-collector-backup/kafka_outage.json"
if [[ -f "$PRIMARY_JOURNAL" ]]; then
    msg "Primary KafkaOutageJournal: $(cat "$PRIMARY_JOURNAL")"
else
    msg "WARNING: Primary KafkaOutageJournal not found at ${PRIMARY_JOURNAL}"
fi
if [[ -f "$BACKUP_JOURNAL" ]]; then
    msg "Backup KafkaOutageJournal: $(cat "$BACKUP_JOURNAL")"
else
    msg "WARNING: Backup KafkaOutageJournal not found at ${BACKUP_JOURNAL}"
fi

msg "Restarting Redpanda…"
start_service "redpanda"
wait_healthy 150

msg "Waiting 120s for kafka_producer_outage gap envelopes to be replayed and archived…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"
assert_gap_present "kafka_producer_outage" "$HOST_DATA_DIR"

scenario_pass
