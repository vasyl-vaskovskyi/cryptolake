#!/usr/bin/env bash
# 23_kafka_full_outage.sh
#
# Scenario: redpanda_full_outage_long
# Chaos:    Stop redpanda completely; let collectors accumulate
#           KafkaOutageJournal entries for an extended outage window;
#           restart redpanda
# Expected: gap reason=kafka_producer_outage (real loss)
# Flow:     MAIN+BACKUP both healthy and receiving from Binance →
#           redpanda fully stopped → both producers' egress fails;
#           records overflow producer buffers → BOTH collectors append
#           KafkaOutageJournal entries for the affected streams →
#           writer's consumer also has nothing to read → redpanda
#           restarted → on producer-side recovery KafkaOutageJournal
#           replays one kafka_producer_outage envelope per stream
#           covering the outage window.
# Why:      Redpanda outage takes BOTH collectors' delivery paths down
#           AND blocks the writer's consumer simultaneously. The
#           TWO-COLLECTOR rule's "BOTH fail" case applies (mediated
#           by the shared transport). Real loss; gap is correct.

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
