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

# Hold long enough for delivery.timeout.ms (120s) to expire on at least
# one in-flight record, so the producer's record-error-rate metric goes
# above 0 and KafkaProducerHealthMonitor.probeHealth() finally returns
# false. (The probe is intentionally lenient about cached metadata; only
# real back-pressure / record errors trip the DEGRADED → PAUSED path.)
msg "Holding Redpanda down for 180s (>delivery.timeout.ms + DEGRADED window)…"
sleep 180

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

# Assertions — full kafka outage: both collectors must enter+exit kafka outage
# state. The writer records gaps via the GAP_ACCEPTED_NO_COVERAGE path
# (no parking — neither source has fresh data, decision is immediate),
# with reason=kafka_producer_outage for the journal-replayed window plus
# reason=kafka_delivery_failed for individual records that timed out
# during the outage. Both are valid loss markers.
expect_lifecycle_event   "MAIN collector enters kafka outage"      "COLLECTOR_KAFKA_OUTAGE_ENTERED" collector
expect_lifecycle_event   "MAIN collector exits kafka outage"       "COLLECTOR_KAFKA_OUTAGE_EXITED"  collector
expect_lifecycle_event   "BACKUP collector enters kafka outage"    "COLLECTOR_KAFKA_OUTAGE_ENTERED" collector-backup
expect_lifecycle_event   "BACKUP collector exits kafka outage"     "COLLECTOR_KAFKA_OUTAGE_EXITED"  collector-backup
expect_lifecycle_event   "uncovered gap accepted (no source had fresh data)" "GAP_ACCEPTED_NO_COVERAGE"
expect_gap_present_check "kafka_producer_outage gap recorded"      "kafka_producer_outage"
expect_only_these_gaps_check kafka_producer_outage kafka_delivery_failed snapshot_poll_miss

verdict
