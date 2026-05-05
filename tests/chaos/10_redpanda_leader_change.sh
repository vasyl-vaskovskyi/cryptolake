#!/usr/bin/env bash
# 10_redpanda_leader_change.sh
#
# Scenario: redpanda_brief_restart
# Chaos:    docker compose restart redpanda (brief Kafka outage)
# Expected: gap reason=kafka_producer_outage (transient — real loss if outage ≥ 30s)
# Flow:     MAIN+BACKUP both healthy → redpanda restarted → BOTH producer
#           paths blocked at the same time → writer consumer also paused
#           (no broker to read from) → redpanda recovers → both producers
#           reconnect → KafkaOutageJournal replays a gap envelope ONLY if
#           the outage window exceeds the in-process buffer capacity.
# Why:      Redpanda is the single transport between collectors and the
#           writer; restarting it takes BOTH collectors' delivery paths
#           offline simultaneously. Whether a gap is emitted depends on
#           outage length vs producer buffer + linger.ms. Borderline case.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "10" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Restarting Redpanda (simulates leader change) ==="
clean_stop_service "redpanda"

msg "Holding Redpanda down for 45s (>30s degraded threshold)…"
sleep 45

start_service "redpanda"
wait_healthy 150

msg "Waiting 120s for Kafka outage gap envelopes to be emitted…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — a 45s redpanda restart is short enough that the producer's
# 1 GiB buffer doesn't deplete and cached cluster metadata still satisfies
# partitionsFor(), so KafkaProducerHealthMonitor stays HEALTHY and
# COLLECTOR_KAFKA_OUTAGE_ENTERED does NOT fire. The PAUSED-threshold path
# is only reachable for sustained outages — see test 16 (kafka_full_outage)
# which forces a long outage to cross the buffer-depletion threshold.
# What this test actually verifies: the system survives a brief broker
# restart without any spurious gap envelope or uncovered-gap acceptance.
expect_lifecycle_event_absent "no uncovered gap accepted on brief restart" "GAP_ACCEPTED_NO_COVERAGE"
expect_only_these_gaps_check  kafka_producer_outage kafka_consumer_outage collector_restart

verdict
