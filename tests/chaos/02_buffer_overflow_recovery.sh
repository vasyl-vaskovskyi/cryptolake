#!/usr/bin/env bash
# 02_buffer_overflow_recovery.sh
#
# Scenario: main_buffer_overflow
# Chaos:    Drop MAIN's TCP traffic to Kafka (port 9092) via iptables for 90s
#           with a deliberately small producer buffer (~2 MB). MAIN's
#           in-process Kafka producer accumulator fills, BufferExhaustedException
#           fires, KafkaProducerHealthMonitor transitions HEALTHY → DEGRADED →
#           PAUSED, and emits a kafka_producer_outage gap journal entry.
# Expected: NO gap archived (redundancy worked) AND a positive proof that
#           the buffer-overflow code path was exercised:
#           LIFECYCLE COLLECTOR_KAFKA_OUTAGE_ENTERED fires on MAIN.
# Flow:     MAIN's producer accumulator fills → MAIN's KafkaProducerHealthMonitor
#           goes DEGRADED then PAUSED → MAIN journals the outage and stops
#           publishing → BACKUP keeps publishing normally → writer fails over
#           to BACKUP → writer archives BACKUP throughout → iptables rule
#           removed → MAIN's producer reconnects, drains, journal replays a
#           kafka_producer_outage gap envelope → CoverageFilter suppresses
#           the gap because BACKUP covered the window → writer switches back
#           to MAIN.
# Why:      Only MAIN's egress to the broker failed; BACKUP fed the writer
#           the whole time. Under the TWO-COLLECTOR rule no gap is archived,
#           but MAIN's own kafka_producer_outage event MUST fire (it's the
#           buffer-overflow code path's signature). Distinct from scenario 17
#           which exercises a coarser "MAIN egress fully blocked" path.
#
# Pre-condition: KAFKA_PRODUCER_BUFFER_MEMORY=2000000 is exported before the
# stack starts so MAIN's buffer is small enough to overflow within 90s at
# typical WS rates. The host env var is passed through to both collector
# services via docker-compose.yml.

set -euo pipefail

# Set the small buffer BEFORE init_scenario / start_stack so docker-compose
# picks it up via env-var interpolation.
export KAFKA_PRODUCER_BUFFER_MEMORY=2000000

source "$(dirname "$0")/common.sh"

init_scenario "02" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s (small buffer; allow producer to settle and prove no spurious overflow)…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Blocking MAIN's TCP traffic to Kafka (port 9092) via iptables ==="
block_kafka_egress_iptables "collector" 9092

# Hold long enough for MAIN's producer to fill its 2 MB buffer AND for
# KafkaProducerHealthMonitor to transition through DEGRADED (any failed probe)
# to PAUSED (after PAUSED_THRESHOLD_NS = 30s of degraded). 90s gives ~60s in
# PAUSED state, plenty for the LIFECYCLE event to fire.
msg "Holding iptables block for 90s (PAUSED_THRESHOLD_NS=30s; budget for LIFECYCLE fire)…"
sleep 90

msg "Restoring MAIN's Kafka egress…"
unblock_kafka_egress_iptables "collector" 9092

# wait_healthy gives MAIN's producer time to reconnect, drain, and the
# KafkaOutageJournal to replay the gap envelope.
wait_healthy 120

msg "Waiting 30s for MAIN's outage-replay gap envelope to flow through writer + CoverageFilter…"
sleep 30

run_verify "$(today)" "$HOST_DATA_DIR"

# Acceptance assertions:
# 1. No archived gap with reason=kafka_producer_outage (CoverageFilter must
#    have suppressed it because BACKUP covered the window).
assert_gap_absent "kafka_producer_outage" "$HOST_DATA_DIR"

# 2. Positive proof that MAIN's buffer-overflow code path actually fired.
#    Without this, the test could pass for the wrong reason (e.g. the
#    writer's silence-timeout failover ran but MAIN's producer never
#    actually overflowed). The COLLECTOR_KAFKA_OUTAGE_ENTERED event is
#    emitted by KafkaProducerHealthMonitor when DEGRADED → PAUSED.
assert_lifecycle_event "COLLECTOR_KAFKA_OUTAGE_ENTERED" "collector"

scenario_pass
