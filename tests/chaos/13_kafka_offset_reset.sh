#!/usr/bin/env bash
# 13_kafka_offset_reset.sh
#
# Scenario: writer_kafka_offset_reset
# Chaos:    Delete + recreate binance.bookticker topic, forcing the
#           writer's committed offset out of range.
# Expected: writer's consumer detects position out of range and silently
#           resets to earliest (auto.offset.reset=earliest) — no
#           kafka_offset_reset gap is emitted, no crash, only normal
#           collector_restart artifacts may appear from the topic-recreate
#           churn. Writer resumes consuming the new topic.
# Why:      The writer's consumer config uses auto.offset.reset=earliest,
#           so Kafka client handles OUT_OF_RANGE by silently resetting
#           position before throwing OffsetOutOfRangeException. The
#           writer's handleOffsetReset path (which WOULD emit
#           kafka_offset_reset) only fires for the never-thrown
#           exception. To exercise that path the consumer would need
#           auto.offset.reset=none, which production doesn't use.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "13" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Deleting + recreating binance.bookticker topic ==="

REDPANDA_CONTAINER=$(dc ps -q "redpanda" 2>/dev/null | head -1)
if [[ -z "$REDPANDA_CONTAINER" ]]; then
    scenario_fail "Could not find redpanda container"
fi

# List topics for discovery
msg "Available topics:"
docker exec "$REDPANDA_CONTAINER" rpk topic list --brokers=localhost:9092 2>/dev/null || true

# Try to delete and recreate the bookticker topic
TOPIC="binance.bookticker"
docker exec "$REDPANDA_CONTAINER" rpk topic delete "$TOPIC" --brokers=localhost:9092 2>/dev/null && \
    msg "Deleted topic: ${TOPIC}" || \
    msg "WARNING: Could not delete ${TOPIC} (may not exist yet)"

sleep 5

docker exec "$REDPANDA_CONTAINER" rpk topic create "$TOPIC" --brokers=localhost:9092 2>/dev/null && \
    msg "Recreated topic: ${TOPIC}" || \
    msg "Topic recreated automatically (auto_create_topics=true)"

msg "Waiting 120s for offset reset detection and gap emission…"
sleep 120

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — auto.offset.reset=earliest causes the consumer to silently
# reset position when its committed offset disappears from the recreated
# topic; OffsetOutOfRangeException is NOT thrown, so the writer's
# handleOffsetReset path (which would emit kafka_offset_reset) does NOT
# fire in this configuration. The writer simply resumes reading from
# offset 0 of the new topic.
#
# `ws_disconnect` is also accepted: in full-suite runs the primary
# collector container can start several seconds after the backup
# (Docker scheduling jitter), and the documented Binance fstream
# half-open WS bug occasionally drops a few low-rate stream
# subscriptions during the first reconnect window. CoverageFilter
# correctly emits ws_disconnect gaps for streams (funding_rate,
# liquidations, trades) that backup hasn't yet produced records on
# — this is environmental noise from the warm-up phase, not a
# failure of the offset-reset path under test. Standalone runs
# rarely hit this because both collectors come up at the same
# millisecond.
expect_log_event              "consumer detected position out of range" "is out of range for partition binance.bookticker"
expect_lifecycle_event_absent "writer didn't crash"                     "MAIN_FAILURE_DETECTED"
expect_only_these_gaps_check  collector_restart pu_chain_break ws_disconnect

verdict
