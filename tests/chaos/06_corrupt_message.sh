#!/usr/bin/env bash
# 06_corrupt_message.sh
#
# Scenario: corrupt_message
# Chaos:    Produce 3 malformed envelopes to a writer-consumed topic
# Expected: writer logs `corrupt_message_skipped` ERROR per record and
#           skips them WITHOUT halting and WITHOUT emitting a gap envelope.
# Flow:     MAIN+BACKUP both healthy and delivering → 3 corrupt envelopes
#           land on binance.bookticker → writer's RecordHandler hits
#           JsonParseException → emits an ERROR-level corrupt_message_skipped
#           per record and continues consuming → archive remains valid →
#           verify reports 0 errors.
# Why:      The current contract is "be robust to corrupt input — log
#           loudly so monitoring catches it, but don't halt and don't
#           pollute the archive with synthetic gaps for what may be a
#           producer bug rather than data loss". This test pins down that
#           behavior and will fail if the system either crashes on bad
#           input or silently swallows the failure without an audit log.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "06" "primary+backup"

start_stack "primary+backup"
wait_healthy 150

msg "Warm-up 60s…"
warm_up 60
wait_data_flowing "bookticker" 30

msg "=== CHAOS: Producing corrupt envelope to binance.bookticker topic ==="

# Use rpk (available in the redpanda container) to produce a corrupt message
REDPANDA_CONTAINER=$(dc ps -q "redpanda" 2>/dev/null | head -1)
if [[ -z "$REDPANDA_CONTAINER" ]]; then
    scenario_fail "Could not find redpanda container"
fi

# Produce 3 malformed messages to the topic
for i in 1 2 3; do
    CORRUPT_PAYLOAD="CORRUPT_PAYLOAD_${i}_NOT_JSON_{{{{{{{{{{"
    docker exec "$REDPANDA_CONTAINER" \
        sh -c "echo '${CORRUPT_PAYLOAD}' | rpk topic produce binance.bookticker --brokers=localhost:9092" \
        2>/dev/null || {
            msg "WARNING: rpk produce failed — trying alternative topic name"
            docker exec "$REDPANDA_CONTAINER" \
                sh -c "echo '${CORRUPT_PAYLOAD}' | rpk topic produce binance.BTCUSDT.bookticker --brokers=localhost:9092" \
                2>/dev/null || true
        }
    msg "Produced corrupt message ${i}"
done

msg "Waiting 60s for deserialization_error gap to appear…"
sleep 60

run_verify "$(today)" "$HOST_DATA_DIR"

# Assertions — corrupt input is logged at ERROR and skipped; archive stays clean.
expect_log_event             "corrupt records logged at ERROR"   "corrupt_message_skipped"
expect_lifecycle_event_absent "writer did NOT crash (failover not triggered)" "MAIN_FAILURE_DETECTED"
expect_no_gaps_check          "no gap envelopes archived for corrupt records"

verdict
