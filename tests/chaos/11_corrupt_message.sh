#!/usr/bin/env bash
# 11_corrupt_message.sh
#
# Chaos:    Produce a malformed envelope to a topic the writer reads
# Expected: gap reason=deserialization_error (real loss)
# Why:      The corrupt envelope is unreadable; that record is lost.

set -euo pipefail
source "$(dirname "$0")/common.sh"

init_scenario "11" "primary+backup"

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

assert_gap_present "deserialization_error" "$HOST_DATA_DIR"

scenario_pass
