#!/usr/bin/env bash
# Set 30-minute retention on all backup.* Redpanda topics.
set -euo pipefail

BROKER="${1:-redpanda:9092}"
RETENTION_MS=1800000  # 30 minutes

echo "Waiting for backup topics to be created..."
sleep 10

for TOPIC in $(rpk topic list --brokers "$BROKER" 2>/dev/null | grep '^backup\.' | awk '{print $1}'); do
    echo "Setting retention ${RETENTION_MS}ms on ${TOPIC}"
    rpk topic alter-config "$TOPIC" --set "retention.ms=${RETENTION_MS}" --brokers "$BROKER"
done

echo "Done."
