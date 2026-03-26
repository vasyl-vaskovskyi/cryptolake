#!/usr/bin/env bash
# CryptoLake disk usage monitor (runs via cron every 6 hours)
set -euo pipefail

THRESHOLD=80
DATA_DIR="/data"

for MOUNT in "/" "$DATA_DIR"; do
    if [[ -d "$MOUNT" ]]; then
        USAGE=$(df "$MOUNT" --output=pcent | tail -1 | tr -d ' %')
        if [[ "$USAGE" -gt "$THRESHOLD" ]]; then
            logger -p user.warning -t cryptolake-disk-monitor \
                "WARNING: $MOUNT at ${USAGE}% (threshold: ${THRESHOLD}%)"
        fi
    fi
done
