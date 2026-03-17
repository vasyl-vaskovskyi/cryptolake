#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Depth Reconnect Inflight ==="
echo "1. Let the collector stream depth updates for 30 seconds..."
sleep 30
echo "2. Restarting collector during active depth flow..."
docker kill cryptolake-collector-1
sleep 5
docker compose up -d collector
echo "3. Waiting 60 seconds for depth snapshot resync..."
sleep 60
echo "4. Running archive verification for depth data..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
echo "=== Check: depth replay remains valid after reconnect and snapshot recovery ==="
