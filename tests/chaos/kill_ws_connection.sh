#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Kill WebSocket Connection ==="
echo "1. Capturing pre-kill topic offsets..."
docker compose exec redpanda rpk topic consume binance.trades -n 1 --offset end
echo "2. Killing collector to simulate ws_disconnect..."
docker kill cryptolake-collector-1
sleep 5
echo "3. Restarting collector..."
docker compose up -d collector
echo "4. Waiting 30 seconds for reconnect..."
sleep 30
echo "5. Checking for gap records..."
docker compose exec redpanda rpk topic consume binance.trades -n 20 --offset end | grep -c '"type":"gap"' || echo "No gap records found (check manually)"
echo "=== Check: Gap record with reason=ws_disconnect should exist ==="
