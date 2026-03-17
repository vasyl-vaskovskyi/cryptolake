#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Kill Writer ==="
echo "1. Killing writer container..."
docker kill cryptolake-writer-1
echo "2. Waiting 120 seconds (data accumulates in Redpanda)..."
sleep 120
echo "3. Restarting writer..."
docker compose up -d writer
echo "4. Waiting 60 seconds for catch-up..."
sleep 60
echo "5. Running verification..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
echo "=== Check: Errors should be 0, writer should have caught up ==="
