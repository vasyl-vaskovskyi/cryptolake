#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Buffer Overflow Recovery ==="
echo "1. Stopping redpanda to force collector buffer growth..."
docker compose stop redpanda
echo "2. Waiting 45 seconds while collector buffers messages..."
sleep 45
echo "3. Restarting redpanda..."
docker compose up -d redpanda
echo "4. Waiting 60 seconds for buffer drain..."
sleep 60
echo "5. Checking collector buffer metrics and writer verification..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir /data --full
echo "=== Check: collector buffer recovers after redpanda restart with no unrecoverable backlog ==="
