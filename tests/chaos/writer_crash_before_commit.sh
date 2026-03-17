#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Writer Crash Before Commit ==="
echo "1. Sending SIGKILL to writer (simulates crash mid-flush)..."
docker kill -s KILL cryptolake-writer-1
echo "2. Restarting writer..."
docker compose up -d writer
echo "3. Waiting 60 seconds for recovery..."
sleep 60
echo "4. Running full verification..."
docker compose exec writer uv run cryptolake verify --date "$(date -u +%Y-%m-%d)" --base-dir "${HOST_DATA_DIR:-/data}" --full
echo "=== Check: No duplicates, no corrupt zstd frames, verification passes ==="
