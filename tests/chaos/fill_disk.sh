#!/usr/bin/env bash
set -euo pipefail

echo "=== Chaos: Fill Disk ==="
echo "1. Creating large file in data volume..."
docker compose exec writer dd if=/dev/zero of=/data/fill_disk.tmp bs=1M count=500 2>/dev/null || true
echo "2. Checking disk usage..."
docker compose exec writer df -h /data
echo "3. Waiting 60 seconds..."
sleep 60
echo "4. Checking writer health..."
curl -s http://localhost:8001/health || echo "Writer health check failed"
echo "5. Cleaning up..."
docker compose exec writer rm -f /data/fill_disk.tmp
echo "6. Waiting 30 seconds for recovery..."
sleep 30
echo "=== Check: Writer should pause at 95%, resume after cleanup ==="
