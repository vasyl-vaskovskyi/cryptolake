#!/usr/bin/env bash
# tests/skeleton/run-skeleton.sh
# End-to-end smoke test for the walking-skeleton pipeline.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

# 1) Build everything (skip tests; they have their own runs).
echo "[skeleton] building..."
./mvnw -q -DskipTests package

# 2) Start MinIO in background (docker required).
MINIO_NAME="cryptopanner-skeleton-minio"
docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
docker run -d --name "$MINIO_NAME" -p 9000:9000 \
  -e MINIO_ROOT_USER=cryptopanner \
  -e MINIO_ROOT_PASSWORD=changeme-dev \
  minio/minio:RELEASE.2024-05-10T01-41-38Z server /data >/dev/null

# 3) Start the mock WS server (Java).
"$REPO_ROOT/tests/mocks/binance-ws/target/install/bin/mock-binance-ws" &
MOCK_PID=$!

# Cleanup on exit (success or failure).
cleanup() {
  kill "$MOCK_PID" >/dev/null 2>&1 || true
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 4) Wait for both services to be ready.
echo "[skeleton] waiting for MinIO..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:9000/minio/health/ready >/dev/null 2>&1; then
    echo "[skeleton] MinIO ready"
    break
  fi
  sleep 1
done
# mock-ws starts immediately; give it a beat.
sleep 1

# 5) Clean local data from prior runs.
rm -rf /tmp/cryptopanner
mkdir -p /tmp/cryptopanner/segments /tmp/cryptopanner/sealed

# 6) Run Collector — it self-exits after collector_max_runtime_s seconds.
echo "[skeleton] running Collector..."
JAVA_OPTS="-Dconfig=$REPO_ROOT/config/dev/skeleton.yaml" \
  "$REPO_ROOT/collector/target/install/bin/collector"

# 7) Determine which (date, hour) was captured.
#    Segment layout: segments/btcusdt/trade/<date>/minute-<HH>-<mm>.jsonl.zst
STREAM_DIR="/tmp/cryptopanner/segments/btcusdt/trade"
DATE=$(ls "$STREAM_DIR" | sort | tail -1)
# Extract the hour (HH) from the first part of minute-HH-mm filenames.
HOUR=$(ls "$STREAM_DIR/$DATE/" \
  | grep -oE '^minute-([0-9]{2})-' \
  | sed -E 's/^minute-([0-9]{2})-.*/\1/' \
  | sort -u | tail -1)
echo "[skeleton] sealing hour $HOUR of $DATE"

# 8) Run Sealer.
JAVA_OPTS="-Dconfig=$REPO_ROOT/config/dev/skeleton.yaml" \
  "$REPO_ROOT/sealer/target/install/bin/sealer" \
  --date "$DATE" --hour "$HOUR"

# 9) Run Uploader.
JAVA_OPTS="-Dconfig=$REPO_ROOT/config/dev/skeleton.yaml" \
  "$REPO_ROOT/uploader/target/install/bin/uploader" \
  --date "$DATE" --hour "$HOUR"

# 10) Run Verify.
"$REPO_ROOT/verify/target/install/bin/verify" verify \
  --endpoint http://localhost:9000 \
  --bucket cryptopanner-dev \
  --node-id dev-node \
  --symbol btcusdt \
  --stream trade \
  --date "$DATE" \
  --hour "$HOUR" \
  --access-key cryptopanner \
  --secret-key changeme-dev

echo "[skeleton] OK"
