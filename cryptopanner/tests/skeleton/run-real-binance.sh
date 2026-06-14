#!/usr/bin/env bash
# tests/skeleton/run-real-binance.sh
# End-to-end smoke test against the REAL Binance USD-M Futures WS.
# Variant of run-skeleton.sh that skips the mock-binance-ws and points the
# Collector at wss://fstream.binance.com/ws instead.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REPO_ROOT"

CONFIG="$REPO_ROOT/config/dev/real-binance.yaml"

# 1) Build everything (skip tests; they have their own runs).
echo "[real-binance] building..."
./mvnw -q -DskipTests package

# 2) Start MinIO in background (docker required).
MINIO_NAME="cryptopanner-real-minio"
docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
docker run -d --name "$MINIO_NAME" -p 9000:9000 \
  -e MINIO_ROOT_USER=cryptopanner \
  -e MINIO_ROOT_PASSWORD=changeme-dev \
  minio/minio:RELEASE.2024-05-10T01-41-38Z server /data >/dev/null

cleanup() {
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# 3) Wait for MinIO to be ready.
echo "[real-binance] waiting for MinIO..."
for i in $(seq 1 30); do
  if curl -sf http://localhost:9000/minio/health/ready >/dev/null 2>&1; then
    echo "[real-binance] MinIO ready"
    break
  fi
  sleep 1
done

# 4) Clean local data from prior runs.
rm -rf /tmp/cryptopanner-real
mkdir -p /tmp/cryptopanner-real/segments /tmp/cryptopanner-real/sealed

# 5) Run Collector against real Binance (self-exits after collector_max_runtime_s).
echo "[real-binance] running Collector against wss://fstream.binance.com/ws..."
JAVA_OPTS="-Dconfig=$CONFIG" \
  "$REPO_ROOT/collector/target/install/bin/collector"

# 6) Determine which (date, hour) was captured.
STREAM_DIR="/tmp/cryptopanner-real/segments/btcusdt/trade"
DATE=$(ls "$STREAM_DIR" | sort | tail -1)
HOUR=$(ls "$STREAM_DIR/$DATE/" \
  | grep -oE '^minute-([0-9]{2})-' \
  | sed -E 's/^minute-([0-9]{2})-.*/\1/' \
  | sort -u | tail -1)
echo "[real-binance] sealing hour $HOUR of $DATE"
echo "[real-binance] minute files captured:"
ls -lh "$STREAM_DIR/$DATE/" | head -20

# 7) Run Sealer.
JAVA_OPTS="-Dconfig=$CONFIG" \
  "$REPO_ROOT/sealer/target/install/bin/sealer" \
  --date "$DATE" --hour "$HOUR"

# 8) Run Uploader.
JAVA_OPTS="-Dconfig=$CONFIG" \
  "$REPO_ROOT/uploader/target/install/bin/uploader" \
  --date "$DATE" --hour "$HOUR"

# 9) Run Verify across all configured subscriptions.
"$REPO_ROOT/verify/target/install/bin/verify" verify \
  --config "$CONFIG" \
  --date "$DATE" \
  --hour "$HOUR"

echo "[real-binance] OK"
