# shellcheck shell=bash
# Common helpers for chaos scenarios. Sourced — not executed.
#
# Each scenario is responsible for:
#   1) setting SCENARIO_NUM (two-digit string, e.g. "01") and SCENARIO_NAME before sourcing,
#   2) calling chaos::bootstrap_stack, then injecting its fault,
#   3) calling chaos::run_seal_upload_verify to finish the pipeline,
#   4) running its own per-scenario assertions on the local manifests.
#
# Cleanup is automatic via the EXIT trap set in chaos::bootstrap_stack.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHAOS_CFG="${REPO_ROOT}/config/dev/skeleton.yaml"
CHAOS_DATA_DIR="/tmp/cryptopanner"   # skeleton.yaml-pinned; one scenario at a time.

chaos::log() {
  echo "[chaos-${SCENARIO_NUM}] $*"
}

chaos::die() {
  echo "[chaos-${SCENARIO_NUM}] FAIL — $*" >&2
  exit 1
}

chaos::cleanup() {
  # Kill any orphan collector/mock processes we spawned.
  pkill -f "collector/target/install/bin/collector" 2>/dev/null || true
  pkill -f "binance-ws/target/install/bin/mock-binance-ws" 2>/dev/null || true
  if [[ -n "${MINIO_NAME:-}" ]]; then
    docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
  fi
}

chaos::bootstrap_stack() {
  : "${SCENARIO_NUM:?SCENARIO_NUM must be set before sourcing _common.sh}"
  MINIO_NAME="cryptopanner-chaos-${SCENARIO_NUM}-minio"
  trap chaos::cleanup EXIT

  chaos::log "rebuilding (skip tests)..."
  (cd "$REPO_ROOT" && ./mvnw -q -DskipTests package >/dev/null)

  chaos::log "starting MinIO ($MINIO_NAME) on :9000..."
  docker rm -f "$MINIO_NAME" >/dev/null 2>&1 || true
  docker run -d --name "$MINIO_NAME" -p 9000:9000 \
    -e MINIO_ROOT_USER=cryptopanner \
    -e MINIO_ROOT_PASSWORD=changeme-dev \
    minio/minio:RELEASE.2024-05-10T01-41-38Z server /data >/dev/null

  chaos::log "starting mock-binance-ws on :9001/:9002..."
  "${REPO_ROOT}/tests/mocks/binance-ws/target/install/bin/mock-binance-ws" \
    >"${CHAOS_DATA_DIR}.mock.log" 2>&1 &
  MOCK_PID=$!

  for _ in $(seq 1 30); do
    if curl -sf http://localhost:9000/minio/health/ready >/dev/null 2>&1; then break; fi
    sleep 1
  done
  sleep 1
  chaos::log "stack ready (mock pid=$MOCK_PID)"

  rm -rf "$CHAOS_DATA_DIR"
  mkdir -p "${CHAOS_DATA_DIR}/segments" "${CHAOS_DATA_DIR}/sealed"
}

# Spawns the collector in the background and sets CHAOS_COLLECTOR_PID. Must run in the
# parent shell (not a $(...) subshell) so the caller's `wait $CHAOS_COLLECTOR_PID` succeeds.
chaos::collector_start_bg() {
  local log_path="$1"
  JAVA_OPTS="-Dconfig=${CHAOS_CFG}" \
    "${REPO_ROOT}/collector/target/install/bin/collector" >"$log_path" 2>&1 &
  CHAOS_COLLECTOR_PID=$!
}

# Determine which (date, hour) was captured.
chaos::discover_date_hour() {
  local stream_dir="${CHAOS_DATA_DIR}/segments/btcusdt/trade"
  [[ -d "$stream_dir" ]] || chaos::die "no trade segments captured"
  CHAOS_DATE=$(ls "$stream_dir" | sort | tail -1)
  CHAOS_HOUR=$(ls "$stream_dir/$CHAOS_DATE/" \
    | grep -oE '^minute-([0-9]{2})-' \
    | sed -E 's/^minute-([0-9]{2})-.*/\1/' \
    | sort -u | tail -1)
  chaos::log "captured date=$CHAOS_DATE hour=$CHAOS_HOUR"
}

# Seal + Upload + Verify. Caller may assert on local manifests before sealing-side files
# are deleted by Uploader (Sealer keeps them only until upload succeeds).
chaos::run_sealer() {
  JAVA_OPTS="-Dconfig=${CHAOS_CFG}" \
    "${REPO_ROOT}/sealer/target/install/bin/sealer" \
    --date "$CHAOS_DATE" --hour "$CHAOS_HOUR"
}

chaos::run_uploader() {
  JAVA_OPTS="-Dconfig=${CHAOS_CFG}" \
    "${REPO_ROOT}/uploader/target/install/bin/uploader" \
    --date "$CHAOS_DATE" --hour "$CHAOS_HOUR"
}

chaos::run_verify() {
  "${REPO_ROOT}/verify/target/install/bin/verify" verify \
    --config "$CHAOS_CFG" \
    --date "$CHAOS_DATE" \
    --hour "$CHAOS_HOUR"
}

# Asserts the local manifest carries a "<key>" : "<value>" pair (whitespace-tolerant —
# Jackson's pretty-printer uses `"key" : "value"` with spaces on both sides of the colon).
chaos::assert_manifest_kv() {
  local manifest_path="$1"
  local key="$2"
  local value="$3"
  [[ -f "$manifest_path" ]] || chaos::die "manifest not found: $manifest_path"
  grep -E -q "\"${key}\"[[:space:]]*:[[:space:]]*\"${value}\"" "$manifest_path" \
    || chaos::die "manifest $manifest_path lacks ${key} = ${value}"
}

chaos::pass() {
  chaos::log "PASS"
}
