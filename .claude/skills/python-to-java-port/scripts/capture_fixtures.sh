#!/usr/bin/env bash
# capture_fixtures.sh
# Runs the Python collector in tap mode for a bounded window,
# then captures the Prometheus metric skeleton and verify CLI expected output.
# Outputs to cryptolake-java/parity-fixtures/{websocket-frames,metrics,verify}.

set -euo pipefail

WINDOW_SECS="${WINDOW_SECS:-600}"            # default 10 minutes
OUT_ROOT="cryptolake-java/parity-fixtures"
FRAMES_DIR="$OUT_ROOT/websocket-frames"
METRICS_DIR="$OUT_ROOT/metrics"
VERIFY_DIR="$OUT_ROOT/verify"
mkdir -p "$FRAMES_DIR" "$METRICS_DIR" "$VERIFY_DIR"

# 1. Start Python collector with tap mode.
export COLLECTOR__TAP_OUTPUT_DIR="$FRAMES_DIR"
echo "Starting Python collector in tap mode for ${WINDOW_SECS}s ..."
uv run python -m src.collector.main &
COLLECTOR_PID=$!
trap 'kill $COLLECTOR_PID 2>/dev/null || true' EXIT

# 2. Sleep a moment for streams to connect, then start metrics scrape.
sleep 15
METRICS_RAW="$(mktemp)"
for svc in collector writer; do
  # Scrape each service's /metrics endpoint. Ports match docker-compose.yml:
  # collector→8000, writer→8001. Overridable via env for non-default setups.
  case "$svc" in
    collector) PORT="${COLLECTOR_METRICS_PORT:-8000}" ;;
    writer)    PORT="${WRITER_METRICS_PORT:-8001}" ;;
  esac
  echo "Scraping $svc /metrics on :$PORT for 120s ..."
  end=$(( $(date +%s) + 120 ))
  : > "$METRICS_RAW"
  while (( $(date +%s) < end )); do
    curl -sf "http://localhost:${PORT}/metrics" >> "$METRICS_RAW" || true
    sleep 5
  done
  # Canonicalize: metric name + sorted label keys only (values dropped).
  awk '
    /^[a-zA-Z_][a-zA-Z0-9_]*(\{|$)/ {
      # strip values, keep "name{k1,k2,k3}" with sorted keys
      line=$0
      if (match(line, /^[a-zA-Z_][a-zA-Z0-9_]*/)) {
        name=substr(line, 1, RLENGTH)
      }
      keys=""
      if (match(line, /\{[^}]*\}/)) {
        inner=substr(line, RSTART+1, RLENGTH-2)
        n=split(inner, parts, ",")
        for (i=1;i<=n;i++) { split(parts[i], kv, "="); ks[i]=kv[1] }
        # sort
        asort(ks)
        for (i=1;i<=n;i++) keys = keys (i>1?",":"") ks[i]
        print name "{" keys "}"
      } else {
        print name
      }
    }
  ' "$METRICS_RAW" | sort -u > "$METRICS_DIR/$svc.txt"
done

# 3. Let collector finish the recording window.
echo "Waiting for tap window to complete ..."
wait_until=$(( $(date +%s) + WINDOW_SECS - 135 ))
while (( $(date +%s) < wait_until )); do sleep 30; done

# 4. Stop collector and let writer flush.
kill "$COLLECTOR_PID"
trap - EXIT
wait "$COLLECTOR_PID" 2>/dev/null || true
sleep 20

# 5. Snapshot verify CLI output on archives produced during the recording.
TODAY=$(date -u +"%Y-%m-%d")
uv run python -m src.cli.verify --date "$TODAY" > "$VERIFY_DIR/expected.txt" || true

echo "=== capture_fixtures.sh summary ==="
echo "frames:   $(find "$FRAMES_DIR" -type f -name '*.raw' | wc -l)"
echo "metrics:  $(wc -l < "$METRICS_DIR/collector.txt") collector + $(wc -l < "$METRICS_DIR/writer.txt") writer"
echo "verify:   $VERIFY_DIR/expected.txt ($(wc -l < "$VERIFY_DIR/expected.txt") lines)"
