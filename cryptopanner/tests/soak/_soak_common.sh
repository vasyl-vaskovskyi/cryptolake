# shellcheck shell=bash
# Helpers for tests/soak/run.sh (§14.e). Sourced, not executed.

soak::config() {
  CFG="${SOAK_CONFIG:-${REPO_ROOT}/config/dev/config.yaml}"
  SOAK_DIR="${SOAK_DIR:-/tmp/cryptopanner-soak}"
  RUN_SECONDS="${SOAK_RUN_SECONDS:-120}"
  REPLAY_HZ="${SOAK_REPLAY_HZ:-50}"
  BUCKET="${SOAK_BUCKET:-cryptopanner-dev}"
  NET="cryptopanner-soak-net"
  MINIO="cryptopanner-soak-minio"
  MC="cryptopanner-soak-mc"
  TOKEN="soak-token"
  PIDS=()
}

soak::log() { echo "[soak] $*"; }
soak::die() {
  echo "[soak] FAIL — $*" >&2
  exit 1
}

soak::cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  docker rm -f "$MINIO" "$MC" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}

soak::build() {
  if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    soak::log "SKIP_BUILD=1 — reusing existing binaries"
    return
  fi
  soak::log "building (mvn -q -DskipTests package)..."
  (cd "$REPO_ROOT" && ./mvnw -q -DskipTests package >/dev/null) || soak::die "build failed"
}

soak::bin() { echo "${REPO_ROOT}/$1/target/install/bin/$2"; }

soak::prepare_dirs() {
  rm -rf "$SOAK_DIR"
  mkdir -p "$SOAK_DIR"/{segments,sealed,staging,deploy,logs}
  printf '%s' "$TOKEN" >"$SOAK_DIR/agent.token"
  printf 'a' >"$SOAK_DIR/deploy/active-slot"
}

soak::stack_up() {
  soak::log "starting MinIO + bucket $BUCKET..."
  docker network create "$NET" >/dev/null 2>&1 || true
  docker rm -f "$MINIO" >/dev/null 2>&1 || true
  docker run -d --name "$MINIO" --network "$NET" -p 9000:9000 \
    -e MINIO_ROOT_USER=cryptopanner -e MINIO_ROOT_PASSWORD=changeme-dev \
    minio/minio:RELEASE.2024-05-10T01-41-38Z server /data >/dev/null \
    || soak::die "minio start failed"
  local i
  for i in $(seq 1 60); do
    curl -sf http://localhost:9000/minio/health/ready >/dev/null 2>&1 && break
    sleep 1
  done
  # Named + bounded so it can never orphan if MinIO is torn down before mc connects.
  docker rm -f "$MC" >/dev/null 2>&1 || true
  docker run --rm --name "$MC" --network "$NET" --entrypoint sh minio/mc -c \
    "for i in \$(seq 1 30); do mc alias set m http://$MINIO:9000 cryptopanner changeme-dev >/dev/null 2>&1 && break; sleep 1; done; mc mb -p m/$BUCKET" \
    >/dev/null 2>&1 || true

  soak::log "starting mock-binance-ws (replay ${REPLAY_HZ}Hz, wall-clock event-time)..."
  MOCK_WS_PORT=9001 MOCK_REST_PORT=9002 REPLAY_RATE_HZ="$REPLAY_HZ" MOCK_REWRITE_EVENT_TIME=1 \
    "$(soak::bin tests/mocks/binance-ws mock-binance-ws)" >"$SOAK_DIR/mock.log" 2>&1 &
  PIDS+=($!)
  sleep 2

  soak::log "starting collector (slot a)..."
  CRYPTOPANNER_SLOT=a JAVA_OPTS="-Dconfig=$CFG" \
    "$(soak::bin collector collector)" >"$SOAK_DIR/collector.log" 2>&1 &
  COLLECTOR_PID=$!
  PIDS+=("$COLLECTOR_PID")

  soak::log "starting agent (test_mode, no systemd)..."
  CRYPTOPANNER_AGENT_TOKEN="$TOKEN" JAVA_OPTS="-Dconfig=$CFG" \
    "$(soak::bin agent agent)" >"$SOAK_DIR/agent.log" 2>&1 &
  PIDS+=($!)

  soak::write_monitor_config
  soak::log "starting monitor (dashboard :9200)..."
  JAVA_OPTS="-Dconfig=$SOAK_DIR/monitor.yaml" \
    "$(soak::bin monitor monitor)" >"$SOAK_DIR/monitor.log" 2>&1 &
  PIDS+=($!)
}

soak::write_monitor_config() {
  cat >"$SOAK_DIR/monitor.yaml" <<YAML
nodes:
  - id: dev-node
    endpoint: 127.0.0.1:9100
    token_file: $SOAK_DIR/agent.token
scrape_interval_s: 2
dashboard: { listen_address: "127.0.0.1:9200", refresh_interval_s: 5 }
restart:
  backoff: [5s, 15s, 60s, 300s]
  circuit_breaker: { failure_count: 3, window: 5m }
alert:
  dedup_ttl: 1h
  correlation: { threshold_nodes: 3, window: 1m }
  warning:  { degraded_persisting: 2m, upload_backlog_age: 30m, deploy_stuck: 1h, rest_failed_poll_rate_pct: 10, rest_failed_poll_window: 10m, disk_data_pct: 80, clock_skew_s: 1 }
  critical: { extended_ws_disconnect: 5m, rest_rate_limit_persistence_hours: 2, disk_data_pct: 95, clock_skew_s: 5 }
alerting:
  telegram: { webhook_url: "" }
  whatsapp: { webhook_url: "" }
  healthchecks_url: ""
dead_man: { healthchecks_push_interval: 60s, self_test_time_utc: "23:59" }
YAML
}

soak::wait_healthy() {
  local i
  for i in $(seq 1 30); do
    curl -sf http://localhost:9100/status >/dev/null 2>&1 && break
    sleep 1
  done
  curl -sf http://localhost:9100/status >/dev/null 2>&1 || soak::die "agent /status never came up"
  for i in $(seq 1 30); do
    curl -sf http://localhost:9200/api/nodes >/dev/null 2>&1 && break
    sleep 1
  done
  curl -sf http://localhost:9200/api/nodes >/dev/null 2>&1 || soak::die "monitor /api/nodes never came up"
  soak::log "agent + monitor healthy"
}

soak::run_window_with_rotation() {
  local half=$((RUN_SECONDS / 2))
  soak::log "capturing for ${RUN_SECONDS}s; rotation at ~${half}s..."
  sleep "$half"
  soak::log "triggering one WS rotation via agent /rotation/trigger..."
  curl -sf -X POST \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    http://localhost:9100/rotation/trigger >/dev/null 2>&1 \
    && soak::log "rotation trigger accepted" \
    || soak::log "rotation trigger POST failed (continuing)"
  sleep "$half"
}

soak::assert_monitor_sees_node() {
  local nodes
  nodes="$(curl -sf http://localhost:9200/api/nodes || true)"
  echo "$nodes" | grep -q '"reachable":true' || soak::die "monitor: node not reachable"
  echo "$nodes" | grep -q '"cryptopanner-agent"[^}]*"running"' \
    || soak::log "WARN: agent component not 'running' in monitor view"
  soak::log "monitor sees node reachable (live /api/nodes scrape OK)"
  MONITOR_NODES_JSON="$nodes"
}

soak::stop_collector() {
  soak::log "SIGTERM collector (flush + seal-on-shutdown)..."
  kill -TERM "$COLLECTOR_PID" 2>/dev/null || true
  local i
  for i in $(seq 1 20); do
    kill -0 "$COLLECTOR_PID" 2>/dev/null || break
    sleep 1
  done
  kill -9 "$COLLECTOR_PID" 2>/dev/null || true
}

soak::discover_date_hour() {
  local stream_dir="$SOAK_DIR/segments/btcusdt/trade"
  [[ -d "$stream_dir" ]] || soak::die "no trade segments captured"
  SOAK_DATE="$(ls "$stream_dir" | sort | tail -1)"
  SOAK_HOUR="$(ls "$stream_dir/$SOAK_DATE/" \
    | grep -oE '^minute-([0-9]{2})-' | sed -E 's/^minute-([0-9]{2})-.*/\1/' \
    | sort -u | tail -1)"
  soak::log "captured date=$SOAK_DATE hour=$SOAK_HOUR"
}

soak::assert_event_time_progression() {
  # With the wall-clock event-time rewrite, frames carry "now" — so the captured date is today and
  # the run fills consecutive real-time minutes (vs. the fixture's fixed historical minute).
  local today minutes
  today="$(date -u +%Y-%m-%d)"
  [[ "$SOAK_DATE" == "$today" ]] \
    || soak::die "event-time rewrite not active: captured date $SOAK_DATE != today $today"
  minutes="$(ls "$SOAK_DIR/segments/btcusdt/trade/$SOAK_DATE/" 2>/dev/null \
    | grep -oE 'minute-[0-9]{2}-[0-9]{2}' | sort -u | wc -l | tr -d ' ')"
  CAPTURED_MINUTES="$minutes"
  [[ "${minutes:-0}" -ge 2 ]] \
    || soak::die "expected >=2 distinct captured minutes (sustained progression), got $minutes"
  soak::log "event-time rewrite active: $minutes distinct real-time minutes captured on $SOAK_DATE"
}

soak::seal_upload_verify() {
  soak::discover_date_hour
  soak::assert_event_time_progression
  # Sealer + uploader now seal an empty hour as a zero-record manifest instead of hard-failing
  # (§12.k gap-surfacing), so both must exit 0 even when the REST-derived streams have no data.
  soak::log "sealing hour $SOAK_HOUR..."
  JAVA_OPTS="-Dconfig=$CFG" "$(soak::bin sealer sealer)" \
    --date "$SOAK_DATE" --hour "$SOAK_HOUR" >"$SOAK_DIR/sealer.log" 2>&1 \
    || soak::die "sealer exited non-zero (see $SOAK_DIR/sealer.log)"

  # Snapshot the sealed (symbol,stream) pairs BEFORE upload — the uploader deletes local sealed
  # files once the S3 upload succeeds (durability handoff), so this list must be taken now.
  local manifests
  manifests=$(find "$SOAK_DIR/sealed" -path "*/$SOAK_DATE/hour-$SOAK_HOUR.manifest.json" | sort)
  # rotation evidence (best-effort: each WS connection gets an independent replay, so the shadow's
  # overlap minute diverges from the primary's and equivalence rarely passes cleanly — reliable
  # rotation needs synchronized dual-connection fan-out, §14.c) — read now, before upload deletes
  # the local manifests.
  if [[ -n "$manifests" ]] && grep -lqs 'connection_rotation_events' $manifests 2>/dev/null \
    && ! grep -hs 'connection_rotation_events' $manifests | grep -q '"connection_rotation_events" : \[ \]'; then
    ROTATION_RECORDED="yes"
  else
    ROTATION_RECORDED="no"
  fi

  soak::log "uploading hour $SOAK_HOUR..."
  JAVA_OPTS="-Dconfig=$CFG" "$(soak::bin uploader uploader)" \
    --date "$SOAK_DATE" --hour "$SOAK_HOUR" >"$SOAK_DIR/uploader.log" 2>&1 \
    || soak::die "uploader exited non-zero (see $SOAK_DIR/uploader.log)"

  # Gate: every (symbol,stream) that sealed a manifest must verify ERRORS=0 from S3.
  SEALED_STREAMS=0
  VERIFY_ERRORS=0
  : >"$SOAK_DIR/verify.log"
  local m rel symbol stream errs
  while IFS= read -r m; do
    [[ -n "$m" ]] || continue
    rel="${m#"$SOAK_DIR"/sealed/}"
    symbol="${rel%%/*}"
    stream="$(echo "$rel" | cut -d/ -f2)"
    SEALED_STREAMS=$((SEALED_STREAMS + 1))
    "$(soak::bin verify verify)" verify \
      --config "$CFG" --date "$SOAK_DATE" --hour "$SOAK_HOUR" \
      --symbol "$symbol" --stream "$stream" >>"$SOAK_DIR/verify.log" 2>&1 || true
    errs="$(grep -oE 'ERRORS=[0-9]+' "$SOAK_DIR/verify.log" | tail -1 | cut -d= -f2)"
    VERIFY_ERRORS=$((VERIFY_ERRORS + ${errs:-1}))
    soak::log "  verify $symbol/$stream → ERRORS=${errs:-?}"
  done <<<"$manifests"
  [[ "$SEALED_STREAMS" -ge 5 ]] || soak::die "only $SEALED_STREAMS streams sealed (expected >=5)"
  soak::log "verified $SEALED_STREAMS streams; total ERRORS=$VERIFY_ERRORS"
}

soak::summary() {
  echo
  soak::log "================= SOAK SUMMARY ================="
  soak::log "captured:           $SOAK_DATE hour $SOAK_HOUR (${CAPTURED_MINUTES:-?} distinct minutes)"
  soak::log "streams verified:   $SEALED_STREAMS"
  soak::log "verify ERRORS:      $VERIFY_ERRORS"
  soak::log "monitor saw node:   reachable"
  soak::log "rotation recorded:  $ROTATION_RECORDED (best-effort; needs synced dual-fanout, §14.c)"
  soak::log "logs:               $SOAK_DIR/*.log"
  soak::log "==============================================="
  [[ "$VERIFY_ERRORS" == "0" ]] || soak::die "verify reported ERRORS=$VERIFY_ERRORS across sealed streams"
  soak::log "SOAK PASSED — $SEALED_STREAMS streams captured→sealed→uploaded→verified clean; monitor live"
}
