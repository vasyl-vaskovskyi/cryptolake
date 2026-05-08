#!/usr/bin/env bash
# Roll the CryptoLake stack to a new image tag without losing data.
#
# Sequence (writer first, capture last) — see
# docs/2026-05-08-rolling-update-plan.md for the full rationale:
#
#   writer → consolidation → backfill → collector-backup → collector
#
# Each step waits for the service's /ready endpoint before moving on.
# On healthcheck failure, the failing service is rolled back to the
# previous tag and the script aborts. Already-updated services stay on
# the new tag.
#
# Usage:
#   sudo scripts/rolling-update.sh <tag>              # full sequence
#   sudo scripts/rolling-update.sh <tag> writer       # just writer
#   sudo scripts/rolling-update.sh <tag> --force      # ignore maintenance window
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_DIR="$(pwd)"
ENV_FILE="$REPO_DIR/.env"
COMPOSE=(docker compose -f docker-compose.yml -f docker-compose.vps.yml)

# Order matters — see docs/2026-05-08-rolling-update-plan.md
SEQUENCE=(writer consolidation backfill collector-backup collector)

# /ready endpoint per service. Empty = no healthcheck endpoint; we
# fall back to "container is running and didn't crash" for those.
declare -A HEALTH_URL=(
  [writer]="http://127.0.0.1:8001/ready"
  [consolidation]="http://127.0.0.1:8003/ready"
  [backfill]=""
  [collector-backup]="http://127.0.0.1:8004/ready"
  [collector]="http://127.0.0.1:8000/ready"
)

HEALTH_TIMEOUT_SEC=90

red()    { printf '\033[0;31m[x]\033[0m %s\n' "$*" >&2; }
yellow() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
green()  { printf '\033[0;32m[+]\033[0m %s\n' "$*"; }
section(){ echo; printf '\033[1m== %s ==\033[0m\n' "$*"; }

usage() { sed -n '2,18p' "$0"; exit 2; }

[[ $# -ge 1 ]] || usage
NEW_TAG=$1; shift

FORCE=0
SUBSET=()
for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help) usage ;;
    *)
      [[ -n "${HEALTH_URL[$arg]+x}" ]] || { red "Unknown service: $arg"; usage; }
      SUBSET+=("$arg")
      ;;
  esac
done

if [[ ${#SUBSET[@]} -gt 0 ]]; then
  TARGETS=("${SUBSET[@]}")
else
  TARGETS=("${SEQUENCE[@]}")
fi

# ── Pre-flight ────────────────────────────────────────────────────────

section "Pre-flight"

[[ -f "$ENV_FILE" ]] || { red ".env not found at $ENV_FILE"; exit 1; }

# shellcheck disable=SC1090
ROLLBACK_TAG=$(grep -E '^CRYPTOLAKE_TAG=' "$ENV_FILE" | cut -d= -f2- | tr -d '\n' || true)
[[ -n "$ROLLBACK_TAG" ]] || { red "CRYPTOLAKE_TAG is unset in $ENV_FILE"; exit 1; }
green "Current tag: $ROLLBACK_TAG  →  new tag: $NEW_TAG"

if [[ "$ROLLBACK_TAG" == "$NEW_TAG" && ${#SUBSET[@]} -eq 0 ]]; then
  yellow "New tag matches current. Nothing to do."
  exit 0
fi

# Maintenance window: 02:20–03:30 UTC blocks consolidation seal
if [[ $FORCE -eq 0 ]]; then
  HOUR=$(date -u +%H); MIN=$(date -u +%M)
  if (( 10#$HOUR == 2 && 10#$MIN >= 20 )) || (( 10#$HOUR == 3 && 10#$MIN <= 30 )); then
    red "Inside consolidation window (02:20–03:30 UTC). Use --force to override."
    exit 1
  fi
fi

# All target images present locally
for svc in "${TARGETS[@]}"; do
  case "$svc" in
    collector|collector-backup) image="cryptolake/collector:$NEW_TAG" ;;
    *)                          image="cryptolake/$svc:$NEW_TAG" ;;
  esac
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    red "Image not loaded: $image"
    red "Run scripts/publish-images.sh from your laptop first."
    exit 1
  fi
done
green "All required images present locally"

# Current stack must be healthy before we start rolling
for svc in "${TARGETS[@]}"; do
  url=${HEALTH_URL[$svc]}
  [[ -n "$url" ]] || continue
  if ! curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
    red "$svc is not currently /ready ($url). Refuse to roll a degraded stack."
    red "Investigate first or pass an explicit subset."
    exit 1
  fi
done
green "All targeted services currently healthy"

# ── Maintenance intent ────────────────────────────────────────────────
#
# We can't call cryptolake-maintenance.sh — its `stop`/`restart` paths
# both run `docker compose down`, which is exactly what we're avoiding.
# Instead, write the same lifecycle-ledger marker the wrapper would have
# written, so the writer's gap classifier tags upcoming offset jumps as
# 'planned' rather than 'host_reboot'.

section "Recording maintenance intent"
LIFECYCLE_LEDGER_PATH="${LIFECYCLE_LEDGER_PATH:-/data/.cryptolake/lifecycle/events.jsonl}"
mkdir -p "$(dirname "$LIFECYCLE_LEDGER_PATH")"
MAINT_ID="rolling-$(date -u +%Y%m%dT%H%M%SZ)"
TS=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)
printf '{"ts":"%s","event_type":"maintenance_intent","maintenance_id":"%s","reason":"rolling-update to %s","source":"rolling_update"}\n' \
  "$TS" "$MAINT_ID" "$NEW_TAG" >> "$LIFECYCLE_LEDGER_PATH"
green "Ledger intent: $MAINT_ID"

# ── Helpers ───────────────────────────────────────────────────────────

set_tag() {
  local tag=$1
  if grep -qE '^CRYPTOLAKE_TAG=' "$ENV_FILE"; then
    sed -i.bak "s|^CRYPTOLAKE_TAG=.*|CRYPTOLAKE_TAG=$tag|" "$ENV_FILE"
    rm -f "$ENV_FILE.bak"
  else
    printf '\nCRYPTOLAKE_TAG=%s\n' "$tag" >> "$ENV_FILE"
  fi
}

wait_ready() {
  local svc=$1 url=${HEALTH_URL[$1]}
  if [[ -z "$url" ]]; then
    # No HTTP endpoint — wait for container to be running and not in
    # a restart loop for 30 s.
    local stable_for=0
    while (( stable_for < 30 )); do
      local state
      state=$("${COMPOSE[@]}" ps --format '{{.Service}} {{.State}}' \
              | awk -v s="$svc" '$1==s {print $2}')
      [[ "$state" == "running" ]] && stable_for=$((stable_for+2)) || stable_for=0
      sleep 2
    done
    return 0
  fi
  local elapsed=0
  while (( elapsed < HEALTH_TIMEOUT_SEC )); do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 3; elapsed=$((elapsed+3))
  done
  return 1
}

recreate() {
  local svc=$1
  "${COMPOSE[@]}" up -d --no-deps "$svc"
}

rollback_one() {
  local svc=$1
  red "Rolling $svc back to $ROLLBACK_TAG"
  set_tag "$ROLLBACK_TAG"
  recreate "$svc"
  if wait_ready "$svc"; then
    yellow "$svc restored to $ROLLBACK_TAG"
  else
    red "Rollback of $svc ALSO failed. Manual intervention needed:"
    red "  docker compose logs $svc"
    red "  Then: $REPO_DIR/scripts/rolling-update.sh $ROLLBACK_TAG $svc"
    exit 2
  fi
}

# ── Roll ──────────────────────────────────────────────────────────────

section "Rolling update"
set_tag "$NEW_TAG"
green "Set CRYPTOLAKE_TAG=$NEW_TAG"

for svc in "${TARGETS[@]}"; do
  echo
  green "→ $svc"
  recreate "$svc"
  if ! wait_ready "$svc"; then
    red "$svc did not become ready within ${HEALTH_TIMEOUT_SEC}s"
    rollback_one "$svc"
    exit 1
  fi
  green "$svc ready on $NEW_TAG"
done

section "Done"
green "All targeted services on $NEW_TAG"
"${COMPOSE[@]}" ps
