#!/usr/bin/env bash
# Apply 30-minute retention to all backup.* Redpanda topics.
#
# Runs against the running stack via `docker compose exec redpanda rpk`,
# so it works from any host with docker — the broker hostname
# `redpanda:9092` is only resolvable from inside the docker network,
# and `rpk` is not assumed to be on the host PATH.
#
# Idempotent: re-running just re-applies the same retention. Touches
# the sentinel file only on full success so a failed run will be
# retried on the next boot.
#
# Usage:
#   bash scripts/setup-backup-topics.sh                  # one-shot
#   COMPOSE_DIR=/opt/cryptolake bash scripts/setup-backup-topics.sh
#
set -euo pipefail

REPO_DIR="${COMPOSE_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
SENTINEL="$REPO_DIR/.cryptolake-backup-topics-configured"
RETENTION_MS=1800000   # 30 minutes
WAIT_REDPANDA_SEC=120  # max time to wait for redpanda to be healthy
WAIT_TOPICS_SEC=300    # max time to wait for at least one backup.* topic to exist

cd "$REPO_DIR"

log() { printf '[setup-backup-topics] %s\n' "$*"; }

rpk() {
  # shellcheck disable=SC2068
  docker compose exec -T redpanda rpk $@ 2>&1
}

# 1. Wait for redpanda to be healthy
log "Waiting up to ${WAIT_REDPANDA_SEC}s for redpanda to be healthy..."
end=$(( $(date +%s) + WAIT_REDPANDA_SEC ))
until rpk cluster health 2>/dev/null | grep -q 'Healthy:[[:space:]]*true'; do
  if (( $(date +%s) >= end )); then
    log "ERROR: redpanda did not become healthy within ${WAIT_REDPANDA_SEC}s"
    exit 1
  fi
  sleep 2
done
log "redpanda healthy"

# 2. Wait for at least one backup.* topic (created on first message from collector-backup)
log "Waiting up to ${WAIT_TOPICS_SEC}s for backup topics to appear..."
end=$(( $(date +%s) + WAIT_TOPICS_SEC ))
while ! rpk topic list 2>/dev/null | awk 'NR>1 && /^backup\./ {found=1; exit} END {exit !found}'; do
  if (( $(date +%s) >= end )); then
    log "ERROR: no backup.* topics appeared within ${WAIT_TOPICS_SEC}s"
    log "       collector-backup may not be running. Check: docker compose ps"
    exit 1
  fi
  sleep 5
done

# 3. Apply retention to every backup.* topic
TOPICS=$(rpk topic list 2>/dev/null | awk 'NR>1 && /^backup\./ {print $1}')
if [[ -z "$TOPICS" ]]; then
  log "ERROR: backup topic list is empty after wait — refusing to mark configured"
  exit 1
fi

count=0
while IFS= read -r topic; do
  [[ -n "$topic" ]] || continue
  log "Setting retention.ms=${RETENTION_MS} on $topic"
  rpk topic alter-config "$topic" --set "retention.ms=${RETENTION_MS}" >/dev/null
  count=$((count + 1))
done <<<"$TOPICS"

# 4. Mark configured so the systemd unit / start-local.sh doesn't re-run on every boot
touch "$SENTINEL"
log "Configured retention on $count backup.* topic(s). Sentinel: $SENTINEL"
