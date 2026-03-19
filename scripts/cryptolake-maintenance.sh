#!/usr/bin/env bash
# cryptolake-maintenance.sh — Planned maintenance wrapper
#
# Records a maintenance intent BEFORE stopping services so the writer
# can classify the resulting restart gap as planned.
#
# Usage:
#   scripts/cryptolake-maintenance.sh stop   [--reason "..."]
#   scripts/cryptolake-maintenance.sh restart [--reason "..."]
#
# The wrapper:
# 1. Generates a deterministic maintenance ID
# 2. Writes intent to the host lifecycle ledger (always available)
# 3. Attempts to write intent to PG via CLI (best-effort — may fail
#    if PG is the maintenance target)
# 4. Stops or restarts services only after the ledger write succeeds

set -euo pipefail

LIFECYCLE_LEDGER_PATH="${LIFECYCLE_LEDGER_PATH:-/data/.cryptolake/lifecycle/events.jsonl}"
COMPOSE_DIR="${COMPOSE_DIR:-/opt/cryptolake}"
DB_URL="${DATABASE_URL:-postgresql://cryptolake:cryptolake@localhost:5432/cryptolake}"
TTL_MINUTES="${TTL_MINUTES:-60}"

ACTION="${1:-}"
REASON="scheduled maintenance"

# Parse options
shift || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --reason)
            REASON="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "$ACTION" != "stop" && "$ACTION" != "restart" ]]; then
    echo "Usage: $0 {stop|restart} [--reason \"...\"]" >&2
    exit 1
fi

# Generate deterministic maintenance ID
MAINT_ID="maint-$(date -u +%Y%m%dT%H%M%SZ)"
echo "==> Maintenance ID: $MAINT_ID"
echo "==> Reason: $REASON"
echo "==> Action: $ACTION"

# Step 1: Write intent to host lifecycle ledger (always available)
mkdir -p "$(dirname "$LIFECYCLE_LEDGER_PATH")"
TS=$(date -u +%Y-%m-%dT%H:%M:%S+00:00)
printf '{"ts":"%s","event_type":"maintenance_intent","maintenance_id":"%s","reason":"%s","source":"maintenance_wrapper"}\n' \
    "$TS" "$MAINT_ID" "$REASON" >> "$LIFECYCLE_LEDGER_PATH"
echo "==> Ledger intent written to $LIFECYCLE_LEDGER_PATH"

# Step 2: Attempt PG write via CLI (best-effort)
PG_OK=false
if command -v cryptolake >/dev/null 2>&1; then
    if cryptolake mark-maintenance \
        --db-url "$DB_URL" \
        --scope system \
        --maintenance-id "$MAINT_ID" \
        --reason "$REASON" \
        --ttl-minutes "$TTL_MINUTES" 2>/dev/null; then
        PG_OK=true
        echo "==> PG intent written via CLI"
    else
        echo "==> PG write failed (best-effort) — ledger-only intent"
    fi
else
    echo "==> cryptolake CLI not found — ledger-only intent"
fi

# Step 3: Perform the requested action
echo "==> Performing: docker compose $ACTION"
cd "$COMPOSE_DIR"

case "$ACTION" in
    stop)
        docker compose down
        echo "==> Stack stopped. Maintenance ID: $MAINT_ID"
        ;;
    restart)
        docker compose down
        echo "==> Stack stopped. Restarting..."
        docker compose up -d
        echo "==> Stack restarted. Maintenance ID: $MAINT_ID"
        ;;
esac

echo "==> Maintenance complete."
if $PG_OK; then
    echo "    Intent recorded in: PG + ledger"
else
    echo "    Intent recorded in: ledger only"
fi
