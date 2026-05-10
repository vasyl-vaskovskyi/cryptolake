#!/usr/bin/env bash
# Shut down the local CryptoLake stack.
#
# Counterpart to start-local.sh. Three modes:
#
#   scripts/stop-local.sh           # docker compose stop
#                                   # Containers stop but stay defined.
#                                   # Fastest "I'll be back in a minute" exit.
#                                   # Restart with: scripts/start-local.sh OR
#                                   #               docker compose start
#
#   scripts/stop-local.sh --remove  # docker compose down
#                                   # Removes containers + networks. Volumes
#                                   # (postgres_data, redpanda_data,
#                                   # prometheus_data, grafana_data) are kept,
#                                   # so the archive on $HOST_DATA_DIR and
#                                   # all PG state survives. Restart needs
#                                   # docker compose up -d (start-local.sh).
#
#   scripts/stop-local.sh --wipe    # docker compose down -v
#                                   # ⚠ DESTRUCTIVE: also removes named
#                                   # volumes — Postgres, Redpanda, Prom and
#                                   # Grafana state are wiped. Files under
#                                   # $HOST_DATA_DIR are NOT touched (they
#                                   # are bind-mounted, not volumes).
#
# This script is intentionally NOT a planned-maintenance wrapper. For
# production-style "stop with maintenance ledger so the writer
# classifies the resulting gap as planned", use
# scripts/cryptolake-maintenance.sh stop instead.

set -euo pipefail

cd "$(dirname "$0")/.."

red()    { printf '\033[0;31m[x]\033[0m %s\n' "$*" >&2; }
yellow() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
green()  { printf '\033[0;32m[+]\033[0m %s\n' "$*"; }

# Kill the caffeinate watchdog that start-local.sh launched (macOS only).
# Idempotent — no error if the PID file is missing or the process is already
# gone.
release_caffeinate() {
    local pid_file=".cryptolake-caffeinate.pid"
    if [[ -f "$pid_file" ]]; then
        local pid
        pid="$(cat "$pid_file" 2>/dev/null || true)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            green "caffeinate stopped (pid=$pid) — system can idle-sleep again."
        fi
        rm -f "$pid_file"
    fi
}

MODE="${1:-stop}"

case "$MODE" in
    "" | stop)
        green "Stopping CryptoLake stack (containers preserved)…"
        docker compose stop
        release_caffeinate
        green "Stopped. Restart with: scripts/start-local.sh"
        ;;
    --remove|-r)
        yellow "Removing CryptoLake containers + networks (volumes preserved)…"
        docker compose down
        release_caffeinate
        green "Removed. Postgres / Redpanda / Prometheus / Grafana state intact."
        green "Restart with: scripts/start-local.sh"
        ;;
    --wipe|-w)
        red  "Wiping CryptoLake — removes containers, networks, AND named volumes."
        red  "Postgres, Redpanda, Prometheus and Grafana state will be lost."
        yellow "(Files under \$HOST_DATA_DIR are bind-mounted and NOT touched.)"
        printf "Type 'wipe' to confirm: "
        read -r CONFIRM
        if [[ "$CONFIRM" != "wipe" ]]; then
            red "Aborted."
            exit 1
        fi
        docker compose down -v
        release_caffeinate
        green "Wiped. Next start-local.sh will start with empty PG / Redpanda / Grafana."
        ;;
    -h|--help)
        sed -n '2,32p' "$0"
        exit 0
        ;;
    *)
        red "Unknown argument: $MODE"
        echo
        sed -n '2,32p' "$0"
        exit 1
        ;;
esac
