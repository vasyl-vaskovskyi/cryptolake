#!/usr/bin/env bash
# Start the full CryptoLake stack locally.
#
# Assumes preconfiguration is already in place:
#   - .env exists with POSTGRES_PASSWORD set
#   - config/config.yaml has at least one symbol enabled
#   - Docker daemon is running
#
# This script does NOT generate secrets or rewrite config — it only
# validates the preconditions and brings the compose stack up. If
# anything is missing it exits with an explicit message pointing at the
# fix. Run scripts/start-local.sh again after each correction.
#
# Usage:
#   scripts/start-local.sh           # bring stack up, follow nothing
#   scripts/start-local.sh --logs    # bring up and tail writer logs
#
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_DIR="$(pwd)"

red()    { printf '\033[0;31m[x]\033[0m %s\n' "$*" >&2; }
yellow() { printf '\033[1;33m[!]\033[0m %s\n' "$*"; }
green()  { printf '\033[0;32m[+]\033[0m %s\n' "$*"; }

fail() {
  red "$1"
  echo
  echo "Fix:"
  printf '  %s\n' "${@:2}"
  exit 1
}

# 1. Docker daemon reachable
if ! docker info >/dev/null 2>&1; then
  fail "Docker daemon is not reachable." \
    "Start Docker Desktop (or 'sudo systemctl start docker' on Linux)" \
    "and re-run this script."
fi
green "Docker daemon: ok"

# 2. .env file present
if [[ ! -f .env ]]; then
  fail ".env is missing." \
    "cp .env.example .env" \
    "Then open .env and set POSTGRES_PASSWORD and CALLMEBOT_* if you" \
    "use the WhatsApp bridge."
fi

# Source .env so we can validate values without leaking them in logs.
# shellcheck disable=SC1091
set -a; source .env; set +a

# 3. Required secrets
if [[ -z "${POSTGRES_PASSWORD:-}" ]]; then
  fail "POSTGRES_PASSWORD is empty in .env." \
    "Open .env and set POSTGRES_PASSWORD to a non-empty value." \
    "Use 'openssl rand -base64 24' if you need a fresh one."
fi

# WhatsApp bridge is enforced via compose's :? syntax — surface the
# requirement here so the user gets a friendly error instead of a
# compose stack-trace.
if [[ -z "${CALLMEBOT_PHONE:-}" || -z "${CALLMEBOT_APIKEY:-}" ]]; then
  fail "CALLMEBOT_PHONE / CALLMEBOT_APIKEY are required (used by alertmanager)." \
    "If you don't have CallMeBot set up, put placeholder values" \
    "(e.g. CALLMEBOT_PHONE=0 and CALLMEBOT_APIKEY=disabled) in .env." \
    "Webhook delivery will silently fail but the stack will still start."
fi
green ".env: required values present"

# 4. config.yaml has at least one enabled symbol
CONFIG=config/config.yaml
if [[ ! -f "$CONFIG" ]]; then
  fail "$CONFIG is missing." \
    "Restore it from git (git checkout -- $CONFIG) or copy from" \
    "the repo's last known good version."
fi

# Crude grep — config.yaml is short and well-structured. We just want
# to surface the case where someone deleted all symbol entries.
if ! grep -E '^\s+- [a-z0-9]+' "$CONFIG" >/dev/null; then
  fail "$CONFIG has no symbols configured under exchanges.binance.symbols." \
    "Open $CONFIG and add at least one symbol, e.g.:" \
    "    symbols:" \
    "      - btcusdt"
fi

SYMBOLS=$(awk '/^\s+symbols:/{f=1; next} f && /^\s+- /{print $2; next} f{f=0}' "$CONFIG" | paste -sd, -)
green "config.yaml: symbols=${SYMBOLS}"

# 5. Bring the stack up
green "Starting cryptolake stack..."
docker compose up -d

# 6. First-run only: apply 30-min retention to backup.* topics. Sentinel-gated
# so re-running this script doesn't re-do the work. Failure is non-fatal —
# the stack is already running.
if [[ ! -f .cryptolake-backup-topics-configured ]]; then
  yellow "First run detected; configuring backup.* topic retention in background..."
  (bash scripts/setup-backup-topics.sh \
    || red "setup-backup-topics.sh failed — re-run manually once collector-backup is healthy.") &
fi

echo
green "Stack started. Health endpoints (loopback only):"
cat <<EOF
  collector primary  http://127.0.0.1:8000/ready
  collector backup   http://127.0.0.1:8004/ready
  writer             http://127.0.0.1:8001/ready
  consolidation      http://127.0.0.1:8003/ready
  prometheus         http://127.0.0.1:9090
EOF

if [[ "${1:-}" == "--logs" ]]; then
  echo
  green "Tailing writer logs (Ctrl+C to detach; stack keeps running)..."
  docker compose logs -f writer
fi
