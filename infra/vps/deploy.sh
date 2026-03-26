#!/usr/bin/env bash
# CryptoLake Deploy Script
# Run from repo root: bash infra/vps/deploy.sh
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE="docker compose -f $REPO_DIR/docker-compose.yml -f $REPO_DIR/docker-compose.vps.yml"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${GREEN}[+]${NC} $1"; }
warn()  { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[x]${NC} $1"; exit 1; }

# --- Pre-checks ---
[[ ! -f "$REPO_DIR/.env" ]] && error ".env not found. Run setup.sh first or copy from .env.example"

# Disk space check
DISK_USAGE=$(df "$REPO_DIR" --output=pcent | tail -1 | tr -d ' %')
if [[ "$DISK_USAGE" -gt 90 ]]; then
    error "Disk usage is at ${DISK_USAGE}% — free space before deploying"
elif [[ "$DISK_USAGE" -gt 80 ]]; then
    warn "Disk usage is at ${DISK_USAGE}% — consider freeing space soon"
fi

# --- Pull latest code ---
info "Pulling latest code..."
cd "$REPO_DIR"
BEFORE=$(git rev-parse HEAD)
git pull origin main
AFTER=$(git rev-parse HEAD)

if [[ "$BEFORE" != "$AFTER" ]]; then
    info "Updated: $(git log --oneline "$BEFORE".."$AFTER" | wc -l) new commit(s)"
    git log --oneline "$BEFORE".."$AFTER"
else
    info "Already up to date"
fi

# --- Build ---
info "Building images..."
$COMPOSE build

# --- Deploy ---
info "Starting services..."
$COMPOSE up -d

# --- Health check ---
info "Waiting for health checks (30s)..."
sleep 30

echo ""
info "Service status:"
$COMPOSE ps
echo ""

# Check for unhealthy services
UNHEALTHY=$($COMPOSE ps --format json 2>/dev/null \
    | jq -r 'select(.Health != null and .Health != "healthy") | .Service' 2>/dev/null \
    || true)

if [[ -n "$UNHEALTHY" ]]; then
    warn "Services not yet healthy: $UNHEALTHY"
    warn "Check logs: cd $REPO_DIR && $COMPOSE logs $UNHEALTHY"
else
    info "All services healthy"
fi

info "Deploy complete. Monitor: sampler -c $REPO_DIR/infra/sampler/sampler.yml"
