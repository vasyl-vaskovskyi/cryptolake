#!/usr/bin/env bash
# Build the Java service images locally and ship them to a remote host over
# SSH using `docker save | ssh host docker load`. No registry needed.
#
# Usage:
#   scripts/publish-images.sh user@host                 # all four images
#   scripts/publish-images.sh user@host writer          # subset
#
# Env:
#   CRYPTOLAKE_TAG   image tag to apply (default: latest). docker-compose.yml
#                    reads the same variable, so set it identically on the host.
#
# After this finishes, on the host:
#   cd /opt/cryptolake && docker compose up -d
#
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -lt 1 ]]; then
  echo "usage: $0 user@host [service...]" >&2
  echo "       services: collector writer backfill consolidation" >&2
  exit 2
fi

HOST="$1"
shift

if [[ $# -gt 0 ]]; then
  SERVICES=("$@")
else
  SERVICES=(collector writer backfill consolidation)
fi

TAG="${CRYPTOLAKE_TAG:-latest}"
export DOCKER_BUILDKIT=1

for svc in "${SERVICES[@]}"; do
  dockerfile="docker/Dockerfile.${svc}"
  if [[ ! -f "$dockerfile" ]]; then
    echo "no such Dockerfile: $dockerfile" >&2
    exit 1
  fi
  image="cryptolake/${svc}:${TAG}"

  echo "==> Building ${image}"
  docker build -f "$dockerfile" -t "$image" .

  echo "==> Shipping ${image} to ${HOST}"
  docker save "$image" | ssh "$HOST" 'docker load'
done

echo
echo "Loaded on ${HOST}: ${SERVICES[*]} (tag=${TAG})"
echo "Next: ssh ${HOST} 'cd /opt/cryptolake && docker compose up -d'"
