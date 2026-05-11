#!/usr/bin/env bash
# Thin wrapper around cryptolake-verify audit backfill.
set -euo pipefail
cd "$(dirname "$0")/.."
exec verify/build/install/verify/bin/verify audit backfill "$@"
