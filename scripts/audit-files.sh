#!/usr/bin/env bash
# Thin wrapper around cryptolake-verify audit files.
set -euo pipefail
cd "$(dirname "$0")/.."
exec verify/build/install/verify/bin/verify audit files "$@"
