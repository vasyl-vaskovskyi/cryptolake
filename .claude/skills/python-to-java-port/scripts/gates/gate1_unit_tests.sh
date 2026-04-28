#!/usr/bin/env bash
# gate1: unit + integration tests for the module.
set -euo pipefail
MODULE="${1:?module}"
cd "$REPO_ROOT"
./gradlew ":${MODULE}:test" --tests '*' --info
