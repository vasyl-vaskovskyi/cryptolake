#!/usr/bin/env bash
# gate1: unit + integration tests for the module.
set -euo pipefail
MODULE="${1:?module}"
cd cryptolake-java
./gradlew ":${MODULE}:test" --tests '*' --info
