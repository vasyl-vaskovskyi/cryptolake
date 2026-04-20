#!/usr/bin/env bash
# gate2: chaos tests. Trivially passes for common + cli (no chaos suite).
set -euo pipefail
MODULE="${1:?module}"
case "$MODULE" in
  common|cli)
    echo "gate2: no chaos suite for $MODULE (pass-by-definition)"
    exit 0
    ;;
esac
cd cryptolake-java
./gradlew ":${MODULE}:test" --tests '*chaos*' --info
