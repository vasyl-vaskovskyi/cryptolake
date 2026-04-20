#!/usr/bin/env bash
# gate3: replay recorded WebSocket frames through the Java capture path,
#        compare raw_text bytes + raw_sha256 against Python-produced envelopes.
# Pass-by-definition for modules without a capture path.
set -euo pipefail
MODULE="${1:?module}"

case "$MODULE" in
  common|cli)
    echo "gate3: $MODULE has no capture path (pass-by-definition)"
    exit 0
    ;;
esac

FIXTURE_ROOT="cryptolake-java/parity-fixtures/websocket-frames"
if [[ ! -d "$FIXTURE_ROOT" ]]; then
  echo "gate3 FAIL: fixture directory missing: $FIXTURE_ROOT" >&2
  exit 1
fi

cd cryptolake-java
# Java harness class lives in the module's test sourceSet.
# Harness is named RawTextParityHarness. It reads FIXTURE_ROOT, runs frames
# through the module's capture, prints "OK <n> frames" or "FAIL ..." with
# per-frame diffs. Non-zero exit on any diff.
./gradlew ":${MODULE}:runRawTextParity" --info
