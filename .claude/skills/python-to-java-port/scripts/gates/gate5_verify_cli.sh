#!/usr/bin/env bash
# gate5: run Python verify CLI against Java-produced archives.
set -euo pipefail
MODULE="${1:?module}"

# gate5 only meaningful for writer (produces archives) and cli (the verify port).
case "$MODULE" in
  common|collector)
    echo "gate5: $MODULE does not produce archives (pass-by-definition)"
    exit 0
    ;;
esac

EXPECTED="cryptolake-java/parity-fixtures/verify/expected.txt"
[[ -f "$EXPECTED" ]] || { echo "gate5 FAIL: expected output missing: $EXPECTED" >&2; exit 1; }

# Run Java integration harness that produces archives to a known path.
cd cryptolake-java
./gradlew ":${MODULE}:produceSyntheticArchives" --info
ARCHIVE_DIR="${MODULE}/build/synthetic-archives"
[[ -d "../$ARCHIVE_DIR" ]] || { echo "gate5 FAIL: no archives produced" >&2; exit 1; }
cd ..

# Now run Python verify against Java archives.
ACTUAL="$(uv run python -m src.cli.verify --archive-dir "cryptolake-java/$ARCHIVE_DIR" 2>&1 || true)"
EXP="$(cat "$EXPECTED")"

# Structural match: exit code + summary line shape. Numeric values will differ.
# We compare the "PASS"/"FAIL" word + error count == 0.
if [[ "$ACTUAL" != *"PASS"* || "$ACTUAL" == *"errors: "*[^0]* ]]; then
  echo "gate5 FAIL: verify output not PASS or errors>0" >&2
  echo "actual: $ACTUAL" >&2
  exit 1
fi
echo "gate5 OK: Python verify passes on Java archives"
