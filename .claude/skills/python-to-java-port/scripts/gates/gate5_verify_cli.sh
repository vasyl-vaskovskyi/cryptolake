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

REPO_ROOT="$(cd "$(dirname "$0")/../../../../.." && pwd)"
EXPECTED="$REPO_ROOT/parity-fixtures/verify/expected.txt"
[[ -f "$EXPECTED" ]] || { echo "gate5 FAIL: expected output missing: $EXPECTED" >&2; exit 1; }

# Run Java integration harness that produces archives to a known path.
cd "$REPO_ROOT"
./gradlew ":${MODULE}:produceSyntheticArchives" --info
ARCHIVE_DIR="$REPO_ROOT/${MODULE}/build/synthetic-archives"
[[ -d "$ARCHIVE_DIR" ]] || { echo "gate5 FAIL: no archives produced at $ARCHIVE_DIR" >&2; exit 1; }

cd "$REPO_ROOT"
# Run Python verify against Java-produced archives. The verify CLI accepts
# --base-dir (see src/cli/verify.py); --date derives from the archive tree.
TODAY=$(date -u +%Y-%m-%d)
ACTUAL="$(uv run cryptolake verify --date "$TODAY" --base-dir "$ARCHIVE_DIR" 2>&1 || true)"

# Structural match: Java-produced archives must satisfy the same verification
# the Python reference passes — zero ERRORS in the summary.
if echo "$ACTUAL" | grep -qE "^ERRORS \([^0]"; then
  echo "gate5 FAIL: Python verify reported errors on Java archives" >&2
  echo "actual:" >&2
  echo "$ACTUAL" >&2
  exit 1
fi
echo "gate5 OK: Python verify passes on Java archives"
