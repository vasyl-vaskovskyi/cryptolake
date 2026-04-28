#!/usr/bin/env bash
# gate6: Spotless + Error Prone + NullAway + custom rule checks.
set -euo pipefail
MODULE="${1:?module}"
cd "$REPO_ROOT"
./gradlew ":${MODULE}:check" --info

# Custom rule: no `synchronized (` near blocking I/O patterns in this module.
BAD_SYNC=$(grep -RnE 'synchronized *\(' "${MODULE}/src/main/java" || true)
if [[ -n "$BAD_SYNC" ]]; then
  echo "gate6 FAIL: synchronized blocks found (virtual-thread pinning risk):" >&2
  echo "$BAD_SYNC" >&2
  exit 1
fi

# Custom rule: no Thread.sleep in main sources.
BAD_SLEEP=$(grep -RnE 'Thread\.sleep *\(' "${MODULE}/src/main/java" || true)
if [[ -n "$BAD_SLEEP" ]]; then
  echo "gate6 FAIL: Thread.sleep found in main sources:" >&2
  echo "$BAD_SLEEP" >&2
  exit 1
fi

echo "gate6 OK: static checks pass for $MODULE"
