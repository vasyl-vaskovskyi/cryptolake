#!/usr/bin/env bash
# scripts/run-chaos-tests.sh
# Iterates every tests/chaos/NN_*.sh, runs them sequentially, and reports pass/fail.
# Filter to a subset by passing scenario numbers as args: run-chaos-tests.sh 01 03
#
# Per master spec §14.j chaos scripts are independently runnable; this runner is the
# convenience wrapper that mirrors what the JUnit ChaosVerifyIT harness will eventually do.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${REPO_ROOT}/build/chaos-logs"
mkdir -p "$LOG_DIR"

# Optional positional filters: "01 03" means only those two.
declare -a filters=("$@")

scenarios=()
shopt -s nullglob
for f in "${REPO_ROOT}/tests/chaos/"[0-9][0-9]_*.sh; do
  scenarios+=("$f")
done
shopt -u nullglob

if [[ ${#scenarios[@]} -eq 0 ]]; then
  echo "no chaos scenarios found under tests/chaos/"
  exit 1
fi

passes=0
fails=()
for scenario in "${scenarios[@]}"; do
  name=$(basename "$scenario" .sh)
  num="${name%%_*}"
  if [[ ${#filters[@]} -gt 0 ]]; then
    skip=1
    for keep in "${filters[@]}"; do
      [[ "$num" == "$keep" ]] && skip=0
    done
    [[ $skip -eq 1 ]] && continue
  fi

  log="${LOG_DIR}/${name}.log"
  echo "=== running $name ==="
  if bash "$scenario" >"$log" 2>&1; then
    echo "    PASS — log: $log"
    passes=$((passes + 1))
  else
    echo "    FAIL — log: $log"
    fails+=("$name")
    tail -20 "$log" | sed 's/^/    | /'
  fi
done

echo
echo "==== chaos summary ===="
echo "passed: $passes"
echo "failed: ${#fails[@]}"
if [[ ${#fails[@]} -gt 0 ]]; then
  for f in "${fails[@]}"; do
    echo "  - $f"
  done
  exit 1
fi
exit 0
