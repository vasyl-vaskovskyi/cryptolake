#!/usr/bin/env bash
# Tail logs produced by run-all-tests.sh.
#
# Usage:
#   bash scripts/tail-test-logs.sh                # tail every phase log + every chaos scenario log
#   bash scripts/tail-test-logs.sh chaos          # tail only the combined chaos phase log
#   bash scripts/tail-test-logs.sh jvm spotless   # tail specific phase logs by name
#   bash scripts/tail-test-logs.sh 01 09          # tail specific chaos scenarios by number
#
# Each arg is matched against build/run-all-tests/<arg>.log first, then
# against build/chaos-logs/<arg>*.log. Files that don't exist yet are waited
# for (tail -F), so you can start this before run-all-tests.sh finishes.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PHASE_DIR="${REPO_ROOT}/build/run-all-tests"
CHAOS_DIR="${REPO_ROOT}/build/chaos-logs"

files=()

resolve() {
    local arg="$1"
    if [[ -f "${PHASE_DIR}/${arg}.log" ]]; then
        files+=("${PHASE_DIR}/${arg}.log")
        return
    fi
    local matches=( "${CHAOS_DIR}/${arg}"*.log )
    if [[ -e "${matches[0]}" ]]; then
        files+=("${matches[@]}")
        return
    fi
    echo "warn: no log matched '${arg}' under ${PHASE_DIR} or ${CHAOS_DIR}" >&2
}

if [[ $# -eq 0 ]]; then
    [[ -d "$PHASE_DIR" ]] && while IFS= read -r -d '' f; do files+=("$f"); done \
        < <(find "$PHASE_DIR" -maxdepth 1 -type f -name '*.log' -print0)
    [[ -d "$CHAOS_DIR" ]] && while IFS= read -r -d '' f; do files+=("$f"); done \
        < <(find "$CHAOS_DIR" -maxdepth 1 -type f -name '*.log' -print0)
else
    for arg in "$@"; do resolve "$arg"; done
fi

if [[ ${#files[@]} -eq 0 ]]; then
    echo "No log files found. Run 'bash scripts/run-all-tests.sh' first." >&2
    exit 1
fi

echo "Tailing ${#files[@]} file(s). Ctrl-C to stop."
printf '  %s\n' "${files[@]}"
echo
exec tail -n +1 -F "${files[@]}"
