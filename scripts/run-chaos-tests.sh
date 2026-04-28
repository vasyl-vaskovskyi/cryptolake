#!/usr/bin/env bash
# scripts/run-chaos-tests.sh — Run all CryptoLake chaos scenarios
#
# Usage:
#   bash scripts/run-chaos-tests.sh [NN]
#
# If an argument is given, only run scenario NN (e.g. "01" or "1").
#
# Exit code:
#   0 — all scenarios passed
#   1 — one or more scenarios failed

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CHAOS_DIR="${REPO_ROOT}/tests/chaos"
LOG_DIR="${REPO_ROOT}/build/chaos-logs"
FILTER="${1:-}"

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Pre-suite cleanup: kill any leftover cryptolake-chaos-* projects
# ---------------------------------------------------------------------------
echo "[runner] Pre-suite cleanup of leftover chaos projects…"
for proj in $(docker compose ls --format json 2>/dev/null \
    | python3 -c "import sys,json; [print(p['Name']) for p in json.load(sys.stdin) if 'cryptolake-chaos' in p['Name']]" 2>/dev/null || true); do
    echo "[runner] Removing leftover project: ${proj}"
    docker compose --project-name "$proj" down -v --remove-orphans 2>/dev/null || true
done
# Clean any leftover data dirs
rm -rf /tmp/cryptolake-chaos-*-data 2>/dev/null || true
echo "[runner] Pre-suite cleanup done."

# ---------------------------------------------------------------------------
# Build verify CLI once
# ---------------------------------------------------------------------------
VERIFY_BIN="${REPO_ROOT}/verify/build/install/verify/bin/verify"
if [[ ! -x "$VERIFY_BIN" ]]; then
    echo "[runner] Building verify CLI…"
    (cd "$REPO_ROOT" && ./gradlew :verify:installDist -q 2>&1 | tail -3)
fi

# ---------------------------------------------------------------------------
# Discover scenarios
# ---------------------------------------------------------------------------
mapfile -t ALL_SCENARIOS < <(find "$CHAOS_DIR" -name '[0-9][0-9]_*.sh' | sort)

if [[ ${#ALL_SCENARIOS[@]} -eq 0 ]]; then
    echo "[runner] No scenario scripts found in ${CHAOS_DIR}"
    exit 1
fi

# ---------------------------------------------------------------------------
# Filter if argument given
# ---------------------------------------------------------------------------
declare -a SCENARIOS=()
for s in "${ALL_SCENARIOS[@]}"; do
    base="$(basename "$s")"
    nn="${base%%_*}"
    if [[ -z "$FILTER" || "$nn" == "$FILTER" || "$nn" == "$(printf '%02d' "$FILTER" 2>/dev/null)" ]]; then
        SCENARIOS+=("$s")
    fi
done

if [[ ${#SCENARIOS[@]} -eq 0 ]]; then
    echo "[runner] No scenarios match filter '${FILTER}'"
    exit 1
fi

echo "[runner] Running ${#SCENARIOS[@]} scenario(s)…"
echo ""

# ---------------------------------------------------------------------------
# Run each scenario
# ---------------------------------------------------------------------------
declare -A RESULTS   # name -> PASS|FAIL
declare -A DURATIONS # name -> seconds

PASS_COUNT=0
FAIL_COUNT=0
START_ALL=$SECONDS

for scenario in "${SCENARIOS[@]}"; do
    base="$(basename "$scenario" .sh)"
    log_file="${LOG_DIR}/${base}.log"

    printf "[runner] %-50s  " "$base"
    t_start=$SECONDS

    # Each scenario runs in a subshell so its `set -e` + trap don't affect us
    if bash "$scenario" > "$log_file" 2>&1; then
        status="PASS"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        status="FAIL"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi

    t_elapsed=$(( SECONDS - t_start ))
    RESULTS["$base"]="$status"
    DURATIONS["$base"]="$t_elapsed"
    printf "%s  (%ds)  log=%s\n" "$status" "$t_elapsed" "$log_file"
done

TOTAL_ELAPSED=$(( SECONDS - START_ALL ))

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "Chaos test summary  (${TOTAL_ELAPSED}s total)"
echo "=========================================="
printf "%-50s  %-6s  %s\n" "Scenario" "Result" "Duration"
echo "------------------------------------------"
for scenario in "${SCENARIOS[@]}"; do
    base="$(basename "$scenario" .sh)"
    result="${RESULTS[$base]:-N/A}"
    dur="${DURATIONS[$base]:-?}"
    printf "%-50s  %-6s  %ds\n" "$base" "$result" "$dur"
done
echo "=========================================="
echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}  TOTAL: ${#SCENARIOS[@]}"
echo "Logs in: ${LOG_DIR}"

if (( FAIL_COUNT > 0 )); then
    echo ""
    echo "FAILED scenarios:"
    for scenario in "${SCENARIOS[@]}"; do
        base="$(basename "$scenario" .sh)"
        if [[ "${RESULTS[$base]}" == "FAIL" ]]; then
            echo "  - ${base}  (see ${LOG_DIR}/${base}.log)"
        fi
    done
    exit 1
fi

echo "All scenarios PASSED."
exit 0
