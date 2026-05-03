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

# ---------------------------------------------------------------------------
# Color codes — only emit ANSI when stdout is a TTY and NO_COLOR is unset.
# Final verdict block uses these so the result is unmissable in a long log.
# ---------------------------------------------------------------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    C_RED=$'\033[1;37;41m'   # bold white on red
    C_GRN=$'\033[1;37;42m'   # bold white on green
    C_YEL=$'\033[1;30;43m'   # bold black on yellow
    C_BOLD=$'\033[1m'
    C_OFF=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_BOLD=""; C_OFF=""
fi

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Pre-suite cleanup: kill any leftover cryptolake-chaos-* projects
# `--rmi local` removes the per-project locally-built images so they don't
# accumulate in Docker Desktop across runs.
# ---------------------------------------------------------------------------
echo "[runner] Pre-suite cleanup of leftover chaos projects…"
for proj in $(docker compose ls --all --format json 2>/dev/null \
    | python3 -c "import sys,json; [print(p['Name']) for p in json.load(sys.stdin) if 'cryptolake-chaos' in p['Name']]" 2>/dev/null || true); do
    echo "[runner] Removing leftover project: ${proj}"
    docker compose --project-name "$proj" down -v --remove-orphans --rmi local 2>/dev/null || true
done
# Sweep any orphaned cryptolake-chaos-* images that survived an aborted run
# (e.g., scripts killed before teardown ran). Match by image name prefix.
leftover_imgs=$(docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null \
    | grep -E '^cryptolake-chaos-[0-9]+' || true)
if [[ -n "$leftover_imgs" ]]; then
    echo "[runner] Removing leftover chaos images:"
    echo "$leftover_imgs" | sed 's/^/  /'
    echo "$leftover_imgs" | xargs -r docker rmi -f 2>/dev/null || true
fi
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
ALL_SCENARIOS=()
while IFS= read -r line; do
    ALL_SCENARIOS+=("$line")
done < <(find "$CHAOS_DIR" -name '[0-9][0-9]_*.sh' | sort)

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
# Run each scenario.  Parallel arrays for bash 3.2 compatibility (macOS
# /bin/bash is 3.2 — no associative arrays, no mapfile).
# ---------------------------------------------------------------------------
NAMES=()
RESULTS=()
DURATIONS=()

PASS_COUNT=0
FAIL_COUNT=0
START_ALL=$SECONDS

for scenario in "${SCENARIOS[@]}"; do
    base="$(basename "$scenario" .sh)"
    log_file="${LOG_DIR}/${base}.log"

    printf "[runner] %-50s  " "$base"
    t_start=$SECONDS

    if bash "$scenario" > "$log_file" 2>&1; then
        status="PASS"
        PASS_COUNT=$(( PASS_COUNT + 1 ))
    else
        status="FAIL"
        FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    fi

    t_elapsed=$(( SECONDS - t_start ))
    NAMES+=("$base")
    RESULTS+=("$status")
    DURATIONS+=("$t_elapsed")
    printf "%s  (%ds)  log=%s\n" "$status" "$t_elapsed" "$log_file"
done

TOTAL_ELAPSED=$(( SECONDS - START_ALL ))

# ---------------------------------------------------------------------------
# Post-suite sweep: per-scenario teardown does `down -v --rmi local`, but a
# scenario killed before its EXIT trap fired (Ctrl-C, OOM, SIGKILL) can leave
# its images behind. Sweep anything still tagged cryptolake-chaos-NN-*.
# ---------------------------------------------------------------------------
leftover_imgs=$(docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null \
    | grep -E '^cryptolake-chaos-[0-9]+' || true)
if [[ -n "$leftover_imgs" ]]; then
    echo "[runner] Post-suite cleanup of leftover chaos images:"
    echo "$leftover_imgs" | sed 's/^/  /'
    echo "$leftover_imgs" | xargs -r docker rmi -f 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Per-scenario summary table
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "Chaos test summary  (${TOTAL_ELAPSED}s total)"
echo "=========================================="
printf "%-50s  %-6s  %s\n" "Scenario" "Result" "Duration"
echo "------------------------------------------"
for i in "${!NAMES[@]}"; do
    case "${RESULTS[$i]}" in
        PASS) result_colored="${C_GRN} PASS ${C_OFF}" ;;
        FAIL) result_colored="${C_RED} FAIL ${C_OFF}" ;;
        *)    result_colored="${RESULTS[$i]}" ;;
    esac
    printf "%-50s  %b  %ss\n" "${NAMES[$i]}" "$result_colored" "${DURATIONS[$i]}"
done
echo "=========================================="
echo "PASS: ${PASS_COUNT}  FAIL: ${FAIL_COUNT}  TOTAL: ${#SCENARIOS[@]}"
echo "Logs in: ${LOG_DIR}"

if (( FAIL_COUNT > 0 )); then
    echo ""
    echo "FAILED scenarios:"
    for i in "${!NAMES[@]}"; do
        if [[ "${RESULTS[$i]}" == "FAIL" ]]; then
            echo "  - ${NAMES[$i]}  (see ${LOG_DIR}/${NAMES[$i]}.log)"
        fi
    done
fi

# ---------------------------------------------------------------------------
# Final verdict — large color banner so the outcome is impossible to miss
# in a long scrollback. Layout:
#
#     ##########################################################
#     #                                                        #
#     #   CHAOS TESTS: PASS  (23/23, 1234s)                    #
#     #                                                        #
#     ##########################################################
#
# Banner color: green on all-pass, red on any-fail, yellow on no-scenarios.
# ---------------------------------------------------------------------------
print_verdict_banner() {
    local color="$1"
    local headline="$2"
    local detail="$3"
    local bar="##########################################################"
    local pad="#                                                        #"
    # Bar is 58 cols: "#" + 3 spaces + 50-char headline + 3 spaces + "#".
    # If the headline+detail is wider than 50 chars, truncate so the right-
    # hand "#" border stays intact.
    local text="${headline}  ${detail}"
    text="${text:0:50}"
    local body
    body=$(printf "#   %-50s   #" "$text")

    echo ""
    printf '%b%s%b\n' "$color" "$bar" "$C_OFF"
    printf '%b%s%b\n' "$color" "$pad" "$C_OFF"
    printf '%b%s%b\n' "$color" "$body" "$C_OFF"
    printf '%b%s%b\n' "$color" "$pad" "$C_OFF"
    printf '%b%s%b\n' "$color" "$bar" "$C_OFF"
    echo ""
}

# Detail is kept short so it fits inside the 50-col banner body alongside
# the headline. Per-scenario breakdown is already in the table above.
if (( ${#SCENARIOS[@]} == 0 )); then
    print_verdict_banner "$C_YEL" "CHAOS TESTS: NO SCENARIOS RUN" ""
    exit 1
elif (( FAIL_COUNT > 0 )); then
    print_verdict_banner "$C_RED" "CHAOS TESTS: FAIL" "${FAIL_COUNT}/${#SCENARIOS[@]} failed"
    exit 1
else
    print_verdict_banner "$C_GRN" "CHAOS TESTS: PASS" "${PASS_COUNT}/${#SCENARIOS[@]}"
    exit 0
fi
