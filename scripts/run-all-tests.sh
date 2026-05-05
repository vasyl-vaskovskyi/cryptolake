#!/usr/bin/env bash
# scripts/run-all-tests.sh — Run every test in the repository.
#
# Layout:
#   1. JVM tests           ./gradlew test              (one phase per module,
#                                                       so a failure in one
#                                                       module's tests doesn't
#                                                       block the next module)
#   2. Spotless format gate ./gradlew spotlessCheck
#   3. Chaos suite          tests/chaos/NN_*.sh         one scenario at a time
#                                                       via run-chaos-tests.sh
#
# Each phase is timed and recorded; a single colored summary prints at the
# end. Exit code: 0 if everything passed (chaos SKIPs count as pass), non-zero
# if any phase failed.
#
# Usage:
#   bash scripts/run-all-tests.sh            # full suite (JVM + spotless + chaos)
#   bash scripts/run-all-tests.sh --no-chaos # skip the chaos suite (~30+ min)
#   bash scripts/run-all-tests.sh --chaos-only
#
# Logs land under build/run-all-tests/<phase>.log; the chaos runner also writes
# its own per-scenario logs under build/chaos-logs/.

set -uo pipefail   # NB: not -e — we record per-phase exit codes ourselves

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${REPO_ROOT}/build/run-all-tests"
mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
RUN_JVM=1
RUN_SPOTLESS=1
RUN_CHAOS=1
for arg in "$@"; do
    case "$arg" in
        --no-chaos)  RUN_CHAOS=0 ;;
        --no-jvm)    RUN_JVM=0; RUN_SPOTLESS=0 ;;
        --chaos-only) RUN_JVM=0; RUN_SPOTLESS=0 ;;
        --help|-h)
            sed -n '2,20p' "$0"
            exit 0
            ;;
        *)
            echo "Unknown arg: $arg" >&2
            echo "Use --help for usage." >&2
            exit 2
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Color codes — only ANSI when stdout is a TTY and NO_COLOR is unset.
# ---------------------------------------------------------------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    C_RED=$'\033[1;37;41m'
    C_GRN=$'\033[1;37;42m'
    C_YEL=$'\033[1;30;43m'
    C_OFF=$'\033[0m'
else
    C_RED=""; C_GRN=""; C_YEL=""; C_OFF=""
fi

# Parallel arrays (bash 3.2 friendly: macOS /bin/bash)
PHASES=()
RESULTS=()
DURATIONS=()
LOGS=()

run_phase() {
    local name="$1"; shift
    local log="${LOG_DIR}/${name}.log"
    local t0=$SECONDS
    printf "[runner] %-40s  " "$name"
    if "$@" > "$log" 2>&1; then
        local status="PASS"
    else
        local status="FAIL"
    fi
    local elapsed=$(( SECONDS - t0 ))
    PHASES+=("$name")
    RESULTS+=("$status")
    DURATIONS+=("$elapsed")
    LOGS+=("$log")
    printf "%s  (%ds)  log=%s\n" "$status" "$elapsed" "$log"
}

# ---------------------------------------------------------------------------
# Phase 1: JVM tests, one module at a time
# ---------------------------------------------------------------------------
JVM_MODULES=(common collector writer verify backfill consolidation)

START_ALL=$SECONDS

if (( RUN_JVM )); then
    echo ""
    echo "=== Phase 1: JVM unit + integration tests (per-module) ==="
    for mod in "${JVM_MODULES[@]}"; do
        # `./gradlew :<mod>:test` runs that module's @Test methods; the
        # consolidation module's build excludes @Tag("chaos") here on purpose
        # so the long-running chaos tests stay in phase 3.
        run_phase "jvm:${mod}:test" \
            "${REPO_ROOT}/gradlew" ":${mod}:test" --console=plain
    done
fi

# ---------------------------------------------------------------------------
# Phase 2: Spotless / static checks
# ---------------------------------------------------------------------------
if (( RUN_SPOTLESS )); then
    echo ""
    echo "=== Phase 2: Spotless format check ==="
    run_phase "spotlessCheck" \
        "${REPO_ROOT}/gradlew" spotlessCheck --console=plain
fi

# ---------------------------------------------------------------------------
# Phase 3: Chaos suite — delegate to the existing per-scenario runner
# ---------------------------------------------------------------------------
if (( RUN_CHAOS )); then
    echo ""
    echo "=== Phase 3: Chaos scenarios (one at a time) ==="
    # run-chaos-tests.sh prints its own per-scenario PASS/FAIL line, the
    # final summary table, and the big banner. We capture all of that into
    # a single phase log AND let it stream to the terminal so the user
    # can watch the progress live.
    log="${LOG_DIR}/chaos.log"
    t0=$SECONDS
    if bash "${REPO_ROOT}/scripts/run-chaos-tests.sh" 2>&1 | tee "$log"; then
        # tee always returns 0 — read run-chaos-tests.sh's status from PIPESTATUS
        chaos_rc=${PIPESTATUS[0]}
    else
        chaos_rc=${PIPESTATUS[0]}
    fi
    elapsed=$(( SECONDS - t0 ))
    if [[ "$chaos_rc" -eq 0 ]]; then
        status="PASS"
    else
        status="FAIL"
    fi
    PHASES+=("chaos-suite")
    RESULTS+=("$status")
    DURATIONS+=("$elapsed")
    LOGS+=("$log")
fi

TOTAL_ELAPSED=$(( SECONDS - START_ALL ))

# ---------------------------------------------------------------------------
# Per-phase summary table
# ---------------------------------------------------------------------------
PASS_COUNT=0
FAIL_COUNT=0
echo ""
echo "============================================================"
echo "Run-all-tests summary  (${TOTAL_ELAPSED}s total)"
echo "============================================================"
printf "%-30s  %-6s  %-10s  %s\n" "Phase" "Result" "Duration" "Log"
echo "------------------------------------------------------------"
for i in "${!PHASES[@]}"; do
    case "${RESULTS[$i]}" in
        PASS) colored="${C_GRN} PASS ${C_OFF}"; PASS_COUNT=$(( PASS_COUNT + 1 )) ;;
        FAIL) colored="${C_RED} FAIL ${C_OFF}"; FAIL_COUNT=$(( FAIL_COUNT + 1 )) ;;
        *)    colored="${RESULTS[$i]}" ;;
    esac
    printf "%-30s  %b  %-10s  %s\n" \
        "${PHASES[$i]}" "$colored" "${DURATIONS[$i]}s" "${LOGS[$i]}"
done
echo "------------------------------------------------------------"
echo "PASS: ${PASS_COUNT}   FAIL: ${FAIL_COUNT}   TOTAL: ${#PHASES[@]}"
echo "Phase logs in: ${LOG_DIR}"
echo "Chaos per-scenario logs in: ${REPO_ROOT}/build/chaos-logs"
echo ""

# ---------------------------------------------------------------------------
# Final verdict banner
# ---------------------------------------------------------------------------
print_banner() {
    local color="$1"; local text="${2:0:50}"
    local bar="##########################################################"
    local pad="#                                                        #"
    local body
    body=$(printf "#   %-50s   #" "$text")
    echo ""
    printf '%b%s%b\n' "$color" "$bar" "$C_OFF"
    printf '%b%s%b\n' "$color" "$pad"  "$C_OFF"
    printf '%b%s%b\n' "$color" "$body" "$C_OFF"
    printf '%b%s%b\n' "$color" "$pad"  "$C_OFF"
    printf '%b%s%b\n' "$color" "$bar"  "$C_OFF"
    echo ""
}

if (( FAIL_COUNT > 0 )); then
    print_banner "$C_RED" "RUN-ALL-TESTS: FAIL  (${FAIL_COUNT}/${#PHASES[@]} failed)"
    echo "FAILED phases:"
    for i in "${!PHASES[@]}"; do
        if [[ "${RESULTS[$i]}" == "FAIL" ]]; then
            echo "  - ${PHASES[$i]}  (see ${LOGS[$i]})"
        fi
    done
    exit 1
elif (( ${#PHASES[@]} == 0 )); then
    print_banner "$C_YEL" "RUN-ALL-TESTS: NOTHING RAN"
    exit 1
else
    print_banner "$C_GRN" "RUN-ALL-TESTS: PASS  (${PASS_COUNT}/${#PHASES[@]})"
    exit 0
fi
