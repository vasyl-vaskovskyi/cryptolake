#!/usr/bin/env bash
# Master test runner: unit, integration, and chaos tests.
# Usage: scripts/run-all-tests.sh [--unit] [--integration] [--chaos] [--chaos-test N]
# No flags = run all test suites.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${REPO_ROOT}"

UNIT=false
INTEGRATION=false
CHAOS=false
CHAOS_TEST=""
RUN_ALL=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --unit)        UNIT=true; RUN_ALL=false; shift ;;
        --integration) INTEGRATION=true; RUN_ALL=false; shift ;;
        --chaos)       CHAOS=true; RUN_ALL=false; shift ;;
        --chaos-test)  CHAOS=true; RUN_ALL=false; CHAOS_TEST="$2"; shift 2 ;;
        -h|--help)
            echo "Usage: $0 [--unit] [--integration] [--chaos] [--chaos-test N]"
            echo "  No flags = run all test suites."
            echo "  --chaos-test N = run only chaos test N (e.g. --chaos-test 3)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

if $RUN_ALL; then UNIT=true; INTEGRATION=true; CHAOS=true; fi

PASS=0
FAIL=0
SKIP=0
RESULTS=()

record() {
    local suite="$1" status="$2" detail="${3:-}"
    if [[ "$status" == "PASS" ]]; then
        PASS=$((PASS + 1))
        RESULTS+=("  PASS  $suite${detail:+ ($detail)}")
    elif [[ "$status" == "SKIP" ]]; then
        SKIP=$((SKIP + 1))
        RESULTS+=("  SKIP  $suite${detail:+ ($detail)}")
    else
        FAIL=$((FAIL + 1))
        RESULTS+=("  FAIL  $suite${detail:+ ($detail)}")
    fi
}

SECONDS=0

# ─────────────────────────────────────────────────────────────────────────────
# Unit Tests
# ─────────────────────────────────────────────────────────────────────────────
if $UNIT; then
    echo "==========================================="
    echo "  UNIT TESTS"
    echo "==========================================="
    if uv run pytest tests/unit/ -v --tb=short 2>&1; then
        record "unit tests" "PASS"
    else
        record "unit tests" "FAIL"
    fi
    echo ""
fi

# ─────────────────────────────────────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────────────────────────────────────
if $INTEGRATION; then
    echo "==========================================="
    echo "  INTEGRATION TESTS"
    echo "==========================================="
    if docker info >/dev/null 2>&1; then
        if uv run pytest tests/integration/ -v -m integration --tb=short 2>&1; then
            record "integration tests" "PASS"
        else
            record "integration tests" "FAIL"
        fi
    else
        record "integration tests" "SKIP" "Docker not available"
    fi
    echo ""
fi

# ─────────────────────────────────────────────────────────────────────────────
# Chaos Tests
# ─────────────────────────────────────────────────────────────────────────────
if $CHAOS; then
    echo "==========================================="
    echo "  CHAOS TESTS"
    echo "==========================================="

    # Check if production stack is running
    prod_running=$(docker compose -f docker-compose.yml ps -q 2>/dev/null | head -1 || true)
    if [[ -n "${prod_running}" ]]; then
        echo "WARNING: Production stack is running. Skipping chaos tests."
        record "chaos tests" "SKIP" "production stack running"
    else
        chaos_scripts=()
        if [[ -n "$CHAOS_TEST" ]]; then
            # Run specific chaos test
            match=$(ls tests/chaos/${CHAOS_TEST}_*.sh 2>/dev/null || true)
            if [[ -z "$match" ]]; then
                echo "ERROR: No chaos test matching '${CHAOS_TEST}'"
                exit 1
            fi
            chaos_scripts=("$match")
        else
            # Run all chaos tests in order
            for f in tests/chaos/[0-9]*.sh; do
                chaos_scripts+=("$f")
            done
        fi

        COMPOSE_FILE="${REPO_ROOT}/docker-compose.test.yml"

        # Pre-suite cleanup: ensure no leftover test resources from a prior run
        echo "Cleaning stale Docker resources from project cryptolake-test..."
        docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true

        for script in "${chaos_scripts[@]}"; do
            name="$(basename "$script" .sh)"
            echo ""
            echo "--- Running: ${name} ---"
            start_ts=$SECONDS
            if bash "$script" 2>&1; then
                elapsed=$((SECONDS - start_ts))
                record "chaos: ${name}" "PASS" "${elapsed}s"
            else
                elapsed=$((SECONDS - start_ts))
                record "chaos: ${name}" "FAIL" "${elapsed}s"
            fi
            # Safety net: ensure all test containers/volumes/networks are gone
            # before the next test, even if the test's own teardown was incomplete.
            docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true
        done
    fi
    echo ""
fi

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
total_elapsed=$SECONDS
echo "==========================================="
echo "  FINAL RESULTS"
echo "==========================================="
for r in "${RESULTS[@]}"; do
    echo "$r"
done
echo "-------------------------------------------"
echo "  Total: $((PASS + FAIL + SKIP)) suites"
echo "  Pass:  ${PASS}  Fail: ${FAIL}  Skip: ${SKIP}"
echo "  Time:  ${total_elapsed}s"
echo "==========================================="

if (( FAIL > 0 )); then exit 1; fi
