#!/usr/bin/env bash
# run_gates.sh — runs gates 1–7 sequentially for a module.
# Records each result via state.sh. Exit code: 0 all pass, 1 any fail, 2 gate7 pending.

set -uo pipefail

MODULE="${1:?module}"
STATE_SH=".claude/skills/python-to-java-port/scripts/state.sh"
GATE_DIR=".claude/skills/python-to-java-port/scripts/gates"
FAILURE_LOG="docs/superpowers/port/${MODULE}/.last-gate-failure.txt"
mkdir -p "$(dirname "$FAILURE_LOG")"
: > "$FAILURE_LOG"

run_gate() {
  local n="$1" name="$2" script="$3"
  echo "=== gate $n: $name ==="
  if output=$(bash "$script" "$MODULE" 2>&1); then
    echo "$output"
    bash "$STATE_SH" record_gate_result "$n" pass
    return 0
  fi
  local rc=$?
  echo "$output"
  if [[ $rc -eq 2 && "$n" == "7" ]]; then
    bash "$STATE_SH" record_gate_result 7 pending
    return 2
  fi
  echo "=== gate $n FAILED ===" >> "$FAILURE_LOG"
  echo "$output" >> "$FAILURE_LOG"
  bash "$STATE_SH" record_gate_result "$n" fail
  return 1
}

FINAL=0
run_gate 1 "unit_tests"         "$GATE_DIR/gate1_unit_tests.sh"         || FINAL=1
run_gate 2 "chaos_tests"        "$GATE_DIR/gate2_chaos_tests.sh"        || FINAL=1
run_gate 3 "raw_text_parity"    "$GATE_DIR/gate3_raw_text_parity.sh"    || FINAL=1
run_gate 4 "metric_parity"      "$GATE_DIR/gate4_metric_parity.sh"      || FINAL=1
run_gate 5 "verify_cli"         "$GATE_DIR/gate5_verify_cli.sh"         || FINAL=1
run_gate 6 "static_checks"      "$GATE_DIR/gate6_static_checks.sh"      || FINAL=1

# gate 7 may be "pending" (no signoff yet) — that's not a hard failure for this run.
gate7_rc=0
run_gate 7 "architect_signoff"  "$GATE_DIR/gate7_architect_signoff.sh"  || gate7_rc=$?
if [[ $gate7_rc -eq 1 ]]; then FINAL=1; fi

if [[ $FINAL -eq 0 && $gate7_rc -eq 0 ]]; then
  echo "ALL GATES PASS"
  exit 0
fi
if [[ $FINAL -eq 0 && $gate7_rc -eq 2 ]]; then
  echo "GATES 1-6 PASS; gate 7 PENDING architect dispatch"
  exit 2
fi
echo "GATES FAILED; see $FAILURE_LOG"
exit 1
