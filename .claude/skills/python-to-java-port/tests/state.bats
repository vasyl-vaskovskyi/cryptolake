#!/usr/bin/env bats
# tests/state.bats

load '../scripts/lib/assert.bash'

SCRIPT=".claude/skills/python-to-java-port/scripts/state.sh"

setup() {
  TMPDIR="$(mktemp -d)"
  export PORT_STATE_FILE="$TMPDIR/state.json"
}

teardown() {
  rm -rf "$TMPDIR"
}

@test "init creates state.json with 4 pending modules" {
  bash "$SCRIPT" init
  assert_file_exists "$PORT_STATE_FILE"
  assert_json_eq "$PORT_STATE_FILE" '.version' '1'
  assert_json_eq "$PORT_STATE_FILE" '.current_module' 'common'
  assert_json_eq "$PORT_STATE_FILE" '.modules | length' '4'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].name' 'common'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].status' 'in_progress'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].phase' 'pending'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].name' 'writer'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].status' 'pending'
}

@test "init refuses to overwrite existing state.json" {
  bash "$SCRIPT" init
  run bash "$SCRIPT" init
  [ "$status" -ne 0 ]
  [[ "$output" == *"already exists"* ]]
}

@test "get_current_phase returns pending after init" {
  bash "$SCRIPT" init
  run bash "$SCRIPT" get_current_phase
  [ "$status" -eq 0 ]
  assert_equal "pending" "$output"
}

@test "set_phase updates the current module's phase" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase analyst
  run bash "$SCRIPT" get_current_phase
  assert_equal "analyst" "$output"
}

@test "advance_module rotates current to writer" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase accepted
  bash "$SCRIPT" advance_module
  run bash "$SCRIPT" get_current_module
  assert_equal "writer" "$output"
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].status' 'in_progress'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].phase' 'pending'
}

@test "advance_module refuses if current phase != accepted" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase developer
  run bash "$SCRIPT" advance_module
  [ "$status" -ne 0 ]
  [[ "$output" == *"not accepted"* ]]
}

@test "set_halt writes halt_reason; clear_halt removes it" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_halt "gate_failed"
  assert_json_eq "$PORT_STATE_FILE" '.halt_reason' 'gate_failed'
  bash "$SCRIPT" clear_halt
  assert_json_eq "$PORT_STATE_FILE" '.halt_reason' 'null'
}

@test "record_gate_result stores pass/fail on current module" {
  bash "$SCRIPT" init
  bash "$SCRIPT" record_gate_result 3 pass
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].gates.raw_text_byte_parity' 'pass'
}

@test "increment_attempts bumps analyst counter" {
  bash "$SCRIPT" init
  bash "$SCRIPT" increment_attempts analyst
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].attempts.analyst' '1'
  bash "$SCRIPT" increment_attempts analyst
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].attempts.analyst' '2'
}
