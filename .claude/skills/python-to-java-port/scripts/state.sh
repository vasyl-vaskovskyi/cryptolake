#!/usr/bin/env bash
# .claude/skills/python-to-java-port/scripts/state.sh
# State-machine helpers for the port orchestrator.

set -euo pipefail

STATE_FILE="${PORT_STATE_FILE:-docs/superpowers/port/state.json}"

MODULES='["common","writer","collector","cli"]'

_read() {
  [[ -f "$STATE_FILE" ]] || { echo "state file missing: $STATE_FILE" >&2; exit 1; }
  cat "$STATE_FILE"
}

_update() {
  local filter="$1"
  local tmp
  tmp="$(mktemp)"
  _read | jq "$filter" > "$tmp"
  mv "$tmp" "$STATE_FILE"
}

_update_with_args() {
  # Usage: _update_with_args --arg KEY VAL ... -- '<filter>'
  local args=()
  while [[ "${1:-}" != "--" ]]; do
    args+=("$1"); shift
  done
  shift  # drop --
  local filter="$1"
  local tmp
  tmp="$(mktemp)"
  _read | jq "${args[@]}" "$filter" > "$tmp"
  mv "$tmp" "$STATE_FILE"
}

cmd_init() {
  if [[ -f "$STATE_FILE" ]]; then
    echo "state already exists: $STATE_FILE" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$STATE_FILE")"
  local now; now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  jq -n --arg ts "$now" --argjson modules "$MODULES" '
    {
      version: 1,
      started_at: $ts,
      current_module: "common",
      halt_reason: null,
      modules: ($modules | to_entries | map({
        name: .value,
        status: (if .key == 0 then "in_progress" else "pending" end),
        phase: "pending",
        checkpoint0_done: false,
        artifacts: {mapping:null, design:null, completion:null},
        gates: {
          ported_unit_tests:null,
          ported_chaos_tests:null,
          raw_text_byte_parity:null,
          metric_parity:null,
          verify_cli:null,
          static_checks:null,
          architect_signoff:null
        },
        attempts: {analyst:0, architect:0, developer:0},
        escalations: []
      }))
    }
  ' > "$STATE_FILE"
}

cmd_get_current_module() {
  _read | jq -r '.current_module'
}

cmd_get_current_phase() {
  _read | jq -r '
    .current_module as $cm
    | (.modules[] | select(.name == $cm) | .phase)
  '
}

cmd_set_phase() {
  local new_phase="$1"
  _update_with_args --arg p "$new_phase" -- '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .phase = $p else . end
      )
  '
}

cmd_advance_module() {
  local cur_phase
  cur_phase="$(cmd_get_current_phase)"
  if [[ "$cur_phase" != "accepted" ]]; then
    echo "current module phase is '$cur_phase', not accepted" >&2
    exit 1
  fi
  _update '
    . as $root
    | (.modules | map(.name) | index($root.current_module)) as $idx
    | if $idx == null or $idx + 1 >= (.modules | length) then
        .current_module = "DONE"
      else
        .current_module = .modules[$idx + 1].name
        | .modules[$idx + 1].status = "in_progress"
        | .modules[$idx + 1].phase = "pending"
      end
  '
}

cmd_set_halt() {
  local reason="$1"
  _update_with_args --arg r "$reason" -- '.halt_reason = $r'
}

cmd_clear_halt() {
  _update '.halt_reason = null'
}

cmd_record_gate_result() {
  local gate_num="$1" result="$2"
  local gate_key
  case "$gate_num" in
    1) gate_key="ported_unit_tests" ;;
    2) gate_key="ported_chaos_tests" ;;
    3) gate_key="raw_text_byte_parity" ;;
    4) gate_key="metric_parity" ;;
    5) gate_key="verify_cli" ;;
    6) gate_key="static_checks" ;;
    7) gate_key="architect_signoff" ;;
    *) echo "bad gate: $gate_num" >&2; exit 1 ;;
  esac
  _update_with_args --arg k "$gate_key" --arg v "$result" -- '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .gates[$k] = $v else . end
      )
  '
}

cmd_increment_attempts() {
  local role="$1"
  _update_with_args --arg r "$role" -- '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .attempts[$r] += 1 else . end
      )
  '
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    init)                    cmd_init ;;
    get_current_module)      cmd_get_current_module ;;
    get_current_phase)       cmd_get_current_phase ;;
    set_phase)               cmd_set_phase "$@" ;;
    advance_module)          cmd_advance_module ;;
    set_halt)                cmd_set_halt "$@" ;;
    clear_halt)              cmd_clear_halt ;;
    record_gate_result)      cmd_record_gate_result "$@" ;;
    increment_attempts)      cmd_increment_attempts "$@" ;;
    *) echo "usage: state.sh {init|get_current_module|get_current_phase|set_phase|advance_module|set_halt|clear_halt|record_gate_result|increment_attempts}" >&2; exit 2 ;;
  esac
}

main "$@"
