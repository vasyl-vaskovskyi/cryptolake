#!/usr/bin/env bash
# .claude/skills/python-to-java-port/scripts/validate_artifact.sh

set -euo pipefail

FORBIDDEN_STRINGS=("TBD" "TODO" "???" "FIXME" "XXX")

_fail() {
  echo "VALIDATION FAILED: $*" >&2
  exit 1
}

_check_forbidden() {
  local file="$1"
  for bad in "${FORBIDDEN_STRINGS[@]}"; do
    if grep -q -- "$bad" "$file"; then
      _fail "forbidden placeholder '$bad' present in $file"
    fi
  done
}

_check_frontmatter_value() {
  local file="$1" key="$2" expected="$3"
  local actual
  actual="$(awk -v k="^$key:" '/^---$/ {in_fm=!in_fm; next} in_fm && $0 ~ k {sub(k " *",""); print; exit}' "$file")"
  actual="${actual# }"
  if [[ "$actual" != "$expected" ]]; then
    _fail "$file frontmatter '$key' is '$actual', expected '$expected'"
  fi
}

_check_heading() {
  local file="$1" heading="$2"
  if ! grep -Fxq "$heading" "$file"; then
    _fail "missing required heading '$heading' in $file"
  fi
}

validate_mapping() {
  local file="$1"
  [[ -f "$file" ]] || _fail "file not found: $file"
  _check_forbidden "$file"
  _check_frontmatter_value "$file" "status" "complete"
  _check_frontmatter_value "$file" "produced_by" "analyst"
  local headings=(
    "## 1. Module summary"
    "## 2. File inventory"
    "## 3. Public API surface"
    "## 4. Internal structure"
    "## 5. Concurrency surface"
    "## 6. External I/O"
    "## 7. Data contracts"
    "## 8. Test catalog"
    "## 9. Invariants touched (Tier 1 rules)"
    "## 10. Port risks"
    "## 11. Rule compliance"
  )
  for h in "${headings[@]}"; do _check_heading "$file" "$h"; done
}

validate_design() {
  local file="$1"
  [[ -f "$file" ]] || _fail "file not found: $file"
  _check_forbidden "$file"
  _check_frontmatter_value "$file" "status" "approved"
  _check_frontmatter_value "$file" "produced_by" "architect"
  local headings=(
    "## 1. Package layout"
    "## 2. Class catalog"
    "## 3. Concurrency design"
    "## 4. Python → Java mapping table"
    "## 5. Library mapping"
    "## 6. Data contracts"
    "## 7. Error model"
    "## 8. Test plan"
    "## 9. Metrics plan"
    "## 10. Rule compliance"
    "## 11. Open questions for developer"
  )
  for h in "${headings[@]}"; do _check_heading "$file" "$h"; done
}

validate_completion() {
  local file="$1"
  [[ -f "$file" ]] || _fail "file not found: $file"
  _check_forbidden "$file"
  _check_frontmatter_value "$file" "status" "complete"
  _check_frontmatter_value "$file" "produced_by" "developer"
  local headings=(
    "## 1. Gate results"
    "## 2. Deviations from design"
    "## 3. Rule compliance"
    "## 4. Escalations"
    "## 5. Known follow-ups"
  )
  for h in "${headings[@]}"; do _check_heading "$file" "$h"; done
}

main() {
  local kind="${1:-}" file="${2:-}"
  case "$kind" in
    mapping)    validate_mapping "$file" ;;
    design)     validate_design "$file" ;;
    completion) validate_completion "$file" ;;
    *) echo "usage: validate_artifact.sh {mapping|design|completion} <file>" >&2; exit 2 ;;
  esac
  echo "OK: $file"
}

main "$@"
