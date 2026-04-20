#!/usr/bin/env bash
# Assembles a role prompt by substituting placeholders into the template.
# Usage: assemble_prompt.sh <role> <module> [attempt_number] [failure_details]
# Prints the assembled prompt to stdout.

set -euo pipefail

ROLE="${1:?role: analyst|architect|developer}"
MODULE="${2:?module name}"
ATTEMPT="${3:-1}"
FAILURE="${4:-none}"

TEMPLATE=".claude/skills/python-to-java-port/prompts/${ROLE}.md"
[[ -f "$TEMPLATE" ]] || { echo "no template: $TEMPLATE" >&2; exit 1; }

TIER5_FILE=".claude/skills/python-to-java-port/tier5-translation-rules.md"
[[ -f "$TIER5_FILE" ]] || { echo "tier5 translation-rules file missing: $TIER5_FILE" >&2; exit 1; }

# Module-scoped file lists.
# Test-file list is scoped to files whose *only* top-level src.* imports are
# from src.<module> (optionally also src.exchanges as a shared helper).
# A test that imports both src.common and src.writer is primarily a writer
# test; it doesn't belong in the common Analyst's §8. This keeps the
# Analyst focused instead of cataloguing cross-module tests.
_scope_tests_for_module() {
  local target="$1"
  local other_pattern
  case "$target" in
    common)    other_pattern='src\.(writer|collector|cli)' ;;
    writer)    other_pattern='src\.(common|collector|cli)' ;;
    collector) other_pattern='src\.(common|writer|cli)' ;;
    cli)       other_pattern='src\.(common|writer|collector)' ;;
  esac
  grep -rl "from src\\.${target}\\b\\|import src\\.${target}\\b" tests 2>/dev/null \
    | while read -r f; do
        grep -qE "from $other_pattern|import $other_pattern" "$f" || echo "$f"
      done \
    | sort -u | sed 's/^/- /'
}

PY_FILES=""
PY_TEST_FILES=""
case "$MODULE" in
  common|writer|collector)
    PY_FILES="$(find "src/$MODULE" -type f -name '*.py' 2>/dev/null | sort | sed 's/^/- /')"
    PY_TEST_FILES="$(_scope_tests_for_module "$MODULE" || true)"
    ;;
  cli)
    PY_FILES="$(find src/cli -type f -name '*.py' 2>/dev/null | sort | sed 's/^/- /')"
    PY_TEST_FILES="$(_scope_tests_for_module cli || true)"
    ;;
  *) echo "unknown module: $MODULE" >&2; exit 1 ;;
esac

MAPPING_SHA=""
if [[ -f "docs/superpowers/port/$MODULE/mapping.md" ]]; then
  MAPPING_SHA="$(git hash-object "docs/superpowers/port/$MODULE/mapping.md" 2>/dev/null || echo "uncommitted")"
fi

GATE_FAILURES=""
if [[ "$ROLE" == "developer" && "$ATTEMPT" -gt 1 ]]; then
  GATE_FAILURES="$(cat "docs/superpowers/port/$MODULE/.last-gate-failure.txt" 2>/dev/null || echo "(no stored failure)")"
fi

# Export values for Python substitution (Python handles multi-line replacements
# cleanly, unlike BSD awk on macOS which rejects newlines in -v variables).
export P2J_TEMPLATE="$TEMPLATE"
export P2J_TIER5_FILE="$TIER5_FILE"
export P2J_MODULE="$MODULE"
export P2J_ATTEMPT="$ATTEMPT"
export P2J_FAILURE="$FAILURE"
export P2J_PY_FILES="$PY_FILES"
export P2J_PY_TEST_FILES="$PY_TEST_FILES"
export P2J_MAPPING_SHA="$MAPPING_SHA"
export P2J_GATE_FAILURES="$GATE_FAILURES"

python3 <<'PY'
import os
import sys
from pathlib import Path

template_path = Path(os.environ["P2J_TEMPLATE"])
tier5_path = Path(os.environ["P2J_TIER5_FILE"])

replacements = {
    "{{module}}":         os.environ["P2J_MODULE"],
    "{{attempt_number}}": os.environ["P2J_ATTEMPT"],
    "{{failure_details}}":os.environ["P2J_FAILURE"],
    "{{python_files}}":   os.environ["P2J_PY_FILES"],
    "{{python_test_files}}": os.environ["P2J_PY_TEST_FILES"],
    "{{mapping_sha}}":    os.environ["P2J_MAPPING_SHA"],
    "{{gate_failures}}":  os.environ["P2J_GATE_FAILURES"],
    "{{tier5_rules}}":    tier5_path.read_text(),
}

out = template_path.read_text()
for placeholder, value in replacements.items():
    out = out.replace(placeholder, value)

sys.stdout.write(out)
PY
