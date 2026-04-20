# .claude/skills/python-to-java-port/scripts/lib/assert.bash
# Minimal assertion helpers for bats tests.

assert_equal() {
  local expected="$1" actual="$2"
  if [[ "$expected" != "$actual" ]]; then
    echo "expected: $expected" >&2
    echo "actual:   $actual" >&2
    return 1
  fi
}

assert_file_exists() {
  local path="$1"
  if [[ ! -f "$path" ]]; then
    echo "expected file to exist: $path" >&2
    return 1
  fi
}

assert_json_eq() {
  local path="$1" jq_expr="$2" expected="$3"
  local actual
  actual="$(jq -r "$jq_expr" "$path")"
  assert_equal "$expected" "$actual"
}
