#!/usr/bin/env bats

load '../scripts/lib/assert.bash'

SCRIPT=".claude/skills/python-to-java-port/scripts/validate_artifact.sh"

setup() {
  TMPDIR="$(mktemp -d)"
}

teardown() {
  rm -rf "$TMPDIR"
}

write_mapping_good() {
  cat > "$1" <<'EOF'
---
module: common
status: complete
produced_by: analyst
python_files: [foo.py]
python_test_files: [test_foo.py]
---

## 1. Module summary
x

## 2. File inventory
x

## 3. Public API surface
x

## 4. Internal structure
x

## 5. Concurrency surface
x

## 6. External I/O
x

## 7. Data contracts
x

## 8. Test catalog
x

## 9. Invariants touched (Tier 1 rules)
rule 1: not applicable
rule 2: not applicable
rule 3: not applicable
rule 4: not applicable
rule 5: not applicable
rule 6: not applicable
rule 7: not applicable

## 10. Port risks
x

## 11. Rule compliance
surfaced in §9
EOF
}

@test "mapping: valid file passes" {
  write_mapping_good "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -eq 0 ]
}

@test "mapping: missing section 11 fails" {
  write_mapping_good "$TMPDIR/mapping.md"
  # drop §11
  sed -i.bak '/## 11./,$d' "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -ne 0 ]
  [[ "$output" == *"## 11."* ]]
}

@test "mapping: TBD in body fails" {
  write_mapping_good "$TMPDIR/mapping.md"
  echo "TBD: finish this" >> "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -ne 0 ]
  [[ "$output" == *"TBD"* ]]
}

@test "mapping: TODO in body is ALLOWED (analyst may describe Python TODOs)" {
  write_mapping_good "$TMPDIR/mapping.md"
  echo "The Python source has a \`# TODO: handle empty case\` comment at producer.py:42" >> "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -eq 0 ]
}

@test "mapping: wrong status in frontmatter fails" {
  write_mapping_good "$TMPDIR/mapping.md"
  sed -i.bak 's/status: complete/status: draft/' "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -ne 0 ]
}

@test "design: status approved passes minimal skeleton" {
  cat > "$TMPDIR/design.md" <<'EOF'
---
module: common
status: approved
produced_by: architect
based_on_mapping: abc123
---

## 1. Package layout
x
## 2. Class catalog
x
## 3. Concurrency design
x
## 4. Python → Java mapping table
x
## 5. Library mapping
x
## 6. Data contracts
x
## 7. Error model
x
## 8. Test plan
x
## 9. Metrics plan
x
## 10. Rule compliance
x
## 11. Open questions for developer
none
EOF
  run bash "$SCRIPT" design "$TMPDIR/design.md"
  [ "$status" -eq 0 ]
}

@test "design: status draft fails" {
  cat > "$TMPDIR/design.md" <<'EOF'
---
module: common
status: draft
produced_by: architect
based_on_mapping: abc
---

## 1. Package layout
x
## 2. Class catalog
x
## 3. Concurrency design
x
## 4. Python → Java mapping table
x
## 5. Library mapping
x
## 6. Data contracts
x
## 7. Error model
x
## 8. Test plan
x
## 9. Metrics plan
x
## 10. Rule compliance
x
## 11. Open questions for developer
x
EOF
  run bash "$SCRIPT" design "$TMPDIR/design.md"
  [ "$status" -ne 0 ]
  [[ "$output" == *"approved"* ]]
}
