# mapping.md schema

Produced by: Analyst.
Path: `docs/superpowers/port/<module>/mapping.md`.

## Frontmatter (required)

```
---
module: <name>
status: complete
produced_by: analyst
python_files: [list of source files]
python_test_files: [list of test files]
---
```

## Required sections (in order)

1. `## 1. Module summary`
2. `## 2. File inventory`
3. `## 3. Public API surface`
4. `## 4. Internal structure`
5. `## 5. Concurrency surface`
6. `## 6. External I/O`
7. `## 7. Data contracts`
8. `## 8. Test catalog`
9. `## 9. Invariants touched (Tier 1 rules)`
10. `## 10. Port risks`
11. `## 11. Rule compliance`

## Validation rules

- Frontmatter present with all 5 keys.
- `status: complete` literal.
- All 11 section headings present (exact match, including the numeric prefix).
- `## 9.` must list all 7 Tier-1 rules with `touched` or `not applicable`.
- `## 11.` must not be empty.
- No string `TBD`, `TODO`, `???`, `FIXME` anywhere in the body.
