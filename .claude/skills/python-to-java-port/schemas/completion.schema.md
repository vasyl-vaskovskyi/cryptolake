# completion.md schema

Produced by: Developer.
Path: `docs/superpowers/port/<module>/completion.md`.

## Frontmatter (required)

```
---
module: <name>
status: complete
produced_by: developer
commits: [list of sha]             # range: developer_start_sha..HEAD on main
---
```

## Required sections (in order)

1. `## 1. Gate results`
2. `## 2. Deviations from design`
3. `## 3. Rule compliance`
4. `## 4. Escalations`
5. `## 5. Known follow-ups`

## Validation rules

- Frontmatter present with all 4 keys; `status: complete` literal.
- All 5 section headings present.
- `## 1.` reports all 7 gates, each with pass/fail state.
- `## 3.` lists every rule from Tiers 1, 2, 3 with `file:line` reference.
- No `TBD`, `TODO`, `???`, `FIXME`.
