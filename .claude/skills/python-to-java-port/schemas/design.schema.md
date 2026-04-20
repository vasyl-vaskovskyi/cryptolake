# design.md schema

Produced by: Architect.
Path: `docs/superpowers/port/<module>/design.md`.

## Frontmatter (required)

```
---
module: <name>
status: approved                  # MUST be 'approved' for Developer dispatch
produced_by: architect
based_on_mapping: <sha>
---
```

## Required sections (in order)

1. `## 1. Package layout`
2. `## 2. Class catalog`
3. `## 3. Concurrency design`
4. `## 4. Python → Java mapping table`
5. `## 5. Library mapping`
6. `## 6. Data contracts`
7. `## 7. Error model`
8. `## 8. Test plan`
9. `## 9. Metrics plan`
10. `## 10. Rule compliance`
11. `## 11. Open questions for developer`

## Validation rules

- Frontmatter present with all 4 keys; `status: approved` literal (any other value halts Developer dispatch).
- All 11 section headings present.
- `## 10.` lists every rule from Tiers 1, 2, 3 with `honored by <class:method>` or `N/A because <reason>`.
- No `TBD`, `TODO`, `???`, `FIXME`.
