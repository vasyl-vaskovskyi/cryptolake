# Journal

A lightweight, tracked progress log so work continues seamlessly across machines and sessions.

## Why this exists

`git log` records *what* changed. Commit messages capture the *what* well, but they don't capture:

- **Why** a decision was made (the rejected alternatives, the constraint that drove it)
- **What's in-flight** at the end of a session (so the next machine can resume)
- **What's next** (so the first move on the next session isn't "figure out where I left off")
- **Cross-cutting context** that doesn't fit any single commit

The journal is the smallest possible structure that fills those gaps. It is checked into git, so it travels with the repo.

## File layout

```
docs/journal/
  README.md                 ← this file
  YYYY-MM-DD.md             ← one entry per active day (only days with material changes)
```

No weekly rollups, no nesting. Active days only — quiet days get no entry.

## Entry template

```markdown
# YYYY-MM-DD

## Shipped
- `<sha>` short description (one line per commit, or grouped if tightly related)

## Context / decisions
- The *why* behind non-obvious choices made today. Skip if everything was mechanical.

## In-flight (end of day)
- Anything mid-stream: half-implemented features, failing tests, unanswered questions.
- Each item: what + where (file paths, branch names) + what's left.
- Omit the section entirely if nothing is in-flight.

## Next
- The first concrete move for the next session. One or two items.
```

## Conventions

- **Terse.** 3–10 lines per section. If you need more, it probably belongs in a plan file under `docs/superpowers/plans/` and the journal should just point at it.
- **Reference commits by short SHA**, not by hand-recapping diffs.
- **Reference plans/specs by path**, e.g. `docs/superpowers/plans/2026-06-14-walking-skeleton.md`.
- **One entry per active day.** Multiple sessions on the same day go into the same file; just append a `## Session HH:MM` block if you need to separate them.
- **Append-only after the fact.** Edits to past entries are fine to fix wrong info, but don't rewrite history to look tidier.

## Relationship to other artefacts

| Artefact | Scope | Tracked in git? |
|---|---|---|
| `docs/00-master-spec.md` | The system invariants and behaviour. Authoritative. | Yes |
| `docs/superpowers/specs/*.md` | Component-level designs (e.g. hot-swap, WS rotation). | Yes |
| `docs/superpowers/plans/*.md` | TDD-style implementation plans with checkboxes. | Yes |
| `docs/journal/*.md` (this dir) | Daily progress, decisions, what's next. | Yes |
| `.remember/*.md` | Per-session WIP buffer (superpowers plugin). | **No** (gitignored) |
| `git log` | Authoritative atomic record of *what* changed. | n/a |

The journal is the bridge between `git log` (what) and the plans/spec (the larger why). `.remember/` is local-only scratch and does not travel.

## Workflow

1. **Start of session on a new machine:** `git pull`, then read the most recent journal entry. Resume from its `## Next`.
2. **During work:** make commits as normal.
3. **End of session:** append/create today's `YYYY-MM-DD.md`. ~60 seconds. Commit it.

If a session ends in the middle of something, the `## In-flight` section is the load-bearing part — write it like you're handing off to a teammate who hasn't seen your screen.
