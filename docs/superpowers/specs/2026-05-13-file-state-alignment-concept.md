# File ↔ State Alignment — Concept

**Date:** 2026-05-13
**Status:** concept draft (awaiting user decision)

## The problem in one line

CryptoLake has two parallel gap-reporting systems — gap envelopes on disk and a half-dozen state surfaces (PG, ledger, sidecars, manifests, metrics) — that are written by different code paths under different durability guarantees and therefore drift.

## What the investigation found

Concrete divergence vectors that exist today:

1. **Silent SQL failures.** `LifecycleStateManager` catches every `SQLException`, logs WARN, and continues. A column-name bug caused `last_heartbeat_at` to never update for weeks; the file side was correct (no gap on disk) but PG showed the collector as dead.
2. **`kafka_outage.json` has no cleanup path.** If the producer recovers but the collector crashes before deleting the file, it orphans. File side: outage active. PG side: nothing.
3. **`clean_shutdown_at` SIGKILL semantics.** On `kill -9`, the row stays in its prior state. Next startup writes a new row with fresh `started_at`. The old row's gap window can be wildly wrong.
4. **Manifest time-of-flight.** Gap envelopes hit disk in seconds; `missing_hours[]` in the manifest is written hours later (when consolidation runs). For audit windows < 1 day, the two views disagree.
5. **`CoverageFilter` suppresses gaps in memory.** When the backup source covers a primary-source gap, no envelope is ever written. PG/ledger may still have a record of the failover; the audit reconciler sees an "orphan state" row.
6. **Failed consolidation leaves no breadcrumb.** If the consolidation process crashes mid-day, the daily file never gets created, and no state surface records "consolidation failed for date D" — the failure is only visible the next time someone runs an audit.
7. **Lifecycle ledger corruption is silent.** `LifecycleJournalReader` degrades gracefully on missing/corrupt files. Partial evidence is treated as no evidence.

The pattern: every divergence vector is a place where one write succeeds and a sibling write fails (or is delayed), with no transaction binding them together.

## The principle

> **One write, two views.** Every gap-relevant fact lands in *one* durable log. File-side and state-side surfaces are both materializations of that log. Two surfaces cannot disagree if they read from the same authoritative table.

This makes drift impossible *by construction*, not by discipline.

## Three concept options

### Option A — "Outbox" / Single Write-Ahead Log

**Idea:** Add a new PG table `gap_event_log` (append-only) plus a JSONL mirror on disk. Every gap-related code path writes here *first*. Both the archive file envelope and the per-component state rows are derived (synchronously or asynchronously) from this log.

```
                ┌───────────────────────┐
                │   gap_event_log (PG)  │  ← single authoritative write
                │   + gap_events.jsonl  │
                └───────────┬───────────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        gap envelope   component_runtime  manifest
        in *.jsonl.zst    state row     missing_hours[]
```

Pros: divergence cannot exist; future audit becomes a single SELECT.
Cons: invasive rewrite; introduces a critical-path PG dependency on the collector hot path.

### Option B — "Archive-as-Truth" with Materialized State

**Idea:** Treat the archive (`*.jsonl.zst` + a new per-process `gap_events.jsonl` ledger) as the canonical record. PG becomes a derived cache, rebuilt by a "materializer" process that runs continuously. If state and archive diverge, the materializer overwrites state from archive.

```
       archive (*.jsonl.zst)    gap_events.jsonl
              │                       │
              └───────────┬───────────┘
                          ▼
                  materializer daemon
                          │
                          ▼
                 PG state (cache)
```

Pros: aligns with CryptoLake's existing "raw payload fidelity" invariant — the archive *is* the truth, already. State drift becomes self-healing (PG rebuilt from disk).
Cons: introduces a new daemon; lifecycle events (maintenance intents, planned shutdowns) need a place to land *before* a file envelope is written, so a small writeahead ledger is still needed.

### Option C — "Discipline + Continuous Reconciler"

**Idea:** Keep the current write paths but add three rules and one process:
- Rule 1: make silent failures loud. Heartbeat SQL error → `Runtime.exit()`. No more best-effort on gap-relevant code paths.
- Rule 2: every state mutation must be paired with its file-side marker, in this order: (1) write gap envelope, (2) confirm durable, (3) update PG. If step 3 fails after step 2 succeeded, re-emit step 3 on restart.
- Rule 3: every sidecar (`kafka_outage.json`) must have an explicit cleanup path with a "stale-after" TTL.
- Process: a "reconciler daemon" runs every N minutes, applies the existing `audit reconcile` logic to a sliding window, and POSTs to Alertmanager on any divergence older than the TTL.

Pros: incremental, low-risk, builds on existing tooling.
Cons: doesn't fix the architecture; just narrows and monitors the divergence window. Drift is still possible during the window.

## Recommendation: Option B + the loud-failure parts of Option C

Why:

- The archive is *already* declared the source of truth (CLAUDE.md §"raw-payload fidelity"). The current state surfaces are *operationally* the source — that's the contradiction. Option B resolves it without philosophical churn.
- The collector hot path stays cheap (no synchronous PG dependency added). PG remains a cache, rebuilt from disk.
- Option C's "loud failure" disciplines are required either way — fixing them in isolation buys immediate reliability before any architectural change.
- The materializer daemon is the same kind of process the existing `audit reconcile` already is — we extend it from "report" to "report + write".

What lands in this design:

1. **New per-collector `gap_events.jsonl` write-ahead ledger** alongside the existing `lifecycle.jsonl`. Every gap *intent* lands here first (sequence number + monotonic ts), with `fsync`. Only then does the gap envelope ship to Kafka and the PG row attempt to write.
2. **Materializer daemon** (could live in `consolidation` or a new `materializer` module). Periodically: read archive + `gap_events.jsonl` + `lifecycle.jsonl` + `kafka_outage.json` into a canonical view, diff against PG state, and rewrite PG to match. PG becomes idempotent-overwrite.
3. **Loud failures** (immediate, independent fix): `LifecycleStateManager.heartbeat()` and friends crash on SQL error instead of swallowing. The `heartbeat_failed` log spam becomes impossible because the process exits and is restarted by systemd, which writes a clean lifecycle event.
4. **Sidecar TTL + cleanup**: `kafka_outage.json` carries `started_at_ns` already; add `expected_resolution_by_ns` and on startup, if `now > expected_resolution_by_ns` and the producer is healthy, delete the file and emit a `kafka_producer_outage` gap envelope of the elapsed window (so the audit sees the outage even after the process crash).
5. **Consolidation breadcrumbs**: when consolidation runs (or fails) for date D, write a `consolidation_run.jsonl` event so failed runs are visible. This is one new event type in the ledger; the materializer fans it into a PG row.
6. **CoverageFilter audit trail**: when a gap is suppressed by coverage, record a `coverage_suppressed_gap` event in the ledger. The audit reconciler can then verify "every state-side gap that was suppressed has a matching ledger entry" — orphan state rows become impossible.

## Phased rollout

| Phase | What lands | Risk | Approx scope |
|---|---|---|---|
| **1 — Loud failures** | Replace best-effort SQL with fail-fast in `LifecycleStateManager` + writer's `StateManager`. Add a sidecar TTL to `kafka_outage.json`. | low | 1–2 PRs |
| **2 — Write-ahead ledger** | Add `gap_events.jsonl` to collector + writer. Every gap emit-site writes to ledger before Kafka. Add `consolidation_run` and `coverage_suppressed_gap` event types. | medium | 1 design + 1 plan |
| **3 — Materializer** | New process: read ledger + archive, derive PG state, overwrite. Run as a daemon. | medium | 1 design + 1 plan |
| **4 — Retire write-paths** | Collector + writer stop writing PG directly; ledger + materializer are the only writers. | low (after phases 1–3 stabilize) | 1 small PR |
| **5 — Audit becomes a single query** | `audit reconcile` reads from materialized PG only; the file-walking sources stay as offline integrity tools. | low | docs + tooling |

## Things to fix immediately regardless of which concept you pick

These are bug-class issues that don't depend on the architectural decision:

- **Silent heartbeat / state writes** — change `LifecycleStateManager` to `Runtime.exit(1)` on `SQLException` after N retries (or even N=1). Same for the writer's state manager.
- **`kafka_outage.json` orphan path** — add a delete on resume, plus a startup check that emits a gap envelope for the orphaned window.
- **Lifecycle journal corruption** — make corruption an error, not a graceful degradation. The reconciler should see "ledger corrupted at offset X" and surface it.

## What I want from you next

Three branching options:

1. **Approve Option B** — I write a full design spec for Phase 2 (write-ahead ledger), get your sign-off, plan it out, execute via subagent-driven-development.
2. **Approve Phase 1 only** for now (the loud-failures + TTL cleanup) — small, fast, defers the architectural call.
3. **Pick a different option** (A or C) or push back on the framing.

You can also park this and revisit later — the refactor is at a clean break point.
