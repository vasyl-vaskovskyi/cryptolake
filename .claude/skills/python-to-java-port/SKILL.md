---
name: python-to-java-port
description: Use when porting a Python service to Java 21. Enforces behavioral parity, raw-payload fidelity, 7-gate module-done protocol, and Analyst→Architect→Developer role handoffs. Trigger on "port to Java", "convert to Java", "Java rewrite", or user invocation of /port-module.
---

# python-to-java-port

## Purpose

Port a Python codebase (here: CryptoLake) to Java 21 with behavioral parity, byte-identity for raw payload fidelity, and a fixed Analyst→Architect→Developer handoff per module.

Driven by slash commands in `.claude/commands/port-*.md`. State lives in `docs/superpowers/port/state.json`. Reference design: `docs/superpowers/specs/2026-04-18-python-to-java-port-design.md`.

## Module order (fixed)

1. `common`
2. `writer`
3. `collector`
4. `cli`

Checkpoint 0: after the Analyst produces `common/mapping.md`, halt for user review.
Module boundaries: halt after each module passes all 7 gates; user runs `/port-advance` to proceed.

## Tier 1 — Invariants (never violate)

1. `raw_text` is captured from the `onBinary`/`onText` WebSocket callback BEFORE any JSON parse. Never reconstructed from a parsed object.
2. `raw_sha256` is computed over the exact `raw_text` bytes, once, at capture time. Recomputing downstream is forbidden.
3. Disabled streams emit zero artifacts: no subscribe, no poll, no producer send, no gap record.
4. Kafka consumer offsets are committed only AFTER file flush returns successfully (fsync done). A single function owns this ordering.
5. Every detected gap emits metric, log, and archived gap record — all three.
6. Recovery prefers replay from Kafka or exchange cursors. Reconstruction from inferred state is forbidden.
7. JSON codec must not re-order, re-quote, or re-format `raw_text`. `raw_text` travels byte-for-byte identical to Python's handling.

## Tier 2 — Java practices

8. Java 21 only; `--enable-preview` only if required for `StructuredTaskScope`.
9. No `synchronized (...)` around any blocking call (virtual-thread pinning). Use `ReentrantLock` if locking is unavoidable.
10. No `Thread.sleep` in hot paths; use `ScheduledExecutorService` or parked virtual threads.
11. No reflection-heavy frameworks (no Spring, no CDI, no Guice). Explicit wiring in `main`.
12. Immutable data carriers: Java records, never POJOs with setters.
13. No checked-exception leaks across module boundaries; wrap in `CryptoLakeException`.
14. One `HttpClient` per service; one `KafkaProducer` per service; one `ObjectMapper` per service.
15. Logs are JSON via Logback + Logstash encoder; MDC for `symbol`, `stream`, `session_id`.
16. Fail-fast on invariant violation; retry with exponential backoff only on declared-transient errors.

## Tier 3 — Parity rules

17. Every Python test has a JUnit 5 counterpart with trace comment `// ports: tests/unit/writer/test_X.py::test_name`.
18. Prometheus metric names + labels diff-match Python — generated test asserts set-equality.
19. `raw_text` / `raw_sha256` byte-identity via recorded WebSocket-frame fixture corpus in `parity-fixtures/`.
20. Python `verify` CLI, run against Java-produced archives, must pass.
21. Envelope field order in serialized JSON follows Python canonical order (defined once in `EnvelopeCodec`).

## Tier 5 — Translation patterns

68 rules, 13 categories (A–M): Concurrency, JSON codec, Kafka, WebSocket/HTTP, Numerics, Timestamps, Exceptions, Logging, File I/O, Configuration, CLI, Testing, Domain-specific.

Full content: `.claude/skills/python-to-java-port/tier5-translation-rules.md`.

The orchestrator's `assemble_prompt.sh` inlines Tier 5 verbatim into every Analyst, Architect, and Developer role prompt at dispatch time (under the `{{tier5_rules}}` placeholder). Agents do not need to Read the file separately — it arrives in their prompt.

Analysts cite Tier 5 rule IDs in `mapping.md §10 Port risks`. Architects cite Tier 5 rule IDs in `design.md §10 Rule compliance`. Developers cite Tier 5 rule IDs in `completion.md §3 Rule compliance`.

## Tier 4 — Process rules

22. Agent order per module is fixed: Analyst → Architect → Developer. No out-of-order dispatch.
23. Artifact paths are fixed:
    - Analyst → `docs/superpowers/port/<module>/mapping.md`
    - Architect → `docs/superpowers/port/<module>/design.md`
    - Developer → `docs/superpowers/port/<module>/completion.md`
24. Each artifact must contain a `## Rule compliance` (analyst) / `## 10. Rule compliance` (architect) / `## 3. Rule compliance` (developer) section. Missing = reject + redispatch.
25. Developer does not start until `design.md` frontmatter contains `status: approved`.
26. After Developer reports complete, orchestrator runs all 7 gates; failure re-dispatches Developer (max 3 attempts).
27. At module boundary, orchestrator halts and waits for user — no auto-advance.
28. First-module exception: after Analyst produces `common/mapping.md`, pause for user review (checkpoint 0).

## How to use

Driven exclusively via slash commands:

- `/port-init` — one-time scaffold + fixture capture
- `/port-module` — advance current module by one phase (idempotent)
- `/port-status` — print state.json summary
- `/port-retry` — re-dispatch last failed agent
- `/port-advance` — release module-boundary checkpoint
- `/port-rollback <module>` — state-only rollback

Agents dispatched by this skill receive the role prompt assembled by `scripts/assemble_prompt.sh`, which embeds the rule tiers above verbatim.

## Retry and escalation

| Failure | Action |
|---|---|
| Malformed artifact | Re-dispatch with violation (1 retry), else escalate |
| Gate 1/2/4/5/6 fails | Re-dispatch Developer with output (max 3 attempts), else escalate |
| Gate 7 (architect sign-off) rejected | Re-dispatch Developer with reasons (1 attempt), else escalate |
| Invariant conflict flagged | Halt immediately, escalate |
| Gate 3 (byte-identity) fails | Halt immediately, escalate |
| Developer §4 escalation | Halt, escalate |
