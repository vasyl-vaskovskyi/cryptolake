# Python-to-Java Port Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `python-to-java-port` skill, its orchestrator, 7-gate scripts, 6 slash commands, the Gradle scaffold for `cryptolake-java/`, and the Python fixture-capture "tap" mode — so the user can run `/port-init` to a clean ready-to-port state.

**Architecture:** Shell-based orchestrator (Bash + jq) drives state in `docs/superpowers/port/state.json`. Slash commands are Markdown documents that instruct Claude to run helper scripts and then invoke the `Agent` tool. The skill's rule tiers are embedded verbatim in `SKILL.md` and inlined into role prompts at dispatch time.

**Tech Stack:** Bash, jq, Gradle (Kotlin DSL), Java 21, Python 3.12 (existing collector for tap mode), Markdown (skill + slash commands).

**Scope boundary:** This plan builds the infrastructure. Running the port (`common` → `writer` → `collector` → `cli`) happens **after** this plan completes, via `/port-module` invocations in later sessions. Those module runs are NOT part of this plan.

**Spec reference:** `docs/superpowers/specs/2026-04-18-python-to-java-port-design.md`

**Prerequisites (operator installs before starting):**
- `jq` ≥ 1.6
- `bats-core` ≥ 1.10 (for shell-script tests): `brew install bats-core`
- Java 21 (Temurin recommended): `brew install --cask temurin`
- Existing Python environment with `uv` and running Python collector config

---

## File Structure

### Created

- `.claude/skills/python-to-java-port/SKILL.md` — frontmatter, rule tiers, process
- `.claude/skills/python-to-java-port/prompts/analyst.md`
- `.claude/skills/python-to-java-port/prompts/architect.md`
- `.claude/skills/python-to-java-port/prompts/developer.md`
- `.claude/skills/python-to-java-port/schemas/mapping.schema.md`
- `.claude/skills/python-to-java-port/schemas/design.schema.md`
- `.claude/skills/python-to-java-port/schemas/completion.schema.md`
- `.claude/skills/python-to-java-port/scripts/state.sh`
- `.claude/skills/python-to-java-port/scripts/validate_artifact.sh`
- `.claude/skills/python-to-java-port/scripts/assemble_prompt.sh`
- `.claude/skills/python-to-java-port/scripts/run_gates.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate1_unit_tests.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate2_chaos_tests.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate3_raw_text_parity.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate4_metric_parity.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate5_verify_cli.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate6_static_checks.sh`
- `.claude/skills/python-to-java-port/scripts/gates/gate7_architect_signoff.sh`
- `.claude/skills/python-to-java-port/scripts/capture_fixtures.sh`
- `.claude/skills/python-to-java-port/scripts/lib/assert.bash` — tiny assert helpers used by all tests
- `.claude/skills/python-to-java-port/tests/state.bats`
- `.claude/skills/python-to-java-port/tests/validate_artifact.bats`
- `.claude/commands/port-init.md`
- `.claude/commands/port-module.md`
- `.claude/commands/port-status.md`
- `.claude/commands/port-retry.md`
- `.claude/commands/port-advance.md`
- `.claude/commands/port-rollback.md`
- `cryptolake-java/settings.gradle.kts`
- `cryptolake-java/build.gradle.kts`
- `cryptolake-java/gradle/libs.versions.toml`
- `cryptolake-java/common/build.gradle.kts`
- `cryptolake-java/writer/build.gradle.kts`
- `cryptolake-java/collector/build.gradle.kts`
- `cryptolake-java/backfill/build.gradle.kts`
- `cryptolake-java/consolidation/build.gradle.kts`
- `cryptolake-java/verify/build.gradle.kts`
- `cryptolake-java/docker/Dockerfile.writer`
- `cryptolake-java/docker/Dockerfile.collector`
- `cryptolake-java/docker/Dockerfile.backfill`
- `cryptolake-java/docker/Dockerfile.consolidation`
- `cryptolake-java/.gitignore`
- `src/collector/tap.py` — Python tap mode for fixture capture
- `tests/unit/collector/test_tap.py`
- `docs/superpowers/port/.gitkeep` (dir anchor; state.json and fixtures are created by `/port-init`)

### Modified

- `src/collector/main.py` — wire tap mode toggle from config
- `src/common/config.py` — add optional `tap_output_dir` config field
- `src/cli/verify.py` — none (used only as reference by gate 5)
- `pyproject.toml` — no change (reuse existing deps)

---

## Phase 1 — Skill directory + rulebook

### Task 1: Create skill directory structure

**Files:**
- Create: `.claude/skills/python-to-java-port/` (plus subdirs `prompts/`, `schemas/`, `scripts/`, `scripts/gates/`, `scripts/lib/`, `tests/`)
- Create: `.claude/skills/python-to-java-port/.gitkeep`

- [ ] **Step 1: Make directories**

```bash
mkdir -p .claude/skills/python-to-java-port/{prompts,schemas,scripts/gates,scripts/lib,tests}
touch .claude/skills/python-to-java-port/.gitkeep
```

- [ ] **Step 2: Verify layout**

Run: `find .claude/skills/python-to-java-port -type d | sort`

Expected output:
```
.claude/skills/python-to-java-port
.claude/skills/python-to-java-port/prompts
.claude/skills/python-to-java-port/schemas
.claude/skills/python-to-java-port/scripts
.claude/skills/python-to-java-port/scripts/gates
.claude/skills/python-to-java-port/scripts/lib
.claude/skills/python-to-java-port/tests
```

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/python-to-java-port/
git commit -m "chore(port): scaffold python-to-java-port skill directory"
```

---

### Task 2: Write SKILL.md with all 4 rule tiers

**Files:**
- Create: `.claude/skills/python-to-java-port/SKILL.md`

- [ ] **Step 1: Write the skill document**

Write file content:

````markdown
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
````

- [ ] **Step 2: Verify file length and frontmatter**

Run:
```bash
head -4 .claude/skills/python-to-java-port/SKILL.md
wc -l .claude/skills/python-to-java-port/SKILL.md
```

Expected: frontmatter lines present, file is ≥ 70 lines.

- [ ] **Step 3: Commit**

```bash
git add .claude/skills/python-to-java-port/SKILL.md
git commit -m "feat(port): add SKILL.md with 4 rule tiers"
```

---

### Task 3: Write artifact schemas

**Files:**
- Create: `.claude/skills/python-to-java-port/schemas/mapping.schema.md`
- Create: `.claude/skills/python-to-java-port/schemas/design.schema.md`
- Create: `.claude/skills/python-to-java-port/schemas/completion.schema.md`

- [ ] **Step 1: Write `mapping.schema.md`**

````markdown
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
````

- [ ] **Step 2: Write `design.schema.md`**

````markdown
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
````

- [ ] **Step 3: Write `completion.schema.md`**

````markdown
# completion.md schema

Produced by: Developer.
Path: `docs/superpowers/port/<module>/completion.md`.

## Frontmatter (required)

```
---
module: <name>
status: complete
produced_by: developer
commits: [list of sha]
branch: port/<module>
---
```

## Required sections (in order)

1. `## 1. Gate results`
2. `## 2. Deviations from design`
3. `## 3. Rule compliance`
4. `## 4. Escalations`
5. `## 5. Known follow-ups`

## Validation rules

- Frontmatter present with all 5 keys; `status: complete` literal.
- All 5 section headings present.
- `## 1.` reports all 7 gates, each with pass/fail state.
- `## 3.` lists every rule from Tiers 1, 2, 3 with `file:line` reference.
- No `TBD`, `TODO`, `???`, `FIXME`.
````

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/python-to-java-port/schemas/
git commit -m "feat(port): add artifact schemas for mapping/design/completion"
```

---

### Task 4: Write Analyst role prompt template

**Files:**
- Create: `.claude/skills/python-to-java-port/prompts/analyst.md`

- [ ] **Step 1: Write the template**

````markdown
# Analyst role — prompt template

You are the **Analyst** for the CryptoLake Python-to-Java port.

## Your job

Produce a complete, unambiguous mapping document describing the Python module `{{module}}` so an Architect can design the Java realization without re-reading the Python source.

You are read-only. You do not propose Java design. You do not edit Python code. You do not read beyond the module unless a dependency trace requires it.

## Inputs

- Module: `{{module}}`
- Python source files (in scope):
{{python_files}}
- Python test files (in scope):
{{python_test_files}}
- Skill rules Tier 1 (invariants) — see below. You must surface where each is touched.

## Allowed tools

- Read, Grep, Glob
- Bash: read-only commands (`wc`, `ls`, `python -c` for behavior probes). No writes.
- Write: only the mapping artifact at the fixed path below.

## Forbidden

- Proposing Java design
- Editing Python code
- Reading unrelated modules (beyond dependency tracing)
- Using the `Agent` tool (no sub-delegation)

## Output artifact (strict)

Write to: `docs/superpowers/port/{{module}}/mapping.md`

Frontmatter (required):
```
---
module: {{module}}
status: complete
produced_by: analyst
python_files: [list]
python_test_files: [list]
---
```

Required sections (in order, exact headings):

1. `## 1. Module summary` — one paragraph; purpose, public entry points, upstream consumers.
2. `## 2. File inventory` — table: file | lines | role (domain / IO / config / glue) | public symbols.
3. `## 3. Public API surface` — each function/class/constant imported from outside the module; signature, docstring, semantics, callers.
4. `## 4. Internal structure` — call graph within the module. Flag god-objects, functions > 80 lines, cyclomatic hotspots.
5. `## 5. Concurrency surface` — every `async def`; every asyncio primitive used (`Queue`, `TaskGroup`, `Lock`, `Event`, `gather`, `shield`, `timeout`); every blocking call; backpressure points.
6. `## 6. External I/O` — Kafka topics (in/out with key/value shape), HTTP endpoints, WebSocket subscriptions (URL + message shapes), filesystem paths, DB queries, env vars / config keys.
7. `## 7. Data contracts` — every dataclass / pydantic model defined or accepted; field name, type, nullable, default. Every JSON schema produced/parsed.
8. `## 8. Test catalog` — each test file → list of tests → invariant/scenario exercised. Mark chaos tests separately. Mark tests requiring Testcontainers.
9. `## 9. Invariants touched (Tier 1 rules)` — for each of rules 1–7 (see below), state `touched in <file>:<line> where <reason>` or `not applicable`.
10. `## 10. Port risks` — explicit surprising things. Example: "orjson uses `sort_keys=False` — Jackson default differs and must be forced."
11. `## 11. Rule compliance` — for each Tier 1 rule: `surfaced in §9` or `not applicable to this module`.

## Tier 1 — Invariants (verbatim, for reference in §9)

1. `raw_text` is captured from the `onBinary`/`onText` WebSocket callback BEFORE any JSON parse.
2. `raw_sha256` is computed over the exact `raw_text` bytes, once, at capture time.
3. Disabled streams emit zero artifacts.
4. Kafka consumer offsets are committed only AFTER file flush returns successfully.
5. Every detected gap emits metric, log, AND archived gap record.
6. Recovery prefers replay from Kafka / exchange cursors over inferred reconstruction.
7. JSON codec must not re-order, re-quote, or re-format `raw_text`.

## Acceptance

The orchestrator will validate your artifact against `.claude/skills/python-to-java-port/schemas/mapping.schema.md`. Missing sections or empty `## 11.` = rejection. You get 1 retry before escalation.

## Attempt context

Attempt number: {{attempt_number}}
Prior failure (if retry): {{failure_details}}
````

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/python-to-java-port/prompts/analyst.md
git commit -m "feat(port): add Analyst role prompt template"
```

---

### Task 5: Write Architect role prompt template

**Files:**
- Create: `.claude/skills/python-to-java-port/prompts/architect.md`

- [ ] **Step 1: Write the template**

````markdown
# Architect role — prompt template

You are the **Architect** for the CryptoLake Python-to-Java port.

## Your job

Design the Java realization of module `{{module}}`. Produce a complete design document the Developer can implement without architectural decisions of their own.

You do not write Java code. You do not modify Python code. You do not change the library choices locked by the spec without explicit user escalation.

## Inputs

- Analyst's mapping: `docs/superpowers/port/{{module}}/mapping.md` (sha: {{mapping_sha}})
- Target architecture reference: `docs/superpowers/specs/2026-04-18-python-to-java-port-design.md` §1
- Skill rule tiers 1, 2, 3 — embedded below. You must honor all.

## Allowed tools

- Read (Python source for cross-check if mapping is ambiguous), Grep, Glob, WebFetch (JDK / Kafka / Jackson API docs)
- Write: only the design artifact at the fixed path below.

## Forbidden

- Writing Java code (class sketches as markdown inside the artifact are fine; compilable code is not)
- Changing library choices from the spec's §1.3 mapping table without explicit user escalation
- Introducing a framework (Spring, Micronaut, Quarkus, CDI, Guice) — see Tier 2 rule 11
- Using the `Agent` tool

## Output artifact (strict)

Write to: `docs/superpowers/port/{{module}}/design.md`

Frontmatter (required):
```
---
module: {{module}}
status: approved
produced_by: architect
based_on_mapping: {{mapping_sha}}
---
```

**Frontmatter `status: approved` is REQUIRED.** If you cannot approve (invariant conflict, missing information, ambiguous mapping), do not produce the document. Instead, output a single message starting with `ESCALATE:` and describe the blocker. The orchestrator will halt and escalate to the user.

Required sections (in order, exact headings):

1. `## 1. Package layout` — Java package tree with class names.
2. `## 2. Class catalog` — per class: purpose, type (record / sealed interface / final class / utility), public API, dependencies (constructor-injected), thread-safety notes.
3. `## 3. Concurrency design` — per class/method: virtual-thread context, StructuredTaskScope boundaries, locking strategy, cancellation/shutdown. Must address every entry in mapping §5.
4. `## 4. Python → Java mapping table` — every symbol from mapping §3 + §4 → target Java class + method. Any symbol intentionally dropped listed with reason.
5. `## 5. Library mapping` — every dep from mapping §6 → Java library + version (from `cryptolake-java/gradle/libs.versions.toml`). Justify any deviation from spec §1.3.
6. `## 6. Data contracts` — every pydantic/dataclass from mapping §7 → Java record. Field-by-field; JSON property name must match Python. ObjectMapper config required.
7. `## 7. Error model` — exception hierarchy; what wraps what; what is retried; what kills the process.
8. `## 8. Test plan` — every test from mapping §8 → JUnit 5 class + method name. Mark tests needing new fixtures or recorded WebSocket frames.
9. `## 9. Metrics plan` — every Prometheus metric → Micrometer `Meter` with identical name + tags. Describe the generated parity test.
10. `## 10. Rule compliance` — per rule (all Tiers 1, 2, 3): `honored by <X>` or `N/A because <Y>`.
11. `## 11. Open questions for developer` — anything you couldn't resolve. Developer may not invent answers; they must escalate.

## Rule tiers (verbatim, for §10)

### Tier 1 — Invariants

1. `raw_text` captured pre-parse from WebSocket callback.
2. `raw_sha256` computed over raw_text bytes once at capture.
3. Disabled streams emit zero artifacts.
4. Kafka offsets committed only after fsync-completed flush.
5. Every gap emits metric + log + archived record.
6. Recovery prefers replay over reconstruction.
7. JSON codec preserves raw_text byte-for-byte.

### Tier 2 — Java practices

8. Java 21 only.
9. No `synchronized` around blocking calls.
10. No `Thread.sleep` in hot paths.
11. No reflection-heavy frameworks.
12. Immutable records, no setters.
13. No checked exception leaks.
14. Single `HttpClient` / `KafkaProducer` / `ObjectMapper` per service.
15. JSON logs via Logback + Logstash + MDC.
16. Fail-fast on invariant violation; retry only declared-transient errors.

### Tier 3 — Parity rules

17. Every Python test has a JUnit 5 counterpart with trace comment.
18. Prometheus metric names + labels diff-match Python.
19. `raw_text` / `raw_sha256` byte-identity gate via fixture corpus.
20. Python `verify` CLI passes on Java archives.
21. Envelope JSON field order follows Python canonical order.

## Acceptance

Orchestrator validates against `schemas/design.schema.md`. Missing sections or `status` != `approved` = rejection.

## Attempt context

Attempt: {{attempt_number}}
Prior failure (if retry): {{failure_details}}
````

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/python-to-java-port/prompts/architect.md
git commit -m "feat(port): add Architect role prompt template"
```

---

### Task 6: Write Developer role prompt template

**Files:**
- Create: `.claude/skills/python-to-java-port/prompts/developer.md`

- [ ] **Step 1: Write the template**

````markdown
# Developer role — prompt template

You are the **Developer** for the CryptoLake Python-to-Java port.

## Your job

Implement module `{{module}}` per the Architect's design. Port tests to JUnit 5. Commit on branch `port/{{module}}`. Run the 7 gates; iterate until green. Write `completion.md`.

## Inputs (verbatim — read and honor)

- Mapping: `docs/superpowers/port/{{module}}/mapping.md`
- Design: `docs/superpowers/port/{{module}}/design.md` (must have `status: approved` — you did not dispatch without this)
- Skill rule tiers 1, 2, 3 — embedded below
- Python source for `src/{{module}}/` — read-only reference (do NOT translate line-by-line; implement per design)

## Allowed tools

- All except `Agent` (no sub-delegation; escalation goes via `completion.md §4`)
- Bash: `./gradlew` commands, git, fixture scripts

## Forbidden

- Changing the design (if something is wrong, write to `completion.md §4 Escalations` and halt; do not improvise)
- Editing Python code
- Using `@SuppressWarnings` or `--no-verify` to pass gates
- Modifying the skill (`.claude/skills/...`) or other modules' artifacts
- Modifying main branch directly; all work on branch `port/{{module}}`

## Workflow

1. Create branch: `git checkout -b port/{{module}}` (from latest `main`).
2. Implement class-by-class per design §2. Commit in logical chunks (one class or one coherent feature per commit).
3. Port tests per design §8. Each port carries a trace comment: `// ports: tests/unit/{{module}}/test_X.py::test_name`.
4. Capture additional parity fixtures if design §8 requires — place under `parity-fixtures/{{module}}/additions/`.
5. Run gates in order. Fix failures. Do NOT mark a gate pass without evidence.
6. Write `completion.md` at `docs/superpowers/port/{{module}}/completion.md`.

## The 7 gates

Run via `.claude/skills/python-to-java-port/scripts/run_gates.sh {{module}}`. Each gate emits pass/fail with output. All must pass.

1. Unit/integration tests: `./gradlew :{{module}}:test`
2. Chaos tests: `./gradlew :{{module}}:test --tests "*chaos*"` (writer + collector only; trivially passes for common + cli)
3. `raw_text` byte-identity: replay `parity-fixtures/websocket-frames/**` and compare
4. Metric parity: diff Java Prometheus exposition vs `parity-fixtures/metrics/<service>.txt`
5. `verify` CLI parity: Python `verify` against Java archives
6. Static checks: `./gradlew :{{module}}:check` (Spotless, Error Prone, NullAway, custom rules)
7. Architect sign-off: orchestrator re-dispatches Architect read-only on your final diff

## Output artifact (strict)

Write to: `docs/superpowers/port/{{module}}/completion.md`

Frontmatter:
```
---
module: {{module}}
status: complete
produced_by: developer
commits: [list of sha]
branch: port/{{module}}
---
```

Required sections:

1. `## 1. Gate results` — each gate: status, command run, output excerpt.
2. `## 2. Deviations from design` — any implementation difference from design §2–§6. Non-empty = orchestrator re-runs Architect review.
3. `## 3. Rule compliance` — per rule (Tiers 1, 2, 3): `file:line` reference showing compliance. Required.
4. `## 4. Escalations` — blockers design didn't resolve. Non-empty = orchestrator halts.
5. `## 5. Known follow-ups` — non-blocking; becomes tasks, not a blocker.

## Rule tiers (verbatim)

### Tier 1 — Invariants
1. raw_text pre-parse capture.
2. raw_sha256 at capture, not downstream.
3. Disabled streams emit nothing.
4. Offset commit only after fsync flush.
5. Gap → metric + log + archived record.
6. Replay over reconstruction.
7. raw_text byte-for-byte.

### Tier 2 — Java practices
8. Java 21 only.
9. No `synchronized` around blocking.
10. No `Thread.sleep` in hot paths.
11. No frameworks.
12. Records, not POJOs.
13. No checked-exception leaks.
14. One HttpClient/KafkaProducer/ObjectMapper per service.
15. JSON logs via Logback + MDC.
16. Fail-fast; retry only transient.

### Tier 3 — Parity rules
17. JUnit 5 counterpart for each Python test + trace comment.
18. Metric parity.
19. raw_text fixture byte-identity.
20. verify CLI parity.
21. Envelope field order.

## Acceptance

Orchestrator validates `completion.md` against `schemas/completion.schema.md`. All 7 gates must be `pass`. §2 Deviations non-empty → Architect review. §4 Escalations non-empty → halt.

Gate 3 (byte-identity) failing 3× → immediate halt. Other gates retry up to 3× before escalation.

## Attempt context

Attempt: {{attempt_number}}
Prior gate failures (if retry): {{gate_failures}}
````

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/python-to-java-port/prompts/developer.md
git commit -m "feat(port): add Developer role prompt template"
```

---

## Phase 2 — State management scripts

### Task 7: Write `scripts/lib/assert.bash` helper

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/lib/assert.bash`

- [ ] **Step 1: Write helpers**

```bash
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
```

- [ ] **Step 2: Commit**

```bash
git add .claude/skills/python-to-java-port/scripts/lib/assert.bash
git commit -m "feat(port): add bats assert helpers"
```

---

### Task 8: Write failing tests for `state.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/tests/state.bats`

- [ ] **Step 1: Write the test file**

```bash
#!/usr/bin/env bats
# tests/state.bats

load '../scripts/lib/assert.bash'

SCRIPT=".claude/skills/python-to-java-port/scripts/state.sh"

setup() {
  TMPDIR="$(mktemp -d)"
  export PORT_STATE_FILE="$TMPDIR/state.json"
}

teardown() {
  rm -rf "$TMPDIR"
}

@test "init creates state.json with 4 pending modules" {
  bash "$SCRIPT" init
  assert_file_exists "$PORT_STATE_FILE"
  assert_json_eq "$PORT_STATE_FILE" '.version' '1'
  assert_json_eq "$PORT_STATE_FILE" '.current_module' 'common'
  assert_json_eq "$PORT_STATE_FILE" '.modules | length' '4'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].name' 'common'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].status' 'in_progress'
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].phase' 'pending'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].name' 'writer'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].status' 'pending'
}

@test "init refuses to overwrite existing state.json" {
  bash "$SCRIPT" init
  run bash "$SCRIPT" init
  [ "$status" -ne 0 ]
  [[ "$output" == *"already exists"* ]]
}

@test "get_current_phase returns pending after init" {
  bash "$SCRIPT" init
  run bash "$SCRIPT" get_current_phase
  [ "$status" -eq 0 ]
  assert_equal "pending" "$output"
}

@test "set_phase updates the current module's phase" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase analyst
  run bash "$SCRIPT" get_current_phase
  assert_equal "analyst" "$output"
}

@test "advance_module rotates current to writer" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase accepted
  bash "$SCRIPT" advance_module
  run bash "$SCRIPT" get_current_module
  assert_equal "writer" "$output"
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].status' 'in_progress'
  assert_json_eq "$PORT_STATE_FILE" '.modules[1].phase' 'pending'
}

@test "advance_module refuses if current phase != accepted" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_phase developer
  run bash "$SCRIPT" advance_module
  [ "$status" -ne 0 ]
  [[ "$output" == *"not accepted"* ]]
}

@test "set_halt writes halt_reason; clear_halt removes it" {
  bash "$SCRIPT" init
  bash "$SCRIPT" set_halt "gate_failed"
  assert_json_eq "$PORT_STATE_FILE" '.halt_reason' 'gate_failed'
  bash "$SCRIPT" clear_halt
  assert_json_eq "$PORT_STATE_FILE" '.halt_reason' 'null'
}

@test "record_gate_result stores pass/fail on current module" {
  bash "$SCRIPT" init
  bash "$SCRIPT" record_gate_result 3 pass
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].gates.raw_text_byte_parity' 'pass'
}

@test "increment_attempts bumps analyst counter" {
  bash "$SCRIPT" init
  bash "$SCRIPT" increment_attempts analyst
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].attempts.analyst' '1'
  bash "$SCRIPT" increment_attempts analyst
  assert_json_eq "$PORT_STATE_FILE" '.modules[0].attempts.analyst' '2'
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
bats .claude/skills/python-to-java-port/tests/state.bats
```

Expected: all 9 tests FAIL (no such file `scripts/state.sh` yet).

- [ ] **Step 3: Commit (red)**

```bash
git add .claude/skills/python-to-java-port/tests/state.bats
git commit -m "test(port): failing tests for state.sh"
```

---

### Task 9: Implement `scripts/state.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/state.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# .claude/skills/python-to-java-port/scripts/state.sh
# State-machine helpers for the port orchestrator.

set -euo pipefail

STATE_FILE="${PORT_STATE_FILE:-docs/superpowers/port/state.json}"

MODULES='["common","writer","collector","cli"]'

_read() {
  [[ -f "$STATE_FILE" ]] || { echo "state file missing: $STATE_FILE" >&2; exit 1; }
  cat "$STATE_FILE"
}

_write() {
  local tmp
  tmp="$(mktemp)"
  cat > "$tmp"
  mv "$tmp" "$STATE_FILE"
}

_jq() {
  _read | jq "$@"
}

_update() {
  local filter="$1"
  local tmp
  tmp="$(mktemp)"
  _read | jq "$filter" > "$tmp"
  mv "$tmp" "$STATE_FILE"
}

cmd_init() {
  if [[ -f "$STATE_FILE" ]]; then
    echo "state already exists: $STATE_FILE" >&2
    exit 1
  fi
  mkdir -p "$(dirname "$STATE_FILE")"
  local now; now="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  jq -n --arg ts "$now" --argjson modules "$MODULES" '
    {
      version: 1,
      started_at: $ts,
      current_module: "common",
      halt_reason: null,
      modules: ($modules | to_entries | map({
        name: .value,
        status: (if .key == 0 then "in_progress" else "pending" end),
        phase: "pending",
        checkpoint0_done: false,
        artifacts: {mapping:null, design:null, completion:null},
        gates: {
          ported_unit_tests:null,
          ported_chaos_tests:null,
          raw_text_byte_parity:null,
          metric_parity:null,
          verify_cli:null,
          static_checks:null,
          architect_signoff:null
        },
        attempts: {analyst:0, architect:0, developer:0},
        escalations: []
      }))
    }
  ' > "$STATE_FILE"
}

cmd_get_current_module() {
  _jq -r '.current_module'
}

cmd_get_current_phase() {
  _jq -r '.modules[] | select(.name == (input_filename | "unused") or true) | .' >/dev/null
  _jq -r '[.modules[] | select(.name == .current_module // .)][0].phase' 2>/dev/null || \
  _jq -r '.modules[] as $m | select($m.name == .current_module) | $m.phase'
  # simpler:
}

# redefine for clarity:
cmd_get_current_phase() {
  _jq -r '.modules[] | select(.name == (..|.current_module? // empty))' >/dev/null 2>&1 || true
  _jq -r '
    .current_module as $cm
    | .modules[] | select(.name == $cm) | .phase
  '
}

cmd_set_phase() {
  local new_phase="$1"
  _update --arg p "$new_phase" '
    (.modules[] | select(.name == .current_module // .) | .phase) = $p
  ' 2>/dev/null || true
  # robust version:
  _update --arg p "$new_phase" '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .phase = $p else . end
      )
  '
}

cmd_advance_module() {
  local cur_phase
  cur_phase="$(cmd_get_current_phase)"
  if [[ "$cur_phase" != "accepted" ]]; then
    echo "current module phase is '$cur_phase', not accepted" >&2
    exit 1
  fi
  _update '
    . as $root
    | (.modules | map(.name) | index($root.current_module)) as $idx
    | if $idx == null or $idx + 1 >= (.modules | length) then
        .current_module = "DONE"
      else
        .current_module = .modules[$idx + 1].name
        | .modules[$idx + 1].status = "in_progress"
        | .modules[$idx + 1].phase = "pending"
      end
  '
}

cmd_set_halt() {
  local reason="$1"
  _update --arg r "$reason" '.halt_reason = $r'
}

cmd_clear_halt() {
  _update '.halt_reason = null'
}

cmd_record_gate_result() {
  local gate_num="$1" result="$2"
  local gate_key
  case "$gate_num" in
    1) gate_key="ported_unit_tests" ;;
    2) gate_key="ported_chaos_tests" ;;
    3) gate_key="raw_text_byte_parity" ;;
    4) gate_key="metric_parity" ;;
    5) gate_key="verify_cli" ;;
    6) gate_key="static_checks" ;;
    7) gate_key="architect_signoff" ;;
    *) echo "bad gate: $gate_num" >&2; exit 1 ;;
  esac
  _update --arg k "$gate_key" --arg v "$result" '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .gates[$k] = $v else . end
      )
  '
}

cmd_increment_attempts() {
  local role="$1"
  _update --arg r "$role" '
    . as $root
    | .modules |= map(
        if .name == $root.current_module then .attempts[$r] += 1 else . end
      )
  '
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    init)                    cmd_init ;;
    get_current_module)      cmd_get_current_module ;;
    get_current_phase)       cmd_get_current_phase ;;
    set_phase)               cmd_set_phase "$@" ;;
    advance_module)          cmd_advance_module ;;
    set_halt)                cmd_set_halt "$@" ;;
    clear_halt)              cmd_clear_halt ;;
    record_gate_result)      cmd_record_gate_result "$@" ;;
    increment_attempts)      cmd_increment_attempts "$@" ;;
    *) echo "usage: state.sh {init|get_current_module|get_current_phase|set_phase|advance_module|set_halt|clear_halt|record_gate_result|increment_attempts}" >&2; exit 2 ;;
  esac
}

main "$@"
```

- [ ] **Step 2: Make executable and run tests**

Run:
```bash
chmod +x .claude/skills/python-to-java-port/scripts/state.sh
bats .claude/skills/python-to-java-port/tests/state.bats
```

Expected: all 9 tests PASS.

- [ ] **Step 3: Commit (green)**

```bash
git add .claude/skills/python-to-java-port/scripts/state.sh
git commit -m "feat(port): implement state.sh (init/get/set/advance/halt/gates/attempts)"
```

---

### Task 10: Write failing tests for `validate_artifact.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/tests/validate_artifact.bats`

- [ ] **Step 1: Write the test**

```bash
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

@test "mapping: TODO in body fails" {
  write_mapping_good "$TMPDIR/mapping.md"
  echo "TODO: finish this" >> "$TMPDIR/mapping.md"
  run bash "$SCRIPT" mapping "$TMPDIR/mapping.md"
  [ "$status" -ne 0 ]
  [[ "$output" == *"TODO"* ]]
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
bats .claude/skills/python-to-java-port/tests/validate_artifact.bats
```

Expected: all 6 FAIL.

- [ ] **Step 3: Commit (red)**

```bash
git add .claude/skills/python-to-java-port/tests/validate_artifact.bats
git commit -m "test(port): failing tests for validate_artifact.sh"
```

---

### Task 11: Implement `scripts/validate_artifact.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/validate_artifact.sh`

- [ ] **Step 1: Write the script**

```bash
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
```

- [ ] **Step 2: Make executable and run tests**

Run:
```bash
chmod +x .claude/skills/python-to-java-port/scripts/validate_artifact.sh
bats .claude/skills/python-to-java-port/tests/validate_artifact.bats
```

Expected: all 6 PASS.

- [ ] **Step 3: Commit (green)**

```bash
git add .claude/skills/python-to-java-port/scripts/validate_artifact.sh
git commit -m "feat(port): implement validate_artifact.sh"
```

---

## Phase 3 — Prompt assembly

### Task 12: Implement `scripts/assemble_prompt.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/assemble_prompt.sh`

- [ ] **Step 1: Write the script**

```bash
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

# Module-scoped file lists.
PY_FILES=""
PY_TEST_FILES=""
case "$MODULE" in
  common|writer|collector)
    PY_FILES="$(find "src/$MODULE" -type f -name '*.py' 2>/dev/null | sort | sed 's/^/- /')"
    PY_TEST_FILES="$(grep -rl "from src\\.$MODULE\\b\\|import src\\.$MODULE\\b" tests 2>/dev/null | sort -u | sed 's/^/- /' || true)"
    ;;
  cli)
    PY_FILES="$(find src/cli -type f -name '*.py' 2>/dev/null | sort | sed 's/^/- /')"
    PY_TEST_FILES="$(grep -rl "from src\\.cli\\b\\|import src\\.cli\\b" tests 2>/dev/null | sort -u | sed 's/^/- /' || true)"
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

awk -v module="$MODULE" \
    -v attempt="$ATTEMPT" \
    -v failure="$FAILURE" \
    -v py_files="$PY_FILES" \
    -v py_test_files="$PY_TEST_FILES" \
    -v mapping_sha="$MAPPING_SHA" \
    -v gate_failures="$GATE_FAILURES" \
'
{
  gsub(/\{\{module\}\}/, module)
  gsub(/\{\{attempt_number\}\}/, attempt)
  gsub(/\{\{failure_details\}\}/, failure)
  gsub(/\{\{python_files\}\}/, py_files)
  gsub(/\{\{python_test_files\}\}/, py_test_files)
  gsub(/\{\{mapping_sha\}\}/, mapping_sha)
  gsub(/\{\{gate_failures\}\}/, gate_failures)
  print
}
' "$TEMPLATE"
```

- [ ] **Step 2: Smoke-test assembly**

Run:
```bash
chmod +x .claude/skills/python-to-java-port/scripts/assemble_prompt.sh
bash .claude/skills/python-to-java-port/scripts/assemble_prompt.sh analyst common 1 none | head -20
```

Expected: first lines contain `# Analyst role`, `Module: common`, and no `{{` placeholders.

- [ ] **Step 3: Assert no unfilled placeholders**

Run:
```bash
bash .claude/skills/python-to-java-port/scripts/assemble_prompt.sh analyst common | grep -c '{{' | xargs -I{} test {} = 0
echo "placeholders clean: $?"
```

Expected: `placeholders clean: 0` (exit 0 = success).

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/python-to-java-port/scripts/assemble_prompt.sh
git commit -m "feat(port): implement assemble_prompt.sh"
```

---

## Phase 4 — Gate scripts

### Task 13: Implement gates 1 and 2 (Java test runners)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate1_unit_tests.sh`
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate2_chaos_tests.sh`

- [ ] **Step 1: Write gate 1**

```bash
#!/usr/bin/env bash
# gate1: unit + integration tests for the module.
set -euo pipefail
MODULE="${1:?module}"
cd cryptolake-java
./gradlew ":${MODULE}:test" --tests '*' --info
```

- [ ] **Step 2: Write gate 2**

```bash
#!/usr/bin/env bash
# gate2: chaos tests. Trivially passes for common + cli (no chaos suite).
set -euo pipefail
MODULE="${1:?module}"
case "$MODULE" in
  common|cli)
    echo "gate2: no chaos suite for $MODULE (pass-by-definition)"
    exit 0
    ;;
esac
cd cryptolake-java
./gradlew ":${MODULE}:test" --tests '*chaos*' --info
```

- [ ] **Step 3: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate1_unit_tests.sh
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate2_chaos_tests.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate1_unit_tests.sh .claude/skills/python-to-java-port/scripts/gates/gate2_chaos_tests.sh
git commit -m "feat(port): add gate1 (unit tests) and gate2 (chaos)"
```

---

### Task 14: Implement gate 3 (raw_text byte-identity)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate3_raw_text_parity.sh`

- [ ] **Step 1: Write the gate**

```bash
#!/usr/bin/env bash
# gate3: replay recorded WebSocket frames through the Java capture path,
#        compare raw_text bytes + raw_sha256 against Python-produced envelopes.
# Pass-by-definition for modules without a capture path.
set -euo pipefail
MODULE="${1:?module}"

case "$MODULE" in
  common|cli)
    echo "gate3: $MODULE has no capture path (pass-by-definition)"
    exit 0
    ;;
esac

FIXTURE_ROOT="cryptolake-java/parity-fixtures/websocket-frames"
if [[ ! -d "$FIXTURE_ROOT" ]]; then
  echo "gate3 FAIL: fixture directory missing: $FIXTURE_ROOT" >&2
  exit 1
fi

cd cryptolake-java
# Java harness class lives in the module's test sourceSet.
# Harness is named RawTextParityHarness. It reads FIXTURE_ROOT, runs frames
# through the module's capture, prints "OK <n> frames" or "FAIL ..." with
# per-frame diffs. Non-zero exit on any diff.
./gradlew ":${MODULE}:runRawTextParity" --info
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate3_raw_text_parity.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate3_raw_text_parity.sh
git commit -m "feat(port): add gate3 (raw_text byte-identity)"
```

---

### Task 15: Implement gate 4 (metric parity)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate4_metric_parity.sh`

- [ ] **Step 1: Write the gate**

```bash
#!/usr/bin/env bash
# gate4: diff Java Prometheus metric name+label set vs Python reference.
set -euo pipefail
MODULE="${1:?module}"

case "$MODULE" in
  common|cli)
    echo "gate4: $MODULE has no metric exposition (pass-by-definition)"
    exit 0
    ;;
esac

SERVICE="$MODULE"
REF="cryptolake-java/parity-fixtures/metrics/${SERVICE}.txt"
if [[ ! -f "$REF" ]]; then
  echo "gate4 FAIL: reference metrics missing: $REF" >&2
  exit 1
fi

cd cryptolake-java
# Gradle task :<module>:dumpMetricSkeleton starts the service in a test JVM,
# scrapes /metrics, writes to build/metrics-skeleton.txt (canonicalized:
# metric_name{sorted,label,keys} with VALUE stripped).
./gradlew ":${SERVICE}:dumpMetricSkeleton" --info

if ! diff -u "$REF" "${SERVICE}/build/metrics-skeleton.txt"; then
  echo "gate4 FAIL: metric skeleton differs from Python reference" >&2
  exit 1
fi
echo "gate4 OK: metric skeleton matches reference"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate4_metric_parity.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate4_metric_parity.sh
git commit -m "feat(port): add gate4 (metric parity)"
```

---

### Task 16: Implement gate 5 (verify CLI parity)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate5_verify_cli.sh`

- [ ] **Step 1: Write the gate**

```bash
#!/usr/bin/env bash
# gate5: run Python verify CLI against Java-produced archives.
set -euo pipefail
MODULE="${1:?module}"

# gate5 only meaningful for writer (produces archives) and cli (the verify port).
case "$MODULE" in
  common|collector)
    echo "gate5: $MODULE does not produce archives (pass-by-definition)"
    exit 0
    ;;
esac

EXPECTED="cryptolake-java/parity-fixtures/verify/expected.txt"
[[ -f "$EXPECTED" ]] || { echo "gate5 FAIL: expected output missing: $EXPECTED" >&2; exit 1; }

# Run Java integration harness that produces archives to a known path.
cd cryptolake-java
./gradlew ":${MODULE}:produceSyntheticArchives" --info
ARCHIVE_DIR="${MODULE}/build/synthetic-archives"
[[ -d "../$ARCHIVE_DIR" ]] || { echo "gate5 FAIL: no archives produced" >&2; exit 1; }
cd ..

# Now run Python verify against Java archives.
ACTUAL="$(uv run python -m src.cli.verify --archive-dir "cryptolake-java/$ARCHIVE_DIR" 2>&1 || true)"
EXP="$(cat "$EXPECTED")"

# Structural match: exit code + summary line shape. Numeric values will differ.
# We compare the "PASS"/"FAIL" word + error count == 0.
if [[ "$ACTUAL" != *"PASS"* || "$ACTUAL" == *"errors: "*[^0]* ]]; then
  echo "gate5 FAIL: verify output not PASS or errors>0" >&2
  echo "actual: $ACTUAL" >&2
  exit 1
fi
echo "gate5 OK: Python verify passes on Java archives"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate5_verify_cli.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate5_verify_cli.sh
git commit -m "feat(port): add gate5 (verify CLI parity)"
```

---

### Task 17: Implement gate 6 (static checks)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate6_static_checks.sh`

- [ ] **Step 1: Write the gate**

```bash
#!/usr/bin/env bash
# gate6: Spotless + Error Prone + NullAway + custom rule checks.
set -euo pipefail
MODULE="${1:?module}"
cd cryptolake-java
./gradlew ":${MODULE}:check" --info

# Custom rule: no `synchronized (` near blocking I/O patterns in this module.
BAD_SYNC=$(grep -RnE 'synchronized *\(' "${MODULE}/src/main/java" || true)
if [[ -n "$BAD_SYNC" ]]; then
  echo "gate6 FAIL: synchronized blocks found (virtual-thread pinning risk):" >&2
  echo "$BAD_SYNC" >&2
  exit 1
fi

# Custom rule: no Thread.sleep in main sources.
BAD_SLEEP=$(grep -RnE 'Thread\.sleep *\(' "${MODULE}/src/main/java" || true)
if [[ -n "$BAD_SLEEP" ]]; then
  echo "gate6 FAIL: Thread.sleep found in main sources:" >&2
  echo "$BAD_SLEEP" >&2
  exit 1
fi

echo "gate6 OK: static checks pass for $MODULE"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate6_static_checks.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate6_static_checks.sh
git commit -m "feat(port): add gate6 (static checks)"
```

---

### Task 18: Implement gate 7 (architect sign-off stub)

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/gates/gate7_architect_signoff.sh`

- [ ] **Step 1: Write the stub**

```bash
#!/usr/bin/env bash
# gate7: architect sign-off. This script does NOT dispatch the Architect
# (that happens in the /port-module slash command's Claude flow).
# It checks whether a signoff file exists and is "approved".
set -euo pipefail
MODULE="${1:?module}"
SIGNOFF="docs/superpowers/port/${MODULE}/architect-signoff.txt"

if [[ ! -f "$SIGNOFF" ]]; then
  echo "gate7 PENDING: no signoff file at $SIGNOFF — orchestrator must dispatch Architect"
  exit 2  # 2 = pending, not a failure
fi

status=$(head -n1 "$SIGNOFF" | tr -d '\r\n ')
if [[ "$status" == "approved" ]]; then
  echo "gate7 OK: architect signoff approved"
  exit 0
fi
echo "gate7 FAIL: architect signoff status='$status' (not approved)"
exit 1
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/gates/gate7_architect_signoff.sh
git add .claude/skills/python-to-java-port/scripts/gates/gate7_architect_signoff.sh
git commit -m "feat(port): add gate7 (architect signoff check)"
```

---

### Task 19: Implement `scripts/run_gates.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/run_gates.sh`

- [ ] **Step 1: Write the orchestrator**

```bash
#!/usr/bin/env bash
# run_gates.sh — runs gates 1–7 sequentially for a module.
# Records each result via state.sh. Exit code: 0 all pass, 1 any fail, 2 gate7 pending.

set -uo pipefail

MODULE="${1:?module}"
STATE_SH=".claude/skills/python-to-java-port/scripts/state.sh"
GATE_DIR=".claude/skills/python-to-java-port/scripts/gates"
FAILURE_LOG="docs/superpowers/port/${MODULE}/.last-gate-failure.txt"
mkdir -p "$(dirname "$FAILURE_LOG")"
: > "$FAILURE_LOG"

run_gate() {
  local n="$1" name="$2" script="$3"
  echo "=== gate $n: $name ==="
  if output=$(bash "$script" "$MODULE" 2>&1); then
    echo "$output"
    bash "$STATE_SH" record_gate_result "$n" pass
    return 0
  fi
  local rc=$?
  echo "$output"
  if [[ $rc -eq 2 && "$n" == "7" ]]; then
    bash "$STATE_SH" record_gate_result 7 pending
    return 2
  fi
  echo "=== gate $n FAILED ===" >> "$FAILURE_LOG"
  echo "$output" >> "$FAILURE_LOG"
  bash "$STATE_SH" record_gate_result "$n" fail
  return 1
}

FINAL=0
run_gate 1 "unit_tests"         "$GATE_DIR/gate1_unit_tests.sh"         || FINAL=1
run_gate 2 "chaos_tests"        "$GATE_DIR/gate2_chaos_tests.sh"        || FINAL=1
run_gate 3 "raw_text_parity"    "$GATE_DIR/gate3_raw_text_parity.sh"    || FINAL=1
run_gate 4 "metric_parity"      "$GATE_DIR/gate4_metric_parity.sh"      || FINAL=1
run_gate 5 "verify_cli"         "$GATE_DIR/gate5_verify_cli.sh"         || FINAL=1
run_gate 6 "static_checks"      "$GATE_DIR/gate6_static_checks.sh"      || FINAL=1

# gate 7 may be "pending" (no signoff yet) — that's not a hard failure for this run.
gate7_rc=0
run_gate 7 "architect_signoff"  "$GATE_DIR/gate7_architect_signoff.sh"  || gate7_rc=$?
if [[ $gate7_rc -eq 1 ]]; then FINAL=1; fi

if [[ $FINAL -eq 0 && $gate7_rc -eq 0 ]]; then
  echo "ALL GATES PASS"
  exit 0
fi
if [[ $FINAL -eq 0 && $gate7_rc -eq 2 ]]; then
  echo "GATES 1-6 PASS; gate 7 PENDING architect dispatch"
  exit 2
fi
echo "GATES FAILED; see $FAILURE_LOG"
exit 1
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/run_gates.sh
git add .claude/skills/python-to-java-port/scripts/run_gates.sh
git commit -m "feat(port): add run_gates.sh orchestrator"
```

---

## Phase 5 — Python tap mode (fixture capture)

### Task 20: Write failing test for `src/collector/tap.py`

**Files:**
- Create: `tests/unit/collector/test_tap.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/collector/test_tap.py
import json
from pathlib import Path

import pytest

from src.collector.tap import FrameTap


def test_tap_writes_raw_bytes_and_envelope(tmp_path: Path):
    tap = FrameTap(output_dir=tmp_path, stream="depth")
    raw = b'{"e":"depthUpdate","s":"BTCUSDT"}'
    envelope = {
        "v": 1,
        "type": "depth",
        "exchange": "binance",
        "symbol": "btcusdt",
        "stream": "depth",
        "received_at": "2026-04-18T00:00:00Z",
        "raw_text": raw.decode(),
        "raw_sha256": "deadbeef",
    }

    tap.write(raw, envelope)

    files = sorted(tmp_path.iterdir())
    assert len(files) == 2
    raw_file = next(f for f in files if f.suffix == ".raw")
    env_file = next(f for f in files if f.suffix == ".json")

    assert raw_file.read_bytes() == raw
    loaded = json.loads(env_file.read_text())
    assert loaded == envelope


def test_tap_filenames_are_time_ordered(tmp_path: Path):
    tap = FrameTap(output_dir=tmp_path, stream="trades")
    tap.write(b"a", {"n": 1})
    tap.write(b"b", {"n": 2})
    raws = sorted(p.name for p in tmp_path.glob("*.raw"))
    assert raws[0] < raws[1]


def test_tap_disabled_when_output_dir_none():
    tap = FrameTap(output_dir=None, stream="depth")
    # Must not raise; must be a no-op.
    tap.write(b"x", {"k": "v"})
```

- [ ] **Step 2: Run tests to verify failure**

Run:
```bash
uv run pytest tests/unit/collector/test_tap.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` for `src.collector.tap`.

- [ ] **Step 3: Commit (red)**

```bash
git add tests/unit/collector/test_tap.py
git commit -m "test(collector): failing tests for tap mode"
```

---

### Task 21: Implement `src/collector/tap.py`

**Files:**
- Create: `src/collector/tap.py`

- [ ] **Step 1: Write the module**

```python
# src/collector/tap.py
"""Fixture-capture tap.

Writes each inbound WebSocket frame verbatim to disk plus its resulting
envelope. Used by /port-init to build the parity fixture corpus.

No-op when output_dir is None.
"""
from __future__ import annotations

import itertools
import json
import time
from pathlib import Path
from typing import Mapping


class FrameTap:
    """Persists raw frames + envelopes for later Java parity checks."""

    def __init__(self, output_dir: Path | None, stream: str) -> None:
        self._output_dir = output_dir
        self._stream = stream
        self._counter = itertools.count()
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)

    def write(self, raw: bytes, envelope: Mapping[str, object]) -> None:
        if self._output_dir is None:
            return
        seq = next(self._counter)
        stem = f"{time.time_ns():020d}-{seq:06d}-{self._stream}"
        raw_path = self._output_dir / f"{stem}.raw"
        env_path = self._output_dir / f"{stem}.json"
        raw_path.write_bytes(raw)
        env_path.write_text(json.dumps(dict(envelope)))
```

- [ ] **Step 2: Run tests**

Run:
```bash
uv run pytest tests/unit/collector/test_tap.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 3: Commit (green)**

```bash
git add src/collector/tap.py
git commit -m "feat(collector): add FrameTap for fixture capture"
```

---

### Task 22: Wire tap into collector main + config

**Files:**
- Modify: `src/common/config.py` — add `tap_output_dir: Path | None = None` to the collector config section
- Modify: `src/collector/main.py` — construct a `FrameTap` per stream when `tap_output_dir` is set; pass it into the producer path (or wherever envelopes are emitted) and call `.write(raw, envelope)`

- [ ] **Step 1: Read current config shape**

Run:
```bash
grep -n "class " src/common/config.py | head -20
grep -n "tap\|fixture" src/common/config.py
```

- [ ] **Step 2: Add `tap_output_dir` to config**

Edit `src/common/config.py`: add `tap_output_dir: Path | None = None` to the collector's Pydantic settings class alongside other collector fields.

Example addition (position it next to existing collector fields):

```python
from pathlib import Path

class CollectorSettings(BaseModel):
    # ... existing fields ...
    tap_output_dir: Path | None = None  # fixture-capture output root (None disables)
```

- [ ] **Step 3: Wire FrameTap into `src/collector/main.py`**

Edit `src/collector/main.py`: after config load and before stream startup, for each stream `s`, if `config.collector.tap_output_dir` is set, construct:

```python
from src.collector.tap import FrameTap
tap = FrameTap(
    output_dir=config.collector.tap_output_dir / s.stream_name if config.collector.tap_output_dir else None,
    stream=s.stream_name,
)
```

Pass `tap` into the producer. In the producer's envelope-emit call site, add:

```python
tap.write(raw_bytes, envelope_dict)
```

where `raw_bytes` is the bytes received from `websockets.recv()` before parse, and `envelope_dict` is the dict that gets serialized to Kafka.

- [ ] **Step 4: Run collector unit tests**

Run:
```bash
uv run pytest tests/unit/collector -v
```

Expected: all existing tests PASS (tap is a no-op when unset).

- [ ] **Step 5: Commit**

```bash
git add src/common/config.py src/collector/main.py src/collector/producer.py
git commit -m "feat(collector): wire FrameTap into main and producer"
```

---

### Task 23: Write `capture_fixtures.sh`

**Files:**
- Create: `.claude/skills/python-to-java-port/scripts/capture_fixtures.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# capture_fixtures.sh
# Runs the Python collector in tap mode for a bounded window,
# then captures the Prometheus metric skeleton and verify CLI expected output.
# Outputs to cryptolake-java/parity-fixtures/{websocket-frames,metrics,verify}.

set -euo pipefail

WINDOW_SECS="${WINDOW_SECS:-600}"            # default 10 minutes
OUT_ROOT="cryptolake-java/parity-fixtures"
FRAMES_DIR="$OUT_ROOT/websocket-frames"
METRICS_DIR="$OUT_ROOT/metrics"
VERIFY_DIR="$OUT_ROOT/verify"
mkdir -p "$FRAMES_DIR" "$METRICS_DIR" "$VERIFY_DIR"

# 1. Start Python collector with tap mode.
export COLLECTOR__TAP_OUTPUT_DIR="$FRAMES_DIR"
echo "Starting Python collector in tap mode for ${WINDOW_SECS}s ..."
uv run python -m src.collector.main &
COLLECTOR_PID=$!
trap 'kill $COLLECTOR_PID 2>/dev/null || true' EXIT

# 2. Sleep a moment for streams to connect, then start metrics scrape.
sleep 15
METRICS_RAW="$(mktemp)"
for svc in collector writer; do
  # Scrape each service's /metrics endpoint; endpoint port derived from config.
  # Adjust PORT values to match this project's health/metric server config.
  case "$svc" in
    collector) PORT=8091 ;;
    writer)    PORT=8092 ;;
  esac
  echo "Scraping $svc /metrics on :$PORT for 120s ..."
  end=$(( $(date +%s) + 120 ))
  : > "$METRICS_RAW"
  while (( $(date +%s) < end )); do
    curl -sf "http://localhost:${PORT}/metrics" >> "$METRICS_RAW" || true
    sleep 5
  done
  # Canonicalize: metric name + sorted label keys only (values dropped).
  awk '
    /^[a-zA-Z_][a-zA-Z0-9_]*(\{|$)/ {
      # strip values, keep "name{k1,k2,k3}" with sorted keys
      line=$0
      if (match(line, /^[a-zA-Z_][a-zA-Z0-9_]*/)) {
        name=substr(line, 1, RLENGTH)
      }
      keys=""
      if (match(line, /\{[^}]*\}/)) {
        inner=substr(line, RSTART+1, RLENGTH-2)
        n=split(inner, parts, ",")
        for (i=1;i<=n;i++) { split(parts[i], kv, "="); ks[i]=kv[1] }
        # sort
        asort(ks)
        for (i=1;i<=n;i++) keys = keys (i>1?",":"") ks[i]
        print name "{" keys "}"
      } else {
        print name
      }
    }
  ' "$METRICS_RAW" | sort -u > "$METRICS_DIR/$svc.txt"
done

# 3. Let collector finish the recording window.
echo "Waiting for tap window to complete ..."
wait_until=$(( $(date +%s) + WINDOW_SECS - 135 ))
while (( $(date +%s) < wait_until )); do sleep 30; done

# 4. Stop collector and let writer flush.
kill "$COLLECTOR_PID"
trap - EXIT
wait "$COLLECTOR_PID" 2>/dev/null || true
sleep 20

# 5. Snapshot verify CLI output on archives produced during the recording.
TODAY=$(date -u +"%Y-%m-%d")
uv run python -m src.cli.verify --date "$TODAY" > "$VERIFY_DIR/expected.txt" || true

echo "=== capture_fixtures.sh summary ==="
echo "frames:   $(find "$FRAMES_DIR" -type f -name '*.raw' | wc -l)"
echo "metrics:  $(wc -l < "$METRICS_DIR/collector.txt") collector + $(wc -l < "$METRICS_DIR/writer.txt") writer"
echo "verify:   $VERIFY_DIR/expected.txt ($(wc -l < "$VERIFY_DIR/expected.txt") lines)"
```

- [ ] **Step 2: Make executable and commit**

```bash
chmod +x .claude/skills/python-to-java-port/scripts/capture_fixtures.sh
git add .claude/skills/python-to-java-port/scripts/capture_fixtures.sh
git commit -m "feat(port): add capture_fixtures.sh (tap recording + metric + verify snapshots)"
```

---

## Phase 6 — Gradle scaffold

### Task 24: Create Gradle root files

**Files:**
- Create: `cryptolake-java/settings.gradle.kts`
- Create: `cryptolake-java/build.gradle.kts`
- Create: `cryptolake-java/gradle/libs.versions.toml`
- Create: `cryptolake-java/.gitignore`

- [ ] **Step 1: Write `settings.gradle.kts`**

```kotlin
// cryptolake-java/settings.gradle.kts
rootProject.name = "cryptolake-java"

include(
    "common",
    "writer",
    "collector",
    "backfill",
    "consolidation",
    "verify",
)
```

- [ ] **Step 2: Write `gradle/libs.versions.toml`**

```toml
# cryptolake-java/gradle/libs.versions.toml
[versions]
jackson = "2.17.2"
kafka = "3.8.0"
zstdJni = "1.5.6-4"
micrometer = "1.13.4"
logback = "1.5.8"
logstashEncoder = "7.4"
slf4j = "2.0.13"
postgresql = "42.7.4"
hikari = "5.1.0"
picocli = "4.7.6"
hibernateValidator = "8.0.1.Final"
junit = "5.11.0"
assertj = "3.26.3"
testcontainers = "1.20.1"
errorProne = "2.31.0"
nullaway = "0.11.1"
spotless = "7.0.0.BETA2"

[libraries]
jackson-databind     = { module = "com.fasterxml.jackson.core:jackson-databind",               version.ref = "jackson" }
jackson-yaml         = { module = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml",  version.ref = "jackson" }
kafka-clients        = { module = "org.apache.kafka:kafka-clients",                             version.ref = "kafka"   }
zstd-jni             = { module = "com.github.luben:zstd-jni",                                  version.ref = "zstdJni" }
micrometer-prometheus= { module = "io.micrometer:micrometer-registry-prometheus",               version.ref = "micrometer" }
logback-classic      = { module = "ch.qos.logback:logback-classic",                             version.ref = "logback" }
logstash-encoder     = { module = "net.logstash.logback:logstash-logback-encoder",              version.ref = "logstashEncoder" }
slf4j-api            = { module = "org.slf4j:slf4j-api",                                        version.ref = "slf4j"   }
postgresql           = { module = "org.postgresql:postgresql",                                  version.ref = "postgresql" }
hikaricp             = { module = "com.zaxxer:HikariCP",                                        version.ref = "hikari"  }
picocli              = { module = "info.picocli:picocli",                                       version.ref = "picocli" }
hibernate-validator  = { module = "org.hibernate.validator:hibernate-validator",                version.ref = "hibernateValidator" }
junit-bom            = { module = "org.junit:junit-bom",                                        version.ref = "junit"   }
junit-jupiter        = { module = "org.junit.jupiter:junit-jupiter" }
assertj              = { module = "org.assertj:assertj-core",                                   version.ref = "assertj" }
testcontainers-bom   = { module = "org.testcontainers:testcontainers-bom",                      version.ref = "testcontainers" }
testcontainers-kafka = { module = "org.testcontainers:kafka" }
testcontainers-postgres = { module = "org.testcontainers:postgresql" }

[plugins]
spotless  = { id = "com.diffplug.spotless",                   version.ref = "spotless" }
errorProne= { id = "net.ltgt.errorprone",                     version = "4.0.1" }
nullawayPlug = { id = "net.ltgt.nullaway",                    version = "2.0.0" }
```

- [ ] **Step 3: Write root `build.gradle.kts`**

```kotlin
// cryptolake-java/build.gradle.kts
plugins {
    alias(libs.plugins.spotless) apply false
}

allprojects {
    group = "com.cryptolake"
    version = "0.1.0"
    repositories { mavenCentral() }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    dependencies {
        val testImpl = configurations.getByName("testImplementation")
        testImpl(platform(libs.junit.bom))
        testImpl(libs.junit.jupiter)
        testImpl(libs.assertj)
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }

    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat("1.23.0")
            removeUnusedImports()
            endWithNewline()
        }
    }
}
```

- [ ] **Step 4: Write `.gitignore`**

```
# cryptolake-java/.gitignore
.gradle/
build/
*.class
.idea/
*.iml
parity-fixtures/websocket-frames/
parity-fixtures/verify/expected.txt
!parity-fixtures/metrics/
# (metrics snapshots are small and worth tracking; frames + verify output are large)
```

- [ ] **Step 5: Commit**

```bash
git add cryptolake-java/settings.gradle.kts cryptolake-java/build.gradle.kts cryptolake-java/gradle/libs.versions.toml cryptolake-java/.gitignore
git commit -m "feat(port): Gradle root scaffold with version catalog"
```

---

### Task 25: Create subproject build files

**Files:**
- Create: `cryptolake-java/common/build.gradle.kts`
- Create: `cryptolake-java/writer/build.gradle.kts`
- Create: `cryptolake-java/collector/build.gradle.kts`
- Create: `cryptolake-java/backfill/build.gradle.kts`
- Create: `cryptolake-java/consolidation/build.gradle.kts`
- Create: `cryptolake-java/verify/build.gradle.kts`

- [ ] **Step 1: Write `common/build.gradle.kts`**

```kotlin
// cryptolake-java/common/build.gradle.kts
plugins {
    `java-library`
}

dependencies {
    api(libs.slf4j.api)
    api(libs.jackson.databind)
    api(libs.jackson.yaml)
    api(libs.micrometer.prometheus)
    implementation(libs.logback.classic)
    implementation(libs.logstash.encoder)
}
```

- [ ] **Step 2: Write `writer/build.gradle.kts`**

```kotlin
// cryptolake-java/writer/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.writer.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.zstd.jni)
    implementation(libs.postgresql)
    implementation(libs.hikaricp)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.postgres)
}

tasks.register("dumpMetricSkeleton") {
    group = "port"
    description = "Start service in dump mode, scrape /metrics, write canonicalized skeleton."
    doLast {
        println("TODO: implement after Main exists; placeholder for gate4")
    }
}

tasks.register("produceSyntheticArchives") {
    group = "port"
    description = "Run integration harness that produces archives for gate5."
    doLast {
        println("TODO: implement after Main exists; placeholder for gate5")
    }
}
```

- [ ] **Step 3: Write `collector/build.gradle.kts`**

```kotlin
// cryptolake-java/collector/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.collector.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
}

tasks.register("dumpMetricSkeleton") {
    group = "port"
    doLast {
        println("TODO: implement after Main exists; placeholder for gate4")
    }
}

tasks.register("runRawTextParity") {
    group = "port"
    description = "Replay fixtures through capture path and compare raw_text/raw_sha256."
    doLast {
        println("TODO: implement after capture path exists; placeholder for gate3")
    }
}
```

- [ ] **Step 4: Write `backfill/build.gradle.kts`, `consolidation/build.gradle.kts`, `verify/build.gradle.kts`**

Each is a minimal CLI build:

```kotlin
// cryptolake-java/backfill/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.backfill.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
```

```kotlin
// cryptolake-java/consolidation/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.consolidation.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
```

```kotlin
// cryptolake-java/verify/build.gradle.kts
plugins {
    application
}

application {
    mainClass.set("com.cryptolake.verify.Main")
}

dependencies {
    implementation(project(":common"))
    implementation(libs.picocli)
}
```

- [ ] **Step 5: Create minimal `src/main/java` directories with `.gitkeep` per module**

```bash
for m in common writer collector backfill consolidation verify; do
  mkdir -p "cryptolake-java/$m/src/main/java" "cryptolake-java/$m/src/test/java"
  touch "cryptolake-java/$m/src/main/java/.gitkeep" "cryptolake-java/$m/src/test/java/.gitkeep"
done
```

- [ ] **Step 6: Commit**

```bash
git add cryptolake-java/{common,writer,collector,backfill,consolidation,verify}
git commit -m "feat(port): subproject Gradle builds + source skeletons"
```

---

### Task 26: Create Gradle wrapper

**Files:**
- Create: `cryptolake-java/gradlew`, `cryptolake-java/gradlew.bat`, `cryptolake-java/gradle/wrapper/gradle-wrapper.jar`, `cryptolake-java/gradle/wrapper/gradle-wrapper.properties`

- [ ] **Step 1: Generate wrapper**

Run:
```bash
cd cryptolake-java
gradle wrapper --gradle-version 8.10 --distribution-type bin
cd ..
```

If `gradle` is not installed globally:
```bash
brew install gradle
```

- [ ] **Step 2: Smoke-test**

Run:
```bash
cd cryptolake-java
./gradlew projects
cd ..
```

Expected: lists 6 subprojects (`common`, `writer`, `collector`, `backfill`, `consolidation`, `verify`).

- [ ] **Step 3: Commit wrapper**

```bash
git add cryptolake-java/gradlew cryptolake-java/gradlew.bat cryptolake-java/gradle/wrapper/
git commit -m "feat(port): add Gradle wrapper 8.10"
```

---

### Task 27: Add parity-fixtures directory structure

**Files:**
- Create: `cryptolake-java/parity-fixtures/websocket-frames/.gitkeep`
- Create: `cryptolake-java/parity-fixtures/metrics/.gitkeep`
- Create: `cryptolake-java/parity-fixtures/verify/.gitkeep`
- Create: `cryptolake-java/parity-fixtures/README.md`

- [ ] **Step 1: Make dirs**

```bash
mkdir -p cryptolake-java/parity-fixtures/{websocket-frames,metrics,verify}
touch cryptolake-java/parity-fixtures/websocket-frames/.gitkeep
touch cryptolake-java/parity-fixtures/metrics/.gitkeep
touch cryptolake-java/parity-fixtures/verify/.gitkeep
```

- [ ] **Step 2: Write `parity-fixtures/README.md`**

```markdown
# parity-fixtures

Ground-truth artifacts used by port gates 3, 4, 5.

- `websocket-frames/<stream>/*.raw` — verbatim inbound WebSocket frame bytes
- `websocket-frames/<stream>/*.json` — Python-produced envelope for each frame
- `metrics/<service>.txt` — canonicalized Prometheus metric name+label set
- `verify/expected.txt` — expected stdout of Python `cryptolake verify`

Populated by `/port-init` running `scripts/capture_fixtures.sh`. Do not edit by hand after initial capture — see spec §5.5 for immutability rules.

`websocket-frames/` and `verify/expected.txt` are gitignored (large, per-environment). `metrics/` snapshots are small and worth tracking.
```

- [ ] **Step 3: Commit**

```bash
git add cryptolake-java/parity-fixtures/
git commit -m "docs(port): parity-fixtures directory and README"
```

---

### Task 28: Create Dockerfile templates

**Files:**
- Create: `cryptolake-java/docker/Dockerfile.writer`
- Create: `cryptolake-java/docker/Dockerfile.collector`
- Create: `cryptolake-java/docker/Dockerfile.backfill`
- Create: `cryptolake-java/docker/Dockerfile.consolidation`

- [ ] **Step 1: Write `Dockerfile.writer` (model for the others)**

```dockerfile
# cryptolake-java/docker/Dockerfile.writer
# syntax=docker/dockerfile:1.7

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY . /src/
RUN ./gradlew :writer:installDist --no-daemon

FROM eclipse-temurin:21-jre
ENV JAVA_OPTS="-XX:+UseZGC -XX:+ZGenerational"
WORKDIR /app
COPY --from=build /src/writer/build/install/writer /app/
EXPOSE 8092
ENTRYPOINT ["/app/bin/writer"]
```

- [ ] **Step 2: Duplicate pattern for collector, backfill, consolidation**

`Dockerfile.collector`: replace `writer` with `collector`, `8092` with `8091`.
`Dockerfile.backfill`: replace `writer` with `backfill`, remove `EXPOSE`.
`Dockerfile.consolidation`: replace `writer` with `consolidation`, remove `EXPOSE`.

- [ ] **Step 3: Commit**

```bash
git add cryptolake-java/docker/
git commit -m "feat(port): Dockerfile templates for writer/collector/backfill/consolidation"
```

---

## Phase 7 — Slash commands

### Task 29: Write `/port-init` slash command

**Files:**
- Create: `.claude/commands/port-init.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: One-time setup for the Python-to-Java port. Scaffolds Gradle project, captures parity fixtures, writes state.json.
---

# /port-init

This is the one-time bootstrap for the port. Run it once per project.

## Preconditions (Claude, verify before proceeding)

1. Working tree is clean: `git status --porcelain` returns empty.
2. `docs/superpowers/port/state.json` does NOT exist.
3. `cryptolake-java/` directory exists (already committed from this plan).
4. The Python collector is runnable (`uv run python -m src.collector.main --help`).
5. `jq`, Java 21, and `gradle` wrapper are available.

If any precondition fails, STOP and report the failure to the user. Do not attempt to recover.

## Steps

1. **Initialize state.json.** Run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/state.sh init
   ```
   Expected: creates `docs/superpowers/port/state.json` with 4 modules, `current_module: "common"`, all phases `pending`.

2. **Verify Gradle project builds.** Run:
   ```bash
   cd cryptolake-java && ./gradlew projects --no-daemon && cd ..
   ```
   Expected: lists 6 subprojects. If this fails, halt and escalate.

3. **Capture parity fixtures.** Warn the user this will take ~10 minutes plus a 2-minute metrics scrape, requires live Binance connection, and will produce ~50–200 MB of fixture data.
   Ask the user to confirm before proceeding.
   On confirmation, run:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/capture_fixtures.sh
   ```
   Expected summary output: frame count, metric line count, verify output line count.

4. **Commit the state + fixtures.** Run:
   ```bash
   git add docs/superpowers/port/state.json cryptolake-java/parity-fixtures/metrics/
   git commit -m "chore(port): init state and capture parity fixtures"
   ```
   Note: frames + verify expected output are gitignored; only the small metrics snapshots are committed.

5. **Report summary.** Print:
   - state.json location
   - fixture corpus summary (frames, metrics lines)
   - next user action: "Run /port-module to begin porting `common`."

## Failure handling

If any step fails after preconditions passed, run:
```bash
bash .claude/skills/python-to-java-port/scripts/state.sh set_halt "port_init_failed: <reason>"
```
Then report the failure and halt. Do not retry automatically — the user decides next step.
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-init.md
git commit -m "feat(port): /port-init slash command"
```

---

### Task 30: Write `/port-module` slash command

**Files:**
- Create: `.claude/commands/port-module.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: Advance the current module by one phase (pending → analyst → architect → developer → gates → complete). Idempotent.
---

# /port-module

Drives the current module forward by exactly one phase.

## Preconditions (Claude, verify)

1. `docs/superpowers/port/state.json` exists.
2. `.halt_reason` is null (if non-null, tell the user and suggest `/port-retry` or `/port-rollback`).
3. Working tree is clean outside `docs/superpowers/port/` and `cryptolake-java/`.

## Read state

```bash
STATE_SH=.claude/skills/python-to-java-port/scripts/state.sh
MODULE=$(bash $STATE_SH get_current_module)
PHASE=$(bash $STATE_SH get_current_phase)
echo "module=$MODULE phase=$PHASE"
```

If MODULE is `DONE`, tell the user the port is complete and exit.

## Dispatch by phase

### If PHASE == "pending"

**Dispatch the Analyst.**

Assemble the prompt:
```bash
ATTEMPTS=$(bash $STATE_SH ... ) # current analyst attempts (read from state.json via jq if needed)
bash .claude/skills/python-to-java-port/scripts/assemble_prompt.sh analyst "$MODULE" 1 none > /tmp/analyst-prompt.txt
```

Invoke the `Agent` tool:
- `subagent_type`: `Explore`
- `description`: "Analyst: map <module>"
- `prompt`: the contents of `/tmp/analyst-prompt.txt`

After the agent returns:
1. Validate artifact:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh mapping docs/superpowers/port/$MODULE/mapping.md
   ```
2. If invalid: bump attempts (`bash $STATE_SH increment_attempts analyst`). If attempts < 2, re-dispatch once with failure details. Else halt and escalate.
3. If valid: `bash $STATE_SH set_phase analyst`.
4. Commit the mapping: `git add docs/superpowers/port/$MODULE/mapping.md && git commit -m "chore(port): $MODULE analyst mapping"`.
5. If `$MODULE` == "common" AND `checkpoint0_done` is false: mark phase `analyst_review` via a jq update to state.json, set `halt_reason: "checkpoint 0: review common/mapping.md, run /port-advance"`. Stop and inform user.
6. Otherwise: set phase `architect` and tell user to run `/port-module` again to dispatch Architect.

### If PHASE == "analyst_review"

No-op. Tell the user: "In checkpoint 0. Review `docs/superpowers/port/common/mapping.md` and run `/port-advance` to continue."

### If PHASE == "analyst"

**Dispatch the Architect.**

Same pattern:
- Assemble prompt via `assemble_prompt.sh architect $MODULE`.
- Invoke `Agent` tool:
  - `subagent_type`: `general-purpose`
  - `model`: `opus`
  - `description`: "Architect: design Java for <module>"
  - `prompt`: assembled prompt.
- On return, check for the string `ESCALATE:` at the start of the agent's output → halt with that message.
- Validate `design.md`:
  ```bash
  bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh design docs/superpowers/port/$MODULE/design.md
  ```
- If invalid: 1 retry, else halt.
- If valid: `bash $STATE_SH set_phase architect`, commit design, tell user to run `/port-module`.

### If PHASE == "architect"

**Dispatch the Developer.**

- Ensure Developer branch exists:
  ```bash
  git checkout -B port/$MODULE main
  ```
- Assemble prompt, dispatch `Agent` with `subagent_type: general-purpose`, `model: sonnet` (first attempt; opus on 3rd retry).
- On return, validate `completion.md`:
  ```bash
  bash .claude/skills/python-to-java-port/scripts/validate_artifact.sh completion docs/superpowers/port/$MODULE/completion.md
  ```
- If invalid: 1 retry, else halt.
- If valid: `bash $STATE_SH set_phase developer`, tell user to run `/port-module` to run gates.

### If PHASE == "developer"

**Run gates.**

```bash
bash .claude/skills/python-to-java-port/scripts/run_gates.sh $MODULE
```

- If exit 0: `bash $STATE_SH set_phase gates` then `set_phase complete`. Halt with "Module <m> complete. Review the branch and run /port-advance."
- If exit 2 (gate 7 pending): dispatch Architect read-only to produce signoff file:
  - Prompt the Architect with the final diff (`git diff main...port/$MODULE`) and `design.md`, ask for `approved` or `rejected` on first line of output.
  - Write stdout to `docs/superpowers/port/$MODULE/architect-signoff.txt`.
  - Re-run `run_gates.sh`.
- If exit 1: increment developer attempts. If < 3: re-dispatch Developer with gate failure log attached. If == 3: halt and escalate.
- Special: if gate 3 failed, halt immediately regardless of attempts. Set halt_reason.

### If PHASE == "gates" or "complete"

Tell the user "Module is complete. Run `/port-advance` to merge and move to the next module."

## Post-dispatch commit

After any successful phase transition:
```bash
git add docs/superpowers/port/state.json
git commit -m "chore(port): $MODULE phase -> $(bash $STATE_SH get_current_phase)"
```
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-module.md
git commit -m "feat(port): /port-module slash command"
```

---

### Task 31: Write `/port-status` slash command

**Files:**
- Create: `.claude/commands/port-status.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: Read-only. Print the current state of the port from state.json.
---

# /port-status

Read-only snapshot of port progress.

## Steps

1. Check state exists:
   ```bash
   STATE=docs/superpowers/port/state.json
   [[ -f $STATE ]] || { echo "no port in progress (state.json missing). Run /port-init"; exit 0; }
   ```

2. Print summary:
   ```bash
   jq -r '
     "=== Port status ===",
     "started:        " + .started_at,
     "current module: " + .current_module,
     "halt reason:    " + (.halt_reason // "none"),
     "",
     "Modules:",
     (.modules[] | "  \(.name): status=\(.status) phase=\(.phase) gates_passed=\(
       [.gates | to_entries[] | select(.value == "pass")] | length
     )/7 attempts=\(.attempts.analyst)/\(.attempts.architect)/\(.attempts.developer)"),
     "",
     "Latest artifacts:",
     (.modules[] | select(.artifacts.mapping != null) | "  " + .artifacts.mapping),
     (.modules[] | select(.artifacts.design != null) | "  " + .artifacts.design),
     (.modules[] | select(.artifacts.completion != null) | "  " + .artifacts.completion)
   ' $STATE
   ```

3. If `halt_reason` is non-null, suggest: `/port-retry` to resume, or `/port-rollback <module>` to restart the current module.
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-status.md
git commit -m "feat(port): /port-status slash command"
```

---

### Task 32: Write `/port-retry` slash command

**Files:**
- Create: `.claude/commands/port-retry.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: Clear halt_reason and re-dispatch the last failed agent for the current module's phase.
---

# /port-retry

Use after fixing an environmental issue (Docker down, Gradle cache, network, etc.).

## Steps

1. Read halt_reason:
   ```bash
   STATE=docs/superpowers/port/state.json
   REASON=$(jq -r '.halt_reason // empty' $STATE)
   [[ -n $REASON ]] || { echo "no halt in progress — nothing to retry"; exit 0; }
   echo "retrying from halt: $REASON"
   ```

2. Clear halt:
   ```bash
   bash .claude/skills/python-to-java-port/scripts/state.sh clear_halt
   ```

3. Re-invoke `/port-module`. This is the same entry point; the state machine resumes from the saved phase.
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-retry.md
git commit -m "feat(port): /port-retry slash command"
```

---

### Task 33: Write `/port-advance` slash command

**Files:**
- Create: `.claude/commands/port-advance.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: Release the module-boundary checkpoint (or checkpoint 0). Merges feature branch into main, advances to next module.
---

# /port-advance

## Preconditions

1. `docs/superpowers/port/state.json` exists; `.halt_reason` is null.
2. Current phase is `complete` OR `analyst_review` (checkpoint 0).

## Steps

Read state:
```bash
STATE_SH=.claude/skills/python-to-java-port/scripts/state.sh
MODULE=$(bash $STATE_SH get_current_module)
PHASE=$(bash $STATE_SH get_current_phase)
```

### If PHASE == "analyst_review" (checkpoint 0, common only)

1. Confirm with user they have reviewed `docs/superpowers/port/common/mapping.md`.
2. Mark checkpoint done:
   ```bash
   jq '(.modules[] | select(.name=="common") | .checkpoint0_done) = true' $STATE > $STATE.tmp && mv $STATE.tmp $STATE
   ```
3. Set phase back to `analyst` so the next `/port-module` dispatches Architect:
   ```bash
   bash $STATE_SH set_phase analyst
   ```
4. Commit:
   ```bash
   git add docs/superpowers/port/state.json
   git commit -m "chore(port): release checkpoint 0 for common"
   ```
5. Tell user: run `/port-module` to dispatch Architect.

### If PHASE == "complete"

1. Confirm with user they've reviewed branch `port/$MODULE`.
2. Merge:
   ```bash
   git checkout main
   git merge --no-ff port/$MODULE -m "chore(port): $MODULE module accepted"
   ```
3. Advance state (requires phase `accepted`):
   ```bash
   bash $STATE_SH set_phase accepted
   bash $STATE_SH advance_module
   ```
4. Commit state update:
   ```bash
   git add docs/superpowers/port/state.json
   git commit -m "chore(port): advance to $(bash $STATE_SH get_current_module)"
   ```
5. Tell user next module name; ask whether to run `/port-module` now.

### Else

Tell user the current phase doesn't allow /port-advance. Show current phase and suggest `/port-status`.
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-advance.md
git commit -m "feat(port): /port-advance slash command"
```

---

### Task 34: Write `/port-rollback` slash command

**Files:**
- Create: `.claude/commands/port-rollback.md`

- [ ] **Step 1: Write the command**

````markdown
---
description: State-only rollback for a module. Leaves code in place for inspection.
argument-hint: [module-name]
---

# /port-rollback

Reset a module's state to `pending` without touching code. Use when a module has been completed but later found broken and you want to redo it without manually editing state.json.

Argument: module name (`common`, `writer`, `collector`, or `cli`).

## Steps

1. Read target module from `$1`. Abort if not one of the four.
2. Confirm with user: "This will clear artifacts and state for $1 but leave code on branch `port/$1` in place. Proceed?"
3. On confirm:
   ```bash
   MODULE=$1
   STATE=docs/superpowers/port/state.json

   # Reset module state
   jq --arg m "$MODULE" '
     .modules |= map(
       if .name == $m then
         .status = "in_progress"
         | .phase = "pending"
         | .checkpoint0_done = false
         | .artifacts = {mapping:null, design:null, completion:null}
         | .gates = {ported_unit_tests:null, ported_chaos_tests:null, raw_text_byte_parity:null, metric_parity:null, verify_cli:null, static_checks:null, architect_signoff:null}
         | .attempts = {analyst:0, architect:0, developer:0}
         | .escalations = []
       else . end
     )
     | .current_module = $m
     | .halt_reason = null
   ' $STATE > $STATE.tmp && mv $STATE.tmp $STATE

   # Remove artifacts (but keep them in git history)
   git rm -rf "docs/superpowers/port/$MODULE/" || true

   git add $STATE
   git commit -m "chore(port): rollback $MODULE to pending"
   ```

4. Tell user: code on `port/$MODULE` branch is untouched; run `/port-module` to restart.
````

- [ ] **Step 2: Commit**

```bash
git add .claude/commands/port-rollback.md
git commit -m "feat(port): /port-rollback slash command"
```

---

## Phase 8 — End-to-end smoke + final commit

### Task 35: Smoke-test state + validation without running a full port

**Files:**
- None created; this is a verification step.

- [ ] **Step 1: Run full bats suite**

Run:
```bash
bats .claude/skills/python-to-java-port/tests/
```

Expected: all tests from `state.bats` and `validate_artifact.bats` pass (15 total).

- [ ] **Step 2: Smoke-test `/port-init` preconditions (without actually capturing fixtures)**

Run:
```bash
# In a throwaway test, init state only; do not run capture_fixtures.
TMP=$(mktemp -d)
PORT_STATE_FILE=$TMP/state.json bash .claude/skills/python-to-java-port/scripts/state.sh init
jq . $TMP/state.json
rm -rf $TMP
```

Expected: pretty-printed state.json with 4 modules, `current_module: "common"`.

- [ ] **Step 3: Smoke-test Gradle build**

Run:
```bash
cd cryptolake-java
./gradlew projects --no-daemon
./gradlew compileJava --no-daemon
cd ..
```

Expected: `projects` lists 6 subprojects; `compileJava` succeeds (no Java sources yet, so nothing to compile, but plugins should resolve).

- [ ] **Step 4: Smoke-test prompt assembly**

Run:
```bash
bash .claude/skills/python-to-java-port/scripts/assemble_prompt.sh analyst common 1 none | grep -c '{{' || echo "no placeholders remaining"
```

Expected: `no placeholders remaining` OR grep exits with count 0.

- [ ] **Step 5: Commit a smoke-test summary**

```bash
cat > /tmp/smoke-summary.md <<'EOF'
# Smoke test summary (port infrastructure)

- bats: state.bats + validate_artifact.bats — all pass
- state.sh init — produces valid state.json
- Gradle projects — 6 subprojects resolve
- Gradle compileJava — succeeds (no sources yet)
- assemble_prompt.sh — no unfilled placeholders for analyst/common

Port infrastructure is ready. Next: user runs /port-init for live fixture capture, then /port-module to begin porting common.
EOF
git add /tmp/smoke-summary.md
# (summary is informational only; do not commit if you prefer not to add transient files)
```

(Skip committing this summary if preferred; it's purely informational.)

---

### Task 36: Final integration sanity check

**Files:**
- None created. Final verification step.

- [ ] **Step 1: Check `.claude/` tree is complete**

Run:
```bash
find .claude -type f -not -path '*/settings.local.json' -not -path '*/worktrees/*' | sort
```

Expected output (minimum):
```
.claude/commands/port-advance.md
.claude/commands/port-init.md
.claude/commands/port-module.md
.claude/commands/port-retry.md
.claude/commands/port-rollback.md
.claude/commands/port-status.md
.claude/skills/python-to-java-port/SKILL.md
.claude/skills/python-to-java-port/prompts/analyst.md
.claude/skills/python-to-java-port/prompts/architect.md
.claude/skills/python-to-java-port/prompts/developer.md
.claude/skills/python-to-java-port/schemas/completion.schema.md
.claude/skills/python-to-java-port/schemas/design.schema.md
.claude/skills/python-to-java-port/schemas/mapping.schema.md
.claude/skills/python-to-java-port/scripts/assemble_prompt.sh
.claude/skills/python-to-java-port/scripts/capture_fixtures.sh
.claude/skills/python-to-java-port/scripts/gates/gate1_unit_tests.sh
.claude/skills/python-to-java-port/scripts/gates/gate2_chaos_tests.sh
.claude/skills/python-to-java-port/scripts/gates/gate3_raw_text_parity.sh
.claude/skills/python-to-java-port/scripts/gates/gate4_metric_parity.sh
.claude/skills/python-to-java-port/scripts/gates/gate5_verify_cli.sh
.claude/skills/python-to-java-port/scripts/gates/gate6_static_checks.sh
.claude/skills/python-to-java-port/scripts/gates/gate7_architect_signoff.sh
.claude/skills/python-to-java-port/scripts/lib/assert.bash
.claude/skills/python-to-java-port/scripts/run_gates.sh
.claude/skills/python-to-java-port/scripts/state.sh
.claude/skills/python-to-java-port/scripts/validate_artifact.sh
.claude/skills/python-to-java-port/tests/state.bats
.claude/skills/python-to-java-port/tests/validate_artifact.bats
```

- [ ] **Step 2: Check executable bits on scripts**

Run:
```bash
find .claude/skills/python-to-java-port/scripts -type f -name '*.sh' ! -executable
```

Expected: no output (all scripts are executable).

If any are not: `chmod +x <file>` and recommit.

- [ ] **Step 3: Final commit (optional consolidation)**

All commits have already been made per task. No additional commit needed. Print summary to user:

```
Port infrastructure complete.
- Skill: .claude/skills/python-to-java-port/
- Commands: /port-init, /port-module, /port-status, /port-retry, /port-advance, /port-rollback
- Gradle scaffold: cryptolake-java/ (6 subprojects)
- Python tap: src/collector/tap.py

Next step (user): run /port-init to capture live parity fixtures.
After that: /port-module to begin porting the common module.
```

---

## Self-review checklist (run at end of plan execution)

- [ ] All 28 skill rules present in `SKILL.md`
- [ ] All 11 mapping/design sections enforced by `validate_artifact.sh`
- [ ] All 5 completion sections enforced
- [ ] All 7 gates have their own script in `scripts/gates/`
- [ ] All 6 slash commands in `.claude/commands/`
- [ ] Python tap module has 3 passing tests
- [ ] Gradle scaffold resolves 6 subprojects
- [ ] Fixture directory skeleton in place with README
- [ ] State machine has: init, get_current_module, get_current_phase, set_phase, advance_module, set_halt, clear_halt, record_gate_result, increment_attempts — all with bats coverage
- [ ] No tasks in this plan contain `TBD`, `TODO`, `???`, `FIXME` placeholders (the FORBIDDEN_STRINGS array in `validate_artifact.sh` is not a violation; it's a list of strings the script searches for)

---

## What this plan does NOT build

Explicitly out of scope for this plan:

- Java `Main` classes, domain classes, or any implementation code inside `cryptolake-java/*/src/main/java/`
- Actual `:writer:dumpMetricSkeleton`, `:writer:produceSyntheticArchives`, `:collector:runRawTextParity` Gradle tasks (placeholders only; real implementations come during the port itself, as part of the Developer role's work)
- Running the port (`common` → `writer` → `collector` → `cli`) — driven afterward by user-invoked `/port-module` sessions
- Retiring the Python services — happens after all 4 modules are accepted

These happen in separate sessions after this plan is executed.
