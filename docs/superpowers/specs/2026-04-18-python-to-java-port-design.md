---
title: CryptoLake — Python-to-Java Port Design
date: 2026-04-18
status: approved
scope: design-only (implementation plan produced separately)
---

# CryptoLake — Python-to-Java Port Design

## 0. Purpose and summary

Port the CryptoLake collector platform from Python 3.12 (asyncio + uvloop) to Java 21 (virtual threads) with **behavioral parity**, **byte-identity for raw payload fidelity**, and a **three-role multi-agent workflow** (Analyst → Architect → Developer) driven by a purpose-built skill.

The port runs module-by-module (`common` → `writer` → `collector` → `cli`), with user-reviewed checkpoints at each module boundary and one upfront calibration checkpoint after the first Analyst run. Every module must pass a fixed 7-gate "done" protocol before advancing.

### Locked decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Java target | Plain Java 21 + virtual threads, minimal framework |
| 2 | Conversion strategy | Behavioral parity, module-by-module |
| 3 | Module order | `common` → `writer` → `collector` → `cli` |
| 4 | Verification gate | Behavioral parity + byte-identity for `raw_text` / `raw_sha256` |
| 5 | Orchestration | Auto per module, review per module, +1 calibration after first Analyst output |

### Non-goals

- Not a line-by-line translation. A Python symbol may have no Java counterpart if the architect deems it dead or redundant.
- Not a framework-driven rewrite. No Spring, Micronaut, Quarkus, or equivalent.
- Not a reactive rewrite. No Reactor, RxJava, or coroutine library.
- Not a schema redesign. Envelope, Kafka topics, archive layout, metric names, and `verify` CLI output remain byte-compatible.

---

## 1. Target architecture

### 1.1 Build and module layout

Single Gradle multi-module project:

```
cryptolake-java/
├── settings.gradle.kts
├── build.gradle.kts                    # versions, Spotless, JaCoCo, Error Prone, NullAway
├── gradle/libs.versions.toml           # version catalog
├── common/                             # shared library (jar)
├── writer/                             # :writer application
├── collector/                          # :collector application
├── backfill/                           # :backfill CLI app
├── consolidation/                      # :consolidation CLI app
├── verify/                             # :verify CLI app (gaps/integrity)
├── parity-fixtures/                    # recorded WebSocket frames + expected outputs
└── docker/                             # Dockerfile.writer, Dockerfile.collector, ...
```

Root package: `com.cryptolake.*`. One Gradle subproject per Python top-level package. Python's single `cli/` is split into `backfill`, `consolidation`, `verify` subprojects to match existing Dockerfiles and Java's one-`main`-per-artifact convention.

### 1.2 Package layout per module

```
com.cryptolake.common
├── envelope/           # EnvelopeRecord, GapRecord (records), EnvelopeCodec
├── config/             # AppConfig (record), YamlConfigLoader (Jackson YAML)
├── logging/            # StructuredLogger facade over SLF4J MDC
├── jsonl/              # JsonlReader / JsonlWriter
├── health/             # HealthServer (jdk.httpserver), HealthState
├── identity/           # SystemIdentity (hostname, pid, session_id)
├── kafka/              # KafkaAdminHelper, TopicNames
└── util/               # Sha256, ClockSupplier, Shutdownable
```

```
com.cryptolake.writer
├── Main                            # main() wires Consumer + BufferManager + Rotator
├── consumer/KafkaConsumerLoop      # split from consumer.py (1343 lines)
├── consumer/RecordHandler
├── consumer/OffsetCommitCoordinator
├── buffer/BufferManager
├── rotate/FileRotator
├── compress/ZstdCompressor         # zstd-jni streaming wrapper
├── state/StateManager
├── failover/FailoverController
├── failover/HostLifecycleReader
├── failover/RestartGapClassifier
└── metrics/WriterMetrics           # Micrometer registry
```

```
com.cryptolake.collector
├── Main
├── connection/WebSocketConnection  # java.net.http.WebSocket + reconnect backoff
├── streams/StreamSubscriber        # base; impls per stream type
├── streams/DepthStream             # depth diff + snapshot resync
├── streams/SimpleStream            # trades, bookticker, funding, liquidations
├── streams/OpenInterestStream      # REST-polled
├── snapshot/SnapshotFetcher
├── producer/KafkaProducerBridge
├── gap/GapDetector
├── backup/BackupChainReader
└── metrics/CollectorMetrics
```

`backfill`, `consolidation`, `verify` each: `Main` + domain classes. No framework, explicit wiring in `main()`.

### 1.3 Library mapping

| Python dep | Java replacement | Notes |
|---|---|---|
| `asyncio` + `uvloop` | Java 21 virtual threads (`Executors.newVirtualThreadPerTaskExecutor()`) | Straight-line blocking code; no Reactor |
| `aiohttp` | `java.net.http.HttpClient` | Built-in, virtual-thread friendly |
| `websockets` | `java.net.http.HttpClient.newWebSocketBuilder()` | `Listener.onBinary`/`onText` preserve raw frames — critical for `raw_text` |
| `confluent-kafka` | `org.apache.kafka:kafka-clients` | Official, no Spring Kafka |
| `orjson` | `com.fasterxml.jackson:jackson-databind` + `jackson-dataformat-yaml` | Raw bytes via `byte[]` passthrough |
| `pydantic` / `pydantic-settings` | Java 21 records + Jackson + Hibernate Validator | Config records deserialized from YAML |
| `psycopg[binary]` | JDBC (`org.postgresql:postgresql`) + HikariCP | Blocking JDBC on virtual threads |
| `zstandard` | `com.github.luben:zstd-jni` | Native, does not pin virtual threads |
| `prometheus-client` | `io.micrometer:micrometer-registry-prometheus` | Metric name + label parity is a gate |
| `structlog` | SLF4J + Logback + Logstash encoder (JSON) + MDC | Context via MDC |
| `click` | `info.picocli:picocli` | CLI apps |
| `pyyaml` | `jackson-dataformat-yaml` | Same Jackson instance |
| `pytest` + `pytest-asyncio` | JUnit 5 | Parallel + extensions |
| `testcontainers[kafka,postgres]` | `org.testcontainers:kafka`, `:postgresql` | Same library family |

### 1.4 Concurrency model

- Every service's `main()` opens `Executors.newVirtualThreadPerTaskExecutor()` in try-with-resources.
- Per-symbol / per-stream work runs on dedicated virtual threads.
- Structured concurrency via `java.util.concurrent.StructuredTaskScope` groups child tasks per connection for deterministic shutdown (Java equivalent of `asyncio.TaskGroup`).
- Backpressure via bounded `LinkedBlockingQueue` between WebSocket handler and Kafka producer; `put()` blocks the receiver virtual thread when Kafka lags.
- Pinning: `zstd-jni` and `kafka-clients` do not pin virtual threads. The skill forbids `synchronized (...)` around any blocking call; a static check enforces this.

### 1.5 Data flow per service (unchanged semantics)

- **Collector**: WebSocket frame → `raw_text` extracted pre-parse → envelope → `KafkaProducer.send()`. Depth resync: snapshot REST fetch → buffer diffs during fetch → apply in `lastUpdateId` order.
- **Writer**: `KafkaConsumer.poll()` → `RecordHandler` → `BufferManager` → `FileRotator` → `ZstdCompressor` → `fsync()` → **then** `commitSync(offsets)`. Flush-before-commit ownership in a single explicit ordering function; unit test asserts invariant.
- **CLIs**: `main()` opens files, processes, exits.

### 1.6 Deployment

- One Dockerfile per service, `eclipse-temurin:21-jre` runtime, multi-stage with `:21-jdk` build stage.
- JVM args: `-XX:+UseZGC -XX:+ZGenerational -Xmx<per-service>m` plus `--enable-preview` only if `StructuredTaskScope` preview is used.
- Same `docker-compose.yml` shape; services renamed/swapped per module during port.

### 1.7 Invariants preserved (non-goals unchanged)

- Redpanda/Kafka topic names and partitioning
- Envelope schema fields and their canonical order
- Archive file naming, directory structure, sidecar checksums
- Prometheus metric names + labels
- `verify` CLI output contract

---

## 2. The skill

### 2.1 Identity

- **Name**: `python-to-java-port`
- **Location**: `.claude/skills/python-to-java-port/SKILL.md` (project-scoped under the repo; encodes CryptoLake-specific invariants and module order)
- **Trigger** (frontmatter `description`): "Use when porting a Python service to Java 21. Enforces behavioral parity, raw-payload fidelity, 7-gate module-done protocol, and Analyst→Architect→Developer role handoffs. Trigger on: 'port to Java', 'convert to Java', 'Java rewrite', or user invocation of `/port-module`."
- **Invocation**: automatic on trigger phrases; explicit via `/port-module` slash command (normal driver)

### 2.2 Ruleset

Four tiers. Agents read the skill at start of each run; violations block progression.

**Tier 1 — Invariants (from design §1.4.1):**

1. `raw_text` is captured from the `onBinary`/`onText` WebSocket callback *before* any JSON parse. Never reconstructed from a parsed object.
2. `raw_sha256` is computed over the exact `raw_text` bytes, once, at capture time. Recomputing downstream is forbidden.
3. Disabled streams emit zero artifacts: no subscribe, no poll, no producer send, no gap record.
4. Kafka consumer offsets are committed only after file flush returns successfully (`fsync` done). A single function owns this ordering.
5. Every detected gap emits metric, log, and archived gap record — all three.
6. Recovery prefers replay from Kafka / exchange cursors. Reconstruction from inferred state is forbidden.
7. JSON codec must not re-order, re-quote, or re-format `raw_text`. `raw_text` travels byte-for-byte identical to Python's handling.

**Tier 2 — Java practices:**

8. Java 21 only; `--enable-preview` only if required for `StructuredTaskScope`.
9. No `synchronized (...)` around any blocking call. Use `ReentrantLock` if locking is unavoidable.
10. No `Thread.sleep` in hot paths; use `ScheduledExecutorService` or parked virtual threads.
11. No reflection-heavy frameworks. Explicit wiring in `main`.
12. Immutable data carriers: records, never POJOs with setters.
13. No checked-exception leaks across module boundaries; wrap in `CryptoLakeException`.
14. One `HttpClient` per service; one `KafkaProducer` per service; one `ObjectMapper` per service.
15. Logs are JSON via Logback + Logstash encoder; MDC for `symbol`, `stream`, `session_id`.
16. Fail-fast on invariant violation; retry with exponential backoff only on declared-transient errors.

**Tier 3 — Parity rules:**

17. Every Python test has a JUnit 5 counterpart with trace comment `// ports: tests/unit/writer/test_X.py::test_name`.
18. Prometheus metric names + labels diff-match Python — generated test asserts set-equality.
19. `raw_text` / `raw_sha256` byte-identity via recorded WebSocket-frame fixture corpus in `parity-fixtures/`.
20. Python `verify` CLI, run against Java-produced archives, must pass.
21. Envelope field order in serialized JSON follows Python canonical order (defined once in `EnvelopeCodec`).

**Tier 5 — Translation patterns:**

68 rules across 13 categories (A–M: concurrency, JSON codec, Kafka, WebSocket/HTTP, numerics, timestamps, exceptions, logging, file I/O, configuration, CLI, testing, domain-specific), mined from the Python codebase during Phase 0a. Full catalog lives in `.claude/skills/python-to-java-port/tier5-translation-rules.md`. The orchestrator's `assemble_prompt.sh` inlines Tier 5 verbatim into every role prompt (Analyst, Architect, Developer) at dispatch time via a `{{tier5_rules}}` placeholder.

- Analysts cite Tier 5 rule IDs in `mapping.md §10 Port risks`.
- Architects cite Tier 5 rule IDs in `design.md §10 Rule compliance`.
- Developers cite Tier 5 rule IDs in `completion.md §3 Rule compliance`.

This tier is advisory-authoritative: it locks translation decisions once to prevent drift across modules, but an agent may escalate if a rule conflicts with an invariant (Tier 1 always wins).

**Tier 4 — Process rules:**

22. Agent order per module is fixed: **Analyst → Architect → Developer**. No out-of-order dispatch.
23. Artifact paths are fixed: `docs/superpowers/port/<module>/mapping.md` (Analyst), `design.md` (Architect), `completion.md` (Developer).
24. Each artifact must contain a `## Rule compliance` section. Missing section = orchestrator rejects and re-dispatches.
25. Developer does not start until `design.md` frontmatter contains `status: approved`.
26. After Developer reports complete, orchestrator runs all 7 gates; failure re-dispatches Developer (max 3 attempts).
27. At module boundary, orchestrator halts and waits for user — no auto-advance.
28. First-module exception: after Analyst produces `common/mapping.md`, orchestrator pauses for user review (checkpoint 0).

### 2.3 Prompts

Each role prompt is assembled from:

1. Static role template (`skill/prompts/<role>.md`): identity, allowed tools, forbidden actions, output schema, rules it honors.
2. Module context (dynamic): module name, Python files, links to prior artifacts, attempt number, gate failures if retrying.
3. Success criteria (role-specific).

### 2.4 Checkpoint protocol

State in `docs/superpowers/port/state.json` (committed). Phase values: `pending` → `analyst` → `analyst_review` (common only) → `architect` → `developer` → `gates` → `complete` → `accepted`.

### 2.5 Failure handling

- Malformed artifact: re-dispatch with violation details (1 retry), then escalate.
- Gate 1, 2, 4, 5, 6 fails: re-dispatch Developer with output (max 3 attempts), then escalate.
- Gate 3 (`raw_text` byte-identity) fails: halt immediately, escalate.
- Architect flags invariant conflict: halt immediately, escalate. No retry.
- Gate 7 (architect sign-off) rejects: re-dispatch Developer (1 attempt) with reasons, then escalate.

---

## 3. Agent role contracts

Each agent runs as a separate `Agent` tool dispatch. Agents do not see each other's conversation — only the artifact the prior role produced.

### 3.1 Analyst

- **Subagent type**: `Explore` with thoroughness `"very thorough"`
- **Inputs**: module name, Python file list, test file list, Tier 1 rules
- **Allowed tools**: Read, Grep, Glob, read-only Bash (`wc`, behavior probes)
- **Forbidden**: Java design proposals, Python edits, cross-module reads beyond dependency tracing
- **Output**: `docs/superpowers/port/<module>/mapping.md` (11 required sections; see §3.1.1)
- **Success criteria**: all sections present; every Python file appears in §2 (File inventory); every Tier 1 rule appears in §9 (Invariants touched); §11 (Rule compliance) exists

#### 3.1.1 Mapping.md schema

```
---
module: <name>
status: complete
produced_by: analyst
python_files: [list]
python_test_files: [list]
---

## 1. Module summary
## 2. File inventory
## 3. Public API surface
## 4. Internal structure
## 5. Concurrency surface
## 6. External I/O
## 7. Data contracts
## 8. Test catalog
## 9. Invariants touched (Tier 1 rules)
## 10. Port risks
## 11. Rule compliance
```

### 3.2 Architect

- **Subagent type**: `general-purpose` with model `opus`
- **Inputs**: `mapping.md`, Tier 1+2+3 rules, target architecture
- **Allowed tools**: Read, Grep, Glob, WebFetch; Write only to design artifact
- **Forbidden**: writing Java code, changing library choices from §1.3 without user escalation, introducing a framework
- **Output**: `docs/superpowers/port/<module>/design.md` (11 sections; see §3.2.1)
- **Success criteria**: frontmatter `status: approved`; §4 covers every mapping §3+§4 item; §10 has entries for all Tier 1/2/3 rules; no TODO/TBD strings

#### 3.2.1 Design.md schema

```
---
module: <name>
status: approved
produced_by: architect
based_on_mapping: <sha of mapping.md>
---

## 1. Package layout
## 2. Class catalog
## 3. Concurrency design
## 4. Python → Java mapping table
## 5. Library mapping
## 6. Data contracts
## 7. Error model
## 8. Test plan
## 9. Metrics plan
## 10. Rule compliance
## 11. Open questions for developer
```

### 3.3 Developer

- **Subagent type**: `general-purpose` with model `sonnet` (promote to `opus` on 3-retry escalation)
- **Inputs**: `mapping.md`, `design.md`, Tier 1+2+3 rules, Python source (read-only reference)
- **Allowed tools**: all except `Agent` (no sub-delegation)
- **Forbidden**: changing the design (escalate in completion.md §4 instead), editing Python, `@SuppressWarnings`, `--no-verify`
- **Output**: Java code + tests committed directly to `main` (one commit per logical chunk); parity fixtures if required; `docs/superpowers/port/<module>/completion.md`
- **Success criteria**: all 7 gates show `pass`; §2 (Deviations) empty or re-approved; §3 (Rule compliance) lists every Tier 1/2/3 rule with file:line; §4 (Escalations) empty; `./gradlew :<module>:check` clean

#### 3.3.1 Completion.md schema

```
---
module: <name>
status: complete
produced_by: developer
commits: [list of sha]             # range: developer_start_sha..HEAD, module's commits on main
---

## 1. Gate results
## 2. Deviations from design
## 3. Rule compliance
## 4. Escalations
## 5. Known follow-ups
```

### 3.4 Retry and escalation summary

| Failure | Action |
|---|---|
| Malformed artifact | Re-dispatch (1 retry), then escalate |
| Gate 1/2/4/5/6 fails | Re-dispatch Developer (max 3), then escalate |
| Gate 7 (architect sign-off) fails | Re-dispatch Developer (1), then escalate |
| Invariant conflict flagged by Architect | Halt immediately, escalate |
| Gate 3 (byte-identity) fails | Halt immediately, escalate |
| Developer §4 escalation | Halt, escalate |

---

## 4. Orchestration and state

### 4.1 Slash commands

All live in `.claude/commands/`:

| Command | Purpose |
|---|---|
| `/port-init` | One-time setup: scaffold Gradle project, capture parity-fixture corpus, snapshot metrics + verify output, write `state.json` |
| `/port-module` | Advance current module by one phase (idempotent) |
| `/port-status` | Read-only: print state.json in human form |
| `/port-retry` | Re-dispatch last failed agent with failure log after environment fix |
| `/port-advance` | Explicit checkpoint release; tag the accepted commit; advance to next module |
| `/port-rollback <module>` | State-only rollback; leaves code in place |

Normal loop: `/port-init` → `/port-module` (repeat) → review → `/port-advance` → `/port-module` (next) → ...

### 4.2 State file

`docs/superpowers/port/state.json` (committed). Schema:

```json
{
  "version": 1,
  "started_at": "<ISO8601>",
  "current_module": "common",
  "halt_reason": null,
  "modules": [
    {
      "name": "common",
      "status": "in_progress",
      "phase": "analyst",
      "checkpoint0_done": false,
      "artifacts": {
        "mapping": "docs/superpowers/port/common/mapping.md",
        "design": null,
        "completion": null
      },
      "gates": {
        "ported_unit_tests": null,
        "ported_chaos_tests": null,
        "raw_text_byte_parity": null,
        "metric_parity": null,
        "verify_cli": null,
        "static_checks": null,
        "architect_signoff": null
      },
      "attempts": { "analyst": 0, "architect": 0, "developer": 0 },
      "escalations": []
    },
    { "name": "writer",    "status": "pending" },
    { "name": "collector", "status": "pending" },
    { "name": "cli",       "status": "pending" }
  ]
}
```

### 4.3 Orchestrator state machine

```
on /port-module:
  state = read(state.json)
  if state.halt_reason: print and exit
  m = modules[current]

  switch m.phase:
    pending        → dispatch analyst; phase=analyst
    analyst        → validate mapping
                     if invalid and attempts<2: redispatch
                     elif common and not checkpoint0: phase=analyst_review; halt
                     else: dispatch architect; phase=architect
    analyst_review → no-op; /port-advance required
    architect      → validate design
                     if not approved: halt
                     dispatch developer; phase=developer
    developer      → validate completion
                     phase=gates
    gates          → run 7 gates
                     if fail and developer.attempts<3: redispatch
                     elif fail: halt
                     else: phase=complete; halt
    complete       → no-op; /port-advance required
```

### 4.4 Dispatch mechanics

Each dispatch uses the `Agent` tool. Orchestrator assembles prompt from role template + module context + verbatim rule tiers. After agent returns, orchestrator verifies artifact file exists and validates schema. An agent claiming done without an artifact counts as a failed attempt.

### 4.5 Skill directory layout

```
.claude/
├── skills/
│   └── python-to-java-port/
│       ├── SKILL.md
│       ├── prompts/
│       │   ├── analyst.md
│       │   ├── architect.md
│       │   └── developer.md
│       ├── schemas/
│       │   ├── mapping.schema.md
│       │   ├── design.schema.md
│       │   └── completion.schema.md
│       └── scripts/
│           ├── dispatch.sh
│           ├── validate_artifact.sh
│           ├── run_gates.sh
│           ├── state.sh
│           └── gates/
│               ├── gate1_unit_tests.sh
│               ├── gate2_chaos_tests.sh
│               ├── gate3_raw_text_parity.sh
│               ├── gate4_metric_parity.sh
│               ├── gate5_verify_cli.sh
│               ├── gate6_static_checks.sh
│               └── gate7_architect_signoff.sh
└── commands/
    ├── port-init.md
    ├── port-module.md
    ├── port-status.md
    ├── port-retry.md
    ├── port-advance.md
    └── port-rollback.md
```

### 4.6 Resumability

All state is in `state.json` + artifact files. Fully resumable across Claude Code sessions. Start a new session with `/port-status`, resume with `/port-module`.

### 4.7 Git hygiene

- All work lands on `main` directly — the port is sequential, no parallel module development, so branches add overhead without benefit.
- Orchestrator commits artifacts after each successful phase: `chore(port): <module> <phase> complete`
- Developer commits Java code directly to `main` in logical chunks (one commit per class or coherent feature). At Developer-phase dispatch, orchestrator records `developer_start_sha` = current `HEAD` in `state.json` so the module's commit range is recoverable.
- At `/port-advance`, orchestrator records `accepted_sha` in `state.json` and creates an annotated git tag `port-<module>-accepted` at that commit. The tag is the module's release marker.
- Failed gates leave partial Java code on `main`; this is benign (Python services keep running; Java subproject just fails to build) and lets you inspect state directly via `git log`.
- Rollback uses `git revert` over the recorded commit range, never destructive resets.

### 4.8 Safety rails

1. Orchestrator refuses to dispatch if working tree dirty (outside port-produced files and `cryptolake-java/`)
2. `state.json` has schema version; mismatch = halt
3. Skill never calls destructive git (`reset --hard`, `push --force`, `branch -D`, `checkout --`)
4. Rollback uses `git revert` only; the operator always has a chance to review the revert commits before they land

---

## 5. Per-module port recipe

### 5.1 Prerequisite: `/port-init` (runs once)

1. **Create `cryptolake-java/` skeleton.** Gradle multi-module layout from §1.1. Root build pins Java 21, Spotless, Error Prone, NullAway, JaCoCo. Version catalog pins all library versions. Baseline commit.
2. **Capture parity fixture corpus.** Add "tap" mode to Python collector that writes every inbound WebSocket frame verbatim to `parity-fixtures/websocket-frames/<stream>/<timestamp>.raw`, along with arrival timestamp and resulting Python-produced envelope. Run a 10-minute recording against live Binance. This is the ground truth for Gate 3.
3. **Snapshot Prometheus metrics.** Scrape `/metrics` from running Python collector + writer for 2 minutes; canonical name+label sets written to `parity-fixtures/metrics/<service>.txt`.
4. **Snapshot `verify` CLI expected output.** Run Python `verify` against step-2 archives; stdout + exit code saved to `parity-fixtures/verify/expected.txt`.
5. **Write `state.json`** with all 4 modules `pending`, `current_module = "common"`.
6. **Commit** `chore(port): init scaffold and parity fixtures`.

User inspects fixture corpus summary (frame count, byte count) before first `/port-module`.

### 5.2 Per-module flow (runs 4 times)

```
pending
  ↓ /port-module   → dispatch Analyst
analyst (producing mapping.md)
  ↓ /port-module   → validate, dispatch next
[common only: analyst_review]  ← checkpoint 0
  ↓ /port-advance                (only for common)
architect (producing design.md)
  ↓ /port-module
developer (producing code + tests + completion.md)
  ↓ /port-module
gates (7 gates run in sequence)
  ↓ (all pass)
complete  ← module boundary, user reviews
  ↓ /port-advance
accepted; next module becomes current
```

### 5.3 Per-phase activity

- **analyst**: read-only over Python source; produce `mapping.md`; orchestrator validates schema (1 retry allowed)
- **analyst_review** (common only): orchestrator halts; user inspects; `/port-advance` releases
- **architect**: input `mapping.md` + all rule tiers + target architecture; produce `design.md` with `status: approved`; invariant-conflict flag = immediate halt
- **developer**: orchestrator records `developer_start_sha` = HEAD; Developer implements per design on `main`; ports tests to JUnit 5; captures any additional fixtures required; commits directly to `main` in logical chunks; writes `completion.md`
- **gates**: run 7 gates sequentially; results appended to `completion.md §1`
- **complete**: commit `chore(port): <module> complete`; halt with summary; user reviews
- **accepted** (via `/port-advance`): orchestrator records `accepted_sha` = HEAD; creates annotated tag `port-<module>-accepted`; updates `state.json`; next module becomes current

### 5.4 The 7 gates

1. **Unit/integration tests**: `./gradlew :<module>:test` — all pass, zero skipped
2. **Chaos tests**: `./gradlew :<module>:test --tests "*chaos*"` with Testcontainers (writer + collector only)
3. **`raw_text` byte-identity**: replay `parity-fixtures/websocket-frames/**` through Java capture path; compare produced `raw_text` bytes + `raw_sha256` hex against expected envelope per frame
4. **Metric parity**: start Java service with Prometheus registry; scrape `/metrics`; normalize (names + label keys); set-equality diff against `parity-fixtures/metrics/<service>.txt`
5. **`verify` CLI parity**: run Python `verify` against Java-produced archives from synthetic integration run; expect equivalent output
6. **Static checks**: `./gradlew :<module>:check` — Spotless, Error Prone, NullAway, custom rules (no `synchronized` around blocking I/O, no `Thread.sleep` in async paths)
7. **Architect sign-off**: re-dispatch Architect read-only with `design.md` + final diff + gate 1–6 results; returns `signoff: approved` or `rejected`

### 5.5 Parity-fixture discipline

1. Fixtures are immutable once captured. Changes require audit note in `state.json`.
2. Module-specific additional fixtures go to `parity-fixtures/<module>/additions/` as distinct commits from code.
3. Fixtures live in-repo (git-lfs considered later if size demands).

### 5.6 Per-module notes

- **`common`**: gates 2, 3, 5 trivially pass (pure library). Role is to prove the skill + agent flow.
- **`writer`**: gate 2 dominates — failover, disk-full, buffer overflow, offset-commit-after-flush. Testcontainers mandatory. Expect 2–3 developer retries on timing-sensitive chaos tests.
- **`collector`**: gate 3 is critical — `java.net.http.WebSocket.Listener.onBinary(ByteBuffer)` must produce bytes identical to Python's `websockets.recv()`. Budget for one escalation.
- **`cli`**: split into `backfill`, `consolidation`, `verify` subprojects. Gates 2, 3 trivial. Gate 5 is the core parity gate for `verify` itself.

### 5.7 Timing estimate (non-commitment)

- `/port-init` + fixture capture: 1 session (~1 hour wall-clock, mostly recording window)
- `common`: 1–2 sessions
- `writer`: 3–5 sessions
- `collector`: 4–6 sessions
- `cli`: 2–3 sessions

Agent time is token cost, not wall-clock. Primary cost is user review time at module boundaries.

---

## 6. Risk register

| Risk | Severity | Mitigation |
|---|---|---|
| WebSocket byte capture differs between `websockets` lib and `java.net.http.WebSocket` (gate 3 fail) | High | Early validation: gate 3 runs on `collector` first in a spike before full module port if gate 3 fails once; escalate immediately on 3rd failure |
| Virtual-thread pinning in a transitive dependency causes throughput collapse | Medium | JFR profiling in CI; Tier 2 rule 9 blocks `synchronized` around blocking I/O |
| Kafka `commitSync` / flush ordering violated in refactor | High (data-loss) | Tier 1 rule 4; dedicated unit test asserts ordering; architect sign-off checks this |
| Parity fixture corpus stale (Python behavior evolves mid-port) | Medium | Immutability + audit-note discipline (§5.5 rule 1); re-capture triggers explicit review |
| Agent retry loops burn tokens on a fundamentally broken approach | Medium | 3-retry ceiling; gate 3 + invariant conflict = immediate halt (no retry) |
| Spring/Micronaut "helpful" refactor snuck in | Low | Tier 2 rule 11; architect §10 rule-compliance check; developer static check |
| Chaos test timing assumptions don't translate (Java GC pauses differ) | Medium | Writer chaos tests adapted with Java-specific timing tolerances; documented deviations in design §8 |

---

## 7. Open questions deferred to implementation plan

- Exact Gradle version catalog contents (specific library versions)
- Spotless rule set (Google-Java-Format vs Palantir vs Sun?)
- Whether to pin `--enable-preview` for `StructuredTaskScope` or use a tiny in-house scope utility and avoid preview flag
- Exact JVM heap/GC sizing per service for Dockerfiles
- Whether to run parity fixture capture against Binance Mainnet or Testnet (Mainnet gives realistic diversity; Testnet avoids API-key considerations)
- Whether the skill is repo-scoped only, or mirrored to `~/.claude/skills/` for future ports of other Python projects

These are implementation-plan concerns, not design blockers.
