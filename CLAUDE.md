# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

CryptoLake is a raw-first crypto market-data archive. The collector pulls Binance USD-M Futures over WebSocket/REST, the writer drains Redpanda to compressed JSON Lines on disk, and `cryptolake-verify` audits the archive. Java 21, multi-module Gradle. The Python implementation was ported and retired — `src/`, `pyproject.toml`, and `tests/` (Python) no longer exist; only `tests/chaos/` (bash scenarios) remain.

Authoritative spec: `docs/2026-03-13-cryptolake-design.md`. The system invariants in §1.4.1 are non-negotiable — raw-payload fidelity, durability ordering (offsets commit only after disk flush), explicit gap surfacing, and disabled-stream silence are architectural rules, not goals.

## Build & test

Use the Gradle wrapper (`./gradlew`). Spotless (google-java-format 1.23.0) runs on every build; CI fails on unformatted code.

```bash
./gradlew build                                    # compile + spotlessCheck + tests, all modules
./gradlew :writer:test                             # one module
./gradlew :writer:test --tests "*BufferManagerTest*"   # one test class
./gradlew :writer:test --tests "*BufferManagerTest.flushTriggeredAtThreshold"  # one method
./gradlew spotlessApply                            # auto-format before commit
./gradlew :verify:installDist                      # produces verify/build/install/verify/bin/verify
./gradlew :collector:installDist :writer:installDist :backfill:installDist :consolidation:installDist
```

## Running locally

```bash
cp .env.example .env                       # set POSTGRES_PASSWORD, HOST_DATA_DIR, etc.
docker compose up -d                       # full stack: redpanda, pg, collector, collector-backup, writer, backfill, consolidation, prometheus, alertmanager, whatsapp-bridge
docker compose logs -f writer
verify/build/install/verify/bin/verify verify --base-dir $HOST_DATA_DIR --date <YYYY-MM-DD>
```

Health endpoints: collector primary `:8000`, collector backup `:8004`, writer `:8001`, prometheus `:9090`. Each exposes `/ready` and `/metrics`. Default config is mounted from `config/config.yaml`.

## Chaos suite

Each `tests/chaos/NN_*.sh` spins up an isolated `cryptolake-chaos-NN` compose project, injects a fault, then asserts `cryptolake-verify` exits 0 with `ERRORS=0` and the expected gap envelope is archived.

```bash
bash tests/chaos/01_collector_unclean_exit.sh         # one scenario
bash scripts/run-chaos-tests.sh                       # all scenarios; logs to build/chaos-logs/
bash scripts/run-chaos-tests.sh 09                    # filter to one
./gradlew :consolidation:test --tests "*ChaosVerifyIT*"  # JUnit harness wraps all scenarios
```

Scenario index lives in `tests/chaos/README.md` with the gap-reason taxonomy each one validates.

## Architecture (the parts a single file won't show you)

**Pipeline.** `collector → redpanda topics (binance.<stream>) → writer → /data/binance/<symbol>/<stream>/<date>/hour-HH.jsonl.zst + .sha256`. `cryptolake-verify` reads the archive offline.

**Dual-source redundancy (plan dated 2026-05-03 — the *current* design, not the older retrospective-recovery one).** A second collector (`collector-backup` in docker-compose) publishes to `backup.binance.<stream>` topics with 30-min retention. The writer has *two* Kafka consumers wired in `writer/Main.java`: a primary consumer with `group.id=cryptolake-writer` (offsets committed) and a `BackupTailConsumer` with a unique random group, `auto.offset.reset=latest`, `enable.auto.commit=false` — it tails backup topics continuously for liveness, never commits offsets, and is the cover when the primary collector goes silent. `FailoverController` is now state-only; it no longer owns its consumer. When the primary fails, `RecordHandler` picks records out of the backup tail under `BACKUP_PREFIX` rules.

**Wiring is order-dependent.** Read `collector/src/main/java/com/cryptolake/collector/Main.java` and `writer/src/main/java/com/cryptolake/writer/Main.java` — both have a documented wiring order in their class javadoc. `DepthSnapshotResync` ↔ `DepthStreamHandler` is a deliberate forward-reference cycle (depth handler triggers resync on `pu`-chain breaks; resync sets sync points on the handler). Don't try to flatten it.

**Raw fidelity goes through `RawFrameCapture`.** Bytes are captured *before* JSON parsing, hashed (`raw_sha256`), and stored as `raw_text` in the envelope. Anything that re-serializes a parsed JSON object before persisting violates invariant 1.4.1.1.

**Restart-gap classification.** Three signals decide whether a gap is `planned`, `host_reboot`, `clean_shutdown`, etc.: PG `component_runtime` rows, the host lifecycle ledger at `${LIFECYCLE_LEDGER_PATH:-/data/.cryptolake/lifecycle/events.jsonl}`, and the host's `boot_id`. `scripts/cryptolake-maintenance.sh stop|restart` writes a maintenance intent to *both* PG and the ledger before stopping services; that's how the writer learns a restart was planned. Skipping the wrapper means the writer will (correctly) classify the resulting gap as unplanned.

**Half-open WebSockets.** A known fstream bug silently drops some subscriptions on reconnect. `StreamHeartbeatEmitter` is the watchdog — every enabled (symbol, stream) pair must produce traffic within an idle window or a synthetic heartbeat-gap is emitted. The collector uses one consolidated WS connection (not the older public+market split).

**Topic prefix is the redundancy switch.** Primary collector: `topicPrefix=""`. Backup collector: `TOPIC_PREFIX=backup.` env, `COLLECTOR_ID=binance-collector-backup`. Same image, same code, same config file — only env differs. `scripts/setup-backup-topics.sh` sets 30-min retention on all `backup.*` topics post-startup.

**Verify CLI is picocli with five subcommands.** `verify`, `manifest`, `mark-maintenance`, `gaps`, `integrity`. `Main.java` overrides `System.out` to UTF-8 with `\n` line endings — keep that behavior even though the original byte-diff parity fixtures are no longer in the repo.

## Modules at a glance

| module | role | depends on |
|---|---|---|
| `common` | config (YAML), envelope codec, kafka helpers, health server, logging, identity | — |
| `collector` | WS supervisor, capture, gap emitter, snapshot resync, OI poller, lifecycle journal | common |
| `writer` | dual-consumer loop, buffer manager, zstd appender, file rotator, recovery, PG state | common |
| `backfill` | scheduled REST/CSV backfill (binance.vision for trades) | common |
| `consolidation` | hourly seal/consolidation; also hosts `ChaosVerifyIT` test harness | common, verify |
| `verify` | `cryptolake-verify` CLI; archive integrity, gap analysis, manifest | common |

## Audit quad (file reality vs recorded state)

Four `audit` subcommands cross-check what is on disk against what PG / ledger / kafka journals / manifests recorded. `audit files` walks the archive (gap envelopes + missing hours + sequence-ID continuity). `audit state` walks the five persistent state sinks, fanning each event out per `(symbol, stream)` so the columns line up with the file side. **`audit reconcile`** is the unified view: it runs both sides and matches each file gap against a candidate state cause using window-overlap (±2s) + a caused-by table (e.g. `collector_restart` explains `ws_disconnect`, `pu_chain_break`, `session_seq_skip`, `recovery_depth_anchor`). Each row gets a status — `EXPLAINED`, `UNEXPLAINED`, or `orphan`. `audit backfill` then gates a real backfill on `UNEXPLAINED` being empty; on divergence it POSTs an `AuditDivergence` alert to `${CRYPTOLAKE_ALERTMANAGER_WEBHOOK:-http://127.0.0.1:9093/api/v2/alerts}` and writes a `divergence-<ts>.json` report. See `docs/superpowers/plans/2026-05-11-audit-trio.md`.

```bash
# Four audit entry points
./scripts/audit-files.sh     --day 2026-05-11 --symbol btcusdt --stream depth --json
./scripts/audit-state.sh     --day 2026-05-11 --symbol btcusdt --json
./scripts/audit-reconcile.sh --day 2026-05-11 --symbol btcusdt --stream depth,trades,...  # headline view
./scripts/audit-backfill.sh  --day 2026-05-11 --symbol btcusdt --stream trades  # gated backfill
```

## Conventions worth knowing

- **Java 21, virtual threads.** Both services use `Executors.newVirtualThreadPerTaskExecutor()` for I/O loops. SIGTERM hooks must run on a platform thread (JVM requirement) — that one exception is intentional.
- **Single `ObjectMapper` per service.** Constructed via `EnvelopeCodec.newMapper()` and threaded through wiring. Don't create ad-hoc Jackson mappers.
- **Manual offset commit.** `enable.auto.commit=false` everywhere. The writer commits only after the corresponding records are durably flushed and sidecars written.
- **Defer QA on Python-era patches.** Per project memory, chaos/extended runs are skipped on Python patches during the port; the Java gates re-verify. The Python tree is gone, so this mostly affects how to read older commits.
- **Spotless is enforced.** Run `./gradlew spotlessApply` before committing. `removeUnusedImports()` is intentionally omitted from the spotless config because google-java-format already does it (build.gradle.kts comments explain).
