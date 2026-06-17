# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

CryptoPanner is a raw-first, multi-region capture pipeline for Binance USD-M Futures market data. The end product is an append-only, byte-faithful collection of per-node hourly sealed files in IONOS S3-compatible object storage. Java 26, multi-module Maven. Clean-room successor to CryptoLake (v1) — no shared code, configuration, or data.

Authoritative spec: `docs/00-master-spec.md` (reviewed end-to-end 2026-06-12). The make-before-break hot-swap and WS rotation mechanism lives in `docs/superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md` (same review date). The invariants in master spec §3 are non-negotiable: raw-payload fidelity, durability before acknowledgement, manifest-as-source-of-truth, per-node independence, idempotent upload, no silent data loss.

## Build & test

Use Maven 3.9+. Spotless (google-java-format 1.35.0) is bound to the `verify` phase; CI fails on unformatted code. `removeUnusedImports()` is intentionally omitted from the Spotless config — google-java-format already handles it (matches CryptoLake parent convention).

```bash
mvn verify                                         # compile + tests + spotless:check, all modules
mvn -pl collector test                             # one module
mvn -pl collector test -Dtest=ClassNameTest        # one test class
mvn -pl collector test -Dtest=ClassNameTest#method # one test method
mvn spotless:apply                                 # auto-format before commit
mvn -pl verify -am package                         # produces verify/target/install/bin/verify
```

`-am` ("also make") includes the verify module's upstream dependencies (`common`) in the reactor build.

## Running locally

Master spec §14.b describes the local end-to-end stack. One command brings it up:

```bash
make dev-up                                # docker compose: mock binance, minio, all 7 services
make dev-down
docker compose logs -f cryptopanner-collector-a
```

The mock Binance WS replays captured frame fixtures from `tests/fixtures/binance/`. MinIO stands in for IONOS S3. The Monitor's dashboard is at `http://localhost:9200/dashboard`.

## Tests

Test layers (see master spec §14 for detail):

- **Unit + integration** — everything except the soak. `mvn verify` runs the full suite. §12 failure modes are covered here using TinyWsServer + captured fixtures from `tests/fixtures/binance/` + an in-memory S3 stub (or Testcontainers MinIO where real S3 semantics matter, or `ProcessBuilder`-spawned child JVMs for cross-process semantics like `flock(2)` and SIGKILL during `write(2)`).
- **Convention:** test class names reference the §12 entry they cover (e.g. `WsDisconnectIT` for §12.b, `Rest429StormUnitTest` for §12.l). The spec ↔ test mapping is greppable, not spec-enumerated.
- **Soak** — one real-environment test, `tests/soak/run.sh`. Brings up the full `make dev-up` stack with two collector nodes in parallel, runs for ≥ 5 min at target load, triggers one synthetic WS rotation mid-run. Naturally covers rotation happy path (§8.b.2) and multi-node independence (§3.d) at real-env. The pre-release variant in spec §14.g extends to 2 h wall-clock / 24 h simulated.

```bash
mvn verify                          # unit + integration; runs every PR
bash tests/soak/run.sh              # the one real-env test; runs nightly
```

## Modules at a glance

| module | role | depends on |
|---|---|---|
| `common` | config (YAML), envelope codec, S3 client helpers, logging, identity | — |
| `collector` | WS supervisor, capture, slot-templated (`@a`/`@b`), in-process daily rotation, hot-swap mechanics | common |
| `sealer` | hourly merge + REST backfill (gap-fillable streams) + manifest generation | common |
| `uploader` | S3 upload with manifest-last ordering and infinite backoff | common, AWS S3 SDK |
| `agent` | Node Agent HTTP server (`/status`, `/metrics`, `/restart`, `/rotation/trigger`) | common |
| `monitor` | Cross-node monitoring + alerting + restart orchestration | common |
| `verify` | `cryptopanner-verify` audit/integrity CLI; subcommands `verify`, `manifest`, `gaps`, `integrity` | common, picocli |

Each app module's `package` phase runs the `appassembler-maven-plugin` to produce `<module>/target/install/{bin/<name>, repo/*.jar}` — the Maven equivalent of an `installDist` layout. The systemd unit's `ExecStart` on the node points at the `bin/<name>` launcher script.

## Architecture conventions worth knowing

- **Slot-templated Collector.** Two systemd units (`cryptopanner-collector@a.service`, `@b.service`); `active-slot` file at `/data/cryptopanner/deploy/active-slot` selects production. Slots alternate freely with each deploy. See master spec §6.b and design doc §3.3.
- **Make-before-break overlap.** Both JAR deploys (operator-driven) and daily WS rotations (auto, ~daily before Binance's 24h cliff) use the same overlap protocol with `EquivalenceChecker` + `OverlapMerger`. See design doc §4 (Variant A) and §5 (Variant B).
- **`/data/cryptopanner/.fs-heavy.lock`** serializes hourly merge (Sealer), deploy promote, and rotation cutover so they never contend for disk I/O. See master spec §9.d and design doc §4.4 / §5.2.
- **Server-event-time bucketing.** Frames are placed into minute files by the Binance-set `E` (or `T` for `trade`/`aggTrade`) timestamp, not local receive time. See master spec §8.c and design doc §3.4.
- **Manifest-last upload ordering.** S3 upload sequence is `.jsonl.zst` → `.sha256` → `.manifest.json`. Consumers treat manifest presence as the durability signal. See master spec §9.c and §10.c.

## Code conventions

- **Java 26, virtual threads.** Use `Executors.newVirtualThreadPerTaskExecutor()` for I/O loops. SIGTERM hooks run on a platform thread (JVM requirement). That one exception is intentional.
- **Single `ObjectMapper` per service.** Constructed in `common` via `EnvelopeCodec.newMapper()` and threaded through wiring. Don't create ad-hoc Jackson mappers.
- **`CLOCK_MONOTONIC` for grace-window timers.** Wall clock is only used to identify which minute a frame goes to; sealing timers use `System.nanoTime()`. See design doc §3.4.
- **Spotless enforced.** Run `mvn spotless:apply` before committing.
- **TDD for new features.** Test-first: failing test → run (fails) → minimal implementation → run (passes) → commit. Strict for feature work. For bug fixes, write a regression test that reproduces the bug, then fix. Refactors keep all existing tests green. Exemptions: the soak, YAML configs, build scripts (`pom.xml`), docs, fixture captures. See master spec §14.k for the full policy and `docs/superpowers/plans/2026-06-14-walking-skeleton.md` for the canonical task shape (each task = failing test → impl → green → commit). Future plan files MUST follow the same shape.

## Where to look first

- New to the project? Read `docs/00-master-spec.md` end-to-end. It's ~700 lines but covers the whole system.
- Implementing hot-swap? Read design doc §3 (architecture) then §4 (Variant A) then §5 (Variant B).
- Writing tests? Master spec §14 lays out the test layers; §14.k pins the TDD discipline; `tests/fixtures/binance/` holds captured frame fixtures; the walking-skeleton plan demonstrates the TDD task shape end-to-end.
- Touching the manifest? Master spec §10.d shows the full schema with `manifest_schema_version: 1`.
