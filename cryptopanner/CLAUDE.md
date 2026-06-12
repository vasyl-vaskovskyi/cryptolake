# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

CryptoPanner is a raw-first, multi-region capture pipeline for Binance USD-M Futures market data. The end product is an append-only, byte-faithful collection of per-node hourly sealed files in IONOS S3-compatible object storage. Java 21, multi-module Gradle. Clean-room successor to CryptoLake (v1) — no shared code, configuration, or data.

Authoritative spec: `docs/00-master-spec.md` (reviewed end-to-end 2026-06-12). The make-before-break hot-swap and WS rotation mechanism lives in `docs/superpowers/specs/2026-06-09-collector-hot-swap-and-ws-rotation-design.md` (same review date). The invariants in master spec §3 are non-negotiable: raw-payload fidelity, durability before acknowledgement, manifest-as-source-of-truth, per-node independence, idempotent upload, no silent data loss.

## Build & test

Use the Gradle wrapper. Spotless (google-java-format 1.23.0) runs on every build; CI fails on unformatted code. `removeUnusedImports()` is intentionally omitted from the Spotless config — google-java-format already handles it (matches CryptoLake parent convention).

```bash
./gradlew build                                    # compile + spotlessCheck + tests, all modules
./gradlew :collector:test                          # one module
./gradlew :collector:test --tests "*ClassNameTest*"
./gradlew :collector:test --tests "*ClassNameTest.methodName"
./gradlew spotlessApply                            # auto-format before commit
./gradlew :verify:installDist                      # produces verify/build/install/verify/bin/verify
```

## Running locally

Master spec §14.b describes the local end-to-end stack. One command brings it up:

```bash
make dev-up                                # docker compose: mock binance, minio, all 7 services
make dev-down
docker compose logs -f cryptopanner-collector-a
```

The mock Binance WS replays captured frame fixtures from `tests/fixtures/binance/`. MinIO stands in for IONOS S3. The Monitor's dashboard is at `http://localhost:9200/dashboard`.

## Chaos suite

Each `tests/chaos/NN_*.sh` spins up an isolated `cryptopanner-chaos-NN` compose project, injects a fault, then asserts `cryptopanner-verify` exits 0 with `ERRORS=0` and the expected gap/event annotations in the manifest. The 18-scenario catalogue is in master spec §14.e.

```bash
bash tests/chaos/01_collector_active_crash.sh         # one scenario
make chaos-all                                         # all scenarios
./gradlew :verify:test --tests "*ChaosVerifyIT*"       # JUnit harness wraps all scenarios
```

## Modules at a glance

| module | role | depends on |
|---|---|---|
| `common` | config (YAML), envelope codec, S3 client helpers, logging, identity | — |
| `collector` | WS supervisor, capture, slot-templated (`@a`/`@b`), in-process daily rotation, hot-swap mechanics | common |
| `sealer` | hourly merge + REST backfill (gap-fillable streams) + manifest generation | common |
| `uploader` | S3 upload with manifest-last ordering and infinite backoff | common, S3 SDK |
| `agent` | Node Agent HTTP server: `/status`, `/metrics` (OpenMetrics), `/restart/<comp>`, `/rotation/trigger` | common |
| `monitor` | Cross-node monitoring, dashboard, alert escalation, restart orchestration | common |
| `verify` | `cryptopanner-verify` audit/integrity CLI; subcommands `verify`, `manifest`, `gaps`, `integrity` | common, picocli |

## Architecture conventions worth knowing

- **Slot-templated Collector.** Two systemd units (`cryptopanner-collector@a.service`, `@b.service`); `active-slot` file at `/data/cryptopanner/deploy/active-slot` selects production. Slots alternate freely with each deploy. See master spec §6.b and design doc §3.3.
- **Make-before-break overlap.** Both JAR deploys (operator-driven) and daily WS rotations (auto, ~daily before Binance's 24h cliff) use the same overlap protocol with `EquivalenceChecker` + `OverlapMerger`. See design doc §4 (Variant A) and §5 (Variant B).
- **`/data/cryptopanner/.fs-heavy.lock`** serializes hourly merge (Sealer), deploy promote, and rotation cutover so they never contend for disk I/O. See master spec §9.d and design doc §4.4 / §5.2.
- **Server-event-time bucketing.** Frames are placed into minute files by the Binance-set `E` (or `T` for `trade`/`aggTrade`) timestamp, not local receive time. See master spec §8.c and design doc §3.4.
- **Manifest-last upload ordering.** S3 upload sequence is `.jsonl.zst` → `.sha256` → `.manifest.json`. Consumers treat manifest presence as the durability signal. See master spec §9.c and §10.c.

## Code conventions

- **Java 21, virtual threads.** Use `Executors.newVirtualThreadPerTaskExecutor()` for I/O loops. SIGTERM hooks run on a platform thread (JVM requirement). That one exception is intentional.
- **Single `ObjectMapper` per service.** Constructed in `common` via `EnvelopeCodec.newMapper()` and threaded through wiring. Don't create ad-hoc Jackson mappers.
- **`CLOCK_MONOTONIC` for grace-window timers.** Wall clock is only used to identify which minute a frame goes to; sealing timers use `System.nanoTime()`. See design doc §3.4.
- **Spotless enforced.** Run `./gradlew spotlessApply` before committing.

## Where to look first

- New to the project? Read `docs/00-master-spec.md` end-to-end. It's ~700 lines but covers the whole system.
- Implementing hot-swap? Read design doc §3 (architecture) then §4 (Variant A) then §5 (Variant B).
- Implementing the chaos suite? Master spec §14 lists the 18 scenarios with expected outcomes.
- Touching the manifest? Master spec §10.d shows the full schema with `manifest_schema_version: 1`.
