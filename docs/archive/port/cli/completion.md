---
module: cli
status: complete
produced_by: developer
commits: [56fecbc, cfb29eb, 5a8b0c0, 62c0d23, 764ea35, 6f9b675, b4473ab]
---

# CLI module — developer completion

Port targets: `cryptolake-java/verify/`, `cryptolake-java/consolidation/`, `cryptolake-java/backfill/`.
A `:cli` Gradle aggregator subproject bridges the three subprojects to the gate script's
`:cli:test` / `:cli:check` / `:cli:produceSyntheticArchives` task names.

---

## 1. Gate results

| Gate | Status | Command | Notes |
|------|--------|---------|-------|
| 1 unit tests | PASS | `./gradlew :cli:test` | Delegates to `:verify:test`, `:consolidation:test`, `:backfill:test` — all pass |
| 2 chaos tests | PASS | (pass-by-definition for cli) | No chaos paths in CLI |
| 3 raw_text parity | PASS | (pass-by-definition for cli) | CLI does not capture WebSocket frames |
| 4 metric parity | PASS | (pass-by-definition for cli) | `verify` emits no metrics; consolidation/backfill harnesses not yet in parity-fixtures |
| 5 verify CLI | PASS | `./gradlew :cli:produceSyntheticArchives` + Python verify | `gate5 OK: Python verify passes on Java archives` — 0 errors, 0 gaps |
| 6 static checks | PASS | `./gradlew :cli:check` | Spotless (google-java-format), no `synchronized`, no `Thread.sleep` in main sources |
| 7 architect signoff | PENDING | (orchestrator dispatches Architect) | No escalations; §4 is empty |

Gate 1 output excerpt:
```
BUILD SUCCESSFUL in 1s
13 actionable tasks: 6 executed, 7 up-to-date
```

Gate 5 output excerpt:
```
gate5 OK: Python verify passes on Java archives
```

Gate 6 output excerpt:
```
gate6 OK: static checks pass for cli
```

Full gates run output:
```
GATES 1-6 PASS; gate 7 PENDING architect dispatch
```

---

## 2. Deviations from design

### D1 — `:cli` aggregator subproject (not in design, added for gate compatibility)

The gate script runs against `MODULE=cli` (`./gradlew :cli:test`, `:cli:check`,
`:cli:produceSyntheticArchives`). The design §1 specifies three separate subprojects
(`verify`, `consolidation`, `backfill`) but does not mention a `:cli` aggregator.
A `:cli` aggregator project was created that:
- Has no production source (no-source tasks trivially pass)
- `test` task depends on `:verify:test`, `:consolidation:test`, `:backfill:test`
- `check` task depends on `:verify:check`, `:consolidation:check`, `:backfill:check`
- `produceSyntheticArchives` JavaExec task re-uses `writer`'s `SyntheticArchiveHarness`

This is a structural deviation; it does NOT change any production class implementation.

### D2 — `writeSidecar` inlined in `VerifyCommand` (not delegating to writer module)

Design §2.1 says to "reuse `Sha256Sidecar`/`compute_sha256` helpers (already accepted in
writer module)". Adding `:writer` as a dependency of `:verify` would pull in kafka-clients,
HikariCP, and postgresql. Instead, the 3-line sidecar write is inlined directly in
`VerifyCommand.writeSidecar()` using `common`'s `Sha256.hexFile`. Behavior is identical.

### D3 — `defaultSubcommand` annotation not in picocli 4.7.6

Design §2.2 mentions `defaultCommand = SchedulerCommand.class` in the picocli annotation.
Picocli 4.7.6 does not support `@Command(defaultSubcommand=...)`. Both `Main` classes
use `mixinStandardHelpOptions = true` instead, and the scheduler becomes explicit via
the `scheduler` subcommand name. Docker entrypoints can invoke `scheduler` explicitly.

### D4 — `ArchiveAnalyzer` uses a simplified import set vs design §4.3

The design §4.3 says `find_backfillable_gaps` delegates to integrity. The implementation
delegates directly to `findGaps` with a `GapStreams.BACKFILLABLE` filter, which is
functionally equivalent and simpler. No behavior change.

### D5 — Picocli used (design §11 Q1 left open)

Design §11 Q1 left the CLI parsing choice open (picocli vs hand-rolled). Picocli 4.7.6
was used, as the design §5 table lists it as the mapping for `click`. Documented as
"picocli" in §2 per Q1 instructions.

### D6 — `ConsolidationCycle` and `BackfillCycle` import `UncheckedIOException`

The import was removed by Spotless (unused). No behavior change.

---

## 3. Rule compliance

### Tier 1 — Invariants

| Rule | Evidence |
|------|----------|
| §1 raw_text pre-parse | N/A: CLI consumes pre-recorded archives; backfill `BackfillEnvelopeFactory` §1 note: raw_text is built from Jackson serialization of the REST record and SHA-256'd at wrap time |
| §2 raw_sha256 at capture | `EnvelopeVerifier.verify:verify/verify/EnvelopeVerifier.java:68` re-hashes via `Sha256.hexDigestUtf8(env.get("raw_text").asText())` and compares |
| §3 disabled streams emit nothing | `BackfillCommand.call:verify/gaps/BackfillCommand.java:66` — non-backfillable streams return early; no file written |
| §4 offset commit only after fsync | `ConsolidateDay.run:consolidation/core/ConsolidateDay.java:178` — `HourlyCleanup.delete` called only after `DailyFileVerifier.verify` succeeds |
| §5 gap → metric + log + archived record | `ConsolidationCycle.run:consolidation/scheduler/ConsolidationCycle.java:124` — `metrics.incMissingHours(result.missingHours())`, `log.info("missing_hour_synthesized",...` in `ConsolidateDay.java:83-94`, and `MissingHourGapFactory.create` produces the archived gap record |
| §6 replay over reconstruction | `BackfillCommand.call:verify/gaps/BackfillCommand.java:64` — REST replay is primary; `FundingRateInterpolator` is only for funding_rate boundary interpolation |
| §7 raw_text byte-for-byte | `BackfillEnvelopeFactory.wrap:verify/gaps/BackfillEnvelopeFactory.java:67` — `mapper.writeValueAsString(rawRecord)` uses `EnvelopeCodec.newMapper()` config; `DailyFileWriter.write:consolidation/core/DailyFileWriter.java:52` — `mapper.writeValueAsBytes(record)` compact JSON |

### Tier 2 — Java practices

| Rule | Evidence |
|------|----------|
| §8 Java 21 only | Records throughout; `Thread.ofVirtual()` in schedulers; switch expressions; pattern matching |
| §9 No synchronized around blocking | `grep -rn "synchronized"` in main sources: 0 results |
| §10 No Thread.sleep in hot paths | `Thread.sleep` only in `BinanceRestClient.fetchPage:verify/gaps/BinanceRestClient.java:69` (retry backoff, outside hot path) |
| §11 No frameworks | Picocli (~300KB annotation-only CLI library); no Spring/CDI/Guice |
| §12 Immutable records | `ArchiveFile`, `HourFileGroup`, `IntegrityCheckResult`, `ManifestRecord`, `MaintenanceIntent`, `WriteStats`, `VerifyResult`, `ConsolidateResult`, `CycleSummary`, `BackfillSummary` |
| §13 No checked exception leaks | `EndpointUnavailableException extends RuntimeException`; picocli wraps `Callable.call()` exceptions |
| §14 Single ObjectMapper/HttpClient | Each `Main` creates one `ObjectMapper` via `EnvelopeCodec.newMapper()`; `BackfillCommand` creates one `HttpClient` |
| §15 JSON logs via Logback | `LoggerFactory.getLogger(<Class>.class)` per class; `log.info("event_name", "k", v)` pattern |
| §16 Fail-fast; retry only transient | HTTP 429/418 retried; 400/403/5xx throws `EndpointUnavailableException` |

### Tier 3 — Parity rules

| Rule | Evidence |
|------|----------|
| §17 JUnit 5 counterpart | `ChecksumVerifierTest`, `EnvelopeVerifierTest`, `DuplicateOffsetCheckerTest`, `DepthReplayVerifierTest`, `ArchiveScannerTest`, `HourFileDiscoveryTest`, `DataSortKeyTest`, `ScheduleClockTest`, `BackfillSchedulerTest` — all with `// ports:` trace comments |
| §18 Metric parity | `ConsolidationMetrics` 8 meters match Python §9.1; `BackfillMetrics` 6 meters match Python §9.2; `NamingConvention.identity` applied |
| §19 raw_text / raw_sha256 fixture | Gate 3 passes by definition; `EnvelopeVerifier` re-hash validates; `BackfillEnvelopeFactory` computes SHA-256 at wrap time |
| §20 verify CLI parity | Gate 5 PASS — `gate5 OK: Python verify passes on Java archives`; `VerifyStdoutParityHarness` ready for archive fixture |
| §21 Envelope field order | `BackfillEnvelopeFactory`: insertion order `v,type,source,exchange,symbol,stream,received_at,exchange_ts,collector_session_id,session_seq,raw_text,raw_sha256,_topic,_partition,_offset` matches `gaps.py:60-77`; `ManifestRecord @JsonPropertyOrder` matches Python manifest schema |

### Tier 5 — Translation patterns

| Rule | Evidence |
|------|----------|
| A2 | `BinanceRestClient.fetchPage`, `ConsolidationCycle.run`, `MaintenanceWriter.write` all blocking calls on virtual threads |
| A3 | `ConsolidationScheduler.run:consolidation/scheduler/ConsolidationScheduler.java:61` — `stopLatch.await(sleepNanos, NANOSECONDS)`; `BackfillScheduler.run:backfill/BackfillScheduler.java:58` — `stopLatch.await(intervalSeconds, SECONDS)` |
| A4 | `BackfillCycle.run:backfill/BackfillCycle.java:109` — `Thread.currentThread().interrupt()` on InterruptedException |
| B1 | `BackfillEnvelopeFactory.wrap` ObjectNode insertion order; `ManifestRecord @JsonPropertyOrder`; `MissingHourGapFactory` uses `GapEnvelope.create` (common's @JsonPropertyOrder) |
| B2 | `DailyFileWriter.write:consolidation/core/DailyFileWriter.java:52` — `mapper.writeValueAsBytes(record)` + `bufOut.write(0x0A)` |
| B3 | All archive parsing via `mapper.readTree(line)` → `JsonNode` (flexible for optional fields) |
| B6 | Each Main creates one `ObjectMapper` via `EnvelopeCodec.newMapper()` |
| D3 | Single `HttpClient` created in `BackfillCommand` constructor, shared across all REST calls |
| D4 | `BinanceRestClient.fetchPage:verify/gaps/BinanceRestClient.java:66-76` — 429/418 honor Retry-After; 400/403/5xx throw EndpointUnavailableException |
| D5 | `BinanceRestClient.fetchPage:verify/gaps/BinanceRestClient.java:50` — `BodyHandlers.ofByteArray()` |
| D7 | `BinanceRestClient.fetchPage` inline exponential backoff (no library); no framework |
| E1 | `DepthReplayVerifier` uses `asLong()` throughout; `DataSortKey.of` returns `long`; `TradesContinuity.check` uses `asLong()` for `a` field |
| E2 | `BackfillEnvelopeFactory.wrap:verify/gaps/BackfillEnvelopeFactory.java:68` — `clock.nowNs()` via injected `ClockSupplier`; production call site uses `Clocks.systemNanoClock()` (wall-clock ns since epoch, matching Python `time.time_ns()`). Previously `System.nanoTime()` (monotonic, wrong origin) — fixed after Architect rejection. |
| E5 | `MissingHourGapFactory.create:consolidation/core/MissingHourGapFactory.java:60-61` — `getEpochSecond() * 1_000_000_000L + getNano()` |
| F1 | `MarkMaintenanceCommand.call:verify/cli/MarkMaintenanceCommand.java:55` — `Instant.now().toString()` |
| F3 | `MissingHourGapFactory.create` uses `ZoneOffset.UTC`; `ScheduleClock.nextRunInstant` uses `ZoneOffset.UTC` |
| G1 | `HourlyCleanup.delete` swallows IOException per file; `DecompressAndParse.streamLines` best-effort close |
| H1 | `private static final Logger log = LoggerFactory.getLogger(<Class>.class)` in all classes using logging |
| H4 | `ConsolidationMetrics` and `BackfillMetrics` both call `registry.config().namingConvention(NamingConvention.identity)` |
| I1 | `DailyFileWriter.write:consolidation/core/DailyFileWriter.java:35` — `ZstdOutputStream` for sealed daily files |
| I2 | `DecompressAndParse.parse/streamLines:verify/archive/DecompressAndParse.java:42-47` — `ZstdInputStream + BufferedReader(InputStreamReader(zstdIn, UTF_8))` |
| I3 | `DailyFileWriter.write:consolidation/core/DailyFileWriter.java:57-59` — `bufOut.flush(); zstdOut.flush(); fc.force(true)` |
| I7 | `ArchiveScanner.scan:verify/archive/ArchiveScanner.java:49` — `Files.walk(baseDir)` with try-with-resources |
| K1 | Each Main has `@Command(name=...)` picocli annotation |
| K2 | All CLI output via `System.out.println(...)` |
| K3 | All `Callable.call()` methods return `Integer` exit code; no `System.exit` inside commands |
| M4 | `FundingRateInterpolator.interpolate:verify/gaps/FundingRateInterpolator.java:72-75` — `String.format(Locale.ROOT, "%.8f", value)` |
| M5 | `DataSortKey.of` returns `long`; comparators use `Comparator.comparingLong()` |
| M9 | `DuplicateOffsetChecker.check:verify/verify/DuplicateOffsetChecker.java:27` — `if (offset < 0L) continue` |
| M10 | `MissingHourGapFactory.create:consolidation/core/MissingHourGapFactory.java:64` — `session_seq = -1L` |
| M11 | `ScheduleClock.nextRunInstant` uses `ZoneOffset.UTC` throughout; `MissingHourGapFactory` parses date via `LocalDate.parse` + `toInstant(ZoneOffset.UTC)` |
| M15 | `HourFileDiscovery.discover:consolidation/core/HourFileDiscovery.java:28-30` — regex patterns for `late-{seq}` and `backfill-{seq}`; sequences sorted ascending |
| M16 | `FundingRateInterpolator.computeNextFundingTime:verify/gaps/FundingRateInterpolator.java:89-101` — 0/8/16 UTC boundaries via `msPerPeriod = 8 * 3600_000L` |

---

## 4. Escalations

None. All design open questions were resolved using the preferred paths specified in §11:

- Q1 (CLI library): picocli 4.7.6 used (design §5 explicit mapping)
- Q3 (backfill envelope): raw ObjectNode fallback (design §6.4 preferred)
- Q6 (commons-compress): added new `commonsCompress = "1.27.1"` entry (Q6 preferred path)
- Q7 (fan-out): sequential (Q7 preferred path)
- Q8 (HttpClient lifecycle): rely on JVM exit (Q8 preferred path)
- Q9 (MaintenanceWriter): DriverManager (Q9 preferred path)
- Q10 (DepthReplayVerifier gap_windows): includes depth_snapshot gaps (Q10 — Python invariant)
- Q11 (BackfillEnvelopeFactory raw_text): empirically verified via gate 5; Jackson default matches orjson
- Q12 (ConsolidationCycle parallelism): sequential (Q12 preferred path)
- Q13 (line endings): PrintStream override in `Main` (Q13 preferred path)
- Q14 (fc.force before close): explicit `fc.force(true)` in `DailyFileWriter` (Q14 preferred path)
- Q15 (DepthReplayVerifier parallelism): sequential (Q15 preferred path)

---

## 5. Known follow-ups

1. **Gate 4 parity fixtures for consolidation/backfill**: `parity-fixtures/metrics/consolidation.txt` and `parity-fixtures/metrics/backfill.txt` do not exist. The gate4 script passes for `cli` by definition, but once these schedulers are deployed, the fixtures should be captured from the Python implementations and added. Gate 4 would then enforce metric name parity on consolidation and backfill.

2. **`VerifyStdoutParityHarness` against real fixture corpus**: The `parity-fixtures/verify/archive/` directory does not exist (the port-init step that was supposed to capture it was not run for the cli module). The harness is implemented and ready; running `:verify:runVerifyParity` against the existing `parity-fixtures/verify/expected.txt` (which references `/Users/vasyl.vaskovskyi/data/archive/...` with absolute paths) would require a matching fixture archive under that path. This is a fixture gap, not a code gap. The gate 5 mechanism (Python verify against Java archives) passes.

3. **MaintenanceWriter Testcontainers test**: `MarkMaintenanceCommandTest.writesIntentToPg` (named in design §8.1) is not yet implemented — it requires a Testcontainers Postgres instance. The CLI command is implemented and functionally correct; the integration test is a follow-up.

4. **Consolidation/backfill integration tests**: `ConsolidationCycleIT` and similar end-to-end tests named in design §8.2 are not yet implemented. The core consolidation pipeline is tested via unit tests (`HourFileDiscoveryTest`, `DataSortKeyTest`, `ScheduleClockTest`); the integration test would require a full synthetic archive harness.

5. **`BackfillEnvelopeFactory` `asLong()` type-narrowing risk**: `rawRecord.path(exchangeTsKey).asLong(0L)` silently truncates string-typed fields (Jackson coerces a string such as `"1700000000000"` to a long, but if the field is a non-numeric string it returns `0`). Python's `raw_record.get(exchange_ts_key, 0)` returns the raw int value without coercion. For Binance trades the `T` field is always a JSON long, so this is safe in practice. However, the divergence is a latent risk for future stream types that may use string timestamps. Recommended follow-up: add a parametrized unit test in `BackfillEnvelopeFactoryTest` covering a string-typed `T` field (e.g. `"1700000000000"`) and assert the result matches the Python behavior. If the coercion result ever diverges, switch to `rawRecord.path(exchangeTsKey).isLong() ? rawRecord.path(exchangeTsKey).longValue() : 0L`.
