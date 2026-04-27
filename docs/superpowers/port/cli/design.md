---
module: cli
status: approved
produced_by: architect
based_on_mapping: 1d314615d1cc4900b2781325c82b51db9e1a447f
---

# CLI — Java design

Port targets: three Gradle subprojects already scaffolded under `cryptolake-java/`:

- `cryptolake-java/verify/`        → package `com.cryptolake.verify`        (verify + gaps + integrity)
- `cryptolake-java/consolidation/` → package `com.cryptolake.consolidation` (consolidation_scheduler + consolidate library)
- `cryptolake-java/backfill/`      → package `com.cryptolake.backfill`      (backfill_scheduler)

Per spec §1.1 line 51: "verify/ — :verify CLI app (gaps/integrity)". Python's single `src/cli/` package fans out to **three Java apps with three `Main` classes**, matching the existing Dockerfile-per-service convention. The `consolidate.py` library code is reused by the consolidation scheduler at runtime AND by `verify` integration tests; it lives in `consolidation` and is depended on (`implementation(project(":consolidation"))`) by `verify`.

Java 21, virtual-thread concurrency, no framework. Picocli (4.7.6) for CLI parsing — Tier 2 §11 explicitly allows libraries; picocli is annotation-driven CLI parsing only, not a DI/IoC framework. All three apps reuse the accepted `com.cryptolake.common.*` artifacts (envelopes, codecs, `Sha256`, `JsonlReader`, `TopicNames`, config records, Hibernate-Validator wiring) and the writer's `Sha256Sidecar`/`compute_sha256` helpers (already accepted in writer module).

**Tier 1 invariant focus for this module**:
- §1, §2 are validated (re-hashed and compared) by `verify_envelopes`. CLI does not capture `raw_text`; it asserts byte-identity round-trip via `Sha256.hexDigestUtf8(raw_text)`.
- §5 is honored by `consolidate.synthesizeMissingHourGap` (every missing hour emits a `missing_hour` gap envelope into the daily file) and by both schedulers updating Prometheus counters + structured logs alongside.
- §6 is honored by `gaps.backfill` preferring REST replay over interpolation; interpolation is reserved for `funding_rate` only (Python `_interpolate_funding_rate_gap`) and is gated behind explicit gap-window boundaries — every other stream is either backfilled from REST or reported as unrecoverable.
- §7 is honored by `EnvelopeCodec.toJsonBytes(...)` (accepted common). Daily-file writes preserve `raw_text` byte-for-byte; consolidate's stream-mode passthrough copies pre-serialized bytes (Tier 5 B2 / I1 `bytes`-only path).

**Tier 3 §20 / Gate 5 byte-identity invariant**: the Java `cryptolake-verify verify` command's stdout MUST be byte-identical to the Python `cryptolake verify` for the same archive set. This is the single source of truth that gate 5 of writer / common / collector also depends on. Section §6.7 (Verify-stdout contract) and §10 (Tier 3 §20) describe the load-bearing rules; §8 names the harness that enforces it (`VerifyStdoutParityHarness`); §11 raises an open question about fixture refresh.

---

## 1. Package layout

### 1.1 `cryptolake-java/verify/` — `:verify` app

```
com.cryptolake.verify
├── Main                                      # picocli root + dispatch; mainClass.set("com.cryptolake.verify.Main")
├── cli/
│   ├── VerifyCommand                         # @Command(name="verify")     ← gate-5 stdout authority
│   ├── ManifestCommand                       # @Command(name="manifest")
│   └── MarkMaintenanceCommand                # @Command(name="mark-maintenance")
├── archive/
│   ├── ArchiveScanner                        # rglob hour-*.jsonl.zst|late-*|backfill-*; relative-path filtering
│   ├── ArchiveFile                           # record(Path path, String exchange, String symbol, String stream, String date)
│   └── DecompressAndParse                    # decompress one .jsonl.zst → List<JsonNode> (typed-as-needed)
├── verify/
│   ├── ChecksumVerifier                      # ports verify_checksum
│   ├── EnvelopeVerifier                      # ports verify_envelopes (raw_sha256 re-hash + required fields)
│   ├── DuplicateOffsetChecker                # ports check_duplicate_offsets (skips _offset < 0)
│   ├── GapReporter                           # ports report_gaps (filters envelopes by type=="gap")
│   ├── DepthReplayVerifier                   # ports verify_depth_replay (per-symbol pu-chain validator)
│   └── ManifestGenerator                     # ports generate_manifest (no JSON re-ordering)
├── gaps/                                     # ports src/cli/gaps.py
│   ├── GapsCli                               # @Command(name="gaps", subcommands={Analyze.class, Backfill.class})
│   ├── AnalyzeCommand                        # @Command(name="analyze")
│   ├── BackfillCommand                       # @Command(name="backfill")
│   ├── ArchiveAnalyzer                       # ports analyze_archive (missing-hour detection)
│   ├── BackfillOrchestrator                  # backfill cycle: REST fetch + envelope wrap + write backfill-*.jsonl.zst
│   ├── BinanceRestClient                     # one-shot REST client (HttpClient + retry + 429/418 backoff)
│   ├── EndpointUnavailableException          # 400/403/5xx → unrecoverable
│   ├── BackfillEnvelopeFactory               # _wrap_backfill_envelope (data envelope, source="backfill")
│   ├── FundingRateInterpolator               # _interpolate_funding_rate_gap (8-decimal format strings)
│   ├── DeepIdBackfill                        # _fetch_by_id (ID-anchored backfill for trades)
│   ├── GapStreams                            # constants: BACKFILLABLE_STREAMS, NON_BACKFILLABLE_STREAMS, STREAM_TS_KEYS, ENDPOINT_WEIGHTS
│   └── KlineFetcher                          # mark/index/premium klines for funding_rate interpolation
├── integrity/                                # ports src/cli/integrity.py
│   ├── IntegrityCli                          # @Command(name="integrity", subcommands={CheckTrades, CheckDepth, CheckBookticker})
│   ├── CheckTradesCommand
│   ├── CheckDepthCommand
│   ├── CheckBooktickerCommand
│   ├── ContinuityChecker                     # shared: streaming dedup + per-stream walk
│   ├── TradesContinuity                      # _check_trades (a-id continuity)
│   ├── DepthContinuity                       # _check_depth (pu-chain)
│   ├── BooktickerContinuity                  # _check_bookticker (u backwards-jump only)
│   └── IntegrityCheckResult                  # record(int recordCount, List<Break> breaks)
├── maintenance/
│   ├── MaintenanceWriter                     # JDBC port of StateManager.create_maintenance_intent (best-effort PG)
│   └── MaintenanceIntent                     # record (carries the same fields as Python MaintenanceIntent)
└── harness/
    └── VerifyStdoutParityHarness             # gate-5 stdout-byte-identity harness; :verify:runVerifyParity Gradle task
```

### 1.2 `cryptolake-java/consolidation/` — `:consolidation` app

```
com.cryptolake.consolidation
├── Main                                      # picocli root: schedulerOnly default; subcommand `consolidate-day` for ad-hoc
├── scheduler/
│   ├── ConsolidationScheduler                # async port of consolidation_scheduler.main; daily run at 02:30 UTC default
│   ├── ScheduleClock                         # nextRunInstant(now, cfgHourUtc) helper (deterministic for tests)
│   ├── ConsolidationCycle                    # _run_consolidation_cycle: walks symbols/streams, calls consolidate_day
│   └── ConsolidationMetrics                  # 8 Prometheus meters for the scheduler
├── core/
│   ├── ConsolidateDay                        # consolidate_day: discover→merge→write→verify→cleanup
│   ├── HourFileDiscovery                     # discover_hour_files (regex sort by base/late/backfill/seq)
│   ├── HourFileGroup                         # record(Path base, List<Path> late, List<Path> backfill)
│   ├── HourMerger                            # merge_hour: parse all sources, sort data by stream natural-key, splice gaps by gap_start_ts
│   ├── DataSortKey                           # _data_sort_key: long via JsonNode.path("a"|"u").asLong(exchangeTs)
│   ├── DailyFileWriter                       # write_daily_file: zstd-stream output, FileChannel.force(true) on close
│   ├── DailyFileVerifier                     # verify_daily_file: SHA-256 + record count + ordering walk
│   ├── HourlyCleanup                         # cleanup_hourly_files: delete only after verify ok (Tier 1 §4 indirect)
│   ├── ManifestRecord                        # record matching Python {date}.manifest.json schema
│   └── MissingHourGapFactory                 # synthesize_missing_hour_gap (UTC hour boundaries, reason="missing_hour")
└── seal/
    ├── SealDailyArchive                      # seal_daily_archive: tar.zst pack of all stream daily files
    └── ConsolidateStreamToTar                # two-pass per-stream packer (memory-bounded)
```

### 1.3 `cryptolake-java/backfill/` — `:backfill` app

```
com.cryptolake.backfill
├── Main                                      # picocli root: scheduler default; subcommand `run-once` for ad-hoc
├── BackfillScheduler                         # backfill_scheduler.main: 6-hourly loop
├── BackfillCycle                             # _run_backfill_cycle: analyze + fetch + write
└── BackfillMetrics                           # 6 Prometheus meters for the scheduler
```

The `BackfillCycle` delegates the actual REST/interpolation work to `com.cryptolake.verify.gaps.BackfillOrchestrator` via a `implementation(project(":verify"))` Gradle dependency. This avoids duplicating ~600 lines of `gaps.py` logic across two apps. Per Python's structure, `backfill_scheduler.py` imports from `gaps.py` — the Java port mirrors that direction.

Total class count: **~50 production classes/records across the three apps** (verify ~30, consolidation ~16, backfill ~4). All sibling-design conventions preserved: records for data, final classes for behavior, sealed interfaces only when polymorphism exists.

---

## 2. Class catalog

Per class: purpose / type / public API / dependencies / thread-safety. Records are `public record`. Utilities are `final class` with private constructors.

### 2.1 `:verify` app

#### `com.cryptolake.verify.Main` — final class

- **Purpose**: `main(args)` entry point; constructs the picocli root and dispatches.
- **Public**: `static void main(String[] args)` (calls `System.exit(new CommandLine(new Main()).execute(args))`).
- **Dependencies**: `EnvelopeCodec.newMapper()` (single shared `ObjectMapper`, Tier 5 B6, Tier 2 §14); `HttpClient` (single instance for `gaps.backfill`, Tier 5 D3); construct lazily via DI in subcommand constructors.
- **Thread-safety**: top-level wiring; runs once on the platform thread.

The picocli root is itself annotated `@Command(name = "cryptolake-verify", subcommands = {VerifyCommand.class, ManifestCommand.class, MarkMaintenanceCommand.class, GapsCli.class, IntegrityCli.class})`. The runtime exit code is the picocli-returned `int` (Tier 5 K3).

#### `cli.VerifyCommand` — final class implements `Callable<Integer>`

- **Purpose**: ports `@cli.command() def verify(...)` — the gate-5 authoritative stdout source.
- **Public**: `Integer call() throws IOException` (return 0 = ok, 1 = errors).
- **Picocli options** (exact names match Python click options for argv parity):
  - `@Option(names = "--date", required = true)` `String date`
  - `@Option(names = "--base-dir", defaultValue = "<from EnvOverrides.HOST_DATA_DIR or DEFAULT_ARCHIVE_DIR>")` `String baseDir`
  - `@Option(names = "--exchange")` `String exchange` (nullable filter)
  - `@Option(names = "--symbol")` `String symbol`
  - `@Option(names = "--stream")` `String stream`
  - `@Option(names = "--full")` `boolean full`
  - `@Option(names = "--repair-checksums")` `boolean repairChecksums`
- **Algorithm** (mirrors `verify.py:290-376` line-for-line; output format byte-identical, see §6.7):
  1. `ArchiveScanner.scan(baseDir, date, exchange, symbol, stream)` returns the sorted file list.
  2. If empty: `System.out.println("No archive files found for date " + date + " in " + baseDir)`; return 0.
  3. For each `ArchiveFile`:
     - Print `"Verifying: " + relativePath` (matches Python click.echo formatting).
     - Optional `--repair-checksums` writes a missing sidecar via `Sha256Sidecar.write` (reused from writer).
     - `ChecksumVerifier.verify(...)` — append errors.
     - `DecompressAndParse.parse(path)` — append decompression failures to errors and `continue`.
     - `EnvelopeVerifier.verify(envelopes)`, `DuplicateOffsetChecker.check(envelopes)` per-file.
     - `GapReporter.collect(envelopes)` accumulates gaps.
     - If `--full`: also accumulate the envelope list cross-file.
  4. If `--full`: cross-file `DuplicateOffsetChecker.check` on the merged list; `DepthReplayVerifier.verify(depth, snapshot, gap)` on the merged list.
  5. Print final report (verbatim Python format — see §6.7).
  6. Return `errors.isEmpty() ? 0 : 1`.
- **Dependencies**: all `verify/*` collaborators + `ArchiveScanner` + `EnvelopeCodec`.
- **Thread-safety**: single-threaded execution (one `Callable.call()` invocation per process).

#### `cli.ManifestCommand` — final class implements `Callable<Integer>`

- **Purpose**: ports `@cli.command() def manifest(...)` — generates `manifest.json` per stream/date directory and prints the merged manifest as pretty JSON.
- **Public**: `Integer call() throws IOException`; options `--date`, `--base-dir`, `--exchange`.
- **Algorithm** (mirrors `verify.py:379-398`):
  1. `ManifestGenerator.generate(baseDir, exchange, date)` returns a `ManifestNode` (custom `LinkedHashMap`-backed `ObjectNode`).
  2. For each `{exchange}/{symbol}/{stream}/{date}/` directory, write `manifest.json` (UTF-8, 2-space indent, **trailing newline**).
  3. Print `"Written: " + manifestPath` per write.
  4. Print the merged manifest as `mapper.writerWithDefaultPrettyPrinter().writeValueAsString(manifest)`. **Watch-out**: Python's `json.dumps(..., indent=2)` emits `": "` after keys; Jackson's default pretty printer matches this; `JsonlWriter` is NOT used here.
- **Dependencies**: `ManifestGenerator`, `EnvelopeCodec`, `ObjectMapper`.

#### `cli.MarkMaintenanceCommand` — final class implements `Callable<Integer>`

- **Purpose**: ports `@cli.command("mark-maintenance")` — synchronous JDBC write of a maintenance intent row.
- **Public**: `Integer call() throws SQLException`; options `--db-url --scope --maintenance-id --reason --ttl-minutes`.
- **Algorithm**:
  1. Build `MaintenanceIntent` record with `now=Instant.now()` (Tier 5 F1 — `Instant.now().toString()` for ISO-8601 with `Z` suffix). `expires_at = now.plus(Duration.ofMinutes(ttlMinutes)).toString()`.
  2. `MaintenanceWriter.write(dbUrl, intent)` opens a JDBC connection, executes a single INSERT, closes. **No async wrapper** (Tier 5 A2; on a virtual thread, blocking JDBC is fine; for a one-shot CLI process, even a platform thread is fine).
  3. Print `"Maintenance intent recorded: <id> (scope=<scope>, ttl=<ttl>m)"`.
- **Dependencies**: `MaintenanceWriter`.
- **Thread-safety**: single-shot, single-threaded.

#### `archive.ArchiveScanner` — final class

- **Purpose**: ports the rglob block in `verify.py:297-301`. Returns a sorted list of `ArchiveFile` records.
- **Public**: `List<ArchiveFile> scan(Path baseDir, String date, String exchange, String symbol, String stream) throws IOException`.
- **Algorithm**:
  1. `Files.walk(baseDir)` (Tier 5 I7) → filter on regex `^.*/<date>/hour-\d+(?:\.(?:late|backfill)-\d+)?\.jsonl\.zst$`.
  2. For each match, decompose the path into `(exchange, symbol, stream)` from the relative-path components — must be at least 4 segments deep (matches Python `verify.py:307-310` guard).
  3. Filter by the optional exchange/symbol/stream params.
  4. **Sort by `Path.toString()` lex order** (matches Python's `sorted(...)` over `Path` objects, which uses string comparison).
- **Dependencies**: none.
- **Thread-safety**: stateless utility.

#### `archive.ArchiveFile` — record

`public record ArchiveFile(Path path, String exchange, String symbol, String stream, String date) {}` — immutable, identity-by-fields.

#### `archive.DecompressAndParse` — final class

- **Purpose**: ports `decompress_and_parse(file_path)` and `_decompress_and_parse(file_path)` (the consolidate sibling that ignores blank lines).
- **Public**: `static List<JsonNode> parse(Path path, ObjectMapper mapper) throws IOException` and `static Iterator<byte[]> streamLines(Path path) throws IOException` (the latter for streaming-mode consolidation; Tier 5 I2).
- **Algorithm**:
  - Open `Files.newInputStream(path)` → wrap with `ZstdInputStream` → wrap with `BufferedReader(InputStreamReader(zstdIn, UTF_8))` (Tier 5 I2). Stream lines via `BufferedReader.lines()`.
  - For `parse(...)`: `mapper.readTree(line)` per line; skip empty lines.
  - For `streamLines(...)`: yield the UTF-8 bytes of each line (no parse).
- **Thread-safety**: stateless; each call opens its own stream.

#### `verify.ChecksumVerifier` — final class

- **Purpose**: ports `verify_checksum(data_path, sidecar_path)`. Output strings byte-identical to Python.
- **Public**: `static List<String> verify(Path dataPath, Path sidecarPath) throws IOException`.
- **Algorithm**:
  1. If `!Files.exists(sidecarPath)` → return `[ "Sidecar not found: " + sidecarPath ]`.
  2. Read first whitespace-delimited token: `Files.readString(sidecarPath, UTF_8).strip().split("\\s+")[0]`.
  3. `String actual = Sha256.hexFile(dataPath)` (reuse common helper) — lowercase hex.
  4. Mismatch → `[ "Checksum mismatch for " + dataPath.getFileName() ]`.

#### `verify.EnvelopeVerifier` — final class

- **Purpose**: ports `verify_envelopes(envelopes)` — required-fields check + `raw_sha256` re-hash.
- **Public**: `static List<String> verify(List<JsonNode> envelopes)`.
- **Algorithm**:
  - For each envelope at index `i`:
    - Determine required field set: gap-set (`GAP_ENVELOPE_FIELDS ∪ BROKER_COORD_FIELDS`) if `type == "gap"`, else data-set (`DATA_ENVELOPE_FIELDS ∪ BROKER_COORD_FIELDS`). The two sets are static `Set<String>` constants matching `src/common/envelope.py` field sets.
    - `missing = requiredSet \ envelope.fieldNames()` — if non-empty, append `"Line " + i + ": missing fields: " + sortedMissing` and `continue`. **Sort lexicographically** to match Python's `sorted(...)` output (e.g. `"['_offset', '_partition', '_topic', ...]"`). Format must match Python's `str(list)` form including single quotes — see §6.7 watch-out.
    - For `type == "data"`: `String actual = Sha256.hexDigestUtf8(env.get("raw_text").asText())`. If `!actual.equals(env.get("raw_sha256").asText())` → `"Line " + i + ": raw_sha256 mismatch at offset " + env.path("_offset").asLong(0L)`.
- **Watch-out**: the Python format for the sorted missing list uses Python repr (e.g. `"['_offset']"`) — Java must produce the same string. Implementation: `"[" + sorted.stream().map(s -> "'" + s + "'").collect(joining(", ")) + "]"`.

#### `verify.DuplicateOffsetChecker` — final class

- **Purpose**: ports `check_duplicate_offsets(envelopes)`. Tier 5 M9 — `_offset < 0` is a sentinel for synthetic records and is skipped.
- **Public**: `static List<String> check(List<JsonNode> envelopes)`.
- **Algorithm**:
  - `Set<String> seen = new HashSet<>()`. For each envelope:
    - `long offset = env.path("_offset").asLong(0L)`. If `_offset` JSON node is missing entirely (`isMissingNode()`), treat as `null` per Python — i.e. NOT skipped (Python's `offset is not None and offset < 0` check). Use `boolean isPresent = !env.path("_offset").isMissingNode() && !env.path("_offset").isNull();`.
    - If `isPresent && offset < 0L` → continue (skip).
    - `String key = env.path("_topic").asText() + "|" + env.path("_partition").asInt(0) + "|" + offset` (the `|` separator is internal; the Python output uses `tuple` repr).
    - If `seen.contains(key)` → append `"Duplicate broker record: " + tupleRepr` where `tupleRepr` matches Python's `tuple` repr exactly: `"(<topic-quoted-or-None>, <partition-or-None>, <offset-or-None>)"`. **Watch-out**: Python emits `('binance.depth', 0, 12345)` (with single quotes around strings, comma-space separators). The format must match this byte-for-byte.

#### `verify.GapReporter` — final class

- **Purpose**: ports `report_gaps(envelopes)`. Returns a list filtered to gap envelopes, preserving all fields.
- **Public**: `static List<JsonNode> collect(List<JsonNode> envelopes)`.
- **Thread-safety**: stateless.

#### `verify.DepthReplayVerifier` — final class

- **Purpose**: ports the most complex function in the module — `verify_depth_replay(depth, snapshot, gap)` (verify.py:85-212). Per-symbol pu-chain validation with snapshot-spanning re-sync and gap-window tolerance.
- **Public**: `static List<String> verify(List<JsonNode> depthEnvelopes, List<JsonNode> snapshotEnvelopes, List<JsonNode> gapEnvelopes, ObjectMapper mapper)`.
- **Algorithm** (verbatim port of Python; all `long` per Tier 5 E1, M5):
  1. If `depthEnvelopes.isEmpty()` → return `List.of()`.
  2. Bucket by symbol: `Map<String, List<JsonNode>> depthBySym`, `snapBySym`, `gapBySymStream` keyed by `(symbol, stream)`.
  3. For each symbol in `depthBySym`:
     - Sort by `received_at` (stable).
     - Build `gap_windows = List<long[2]>` from depth + depth_snapshot gaps for that symbol.
     - Define `_in_gap(receivedAt)` = any window contains it; `_gap_between(prev, cur)` = any window overlaps `[prev, cur]`.
     - Collect `snap_lids` = `long[]` of `snap.raw_text.lastUpdateId` (parse via `mapper.readTree(rawText).get("lastUpdateId").asLong()`); skip on KeyError/parse error (Tier 5 E1).
     - State: `boolean synced=false; long lastU=0L; long lastReceivedAt=0L`.
     - For each diff envelope:
       - Parse U/u/pu via `mapper.readTree(rawText).path(name).asLong(0L)`.
       - If `!synced`: try to find any `lid` with `U <= lid+1 && lid+1 <= u` → flip synced; if no match and not in gap, emit corresponding error string (two variants: "First diff does not span any snapshot sync point: ..." or "Depth diff at ... has no preceding snapshot").
       - Else: pu-chain check `pu == lastU`; if break and (in gap or gap_between), try re-sync with any lid; otherwise emit `[symbol] pu chain break at received_at <ts>: expected pu=<x>, got pu=<y>`.
       - Update `lastU`, `lastReceivedAt` per Python.
- **Output format**: error strings byte-identical to Python (see §6.7 § for exact templates).
- **Cyclomatic complexity**: ~8 (matches Python). The state machine is intentionally not extracted into helper methods beyond the two `_in_gap` / `_gap_between` private predicates — Python keeps it inline; do the same to maximize line-by-line diff readability.
- **Thread-safety**: stateless (per-call), all locals.

#### `verify.ManifestGenerator` — final class

- **Purpose**: ports `generate_manifest(base_dir, exchange, date)` — produces an `ObjectNode` with the exact key insertion order from Python (Tier 5 B1).
- **Public**: `static ObjectNode generate(Path baseDir, String exchange, String date, ObjectMapper mapper) throws IOException`.
- **Watch-out**: Jackson's `ObjectNode` preserves insertion order via the underlying `LinkedHashMap`. Use `mapper.createObjectNode()` and `set(key, value)` in the exact order Python builds the dict (`date`, `exchange`, `symbols`, then per-symbol `streams`, etc.).

#### `gaps.GapsCli` — final class

`@Command(name = "gaps", subcommands = {AnalyzeCommand.class, BackfillCommand.class})` — picocli sub-tree under verify root. Calling `cryptolake-verify gaps backfill ...` is identical to Python's `python -m src.cli.gaps backfill ...`.

#### `gaps.BackfillCommand` — final class implements `Callable<Integer>`

- **Purpose**: ports `@cli.command() def backfill(...)` — REST-driven gap recovery.
- **Picocli options**: `--base-dir --exchange --symbol --stream --date --dry-run --json` (matches Python click options).
- **Algorithm**:
  1. `ArchiveAnalyzer.findGaps(baseDir, exchange, symbol, stream, date)` returns missing-hour list per-stream.
  2. Filter by `BACKFILLABLE_STREAMS`; report unrecoverable per-Python message format.
  3. If `--dry-run`: print the plan via `System.out.println(...)` and return 0.
  4. For each backfillable hour: `BackfillOrchestrator.fetchAndWrite(...)` — single shared `HttpClient` (Tier 5 D3), exp backoff per Tier 5 D7.
  5. Emit JSON or human-readable summary based on `--json`.
- **Dependencies**: `ArchiveAnalyzer`, `BackfillOrchestrator`, `HttpClient` (single shared), `EnvelopeCodec`.

#### `gaps.AnalyzeCommand` — final class implements `Callable<Integer>`

- **Purpose**: ports `analyze` — read-only gap report.
- **Picocli options**: same as `BackfillCommand` minus `--dry-run`.
- **Algorithm**: invoke `ArchiveAnalyzer.findGaps(...)`; print human-readable or JSON summary. Tier 5 K2 — `System.out.println` only.

#### `gaps.BinanceRestClient` — final class

- **Purpose**: ports `_fetch_historical_page(session, url, max_retries)`. Single instance per CLI process (Tier 5 D3).
- **Public**: `JsonNode fetchPage(String url) throws IOException, EndpointUnavailableException` (max 5 retries; honor `Retry-After` header; on 400/403/5xx throw `EndpointUnavailableException`).
- **Algorithm** (Tier 5 D4, D5, D7):
  - Loop `attempt 0..maxRetries`:
    - `HttpResponse<byte[]> resp = httpClient.send(req, BodyHandlers.ofByteArray())` (always byte-array; Tier 5 D5).
    - `int sc = resp.statusCode()`. If `sc == 429 || sc == 418`: `long retry = resp.headers().firstValueAsLong("Retry-After").orElse(1L << attempt)`; `Thread.sleep(Duration.ofSeconds(retry))`; continue.
    - If `sc == 400 || sc == 403 || sc >= 500`: `String body = new String(resp.body(), UTF_8)`; throw `EndpointUnavailableException(sc + ": " + body.substring(0, Math.min(200, body.length())))`.
    - If `sc >= 400`: throw `IOException("HTTP " + sc + ": " + ...)`.
    - Return `mapper.readTree(resp.body())`.
- **Thread-safety**: `HttpClient` is thread-safe.

#### `gaps.BackfillOrchestrator` — final class

- **Purpose**: ports the orchestration body of `gaps.backfill` plus `_fetch_historical_all` plus the boundary funding-rate interpolation glue.
- **Public**: `BackfillSummary fetchAndWrite(Path baseDir, String exchange, String symbol, String stream, String date, List<Integer> missingHours)`.
- **Dependencies**: `BinanceRestClient`, `BackfillEnvelopeFactory`, `KlineFetcher`, `FundingRateInterpolator`, `DeepIdBackfill`.

#### `gaps.BackfillEnvelopeFactory` — final class

- **Purpose**: ports `_wrap_backfill_envelope` — wraps a Binance raw record into a backfill data envelope. Sets `_topic="backfill"`, `_partition=0`, `_offset=seq`, `source="backfill"`.
- **Public**: `static byte[] wrap(JsonNode rawRecord, String exchange, String symbol, String stream, String sessionId, long seq, String exchangeTsKey, ObjectMapper mapper)`.
- **Algorithm**:
  - `String rawText = mapper.writeValueAsString(rawRecord)` (Tier 5 B2 — compact ASCII output; **watch-out**: Python `orjson.dumps(raw_record).decode()` produces ASCII-only no-space output; Jackson default config matches if `EnvelopeCodec.newMapper()` is used unaltered).
  - Build the envelope as an `ObjectNode` with insertion order: `v, type, source, exchange, symbol, stream, received_at, exchange_ts, collector_session_id, session_seq, raw_text, raw_sha256, _topic, _partition, _offset` (matches `gaps.py:60-77` insertion order).
  - `raw_sha256 = Sha256.hexDigestUtf8(rawText)`.
- **Tier 1 §1**: this is a backfill path — `raw_text` is built FROM the parsed JsonNode and SHA-256'd over its bytes. The "raw" bytes here are the Java serializer output; correctness depends on `mapper.writeValueAsString(rawRecord)` matching `orjson.dumps(raw_record).decode()` byte-for-byte. Tier 5 B2 plus the empirical default matches; if mismatch is detected in fixtures, explicitly disable any default pretty-printer. Open question §11.

#### `gaps.FundingRateInterpolator` — final class

- **Purpose**: ports `_interpolate_funding_rate_gap` — generates one synthetic markPriceUpdate per second across the gap window.
- **Public**: `static List<JsonNode> interpolate(JsonNode beforeRecord, JsonNode afterRecord, long gapStartMs, long gapEndMs, String symbol, ObjectMapper mapper)`.
- **Algorithm** (Tier 5 M4, M16):
  - `double startP = boundaryDouble(beforeRecord, "p")` etc. for `p`, `i`, `P`.
  - `String rate = beforeRecord.path("r").asText("0")`.
  - `long nextFunding = beforeRecord.path("T").asLong(_computeNextFundingTime(gapStartMs))`.
  - Align to second boundaries: `long firstSec = ((gapStartMs / 1000) + 1) * 1000`, `long lastSec = (gapEndMs / 1000) * 1000`, `long totalSpan = Math.max(1, lastSec - firstSec)`.
  - For `t = firstSec; t <= lastSec; t += 1000`:
    - `double frac = (totalSpan > 0) ? (double)(t - firstSec) / totalSpan : 0`.
    - `double p = startP + (endP - startP) * frac` (similarly for i, P).
    - Build `ObjectNode`: insertion order `e, E, s, p, ap, P, i, r, T` (matches `gaps.py:174-184`).
    - Format prices as `String.format(Locale.ROOT, "%.8f", p)` (Tier 5 M4).
- **Thread-safety**: stateless utility.

#### `gaps.KlineFetcher` — final class

- **Purpose**: fetches mark/index/premium klines for the funding-rate boundary build path. One method per kline type; all use the shared `BinanceRestClient`.

#### `gaps.DeepIdBackfill` — final class

- **Purpose**: ports `_fetch_by_id` (ID-anchored backfill for trades; uses `aggTrades` REST with `fromId` + `limit`).
- **Public**: `List<JsonNode> fetchById(String symbol, long fromId, long toId)`.

#### `gaps.GapStreams` — final utility class

- **Purpose**: holds the constant sets/maps from `gaps.py:23-38`. All `Set<String>` / `Map<String, ...>` immutable.
- **Public**: `static Set<String> BACKFILLABLE = Set.of("trades", "funding_rate", "open_interest")`, `NON_BACKFILLABLE`, `STREAM_TS_KEYS`, `ENDPOINT_WEIGHTS`, `BINANCE_REST_BASE = "https://fapi.binance.com"`.

#### `gaps.EndpointUnavailableException` — final class extends `RuntimeException`

- **Purpose**: ports `EndpointUnavailableError`. Tier 2 §13 — RuntimeException, not checked.
- **Public**: `EndpointUnavailableException(String msg)`.

#### `integrity.IntegrityCli` — final class

`@Command(name = "integrity", subcommands = {CheckTradesCommand.class, CheckDepthCommand.class, CheckBooktickerCommand.class})`.

#### `integrity.CheckTradesCommand|CheckDepthCommand|CheckBooktickerCommand` — final classes implements `Callable<Integer>`

- **Purpose**: each ports its corresponding `@cli.command()` from `integrity.py`. Picocli options: `--base-dir --exchange --symbol --date --json`.
- **Algorithm**: discover hourly files for the date → `ContinuityChecker.run(stream, files, mapper)` → return `IntegrityCheckResult` → format output (human or JSON).

#### `integrity.ContinuityChecker` — final class

- **Purpose**: shared driver. Streams records from each `.jsonl.zst` (Tier 5 I2 streaming decompress), dedups in-memory by natural key, dispatches to per-stream checker.
- **Public**: `static IntegrityCheckResult run(String stream, List<Path> files, ObjectMapper mapper) throws IOException`.

#### `integrity.TradesContinuity` — final class

- **Purpose**: ports `_check_trades`. Walks aggregate trade IDs (`a` field) and reports `(field, expected, actual, missing, at_received)` breaks.
- **Public**: `static IntegrityCheckResult check(Iterator<JsonNode> records)`.
- **Algorithm**: track `long lastA = -1L`; for each record, parse `raw_text.a` as `long` (Tier 5 E1); on `a != lastA + 1` (and lastA != -1): record break with `missing = a - lastA - 1`.

#### `integrity.DepthContinuity` — final class

- **Purpose**: ports `_check_depth`. pu-chain walk identical to `DepthReplayVerifier`'s pu-chain check, except no snapshot anchoring (just sequential).

#### `integrity.BooktickerContinuity` — final class

- **Purpose**: ports `_check_bookticker`. Detects backwards jumps in `u`; forward jumps are normal (other symbols share the bookticker stream).

#### `integrity.IntegrityCheckResult` — record

`public record IntegrityCheckResult(int recordCount, List<Break> breaks) {}` and inner record `public record Break(String field, long expected, long actual, long missing, long atReceived) {}`.

#### `maintenance.MaintenanceWriter` — final class

- **Purpose**: ports `StateManager.create_maintenance_intent`. Single INSERT INTO `maintenance_intents (...) VALUES (?,?,?,?,?,?)`.
- **Public**: `void write(String dbUrl, MaintenanceIntent intent) throws SQLException`.
- **Algorithm**: open `DriverManager.getConnection(dbUrl)`; prepared statement with timestamps as ISO-8601 strings (Tier 5 F1).
- **Thread-safety**: each call opens its own connection. No connection pool — Tier 2 §11/§14 single-instance rule applies to long-running services; CLI is one-shot.

#### `maintenance.MaintenanceIntent` — record

`public record MaintenanceIntent(String maintenanceId, String scope, String plannedBy, String reason, String createdAt, String expiresAt) {}` — matches Python's `MaintenanceIntent` field set.

#### `harness.VerifyStdoutParityHarness` — final class

- **Purpose**: gate-5 byte-identity enforcement. Runs `cryptolake-verify verify` against a Python-produced fixture archive set, captures stdout, and diffs against the recorded Python output (`parity-fixtures/verify/expected.txt`).
- **Public**: `static void main(String[] args)` — `args[0]=fixtureArchiveDir`, `args[1]=expectedStdoutPath`, optional `args[2]=reportPath`.
- **Algorithm**:
  1. Launch the verify command in-process: `int rc = new CommandLine(new Main()).setOut(captureWriter).execute("verify", "--date", ..., "--base-dir", fixtureArchiveDir, "--full")`.
  2. Capture stdout into `String javaOutput`.
  3. Read `Files.readString(expectedStdoutPath)` → `String pythonOutput`.
  4. `byte[] javaBytes = javaOutput.getBytes(UTF_8)`; `byte[] pythonBytes = pythonOutput.getBytes(UTF_8)`.
  5. If `Arrays.equals(javaBytes, pythonBytes) && rc == expectedRc` → print `OK <bytes-len>` + exit 0.
  6. Else print first 100 differing bytes (hex) + exit 1.
- **Gradle**: `:verify:runVerifyParity` registers a `JavaExec` task with `mainClass = "com.cryptolake.verify.harness.VerifyStdoutParityHarness"`.
- **Dependencies**: only the in-process verify CLI plus the test resources directory layout.
- **Thread-safety**: stateless single-shot.

### 2.2 `:consolidation` app

#### `Main` — final class

- **Public**: `static void main(String[] args)`.
- **Picocli root**: `@Command(name = "cryptolake-consolidation", subcommands = {ConsolidateDayCommand.class, SchedulerCommand.class}, defaultCommand = SchedulerCommand.class)`. Running with no args = scheduler mode (matches Docker entrypoint).

#### `scheduler.ConsolidationScheduler` — final class

- **Purpose**: ports `consolidation_scheduler.main`. Long-running daily loop.
- **Public**: `void run()` (blocks); `void stop()` (interrupt-safe).
- **Algorithm**:
  1. `HealthServer healthServer = new HealthServer(8003, () -> "OK", metricsScrapeSupplier)` (reuse common; serves Prometheus on `/metrics`).
  2. Start an MBeans-backed `PrometheusMeterRegistry`; register `ConsolidationMetrics`.
  3. Loop:
     - `Instant nextRun = ScheduleClock.nextRunInstant(Instant.now(), startHourUtc)`.
     - `long sleepNanos = Duration.between(Instant.now(), nextRun).toNanos()`.
     - `if (stopLatch.await(sleepNanos, NANOSECONDS)) return;` (Tier 5 A3).
     - `ConsolidationCycle.run(baseDir)` — synchronous (Tier 5 A2).
     - Update metrics.
- **Dependencies**: `HealthServer`, `ConsolidationMetrics`, `ConsolidationCycle`, `ScheduleClock`, `CountDownLatch stopLatch`, `Path baseDir`.
- **Thread-safety**: single-threaded loop on a virtual thread; `stopLatch` allows external stop from SIGTERM hook.

#### `scheduler.ScheduleClock` — final utility class

- **Purpose**: deterministic next-run computation.
- **Public**: `static Instant nextRunInstant(Instant now, int startHourUtc)`. Returns the next 02:30 UTC (or the configured hour:30) ≥ now.
- **Algorithm**: Tier 5 F3 / M11 — always UTC. `LocalDate today = now.atOffset(ZoneOffset.UTC).toLocalDate()`. Try `today.atTime(startHourUtc, 30).toInstant(ZoneOffset.UTC)`; if not after now, add 1 day.

#### `scheduler.ConsolidationCycle` — final class

- **Purpose**: ports `_run_consolidation_cycle` (consolidation_scheduler.py:75-145).
- **Public**: `CycleSummary run(Path baseDir, String dateOverride /* nullable */)`.
- **Algorithm**:
  1. Determine target date: yesterday by default (UTC), or `dateOverride`.
  2. Walk `{baseDir}/{exchange}/{symbol}/{stream}/{date}/` directories.
  3. For each, call `ConsolidateDay.run(...)`.
  4. Update Prometheus counters/gauges and emit structured logs (`consolidation_cycle_started`, `consolidation_cycle_finished`, `consolidation_day_failed`).

#### `scheduler.ConsolidationMetrics` — final class

8 Prometheus meters (matches `consolidation_scheduler.py:46-78`). See §9.

#### `core.ConsolidateDay` — final class

- **Purpose**: ports `consolidate_day` — the orchestration entry point.
- **Public**: `ConsolidateResult run(Path baseDir, String exchange, String symbol, String stream, String date)`.
- **Algorithm** (mirrors `consolidate.py:303-440`):
  1. `Path dateDir = baseDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date)`. If missing → `ConsolidateResult.empty()`.
  2. `Map<Integer, HourFileGroup> groups = HourFileDiscovery.discover(dateDir)`.
  3. `Set<Integer> missing = IntStream.range(0,24).boxed().filter(h -> !groups.containsKey(h) || groups.get(h).isEmpty()).collect(toSet())`.
  4. For each present hour: `HourMerger.merge(hour, groups.get(hour), stream, mapper)` returning `List<JsonNode>`. (Or stream-mode if base file is alone — Tier 5 I2.)
  5. For each missing hour: `MissingHourGapFactory.create(...)` → injected as a single gap envelope.
  6. `DailyFileWriter.write(dailyPath, hourIterator, mapper)` returns `WriteStats`.
  7. `Sha256Sidecar.write(dailyPath, sidecarPath)` (reuse writer's helper).
  8. Build `ManifestRecord` and write `{date}.manifest.json`.
  9. `DailyFileVerifier.verify(dailyPath, expectedCount, sidecarPath, stream)` — must succeed before cleanup.
  10. `HourlyCleanup.delete(originalFiles)` — only if verify passed (Tier 1 §4 indirect).
- **Thread-safety**: single-shot per call. Multiple concurrent calls (across symbols/streams) are isolated by directory (no shared mutable state).

#### `core.HourFileDiscovery` — final utility class

- **Public**: `static Map<Integer, HourFileGroup> discover(Path dateDir) throws IOException`.
- **Algorithm** (regex from consolidate.py:22-24):
  - `_RE_BASE = Pattern.compile("^hour-(\\d{1,2})\\.jsonl\\.zst$")`.
  - `_RE_LATE = Pattern.compile("^hour-(\\d{1,2})\\.late-(\\d+)\\.jsonl\\.zst$")` (Tier 5 M15).
  - `_RE_BACKFILL = Pattern.compile("^hour-(\\d{1,2})\\.backfill-(\\d+)\\.jsonl\\.zst$")`.
  - Sort late and backfill lists by their seq number ascending.

#### `core.HourFileGroup` — record

`public record HourFileGroup(Path base /* nullable */, List<Path> late, List<Path> backfill) { public boolean isEmpty() { return base == null && late.isEmpty() && backfill.isEmpty(); } }`.

#### `core.HourMerger` — final class

- **Purpose**: ports `merge_hour`.
- **Public**: `static List<JsonNode> merge(int hour, HourFileGroup group, String stream, ObjectMapper mapper) throws IOException`.
- **Algorithm**:
  1. Single-base-no-extra fast path: just decompress and return (already sorted by writer).
  2. Otherwise: parse all sources; bucket into `data` and `gap` lists.
  3. Sort `data` by `DataSortKey.of(record, stream)` (long).
  4. Sort `gap` by `gap_start_ts`.
  5. Splice gaps into the data stream by `gap_start_ts <= received_at` (mirrors consolidate.py:135-149).

#### `core.DataSortKey` — final utility class

- **Public**: `static long of(JsonNode record, String stream, ObjectMapper mapper) throws IOException`.
- **Algorithm** (Tier 5 M5):
  - For `trades`: parse `record.path("raw_text").asText("{}")` → `JsonNode raw = mapper.readTree(...)` → `raw.path("a").asLong(record.path("exchange_ts").asLong(0L))`.
  - For `depth`: similar but `path("u")`.
  - Else: `record.path("exchange_ts").asLong(0L)`.
- **Watch-out**: ALL `long`. NEVER `asInt()` (Tier 5 E1).

#### `core.DailyFileWriter` — final class

- **Purpose**: ports `write_daily_file`. Streams or buffers; writes zstd-compressed JSONL.
- **Public**: `WriteStats write(Path outputPath, Iterator<HourBatch> hourBatches, ObjectMapper mapper) throws IOException`.
- **Algorithm** (Tier 5 I1, I3):
  - `Files.createDirectories(outputPath.getParent())`.
  - `try (var fc = FileChannel.open(outputPath, CREATE, WRITE, TRUNCATE_EXISTING); var zstdOut = new ZstdOutputStream(Channels.newOutputStream(fc), 3) /* one-shot stream OK; daily file is sealed */; var bufOut = new BufferedOutputStream(zstdOut))`:
    - For each batch: write `mapper.writeValueAsBytes(record)` + `(byte) '\n'`.
  - **Watch-out**: per Tier 5 I1, the writer module uses one-frame-per-batch (`Zstd.compress(joined, level)`) for crash-safety on hourly appends. Daily files are SEALED in one shot — `ZstdOutputStream` is acceptable here because the file is written once and never appended. After the writer block, `fc.force(true)` (Tier 5 I3) is implicit in close — but explicit `force(true)` BEFORE close is required for durability (call directly on the FileChannel before closing the stream). See open question §11.

#### `core.DailyFileVerifier` — final class

- **Purpose**: ports `verify_daily_file`.
- **Public**: `static VerifyResult verify(Path dailyPath, long expectedCount, Path sidecarPath, String stream, ObjectMapper mapper) throws IOException`.
- **Algorithm**:
  1. SHA-256 check via `Sha256.hexFile(dailyPath)` vs sidecar contents.
  2. Streaming walk: per record, parse `JsonNode`. Track count. For data records, validate `DataSortKey.of(record, stream)` is monotonically non-decreasing (gaps are skipped in ordering checks).
  3. Return `(success, errorMessage)` tuple.

#### `core.HourlyCleanup` — final class

- **Public**: `static void delete(List<Path> originalFiles, List<Path> sidecars)`.
- **Algorithm**: `Files.deleteIfExists(...)` per file, swallowed `IOException` per Tier 5 G1.

#### `core.ManifestRecord` — record

Mirrors Python `{date}.manifest.json` schema (mapping §7). Field order via `@JsonPropertyOrder` (Tier 5 B1):
```text
@JsonPropertyOrder({"version", "exchange", "symbol", "stream", "date",
    "consolidated_at", "daily_file", "daily_file_sha256",
    "total_records", "data_records", "gap_records",
    "hours", "missing_hours", "source_files"})
public record ManifestRecord(int version, String exchange, String symbol, String stream, String date,
    String consolidatedAt, String dailyFile, String dailyFileSha256,
    long totalRecords, long dataRecords, long gapRecords,
    Map<String, HourSummary> hours, List<Integer> missingHours, List<String> sourceFiles) {}
```
The inner `HourSummary` record has `@JsonPropertyOrder({"status", "data_records", "gap_records", "sources"})`.

#### `core.MissingHourGapFactory` — final class

- **Purpose**: ports `synthesize_missing_hour_gap` (consolidate.py:154-178).
- **Public**: `static byte[] create(String exchange, String symbol, String stream, String date, int hour, String sessionId, ObjectMapper mapper)`.
- **Algorithm** (Tier 5 F3, M11; Tier 1 §5):
  - Parse `date` as `LocalDate.parse(date)`.
  - `Instant hourStart = date.atTime(hour, 0).toInstant(ZoneOffset.UTC)`.
  - `Instant hourEndExclusive = hourStart.plus(Duration.ofHours(1))`.
  - `long gapStartNs = hourStart.getEpochSecond() * 1_000_000_000L + hourStart.getNano()` (Tier 5 E5).
  - `long gapEndNs = hourEndExclusive.getEpochSecond() * 1_000_000_000L - 1L` (matches Python).
  - Build `GapEnvelope` via reused common factory, with `reason = "missing_hour"`, `session_seq = -1L` (Tier 5 M10), `detail = "No data files found for hour " + hour + "; not recoverable via backfill"`.
- **Tier 1 §5 honored**: every missing hour emits a metric increment (in `ConsolidationCycle`), a structured log (`missing_hour_synthesized`), and an archived gap envelope (this method's return → daily file).

#### `seal.SealDailyArchive` — final class

- **Purpose**: ports `seal_daily_archive`. Packs all per-stream daily files for a date into a single tar.zst.
- **Public**: `void seal(Path baseDir, String exchange, String symbol, String date)`.
- **Algorithm**: open output `.tar.zst`; for each stream, call `ConsolidateStreamToTar.pack(...)`; close; write `.tar.zst.sha256` sidecar.

#### `seal.ConsolidateStreamToTar` — final class

- **Purpose**: ports `consolidate_stream_to_tar`. Two-pass: pass 1 sizes, pass 2 streams bytes through tar header → zstd → fileChannel.
- **Public**: `StreamPackResult pack(TarArchiveOutputStream tar, Path baseDir, String exchange, String symbol, String stream, String date)`.

### 2.3 `:backfill` app

#### `Main` — final class

- **Public**: `static void main(String[] args)` — picocli root with `BackfillScheduler` as default subcommand and `RunOnceCommand` for ad-hoc.

#### `BackfillScheduler` — final class

- **Purpose**: ports `backfill_scheduler.main`. 6-hourly loop.
- **Public**: `void run()`, `void stop()`.
- **Algorithm**: same shape as `ConsolidationScheduler` — `HealthServer` on port 8002, `BackfillMetrics`, loop calling `BackfillCycle.run(baseDir)` every 6 hours via `stopLatch.await(6 * 3600, SECONDS)` (Tier 5 A3).

#### `BackfillCycle` — final class

- **Purpose**: ports `_run_backfill_cycle`. Calls `verify.gaps.BackfillOrchestrator` for the actual work.
- **Public**: `CycleSummary run(Path baseDir)`.

#### `BackfillMetrics` — final class

6 Prometheus meters (matches `backfill_scheduler.py`). See §9.

---

## 3. Concurrency design

Java 21 virtual threads. No shared long-running state across CLI invocations (the verify/manifest/integrity/gaps subcommands run to completion in a single process invocation). The two scheduler apps own ONE long-running loop each.

### 3.1 Thread topology — `:verify` app (one-shot)

- **T_main** (platform thread): runs the picocli command. All work is sequential. No fan-out.
- **T_http** (kafka-clients-style internal): `HttpClient` may use its own thread pool internally — Java 21 default is virtual-thread-based; no override needed (Tier 5 D3, open question §11).
- **No other threads**. The CLI returns when `Callable.call()` returns, then `System.exit(rc)`.

### 3.2 Thread topology — `:consolidation` app (long-running)

- **T_main** (platform): SIGTERM hook + `shutdownLatch.await()`.
- **T_scheduler** (virtual): `ConsolidationScheduler.run()` loop.
- **T_health_http** (virtual): `HealthServer` (reused common).
- **T_consolidate_cycle** (virtual, ephemeral): `ConsolidationCycle.run(baseDir)` — runs entirely on the scheduler's owning virtual thread (no fan-out across symbols/streams; sequential per Python's structure). Open question §11 — if cycle time exceeds the daily window, may need fan-out.

### 3.3 Thread topology — `:backfill` app (long-running)

Same shape as `:consolidation` but with a 6-hour interval and port 8002.

### 3.4 Locking strategy

| Resource                  | Primitive             | Why                                                             |
| ------------------------- | --------------------- | --------------------------------------------------------------- |
| `shutdownLatch`           | `CountDownLatch`      | SIGTERM hook coordination (Tier 5 A3)                            |
| `stopLatch`               | `CountDownLatch(1)`   | Per-scheduler stop signal; awakens sleep-loop early             |
| `Prometheus meter state`  | Micrometer-internal   | Thread-safe (same as collector / writer)                         |
| `ConsolidationCycle` state| local-variable confined | No shared mutation across cycles                                |
| `BackfillOrchestrator`    | local-variable confined | Each invocation gets its own progress map                       |

**No `synchronized` anywhere.** The CLI is naturally low-concurrency (one-shot or single-loop).

### 3.5 Cancellation / shutdown

For schedulers:
1. SIGTERM hook → `shutdownLatch.countDown()` → `stopLatch.countDown()`.
2. Scheduler loop's `stopLatch.await(...)` returns true → exits.
3. `HealthServer.stop()` (1s grace).
4. `MeterRegistry.close()`.
5. JVM exits.

For one-shot CLIs (verify, manifest, mark-maintenance, gaps subcommands, integrity subcommands):
- No SIGTERM hook needed; the process is short-lived. If `Ctrl+C` arrives, JVM default handlers terminate.
- `Callable.call()` exceptions propagate to picocli's exit-code handler.

### 3.6 Cancellation/shutdown of in-flight HTTP calls

`BinanceRestClient` invocations in `gaps.backfill` are sequential. If SIGTERM arrives during `httpClient.send`, the call may complete — `HttpClient` does not interrupt easily. The CLI is one-shot; no graceful shutdown is required beyond JVM-default behavior. **Watch-out**: a long-running `gaps backfill` cycle that takes hours can be killed via `SIGTERM` and the partial state is fine — `ArchiveAnalyzer` re-detects on the next run.

### 3.7 Mapping §5 concurrency items — addressed

| Python primitive (§5)                           | Java realization                                              |
| ----------------------------------------------- | ------------------------------------------------------------- |
| `async def main(): await asyncio.sleep(secs)`   | `stopLatch.await(secs, SECONDS)` (Tier 5 A3)                   |
| `await _run_consolidation_cycle(base_dir)`      | direct synchronous call on virtual thread (Tier 5 A2)         |
| `async with aiohttp.ClientSession()`            | shared `HttpClient` (Tier 5 D3)                                |
| `await _fetch_historical_page(session, url, ...)`| `BinanceRestClient.fetchPage(url)` blocking (Tier 5 A2, D4)   |
| `await asyncio.sleep(retry_after)`              | `Thread.sleep(Duration.ofSeconds(retryAfter))` on a virtual thread (Tier 5 D7) |
| `asyncio.run(_write_intent())` in mark-maintenance | direct synchronous JDBC call (Tier 5 A2)                    |
| No structural concurrency (no gather, no TaskGroup) | No `StructuredTaskScope` needed (sequential)               |

---

## 4. Python → Java mapping table

Every symbol from mapping §3 (Public API surface) and §4 (Internal structure) → Java class.method. Symbols intentionally dropped marked DROPPED.

### 4.1 `src/cli/__init__.py`

| Python | Java | Notes |
| ------ | ---- | ----- |
| (empty package init) | DROPPED | No analog needed |

### 4.2 `src/cli/verify.py`

| Python symbol                          | Java                                          | Notes |
| -------------------------------------- | --------------------------------------------- | ----- |
| `cli` (click group)                    | `Main` + `@Command(subcommands={...})`         | picocli root |
| `verify` (command)                     | `cli.VerifyCommand`                            |       |
| `manifest` (command)                   | `cli.ManifestCommand`                          |       |
| `mark_maintenance` (command)           | `cli.MarkMaintenanceCommand`                   |       |
| `verify_checksum`                      | `verify.ChecksumVerifier.verify`               |       |
| `verify_envelopes`                     | `verify.EnvelopeVerifier.verify`               |       |
| `check_duplicate_offsets`              | `verify.DuplicateOffsetChecker.check`          | Tier 5 M9 sentinel skip |
| `report_gaps`                          | `verify.GapReporter.collect`                   |       |
| `decompress_and_parse`                 | `archive.DecompressAndParse.parse`             | Tier 5 I2 |
| `verify_depth_replay`                  | `verify.DepthReplayVerifier.verify`            | Cyclomatic ≈ 8 |
| `generate_manifest`                    | `verify.ManifestGenerator.generate`            |       |
| `_REQUIRED_DATA_FIELDS`                | `EnvelopeVerifier.DATA_REQUIRED_FIELDS` static |       |
| `_REQUIRED_GAP_FIELDS`                 | `EnvelopeVerifier.GAP_REQUIRED_FIELDS` static  |       |
| `DEFAULT_ARCHIVE_DIR`                  | `Defaults.ARCHIVE_DIR` (constant from `EnvOverrides`) | Reused common |

### 4.3 `src/cli/consolidate.py`

| Python symbol                          | Java                                              |
| -------------------------------------- | ------------------------------------------------- |
| `discover_hour_files`                  | `core.HourFileDiscovery.discover`                  |
| `_decompress_and_parse`                | `archive.DecompressAndParse.parse` (shared with :verify via `:common` or `:consolidation` direct dep — see open Q §11) |
| `_data_sort_key`                       | `core.DataSortKey.of`                              |
| `_stream_hour_lines`                   | `archive.DecompressAndParse.streamLines`            |
| `merge_hour`                           | `core.HourMerger.merge`                            |
| `synthesize_missing_hour_gap`          | `core.MissingHourGapFactory.create`                |
| `write_daily_file`                     | `core.DailyFileWriter.write`                       |
| `verify_daily_file`                    | `core.DailyFileVerifier.verify`                    |
| `cleanup_hourly_files`                 | `core.HourlyCleanup.delete`                        |
| `consolidate_day`                      | `core.ConsolidateDay.run`                          |
| `seal_daily_archive`                   | `seal.SealDailyArchive.seal`                       |
| `consolidate_stream_to_tar`            | `seal.ConsolidateStreamToTar.pack`                 |
| `_RE_BASE`/`_RE_LATE`/`_RE_BACKFILL`   | `core.HourFileDiscovery` private regex constants   |

### 4.4 `src/cli/consolidation_scheduler.py`

| Python symbol                          | Java                                          |
| -------------------------------------- | --------------------------------------------- |
| `main` (async entry)                   | `scheduler.ConsolidationScheduler.run`         |
| `_run_consolidation_cycle`             | `scheduler.ConsolidationCycle.run`             |
| `_compute_sleep_until_next_run`        | `scheduler.ScheduleClock.nextRunInstant`        |
| `start_http_server(port=8003)`         | `HealthServer` ctor with `port=8003` (reused)   |
| Prometheus gauges/counters             | `scheduler.ConsolidationMetrics` (§9)          |

### 4.5 `src/cli/gaps.py`

| Python symbol                          | Java                                              |
| -------------------------------------- | ------------------------------------------------- |
| `cli` (click group)                    | `gaps.GapsCli`                                    |
| `backfill` (command)                   | `gaps.BackfillCommand`                            |
| `analyze` (command)                    | `gaps.AnalyzeCommand`                             |
| `_hour_to_ms_range`                    | `gaps.HourBoundaries.toMsRange` (private utility)  |
| `_wrap_backfill_envelope`              | `gaps.BackfillEnvelopeFactory.wrap`               |
| `EndpointUnavailableError`             | `gaps.EndpointUnavailableException`                |
| `_fetch_historical_page`               | `gaps.BinanceRestClient.fetchPage`                 |
| `_compute_next_funding_time`           | `gaps.FundingRateInterpolator.computeNextFundingTime` (private) |
| `_build_mark_price_update`             | `gaps.FundingRateInterpolator.buildMarkPriceUpdate` (private) |
| `_interpolate_funding_rate_gap`        | `gaps.FundingRateInterpolator.interpolate`         |
| `_read_boundary_records`               | `gaps.FundingRateInterpolator.readBoundary` (private) |
| `_fetch_by_id`                         | `gaps.DeepIdBackfill.fetchById`                    |
| `_fetch_historical_all`                | `gaps.BackfillOrchestrator.fetchAll` (private)     |
| `_write_backfill_files`                | `gaps.BackfillOrchestrator.writeFiles` (private)   |
| `analyze_archive`                      | `gaps.ArchiveAnalyzer.findGaps`                    |
| `find_backfillable_gaps`               | `gaps.ArchiveAnalyzer.findBackfillable` (delegates to integrity) |
| `BACKFILLABLE_STREAMS`                 | `gaps.GapStreams.BACKFILLABLE`                     |
| `NON_BACKFILLABLE_STREAMS`             | `gaps.GapStreams.NON_BACKFILLABLE`                 |
| `STREAM_TS_KEYS`                       | `gaps.GapStreams.STREAM_TS_KEYS`                   |
| `ENDPOINT_WEIGHTS`                     | `gaps.GapStreams.ENDPOINT_WEIGHTS`                 |
| `BINANCE_REST_BASE`                    | `gaps.GapStreams.BINANCE_REST_BASE`                 |

### 4.6 `src/cli/backfill_scheduler.py`

| Python symbol                          | Java                                       |
| -------------------------------------- | ------------------------------------------ |
| `main` (async)                         | `BackfillScheduler.run`                     |
| `_run_backfill_cycle`                  | `BackfillCycle.run`                         |
| `start_http_server(port=8002)`         | `HealthServer` ctor with `port=8002`         |
| Prometheus gauges/counters             | `BackfillMetrics` (§9)                      |

### 4.7 `src/cli/integrity.py`

| Python symbol                          | Java                                          |
| -------------------------------------- | --------------------------------------------- |
| `cli` (click group)                    | `integrity.IntegrityCli`                       |
| `check_trades` / `check_depth` / `check_bookticker` | `integrity.CheckTradesCommand` / `CheckDepthCommand` / `CheckBooktickerCommand` |
| `_check_trades`                        | `integrity.TradesContinuity.check`             |
| `_check_depth`                         | `integrity.DepthContinuity.check`              |
| `_check_bookticker`                    | `integrity.BooktickerContinuity.check`         |
| `_stream_data_records`                 | `integrity.ContinuityChecker.streamRecords` (private) |
| `CHECKABLE_STREAMS` / `ALL_STREAMS`    | `integrity.ContinuityChecker` constants         |

### 4.8 Symbols dropped

| Python symbol                          | Reason |
| -------------------------------------- | ------ |
| Nothing intentionally dropped — all CLI surface is portable. | — |

---

## 5. Library mapping

| Python dep                    | Java library                                                         | Version                   | Notes                                                        |
| ----------------------------- | -------------------------------------------------------------------- | ------------------------- | ------------------------------------------------------------ |
| `click`                       | `info.picocli:picocli`                                              | `4.7.6`                    | Tier 5 K1–K3. Annotation-driven, ~300 KB, not a framework.    |
| `aiohttp`                     | `java.net.http.HttpClient`                                           | stdlib (Java 21)           | One per CLI process (Tier 5 D3)                              |
| `orjson`                      | `com.fasterxml.jackson.core:jackson-databind`                        | `2.17.2`                   | Via common's `EnvelopeCodec` (Tier 5 B1, B2, B6)             |
| `zstandard`                   | `com.github.luben:zstd-jni`                                          | `1.5.6-4`                  | Both `ZstdInputStream` (Tier 5 I2) and `ZstdOutputStream` (Tier 5 I1 — daily files are sealed in one shot) |
| `prometheus-client`           | `io.micrometer:micrometer-registry-prometheus`                       | `1.13.4`                   | `NamingConvention.identity` (Tier 5 H4); per scheduler         |
| `structlog`                   | `org.slf4j:slf4j-api` + `ch.qos.logback:logback-classic` + `net.logstash.logback:logstash-logback-encoder` | `2.0.13` / `1.5.8` / `7.4` | Reused via common's `LogInit`                                |
| `psycopg[binary]`             | `org.postgresql:postgresql`                                          | `42.7.4`                   | One-shot JDBC for `MaintenanceWriter` only; no HikariCP needed |
| `pydantic-settings`           | `org.hibernate.validator:hibernate-validator`                        | `8.0.1.Final`              | Reused via common config records                             |
| `pyyaml`                      | `com.fasterxml.jackson.dataformat:jackson-dataformat-yaml`           | `2.17.2`                   | Reused via common's `YamlConfigLoader`                        |
| `pytest` / `pytest-asyncio`   | `org.junit.jupiter:junit-jupiter`                                    | `5.11.0`                   | All tests sync on virtual threads (Tier 5 L3)                  |
| `tarfile` (stdlib)            | `org.apache.commons:commons-compress`                                | (already in writer module) | For `seal_daily_archive` tar writer                            |

**Note on `commons-compress`**: The writer's `Tar*` operations (if any) are not in scope for cli; check the writer's `gradle/libs.versions.toml` entries. If `commons-compress` is not yet declared, it should be added with version `1.27.1` (latest 1.x). Open question §11.

No deviations from spec §1.3.

---

## 6. Data contracts

All envelope records and config records reused from `com.cryptolake.common.*`. The CLI does NOT redefine them.

### 6.1 `DataEnvelope` (reused)

Already defined in `com.cryptolake.common.envelope.DataEnvelope`. Field-by-field match per mapping §7. Writer-extended fields preserved; CLI is read-only on archive envelopes — it parses via `JsonNode` (Tier 5 B3) for flexibility on optional restart-metadata fields.

### 6.2 `GapEnvelope` (reused)

Already defined. CLI emits gap envelopes only via `core.MissingHourGapFactory` — exact field set: `v, type=gap, exchange, symbol, stream, received_at, collector_session_id, session_seq=-1, gap_start_ts, gap_end_ts, reason="missing_hour", detail`. Optional restart-metadata fields are NOT emitted by CLI (they are writer-only via `restart_gap_classifier`). Field order via `@JsonPropertyOrder` (Tier 5 B1; common-accepted).

### 6.3 `MaintenanceIntent`

```text
public record MaintenanceIntent(
    String maintenanceId,
    String scope,
    String plannedBy,        // CLI sets this to "cli"
    String reason,
    String createdAt,        // ISO-8601 UTC, Instant.toString()
    String expiresAt) {}     // ISO-8601 UTC
```

Persisted to PG via single INSERT. Tier 5 F1 — `Instant.toString()` for the timestamp form (Z-suffix, fractional millis OK).

### 6.4 `BackfillEnvelope` (a `DataEnvelope` with `source="backfill"`)

The `gaps.backfill` command emits envelopes that include an extra top-level field `"source": "backfill"`. This field is NOT in the canonical `DataEnvelope` record. Two options:

- **Preferred**: extend `DataEnvelope` (in common) with an optional `source` field annotated `@JsonInclude(NON_NULL)` so non-backfill envelopes still serialize without it. Field order: insert `source` between `type` and `exchange` to match `gaps.py:60-77`.
- **Fallback**: emit the backfill envelope as a raw `ObjectNode` with explicit insertion order, bypassing the typed record. This is the simplest path and avoids modifying common. See open question §11.

For the design, we proceed with the **fallback** (raw ObjectNode emission via `BackfillEnvelopeFactory`), to avoid retroactively touching the accepted `common` artifact. Insertion order encoded in the factory: `v, type, source, exchange, symbol, stream, received_at, exchange_ts, collector_session_id, session_seq, raw_text, raw_sha256, _topic, _partition, _offset`.

### 6.5 `ManifestRecord` (consolidation)

See §2.2 for the field order. Written via `ObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(record)`. **Watch-out**: Python's `json.dumps(..., indent=2)` and Jackson's `DefaultPrettyPrinter` differ in the array element separator — Python uses `",\n  "` while Jackson uses `",\n  "` by default; both insert two-space indent. Verify via fixture corpus during gate-5 run; any mismatch means the manifest writer needs a custom `DefaultPrettyPrinter` configuration. Open question §11.

### 6.6 `IntegrityCheckResult` & `Break`

Internal records; serialized to JSON only when `--json` flag is passed. Field order: `record_count, breaks` for the result; `field, expected, actual, missing, at_received` for each break (mirrors `integrity.py`).

### 6.7 Verify-stdout contract (Tier 1+3 invariant — gate 5)

The Java `cryptolake-verify verify` MUST emit stdout byte-identical to the Python `python -m src.cli.verify verify` when invoked against the same archive. Below is the line-by-line specification of every output template, with explicit byte-level details.

#### Line templates

| # | Trigger                                  | Python line                                                                                  | Java equivalent                                                                                                      |
|---|------------------------------------------|----------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| 1 | No files found                           | `No archive files found for date {date} in {base_dir}\n`                                     | `System.out.println("No archive files found for date " + date + " in " + baseDir);`                                  |
| 2 | Per-file header                          | `Verifying: {relative_path}\n`                                                                | `System.out.println("Verifying: " + relativePath);`                                                                  |
| 3 | Sidecar repaired                         | `  Repaired: {sidecar_name}\n`                                                                | `System.out.println("  Repaired: " + sidecarName);`                                                                  |
| 4 | Checksum sidecar missing                 | `Sidecar not found: {sidecar_path}\n`                                                         | identical                                                                                                            |
| 5 | Checksum mismatch                        | `Checksum mismatch for {data_filename}\n`                                                     | identical                                                                                                            |
| 6 | Decompression failed                     | `Decompression failed: {data_path} - {exception}\n`                                           | `errors.add("Decompression failed: " + dataPath + " - " + e.getMessage())`                                            |
| 7 | Missing fields error                     | `Line {i}: missing fields: {sorted_python_repr_list}\n`                                       | Java emits `"Line " + i + ": missing fields: " + pythonReprList(sorted)` where `pythonReprList = "[" + items.stream().map(s -> "'" + s + "'").collect(joining(", ")) + "]"` |
| 8 | raw_sha256 mismatch                      | `Line {i}: raw_sha256 mismatch at offset {offset}\n`                                          | identical (use `String.valueOf(offset)` or `Long.toString(offset)`)                                                  |
| 9 | Duplicate offset                         | `Duplicate broker record: {python_tuple_repr}\n`                                              | `pythonTupleRepr` = `"(" + topicLiteral + ", " + partition + ", " + offset + ")"` where `topicLiteral` is `"None"` if topic is JSON null else `"'" + topic + "'"`. **Watch-out**: Python's tuple repr uses single quotes around strings and `None` for nulls. |
| 10 | Depth replay — first diff no span      | `[{symbol}] First diff does not span any snapshot sync point: U={U}, u={u}, snapshot lids={list_of_first_3} at received_at {ts}\n` | identical; `list_of_first_3` is `"[" + (first three lids joined with ", ") + "]"` (Python list repr — no quotes around ints)             |
| 11 | Depth replay — no preceding snapshot    | `[{symbol}] Depth diff at received_at {ts} has no preceding snapshot\n`                       | identical                                                                                                            |
| 12 | pu chain break                           | `[{symbol}] pu chain break at received_at {ts}: expected pu={x}, got pu={y}\n`                | identical                                                                                                            |
| 13 | Final separator                          | `\n` then `============================================================\n` (60 `=`)            | `System.out.println(); System.out.println("=".repeat(60));`                                                          |
| 14 | "Verification complete..."               | `Verification complete for {date}\n`                                                          | identical                                                                                                            |
| 15 | "Files checked..."                       | `Files checked: {n}\n`                                                                        | identical                                                                                                            |
| 16 | "ERRORS (n):" header                     | `\nERRORS ({n}):\n`                                                                            | `System.out.println(); System.out.println("ERRORS (" + n + "):");`                                                   |
| 17 | Each error line                          | `  - {err}\n`                                                                                  | `System.out.println("  - " + err);`                                                                                   |
| 18 | "Errors: 0"                              | `Errors: 0\n`                                                                                  | identical                                                                                                            |
| 19 | "GAPS (n):" header                       | `\nGAPS ({n}):\n`                                                                              | `System.out.println(); System.out.println("GAPS (" + n + "):");`                                                     |
| 20 | Each gap line                            | `  - {symbol}/{stream}: {reason} ({detail})\n`                                                | `System.out.println("  - " + symbol + "/" + stream + ": " + reason + " (" + detail + ")");`                          |
| 21 | "Gaps: 0"                                | `Gaps: 0\n`                                                                                    | identical                                                                                                            |

#### Byte-level invariants

- **Line endings**: Python `click.echo` emits `\n` on Linux/macOS. Java `System.out.println` emits the platform line.separator. **Override at `Main`**: `System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8) { @Override public void println(String s) { print(s); print('\n'); } });` to force `\n` on all platforms.
- **Charset**: stdout is UTF-8. Force via `PrintStream(..., true, StandardCharsets.UTF_8)`.
- **No trailing newline on the final line in error mode**: the last `click.echo` is the gap or "Gaps: 0" line; verify Java's last call uses `println` (not `print`) to match Python `click.echo` (which always trailing-newlines).
- **Float / long formatting**: pu/u/U are `long` (Tier 5 E1); use `Long.toString(...)` (no scientific notation; matches Python `int` → `str`). `received_at` is `long`; same.
- **Path formatting**: Python's `Path.__str__` uses platform separator. Java's `Path.toString()` does the same. Run only on Linux/macOS in CI; document as a caveat for Windows users (gate 5 fixtures are produced on Linux).
- **Iteration order**: `ArchiveScanner.scan` returns sorted-by-string-path; Python's `sorted(set(...))` does the same. The order of error/gap accumulation MUST match Python's iteration order, which is the file-list order × in-file record-index order. Java implementation: maintain a single `List<String> errors` and append in exactly the same order (file-by-file, line-by-line).

#### Compliance verification — `VerifyStdoutParityHarness`

A dedicated harness (`harness.VerifyStdoutParityHarness`) feeds a Python-produced fixture archive set to the Java `verify` command, captures stdout, and diffs byte-by-byte against the recorded Python output. See §8.5 for the full harness spec. This is the gate-5 enforcement mechanism for `cli`.

### 6.8 ObjectMapper config

Single shared instance per CLI process via `EnvelopeCodec.newMapper()` (already accepted). Settings: `SORT_PROPERTIES_ALPHABETICALLY=false`, `INDENT_OUTPUT=false`, `ORDER_MAP_ENTRIES_BY_KEYS=false`, `FAIL_ON_UNKNOWN_PROPERTIES=false`. The `ManifestCommand` and `ConsolidateDay.writeManifest` use `mapper.writerWithDefaultPrettyPrinter()` — that path is the only one where pretty-printing applies.

---

## 7. Error model

### 7.1 Hierarchy

- `com.cryptolake.common.config.CryptoLakeConfigException extends RuntimeException` — accepted common; thrown on bad config.
- `java.lang.IllegalArgumentException` — fail-fast on invalid CLI args (most caught by picocli before `call()`).
- `java.io.IOException` — wraps Jackson, file I/O, HttpClient. Thrown out of `Callable.call()` → picocli prints stack + returns 2.
- `java.io.UncheckedIOException` — wraps Jackson IO when in stream callbacks (e.g. `Files.walk(...).filter(...)`).
- `gaps.EndpointUnavailableException extends RuntimeException` — Tier 2 §13; runtime, not checked.
- `java.sql.SQLException` — only in `MaintenanceWriter`; surfaces from `Callable.call()` directly (CLI is one-shot, fatal here is correct).
- `InterruptedException` — at every blocking call site in schedulers; restore interrupt flag (Tier 5 A4).

### 7.2 What retries, what kills

| Error                                                  | Policy                                                                  |
| ------------------------------------------------------ | ----------------------------------------------------------------------- |
| Decompression failure on one file (verify)              | Append to errors; continue with next file (matches `verify.py:331-333`)  |
| Missing sidecar                                         | If `--repair-checksums`: regenerate; else error                          |
| Checksum mismatch                                       | Error; continue                                                          |
| Required-field missing                                  | Error (per-line); continue                                               |
| `raw_sha256` mismatch                                   | Error (per-line); continue                                               |
| Duplicate broker offset                                 | Error; continue                                                          |
| Depth pu-chain break                                    | Error if no gap explains it; otherwise re-sync attempt                   |
| HTTP 429 / 418 (gaps backfill)                          | Honor `Retry-After`, retry up to 5 times (Tier 5 D4, D7)                 |
| HTTP 400 / 403 / 5xx (gaps backfill)                    | `EndpointUnavailableException`; mark stream as unrecoverable; continue   |
| Tar packing failure (consolidate seal)                  | Roll back partial `.tar.zst`; raise; cycle metric `consolidation_verification_failures_total++` |
| Daily file verify failure                               | Do NOT delete hourlies (Tier 1 §4 indirect); mark cycle failed; continue |
| `MaintenanceWriter` SQLException                        | Propagate; `mark-maintenance` returns non-zero exit code                  |
| Scheduler `IOException` mid-cycle                       | Catch in cycle; log; update metric; sleep until next interval; continue  |
| `InterruptedException` in scheduler sleep               | Restore interrupt flag; exit loop (Tier 5 A4)                             |

### 7.3 Invariant fail-fast

- `EnvelopeVerifier` and `MissingHourGapFactory` validate `reason ∈ VALID_GAP_REASONS` (Tier 5 M6). On unknown reason: `IllegalArgumentException` from common's `GapReasons.requireValid`.
- Negative durations (e.g. `--ttl-minutes=-1`) → fail-fast in compact constructor or picocli converter.
- Malformed date string → `DateTimeParseException` propagated (matches Python's exception bubble).

---

## 8. Test plan

JUnit 5; `org.assertj:assertj-core` for assertions. All tests live in `<subproject>/src/test/java/com/cryptolake/<subproject>/`. Every ported test method has a `// ports: tests/<path>::test_name` trace comment (Tier 3 §17).

### 8.1 `:verify` unit tests

| JUnit class                        | Method                                              | Port of                                                                |
| ---------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------------- |
| `ChecksumVerifierTest`             | `validChecksumProducesNoErrors`                     | `test_verify.py::TestChecksumVerification::test_valid_checksum`         |
|                                    | `corruptedFileDetected`                             | `::test_corrupted_file`                                                 |
|                                    | `missingSidecarReported`                            | `::test_missing_sidecar`                                                |
| `EnvelopeVerifierTest`             | `validDataEnvelopePasses`                           | `::TestEnvelopeValidation::test_valid`                                  |
|                                    | `sha256MismatchDetected`                            | `::test_sha256_mismatch`                                                |
|                                    | `missingFieldDetected`                              | `::test_missing_field`                                                  |
|                                    | `restartGapEnvelopeWithOptionalFieldsValidates`     | `::TestRestartGapEnvelopeValidation`                                    |
| `DuplicateOffsetCheckerTest`       | `noDuplicates`                                      | `::TestDuplicateOffsets::test_no_duplicates`                            |
|                                    | `duplicateDetected`                                 | `::test_duplicate`                                                      |
|                                    | `negativeOffsetSkipped`                             | (new — Tier 5 M9)                                                        |
|                                    | `tupleReprMatchesPython`                            | (new — gate 5 byte-identity for line 9 in §6.7 table)                   |
| `GapReporterTest`                  | `reportsGapEnvelopesOnly`                           | `::TestGapReporting::test_reports_gaps`                                 |
|                                    | `noGapsReturnsEmpty`                                | `::test_no_gaps`                                                        |
|                                    | `restartGapPreservesAllOptionalFields`              | `::test_restart_gap_preserves_metadata`                                 |
| `DepthReplayVerifierTest`          | `firstDiffSpansSnapshotSyncs`                       | `::TestDepthReplay::test_first_diff_spans_snapshot_syncs`               |
|                                    | `firstDiffNoSpanReportsError`                       | `::test_first_diff_no_span_reports_error`                               |
|                                    | `puChainBreakReportedIfNoGap`                       | `::test_pu_chain_break_no_gap`                                          |
|                                    | `puChainBreakInGapToleratedAndResynced`             | `::test_pu_chain_break_in_gap_resyncs`                                  |
|                                    | `gapBetweenDiffsTolerated`                          | `::test_gap_between_diffs`                                              |
|                                    | `multipleSnapshotsAcceptedAsCheckpoints`            | `::test_multiple_snapshots_checkpoint`                                  |
|                                    | `crossSymbolDiffsIsolated`                          | `::test_cross_symbol_isolation`                                         |
|                                    | (~10 more sub-tests for the full pu-chain matrix)   | full TestDepthReplay class                                              |
| `ArchiveScannerTest`               | `lexOrderMatchesPython`                             | (new — gate 5 ordering)                                                 |
|                                    | `filtersByExchangeSymbolStream`                     | (new — Click option semantics)                                          |
|                                    | `recursivelyDiscoversBaseLateBackfill`              | (new — Tier 5 I7)                                                        |
| `DecompressAndParseTest`           | `streamingDecompressorHandlesMultiFrameZst`         | (new — Tier 5 I2)                                                        |
|                                    | `corruptZstThrows`                                  | (new)                                                                    |
| `ManifestGeneratorTest`            | `generatesExpectedSchema`                           | (new)                                                                    |
|                                    | `keyOrderMatchesPython`                             | (new — Tier 5 B1)                                                        |
| `VerifyCommandTest`                | `cliReturnsZeroOnCleanArchive`                      | port of CliRunner integration test (Tier 5 L4)                           |
|                                    | `cliReturnsOneOnAnyError`                           | (new)                                                                    |
|                                    | `repairChecksumsRegeneratesSidecar`                 | (new)                                                                    |
| `ManifestCommandTest`              | `writesManifestPerStreamDir`                        | port of `test_verify.py`                                                 |
| `MarkMaintenanceCommandTest`       | `writesIntentToPg`                                  | (new — Testcontainers Postgres)                                          |
| `BackfillCommandTest`              | `dryRunShowsPlan`                                   | `test_backfill.py::test_backfill_dry_run_shows_plan`                    |
|                                    | `nonBackfillableStreamsSkipped`                     | `::test_backfill_skips_non_backfillable_streams`                        |
|                                    | `alreadyBackfilledHourSkipped`                      | `::test_backfill_skips_already_backfilled`                              |
| `AnalyzeCommandTest`               | `analyzeFullDayNoGaps`                              | `test_gap_analyzer.py::test_analyze_full_day_no_gaps`                   |
|                                    | `analyzeMissingHours`                               | `::test_analyze_missing_hours`                                          |
|                                    | `backfillFilesCountAsCovered`                       | `::test_analyze_backfill_files_count_as_covered`                        |
|                                    | `jsonOutput`                                        | `::test_analyze_json_output`                                            |
| `BackfillEnvelopeFactoryTest`      | `serializesWithExactInsertionOrder`                 | (new — Tier 5 B1)                                                        |
|                                    | `rawTextSha256MatchesOrjsonOutput`                  | (new — Tier 5 B2 / B6)                                                   |
| `FundingRateInterpolatorTest`      | `interpolatesOneRecordPerSecond`                    | `test_gap_analyzer.py` (interpolation suite)                            |
|                                    | `pricesEmittedAs8DecimalStrings`                    | (new — Tier 5 M4)                                                        |
|                                    | `boundaryAlignmentSecondTickFloors`                 | (new)                                                                    |
|                                    | `nextFundingTimeUtcBoundaries`                      | (new — Tier 5 M16)                                                       |
| `BinanceRestClientTest`            | `429HonorsRetryAfter`                               | (new — Tier 5 D4)                                                        |
|                                    | `400ThrowsEndpointUnavailable`                      | (new)                                                                    |
|                                    | `serverErrorThrowsEndpointUnavailable`              | (new)                                                                    |
| `DeepIdBackfillTest`               | `idAnchoredFetchPaginates`                          | `test_deep_backfill.py`                                                  |
|                                    | `paginationStopsOnEmptyPage`                        | `test_deep_backfill.py`                                                  |
| `IntegrityCheckersTest`            | `tradesAIdContinuityDetectsGap`                     | `test_integrity_gaps.py`                                                 |
|                                    | `depthPuChainContinuity`                            | `test_integrity_gaps.py`                                                 |
|                                    | `booktickerBackwardsJumpDetected`                   | `test_integrity_gaps.py`                                                 |
|                                    | `booktickerForwardJumpAcceptedNoBreak`              | (new — multi-symbol clarification)                                       |

### 8.2 `:consolidation` unit tests

| JUnit class                        | Method                                              | Port of                                                          |
| ---------------------------------- | --------------------------------------------------- | ---------------------------------------------------------------- |
| `HourFileDiscoveryTest`            | `classifiesBaseLateBackfill`                        | (new)                                                            |
|                                    | `lateAndBackfillSortedBySeq`                        | (new — Tier 5 M15)                                                |
| `HourMergerTest`                   | `singleBaseFastPathReturnsAsIs`                     | (new)                                                            |
|                                    | `multipleSourcesSortedByNaturalKey`                 | (new — Tier 5 M5)                                                 |
|                                    | `gapsSplicedByGapStartTs`                           | (new)                                                            |
| `DataSortKeyTest`                  | `tradesUsesAggregateTradeId`                        | (new — Tier 5 E1)                                                |
|                                    | `depthUsesUpdateId`                                 | (new — Tier 5 E1)                                                |
|                                    | `otherStreamsUseExchangeTs`                         | (new)                                                            |
|                                    | `noLossOnIdsAbove2Pow31`                            | (new — Tier 5 E1 watch-out)                                       |
| `MissingHourGapFactoryTest`        | `gapStartEndAtUtcHourBoundaries`                    | (new — Tier 5 F3, M11)                                            |
|                                    | `reasonIsMissingHour`                               | (new — Tier 1 §5, Tier 5 M6)                                      |
|                                    | `sessionSeqIsMinusOne`                              | (new — Tier 5 M10)                                                |
| `DailyFileWriterTest`              | `producesOneFramePerBatchOrSingleStream`            | (new — Tier 5 I1, I2)                                             |
|                                    | `fsyncOnClose`                                      | (new — Tier 5 I3)                                                |
| `DailyFileVerifierTest`            | `verifiesSha256AndOrdering`                         | (new)                                                            |
|                                    | `gapsSkippedInOrderingCheck`                        | (new)                                                            |
| `ConsolidateDayTest`               | `happyPathProducesDailyFile`                        | (new)                                                            |
|                                    | `missingHourEmittedAsGap`                           | (new — Tier 1 §5)                                                 |
|                                    | `verifyFailureSuppressesCleanup`                    | (new — Tier 1 §4 indirect)                                        |
| `SealDailyArchiveTest`             | `tarZstContainsAllStreamFiles`                      | (new)                                                            |
|                                    | `sha256SidecarWritten`                              | (new)                                                            |
| `ScheduleClockTest`                | `nextRunInstantBeforeStartHour`                     | port of `test_consolidation_scheduler.py`                       |
|                                    | `nextRunInstantAtOrAfterStartHourRollsToNextDay`    | port of same                                                     |
|                                    | `dstTransitionsHandledViaUtc`                       | (new — Tier 5 M11)                                                |
| `ConsolidationCycleIT` (`@Tag("integration")`) | `endToEndWithSyntheticArchiveHarness`     | port of integration-style test (uses common's `SyntheticArchiveHarness`) |

### 8.3 `:backfill` unit tests

| JUnit class                | Method                                                  | Port of                              |
| -------------------------- | ------------------------------------------------------- | ------------------------------------ |
| `BackfillSchedulerTest`    | `sleeps6HoursBetweenCycles`                              | (new)                                |
|                            | `cycleRunsExactlyOnceOnRunOnceMode`                      | (new)                                |
| `BackfillCycleTest`        | `delegatesToOrchestrator`                                | (new)                                |

### 8.4 Common harness tests

| JUnit class                        | Method                                              | Notes                                                             |
| ---------------------------------- | --------------------------------------------------- | ----------------------------------------------------------------- |
| `VerifyStdoutParityHarnessTest`    | `runsAgainstFixtureCorpusAndMatchesPythonStdout`    | gate-5 regression; runs `:verify:runVerifyParity` Gradle task     |
| `MetricSkeletonDumpTest` (per scheduler) | `consolidationMetricSkeletonMatchesFixture`   | gate-4 regression for `:consolidation`                            |
|                                    | `backfillMetricSkeletonMatchesFixture`              | gate-4 regression for `:backfill`                                 |

### 8.5 Gate 5 harness — `:verify:runVerifyParity`

`harness.VerifyStdoutParityHarness.main` is the gate-5 enforcement mechanism.

```text
tasks.register<JavaExec>("runVerifyParity") {
    group = "port"
    description = "Run Java verify against Python-produced archive corpus; diff stdout byte-for-byte."
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("com.cryptolake.verify.harness.VerifyStdoutParityHarness")
    args(
        rootProject.file("../parity-fixtures/verify/archive").absolutePath,
        rootProject.file("../parity-fixtures/verify/expected.txt").absolutePath,
        layout.buildDirectory.file("reports/gate5-verify.txt").get().asFile.absolutePath
    )
}
```

Exit 0 = pass; exit 1 = stdout diff or rc mismatch.

The `parity-fixtures/verify/expected.txt` was captured by `port-init` (per spec §1.4: "Snapshot `verify` CLI expected output. Run Python `verify` against step-2 archives; stdout + exit code saved to `parity-fixtures/verify/expected.txt`."). The Java port's contract is to match it byte-identical. **If the harness fails, the gate-5 fixtures of writer, common, AND collector are also at risk** — see §11 open question on coordinated refresh.

### 8.6 Gate 4 harness — `:consolidation:dumpMetricSkeleton` and `:backfill:dumpMetricSkeleton`

Same shape as the writer/collector harnesses. Each scheduler registers its 6–8 meters into a fresh `PrometheusMeterRegistry`, scrapes, canonicalizes (TreeSet, strip `_max`), and writes to `build/reports/<service>-metric-skeleton.txt`. Orchestrator diffs against `parity-fixtures/metrics/{consolidation,backfill}.txt`.

`:verify` does NOT have a metric skeleton — the verify CLI emits no Prometheus metrics (it writes everything to stdout).

---

## 9. Metrics plan

Three independent meter classes — one per long-running app. Names byte-identical to Python (Tier 3 §18). Registered via `Counter.builder("name").register(registry)` with `NamingConvention.identity` set once on construction (Tier 5 H4 — counter names already include `_total` suffix where applicable; Micrometer's default would double-suffix).

### 9.1 `:consolidation` — `ConsolidationMetrics` (8 meters)

| # | Prometheus name                                | Java accessor                                                | Type    | Labels |
| - | ---------------------------------------------- | ------------------------------------------------------------ | ------- | ------ |
| 1 | `consolidation_last_run_timestamp_seconds`     | `void setLastRunTimestamp(double seconds)` (gauge holder)    | Gauge   | none   |
| 2 | `consolidation_last_run_duration_seconds`      | `void setLastRunDuration(double seconds)`                    | Gauge   | none   |
| 3 | `consolidation_last_run_success`               | `void setLastRunSuccess(int value)` — 0 or 1                  | Gauge   | none   |
| 4 | `consolidation_runs_total`                     | `void incRuns()`                                              | Counter | none   |
| 5 | `consolidation_days_processed`                 | `void incDaysProcessed(double n)`                             | Counter | none   |
| 6 | `consolidation_files_consolidated`             | `void incFilesConsolidated(double n)`                         | Counter | none   |
| 7 | `consolidation_verification_failures_total`    | `void incVerificationFailures()`                              | Counter | none   |
| 8 | `consolidation_missing_hours_total`            | `void incMissingHours(double n)`                              | Counter | none   |

### 9.2 `:backfill` — `BackfillMetrics` (6 meters)

| # | Prometheus name                          | Java accessor                                  | Type    | Labels |
| - | ---------------------------------------- | ---------------------------------------------- | ------- | ------ |
| 1 | `backfill_last_run_timestamp_seconds`    | `void setLastRunTimestamp(double seconds)`     | Gauge   | none   |
| 2 | `backfill_last_run_duration_seconds`     | `void setLastRunDuration(double seconds)`      | Gauge   | none   |
| 3 | `backfill_gaps_found`                    | `void setGapsFound(double n)`                  | Gauge   | none   |
| 4 | `backfill_records_written`               | `void setRecordsWritten(double n)`             | Gauge   | none   |
| 5 | `backfill_last_run_success`              | `void setLastRunSuccess(int value)`             | Gauge   | none   |
| 6 | `backfill_runs_total`                    | `void incRuns()`                                | Counter | none   |

**Note on gauge vs counter for `*_total` Python metrics**: Python `prometheus-client` `Counter("name_total", ...)` registers with literal `_total` suffix. Micrometer `Counter.builder("name")` adds `_total` automatically when scraped. Tier 5 H4 watch-out: register with the bare name (without `_total`); Prometheus scrape appends correctly. Apply `NamingConvention.identity` to ensure no other transforms.

### 9.3 `:verify` — no metrics

Verify is a one-shot CLI; output goes to stdout. No metrics surface.

### 9.4 Metric-parity generated tests

Per scheduler: `MetricSkeletonDumpTest` runs the dump harness, canonicalizes, and asserts equal to a fixture file. Orchestrator gate 4 diffs the live build's report against `parity-fixtures/metrics/{consolidation,backfill}.txt`.

Histogram buckets: NONE in this module — all CLI metrics are simple counters/gauges. Tier 5 H5 N/A.

---

## 10. Rule compliance

All rules from Tiers 1, 2, 3, 5. Each entry: `honored by <class/method>` or `N/A because <reason>`.

### Tier 1 — Invariants

1. **raw_text captured pre-parse from WebSocket callback** — N/A: CLI does not capture raw_text; it consumes pre-recorded archives. CLI verifies the captured `raw_text` byte-identity via `EnvelopeVerifier` re-hash (which honors §2 instead).
2. **raw_sha256 computed over raw_text bytes once at capture** — honored by `verify.EnvelopeVerifier.verify`: re-hashes via `Sha256.hexDigestUtf8(env.get("raw_text").asText())` (lowercase hex; matches `hashlib.sha256().hexdigest()`) and compares; mismatches reported as errors. Read-only validation; never re-emits the field.
3. **Disabled streams emit zero artifacts** — honored: CLI does not emit Kafka or stream artifacts. Backfill envelopes (gaps.backfill) are only emitted for streams in `BACKFILLABLE_STREAMS` (§4.5). Disabled streams in config are simply skipped at the archive layer.
4. **Kafka offsets committed only after fsync-completed flush** — N/A: CLI does not commit Kafka offsets. Indirect honor in consolidate: `HourlyCleanup.delete` is invoked only after `DailyFileVerifier.verify` succeeds — analogous discipline (delete-after-fsync-and-verify).
5. **Every gap emits metric + log + archived record** — honored by `MissingHourGapFactory` + `ConsolidationCycle`: every missing hour produces (a) a `consolidation_missing_hours_total` counter increment, (b) a structured log `missing_hour_synthesized` with `(exchange, symbol, stream, date, hour)`, (c) an archived gap envelope inside the daily file. The triad is colocated in `ConsolidationCycle.run` so review can verify all three are emitted side-by-side.
6. **Recovery prefers replay over reconstruction** — honored by `gaps.BackfillCommand`: REST replay is the primary path; `FundingRateInterpolator` (reconstruction) is only invoked for `funding_rate` boundary records and only when no REST data is available for the precise gap window. All other streams either backfill from REST or are reported as unrecoverable. `consolidate.MissingHourGapFactory` does NOT reconstruct — it explicitly emits a `missing_hour` gap envelope (mapping §10 risk).
7. **JSON codec preserves raw_text byte-for-byte** — honored by reused common `EnvelopeCodec` + the CLI never modifies `raw_text` on output. `ConsolidateDay.run` writes envelopes via `mapper.writeValueAsBytes(record)` (Tier 5 B2). `gaps.BackfillEnvelopeFactory` emits via the same mapper. **Watch-out**: `_RE_BACKFILL` files are NEW — the `raw_text` they contain comes from `mapper.writeValueAsString(restRecord)` and must match `orjson.dumps(rest_record).decode()` byte-for-byte; gate 5 catches any divergence.

### Tier 2 — Java practices

8. **Java 21 only** — honored: records, sealed interfaces, virtual threads, switch expressions, `instanceof` patterns. `Files.walk`, `HttpClient`, `BufferedReader.lines()` Stream API.
9. **No `synchronized` around blocking calls** — honored: no `synchronized` blocks anywhere. Schedulers use `CountDownLatch.await(timeout)` for pacing.
10. **No `Thread.sleep` in hot paths** — honored: `Thread.sleep` is used only in `BinanceRestClient.fetchPage` retry (Tier 5 D4 — outside hot path; one HTTP call per minute at most). Schedulers use `stopLatch.await(...)` (Tier 5 A3).
11. **No reflection-heavy frameworks** — honored: no Spring/Micronaut/Quarkus/CDI/Guice. Picocli is annotation-driven CLI parsing only (~300 KB; not a DI/IoC framework). Hibernate Validator (used via reused common config) is bean-validation only. Explicit wiring in each `Main`.
12. **Immutable records, no setters** — honored: every data shape is a `record` (`ArchiveFile`, `MaintenanceIntent`, `ManifestRecord`, `IntegrityCheckResult`, `Break`, `HourFileGroup`, `WriteStats`, `VerifyResult`). No setters.
13. **No checked exception leaks across module boundaries** — honored: `EndpointUnavailableException` is `RuntimeException`. `IOException` from picocli `Callable.call` is wrapped to picocli's exit-code handler. `CryptoLakeConfigException` is `RuntimeException`.
14. **Single `HttpClient` / `KafkaProducer` / `ObjectMapper` per service** — honored: each Main creates exactly one `HttpClient` (Tier 5 D3) and one `ObjectMapper` (Tier 5 B6); no `KafkaProducer` (CLI does not produce to Kafka).
15. **JSON logs via Logback + Logstash + MDC** — honored via reused common `LogInit`. Schedulers wrap each cycle in `MDC.putCloseable("cycle_id", uuid)` (Tier 5 H3); CLI commands use `MDC.putCloseable("command", "<verify|manifest|...>")`.
16. **Fail-fast on invariant violation; retry only declared-transient errors** — honored: bad config throws `CryptoLakeConfigException` at startup; bad `--ttl-minutes` etc. throws `IllegalArgumentException` via picocli; retries are limited to declared transient: HTTP 429/418 (Tier 5 D4), generic 5xx via `EndpointUnavailableException` paths; no blanket retry.

### Tier 3 — Parity rules

17. **Every Python test has a JUnit 5 counterpart with trace comment** — honored via §8: every ported test method has a `// ports: tests/<path>::test_name` comment.
18. **Prometheus metric names + labels diff-match Python** — honored: §9 enumerates all 14 metrics with exact names. Gate 4 harnesses (§8.6) enforce.
19. **raw_text / raw_sha256 byte-identity via fixture corpus** — honored by `EnvelopeVerifier` (validates) + `BackfillEnvelopeFactory` (must produce byte-identical output for new envelopes — see §6.7 watch-out and gate 5).
20. **Python `verify` CLI passes on Java archives** — N/A in reverse direction: this module IS the Java `verify` CLI. The forward direction — Java `verify` produces stdout byte-identical to Python `verify` on the same archive — is enforced by `VerifyStdoutParityHarness` (§8.5). This is the gate-5 mechanism for `cli` AND the regression test for the gate-5 fixtures of common, writer, and collector.
21. **Envelope JSON field order follows Python canonical order** — honored: reused common's `@JsonPropertyOrder`. `BackfillEnvelopeFactory` and `ManifestRecord` insert keys in Python order (§6.4, §6.5).

### Tier 5 — Translation patterns

- **A1** — N/A: CLI has no fan-out concurrency. Schedulers loop sequentially.
- **A2** — honored: `BinanceRestClient.fetchPage`, `MaintenanceWriter.write`, `ConsolidationCycle.run` all run as direct blocking calls on virtual threads where applicable.
- **A3** — honored by `ConsolidationScheduler.run` and `BackfillScheduler.run`: `stopLatch.await(timeoutNanos, NANOSECONDS)` for the outer sleep.
- **A4** — honored: every `catch (InterruptedException e)` restores the flag and exits the loop.
- **A5** — N/A: no shared mutable state requiring `ReentrantLock`.
- **A6** — N/A: no fire-and-forget paths.
- **A7** — honored: no `cancel_tasks` analog.
- **A8** — honored: no uvloop analog.
- **B1** — honored: `BackfillEnvelopeFactory`, `ManifestRecord`, `MissingHourGapFactory` use insertion-ordered `ObjectNode` or `@JsonPropertyOrder`. `EnvelopeCodec` (reused) is the canonical serializer.
- **B2** — honored: daily file writes use `mapper.writeValueAsBytes(record)` — no indent. Newlines appended explicitly as `(byte) '\n'`. **Watch-out**: `ManifestCommand` uses `writerWithDefaultPrettyPrinter()` deliberately for human-readable manifest JSON (matches Python `json.dumps(..., indent=2)`); see §6.5.
- **B3** — honored: CLI parses archive envelopes as `JsonNode` (read-only flexibility for optional restart-metadata fields).
- **B4** — N/A: CLI consumes `raw_text` as a string field of an envelope; never extracts from a WebSocket frame.
- **B5** — N/A: no `BigDecimal` in CLI envelopes; funding rate prices are emitted as 8-decimal strings via `String.format("%.8f", ...)` (Tier 5 M4).
- **B6** — honored: each `Main` creates exactly one `ObjectMapper` via `EnvelopeCodec.newMapper()`.
- **B7** — honored via reused common `YamlConfigLoader` (only used for the `--config` path of schedulers).
- **C1–C8** — N/A: CLI does not connect to Kafka.
- **D1, D2** — N/A: CLI does not use WebSockets.
- **D3** — honored: each Main constructs one `HttpClient`. `BinanceRestClient`, `KlineFetcher`, `DeepIdBackfill` share it.
- **D4** — honored by `BinanceRestClient.fetchPage`: 429/418 read `Retry-After`; 400/403/5xx throw `EndpointUnavailableException`.
- **D5** — honored: `BinanceRestClient` uses `BodyHandlers.ofByteArray()` + `new String(body, UTF_8)`.
- **D6** — N/A: no long-lived WebSocket.
- **D7** — honored: `BinanceRestClient.fetchPage` exponential backoff inline (no retry library).
- **E1** — honored: all sort keys (`a`, `u`, `U`, `pu`) parsed via `.asLong(...)` in `DataSortKey`, `DepthReplayVerifier`, `TradesContinuity`, `DepthContinuity`, `BooktickerContinuity`. Receipt timestamps `long`. `_offset` `long`.
- **E2** — honored: `received_at` ns is `long`; never widened to `BigInteger` or narrowed to `int`.
- **E3** — honored: Binance numeric strings are NOT parsed as numbers in the verify path (they pass through `raw_text`). The `gaps.FundingRateInterpolator` parses to `double` for arithmetic but emits as `String.format("%.8f", ...)` (Tier 5 M4) — never `BigDecimal`.
- **E4** — honored: any ns→ms conversion uses `/ 1_000_000L` (long integer division).
- **E5** — honored: `MissingHourGapFactory` uses `instant.getEpochSecond() * 1_000_000_000L + instant.getNano()`.
- **F1** — honored: `MarkMaintenanceCommand` uses `Instant.now().toString()` and `now.plus(...).toString()`.
- **F2** — honored: any timestamp parse from PG round-trip uses `OffsetDateTime.parse(...)` first, falling back to `LocalDateTime.parse(...).atOffset(UTC)` (only relevant in `MaintenanceWriter` if it ever reads back; current code only writes).
- **F3** — honored: `MissingHourGapFactory`, `ScheduleClock`, `ConsolidationCycle` all use `ZoneOffset.UTC` exclusively.
- **F4** — honored: schedulers' sleep timing uses `System.nanoTime()` for monotonic wait windows; absolute scheduling via `Instant.now()` is acceptable for once-per-day cadence (insensitive to NTP steps <<1h).
- **G1** — honored: `HourlyCleanup.delete` swallows `IOException` per file; `MaintenanceWriter` swallows close-time `SQLException` in `finally`.
- **G2** — honored via reused common `CryptoLakeConfigException`.
- **G3** — honored: `BinanceRestClient.fetchPage` inlines exp-backoff retry (no library).
- **G4** — honored: `DecompressAndParse.parse` catches `IOException` per line in `verify` (matches `verify.py:331-333`); on per-record JSON parse error, the bad line is skipped (matches Python `for line in ... if line: orjson.loads(line)` behavior — empty lines skipped, malformed lines surface as exceptions to the caller).
- **G5** — honored: no `wait_for` analog.
- **H1** — honored: `private static final Logger log = LoggerFactory.getLogger(<class>.class);` per class.
- **H2** — honored: `log.info("event_name", kv("k", v), ...)` style.
- **H3** — honored: schedulers wrap cycles in `MDC.putCloseable("cycle_id", ...)`.
- **H4** — honored: `ConsolidationMetrics` and `BackfillMetrics` constructors set `registry.config().namingConvention(NamingConvention.identity)` and register counters without `_total` suffix.
- **H5** — N/A: no histograms in CLI.
- **H6** — honored: gauges via supplier-backed `MetricHolders` strong-referenced inner class.
- **I1** — honored: `DailyFileWriter` (consolidation) writes one daily file per `(symbol, stream, date)` — file is sealed in one shot, so `ZstdOutputStream` is acceptable. Hourly-file-style multi-frame zstd is the writer module's concern; consolidation produces a sealed daily blob. **Watch-out**: if multi-frame is required for stream-mode passthrough (decompress → re-compress per source file), consider per-source frames; default sealed approach prefers one frame.
- **I2** — honored: `DecompressAndParse.parse/streamLines` uses `ZstdInputStream` + `BufferedReader` (Tier 5 I2 verbatim).
- **I3** — honored: `DailyFileWriter` calls `fc.force(true)` before close (durability). `Sha256Sidecar.write` (reused) writes the sidecar after.
- **I4** — N/A: consolidation writes daily files atomically (write to final path then verify); no partial-frame truncation logic needed because the file is verified before hourlies are deleted.
- **I5** — honored via reused `Sha256.hexFile` (writer-accepted helper).
- **I6** — honored: `Sha256Sidecar.write` writes directly to the final path.
- **I7** — honored: `ArchiveScanner.scan` uses `Files.walk(baseDir)` with try-with-resources.
- **J1** — honored via reused common config records.
- **J2** — honored.
- **J3** — honored.
- **J4** — honored via reused common `EnvOverrides` for `--base-dir` default.
- **K1** — honored: each Main is a picocli `@Command(name = ...)`. Subcommands via `subcommands = {...}`. Group/sub-group structure mirrors Python's click groups.
- **K2** — honored: all CLI output via `System.out.println(...)` (or the contract-fixed `println` override in §6.7). Logs go to SLF4J / stderr via Logback.
- **K3** — honored: `Callable<Integer>` returns picocli exit code; never `System.exit(...)` from inside a command (kills tests).
- **L1** — honored: test base classes have `loadFixture(name)` helpers.
- **L2** — honored: `Testcontainers` for the `MaintenanceWriter` PG integration test only.
- **L3** — honored: no async test framework.
- **L4** — honored: `VerifyCommandTest`, `BackfillCommandTest`, `AnalyzeCommandTest` use `new CommandLine(new <Cmd>()).setOut(stringWriter).execute(args)` (Tier 5 L4).
- **L5** — honored: fixture archive corpus copied byte-for-byte from `parity-fixtures/verify/archive/`.
- **M1** — honored: any URL building (only in `BinanceRestClient` for backfill REST URLs) applies `.toUpperCase(Locale.ROOT)`.
- **M2** — N/A: CLI does not subscribe to streams.
- **M3** — N/A: CLI is read-only on archive raw_text.
- **M4** — honored: `FundingRateInterpolator` emits prices via `String.format(Locale.ROOT, "%.8f", value)`.
- **M5** — honored: `DataSortKey.of` returns `long`.
- **M6** — honored: gap reasons are `String` (validated against `GapReasons.VALID` set from common).
- **M7** — honored: backfill envelopes use `collector_session_id = "backfill-" + java.util.UUID.randomUUID()` per Python's pattern (no Instant suffix; matches `gaps.py` session_id construction).
- **M8** — honored: `_partition` is `int`, `_offset` is `long`.
- **M9** — honored: `DuplicateOffsetChecker` skips `_offset < 0` records.
- **M10** — honored: `MissingHourGapFactory` sets `session_seq = -1L`.
- **M11** — honored: `ScheduleClock`, `MissingHourGapFactory`, all date math use `ZoneOffset.UTC`.
- **M12** — N/A: CLI does not produce to Kafka.
- **M13** — N/A: same.
- **M14** — N/A: file rotation is writer-only.
- **M15** — honored: `HourFileDiscovery` regex matches `hour-{H}.late-{seq}` and `hour-{H}.backfill-{seq}` exactly.
- **M16** — honored: `FundingRateInterpolator.computeNextFundingTime` uses 0/8/16 UTC boundaries.

---

## 11. Open questions for developer

Each question lists a preferred path and a fallback. The developer MUST NOT invent a different answer — if preferred path blocks, escalate.

### Q1. **Where should `consolidate` library code live so both `:consolidation` (runtime) and `:verify` (tests) can reuse it?**

- **Preferred**: keep `consolidate` as `:consolidation`'s public API and have `:verify` depend on it via `testImplementation(project(":consolidation"))`. The verify integration tests (`ConsolidationCycleIT`) live in `:consolidation`; cross-module test utilities are not common practice.
- **Fallback**: if test cycles require `:verify` to also invoke consolidate at runtime (e.g. for round-trip tests), add `implementation(project(":consolidation"))`. Document the dependency in §1.1 and the build files.

### Q2. **How is gate-5 byte-identity enforced on day 1, before the Java verify is feature-complete?**

- **Preferred**: implement `VerifyCommand` end-to-end against the fixture archive corpus (§8.5). Run `:verify:runVerifyParity` as the first integration target. Iterate on output formatting (§6.7 Line templates) until byte-diff is zero. **The harness IS the spec.**
- **Fallback**: if a single byte-divergence cannot be reconciled (e.g. Python repr of a tuple where one element is `None` does not transparently map to Java `String.valueOf`), refresh `parity-fixtures/verify/expected.txt` from the new Java output AFTER review confirms the divergence is semantically equivalent. This requires re-running gate 5 for common, writer, AND collector — coordinate with orchestrator. Treat as escalation.

### Q3. **Should the backfill envelope's extra `source` field extend `DataEnvelope` or use a raw `ObjectNode`?**

- **Preferred** (per §6.4): emit via raw `ObjectNode` in `BackfillEnvelopeFactory`. Avoids modifying the accepted `common` artifact and keeps the backfill schema deviation contained.
- **Fallback**: extend `DataEnvelope` with `@JsonProperty("source") @JsonInclude(NON_NULL) String source` — requires re-acceptance of the common module and gate-3 fixture refresh. Only adopt if the raw `ObjectNode` approach proves brittle (e.g. accidental field-order regression).

### Q4. **Does Jackson's `DefaultPrettyPrinter` produce byte-identical output to Python's `json.dumps(..., indent=2)`?**

- **Preferred**: empirically validate during gate-5 fixture build. If divergent, register a `CustomPrettyPrinter` that uses two-space indent and `": "` after keys (Python's exact format).
- **Fallback**: if even a `CustomPrettyPrinter` cannot match (rare nested-array-edge-cases), accept the divergence for `manifest.json` files (which are not part of gate-5 stdout, only gate-5 file content) and update fixture comparison tooling to JSON-semantic-diff manifest files. Escalate.

### Q5. **Should the schedulers' SIGTERM hook be installed in `Main` directly or via a shared `ShutdownHooks` utility?**

- **Preferred**: directly in each `Main`. The pattern is 5 lines (`Runtime.getRuntime().addShutdownHook(new Thread(() -> { stopLatch.countDown(); shutdownLatch.countDown(); }));`). Sibling `:writer` and `:collector` Mains do the same.
- **Fallback**: extract to `com.cryptolake.common.ShutdownHooks.register(Runnable cleanup)` once a third call site emerges. Premature today.

### Q6. **`commons-compress` dependency — already declared, or new in the cli module?**

- **Preferred**: check `cryptolake-java/gradle/libs.versions.toml` (currently has no `commons-compress` entry). If `:writer` does not depend on it, add a new `versions` entry `commonsCompress = "1.27.1"` and a library entry `commons-compress = { module = "org.apache.commons:commons-compress", version.ref = "commonsCompress" }`. Use only in `:consolidation/seal/`.
- **Fallback**: if writer added it post-acceptance, reuse the same version.

### Q7. **Should `gaps.backfill` use `StructuredTaskScope` to fan out per-stream/per-symbol REST calls?**

- **Preferred**: NO. Python's `gaps.py` is sequential. Tier 5 D7 / G3 advocate keeping retries inline. Per-symbol parallelism is a future optimization; ship sequential to match Python behavior.
- **Fallback**: if a backfill cycle exceeds the 6-hour scheduler interval, introduce per-symbol fan-out via `StructuredTaskScope.ShutdownOnFailure` (Tier 5 A1). Limit concurrency to `Runtime.availableProcessors() * 2` to respect Binance rate limits (see `ENDPOINT_WEIGHTS`).

### Q8. **`HttpClient` lifecycle in one-shot CLI — explicit close or rely on JVM exit?**

- **Preferred**: rely on JVM exit. `HttpClient` is GC-clean; no explicit close API exists in Java 21 (`HttpClient.close()` is Java 22+).
- **Fallback**: register a JVM shutdown hook to await in-flight requests. Only adopt if a future Java version adds `HttpClient.close()` and lints recommend explicit cleanup.

### Q9. **For `MaintenanceWriter`, is `DriverManager.getConnection` acceptable, or do we need HikariCP?**

- **Preferred**: `DriverManager` for one-shot CLI. The Maintenance command writes one row per invocation; pool overhead is wasted.
- **Fallback**: HikariCP if the maintenance command becomes part of a larger long-running tool. Today's design: avoid.

### Q10. **`DepthReplayVerifier`'s `gap_windows` includes `depth_snapshot` gaps in addition to `depth` — is this still correct in Java?**

- **Preferred**: YES. Mirror Python `verify.py:120-122` exactly: include both `(symbol, "depth")` and `(symbol, "depth_snapshot")` gap envelopes in the `gap_windows` list. The depth-snapshot stream's gaps explain depth-stream pu-chain breaks because they are correlated subscription failures.
- **Fallback**: none — this is a Python invariant.

### Q11. **`BackfillEnvelopeFactory` raw_text byte-identity to Python `orjson.dumps` — is this guaranteed by Jackson default config?**

- **Preferred**: empirically validate via a unit test (`BackfillEnvelopeFactoryTest.rawTextSha256MatchesOrjsonOutput`). If divergence exists (e.g. Jackson formats `0` as `0` but `orjson` formats as `0`; both are equivalent — but pathological cases like NaN or Inf may differ), document the contract and add a custom `JsonGenerator` configuration. The most likely difference is unicode escaping: `orjson` emits non-ASCII as UTF-8 bytes; Jackson default also emits UTF-8 bytes (no `\u` escaping for BMP chars). Should be identical.
- **Fallback**: if any divergence is found, accept it as a `BACKFILL_RAW_TEXT_DIVERGENCE` known-issue and document; backfill envelopes' `raw_sha256` is computed over the Java-serialized bytes consistently, so internal verification still passes; only Python's verify on a Java-produced backfill envelope would mismatch — which is a gate-5 failure if exercised. Escalate.

### Q12. **Should `ConsolidationCycle` parallelize across streams within a single (exchange, symbol) directory?**

- **Preferred**: NO. Python's `_run_consolidation_cycle` is sequential. Match Python behavior; defer parallelism to a future optimization.
- **Fallback**: if cycle time exceeds the daily window after months of growth, fan out via `StructuredTaskScope` (Tier 5 A1). Cap at 4 concurrent (disk and zstd-jni are CPU-bound, not I/O-bound).

### Q13. **Output stream override in `Main` — how to enforce `\n` line endings on Windows for cross-platform reproducibility?**

- **Preferred** (per §6.7): override `System.out` in `Main` via a custom `PrintStream` that always writes `\n` (not `\r\n`). One line of code; no portability cost on Linux/macOS; correct gate-5 byte-identity on any host.
- **Fallback**: gate-5 only runs on Linux CI runners; document Windows as unsupported for `verify` byte-identity. Less robust; only accept if the override creates a regression.

### Q14. **For consolidation `FileChannel.force(true)` — is the explicit fsync required when the stream-close path is `BufferedOutputStream → ZstdOutputStream → Channels.newOutputStream(fc)`?**

- **Preferred**: YES, explicit `fc.force(true)` BEFORE closing the stream is required. `Channels.newOutputStream` does NOT force on close. Pattern: flush bufOut → flush zstdOut → fc.force(true) → close. Tier 5 I3 mandates `force(true)`.
- **Fallback**: if a future zstd-jni release exposes a `flush(true)` that maps to fsync, use that. Today: explicit `force` only.

### Q15. **`DepthReplayVerifier` — should the per-symbol pu-chain walk be parallelized via `StructuredTaskScope`?**

- **Preferred**: NO. Python is sequential per symbol; Java match. The pu-chain walk per symbol is O(diffs) and CPU-bound; hundreds of symbols × thousands of diffs each fits in a few seconds on one core. Adding parallelism complicates error message ordering — the gate-5 fixture's per-symbol error block ordering depends on the iteration order of the symbol map, which Python's dict preserves as insertion order. Java's `LinkedHashMap` does the same; sequential traversal preserves the order.
- **Fallback**: if per-call latency becomes a bottleneck, parallelize per-symbol while keeping the final error list reassembled in deterministic symbol-iteration order (sort by symbol name lexicographically).
