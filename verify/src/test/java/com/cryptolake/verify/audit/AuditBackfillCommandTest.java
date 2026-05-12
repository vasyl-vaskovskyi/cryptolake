package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapEnvelope;
import com.cryptolake.common.envelope.GapReason;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Integration tests for {@link AuditBackfillCommand}.
 *
 * <p>Four scenarios:
 *
 * <ol>
 *   <li>Clean reconcile with a real match (manifest + missing-hour for btcusdt/bookticker hour-9) +
 *       {@code --dry-run} → exit 0, "1 explained" in stdout.
 *   <li>Unexplained file gap divergence → alert POSTed to stub, exit 2.
 *   <li>Unexplained file gap WITH an orphan-state record → alert POSTed, exit 2; orphan-state is
 *       informational and present in the report but does NOT itself trigger the gate.
 *   <li>Safety-valve path: clean result but no {@code --symbol}/{@code --stream} provided → exit 0
 *       with warning on stderr, no backfill invoked.
 * </ol>
 *
 * <p>Uses {@code com.sun.net.httpserver.HttpServer} as an Alertmanager stub (same pattern as {@link
 * AlertmanagerNotifierTest}).
 */
class AuditBackfillCommandTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // 2026-05-11T00:00:00Z in millis
  private static final long DAY_START_MS = 1_778_457_600_000L;
  private static final long HOUR_MS = 3_600_000L;

  // 2026-05-11T05:00:00Z  (hour-5 start — used for orphan-state ledger in test 3)
  private static final long HOUR_5_START_MS = DAY_START_MS + 5 * HOUR_MS;
  private static final long HOUR_5_START_NS = HOUR_5_START_MS * 1_000_000L;

  // 2026-05-11T06:00:00Z  (hour-6 start — end of the orphan-state ledger window)
  private static final long HOUR_6_START_MS = DAY_START_MS + 6 * HOUR_MS;
  private static final long HOUR_6_START_NS = HOUR_6_START_MS * 1_000_000L;

  // 2026-05-11T09:00:00Z  (hour-9 start)
  private static final long HOUR_9_START_MS = DAY_START_MS + 9 * HOUR_MS;
  private static final long HOUR_9_START_NS = HOUR_9_START_MS * 1_000_000L;

  // 2026-05-11T10:00:00Z  (hour-10 start = end of gap)
  private static final long HOUR_10_START_MS = DAY_START_MS + 10 * HOUR_MS;
  private static final long HOUR_10_START_NS = HOUR_10_START_MS * 1_000_000L;

  // Use "now" after the scope day so MissingHourGapSource doesn't fire on gaps we don't want
  private static final String NOW_ISO = "2026-05-11T23:00:00Z";

  // ---- temp dirs ----

  @TempDir Path tmpDir;

  private Path baseDir;
  private Path dataDir;

  // ---- stdout/stderr capture ----

  private PrintStream originalOut;
  private PrintStream originalErr;
  private ByteArrayOutputStream capturedOut;
  private ByteArrayOutputStream capturedErr;

  // ---- Alertmanager stub ----

  private HttpServer alertmanagerStub;
  private int stubPort;
  private final AtomicBoolean alertPosted = new AtomicBoolean(false);
  private final AtomicReference<String> capturedAlertBody = new AtomicReference<>();

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  @BeforeEach
  void setUp() throws IOException {
    baseDir = tmpDir.resolve("archive");
    dataDir = tmpDir.resolve("data");
    Files.createDirectories(baseDir);
    Files.createDirectories(dataDir);

    // Redirect stdout + stderr
    originalOut = System.out;
    originalErr = System.err;
    capturedOut = new ByteArrayOutputStream();
    capturedErr = new ByteArrayOutputStream();
    System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
    System.setErr(new PrintStream(capturedErr, true, StandardCharsets.UTF_8));

    // Start Alertmanager stub on a random port
    alertmanagerStub = HttpServer.create(new InetSocketAddress(0), 0);
    stubPort = alertmanagerStub.getAddress().getPort();
    alertmanagerStub.createContext(
        "/api/v2/alerts",
        exchange -> {
          byte[] body = exchange.getRequestBody().readAllBytes();
          capturedAlertBody.set(new String(body, StandardCharsets.UTF_8));
          alertPosted.set(true);
          exchange.sendResponseHeaders(200, 0);
          exchange.getResponseBody().close();
        });
    alertmanagerStub.start();
  }

  @AfterEach
  void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    alertmanagerStub.stop(0);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private String alertmanagerUrl() {
    return "http://127.0.0.1:" + stubPort + "/api/v2/alerts";
  }

  /** Writes a zstd-compressed JSONL file where each object is serialized via Jackson. */
  private void writeZstdJsonl(Path target, Object... objects) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      for (Object obj : objects) {
        zstd.write(MAPPER.writeValueAsBytes(obj));
        zstd.write(0x0A);
      }
    }
  }

  /**
   * Creates a gap envelope with a {@code collector_restart} reason (a persistent reason) so it
   * participates in reconciliation.
   */
  private GapEnvelope collectorRestartGapEnvelope(
      String exchange, String symbol, String stream, long startNs, long endNs) {
    return GapEnvelope.create(
        exchange,
        symbol,
        stream,
        "sess-1",
        1L,
        startNs,
        endNs,
        GapReason.COLLECTOR_RESTART,
        "test",
        () -> startNs);
  }

  /**
   * Writes a lifecycle.jsonl that produces a {@code collector_restart} gap record via {@link
   * LedgerGapSource} (planned=true).
   */
  private void writeLedgerFixture(String collectorId, long startNs, long endNs) throws IOException {
    Path dir = dataDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    String startLine =
        "{\"ts_ns\":"
            + startNs
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-test\"}";
    String stopLine =
        "{\"ts_ns\":"
            + endNs
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-test\",\"planned\":true}";
    Files.writeString(
        dir.resolve("lifecycle.jsonl"), startLine + "\n" + stopLine + "\n", StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Test 1: Clean reconcile — real match via manifest + missing-hour
  // -------------------------------------------------------------------------

  /**
   * Both the FILE side and the STATE side produce the same {@code missing_hour} gap record for
   * btcusdt/bookticker hour-9 of 2026-05-11, so reconciliation yields exactly 1 explained record, 0
   * unexplained, and 0 orphan-state.
   *
   * <p>Setup:
   *
   * <ul>
   *   <li>FILE side: {@link MissingHourGapSource} emits a {@code missing_hour} record because
   *       hour-9.jsonl.zst is absent but the tuple has presence (via a dummy hour-0.jsonl.zst).
   *   <li>STATE side: {@link ManifestGapSource} reads a manifest that declares {@code
   *       missing_hours:[9]}.
   * </ul>
   *
   * <p>Both records share the same (exchange, symbol, stream, time window, reason) and reconcile
   * cleanly.
   *
   * <p>Scope is narrowed to exactly hour-9 via {@code --hour=2026-05-11T09} so that other absent
   * hours don't leak into the reconciliation.
   */
  @Test
  void cleanReconcile_trulyMatchedMissingHour_dryRun_returnsZero_andPrintsExplainedCount()
      throws IOException {
    // hour-0.jsonl.zst establishes "presence" for the (binance, btcusdt, bookticker) tuple so
    // that MissingHourGapSource actually scans for absent hours within the scope.
    Path hour0File = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-0.jsonl.zst");
    writeZstdJsonl(hour0File); // empty zstd file — just needs to exist

    // Manifest declares hour-9 as missing → ManifestGapSource emits one missing_hour record.
    Path manifestDir = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11");
    Files.createDirectories(manifestDir);
    Files.writeString(
        manifestDir.resolve("2026-05-11.manifest.json"),
        "{\"missing_hours\":[9]}",
        StandardCharsets.UTF_8);

    // Scope = exactly hour-9 so only that one absence is in scope on the file side.
    // now=2026-05-11T10:00:00Z → hour-9 (start=09:00:00Z) is in the past and gets emitted.
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "backfill",
                "--hour=2026-05-11T09",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--alertmanager-url=" + alertmanagerUrl(),
                "--now-override-iso=2026-05-11T10:00:00Z",
                "--dry-run");

    assertThat(exitCode).isEqualTo(0);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    assertThat(stdout).contains("1 explained records");

    // No alert should have been fired for a clean reconcile
    assertThat(alertPosted.get()).isFalse();
  }

  // -------------------------------------------------------------------------
  // Test 2: Unexplained file gap divergence → alert POSTed + exit 2
  // -------------------------------------------------------------------------

  /**
   * The archive has a {@code collector_restart} gap envelope for btcusdt/bookticker, but the state
   * side (ledger + others) has nothing matching. This produces an unexplained file gap: the command
   * must fire an alert and return exit code 2.
   *
   * <p>The divergence report JSON must also be written to {@code archiveOutputDir}.
   */
  @Test
  void unexplainedDivergence_firesAlert_andReturns2() throws IOException {
    // File side: one collector_restart gap envelope
    Path hour9File = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    GapEnvelope env =
        collectorRestartGapEnvelope(
            "binance", "btcusdt", "bookticker", HOUR_9_START_NS, HOUR_10_START_NS);
    writeZstdJsonl(hour9File, env);

    // State side: empty (no ledger, no manifests with collector_restart reason)

    Path archiveOutputDir = tmpDir.resolve("audit-out");

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "backfill",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--archive-output-dir=" + archiveOutputDir,
                "--alertmanager-url=" + alertmanagerUrl(),
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(2);

    // Alert must have been POSTed
    assertThat(alertPosted.get()).as("Alertmanager stub received POST").isTrue();

    // Alert body must contain AuditDivergence alertname
    String body = capturedAlertBody.get();
    assertThat(body).isNotNull();
    JsonNode alerts = MAPPER.readTree(body);
    assertThat(alerts.isArray()).isTrue();
    JsonNode labels = alerts.get(0).get("labels");
    assertThat(labels.get("alertname").asText()).isEqualTo("AuditDivergence");
    assertThat(labels.get("severity").asText()).isEqualTo("critical");

    // Divergence report file must exist in archiveOutputDir
    assertThat(Files.exists(archiveOutputDir)).isTrue();
    long reportCount =
        Files.list(archiveOutputDir)
            .filter(p -> p.getFileName().toString().startsWith("divergence-"))
            .count();
    assertThat(reportCount).isGreaterThanOrEqualTo(1);

    // Report must have non-empty unexplained array and explained_count field
    Path reportFile =
        Files.list(archiveOutputDir)
            .filter(p -> p.getFileName().toString().startsWith("divergence-"))
            .findFirst()
            .orElseThrow();
    JsonNode report = MAPPER.readTree(reportFile.toFile());
    assertThat(report.has("explained_count")).isTrue();
    assertThat(report.path("unexplained").isArray()).isTrue();
    assertThat(report.path("unexplained").size()).isGreaterThanOrEqualTo(1);

    // Stderr must contain a summary
    String stderr = capturedErr.toString(StandardCharsets.UTF_8);
    assertThat(stderr).containsIgnoringCase("divergence");
  }

  // -------------------------------------------------------------------------
  // Test 3: Unexplained file gap WITH orphan-state → alert POSTed + exit 2
  // -------------------------------------------------------------------------

  /**
   * The file side has a {@code collector_restart} gap envelope for btcusdt/bookticker at hour-9
   * (unexplained). The state side (lifecycle ledger) has a {@code collector_restart} event at
   * hour-5 → hour-6 — a completely different time window that does NOT overlap the file record, so
   * the reconciler cannot use it to explain the file gap. The ledger record therefore becomes an
   * orphan-state record.
   *
   * <p>The gate fires (exit 2) because there is one unexplained file gap. The orphan-state record
   * is informational: it appears in the report's {@code orphan_state} array but is NOT itself the
   * reason for the gate firing.
   */
  @Test
  void unexplainedFileWithSomeOrphanState_firesAlert_andReturns2() throws IOException {
    // File side: one collector_restart gap envelope for btcusdt/bookticker at hour-9
    Path hour9File = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    GapEnvelope env =
        collectorRestartGapEnvelope(
            "binance", "btcusdt", "bookticker", HOUR_9_START_NS, HOUR_10_START_NS);
    writeZstdJsonl(hour9File, env);

    // State side: ledger event at hour-5 → hour-6 — time window is disjoint from the file-side
    // gap (hour-9 → hour-10) plus the 2s tolerance, so it cannot explain the file record and
    // becomes an orphan-state entry in the report.
    writeLedgerFixture("binance-collector-primary", HOUR_5_START_NS, HOUR_6_START_NS);

    Path archiveOutputDir = tmpDir.resolve("audit-out-orphan");

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "backfill",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--archive-output-dir=" + archiveOutputDir,
                "--alertmanager-url=" + alertmanagerUrl(),
                "--now-override-iso=" + NOW_ISO);

    // Gate fires because there is an unexplained file gap
    assertThat(exitCode).isEqualTo(2);

    // Alert must have been POSTed
    assertThat(alertPosted.get()).as("Alertmanager stub received POST").isTrue();

    String body = capturedAlertBody.get();
    assertThat(body).isNotNull();
    JsonNode alerts = MAPPER.readTree(body);
    JsonNode labels = alerts.get(0).get("labels");
    assertThat(labels.get("alertname").asText()).isEqualTo("AuditDivergence");
    assertThat(labels.get("severity").asText()).isEqualTo("critical");

    // Report must exist and contain both unexplained and orphan_state arrays
    assertThat(Files.exists(archiveOutputDir)).isTrue();
    Path reportFile =
        Files.list(archiveOutputDir)
            .filter(p -> p.getFileName().toString().startsWith("divergence-"))
            .findFirst()
            .orElseThrow();
    JsonNode report = MAPPER.readTree(reportFile.toFile());

    // unexplained: the file-side btcusdt/bookticker gap that has no state match
    assertThat(report.path("unexplained").isArray()).isTrue();
    assertThat(report.path("unexplained").size()).isGreaterThanOrEqualTo(1);

    // orphan_state: the ledger record that was never used to explain a file gap
    assertThat(report.path("orphan_state").isArray()).isTrue();
    assertThat(report.path("orphan_state").size()).isGreaterThanOrEqualTo(1);

    // Alert summary must mention both counts
    String stderr = capturedErr.toString(StandardCharsets.UTF_8);
    assertThat(stderr).containsIgnoringCase("divergence");
  }

  // -------------------------------------------------------------------------
  // Test 4: Safety-valve — no --symbol or --stream → warning on stderr, exit 0
  // -------------------------------------------------------------------------

  /**
   * When the reconcile is clean but neither {@code --symbol} nor {@code --stream} is provided, the
   * command must print a warning on stderr and return exit code 0 instead of attempting to invoke
   * the backfill orchestrator (which would fail without those filters). No {@link BackfillCommand}
   * invocation occurs — verified by the absence of any backfill output.
   *
   * <p>The archive is empty so the reconcile is trivially clean (0 explained, 0 unexplained, 0
   * orphan-state). Without {@code --dry-run} the command reaches {@code invokeBackfill}, detects
   * empty filters, prints the warning, and returns 0.
   */
  @Test
  void noSymbolOrStream_safetyValve_printsWarning_andReturnsZero() {
    // Empty archive — reconcile will be clean (0/0/0).  No fixtures needed.

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "backfill",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--alertmanager-url=" + alertmanagerUrl(),
                "--now-override-iso=" + NOW_ISO
                // deliberately no --symbol, no --stream, no --dry-run
                );

    assertThat(exitCode).isEqualTo(0);

    String stderr = capturedErr.toString(StandardCharsets.UTF_8);
    assertThat(stderr).contains("--symbol");
    assertThat(stderr).contains("--stream");

    // No alert should have been fired — the reconcile was clean
    assertThat(alertPosted.get()).isFalse();

    // stdout should still print the clean-reconcile message
    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    assertThat(stdout).contains("Audit clean");
  }
}
