package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Integration tests for {@link AuditReconcileCommand}.
 *
 * <p>Three scenarios:
 *
 * <ol>
 *   <li>All-explained: gap envelope on disk + matching ledger event → exit 0, explained.size()==1.
 *   <li>Unexplained: gap envelope on disk, no matching state record → exit 1,
 *       unexplained.size()&ge;1.
 *   <li>Orphan: ledger event only, no gap envelope → exit 0 (orphans don't fail),
 *       orphanState.size()&ge;1, unexplained empty.
 * </ol>
 */
class AuditReconcileCommandTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // 2026-05-11T09:00:00Z in millis and nanos
  private static final long HOUR_9_START_MS = 1_778_490_000_000L;
  private static final long HOUR_9_START_NS = HOUR_9_START_MS * 1_000_000L;

  // 2026-05-11T10:00:00Z
  private static final long HOUR_10_START_MS = 1_778_493_600_000L;
  private static final long HOUR_10_START_NS = HOUR_10_START_MS * 1_000_000L;

  private static final String NOW_ISO = "2026-05-11T23:00:00Z";

  @TempDir Path tmpDir;

  private Path baseDir;
  private Path dataDir;

  private PrintStream originalOut;
  private ByteArrayOutputStream capturedOut;

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  @BeforeEach
  void setUp() throws IOException {
    baseDir = tmpDir.resolve("archive");
    dataDir = tmpDir.resolve("data");
    Files.createDirectories(baseDir);
    Files.createDirectories(dataDir);

    originalOut = System.out;
    capturedOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  void tearDown() {
    System.setOut(originalOut);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Writes a zstd-compressed JSONL file containing the given objects. */
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

  /** Creates a collector_restart gap envelope for the given tuple and time window. */
  private GapEnvelope collectorRestartEnvelope(
      String exchange, String symbol, String stream, long startNs, long endNs) {
    return GapEnvelope.create(
        exchange,
        symbol,
        stream,
        "sess-1",
        1L,
        startNs,
        endNs,
        "collector_restart",
        "test",
        () -> startNs);
  }

  /**
   * Writes a lifecycle.jsonl that produces a {@code collector_restart} ledger record (planned=true)
   * for the named collector under {@code dataDir/cryptolake/<collectorId>/}.
   */
  private void writeLedgerFixture(String collectorId, long startNs, long endNs) throws IOException {
    Path dir = dataDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    String startLine =
        "{\"ts_ns\":"
            + startNs
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-1\"}";
    String stopLine =
        "{\"ts_ns\":"
            + endNs
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-1\",\"planned\":true}";
    Files.writeString(
        dir.resolve("lifecycle.jsonl"), startLine + "\n" + stopLine + "\n", StandardCharsets.UTF_8);
  }

  // -------------------------------------------------------------------------
  // Test 1: All-explained — gap envelope + matching ledger event → exit 0
  // -------------------------------------------------------------------------

  /**
   * The file side has one {@code collector_restart} gap envelope for btcusdt/bookticker hour-9. The
   * state side (ledger) has a matching planned-shutdown event for the same window. The reconciler
   * should explain the file record, produce exit 0, and the JSON must have {@code explained.size()
   * == 1}, empty {@code unexplained} and {@code orphanState}.
   */
  @Test
  void allExplained_exitZero_jsonHasOneExplained() throws IOException {
    // File side: collector_restart envelope
    Path hour9File = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(
        hour9File,
        collectorRestartEnvelope(
            "binance", "btcusdt", "bookticker", HOUR_9_START_NS, HOUR_10_START_NS));

    // State side: ledger with planned shutdown covering the same window.
    // Fan-out requires symbol+stream in scope to bind the ledger record to btcusdt/bookticker.
    writeLedgerFixture("binance-collector-primary", HOUR_9_START_NS, HOUR_10_START_NS);

    // Use --hour scope to avoid MissingHourGapSource emitting records for absent hours 0-8,10-22.
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "reconcile",
                "--hour=2026-05-11T09",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--now-override-iso=2026-05-11T10:00:00Z",
                "--json");

    assertThat(exitCode).isEqualTo(0);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    JsonNode root = MAPPER.readTree(stdout);
    assertThat(root.path("explained").isArray()).isTrue();
    assertThat(root.path("explained").size()).isEqualTo(1);
    assertThat(root.path("unexplained").isArray()).isTrue();
    assertThat(root.path("unexplained").size()).isEqualTo(0);
    assertThat(root.path("orphanState").isArray()).isTrue();
    assertThat(root.path("orphanState").size()).isEqualTo(0);
  }

  // -------------------------------------------------------------------------
  // Test 2: Unexplained — gap envelope but no matching state → exit 1
  // -------------------------------------------------------------------------

  /**
   * The file side has one {@code collector_restart} gap envelope for btcusdt/bookticker. The state
   * side is empty (no ledger, no PG sources). The reconciler produces one unexplained record, exit
   * code 1, and the JSON must have {@code unexplained.size() >= 1}.
   */
  @Test
  void unexplained_exitOne_jsonHasUnexplained() throws IOException {
    // File side: collector_restart envelope
    Path hour9File = baseDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(
        hour9File,
        collectorRestartEnvelope(
            "binance", "btcusdt", "bookticker", HOUR_9_START_NS, HOUR_10_START_NS));

    // State side: intentionally empty (no ledger, --db-url= skips PG)

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "reconcile",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--now-override-iso=" + NOW_ISO,
                "--json");

    assertThat(exitCode).isEqualTo(1);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    JsonNode root = MAPPER.readTree(stdout);
    assertThat(root.path("unexplained").isArray()).isTrue();
    assertThat(root.path("unexplained").size()).isGreaterThanOrEqualTo(1);
  }

  // -------------------------------------------------------------------------
  // Test 3: Orphan — ledger event but no gap envelope → exit 0
  // -------------------------------------------------------------------------

  /**
   * The state side (ledger) has a planned-shutdown event for btcusdt/bookticker, but the file side
   * has no gap envelopes. The reconciler should produce one orphan state record. Exit code must be
   * 0 (orphans don't fail the gate) and the JSON must have {@code orphanState.size() >= 1} with
   * empty {@code unexplained}.
   */
  @Test
  void orphan_exitZero_jsonHasOrphanState() throws IOException {
    // State side: ledger with planned shutdown
    writeLedgerFixture("binance-collector-primary", HOUR_9_START_NS, HOUR_10_START_NS);

    // File side: empty (no gap envelopes)

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "reconcile",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--now-override-iso=" + NOW_ISO,
                "--json");

    assertThat(exitCode).isEqualTo(0);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    JsonNode root = MAPPER.readTree(stdout);
    assertThat(root.path("unexplained").isArray()).isTrue();
    assertThat(root.path("unexplained").size()).isEqualTo(0);
    assertThat(root.path("orphanState").isArray()).isTrue();
    assertThat(root.path("orphanState").size()).isGreaterThanOrEqualTo(1);
  }
}
