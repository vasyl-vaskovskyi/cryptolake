package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Integration test for {@link AuditStateCommand} via picocli {@link CommandLine}.
 *
 * <p>Builds a minimal temp fixture with:
 *
 * <ul>
 *   <li>One lifecycle.jsonl with a clean_shutdown event → {@code LedgerGapSource} emits one record
 *   <li>One kafka_outage.json with an active outage → {@code KafkaOutageGapSource} emits one record
 *   <li>One manifest.json with two missing hours → {@code ManifestGapSource} emits two records
 * </ul>
 *
 * <p>DB sources are skipped by passing {@code --db-url=} (empty string) which triggers graceful
 * degradation in both PG sources.
 *
 * <p>Asserts that the JSON output contains records from all three non-PG sources (≥ 4 records).
 */
class AuditStateCommandTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @TempDir Path tmpDir;

  // Separate base-dir (for ManifestGapSource) and data-dir (for Ledger + KafkaOutage)
  private Path baseDir;
  private Path dataDir;

  // 2026-05-11T00:00:00Z in millis
  private static final long DAY_START_MS = 1778457600_000L;
  private static final long HOUR_MS = 3_600_000L;

  // 2026-05-11T09:00:00Z in millis and nanos
  private static final long HOUR_9_START_MS = DAY_START_MS + 9 * HOUR_MS;
  private static final long HOUR_9_START_NS = HOUR_9_START_MS * 1_000_000L;

  // 2026-05-11T10:00:00Z in millis and nanos
  private static final long HOUR_10_START_MS = DAY_START_MS + 10 * HOUR_MS;
  private static final long HOUR_10_START_NS = HOUR_10_START_MS * 1_000_000L;

  // "now" override for KafkaOutageGapSource — 2026-05-11T23:00:00Z (after the scope day)
  private static final String NOW_ISO = "2026-05-11T23:00:00Z";

  // ---- stdout capture ----
  private PrintStream originalOut;
  private ByteArrayOutputStream capturedOut;

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
  void restoreStdout() {
    System.setOut(originalOut);
  }

  // ---- fixture helpers ----

  /** Writes a lifecycle.jsonl with one start+clean_shutdown pair (planned=false). */
  private void writeLedgerFixture(String collectorId) throws IOException {
    Path dir = dataDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    String startLine =
        "{\"ts_ns\":"
            + HOUR_9_START_NS
            + ",\"event\":\"start\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-test\"}";
    String stopLine =
        "{\"ts_ns\":"
            + HOUR_10_START_NS
            + ",\"event\":\"clean_shutdown\",\"host_boot_id\":\"boot-1\","
            + "\"collector_session_id\":\"sess-state-test\",\"planned\":false}";
    Files.writeString(
        dir.resolve("lifecycle.jsonl"), startLine + "\n" + stopLine + "\n", StandardCharsets.UTF_8);
  }

  /** Writes a kafka_outage.json indicating an active outage that started at hour-9. */
  private void writeKafkaOutageFixture(String collectorId) throws IOException {
    Path dir = dataDir.resolve("cryptolake").resolve(collectorId);
    Files.createDirectories(dir);
    Files.writeString(
        dir.resolve("kafka_outage.json"),
        "{\"outage_started_at_ns\":" + HOUR_9_START_NS + "}",
        StandardCharsets.UTF_8);
  }

  /** Writes a manifest with missing hours under baseDir/<exchange>/<symbol>/<stream>/<date>/. */
  private void writeManifestFixture(
      String exchange, String symbol, String stream, String date, int... missingHours)
      throws IOException {
    Path dateDir = baseDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);
    Files.createDirectories(dateDir);

    StringBuilder arr = new StringBuilder("[");
    for (int i = 0; i < missingHours.length; i++) {
      if (i > 0) arr.append(',');
      arr.append(missingHours[i]);
    }
    arr.append(']');

    String json =
        "{\"version\":1,\"exchange\":\""
            + exchange
            + "\",\"symbol\":\""
            + symbol
            + "\",\"stream\":\""
            + stream
            + "\",\"date\":\""
            + date
            + "\","
            + "\"consolidated_at\":\"2026-05-11T01:00:00Z\","
            + "\"daily_file\":\"dummy.jsonl.zst\","
            + "\"daily_file_sha256\":\"abc\","
            + "\"total_records\":100,\"data_records\":98,\"gap_records\":2,"
            + "\"hours\":{},\"missing_hours\":"
            + arr
            + ","
            + "\"source_files\":[]}";

    Files.writeString(dateDir.resolve(date + ".manifest.json"), json, StandardCharsets.UTF_8);
  }

  // ---- tests ----

  @Test
  void jsonOutput_containsRecordsFromAllThreeNonPgSources() throws IOException {
    writeLedgerFixture("binance-collector-primary");
    writeKafkaOutageFixture("binance-collector-primary");
    writeManifestFixture("binance", "btcusdt", "bookticker", "2026-05-11", 9, 10);

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "state",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--json",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    assertThat(stdout).isNotBlank();

    JsonNode array = mapper.readTree(stdout);
    assertThat(array.isArray()).isTrue();

    // Expect: 1 ledger + 1 kafka_outage + 2 manifest = 4 records minimum
    assertThat(array.size()).isGreaterThanOrEqualTo(4);

    List<String> sources = List.of(array.findValuesAsText("source").toArray(new String[0]));
    assertThat(sources).contains("ledger");
    assertThat(sources).contains("kafka_outage");
    assertThat(sources).contains("manifest");

    // Spot-check manifest record: should have exchange/symbol/stream populated
    boolean foundManifest = false;
    for (JsonNode node : array) {
      if ("manifest".equals(node.path("source").asText())) {
        assertThat(node.path("exchange").asText()).isEqualTo("binance");
        assertThat(node.path("symbol").asText()).isEqualTo("btcusdt");
        assertThat(node.path("stream").asText()).isEqualTo("bookticker");
        assertThat(node.path("reason").asText()).isEqualTo("missing_hour");
        foundManifest = true;
        break;
      }
    }
    assertThat(foundManifest).as("manifest record found").isTrue();

    // Spot-check ledger record
    boolean foundLedger = false;
    for (JsonNode node : array) {
      if ("ledger".equals(node.path("source").asText())) {
        assertThat(node.path("reason").asText()).isEqualTo("restart_gap");
        foundLedger = true;
        break;
      }
    }
    assertThat(foundLedger).as("ledger record found").isTrue();
  }

  @Test
  void humanOutput_noCrash() throws IOException {
    writeLedgerFixture("binance-collector-primary");

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "state",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);
    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    // Human output should contain the table header
    assertThat(stdout).contains("source");
  }

  @Test
  void missingPeriodFlag_exits2() {
    int exitCode =
        new CommandLine(new AuditCli())
            .execute("state", "--base-dir=" + baseDir, "--data-dir=" + dataDir, "--db-url=");

    assertThat(exitCode).isEqualTo(2);
  }

  @Test
  void emptyDirs_zeroRecordsJson() {
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "state",
                "--day=2026-05-11",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--json",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);
    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    JsonNode array;
    try {
      array = mapper.readTree(stdout);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    assertThat(array.isArray()).isTrue();
    assertThat(array.size()).isEqualTo(0);
  }

  @Test
  void weekPeriod_acceptedAndExitsZero() throws IOException {
    // --week flag should be accepted; empty dirs → 0 records
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "state",
                "--week=2026-W20",
                "--base-dir=" + baseDir,
                "--data-dir=" + dataDir,
                "--db-url=",
                "--json",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);
  }
}
