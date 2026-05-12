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
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

/**
 * Integration test for {@link AuditFilesCommand} via picocli {@link CommandLine}.
 *
 * <p>Builds a minimal temp archive with:
 *
 * <ul>
 *   <li>One bookticker hour-9 file containing a gap envelope → FileGapSource emits one record
 *   <li>One trades hour-9 file containing an agg-ID jump → SequenceIdGapSource emits one record
 *   <li>Bookticker hour-10 is absent, and "now" is after hour-10 → MissingHourGapSource emits one
 *       record
 * </ul>
 *
 * <p>Asserts that JSON output contains all three records.
 */
class AuditFilesCommandTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @TempDir Path tmpDir;

  // 2026-05-11T09:00:00Z
  private static final long HOUR_9_START_MS = 1778490000000L;
  // 2026-05-11T10:00:00Z
  private static final long HOUR_10_START_MS = 1778493600000L;
  // 2026-05-11T11:00:00Z  (used as "now" so hour-10 is in the past)
  private static final String NOW_ISO = "2026-05-11T11:00:00Z";

  // ---- stdout capture ----
  private PrintStream originalOut;
  private ByteArrayOutputStream capturedOut;

  @BeforeEach
  void redirectStdout() {
    originalOut = System.out;
    capturedOut = new ByteArrayOutputStream();
    System.setOut(new PrintStream(capturedOut, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  void restoreStdout() {
    System.setOut(originalOut);
  }

  // ---- fixture helpers ----

  private void writeZstdJsonl(Path target, Object... objects) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      for (Object obj : objects) {
        zstd.write(mapper.writeValueAsBytes(obj));
        zstd.write(0x0A);
      }
    }
  }

  private void writeZstdLines(Path target, String... lines) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      for (String line : lines) {
        zstd.write(line.getBytes(StandardCharsets.UTF_8));
        zstd.write(0x0A);
      }
    }
  }

  /** Builds a minimal data-envelope JSONL line for a trades record. */
  private String tradeLine(long aggId, long receivedNs) {
    String rawText = "{\"a\":" + aggId + "}";
    String escapedRaw = rawText.replace("\"", "\\\"");
    return "{\"v\":1,\"type\":\"data\",\"exchange\":\"binance\",\"symbol\":\"btcusdt\","
        + "\"stream\":\"trades\",\"received_at\":"
        + receivedNs
        + ",\"exchange_ts\":1,\"collector_session_id\":\"s\",\"session_seq\":1,"
        + "\"raw_text\":\""
        + escapedRaw
        + "\","
        + "\"raw_sha256\":\"x\",\"_topic\":\"t\",\"_partition\":0,\"_offset\":0}";
  }

  @Test
  void jsonOutput_containsAllThreeSourceRecords() throws IOException {
    // --- bookticker hour-9: one gap envelope (FileGapSource) ---
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_10_START_MS * 1_000_000L;
    GapEnvelope gapEnv =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "bookticker",
            "session-1",
            1L,
            gapStartNs,
            gapEndNs,
            com.cryptolake.common.envelope.GapReason.WS_DISCONNECT,
            "test detail",
            () -> gapStartNs);

    Path bookticker9 = tmpDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(bookticker9, gapEnv);
    // bookticker hour-10 is intentionally absent → MissingHourGapSource will emit one record

    // --- trades hour-9: agg-ID jump 101 → 105 (SequenceIdGapSource) ---
    long baseNs = HOUR_9_START_MS * 1_000_000L;
    long delta = 1_000_000_000L;
    Path trades9 = tmpDir.resolve("binance/btcusdt/trades/2026-05-11/hour-9.jsonl.zst");
    writeZstdLines(
        trades9,
        tradeLine(100, baseNs),
        tradeLine(101, baseNs + delta),
        tradeLine(105, baseNs + 2 * delta));

    // Execute CLI: --day=2026-05-11, --symbol=btcusdt, --json, --now-override-iso=<future>
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "files",
                "--day=2026-05-11",
                "--base-dir=" + tmpDir,
                "--symbol=btcusdt",
                "--json",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);

    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    assertThat(stdout).isNotBlank();

    JsonNode array = mapper.readTree(stdout);
    assertThat(array.isArray()).isTrue();

    // Must have at least 3 records: 1 file.envelope + 1 file.sequence_id + 1 file.missing_hour
    assertThat(array.size()).isGreaterThanOrEqualTo(3);

    // Collect sources present in the output
    List<String> sources = List.of(array.findValuesAsText("source").toArray(new String[0]));
    assertThat(sources).contains("file.envelope");
    assertThat(sources).contains("file.sequence_id");
    assertThat(sources).contains("file.missing_hour");

    // Spot-check the file.envelope record
    for (JsonNode node : array) {
      if ("file.envelope".equals(node.path("source").asText())) {
        assertThat(node.path("exchange").asText()).isEqualTo("binance");
        assertThat(node.path("symbol").asText()).isEqualTo("btcusdt");
        assertThat(node.path("stream").asText()).isEqualTo("bookticker");
        assertThat(node.path("reason").asText()).isEqualTo("ws_disconnect");
        break;
      }
    }
  }

  @Test
  void humanOutput_noCrash() throws IOException {
    // Minimal archive: one bookticker gap envelope hour-9, hour-10 absent.
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_10_START_MS * 1_000_000L;
    GapEnvelope gapEnv =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "bookticker",
            "s",
            1L,
            gapStartNs,
            gapEndNs,
            com.cryptolake.common.envelope.GapReason.WS_DISCONNECT,
            null,
            () -> gapStartNs);

    Path bookticker9 = tmpDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(bookticker9, gapEnv);

    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "files",
                "--day=2026-05-11",
                "--base-dir=" + tmpDir,
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);
    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    // Human output should contain a header line
    assertThat(stdout).contains("source");
  }

  @Test
  void missingPeriodFlag_exits2() {
    int exitCode = new CommandLine(new AuditCli()).execute("files", "--base-dir=" + tmpDir);

    // exit code 2 = usage error
    assertThat(exitCode).isEqualTo(2);
  }

  @Test
  void emptyArchive_zeroRecordsJson() throws IOException {
    int exitCode =
        new CommandLine(new AuditCli())
            .execute(
                "files",
                "--day=2026-05-11",
                "--base-dir=" + tmpDir,
                "--symbol=btcusdt",
                "--stream=bookticker",
                "--json",
                "--now-override-iso=" + NOW_ISO);

    assertThat(exitCode).isEqualTo(0);
    String stdout = capturedOut.toString(StandardCharsets.UTF_8);
    JsonNode array = mapper.readTree(stdout);
    assertThat(array.isArray()).isTrue();
    assertThat(array.size()).isEqualTo(0);
  }
}
