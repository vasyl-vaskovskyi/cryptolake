package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SequenceIdGapSourceTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @TempDir Path tmpDir;

  // 2026-05-11T09:00:00Z in millis
  private static final long HOUR_9_START_MS = 1778490000000L;
  // 2026-05-11T10:00:00Z in millis
  private static final long HOUR_9_END_MS = 1778493600000L;

  /** Writes pre-serialized JSONL lines to a zstd-compressed file. */
  private void writeZstdJsonlLines(Path target, String... lines) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      for (String line : lines) {
        zstd.write(line.getBytes(StandardCharsets.UTF_8));
        zstd.write(0x0A);
      }
    }
  }

  /**
   * Builds a minimal data-envelope JSONL line for a trades record.
   *
   * @param aggId the aggregate trade ID ({@code a} field in raw_text)
   * @param receivedNs nanosecond timestamp for {@code received_at}
   */
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

  /** Scope covering exactly hour 9 on 2026-05-11. */
  private AuditScope hour9Scope(String stream) {
    return new AuditScope(
        HOUR_9_START_MS,
        HOUR_9_END_MS,
        List.of("binance"),
        List.of("btcusdt"),
        List.of(stream),
        tmpDir.toString());
  }

  // -------------------------------------------------------------------------
  // Happy-path: one gap at the jump from 101 → 105 (missing 102, 103, 104)
  // -------------------------------------------------------------------------

  @Test
  void tradesAggIdJump_yieldsOneGapRecord() throws IOException {
    long baseNs = HOUR_9_START_MS * 1_000_000L; // convert ms → ns
    long delta = 1_000_000_000L; // 1 second in ns between records

    // records: a=100, a=101, a=105 (jump of 4; missing 102,103,104)
    String line0 = tradeLine(100, baseNs);
    String line1 = tradeLine(101, baseNs + delta);
    String line2 = tradeLine(105, baseNs + 2 * delta);

    Path archiveFile = tmpDir.resolve("binance/btcusdt/trades/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonlLines(archiveFile, line0, line1, line2);

    SequenceIdGapSource source = new SequenceIdGapSource(mapper);
    List<GapRecord> records = source.read(hour9Scope("trades"));

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("file.sequence_id");
    assertThat(r.reason()).isEqualTo("session_seq_skip");
    assertThat(r.exchange()).isEqualTo("binance");
    assertThat(r.symbol()).isEqualTo("btcusdt");
    assertThat(r.stream()).isEqualTo("trades");

    // atReceived = baseNs + 2*delta (the breaking record's received_at)
    long expectedMs = (baseNs + 2 * delta) / 1_000_000L;
    assertThat(r.startMs()).isEqualTo(expectedMs);
    assertThat(r.endMs()).isEqualTo(expectedMs);

    // detail: "a: expected 102, got 105 (missing 3)"
    assertThat(r.detail()).isEqualTo("a: expected 102, got 105 (missing 3)");
  }

  // -------------------------------------------------------------------------
  // Streams without ID continuity semantics are skipped
  // -------------------------------------------------------------------------

  @Test
  void fundingRateFileIsSkipped() throws IOException {
    // Write a file on the funding_rate stream — should produce zero records.
    Path archiveFile = tmpDir.resolve("binance/btcusdt/funding_rate/2026-05-11/hour-9.jsonl.zst");
    // We write a line that could look like data but is irrelevant; the walker never touches it.
    writeZstdJsonlLines(
        archiveFile,
        "{\"v\":1,\"type\":\"data\",\"exchange\":\"binance\",\"symbol\":\"btcusdt\","
            + "\"stream\":\"funding_rate\",\"received_at\":1,\"exchange_ts\":1,"
            + "\"collector_session_id\":\"s\",\"session_seq\":1,"
            + "\"raw_text\":\"{}\",\"raw_sha256\":\"x\","
            + "\"_topic\":\"t\",\"_partition\":0,\"_offset\":0}");

    AuditScope scope =
        new AuditScope(
            HOUR_9_START_MS,
            HOUR_9_END_MS,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("funding_rate"),
            tmpDir.toString());

    List<GapRecord> records = new SequenceIdGapSource(mapper).read(scope);
    assertThat(records).isEmpty();
  }

  @Test
  void liquidationsFileIsSkipped() throws IOException {
    Path archiveFile = tmpDir.resolve("binance/btcusdt/liquidations/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonlLines(
        archiveFile,
        "{\"v\":1,\"type\":\"data\",\"exchange\":\"binance\",\"symbol\":\"btcusdt\","
            + "\"stream\":\"liquidations\",\"received_at\":1,\"exchange_ts\":1,"
            + "\"collector_session_id\":\"s\",\"session_seq\":1,"
            + "\"raw_text\":\"{}\",\"raw_sha256\":\"x\","
            + "\"_topic\":\"t\",\"_partition\":0,\"_offset\":0}");

    AuditScope scope =
        new AuditScope(
            HOUR_9_START_MS,
            HOUR_9_END_MS,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("liquidations"),
            tmpDir.toString());

    List<GapRecord> records = new SequenceIdGapSource(mapper).read(scope);
    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // name() label
  // -------------------------------------------------------------------------

  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new SequenceIdGapSource(mapper).name()).isEqualTo("SequenceIdGapSource");
  }

  // -------------------------------------------------------------------------
  // No gap when IDs are perfectly continuous
  // -------------------------------------------------------------------------

  @Test
  void noGapWhenTradesIdsAreContinuous() throws IOException {
    long baseNs = HOUR_9_START_MS * 1_000_000L;
    long delta = 1_000_000_000L;

    Path archiveFile = tmpDir.resolve("binance/btcusdt/trades/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonlLines(
        archiveFile,
        tradeLine(1, baseNs),
        tradeLine(2, baseNs + delta),
        tradeLine(3, baseNs + 2 * delta));

    List<GapRecord> records = new SequenceIdGapSource(mapper).read(hour9Scope("trades"));
    assertThat(records).isEmpty();
  }
}
