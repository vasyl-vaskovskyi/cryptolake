package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.GapEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileGapSourceTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @TempDir Path tmpDir;

  /** Writes a single-line zstd-compressed JSONL file containing the given envelope. */
  private void writeZstdJsonl(Path target, GapEnvelope envelope) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      zstd.write(mapper.writeValueAsBytes(envelope));
      zstd.write(0x0A); // newline
    }
  }

  /** Writes pre-serialized JSONL lines to a zstd-compressed file. */
  private void writeZstdJsonlLines(Path target, String... lines) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 3)) {
      for (String line : lines) {
        zstd.write(line.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        zstd.write(0x0A);
      }
    }
  }

  // 2026-05-11T09:00:00Z in millis
  private static final long HOUR_9_START_MS = 1778490000000L;
  // 2026-05-11T10:00:00Z in millis
  private static final long HOUR_9_END_MS = 1778493600000L;

  @Test
  void happyPath_oneGapEnvelopeInScope() throws IOException {
    // gap_start_ts = HOUR_9_START_MS * 1_000_000 (nanoseconds)
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_9_END_MS * 1_000_000L;

    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "bookticker",
            "session-1",
            1L,
            gapStartNs,
            gapEndNs,
            "ws_disconnect",
            "test detail",
            () -> gapStartNs);

    // Write to <tmpDir>/binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst
    Path archiveFile = tmpDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(archiveFile, env);

    // Scope: 2026-05-11T09:00Z to 2026-05-11T10:00Z
    AuditScope scope =
        new AuditScope(
            HOUR_9_START_MS,
            HOUR_9_END_MS,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("bookticker"),
            tmpDir.toString());

    FileGapSource source = new FileGapSource(mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("file.envelope");
    assertThat(r.exchange()).isEqualTo("binance");
    assertThat(r.symbol()).isEqualTo("btcusdt");
    assertThat(r.stream()).isEqualTo("bookticker");
    assertThat(r.startMs()).isEqualTo(HOUR_9_START_MS);
    assertThat(r.endMs()).isEqualTo(HOUR_9_END_MS);
    assertThat(r.reason()).isEqualTo("ws_disconnect");
    assertThat(r.detail()).isEqualTo("test detail");
  }

  @Test
  void streamFilterExcludesNonMatchingFile() throws IOException {
    // A file on the trades stream should not be opened when scope filters to bookticker.
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_9_END_MS * 1_000_000L;

    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "session-1",
            1L,
            gapStartNs,
            gapEndNs,
            "ws_disconnect",
            null,
            () -> gapStartNs);

    Path archiveFile = tmpDir.resolve("binance/btcusdt/trades/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(archiveFile, env);

    AuditScope scope =
        new AuditScope(
            HOUR_9_START_MS, HOUR_9_END_MS, null, null, List.of("bookticker"), tmpDir.toString());

    List<GapRecord> records = new FileGapSource(mapper).read(scope);
    assertThat(records).isEmpty();
  }

  @Test
  void nonGapRecordsAreSkippedAndNullDetailRoundTrips() throws IOException {
    // A file containing both a type:"data" record and a type:"gap" record should yield
    // only the gap record. Also asserts detail==null round-trips correctly.
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_9_END_MS * 1_000_000L;

    GapEnvelope gap =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "bookticker",
            "session-1",
            1L,
            gapStartNs,
            gapEndNs,
            "ws_disconnect",
            null,
            () -> gapStartNs);

    String dataLine =
        "{\"v\":1,\"type\":\"data\",\"exchange\":\"binance\",\"symbol\":\"btcusdt\","
            + "\"stream\":\"bookticker\",\"received_at\":"
            + gapStartNs
            + ",\"exchange_ts\":1,\"collector_session_id\":\"session-1\",\"session_seq\":2,"
            + "\"raw_text\":\"{\\\"u\\\":1}\",\"raw_sha256\":\"deadbeef\","
            + "\"_topic\":\"t\",\"_partition\":0,\"_offset\":0}";

    Path archiveFile = tmpDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonlLines(archiveFile, dataLine, mapper.writeValueAsString(gap));

    AuditScope scope =
        new AuditScope(
            HOUR_9_START_MS, HOUR_9_END_MS, null, null, List.of("bookticker"), tmpDir.toString());

    List<GapRecord> records = new FileGapSource(mapper).read(scope);
    assertThat(records).hasSize(1);
    assertThat(records.get(0).reason()).isEqualTo("ws_disconnect");
    assertThat(records.get(0).detail()).isNull();
  }

  @Test
  void fileOutsideScopeTimeRangeIsSkipped() throws IOException {
    long gapStartNs = HOUR_9_START_MS * 1_000_000L;
    long gapEndNs = HOUR_9_END_MS * 1_000_000L;

    GapEnvelope env =
        GapEnvelope.create(
            "binance",
            "btcusdt",
            "bookticker",
            "session-1",
            1L,
            gapStartNs,
            gapEndNs,
            "ws_disconnect",
            null,
            () -> gapStartNs);

    // Write to hour-9, but the scope only covers hour-10 onwards
    Path archiveFile = tmpDir.resolve("binance/btcusdt/bookticker/2026-05-11/hour-9.jsonl.zst");
    writeZstdJsonl(archiveFile, env);

    // Scope starts at 10:00:00Z — after the file ends
    long hour10StartMs = HOUR_9_END_MS; // = 2026-05-11T10:00Z
    long hour10EndMs = hour10StartMs + 3_600_000L;
    AuditScope scope =
        new AuditScope(hour10StartMs, hour10EndMs, null, null, null, tmpDir.toString());

    FileGapSource source = new FileGapSource(mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new FileGapSource(mapper).name()).isEqualTo("FileGapSource");
  }
}
