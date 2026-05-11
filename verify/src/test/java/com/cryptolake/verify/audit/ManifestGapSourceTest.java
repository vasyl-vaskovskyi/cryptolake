package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link ManifestGapSource}.
 *
 * <p>Writes {@code <date>.manifest.json} fixture files into a temp directory that mirrors the
 * {@code <baseDir>/<exchange>/<symbol>/<stream>/<date>/<date>.manifest.json} layout.
 */
class ManifestGapSourceTest {

  @TempDir Path tmpDir;

  private final ObjectMapper mapper = new ObjectMapper();

  // 2026-05-11T00:00:00Z in millis
  private static final long DAY_START_MS = 1778457600_000L;
  private static final long HOUR_MS = 3_600_000L;

  /** Writes a minimal manifest JSON with the given missing_hours under the right path. */
  private void writeManifest(
      String exchange, String symbol, String stream, String date, int... missingHours)
      throws IOException {
    Path dateDir = tmpDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);
    Files.createDirectories(dateDir);

    // Build the missing_hours JSON array
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

  // -------------------------------------------------------------------------
  // Happy path: missing hours in the manifest are emitted as GapRecords
  // -------------------------------------------------------------------------

  @Test
  void missingHoursInManifest_emittedAsGapRecords() throws IOException {
    writeManifest("binance", "btcusdt", "bookticker", "2026-05-11", 9, 10);

    // Scope covers the entire day
    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(2);

    long hour9Start = DAY_START_MS + 9 * HOUR_MS;
    long hour10Start = DAY_START_MS + 10 * HOUR_MS;

    GapRecord r9 =
        records.stream().filter(r -> r.startMs() == hour9Start).findFirst().orElseThrow();
    assertThat(r9.source()).isEqualTo("manifest");
    assertThat(r9.exchange()).isEqualTo("binance");
    assertThat(r9.symbol()).isEqualTo("btcusdt");
    assertThat(r9.stream()).isEqualTo("bookticker");
    assertThat(r9.endMs()).isEqualTo(hour9Start + 3_599_999L);
    assertThat(r9.reason()).isEqualTo("missing_hour");
    assertThat(r9.detail()).isEqualTo("manifest=2026-05-11; hour=9");

    GapRecord r10 =
        records.stream().filter(r -> r.startMs() == hour10Start).findFirst().orElseThrow();
    assertThat(r10.detail()).isEqualTo("manifest=2026-05-11; hour=10");
  }

  // -------------------------------------------------------------------------
  // Empty missing_hours → no records
  // -------------------------------------------------------------------------

  @Test
  void emptyMissingHours_noRecords() throws IOException {
    writeManifest("binance", "btcusdt", "bookticker", "2026-05-11" /* no missing hours */);

    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Missing hours outside scope window are not emitted
  // -------------------------------------------------------------------------

  @Test
  void missingHourOutsideScopeWindow_notEmitted() throws IOException {
    // Manifest has hour 9 missing, but scope only covers hours 0-8
    writeManifest("binance", "btcusdt", "bookticker", "2026-05-11", 9);

    long hour9Start = DAY_START_MS + 9 * HOUR_MS;
    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            hour9Start - 1, // scope ends just before hour 9
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // No manifest file → graceful empty
  // -------------------------------------------------------------------------

  @Test
  void noManifestFile_returnsEmpty() throws IOException {
    // Create the date directory but no manifest.json
    Path dateDir =
        tmpDir.resolve("binance").resolve("btcusdt").resolve("bookticker").resolve("2026-05-11");
    Files.createDirectories(dateDir);

    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Missing base dir → graceful empty
  // -------------------------------------------------------------------------

  @Test
  void missingBaseDir_returnsEmpty() {
    Path nonExistent = tmpDir.resolve("does-not-exist");
    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            nonExistent.toString());

    ManifestGapSource source = new ManifestGapSource(nonExistent, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Unparseable manifest JSON → silently skipped
  // -------------------------------------------------------------------------

  @Test
  void unparseableManifest_silentlySkipped() throws IOException {
    Path dateDir =
        tmpDir.resolve("binance").resolve("btcusdt").resolve("bookticker").resolve("2026-05-11");
    Files.createDirectories(dateDir);
    Files.writeString(
        dateDir.resolve("2026-05-11.manifest.json"), "BROKEN JSON !!!", StandardCharsets.UTF_8);

    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  // -------------------------------------------------------------------------
  // Multi-symbol, multi-stream: manifests across multiple tuples are all read
  // -------------------------------------------------------------------------

  @Test
  void multipleSymbolsAndStreams_allEmitted() throws IOException {
    writeManifest("binance", "btcusdt", "bookticker", "2026-05-11", 5);
    writeManifest("binance", "ethusdt", "trades", "2026-05-11", 6);

    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of(),
            List.of(),
            List.of(),
            tmpDir.toString());

    ManifestGapSource source = new ManifestGapSource(tmpDir, mapper);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(2);
    assertThat(records)
        .extracting(GapRecord::symbol)
        .containsExactlyInAnyOrder("btcusdt", "ethusdt");
  }

  // -------------------------------------------------------------------------
  // name() label
  // -------------------------------------------------------------------------

  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new ManifestGapSource(tmpDir, mapper).name()).isEqualTo("ManifestGapSource");
  }
}
