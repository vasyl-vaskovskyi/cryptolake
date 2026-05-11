package com.cryptolake.verify.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.ZstdOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link MissingHourGapSource}.
 *
 * <p>All tests pin "now" via the {@code Supplier<Instant>} constructor parameter so future-hour
 * skipping is deterministic.
 */
class MissingHourGapSourceTest {

  @TempDir Path tmpDir;

  // 2026-05-11T00:00:00Z — start of day
  private static final long DAY_START_MS = 1778457600000L;
  private static final long HOUR_MS = 3_600_000L;

  /** Writes a minimal (empty content) zstd file at the given path. */
  private void writeTinyZstd(Path target) throws IOException {
    Files.createDirectories(target.getParent());
    try (var out = Files.newOutputStream(target);
        var zstd = new ZstdOutputStream(out, 1)) {
      zstd.write("{}\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }

  // ---- helper to build a path for an hourly file ----
  private Path hourFile(String exchange, String symbol, String stream, String date, int hour) {
    return tmpDir
        .resolve(exchange)
        .resolve(symbol)
        .resolve(stream)
        .resolve(date)
        .resolve(String.format("hour-%d.jsonl.zst", hour));
  }

  // ---- helper to build a path for a daily consolidated file ----
  private Path dailyFile(String exchange, String symbol, String stream, String date) {
    return tmpDir
        .resolve(exchange)
        .resolve(symbol)
        .resolve(stream)
        .resolve(date)
        .resolve(date + ".jsonl.zst");
  }

  /**
   * Happy path: hours 0-5 are present, hour 6 is missing. Now = hour 7 start → hours 7-23 are
   * future and skipped. Expect exactly one missing_hour record for hour 6.
   */
  @Test
  void happyPath_oneHourMissing() throws IOException {
    String date = "2026-05-11";
    // Write hours 0-5 as tiny zstd files
    for (int h = 0; h <= 5; h++) {
      writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", date, h));
    }
    // Hour 6 is absent — the file simply does not exist

    // now = 2026-05-11T07:00:00Z (hour 7 start) → hours 7-23 are future
    Instant now = Instant.parse("2026-05-11T07:00:00Z");

    // Scope covers the entire day
    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("bookticker"),
            tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    assertThat(records).hasSize(1);
    GapRecord r = records.get(0);
    assertThat(r.source()).isEqualTo("file.missing_hour");
    assertThat(r.exchange()).isEqualTo("binance");
    assertThat(r.symbol()).isEqualTo("btcusdt");
    assertThat(r.stream()).isEqualTo("bookticker");
    assertThat(r.reason()).isEqualTo("missing_hour");
    // hour 6 start = day start + 6h
    long hour6Start = DAY_START_MS + 6 * HOUR_MS;
    assertThat(r.startMs()).isEqualTo(hour6Start);
    assertThat(r.endMs()).isEqualTo(hour6Start + 3_599_999L);
  }

  /** All hours present → no missing_hour records. */
  @Test
  void allHoursPresent_noRecords() throws IOException {
    String date = "2026-05-11";
    for (int h = 0; h < 24; h++) {
      writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", date, h));
    }

    Instant now = Instant.parse("2026-05-12T00:00:00Z"); // all hours in the past
    AuditScope scope =
        new AuditScope(
            DAY_START_MS, DAY_START_MS + 24 * HOUR_MS - 1, null, null, null, tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  /**
   * Consolidated daily file present → treat all 24 hours as covered, emit no missing_hour records.
   */
  @Test
  void consolidatedDailyFile_coversAllHours() throws IOException {
    String date = "2026-05-11";
    // No hourly files at all — only the daily consolidated file
    writeTinyZstd(dailyFile("binance", "btcusdt", "bookticker", date));

    Instant now = Instant.parse("2026-05-12T00:00:00Z");
    AuditScope scope =
        new AuditScope(
            DAY_START_MS, DAY_START_MS + 24 * HOUR_MS - 1, null, null, null, tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  /**
   * Future hours are skipped. Hours 0-5 present, hour 6 missing, but now = hour 6 start → hour 6 is
   * in the future and must NOT be reported.
   */
  @Test
  void futureHoursSkipped() throws IOException {
    String date = "2026-05-11";
    for (int h = 0; h <= 5; h++) {
      writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", date, h));
    }
    // hour 6 absent

    // now = exactly hour 6 start → hour 6 is NOT in the past yet (hourStart >= now)
    long hour6Start = DAY_START_MS + 6 * HOUR_MS;
    Instant now = Instant.ofEpochMilli(hour6Start);

    AuditScope scope =
        new AuditScope(
            DAY_START_MS, DAY_START_MS + 24 * HOUR_MS - 1, null, null, null, tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  /** Symbol not present in archive → no records (don't invent tuples that were never collected). */
  @Test
  void unknownSymbol_noRecords() throws IOException {
    // Archive contains btcusdt, but scope asks for ethusdt
    String date = "2026-05-11";
    writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", date, 0));

    Instant now = Instant.parse("2026-05-12T00:00:00Z");
    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            DAY_START_MS + 24 * HOUR_MS - 1,
            List.of("binance"),
            List.of("ethusdt"),
            null,
            tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    assertThat(records).isEmpty();
  }

  /** name() returns the expected label. */
  @Test
  void nameReturnsExpectedLabel() {
    assertThat(new MissingHourGapSource(() -> Instant.now()).name())
        .isEqualTo("MissingHourGapSource");
  }

  /**
   * Multi-day scope: missing hours reported across two dates. Each date's scope is independently
   * evaluated.
   */
  @Test
  void multiDayScope_missingHoursOnBothDates() throws IOException {
    // Day 1: hour 0 present, hours 1-23 absent
    writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", "2026-05-11", 0));
    // Day 2: hour 0 present, hours 1-23 absent
    long day2Start = DAY_START_MS + 24 * HOUR_MS; // 2026-05-12T00:00:00Z
    writeTinyZstd(hourFile("binance", "btcusdt", "bookticker", "2026-05-12", 0));

    // now = 2026-05-13T00:00:00Z → all hours on both days are in the past
    Instant now = Instant.parse("2026-05-13T00:00:00Z");

    AuditScope scope =
        new AuditScope(
            DAY_START_MS,
            day2Start + 24 * HOUR_MS - 1,
            List.of("binance"),
            List.of("btcusdt"),
            List.of("bookticker"),
            tmpDir.toString());

    MissingHourGapSource source = new MissingHourGapSource(() -> now);
    List<GapRecord> records = source.read(scope);

    // 23 missing on day 1 + 23 missing on day 2 = 46
    assertThat(records).hasSize(46);
    assertThat(records).allMatch(r -> r.reason().equals("missing_hour"));
    assertThat(records).allMatch(r -> r.source().equals("file.missing_hour"));
  }
}
