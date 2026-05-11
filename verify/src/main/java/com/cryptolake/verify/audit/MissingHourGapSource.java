package com.cryptolake.verify.audit;

import com.cryptolake.verify.archive.ArchiveScanner;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Emits synthetic {@link GapRecord}s for hours where neither an hourly {@code hour-H.jsonl.zst} nor
 * a consolidated {@code <date>.jsonl.zst} file is present.
 *
 * <p>Only emits records for {@code (exchange, symbol, stream)} tuples that have at least one file
 * present somewhere in the archive under the base directory — this avoids inventing gaps for
 * streams the operator never collected.
 *
 * <p>Future hours (where {@code hourStart >= now()}) are skipped — they haven't happened yet.
 *
 * <p>Each emitted record has {@code source="file.missing_hour"} and {@code reason="missing_hour"}.
 *
 * <p>Thread safety: stateless after construction.
 */
public final class MissingHourGapSource implements GapSource {

  private static final String SOURCE_LABEL = "file.missing_hour";
  private static final String REASON = "missing_hour";
  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final long HOUR_MS = 3_600_000L;
  private static final Pattern HOUR_PATTERN =
      Pattern.compile("^hour-(\\d{1,2})(?:\\.(?:late|backfill)-\\d+)?\\.jsonl\\.zst$");

  private final Supplier<Instant> nowSupplier;

  public MissingHourGapSource(Supplier<Instant> nowSupplier) {
    this.nowSupplier = nowSupplier;
  }

  @Override
  public String name() {
    return "MissingHourGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    Instant now = nowSupplier.get();
    Path baseDir = Path.of(scope.baseDir());
    List<String> dates = datesInScope(scope);
    List<GapRecord> result = new ArrayList<>();

    for (String date : dates) {
      // Enumerate which (exchange, symbol, stream) tuples have presence on this date.
      // We use ArchiveScanner for hourly files; for daily files we must look separately
      // because ArchiveScanner's FILE_PATTERN only matches hour-* filenames.
      Set<Tuple> presenceTuples;
      try {
        presenceTuples = collectPresenceTuples(baseDir, date, scope);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to scan archive for date " + date, e);
      }

      long dayStartMs = dateStartMs(date);

      for (Tuple tuple : presenceTuples) {
        Path dateDir =
            baseDir
                .resolve(tuple.exchange)
                .resolve(tuple.symbol)
                .resolve(tuple.stream)
                .resolve(date);

        // If a daily consolidated file exists, all 24 hours are covered — skip this tuple.
        if (dailyFileExists(dateDir, date)) {
          continue;
        }

        // Collect which hours are present via hourly files.
        boolean[] present = collectPresentHours(dateDir);

        // Emit missing_hour for each absent hour that is within scope and not in the future.
        for (int h = 0; h < 24; h++) {
          long hourStartMs = dayStartMs + (long) h * HOUR_MS;
          // Skip future hours.
          if (hourStartMs >= now.toEpochMilli()) {
            continue;
          }
          // Skip hours outside the scope time window.
          long hourEndMs = hourStartMs + HOUR_MS - 1;
          if (hourEndMs < scope.startMs() || hourStartMs > scope.endMs()) {
            continue;
          }
          if (!present[h]) {
            result.add(
                new GapRecord(
                    SOURCE_LABEL,
                    tuple.exchange,
                    tuple.symbol,
                    tuple.stream,
                    hourStartMs,
                    hourStartMs + 3_599_999L,
                    REASON,
                    "hour " + h + " file absent"));
          }
        }
      }
    }
    return result;
  }

  // ---- helpers ----

  /**
   * Returns a sorted set of {@code (exchange, symbol, stream)} tuples that have at least one file
   * (hourly or daily) present for the given date, filtered by scope.
   */
  private static Set<Tuple> collectPresenceTuples(Path baseDir, String date, AuditScope scope)
      throws IOException {
    Set<Tuple> tuples = new LinkedHashSet<>();

    // 1. Hourly files via ArchiveScanner (respects exchange/symbol/stream filters).
    var hourlyFiles = ArchiveScanner.scan(baseDir, date, null, null, null);
    for (var f : hourlyFiles) {
      if (!matchesScope(f.exchange(), f.symbol(), f.stream(), scope)) {
        continue;
      }
      tuples.add(new Tuple(f.exchange(), f.symbol(), f.stream()));
    }

    // 2. Daily consolidated files: walk the date-level directories directly.
    // Path pattern: <baseDir>/<exchange>/<symbol>/<stream>/<date>/<date>.jsonl.zst
    // We need to iterate exchange → symbol → stream directories and check for <date>.jsonl.zst.
    if (Files.exists(baseDir)) {
      try (var exchanges = Files.list(baseDir)) {
        for (var exchangeDir : (Iterable<Path>) exchanges::iterator) {
          if (!Files.isDirectory(exchangeDir)) continue;
          String exchange = exchangeDir.getFileName().toString();
          if (!isEmpty(scope.exchanges()) && !scope.exchanges().contains(exchange)) continue;
          try (var symbols = Files.list(exchangeDir)) {
            for (var symbolDir : (Iterable<Path>) symbols::iterator) {
              if (!Files.isDirectory(symbolDir)) continue;
              String symbol = symbolDir.getFileName().toString();
              if (!isEmpty(scope.symbols()) && !scope.symbols().contains(symbol)) continue;
              try (var streams = Files.list(symbolDir)) {
                for (var streamDir : (Iterable<Path>) streams::iterator) {
                  if (!Files.isDirectory(streamDir)) continue;
                  String stream = streamDir.getFileName().toString();
                  if (!isEmpty(scope.streams()) && !scope.streams().contains(stream)) continue;
                  Path dateDir = streamDir.resolve(date);
                  if (Files.isDirectory(dateDir) && dailyFileExists(dateDir, date)) {
                    tuples.add(new Tuple(exchange, symbol, stream));
                  }
                }
              }
            }
          }
        }
      }
    }
    return tuples;
  }

  /** Returns true if {@code <date>.jsonl.zst} exists inside {@code dateDir}. */
  private static boolean dailyFileExists(Path dateDir, String date) {
    return Files.exists(dateDir.resolve(date + ".jsonl.zst"));
  }

  /**
   * Scans {@code dateDir} for hourly {@code hour-H[.late|backfill-N].jsonl.zst} files and returns a
   * 24-element boolean array marking which hours are present.
   */
  private static boolean[] collectPresentHours(Path dateDir) {
    boolean[] present = new boolean[24];
    if (!Files.exists(dateDir)) {
      return present;
    }
    try (var files = Files.list(dateDir)) {
      files.forEach(
          f -> {
            Matcher m = HOUR_PATTERN.matcher(f.getFileName().toString());
            if (m.matches()) {
              int h = Integer.parseInt(m.group(1));
              if (h >= 0 && h < 24) {
                present[h] = true;
              }
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to list dateDir " + dateDir, e);
    }
    return present;
  }

  /** Returns all YYYY-MM-DD dates covered by the scope (UTC). */
  private static List<String> datesInScope(AuditScope scope) {
    LocalDate start = Instant.ofEpochMilli(scope.startMs()).atZone(ZoneOffset.UTC).toLocalDate();
    LocalDate end = Instant.ofEpochMilli(scope.endMs()).atZone(ZoneOffset.UTC).toLocalDate();
    List<String> dates = new ArrayList<>();
    for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
      dates.add(d.format(DATE_FMT));
    }
    return dates;
  }

  /** Returns the UTC epoch millis for the start of midnight on the given date. */
  private static long dateStartMs(String date) {
    return LocalDate.parse(date, DATE_FMT).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
  }

  private static boolean matchesScope(
      String exchange, String symbol, String stream, AuditScope scope) {
    if (!isEmpty(scope.exchanges()) && !scope.exchanges().contains(exchange)) return false;
    if (!isEmpty(scope.symbols()) && !scope.symbols().contains(symbol)) return false;
    if (!isEmpty(scope.streams()) && !scope.streams().contains(stream)) return false;
    return true;
  }

  private static boolean isEmpty(List<String> list) {
    return list == null || list.isEmpty();
  }

  private record Tuple(String exchange, String symbol, String stream) {}
}
