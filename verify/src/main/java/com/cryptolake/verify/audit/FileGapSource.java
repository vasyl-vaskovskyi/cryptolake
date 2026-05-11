package com.cryptolake.verify.audit;

import com.cryptolake.verify.archive.ArchiveFile;
import com.cryptolake.verify.archive.ArchiveScanner;
import com.cryptolake.verify.archive.DecompressAndParse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reads gap records from on-disk {@code .jsonl.zst} archive files within the given {@link
 * AuditScope}.
 *
 * <p>Walks the archive using {@link ArchiveScanner}, decompresses each file via {@link
 * DecompressAndParse}, filters to {@code type=="gap"} envelopes, and converts each to a {@link
 * GapRecord} with {@code source="file.envelope"}.
 *
 * <p>Constructor accepts an {@link ObjectMapper} (dependency injection — callers should pass the
 * shared mapper from {@code EnvelopeCodec.newMapper()}).
 *
 * <p>Thread safety: stateless after construction.
 */
public final class FileGapSource implements GapSource {

  private static final String SOURCE_LABEL = "file.envelope";
  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final Pattern HOUR_PATTERN =
      Pattern.compile("^hour-(\\d{1,2})(?:\\.(?:late|backfill)-\\d+)?\\.jsonl\\.zst$");
  private static final long HOUR_MS = 3_600_000L;

  private final ObjectMapper mapper;

  public FileGapSource(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String name() {
    return "FileGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    List<GapRecord> result = new ArrayList<>();
    Path baseDir = Path.of(scope.baseDir());

    for (String date : datesInScope(scope)) {
      List<ArchiveFile> files;
      try {
        // Scan all files for this date; apply scope filters below in code so we make one
        // scanner call per date rather than one per (exchange × symbol × stream) combination.
        files = ArchiveScanner.scan(baseDir, date, null, null, null);
      } catch (IOException e) {
        throw new UncheckedIOException("ArchiveScanner failed for date " + date, e);
      }

      for (ArchiveFile archiveFile : files) {
        if (!matchesScope(archiveFile, scope)) {
          continue;
        }
        int hour = extractHour(archiveFile.path().getFileName().toString());
        if (hour < 0) {
          continue;
        }
        long fileStartMs = fileStartMs(date, hour);
        long fileEndMs = fileStartMs + HOUR_MS;
        // Overlap check: file covers [fileStartMs, fileEndMs); scope is [startMs, endMs].
        if (fileEndMs <= scope.startMs() || fileStartMs > scope.endMs()) {
          continue;
        }

        List<JsonNode> nodes;
        try {
          nodes = DecompressAndParse.parse(archiveFile.path(), mapper);
        } catch (IOException e) {
          throw new UncheckedIOException("DecompressAndParse failed for " + archiveFile.path(), e);
        }

        for (JsonNode node : nodes) {
          if (!"gap".equals(node.path("type").asText())) {
            continue;
          }
          result.add(toGapRecord(node, archiveFile));
        }
      }
    }
    return result;
  }

  // ---- helpers ----

  private static boolean matchesScope(ArchiveFile file, AuditScope scope) {
    if (!isEmpty(scope.exchanges()) && !scope.exchanges().contains(file.exchange())) {
      return false;
    }
    if (!isEmpty(scope.symbols()) && !scope.symbols().contains(file.symbol())) {
      return false;
    }
    if (!isEmpty(scope.streams()) && !scope.streams().contains(file.stream())) {
      return false;
    }
    return true;
  }

  private static boolean isEmpty(List<String> list) {
    return list == null || list.isEmpty();
  }

  /**
   * Returns all YYYY-MM-DD dates that fall within [startMs, endMs] (UTC). The result always
   * includes the date of startMs and the date of endMs, plus any dates in between.
   */
  private static List<String> datesInScope(AuditScope scope) {
    LocalDate start = Instant.ofEpochMilli(scope.startMs()).atZone(ZoneOffset.UTC).toLocalDate();
    LocalDate end = Instant.ofEpochMilli(scope.endMs()).atZone(ZoneOffset.UTC).toLocalDate();
    List<String> dates = new ArrayList<>();
    for (LocalDate d = start; !d.isAfter(end); d = d.plusDays(1)) {
      dates.add(d.format(DATE_FMT));
    }
    return dates;
  }

  /**
   * Returns the hour integer from a filename like {@code hour-9.jsonl.zst}, or -1 if not matched.
   */
  private static int extractHour(String filename) {
    Matcher m = HOUR_PATTERN.matcher(filename);
    if (!m.matches()) {
      return -1;
    }
    return Integer.parseInt(m.group(1));
  }

  /** Returns the epoch-millis of {@code YYYY-MM-DD}T{@code hour}:00:00Z. */
  private static long fileStartMs(String date, int hour) {
    LocalDate d = LocalDate.parse(date, DATE_FMT);
    return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + (long) hour * HOUR_MS;
  }

  /** Converts a gap {@link JsonNode} to a {@link GapRecord}. */
  private static GapRecord toGapRecord(JsonNode node, ArchiveFile archiveFile) {
    long gapStartNs = node.path("gap_start_ts").asLong();
    long gapEndNs = node.path("gap_end_ts").asLong();
    String reason = node.path("reason").asText();
    String detail = node.path("detail").isNull() ? null : node.path("detail").asText(null);
    return new GapRecord(
        SOURCE_LABEL,
        archiveFile.exchange(),
        archiveFile.symbol(),
        archiveFile.stream(),
        gapStartNs / 1_000_000L,
        gapEndNs / 1_000_000L,
        reason,
        detail);
  }
}
