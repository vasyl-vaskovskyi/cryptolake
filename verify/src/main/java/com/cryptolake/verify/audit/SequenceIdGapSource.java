package com.cryptolake.verify.audit;

import com.cryptolake.common.envelope.GapReason;
import com.cryptolake.verify.archive.ArchiveFile;
import com.cryptolake.verify.archive.ArchiveScanner;
import com.cryptolake.verify.archive.DecompressAndParse;
import com.cryptolake.verify.integrity.BooktickerContinuity;
import com.cryptolake.verify.integrity.DepthContinuity;
import com.cryptolake.verify.integrity.IntegrityCheckResult;
import com.cryptolake.verify.integrity.TradesContinuity;
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

/**
 * Emits {@link GapRecord}s for sequence-ID continuity breaks found inside on-disk {@code
 * .jsonl.zst} archive files.
 *
 * <p>Wraps the existing continuity walkers ({@link TradesContinuity}, {@link DepthContinuity},
 * {@link BooktickerContinuity}). For each in-scope file whose stream is one of {@code trades},
 * {@code depth}, or {@code bookticker}, the appropriate walker is invoked and each detected {@link
 * IntegrityCheckResult.Break} is converted to a {@link GapRecord} with:
 *
 * <ul>
 *   <li>{@code source = "file.sequence_id"}
 *   <li>{@code reason = "session_seq_skip"}
 *   <li>{@code startMs = endMs = atReceived / 1_000_000} (nanoseconds → milliseconds)
 *   <li>{@code detail = "<field>: expected <exp>, got <act> (missing <n>)"}
 * </ul>
 *
 * <p>Streams without ID-continuity semantics ({@code funding_rate}, {@code liquidations}, {@code
 * depth_snapshot}, {@code open_interest}) are silently skipped.
 *
 * <p>Thread safety: stateless after construction.
 */
public final class SequenceIdGapSource implements GapSource {

  private static final String SOURCE_LABEL = "file.sequence_id";
  private static final GapReason REASON = GapReason.SESSION_SEQ_SKIP;
  private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
  private static final long HOUR_MS = 3_600_000L;

  private final ObjectMapper mapper;

  public SequenceIdGapSource(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public String name() {
    return "SequenceIdGapSource";
  }

  @Override
  public List<GapRecord> read(AuditScope scope) {
    List<GapRecord> result = new ArrayList<>();
    Path baseDir = Path.of(scope.baseDir());

    for (String date : datesInScope(scope)) {
      List<ArchiveFile> files;
      try {
        files = ArchiveScanner.scan(baseDir, date, null, null, null);
      } catch (IOException e) {
        throw new UncheckedIOException("ArchiveScanner failed for date " + date, e);
      }

      for (ArchiveFile archiveFile : files) {
        if (!matchesScope(archiveFile, scope)) {
          continue;
        }

        String stream = archiveFile.stream();
        // Skip streams that have no ID-continuity semantics.
        if (!isContinuityStream(stream)) {
          continue;
        }

        // Time-range overlap check: use a simple hour-based window from the filename.
        int hour = extractHour(archiveFile.path().getFileName().toString());
        if (hour >= 0) {
          long fileStartMs = fileStartMs(date, hour);
          long fileEndMs = fileStartMs + HOUR_MS;
          if (fileEndMs <= scope.startMs() || fileStartMs > scope.endMs()) {
            continue;
          }
        }

        List<JsonNode> nodes;
        try {
          nodes = DecompressAndParse.parse(archiveFile.path(), mapper);
        } catch (IOException e) {
          throw new UncheckedIOException("DecompressAndParse failed for " + archiveFile.path(), e);
        }

        IntegrityCheckResult checkResult = runWalker(stream, nodes.iterator());

        for (IntegrityCheckResult.Break brk : checkResult.breaks()) {
          long atMs = brk.atReceived() / 1_000_000L;
          // Depth pu-chain and bookticker u-backwards breaks are reference-mismatches, not
          // additive skips — their walkers report missing=0. Don't print "(missing 0)".
          String detail =
              brk.missing() > 0
                  ? String.format(
                      "%s: expected %d, got %d (missing %d)",
                      brk.field(), brk.expected(), brk.actual(), brk.missing())
                  : String.format(
                      "%s: expected %d, got %d (chain break)",
                      brk.field(), brk.expected(), brk.actual());
          result.add(
              new GapRecord(
                  SOURCE_LABEL,
                  archiveFile.exchange(),
                  archiveFile.symbol(),
                  stream,
                  atMs,
                  atMs,
                  REASON,
                  detail));
        }
      }
    }
    return result;
  }

  // ---- helpers ----

  /** Returns true iff the stream has ID-continuity semantics that we can check. */
  private static boolean isContinuityStream(String stream) {
    return "trades".equals(stream) || "depth".equals(stream) || "bookticker".equals(stream);
  }

  /**
   * Dispatches to the appropriate continuity walker for the given stream.
   *
   * @param stream one of "trades", "depth", "bookticker"
   * @param records iterator of parsed envelope {@link JsonNode}s
   * @return walker result
   */
  private IntegrityCheckResult runWalker(String stream, java.util.Iterator<JsonNode> records) {
    return switch (stream) {
      case "trades" -> TradesContinuity.check(records, mapper);
      case "depth" -> DepthContinuity.check(records, mapper);
      case "bookticker" -> BooktickerContinuity.check(records, mapper);
      default ->
          throw new IllegalStateException("runWalker called for non-continuity stream: " + stream);
    };
  }

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
    // Simple extraction: look for "hour-<digits>" prefix.
    if (!filename.startsWith("hour-")) {
      return -1;
    }
    int dotIdx = filename.indexOf('.', 5);
    if (dotIdx < 0) {
      return -1;
    }
    try {
      return Integer.parseInt(filename.substring(5, dotIdx));
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /** Returns the epoch-millis of {@code YYYY-MM-DD}T{@code hour}:00:00Z. */
  private static long fileStartMs(String date, int hour) {
    LocalDate d = LocalDate.parse(date, DATE_FMT);
    return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() + (long) hour * HOUR_MS;
  }
}
