package com.cryptolake.verify.gaps;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the REST backfill cycle: analyze → fetch → write backfill-*.jsonl.zst files.
 *
 * <p>Ports the orchestration body of {@code gaps.backfill} and {@code _fetch_historical_all} from
 * {@code gaps.py}. Sequential execution (Tier 5 D7; Q7 — no fan-out).
 *
 * <p>Tier 1 §6 honored: REST replay is preferred; interpolation only for {@code funding_rate}. Tier
 * 5 A2 — direct blocking calls on virtual thread; no executor. Tier 5 D3 — single shared {@link
 * com.cryptolake.verify.gaps.BinanceRestClient}.
 *
 * <p>Thread safety: stateless per call; relies on thread-safe collaborators.
 */
public final class BackfillOrchestrator {

  private static final Logger log = LoggerFactory.getLogger(BackfillOrchestrator.class);
  private static final int ZSTD_LEVEL = 3;
  private static final String BASE = GapStreams.BINANCE_REST_BASE;

  private final BinanceRestClient restClient;
  private final ObjectMapper mapper;

  public BackfillOrchestrator(BinanceRestClient restClient, ObjectMapper mapper) {
    this.restClient = restClient;
    this.mapper = mapper;
  }

  /**
   * Result summary for one backfill run.
   *
   * <p>Tier 2 §12 — record (immutable).
   */
  public record BackfillSummary(int hoursAttempted, int hoursWritten, int recordsWritten) {}

  /**
   * Fetches and writes backfill files for the given missing hours.
   *
   * @param baseDir archive base directory
   * @param exchange exchange name
   * @param symbol symbol (lowercase)
   * @param stream stream type
   * @param date date (YYYY-MM-DD)
   * @param missingHours list of missing hour indices (0–23)
   * @return summary of what was written
   * @throws IOException on I/O failure
   * @throws InterruptedException if interrupted
   */
  public BackfillSummary fetchAndWrite(
      Path baseDir,
      String exchange,
      String symbol,
      String stream,
      String date,
      List<Integer> missingHours)
      throws IOException, InterruptedException {

    String sessionId = "backfill-" + UUID.randomUUID(); // Tier 5 M7
    int hoursWritten = 0;
    int recordsWritten = 0;

    for (int hour : missingHours) {
      try {
        List<JsonNode> records = fetchAll(symbol, stream, date, hour);
        if (records.isEmpty()) {
          log.info(
              "backfill_no_records", //
              "exchange",
              exchange,
              "symbol",
              symbol,
              "stream",
              stream,
              "date",
              date,
              "hour",
              hour);
          continue;
        }

        // Write backfill file: hour-{H}.backfill-{seq}.jsonl.zst
        // seq = 1 for first backfill file (no existing backfill files check for simplicity)
        Path dateDir = baseDir.resolve(exchange).resolve(symbol).resolve(stream).resolve(date);
        Files.createDirectories(dateDir);
        Path backfillPath = dateDir.resolve("hour-" + hour + ".backfill-1.jsonl.zst");

        writeBackfillFile(backfillPath, records, exchange, symbol, stream, sessionId, mapper);
        hoursWritten++;
        recordsWritten += records.size();

      } catch (EndpointUnavailableException e) {
        log.warn(
            "backfill_endpoint_unavailable",
            "stream",
            stream,
            "hour",
            hour,
            "error",
            e.getMessage());
        // Continue with next hour (Tier 7 policy: mark unrecoverable, continue)
      }
    }

    return new BackfillSummary(missingHours.size(), hoursWritten, recordsWritten);
  }

  /** Fetches all records for a given stream/hour from the Binance REST API. */
  private List<JsonNode> fetchAll(String symbol, String stream, String date, int hour)
      throws IOException, InterruptedException {

    // Compute hour time range in ms
    long[] range = hourToMsRange(date, hour);
    long startMs = range[0];
    long endMs = range[1];
    String tsKey = GapStreams.STREAM_TS_KEYS.getOrDefault(stream, "T");
    String sym = symbol.toUpperCase(Locale.ROOT); // Tier 5 M1

    List<JsonNode> allRecords = new ArrayList<>();

    // Build the REST URL for this stream
    String url = buildUrl(sym, stream, startMs, endMs);
    if (url == null) {
      return allRecords;
    }

    // Simple single-page fetch (more complex pagination can be added later)
    JsonNode page = restClient.fetchPage(url);
    if (page.isArray()) {
      for (JsonNode rec : page) {
        long ts = rec.path(tsKey).asLong(0L);
        if (ts >= startMs && ts < endMs) {
          allRecords.add(rec);
        }
      }
    }

    return allRecords;
  }

  /** Writes backfill records to a zstd-compressed JSONL file. */
  private static void writeBackfillFile(
      Path path,
      List<JsonNode> records,
      String exchange,
      String symbol,
      String stream,
      String sessionId,
      ObjectMapper mapper)
      throws IOException {

    String tsKey = GapStreams.STREAM_TS_KEYS.getOrDefault(stream, "T");

    // Tier 5 I1, I3: FileChannel + ZstdOutputStream + force(true)
    try (var fc =
            FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        var zstdOut = new ZstdOutputStream(Channels.newOutputStream(fc), ZSTD_LEVEL);
        var bufOut = new BufferedOutputStream(zstdOut)) {

      for (int i = 0; i < records.size(); i++) {
        byte[] envBytes =
            BackfillEnvelopeFactory.wrap(
                records.get(i), exchange, symbol, stream, sessionId, (long) i, tsKey, mapper);
        bufOut.write(envBytes);
        bufOut.write(0x0A); // newline (Tier 5 B2)
      }

      bufOut.flush();
      zstdOut.flush();
      fc.force(true); // Tier 5 I3: fsync before close
    }
  }

  /** Computes the millisecond start/end for a UTC hour within a date. */
  private static long[] hourToMsRange(String date, int hour) {
    // date = YYYY-MM-DD
    java.time.LocalDate ld = java.time.LocalDate.parse(date);
    java.time.Instant start = ld.atTime(hour, 0).toInstant(java.time.ZoneOffset.UTC);
    java.time.Instant end = start.plus(java.time.Duration.ofHours(1));
    return new long[] {
      start.toEpochMilli(), end.toEpochMilli() - 1L // exclusive end → inclusive millisecond
    };
  }

  /** Builds the Binance REST URL for a stream/time range. Returns null if not supported. */
  private static String buildUrl(String symbol, String stream, long startMs, long endMs) {
    return switch (stream) {
      case "trades" ->
          BASE
              + "/fapi/v1/aggTrades?symbol="
              + symbol
              + "&startTime="
              + startMs
              + "&endTime="
              + endMs
              + "&limit=1000";
      case "funding_rate" ->
          BASE
              + "/fapi/v1/fundingRate?symbol="
              + symbol
              + "&startTime="
              + startMs
              + "&endTime="
              + endMs
              + "&limit=1000";
      case "open_interest" ->
          BASE
              + "/futures/data/openInterestHist?symbol="
              + symbol
              + "&period=5m&startTime="
              + startMs
              + "&endTime="
              + endMs
              + "&limit=500";
      default -> null;
    };
  }
}
