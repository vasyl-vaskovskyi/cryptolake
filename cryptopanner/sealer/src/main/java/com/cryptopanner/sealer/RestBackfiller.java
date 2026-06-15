package com.cryptopanner.sealer;

import com.cryptopanner.common.SequenceId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetches historical records to fill detected gaps in {@code trade} and {@code aggTrade} streams
 * (master spec §7.d). Each call hits one endpoint once — the multi-attempt + exponential-backoff
 * policy from §9.b.3 is deferred. Pagination across a single gap is not supported either: Binance's
 * page size is 500 records, which is comfortably larger than any gap we'd expect in production
 * (skeleton scope).
 */
public final class RestBackfiller {

  /** What endpoint to hit per gap-fillable stream (master spec §7.d). */
  public static String endpointFor(String stream) {
    return switch (stream) {
      case "trade" -> "/fapi/v1/historicalTrades";
      case "aggTrade" -> "/fapi/v1/aggTrades";
      default -> null;
    };
  }

  /** Outcome of one backfill attempt. */
  public enum Outcome {
    NOT_ATTEMPTED,
    FILLED, // every ID in [from, to] returned
    PARTIAL, // some IDs returned but not all
    FAILED // HTTP / parse failure
  }

  /**
   * Result of one attempt. {@code records} carry the parsed Binance objects (not yet wrapped in the
   * {@code backfill_record} envelope — the caller wraps).
   */
  public record AttemptResult(
      Outcome outcome, List<JsonNode> records, int httpStatus, int recordsInserted, String error) {}

  private final HttpClient client;
  private final URI baseUrl;
  private final String apiKey; // nullable; required for /historicalTrades, not for /aggTrades
  private final ObjectMapper mapper;

  public RestBackfiller(HttpClient client, URI baseUrl, String apiKey, ObjectMapper mapper) {
    this.client = client;
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
    this.mapper = mapper;
  }

  /** Builds an HttpClient suitable for backfill (same timeouts as RestPoller). */
  public static HttpClient newHttpClient() {
    return HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofSeconds(5))
        .build();
  }

  /**
   * Fetches all records in the inclusive ID range {@code [fromId, toId]} for {@code (symbol,
   * stream)}. Returns FILLED when every ID is present in the response, PARTIAL when some are
   * missing, FAILED on HTTP/parse failure.
   */
  public AttemptResult fetchGap(String symbol, String stream, long fromId, long toId) {
    String endpoint = endpointFor(stream);
    if (endpoint == null) {
      return new AttemptResult(
          Outcome.FAILED, List.of(), 0, 0, "stream " + stream + " has no backfill endpoint");
    }
    long expectedCount = toId - fromId + 1;
    // Binance default page size is 500; bump explicitly to be safe for slightly larger gaps.
    int limit = (int) Math.min(1000L, Math.max(expectedCount, 100L));
    URI uri =
        URI.create(
            baseUrl
                + endpoint
                + "?symbol="
                + symbol.toUpperCase()
                + "&fromId="
                + fromId
                + "&limit="
                + limit);
    HttpRequest.Builder rb = HttpRequest.newBuilder(uri).GET().timeout(Duration.ofSeconds(30));
    if (apiKey != null && !apiKey.isBlank()) {
      rb.header("X-MBX-APIKEY", apiKey);
    }
    HttpResponse<String> resp;
    try {
      resp = client.send(rb.build(), HttpResponse.BodyHandlers.ofString());
    } catch (Exception e) {
      return new AttemptResult(
          Outcome.FAILED, List.of(), 0, 0, e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    if (resp.statusCode() != 200) {
      return new AttemptResult(
          Outcome.FAILED,
          List.of(),
          resp.statusCode(),
          0,
          "HTTP " + resp.statusCode() + ": " + resp.body());
    }
    String idField = SequenceId.idField(stream);
    if (idField == null) {
      return new AttemptResult(
          Outcome.FAILED, List.of(), resp.statusCode(), 0, "no ID field for stream " + stream);
    }

    List<JsonNode> kept = new ArrayList<>();
    try {
      JsonNode arr = mapper.readTree(resp.body());
      if (!arr.isArray()) {
        return new AttemptResult(
            Outcome.FAILED,
            List.of(),
            resp.statusCode(),
            0,
            "response is not an array: " + resp.body());
      }
      for (JsonNode rec : arr) {
        // Binance returns the ID under different field names per endpoint — historicalTrades uses
        // "id" (not "t"), aggTrades uses "a". Normalize via per-endpoint lookup.
        String restIdField = "trade".equals(stream) ? "id" : "a";
        JsonNode idNode = rec.get(restIdField);
        if (idNode == null || !idNode.canConvertToLong()) continue;
        long id = idNode.asLong();
        if (id >= fromId && id <= toId) kept.add(rec);
      }
    } catch (IOException e) {
      return new AttemptResult(
          Outcome.FAILED, List.of(), resp.statusCode(), 0, "parse failed: " + e.getMessage());
    }

    Outcome outcome = kept.size() >= expectedCount ? Outcome.FILLED : Outcome.PARTIAL;
    return new AttemptResult(outcome, kept, resp.statusCode(), kept.size(), null);
  }

  /**
   * Wraps a single backfilled record in the {@code backfill_record} envelope. Distinct from the WS
   * {@code {stream,data}} wrapper so consumers can tell the source apart and raw fidelity is
   * preserved (master spec §3.g).
   */
  public byte[] wrapRecord(String endpoint, Instant fetchedAt, JsonNode record) throws IOException {
    ObjectNode env = mapper.createObjectNode();
    env.put("envelope", "backfill_record");
    env.put("endpoint", endpoint);
    env.put("fetched_at", fetchedAt.toString());
    env.set("record", record);
    return (mapper.writeValueAsString(env) + "\n").getBytes(StandardCharsets.UTF_8);
  }
}
