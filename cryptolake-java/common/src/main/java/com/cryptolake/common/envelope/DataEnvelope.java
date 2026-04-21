package com.cryptolake.common.envelope;

import com.cryptolake.common.util.ClockSupplier;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.common.util.Sha256;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Canonical data envelope record.
 *
 * <p>Field order is LOAD-BEARING — it must match Python's {@code create_data_envelope} dict
 * insertion order exactly (Tier 5 B1; Tier 3 §21). The {@code @JsonPropertyOrder} annotation
 * enforces this on Jackson serialization.
 *
 * <p>Immutable record — no setters (Tier 2 §12). {@code rawSha256} is computed once at factory time
 * and stored (Tier 1 §2).
 */
@JsonPropertyOrder({
  "v",
  "type",
  "exchange",
  "symbol",
  "stream",
  "received_at",
  "exchange_ts",
  "collector_session_id",
  "session_seq",
  "raw_text",
  "raw_sha256"
})
public record DataEnvelope(
    @JsonProperty("v") int v,
    @JsonProperty("type") String type,
    @JsonProperty("exchange") String exchange,
    @JsonProperty("symbol") String symbol,
    @JsonProperty("stream") String stream,
    @JsonProperty("received_at") long receivedAt,
    @JsonProperty("exchange_ts") long exchangeTs,
    @JsonProperty("collector_session_id") String collectorSessionId,
    @JsonProperty("session_seq") long sessionSeq,
    @JsonProperty("raw_text") String rawText,
    @JsonProperty("raw_sha256") String rawSha256) {

  /**
   * Creates a data envelope, computing {@code v=1}, {@code type="data"}, {@code
   * receivedAt=clock.nowNs()}, and {@code rawSha256=Sha256.hexDigestUtf8(rawText)}.
   *
   * <p>Tier 1 §1: {@code rawText} is received as a pre-extracted string (caller captures
   * byte-for-byte before any outer parse). Tier 1 §2: SHA-256 computed here, never downstream.
   *
   * @param exchange exchange identifier (e.g. {@code "binance"})
   * @param symbol symbol in lowercase (Tier 5 M1)
   * @param stream stream type (e.g. {@code "trades"})
   * @param rawText raw exchange payload, byte-for-byte passthrough (Tier 1 §7)
   * @param exchangeTs exchange timestamp (ms or ns; may exceed 2^31 — Tier 5 E1)
   * @param collectorSessionId session ID in format {@code collectorId_yyyy-MM-dd'T'HH:mm:ss'Z'}
   *     (Tier 5 M7)
   * @param sessionSeq monotonic sequence; {@code -1L} is the writer-injected sentinel (Tier 5 M10)
   * @param clock nanosecond clock; use {@link Clocks#systemNanoClock()} in production
   */
  public static DataEnvelope create(
      String exchange,
      String symbol,
      String stream,
      String rawText,
      long exchangeTs,
      String collectorSessionId,
      long sessionSeq,
      ClockSupplier clock) {
    return new DataEnvelope(
        1,
        "data",
        exchange,
        symbol,
        stream,
        clock.nowNs(),
        exchangeTs,
        collectorSessionId,
        sessionSeq,
        rawText,
        Sha256.hexDigestUtf8(rawText));
  }
}
