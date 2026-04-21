package com.cryptolake.common.envelope;

import com.cryptolake.common.util.ClockSupplier;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Map;

/**
 * Canonical gap envelope record with optional restart-metadata fields.
 *
 * <p>Field order matches Python's {@code create_gap_envelope} dict insertion order (Tier 5 B1; Tier
 * 3 §21). Optional fields are omitted from JSON when {@code null} via
 * {@code @JsonInclude(NON_NULL)} (replaces Python's {@code _SENTINEL} pattern).
 *
 * <p>Compact constructor validates {@code reason} against {@link GapReasons#VALID} (Tier 2 §16;
 * Tier 5 M6).
 *
 * <p>Immutable record — no setters (Tier 2 §12).
 */
@JsonPropertyOrder({
  "v",
  "type",
  "exchange",
  "symbol",
  "stream",
  "received_at",
  "collector_session_id",
  "session_seq",
  "gap_start_ts",
  "gap_end_ts",
  "reason",
  "detail",
  "component",
  "cause",
  "planned",
  "classifier",
  "evidence",
  "maintenance_id"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record GapEnvelope(
    @JsonProperty("v") int v,
    @JsonProperty("type") String type,
    @JsonProperty("exchange") String exchange,
    @JsonProperty("symbol") String symbol,
    @JsonProperty("stream") String stream,
    @JsonProperty("received_at") long receivedAt,
    @JsonProperty("collector_session_id") String collectorSessionId,
    @JsonProperty("session_seq") long sessionSeq,
    @JsonProperty("gap_start_ts") long gapStartTs,
    @JsonProperty("gap_end_ts") long gapEndTs,
    @JsonProperty("reason") String reason,
    @JsonProperty("detail") String detail,
    // Optional restart metadata fields — omitted from JSON when null
    @JsonProperty("component") String component,
    @JsonProperty("cause") String cause,
    @JsonProperty("planned") Boolean planned,
    @JsonProperty("classifier") String classifier,
    @JsonProperty("evidence") Map<String, Object> evidence,
    @JsonProperty("maintenance_id") String maintenanceId) {

  /** Compact constructor: validates reason (Tier 5 M6; Tier 2 §16). */
  public GapEnvelope {
    GapReasons.requireValid(reason);
  }

  /**
   * Creates a gap envelope with required fields only; all optional restart-metadata fields are
   * {@code null} and will be omitted from JSON.
   *
   * <p>{@code session_seq = -1L} is the sentinel for writer-injected envelopes (Tier 5 M10).
   */
  public static GapEnvelope create(
      String exchange,
      String symbol,
      String stream,
      String collectorSessionId,
      long sessionSeq,
      long gapStartTs,
      long gapEndTs,
      String reason,
      String detail,
      ClockSupplier clock) {
    return new GapEnvelope(
        1,
        "gap",
        exchange,
        symbol,
        stream,
        clock.nowNs(),
        collectorSessionId,
        sessionSeq,
        gapStartTs,
        gapEndTs,
        reason,
        detail,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  /**
   * Creates a gap envelope with optional restart-metadata fields. Any argument may be {@code null};
   * non-null values become serialized JSON fields at the end of the object.
   */
  public static GapEnvelope createWithRestartMetadata(
      String exchange,
      String symbol,
      String stream,
      String collectorSessionId,
      long sessionSeq,
      long gapStartTs,
      long gapEndTs,
      String reason,
      String detail,
      ClockSupplier clock,
      String component,
      String cause,
      Boolean planned,
      String classifier,
      Map<String, Object> evidence,
      String maintenanceId) {
    return new GapEnvelope(
        1,
        "gap",
        exchange,
        symbol,
        stream,
        clock.nowNs(),
        collectorSessionId,
        sessionSeq,
        gapStartTs,
        gapEndTs,
        reason,
        detail,
        component,
        cause,
        planned,
        classifier,
        evidence,
        maintenanceId);
  }
}
