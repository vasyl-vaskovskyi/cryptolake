package com.cryptolake.collector.liveness;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Per-(symbol, stream) liveness heartbeat envelope.
 *
 * <p>Schema matches Python's {@code HEARTBEAT_ENVELOPE_FIELDS} from {@code src/common/envelope.py}
 * (commit 3e068b7). Published to the same Kafka topic as data envelopes for the stream.
 *
 * <p>Immutable record (Tier 2 §12).
 */
@JsonPropertyOrder({
  "v",
  "type",
  "exchange",
  "symbol",
  "stream",
  "received_at",
  "collector_session_id",
  "emitted_at_ns",
  "last_data_at_ns",
  "last_session_seq",
  "status"
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record HeartbeatEnvelope(
    @JsonProperty("v") int v,
    @JsonProperty("type") String type,
    @JsonProperty("exchange") String exchange,
    @JsonProperty("symbol") String symbol,
    @JsonProperty("stream") String stream,
    @JsonProperty("received_at") long receivedAt,
    @JsonProperty("collector_session_id") String collectorSessionId,
    @JsonProperty("emitted_at_ns") long emittedAtNs,
    @JsonProperty("last_data_at_ns") Long lastDataAtNs,
    @JsonProperty("last_session_seq") long lastSessionSeq,
    @JsonProperty("status") String status) {

  /**
   * Creates a heartbeat envelope with the given status and metadata.
   */
  public static HeartbeatEnvelope of(
      String exchange,
      String symbol,
      String stream,
      String collectorSessionId,
      long emittedAtNs,
      Long lastDataAtNs,
      long lastSessionSeq,
      HeartbeatStatus status) {
    return new HeartbeatEnvelope(
        1,
        "heartbeat",
        exchange,
        symbol,
        stream,
        emittedAtNs,
        collectorSessionId,
        emittedAtNs,
        lastDataAtNs,
        lastSessionSeq,
        status.toJsonValue());
  }
}
