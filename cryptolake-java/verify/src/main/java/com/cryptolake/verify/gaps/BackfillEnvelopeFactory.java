package com.cryptolake.verify.gaps;

import com.cryptolake.common.util.Sha256;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;

/**
 * Wraps a Binance REST record into a backfill data envelope.
 *
 * <p>Ports {@code _wrap_backfill_envelope} from {@code gaps.py:60-77}. Sets {@code
 * _topic="backfill"}, {@code _partition=0}, {@code _offset=seq}, {@code source="backfill"}. Uses a
 * raw {@link ObjectNode} with explicit insertion order (design §6.4 fallback — no
 * common.DataEnvelope modification needed).
 *
 * <p>Tier 1 §1: {@code raw_text} is the Java serialization of the REST record; Tier 1 §2: {@code
 * raw_sha256} is computed over {@code raw_text} bytes via {@link Sha256#hexDigestUtf8}. Tier 5 B1:
 * insertion order matches Python's dict construction. Tier 5 B2: compact JSON, no indent. Jackson
 * default config matches orjson default (Q11).
 *
 * <p>Thread safety: stateless utility.
 */
public final class BackfillEnvelopeFactory {

  private BackfillEnvelopeFactory() {}

  /**
   * Wraps a single Binance REST record into a backfill envelope byte array (JSON + newline).
   *
   * @param rawRecord parsed JSON record from the Binance REST API
   * @param exchange exchange name (e.g. "binance")
   * @param symbol symbol name (lowercase)
   * @param stream stream type (e.g. "trades")
   * @param sessionId backfill session ID (e.g. "backfill-<uuid>")
   * @param seq monotonic sequence number within this session
   * @param exchangeTsKey the field in {@code rawRecord} to use as {@code exchange_ts}
   * @param mapper shared {@link ObjectMapper} (Tier 5 B6)
   * @return compact JSON bytes without newline (caller appends 0x0A if needed)
   * @throws IOException on serialization failure
   */
  public static byte[] wrap(
      JsonNode rawRecord,
      String exchange,
      String symbol,
      String stream,
      String sessionId,
      long seq,
      String exchangeTsKey,
      ObjectMapper mapper)
      throws IOException {

    // raw_text: compact serialization of the REST record (Tier 5 B2)
    // Jackson default (via EnvelopeCodec.newMapper()) matches orjson default per Q11
    String rawText = mapper.writeValueAsString(rawRecord);
    String rawSha256 = Sha256.hexDigestUtf8(rawText); // Tier 1 §2

    // exchange_ts: from the record's timestamp field (Tier 5 E1 — asLong)
    long exchangeTs =
        rawRecord.path(exchangeTsKey).asLong(System.currentTimeMillis()); // fallback = now ms

    long receivedAt = System.nanoTime(); // approximate; nanosecond wall-clock (Tier 5 E2)

    // Build ObjectNode with explicit insertion order (design §6.4; Tier 5 B1)
    // Order: v, type, source, exchange, symbol, stream, received_at, exchange_ts,
    //        collector_session_id, session_seq, raw_text, raw_sha256, _topic, _partition, _offset
    ObjectNode node = mapper.createObjectNode();
    node.put("v", 1);
    node.put("type", "data");
    node.put("source", "backfill");
    node.put("exchange", exchange);
    node.put("symbol", symbol);
    node.put("stream", stream);
    node.put("received_at", receivedAt);
    node.put("exchange_ts", exchangeTs);
    node.put("collector_session_id", sessionId);
    node.put("session_seq", seq);
    node.put("raw_text", rawText);
    node.put("raw_sha256", rawSha256);
    node.put("_topic", "backfill");
    node.put("_partition", 0);
    node.put("_offset", seq);

    return mapper.writeValueAsBytes(node); // compact (Tier 5 B2)
  }
}
