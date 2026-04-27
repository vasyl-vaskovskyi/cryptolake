package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Clocks;
import com.cryptolake.verify.gaps.BackfillEnvelopeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link BackfillEnvelopeFactory}.
 *
 * <p>Verifies wall-clock semantic parity with Python {@code _wrap_backfill_envelope}
 * (gaps.py:60-77) and exchange_ts fallback behavior. Added after Architect rejection to close the
 * coverage gap identified in the gate-5 harness.
 */
class BackfillEnvelopeFactoryTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();
  private static final long FIXED_NS = 1_700_000_000_123_456_789L;

  /** Builds a minimal Binance aggTrade-like record with a known timestamp field. */
  private ObjectNode makeRecord(String tsKey, long tsValue) {
    ObjectNode rec = MAPPER.createObjectNode();
    rec.put("a", 123456789L); // agg trade id
    rec.put("p", "29000.50"); // price
    rec.put("q", "0.001"); // qty
    rec.put(tsKey, tsValue);
    return rec;
  }

  /** Builds a minimal record WITHOUT the timestamp field (missing-key case). */
  private ObjectNode makeRecordWithoutTs() {
    ObjectNode rec = MAPPER.createObjectNode();
    rec.put("a", 987654321L);
    rec.put("p", "30000.00");
    rec.put("q", "0.002");
    // intentionally omitting "T"
    return rec;
  }

  /**
   * Asserts that {@code received_at} in the wrapped envelope equals the fixed clock value — not
   * {@code System.nanoTime()} or any other non-wall-clock source. This locks in Tier 5 E2
   * wall-clock parity with Python's {@code time.time_ns()}.
   */
  @Test
  void wrap_receivedAt_usesInjectedWallClockNanos() throws IOException {
    ObjectNode rawRecord = makeRecord("T", 1_700_000_000_000L);

    byte[] envBytes =
        BackfillEnvelopeFactory.wrap(
            rawRecord,
            "binance",
            "btcusdt",
            "trades",
            "backfill-test-session",
            0L,
            "T",
            MAPPER,
            Clocks.fixed(FIXED_NS));

    JsonNode env = MAPPER.readTree(envBytes);
    assertThat(env.get("received_at").longValue())
        .as("received_at must equal the injected wall-clock value (Tier 5 E2)")
        .isEqualTo(FIXED_NS);
  }

  /**
   * Asserts that {@code exchange_ts} is taken from the raw record's timestamp field when present.
   */
  @Test
  void wrap_exchangeTs_usesRecordTimestampField() throws IOException {
    long tradeTs = 1_700_000_000_000L;
    ObjectNode rawRecord = makeRecord("T", tradeTs);

    byte[] envBytes =
        BackfillEnvelopeFactory.wrap(
            rawRecord,
            "binance",
            "btcusdt",
            "trades",
            "backfill-test-session",
            0L,
            "T",
            MAPPER,
            Clocks.fixed(FIXED_NS));

    JsonNode env = MAPPER.readTree(envBytes);
    assertThat(env.get("exchange_ts").longValue())
        .as("exchange_ts must equal the record's T field value")
        .isEqualTo(tradeTs);
  }

  /**
   * Asserts that {@code exchange_ts} falls back to {@code 0L} when the key is absent — matching
   * Python's {@code raw_record.get(exchange_ts_key, 0)} (gaps.py:69).
   *
   * <p>This is the parity fix for Blocker 2: previously the Java code used {@code
   * System.currentTimeMillis()} as the fallback, diverging from Python.
   */
  @Test
  void wrap_exchangeTs_fallsBackToZeroWhenKeyAbsent() throws IOException {
    ObjectNode rawRecord = makeRecordWithoutTs();

    byte[] envBytes =
        BackfillEnvelopeFactory.wrap(
            rawRecord,
            "binance",
            "btcusdt",
            "trades",
            "backfill-test-session",
            0L,
            "T", // key absent in rawRecord
            MAPPER,
            Clocks.fixed(FIXED_NS));

    JsonNode env = MAPPER.readTree(envBytes);
    assertThat(env.get("exchange_ts").longValue())
        .as("exchange_ts must be 0L when timestamp key is absent (parity with Python fallback=0)")
        .isEqualTo(0L);
  }

  /**
   * Smoke test: verifies the envelope fields are fully populated and consistent across a normal
   * wrap call.
   */
  @Test
  void wrap_producesWellFormedEnvelope() throws IOException {
    long tradeTs = 1_699_999_900_000L;
    ObjectNode rawRecord = makeRecord("T", tradeTs);

    byte[] envBytes =
        BackfillEnvelopeFactory.wrap(
            rawRecord,
            "binance",
            "ethusdt",
            "trades",
            "backfill-session-42",
            7L,
            "T",
            MAPPER,
            Clocks.fixed(FIXED_NS));

    JsonNode env = MAPPER.readTree(envBytes);
    assertThat(env.get("v").intValue()).isEqualTo(1);
    assertThat(env.get("type").textValue()).isEqualTo("data");
    assertThat(env.get("source").textValue()).isEqualTo("backfill");
    assertThat(env.get("exchange").textValue()).isEqualTo("binance");
    assertThat(env.get("symbol").textValue()).isEqualTo("ethusdt");
    assertThat(env.get("stream").textValue()).isEqualTo("trades");
    assertThat(env.get("received_at").longValue()).isEqualTo(FIXED_NS);
    assertThat(env.get("exchange_ts").longValue()).isEqualTo(tradeTs);
    assertThat(env.get("collector_session_id").textValue()).isEqualTo("backfill-session-42");
    assertThat(env.get("session_seq").longValue()).isEqualTo(7L);
    assertThat(env.get("_topic").textValue()).isEqualTo("backfill");
    assertThat(env.get("_partition").intValue()).isEqualTo(0);
    assertThat(env.get("_offset").longValue()).isEqualTo(7L);
    // raw_sha256 consistency
    String rawText = env.get("raw_text").textValue();
    String sha = com.cryptolake.common.util.Sha256.hexDigestUtf8(rawText);
    assertThat(env.get("raw_sha256").textValue())
        .as("raw_sha256 must be SHA-256 of raw_text")
        .isEqualTo(sha);
  }
}
