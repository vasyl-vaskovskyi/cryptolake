// ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_serialize_data_envelope_to_bytes
// ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_serialize_gap_envelope_to_bytes
// ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_deserialize_envelope_round_trip
// ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_add_broker_coordinates
package com.cryptolake.common.envelope;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.util.Clocks;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EnvelopeCodecTest {

  private EnvelopeCodec codec;
  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    mapper = EnvelopeCodec.newMapper();
    codec = new EnvelopeCodec(mapper);
  }

  @Test
  void serializeDataEnvelope() throws Exception {
    // ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_serialize_data_envelope_to_bytes
    DataEnvelope env =
        DataEnvelope.create(
            "binance", "btcusdt", "trades", "{\"test\": 1}", 100L, "s", 0L, Clocks.fixed(42L));

    byte[] bytes = codec.toJsonBytes(env);
    assertThat(bytes).isNotEmpty();

    JsonNode parsed = mapper.readTree(bytes);
    assertThat(parsed.get("type").asText()).isEqualTo("data");
    assertThat(parsed.get("raw_text").asText()).isEqualTo("{\"test\": 1}");
  }

  @Test
  void serializeGapEnvelope() throws Exception {
    // ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_serialize_gap_envelope_to_bytes
    GapEnvelope env =
        GapEnvelope.create(
            "binance", "btcusdt", "depth", "s", 0L, 0L, 1L, "ws_disconnect", "test",
            Clocks.fixed(0L));

    byte[] bytes = codec.toJsonBytes(env);
    JsonNode parsed = mapper.readTree(bytes);

    assertThat(parsed.get("type").asText()).isEqualTo("gap");
    assertThat(parsed.has("raw_text")).isFalse();
    assertThat(parsed.has("raw_sha256")).isFalse();
  }

  @Test
  void roundTripPreservesFields() throws Exception {
    // ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_deserialize_envelope_round_trip
    DataEnvelope original =
        DataEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "{\"e\":\"aggTrade\",\"p\":\"0.00100000\"}",
            1741689600120L,
            "test_session",
            42L,
            Clocks.fixed(999_000_000_000L));

    byte[] bytes = codec.toJsonBytes(original);
    DataEnvelope roundTripped = codec.readData(bytes);

    assertThat(roundTripped.v()).isEqualTo(original.v());
    assertThat(roundTripped.type()).isEqualTo(original.type());
    assertThat(roundTripped.exchange()).isEqualTo(original.exchange());
    assertThat(roundTripped.symbol()).isEqualTo(original.symbol());
    assertThat(roundTripped.stream()).isEqualTo(original.stream());
    assertThat(roundTripped.receivedAt()).isEqualTo(original.receivedAt());
    assertThat(roundTripped.exchangeTs()).isEqualTo(original.exchangeTs());
    assertThat(roundTripped.rawText()).isEqualTo(original.rawText());
    assertThat(roundTripped.rawSha256()).isEqualTo(original.rawSha256());
    assertThat(roundTripped.collectorSessionId()).isEqualTo(original.collectorSessionId());
    assertThat(roundTripped.sessionSeq()).isEqualTo(original.sessionSeq());
  }

  @Test
  void withBrokerCoordinatesAppendsFields() throws Exception {
    // ports: tests/unit/test_envelope.py::TestEnvelopeSerialization::test_add_broker_coordinates
    DataEnvelope env =
        DataEnvelope.create(
            "binance", "btcusdt", "trades", "{}", 100L, "s", 0L, Clocks.fixed(0L));

    BrokerCoordinates coords = new BrokerCoordinates("binance.trades", 0, 42L);
    Object wrapped = EnvelopeCodec.withBrokerCoordinates(env, coords);
    byte[] bytes = codec.toJsonBytes(wrapped);

    JsonNode parsed = mapper.readTree(bytes);
    assertThat(parsed.get("_topic").asText()).isEqualTo("binance.trades");
    assertThat(parsed.get("_partition").asInt()).isEqualTo(0);
    assertThat(parsed.get("_offset").asLong()).isEqualTo(42L);
    // Original envelope fields should also be present
    assertThat(parsed.get("type").asText()).isEqualTo("data");
  }

  @Test
  void fieldOrderMatchesPython() throws Exception {
    // New test (design §8 "New tests added") — Tier 3 §21 + Tier 5 B1
    // Asserts that serialized JSON of a fixed envelope has the exact field order matching Python
    DataEnvelope env =
        DataEnvelope.create(
            "binance",
            "btcusdt",
            "trades",
            "{\"e\":\"aggTrade\"}",
            1741689600000L,
            "binance-collector-01_2026-01-01T00:00:00Z",
            1L,
            Clocks.fixed(1_741_689_600_000_000_000L));

    byte[] bytes = codec.toJsonBytes(env);
    String json = new String(bytes, StandardCharsets.UTF_8);

    // Verify field order by checking position indices
    int vPos = json.indexOf("\"v\"");
    int typePos = json.indexOf("\"type\"");
    int exchangePos = json.indexOf("\"exchange\"");
    int symbolPos = json.indexOf("\"symbol\"");
    int streamPos = json.indexOf("\"stream\"");
    int receivedAtPos = json.indexOf("\"received_at\"");
    int exchangeTsPos = json.indexOf("\"exchange_ts\"");
    int sessionIdPos = json.indexOf("\"collector_session_id\"");
    int sessionSeqPos = json.indexOf("\"session_seq\"");
    int rawTextPos = json.indexOf("\"raw_text\"");
    int rawSha256Pos = json.indexOf("\"raw_sha256\"");

    assertThat(vPos).isLessThan(typePos);
    assertThat(typePos).isLessThan(exchangePos);
    assertThat(exchangePos).isLessThan(symbolPos);
    assertThat(symbolPos).isLessThan(streamPos);
    assertThat(streamPos).isLessThan(receivedAtPos);
    assertThat(receivedAtPos).isLessThan(exchangeTsPos);
    assertThat(exchangeTsPos).isLessThan(sessionIdPos);
    assertThat(sessionIdPos).isLessThan(sessionSeqPos);
    assertThat(sessionSeqPos).isLessThan(rawTextPos);
    assertThat(rawTextPos).isLessThan(rawSha256Pos);
  }

  @Test
  void readTreeForRouting() throws Exception {
    byte[] frame = "{\"stream\":\"btcusdt@aggTrade\",\"data\":{}}".getBytes(StandardCharsets.UTF_8);
    JsonNode node = codec.readTree(frame);
    assertThat(node.get("stream").asText()).isEqualTo("btcusdt@aggTrade");
  }

  @Test
  void appendNewlineAddsLf() {
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] result = codec.appendNewline(bytes);
    assertThat(result).hasSize(6);
    assertThat(result[5]).isEqualTo((byte) 0x0A);
  }
}
