package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.common.util.Sha256;
import com.cryptolake.verify.verify.EnvelopeVerifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ports: tests/unit/cli/test_verify.py::TestEnvelopeValidation */
class EnvelopeVerifierTest {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  private ObjectNode buildDataEnvelope(String rawText) {
    ObjectNode node = mapper.createObjectNode();
    node.put("v", 1);
    node.put("type", "data");
    node.put("exchange", "binance");
    node.put("symbol", "btcusdt");
    node.put("stream", "trades");
    node.put("received_at", 1000000000000000000L);
    node.put("exchange_ts", 1700000000000L);
    node.put("collector_session_id", "test-session");
    node.put("session_seq", 1L);
    node.put("raw_text", rawText);
    node.put("raw_sha256", Sha256.hexDigestUtf8(rawText));
    node.put("_topic", "binance.btcusdt.trades");
    node.put("_partition", 0);
    node.put("_offset", 42L);
    return node;
  }

  @Test
  void validDataEnvelopePasses() {
    // ports: tests/unit/cli/test_verify.py::TestEnvelopeValidation::test_valid
    ObjectNode env = buildDataEnvelope("{\"a\":1}");
    List<String> errors = EnvelopeVerifier.verify(List.of(env));
    assertThat(errors).isEmpty();
  }

  @Test
  void sha256MismatchDetected() {
    // ports: tests/unit/cli/test_verify.py::TestEnvelopeValidation::test_sha256_mismatch
    ObjectNode env = buildDataEnvelope("{\"a\":1}");
    env.put("raw_sha256", "deadbeef");
    List<String> errors = EnvelopeVerifier.verify(List.of(env));
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("raw_sha256 mismatch");
  }

  @Test
  void missingFieldDetected() {
    // ports: tests/unit/cli/test_verify.py::TestEnvelopeValidation::test_missing_field
    ObjectNode env = buildDataEnvelope("{\"a\":1}");
    env.remove("_offset"); // remove a required field
    List<String> errors = EnvelopeVerifier.verify(List.of(env));
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("missing fields");
    assertThat(errors.get(0)).contains("'_offset'");
  }
}
