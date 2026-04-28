package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.validation.CrossSourcePuChainValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests that CrossSourcePuChainValidator is correctly invoked during post-merge verify.
 *
 * <p>Exercises the validator directly (since VerifyCommand requires full archive files for
 * integration testing, which is covered by the parity harness).
 */
class VerifyCrossSourcePuChainTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static ObjectNode makeDepthNode(
      String symbol, long bigU, long u, long pu, long receivedAt) {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("v", 1);
    node.put("type", "data");
    node.put("exchange", "binance");
    node.put("symbol", symbol);
    node.put("stream", "depth");
    node.put("received_at", receivedAt);
    node.put("exchange_ts", receivedAt);
    node.put("collector_session_id", "s1");
    node.put("session_seq", 1);
    node.put("raw_text", "{\"U\":" + bigU + ",\"u\":" + u + ",\"pu\":" + pu + "}");
    node.put("raw_sha256", "dummy");
    return node;
  }

  // ── test 1: clean chain emits no errors ──────────────────────────────────

  @Test
  void cleanChainEmitsNoErrors() {
    List<String> errors = new ArrayList<>();
    CrossSourcePuChainValidator validator =
        new CrossSourcePuChainValidator(
            (exchange, symbol, detail, gapStart, gapEnd) ->
                errors.add("cross_source_pu_chain_break: " + symbol + " " + detail));

    // u=100 → u=110 (pu=100 matches last_u=100) → clean
    com.cryptolake.common.envelope.DataEnvelope env1 =
        com.cryptolake.common.envelope.DataEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "{\"U\":90,\"u\":100,\"pu\":89}",
            0L,
            "s1",
            1L,
            () -> 1_000L);
    com.cryptolake.common.envelope.DataEnvelope env2 =
        com.cryptolake.common.envelope.DataEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "{\"U\":101,\"u\":110,\"pu\":100}",
            0L,
            "s1",
            2L,
            () -> 2_000L);

    validator.handle(env1);
    validator.handle(env2);

    assertThat(errors).isEmpty();
  }

  // ── test 2: break emits error ─────────────────────────────────────────────

  @Test
  void breakEmitsError() {
    List<String> errors = new ArrayList<>();
    CrossSourcePuChainValidator validator =
        new CrossSourcePuChainValidator(
            (exchange, symbol, detail, gapStart, gapEnd) ->
                errors.add("cross_source_pu_chain_break: " + symbol + " " + detail));

    // u=200 → then pu=209 (gap: expected 200, got 209)
    com.cryptolake.common.envelope.DataEnvelope env1 =
        com.cryptolake.common.envelope.DataEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "{\"U\":190,\"u\":200,\"pu\":189}",
            0L,
            "s1",
            1L,
            () -> 1_000L);
    com.cryptolake.common.envelope.DataEnvelope env2 =
        com.cryptolake.common.envelope.DataEnvelope.create(
            "binance",
            "btcusdt",
            "depth",
            "{\"U\":210,\"u\":220,\"pu\":209}",
            0L,
            "s1",
            2L,
            () -> 2_000L);

    validator.handle(env1);
    validator.handle(env2);

    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("cross_source_pu_chain_break");
    assertThat(errors.get(0)).contains("btcusdt");
    assertThat(errors.get(0)).contains("pu=209");
    assertThat(errors.get(0)).contains("expected=200");
  }
}
