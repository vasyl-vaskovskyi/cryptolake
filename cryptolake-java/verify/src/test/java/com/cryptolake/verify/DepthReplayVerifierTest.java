package com.cryptolake.verify;

import static org.assertj.core.api.Assertions.assertThat;

import com.cryptolake.common.envelope.EnvelopeCodec;
import com.cryptolake.verify.verify.DepthReplayVerifier;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.junit.jupiter.api.Test;

/** ports: tests/unit/cli/test_verify.py::TestDepthReplay */
class DepthReplayVerifierTest {

  private final ObjectMapper mapper = EnvelopeCodec.newMapper();

  private ObjectNode buildDepthEnv(String symbol, long U, long u, long pu, long receivedAt) {
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "data");
    node.put("stream", "depth");
    node.put("symbol", symbol);
    node.put("received_at", receivedAt);
    // Build raw_text with U/u/pu fields
    node.put(
        "raw_text",
        "{\"U\":"
            + U
            + ",\"u\":"
            + u
            + ",\"pu\":"
            + pu
            + ",\"s\":\""
            + symbol.toUpperCase()
            + "\"}");
    return node;
  }

  private ObjectNode buildSnapshotEnv(String symbol, long lastUpdateId, long receivedAt) {
    ObjectNode node = mapper.createObjectNode();
    node.put("type", "data");
    node.put("stream", "depth_snapshot");
    node.put("symbol", symbol);
    node.put("received_at", receivedAt);
    node.put("raw_text", "{\"lastUpdateId\":" + lastUpdateId + "}");
    return node;
  }

  @Test
  void emptyDepthReturnsNoErrors() {
    // ports: test no envelopes
    List<String> errors = DepthReplayVerifier.verify(List.of(), List.of(), List.of(), mapper);
    assertThat(errors).isEmpty();
  }

  @Test
  void firstDiffSpansSnapshotSyncs() {
    // ports: tests/unit/cli/test_verify.py::TestDepthReplay::test_first_diff_spans_snapshot_syncs
    // lastUpdateId=100, diff U=50,u=101 → spans (50 <= 101 <= 101)
    JsonNode snap = buildSnapshotEnv("BTCUSDT", 100L, 1000L);
    JsonNode diff = buildDepthEnv("BTCUSDT", 50L, 101L, 0L, 2000L);
    JsonNode diff2 = buildDepthEnv("BTCUSDT", 102L, 103L, 101L, 3000L);
    List<String> errors =
        DepthReplayVerifier.verify(List.of(diff, diff2), List.of(snap), List.of(), mapper);
    assertThat(errors).isEmpty();
  }

  @Test
  void puChainBreakReportedIfNoGap() {
    // ports: tests/unit/cli/test_verify.py::TestDepthReplay::test_pu_chain_break_no_gap
    JsonNode snap = buildSnapshotEnv("BTCUSDT", 100L, 1000L);
    JsonNode diff1 = buildDepthEnv("BTCUSDT", 50L, 101L, 0L, 2000L); // syncs
    // diff2 breaks: pu=999 != lastU=101
    JsonNode diff2 = buildDepthEnv("BTCUSDT", 102L, 103L, 999L, 3000L);
    List<String> errors =
        DepthReplayVerifier.verify(List.of(diff1, diff2), List.of(snap), List.of(), mapper);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("pu chain break");
    assertThat(errors.get(0)).contains("expected pu=101");
    assertThat(errors.get(0)).contains("got pu=999");
  }

  @Test
  void firstDiffNoSpanReportsError() {
    // ports: tests/unit/cli/test_verify.py::TestDepthReplay::test_first_diff_no_span_reports_error
    JsonNode snap = buildSnapshotEnv("BTCUSDT", 200L, 1000L); // lid=200
    // diff U=50,u=100 — does NOT span lid+1=201
    JsonNode diff = buildDepthEnv("BTCUSDT", 50L, 100L, 0L, 2000L);
    List<String> errors =
        DepthReplayVerifier.verify(List.of(diff), List.of(snap), List.of(), mapper);
    assertThat(errors).hasSize(1);
    assertThat(errors.get(0)).contains("First diff does not span any snapshot sync point");
  }
}
