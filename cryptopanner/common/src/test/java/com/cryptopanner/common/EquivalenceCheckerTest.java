package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class EquivalenceCheckerTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final EquivalenceChecker checker = new EquivalenceChecker(mapper);

  /** Wraps an inner Binance frame as a ws_frame capture-envelope line (what's on disk). */
  private String line(String stream, String dataJson) {
    String raw = "{\"stream\":\"btcusdt@" + stream + "\",\"data\":" + dataJson + "}";
    return CaptureEnvelope.wsFrame(mapper, raw, Instant.EPOCH);
  }

  private List<String> tradeLines(long... ids) {
    List<String> out = new ArrayList<>();
    for (long id : ids) {
      out.add(line("trade", "{\"t\":" + id + ",\"T\":" + (1000 + id) + "}"));
    }
    return out;
  }

  @Test
  void idStreamIdenticalIdsPass() {
    EquivalenceChecker.Result r = checker.check("trade", tradeLines(1, 2, 3), tradeLines(1, 2, 3));
    assertTrue(r.pass(), r.detail());
  }

  @Test
  void idStreamInteriorHoleFails() {
    // candidate is missing interior ID 2 — a real divergence, not an edge straddle.
    EquivalenceChecker.Result r = checker.check("trade", tradeLines(1, 2, 3), tradeLines(1, 3));
    assertFalse(r.pass(), "interior missing ID must fail");
    assertTrue(r.detail().contains("2"), r.detail());
  }

  @Test
  void idStreamEdgeStragglersPass() {
    // primary has min edge 1, candidate has max edge 4: both differing IDs are at the union edges.
    EquivalenceChecker.Result r = checker.check("trade", tradeLines(1, 2, 3), tradeLines(2, 3, 4));
    assertTrue(r.pass(), "one straddling frame at each edge is allowed: " + r.detail());
  }

  @Test
  void idStreamInteriorPlusEdgeFails() {
    // symmetric diff {2,4}: 4 is an edge but 2 is interior → fail.
    EquivalenceChecker.Result r = checker.check("trade", tradeLines(1, 2, 3), tradeLines(1, 3, 4));
    assertFalse(r.pass(), r.detail());
  }

  @Test
  void depthStreamUsesUpdateIdField() {
    List<String> p =
        List.of(
            line("depth@100ms", "{\"u\":10,\"E\":1}"), line("depth@100ms", "{\"u\":11,\"E\":2}"));
    List<String> c =
        List.of(
            line("depth@100ms", "{\"u\":10,\"E\":1}"), line("depth@100ms", "{\"u\":11,\"E\":2}"));
    assertTrue(checker.check("depth@100ms", p, c).pass());
  }

  @Test
  void nonIdStreamSameTimeHashMultisetPasses() {
    List<String> p =
        List.of(
            line("ticker", "{\"E\":1,\"c\":\"100\"}"), line("ticker", "{\"E\":2,\"c\":\"101\"}"));
    List<String> c =
        List.of(
            line("ticker", "{\"E\":1,\"c\":\"100\"}"), line("ticker", "{\"E\":2,\"c\":\"101\"}"));
    assertTrue(checker.check("ticker", p, c).pass());
  }

  @Test
  void nonIdStreamDivergentPayloadFails() {
    // same event time, different payload bytes → different hash → multiset mismatch → fail.
    List<String> p = List.of(line("ticker", "{\"E\":1,\"c\":\"100\"}"));
    List<String> c = List.of(line("ticker", "{\"E\":1,\"c\":\"999\"}"));
    assertFalse(checker.check("ticker", p, c).pass());
  }
}
