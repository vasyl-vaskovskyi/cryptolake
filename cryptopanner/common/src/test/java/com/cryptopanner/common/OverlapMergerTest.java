package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OverlapMergerTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private final OverlapMerger merger = new OverlapMerger(mapper);

  private String line(String stream, String dataJson) {
    String raw = "{\"stream\":\"btcusdt@" + stream + "\",\"data\":" + dataJson + "}";
    return CaptureEnvelope.wsFrame(mapper, raw, Instant.EPOCH);
  }

  private long innerLong(String envLine, String field) throws Exception {
    JsonNode env = mapper.readTree(envLine);
    return CaptureEnvelope.unwrap(mapper, env).get("data").get(field).asLong();
  }

  private List<Long> ids(List<String> lines, String field) throws Exception {
    List<Long> out = new ArrayList<>();
    for (String l : lines) out.add(innerLong(l, field));
    return out;
  }

  @Test
  void idStreamUnionSortedAscendingNoDuplicates() throws Exception {
    List<String> primary =
        List.of(line("trade", "{\"t\":1,\"T\":1001}"), line("trade", "{\"t\":2,\"T\":1002}"));
    List<String> candidate =
        List.of(line("trade", "{\"t\":2,\"T\":1002}"), line("trade", "{\"t\":3,\"T\":1003}"));

    OverlapMerger.Merged m = merger.merge("trade", primary, candidate);
    assertEquals(
        List.of(1L, 2L, 3L), ids(m.lines(), "t"), "union by ID, sorted ascending, deduped");
    assertTrue(m.divergences().isEmpty());
  }

  @Test
  void idStreamDivergentBytesKeepsPrimaryAndRecords() throws Exception {
    // same trade ID 2, different bytes (T differs) → keep primary's bytes, record the divergence.
    List<String> primary = List.of(line("trade", "{\"t\":2,\"T\":1002}"));
    List<String> candidate = List.of(line("trade", "{\"t\":2,\"T\":9999}"));

    OverlapMerger.Merged m = merger.merge("trade", primary, candidate);
    assertEquals(1, m.lines().size());
    assertEquals(1002L, innerLong(m.lines().get(0), "T"), "primary bytes win");
    assertFalse(m.divergences().isEmpty(), "divergent ID must be recorded");
    assertTrue(m.divergences().get(0).contains("2"), m.divergences().toString());
  }

  @Test
  void depthStreamUnionByUpdateId() throws Exception {
    List<String> primary = List.of(line("depth@100ms", "{\"u\":10,\"E\":1}"));
    List<String> candidate =
        List.of(
            line("depth@100ms", "{\"u\":10,\"E\":1}"), line("depth@100ms", "{\"u\":11,\"E\":2}"));

    OverlapMerger.Merged m = merger.merge("depth@100ms", primary, candidate);
    assertEquals(List.of(10L, 11L), ids(m.lines(), "u"));
  }

  @Test
  void nonIdStreamUnionSortedByEventTime() throws Exception {
    // candidate carries an extra later event; merged is the union sorted by event time ascending.
    List<String> primary = List.of(line("ticker", "{\"E\":2,\"c\":\"b\"}"));
    List<String> candidate =
        List.of(line("ticker", "{\"E\":2,\"c\":\"b\"}"), line("ticker", "{\"E\":1,\"c\":\"a\"}"));

    OverlapMerger.Merged m = merger.merge("ticker", primary, candidate);
    assertEquals(
        List.of(1L, 2L), ids(m.lines(), "E"), "non-ID union sorted by event time ascending");
  }
}
