package com.cryptopanner.sealer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class SequenceAnalyzerTest {

  private static final ObjectMapper M = new ObjectMapper();

  @Test
  void returnsNullForNonIdBearingStream() throws IOException {
    byte[] bytes = "{\"data\":{\"E\":1}}\n".getBytes(StandardCharsets.UTF_8);
    assertNull(SequenceAnalyzer.analyze(bytes, "ticker", M));
  }

  @Test
  void emptyInputProducesEmptyAnalysis() throws IOException {
    SequenceAnalyzer.Analysis a = SequenceAnalyzer.analyze(new byte[0], "trade", M);
    assertEquals(-1L, a.firstId());
    assertEquals(-1L, a.lastId());
    assertTrue(a.gaps().isEmpty());
  }

  @Test
  void contiguousIdsHaveNoGaps() throws IOException {
    String input =
        "{\"data\":{\"t\":100}}\n" + "{\"data\":{\"t\":101}}\n" + "{\"data\":{\"t\":102}}\n";
    SequenceAnalyzer.Analysis a =
        SequenceAnalyzer.analyze(input.getBytes(StandardCharsets.UTF_8), "trade", M);
    assertEquals(100L, a.firstId());
    assertEquals(102L, a.lastId());
    assertTrue(a.gaps().isEmpty());
  }

  @Test
  void detectsMultipleGapsForAggTrade() throws IOException {
    String input =
        "{\"data\":{\"a\":10}}\n"
            + "{\"data\":{\"a\":11}}\n"
            + "{\"data\":{\"a\":15}}\n" // gap: 12..14 (count 3)
            + "{\"data\":{\"a\":17}}\n" // gap: 16..16 (count 1)
            + "{\"data\":{\"a\":18}}\n";
    SequenceAnalyzer.Analysis a =
        SequenceAnalyzer.analyze(input.getBytes(StandardCharsets.UTF_8), "aggTrade", M);
    assertEquals(10L, a.firstId());
    assertEquals(18L, a.lastId());
    assertEquals(2, a.gaps().size());
    assertEquals(12L, a.gaps().get(0).from());
    assertEquals(14L, a.gaps().get(0).to());
    assertEquals(3L, a.gaps().get(0).count());
    assertEquals(16L, a.gaps().get(1).from());
    assertEquals(16L, a.gaps().get(1).to());
    assertEquals(1L, a.gaps().get(1).count());
  }

  @Test
  void duplicatesAreNotGaps() throws IOException {
    String input =
        "{\"data\":{\"t\":50}}\n" + "{\"data\":{\"t\":50}}\n" + "{\"data\":{\"t\":51}}\n";
    SequenceAnalyzer.Analysis a =
        SequenceAnalyzer.analyze(input.getBytes(StandardCharsets.UTF_8), "trade", M);
    assertEquals(50L, a.firstId());
    assertEquals(51L, a.lastId());
    assertTrue(a.gaps().isEmpty());
  }

  @Test
  void missingIdFieldThrows() {
    String input = "{\"data\":{\"E\":1}}\n";
    assertThrows(
        IOException.class,
        () -> SequenceAnalyzer.analyze(input.getBytes(StandardCharsets.UTF_8), "trade", M));
  }
}
