package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class SequenceIdTest {

  @Test
  void wsIdFieldPerStream() {
    assertEquals("t", SequenceId.idField("trade"));
    assertEquals("a", SequenceId.idField("aggTrade"));
    assertNull(SequenceId.idField("ticker"));
    assertNull(SequenceId.idField("depth@100ms"));
  }

  @Test
  void restIdFieldPerStream() {
    // REST backfill responses key the ID differently than the WS frame: trades use "id".
    assertEquals("id", SequenceId.restIdField("trade"));
    assertEquals("a", SequenceId.restIdField("aggTrade"));
    assertNull(SequenceId.restIdField("ticker"));
  }
}
