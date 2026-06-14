package com.cryptopanner.mockbinance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class WsServerTest {

  @Test
  void extractIntField_handlesWhitespaceAndOrderings() {
    assertEquals(1, WsServer.extractIntField("{\"method\":\"SUBSCRIBE\",\"id\":1}", "id"));
    assertEquals(42, WsServer.extractIntField("{ \"id\" : 42 , \"method\": \"X\" }", "id"));
    assertNull(WsServer.extractIntField("{\"method\":\"X\"}", "id"));
  }
}
