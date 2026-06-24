package com.cryptopanner.mockbinance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The mock's wall-clock event-time rewrite (master spec §14.d/§14.e enabler): replay frames carry
 * the original fixed fixture timestamps, so without rewriting all frames bucket into the same
 * historical minute and minutes never seal during a run. The rewriter restamps the Binance event
 * time {@code E} and trade/transaction time {@code T} to "now" so frames fill consecutive real-time
 * minutes — while leaving everything else (event-type strings, ids, sequence/pu-chain fields,
 * prices) byte-for-byte intact.
 */
class EventTimeRewriterTest {

  private static final long NOW = 1_900_000_000_000L;
  private static final String NOW_S = "1900000000000";

  private static int count(String haystack, String needle) {
    int n = 0;
    for (int i = haystack.indexOf(needle);
        i >= 0;
        i = haystack.indexOf(needle, i + needle.length())) {
      n++;
    }
    return n;
  }

  @Test
  void rewritesUppercaseEventAndTradeTime() {
    String in =
        "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1718380800000,\"s\":\"BTCUSDT\",\"t\":3500000001,\"p\":\"60001.00\",\"q\":\"0.001\",\"T\":1718380799990,\"m\":false}}";
    String out = EventTimeRewriter.rewrite(in, NOW);
    assertTrue(out.contains("\"E\":" + NOW_S), out);
    assertTrue(out.contains("\"T\":" + NOW_S), out);
  }

  @Test
  void leavesEventTypeStringAndTradeIdUntouched() {
    String in = "{\"data\":{\"e\":\"trade\",\"E\":1,\"t\":3500000001,\"T\":2}}";
    String out = EventTimeRewriter.rewrite(in, NOW);
    assertTrue(out.contains("\"e\":\"trade\""), out); // lowercase event-type string
    assertTrue(out.contains("\"t\":3500000001"), out); // lowercase trade id
    assertTrue(out.contains("\"E\":" + NOW_S), out);
    assertTrue(out.contains("\"T\":" + NOW_S), out);
  }

  @Test
  void rewritesNestedTimestamps() {
    String in = "{\"data\":{\"e\":\"forceOrder\",\"E\":111,\"o\":{\"s\":\"BTCUSDT\",\"T\":222}}}";
    String out = EventTimeRewriter.rewrite(in, NOW);
    assertEquals(2, count(out, NOW_S), out); // top-level E + nested o.T
    assertFalse(out.contains("111"), out);
    assertFalse(out.contains("222"), out);
  }

  @Test
  void leavesDepthSequenceFieldsUntouched() {
    // depth pu-chain (U/u/pu) must survive so verify's continuity check still passes.
    String in = "{\"data\":{\"e\":\"depthUpdate\",\"E\":1,\"T\":2,\"U\":100,\"u\":200,\"pu\":199}}";
    String out = EventTimeRewriter.rewrite(in, NOW);
    assertTrue(out.contains("\"U\":100"), out);
    assertTrue(out.contains("\"u\":200"), out);
    assertTrue(out.contains("\"pu\":199"), out);
    assertTrue(out.contains("\"E\":" + NOW_S), out);
    assertTrue(out.contains("\"T\":" + NOW_S), out);
  }

  @Test
  void handlesWhitespaceAroundColon() {
    assertTrue(EventTimeRewriter.rewrite("{\"E\" : 5}", NOW).contains(NOW_S));
  }

  @Test
  void leavesFramesWithoutTimestampsUnchanged() {
    String in = "{\"result\":null,\"id\":1}";
    assertEquals(in, EventTimeRewriter.rewrite(in, NOW));
  }

  @Test
  void doesNotTouchQuotedPricesOrSymbols() {
    String in = "{\"data\":{\"E\":1,\"s\":\"ETHUSDT\",\"p\":\"3000.50\"}}";
    String out = EventTimeRewriter.rewrite(in, NOW);
    assertTrue(out.contains("\"s\":\"ETHUSDT\""), out);
    assertTrue(out.contains("\"p\":\"3000.50\""), out);
  }
}
