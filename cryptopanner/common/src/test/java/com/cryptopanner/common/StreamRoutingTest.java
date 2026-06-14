package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class StreamRoutingTest {

  @Test
  void publicStreams() {
    assertEquals(StreamRouting.Socket.PUBLIC, StreamRouting.forStreamType("trade"));
    assertEquals(StreamRouting.Socket.PUBLIC, StreamRouting.forStreamType("depth@100ms"));
    assertEquals(StreamRouting.Socket.PUBLIC, StreamRouting.forStreamType("depth"));
    assertEquals(StreamRouting.Socket.PUBLIC, StreamRouting.forStreamType("bookTicker"));
  }

  @Test
  void marketStreams() {
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("aggTrade"));
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("kline_1m"));
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("kline_5m"));
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("ticker"));
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("markPrice@1s"));
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forStreamType("markPrice"));
  }

  @Test
  void unknownStreamThrows() {
    assertThrows(IllegalArgumentException.class, () -> StreamRouting.forStreamType("foo"));
  }

  @Test
  void forceOrderBroadcast() {
    assertEquals(StreamRouting.Socket.MARKET, StreamRouting.forBroadcast("!forceOrder@arr"));
  }
}
