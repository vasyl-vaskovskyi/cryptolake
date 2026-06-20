package com.cryptopanner.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class EventTimeTest {

  private static final ObjectMapper MAPPER = EnvelopeCodec.newMapper();

  private static JsonNode data(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void tradeBucketsByTradeTimeT() {
    JsonNode d = data("{\"e\":\"trade\",\"E\":1750000000182,\"T\":1750000000180,\"t\":145003}");
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000180L)), EventTime.bucketInstant("trade", d));
  }

  @Test
  void aggTradeBucketsByTradeTimeT() {
    JsonNode d = data("{\"e\":\"aggTrade\",\"E\":1750000000182,\"T\":1750000000180,\"a\":900}");
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000180L)), EventTime.bucketInstant("aggTrade", d));
  }

  @Test
  void depthBucketsByEventTimeE() {
    JsonNode d = data("{\"e\":\"depthUpdate\",\"E\":1750000000182,\"U\":1,\"u\":2,\"pu\":0}");
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000182L)),
        EventTime.bucketInstant("depth@100ms", d));
  }

  @Test
  void otherStreamsBucketByEventTimeE() {
    JsonNode d = data("{\"E\":1750000000182}");
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000182L)), EventTime.bucketInstant("ticker", d));
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000182L)), EventTime.bucketInstant("kline_1m", d));
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000182L)),
        EventTime.bucketInstant("markPrice@1s", d));
  }

  @Test
  void forceOrderBroadcastBucketsByEventTimeE() {
    JsonNode d =
        data(
            "{\"e\":\"forceOrder\",\"E\":1750000000182,\"o\":{\"s\":\"BTCUSDT\",\"T\":1750000000180}}");
    assertEquals(
        Optional.of(Instant.ofEpochMilli(1750000000182L)),
        EventTime.bucketInstant("!forceOrder@arr", d));
  }

  @Test
  void missingTimestampFieldReturnsEmpty() {
    JsonNode tradeNoT = data("{\"e\":\"trade\",\"E\":1750000000182,\"t\":145003}");
    assertTrue(EventTime.bucketInstant("trade", tradeNoT).isEmpty());

    JsonNode noE = data("{\"e\":\"depthUpdate\",\"U\":1,\"u\":2}");
    assertTrue(EventTime.bucketInstant("depth", noE).isEmpty());
  }
}
